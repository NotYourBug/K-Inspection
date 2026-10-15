"""Single-instance SQLite inbox/outbox. Use a verified local/block volume only.

Every operation owns its connection. Acceptance takes a write reservation before
deduplication and commits the receipt and ALL destinations together.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from .alert_routing import AlertBatch
from .delivery_models import AcceptResult, DeliveryJob, DeliveryResult, DeliveryTarget, StoreStats

SCHEMA_VERSION = 1


class StorageUnavailable(RuntimeError):
    pass


class BacklogFull(RuntimeError):
    pass


class UnsupportedSchema(RuntimeError):
    pass


def epoch(value: datetime) -> float:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone aware")
    return value.timestamp()


def utc(value: float | None) -> datetime | None:
    return datetime.fromtimestamp(value, timezone.utc) if value is not None else None


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


class DeliveryStore:
    def __init__(self, path: Path, *, max_backlog: int = 10000, dedup_seconds: int = 300):
        if type(max_backlog) is not int or max_backlog < 1:
            raise ValueError("max_backlog must be positive")
        if type(dedup_seconds) is not int or dedup_seconds < 0:
            raise ValueError("dedup_seconds must be non-negative")
        self.path = Path(path)
        self.max_backlog = max_backlog
        self.dedup_seconds = dedup_seconds
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._connection() as db:
                db.execute("PRAGMA journal_mode=WAL")
            with self._transaction() as db:
                db.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
                versions = db.execute("SELECT version FROM schema_version").fetchall()
                if versions and (len(versions) != 1 or versions[0][0] != SCHEMA_VERSION):
                    raise UnsupportedSchema("unsupported delivery schema")
                if not versions:
                    db.execute("INSERT INTO schema_version VALUES (?)", (SCHEMA_VERSION,))
                db.execute("""CREATE TABLE IF NOT EXISTS inbox (
                    receipt_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, received_at REAL NOT NULL,
                    normalized_payload TEXT NOT NULL, truncated INTEGER NOT NULL)""")
                db.execute("""CREATE TABLE IF NOT EXISTS outbox (
                    delivery_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, destination_id TEXT NOT NULL,
                    content_hash TEXT NOT NULL, generation INTEGER NOT NULL, payload TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('PENDING','SENDING','RETRY_WAIT','SENT','DEAD')),
                    attempt_count INTEGER NOT NULL DEFAULT 0, next_attempt_at REAL NOT NULL,
                    lease_token TEXT, lease_until REAL, external_id TEXT, error_kind TEXT,
                    created_at REAL NOT NULL, sent_at REAL, result_unknown INTEGER NOT NULL DEFAULT 0,
                    error_code TEXT, budget_started_at REAL,
                    UNIQUE(source_id,destination_id,content_hash,generation))""")
                db.execute("""CREATE TABLE IF NOT EXISTS inbox_jobs (
                    receipt_id TEXT NOT NULL REFERENCES inbox(receipt_id) ON DELETE CASCADE,
                    delivery_id TEXT NOT NULL REFERENCES outbox(delivery_id) ON DELETE CASCADE,
                    PRIMARY KEY(receipt_id,delivery_id))""")
                db.execute("CREATE INDEX IF NOT EXISTS outbox_due ON outbox(state,next_attempt_at)")
                db.execute("""CREATE TABLE IF NOT EXISTS delivery_events (
                    event_id INTEGER PRIMARY KEY, delivery_id TEXT NOT NULL REFERENCES outbox(delivery_id)
                    ON DELETE CASCADE, attempt_no INTEGER NOT NULL, lease_token TEXT, at REAL NOT NULL,
                    event_type TEXT NOT NULL, result_unknown INTEGER NOT NULL DEFAULT 0,
                    error_kind TEXT, error_code TEXT, external_id TEXT, reason TEXT)""")
                db.execute("CREATE TABLE IF NOT EXISTS store_health (id INTEGER PRIMARY KEY, probed_at REAL NOT NULL)")
        except OSError as exc:
            raise StorageUnavailable("delivery storage unavailable") from exc

    @contextmanager
    def _connection(self):
        db = None
        try:
            db = sqlite3.connect(self.path, timeout=2, isolation_level=None)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            yield db
        except sqlite3.Error as exc:
            raise StorageUnavailable("delivery storage unavailable") from exc
        finally:
            if db is not None:
                db.close()

    @contextmanager
    def _transaction(self):
        with self._connection() as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def accept(self, source_id: str, batch: AlertBatch, targets: tuple[DeliveryTarget, ...],
               now: datetime) -> AcceptResult:
        stamp = epoch(now)
        if not source_id or not targets or len({t.destination_id for t in targets}) != len(targets):
            raise ValueError("source and unique destinations are required")
        receipt = str(uuid.uuid4())
        new, reused = [], []
        with self._transaction() as db:
            db.execute("INSERT INTO inbox VALUES (?,?,?,?,?)",
                       (receipt, source_id, stamp, _json(asdict(batch)), batch.truncated_alerts))
            backlog = db.execute("SELECT count(*) FROM outbox WHERE state!='SENT'").fetchone()[0]
            for target in targets:
                row = db.execute("""SELECT * FROM outbox WHERE source_id=? AND destination_id=?
                    AND content_hash=? ORDER BY generation DESC LIMIT 1""",
                    (source_id, target.destination_id, target.content_hash)).fetchone()
                if row is not None and (row["state"] != "SENT" or
                    (row["sent_at"] is not None and stamp - row["sent_at"] < self.dedup_seconds)):
                    delivery_id = row["delivery_id"]
                    reused.append(delivery_id)
                else:
                    if backlog >= self.max_backlog:
                        raise BacklogFull("delivery backlog is full")
                    delivery_id = str(uuid.uuid4())
                    generation = row["generation"] + 1 if row else 1
                    db.execute("""INSERT INTO outbox (delivery_id,source_id,destination_id,content_hash,
                        generation,payload,state,next_attempt_at,created_at) VALUES (?,?,?,?,?,?,'PENDING',?,?)""",
                        (delivery_id, source_id, target.destination_id, target.content_hash, generation,
                         _json(target.payload), stamp, stamp))
                    backlog += 1
                    new.append(delivery_id)
                db.execute("INSERT INTO inbox_jobs VALUES (?,?)", (receipt, delivery_id))
        return AcceptResult(receipt, tuple(new), tuple(reused))

    def get_job(self, delivery_id: str) -> DeliveryJob:
        with self._connection() as db:
            row = db.execute("SELECT * FROM outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            if row is None:
                raise KeyError(delivery_id)
            return self._job(row)

    def list_jobs(self, *, state: str | None = None) -> tuple[DeliveryJob, ...]:
        with self._connection() as db:
            query = "SELECT * FROM outbox" + (" WHERE state=?" if state is not None else "")
            return tuple(self._job(r) for r in db.execute(query + " ORDER BY created_at,delivery_id",
                                                        (state,) if state is not None else ()))

    def claim_due(self, now: datetime, lease_seconds: int = 60) -> DeliveryJob | None:
        stamp = epoch(now)
        if type(lease_seconds) is not int or lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        with self._transaction() as db:
            row = db.execute("""SELECT * FROM outbox WHERE
                (state IN ('PENDING','RETRY_WAIT') AND next_attempt_at<=?) OR
                (state='SENDING' AND lease_until<=?) ORDER BY next_attempt_at,created_at,delivery_id LIMIT 1""",
                (stamp, stamp)).fetchone()
            if row is None:
                return None
            delivery_id = row["delivery_id"]
            if row["state"] == "SENDING":
                self._event(db, row, stamp, "lease_expired", unknown=True, error_kind="lease_expired")
                db.execute("UPDATE outbox SET result_unknown=1 WHERE delivery_id=?", (delivery_id,))
            if row["attempt_count"] >= 6:
                db.execute("""UPDATE outbox SET state='DEAD',lease_token=NULL,lease_until=NULL,
                    error_kind='retry_exhausted' WHERE delivery_id=?""", (delivery_id,))
                self._event(db, row, stamp, "dead", error_kind="retry_exhausted")
                return None
            token = str(uuid.uuid4())
            db.execute("""UPDATE outbox SET state='SENDING',attempt_count=attempt_count+1,
                lease_token=?,lease_until=?,budget_started_at=coalesce(budget_started_at,?)
                WHERE delivery_id=?""", (token, stamp + lease_seconds, stamp, delivery_id))
            claimed = db.execute("SELECT * FROM outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            self._event(db, claimed, stamp, "claimed")
            return self._job(claimed)

    @staticmethod
    def _event(db, row, stamp, event_type, *, unknown=False, error_kind=None,
               error_code=None, external_id=None, reason=None):
        db.execute("""INSERT INTO delivery_events (delivery_id,attempt_no,lease_token,at,event_type,
            result_unknown,error_kind,error_code,external_id,reason) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (row["delivery_id"], row["attempt_count"], row["lease_token"], stamp, event_type,
             int(unknown), error_kind, error_code, external_id, reason))

    def finish(self, delivery_id: str, lease_token: str, result: DeliveryResult, now: datetime,
               *, retry_at: datetime | None = None) -> bool:
        stamp = epoch(now)
        retry_stamp = epoch(retry_at) if retry_at is not None else None
        with self._transaction() as db:
            row = db.execute("SELECT * FROM outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            if (row is None or row["state"] != "SENDING" or row["lease_token"] != lease_token
                    or row["lease_until"] <= stamp):
                return False
            state = "SENT" if result.success else (
                "RETRY_WAIT" if result.retryable and retry_stamp is not None and row["attempt_count"] < 6 else "DEAD")
            kind = None if result.success else result.error_kind or "provider_unknown"
            db.execute("""UPDATE outbox SET state=?,next_attempt_at=?,lease_token=NULL,lease_until=NULL,
                external_id=?,error_kind=?,error_code=?,sent_at=?,result_unknown=max(result_unknown,?)
                WHERE delivery_id=?""", (state, retry_stamp if state == "RETRY_WAIT" else stamp,
                result.external_id, kind, result.error_code, stamp if result.success else None,
                int(result.result_unknown), delivery_id))
            self._event(db, row, stamp, state.lower(), unknown=result.result_unknown,
                        error_kind=kind, error_code=result.error_code, external_id=result.external_id)
            return True

    def release_claim(self, delivery_id: str, lease_token: str, now: datetime) -> bool:
        """Release a claim only when its caller has not begun the provider operation."""
        stamp = epoch(now)
        with self._transaction() as db:
            row = db.execute("SELECT * FROM outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            if row is None or row["state"] != "SENDING" or row["lease_token"] != lease_token:
                return False
            self._event(db, row, stamp, "released_before_send")
            db.execute("""UPDATE outbox SET state='PENDING',attempt_count=attempt_count-1,
                lease_token=NULL,lease_until=NULL,next_attempt_at=? WHERE delivery_id=?""", (stamp, delivery_id))
            return True

    def stats(self, now: datetime) -> StoreStats:
        stamp = epoch(now)
        with self._connection() as db:
            counts = {state: 0 for state in ("PENDING", "SENDING", "RETRY_WAIT", "SENT", "DEAD")}
            counts.update(dict(db.execute("SELECT state,count(*) FROM outbox GROUP BY state")))
            oldest = db.execute("SELECT min(created_at) FROM outbox WHERE state IN ('PENDING','SENDING','RETRY_WAIT')").fetchone()[0]
            commit = db.execute("""SELECT max(at) FROM (SELECT received_at AS at FROM inbox
                                  UNION ALL SELECT at FROM delivery_events)""").fetchone()[0]
            unknown = db.execute("SELECT count(*) FROM outbox WHERE result_unknown=1").fetchone()[0]
            return StoreStats(counts, max(0, stamp - oldest) if oldest is not None else 0,
                              utc(commit), unknown, counts["DEAD"], sum(v for k, v in counts.items() if k != "SENT"))

    def probe_write(self, now: datetime) -> None:
        with self._transaction() as db:
            db.execute("""INSERT INTO store_health VALUES (1,?) ON CONFLICT(id)
                          DO UPDATE SET probed_at=excluded.probed_at""", (epoch(now),))

    def prune_sent(self, before: datetime) -> int:
        cutoff = epoch(before)
        with self._transaction() as db:
            count = db.execute("""DELETE FROM outbox WHERE state='SENT' AND sent_at<?
                AND result_unknown=0 AND NOT EXISTS (SELECT 1 FROM delivery_events e
                WHERE e.delivery_id=outbox.delivery_id AND e.result_unknown=1)""", (cutoff,)).rowcount
            db.execute("""DELETE FROM inbox WHERE received_at<? AND NOT EXISTS
                (SELECT 1 FROM inbox_jobs j WHERE j.receipt_id=inbox.receipt_id)""", (cutoff,))
            return count

    def requeue(self, delivery_id: str, reason: str, now: datetime) -> None:
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 1000:
            raise ValueError("a non-empty maintenance reason of at most1000 characters is required")
        stamp = epoch(now)
        with self._transaction() as db:
            row = db.execute("SELECT * FROM outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            if row is None or row["state"] != "DEAD":
                raise ValueError("only a DEAD delivery can be manually requeued")
            self._event(db, row, stamp, "manual_requeue", reason=reason.strip())
            db.execute("""UPDATE outbox SET state='PENDING',attempt_count=0,budget_started_at=NULL,
                next_attempt_at=?,lease_token=NULL,lease_until=NULL WHERE delivery_id=?""", (stamp, delivery_id))

    def events(self, delivery_id: str) -> tuple[dict, ...]:
        with self._connection() as db:
            return tuple(dict(r) for r in db.execute(
                "SELECT * FROM delivery_events WHERE delivery_id=? ORDER BY event_id", (delivery_id,)))

    @staticmethod
    def _job(row) -> DeliveryJob:
        fields = dict(row)
        fields["payload"] = json.loads(fields["payload"])
        for name in ("next_attempt_at", "lease_until", "created_at", "sent_at", "budget_started_at"):
            fields[name] = utc(fields[name])
        fields["result_unknown"] = bool(fields["result_unknown"])
        return DeliveryJob(**fields)
