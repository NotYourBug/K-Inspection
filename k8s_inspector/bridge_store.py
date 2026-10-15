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
from .delivery_models import AcceptResult, DeliveryJob, DeliveryTarget

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
                    error_code TEXT, UNIQUE(source_id,destination_id,content_hash,generation))""")
                db.execute("""CREATE TABLE IF NOT EXISTS inbox_jobs (
                    receipt_id TEXT NOT NULL REFERENCES inbox(receipt_id) ON DELETE CASCADE,
                    delivery_id TEXT NOT NULL REFERENCES outbox(delivery_id) ON DELETE CASCADE,
                    PRIMARY KEY(receipt_id,delivery_id))""")
                db.execute("CREATE INDEX IF NOT EXISTS outbox_due ON outbox(state,next_attempt_at)")
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

    @staticmethod
    def _job(row) -> DeliveryJob:
        fields = dict(row)
        fields["payload"] = json.loads(fields["payload"])
        for name in ("next_attempt_at", "lease_until", "created_at", "sent_at"):
            fields[name] = utc(fields[name])
        fields["result_unknown"] = bool(fields["result_unknown"])
        return DeliveryJob(**fields)
