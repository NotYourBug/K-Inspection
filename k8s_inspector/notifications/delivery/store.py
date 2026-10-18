"""Single-instance SQLite inbox/outbox. Use a verified local/block volume only.

Every operation owns its connection. Acceptance takes a write reservation before
deduplication and commits the receipt and ALL destinations together.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from k8s_inspector.notifications.bridge.routing import Alert, AlertBatch
from k8s_inspector.notifications.delivery.identity import alert_cycle_ids
from k8s_inspector.notifications.delivery.models import AcceptResult, DeliveryJob, DeliveryResult, DeliveryTarget, StoreStats

SCHEMA_VERSION = 3

# Historical flags/states remain immutable evidence. Disposition is a later
# explicit audit event, invalidated by a subsequent failed/unknown operation.
_UNRESOLVED_UNKNOWN = """o.result_unknown=1 AND NOT EXISTS (
    SELECT 1 FROM delivery_events ack WHERE ack.delivery_id=o.delivery_id
    AND ack.event_type='unknown_acknowledged' AND NOT EXISTS (
        SELECT 1 FROM delivery_events u WHERE u.delivery_id=o.delivery_id
        AND u.result_unknown=1 AND u.event_id>ack.event_id))"""
_UNHANDLED_DEAD = """o.state='DEAD' AND NOT EXISTS (
    SELECT 1 FROM delivery_events h WHERE h.delivery_id=o.delivery_id AND h.event_type='dead_handled'
    AND h.event_id>coalesce((SELECT max(e.event_id) FROM delivery_events e
        WHERE e.delivery_id=o.delivery_id AND e.event_type IN ('dead','claimed','manual_requeue')),0))"""


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


_REQUIRED_COLUMNS = {
    "inbox": {"receipt_id", "source_id", "received_at", "normalized_payload", "truncated"},
    "outbox": {"delivery_id", "source_id", "destination_id", "content_hash", "generation", "payload",
        "state", "attempt_count", "next_attempt_at", "lease_token", "lease_until", "external_id",
        "error_kind", "created_at", "sent_at", "result_unknown", "error_code", "budget_started_at"},
    "inbox_jobs": {"receipt_id", "delivery_id"},
    "delivery_events": {"event_id", "delivery_id", "attempt_no", "lease_token", "at", "event_type",
                         "result_unknown", "error_kind", "error_code", "external_id", "reason"},
    "store_health": {"id", "probed_at"},
}


def _read_version(db) -> int | None:
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not tables:
        return None
    if "schema_version" not in tables:
        raise UnsupportedSchema("not a delivery database")
    versions = db.execute("SELECT version FROM schema_version").fetchall()
    if len(versions) != 1 or type(versions[0][0]) is not int or not 1 <= versions[0][0] <= SCHEMA_VERSION:
        raise UnsupportedSchema("unsupported delivery schema")
    version = versions[0][0]
    required = dict(_REQUIRED_COLUMNS)
    if version >= 2:
        required["migration_history"] = {"version", "applied_at", "backup_path"}
    if version >= 3:
        required["delivery_order"] = {"seq", "delivery_id"}
        required["delivery_members"] = {"delivery_id", "cycle_key"}
    for table, columns in required.items():
        present = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
        if not columns <= present:
            raise UnsupportedSchema("incomplete delivery schema")
    if version >= 3 and db.execute("""SELECT 1 FROM outbox o WHERE
        NOT EXISTS (SELECT 1 FROM delivery_order q WHERE q.delivery_id=o.delivery_id) OR
        NOT EXISTS (SELECT 1 FROM delivery_members m WHERE m.delivery_id=o.delivery_id) LIMIT 1""").fetchone():
        raise UnsupportedSchema("incomplete delivery ordering metadata")
    return version


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_posix(temporary: Path, destination: Path) -> None:
    os.link(temporary, destination)  # Atomic, no replacement of existing names.
    _sync_directory(destination.parent)


def _publish_backup(temporary: Path, destination: Path) -> None:
    if os.name != "nt":
        _publish_posix(temporary, destination)
        return
    # Same-directory move, WRITE_THROUGH, deliberately without REPLACE_EXISTING.
    import ctypes
    from ctypes import wintypes
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    move = kernel.MoveFileExW
    move.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
    move.restype = wintypes.BOOL
    if not move(str(temporary.resolve()), str(destination.resolve()), 0x8):
        raise ctypes.WinError(ctypes.get_last_error())


def _backup_sqlite(path: Path, destination: Path) -> Path:
    destination = Path(destination)
    temporary = destination.with_name(destination.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        if destination.resolve() == path.resolve() or os.path.lexists(destination):
            raise ValueError("backup destination must be a new file")
        if not destination.parent.is_dir():
            raise ValueError("backup destination directory must already exist and be verified")
        deadline = time.monotonic() + 10
        def progress(_status, _remaining, _total):
            if time.monotonic() > deadline:
                raise StorageUnavailable("backup deadline exceeded")
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as source:
            with closing(sqlite3.connect(temporary, isolation_level=None)) as output:
                source.backup(output, pages=128, progress=progress, sleep=.05)
                if output.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise StorageUnavailable("backup integrity check failed")
        with temporary.open("r+b") as stream:
            os.fsync(stream.fileno())
        _publish_backup(temporary, destination)
        return destination
    except (OSError, sqlite3.Error) as exc:
        raise StorageUnavailable("delivery backup unavailable") from exc
    finally:
        temporary.unlink(missing_ok=True)


def _upgrade_v2(db, backup: Path):
    db.execute("CREATE TABLE migration_history (version INTEGER PRIMARY KEY, applied_at REAL NOT NULL, backup_path TEXT)")
    db.execute("INSERT INTO migration_history VALUES (2,?,?)", (epoch(datetime.now(timezone.utc)), str(backup)))


def _create_ordering_tables(db):
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_order (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, delivery_id TEXT NOT NULL UNIQUE
        REFERENCES outbox(delivery_id) ON DELETE CASCADE)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_members (
        delivery_id TEXT NOT NULL REFERENCES outbox(delivery_id) ON DELETE CASCADE,
        cycle_key TEXT NOT NULL, PRIMARY KEY(delivery_id,cycle_key))""")
    db.execute("CREATE INDEX IF NOT EXISTS delivery_cycle ON delivery_members(cycle_key,delivery_id)")


def _upgrade_v3(db, backup: Path):
    _create_ordering_tables(db)
    # Prior versions had no domain sequence. Their insertion row order supplies
    # the one-time migration order; afterwards an explicit AUTOINCREMENT owns it.
    for delivery_id, source_id in db.execute("SELECT delivery_id,source_id FROM outbox ORDER BY rowid").fetchall():
        db.execute("INSERT INTO delivery_order(delivery_id) VALUES (?)", (delivery_id,))
        receipt = db.execute("""SELECT i.normalized_payload FROM inbox i JOIN inbox_jobs j
            ON j.receipt_id=i.receipt_id WHERE j.delivery_id=? ORDER BY i.received_at LIMIT 1""",
            (delivery_id,)).fetchone()
        if receipt is None:
            raise UnsupportedSchema("missing receipt for ordering migration")
        data = json.loads(receipt[0])
        original = AlertBatch(alerts=tuple(Alert(**item) for item in data["alerts"]))
        # Historical full receipts can over-associate routed subsets; this is
        # conservative (extra refresh), never suppresses a changed notification.
        for key in alert_cycle_ids(source_id, original):
            db.execute("INSERT INTO delivery_members VALUES (?,?)", (delivery_id, key))
    db.execute("INSERT INTO migration_history VALUES (3,?,?)", (epoch(datetime.now(timezone.utc)), str(backup)))


MIGRATIONS = {2: _upgrade_v2, 3: _upgrade_v3}


def migrate(path: Path, target_version: int) -> None:
    """Offline schema upgrade. Stop all service writers before invoking it."""
    path = Path(path)
    if not path.is_file() or target_version != SCHEMA_VERSION:
        raise UnsupportedSchema("unsupported migration target or missing database")
    db = None
    try:
        db = sqlite3.connect(path, isolation_level=None, timeout=2)
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        version = _read_version(db)
        if version is None:
            raise UnsupportedSchema("empty delivery database")
        if version == target_version:
            return
        backup = _backup_sqlite(path, path.with_name(f"{path.name}.pre-v{target_version}-{uuid.uuid4().hex}.bak"))
        db.execute("BEGIN IMMEDIATE")
        for next_version in range(version + 1, target_version + 1):
            MIGRATIONS[next_version](db, backup)
            db.execute("UPDATE schema_version SET version=?", (next_version,))
        _read_version(db)
        db.commit()
    except sqlite3.Error as exc:
        raise StorageUnavailable("delivery migration failed; pre-upgrade backup retained") from exc
    finally:
        if db is not None:
            if db.in_transaction:
                db.rollback()
            db.close()


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
                version = _read_version(db)
            if version is not None and version < SCHEMA_VERSION:
                migrate(self.path, SCHEMA_VERSION)
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
                db.execute("""CREATE TABLE IF NOT EXISTS migration_history (
                    version INTEGER PRIMARY KEY, applied_at REAL NOT NULL, backup_path TEXT)""")
                if not versions:
                    db.execute("INSERT INTO migration_history VALUES (?,?,NULL)",
                               (SCHEMA_VERSION, epoch(datetime.now(timezone.utc))))
                _create_ordering_tables(db)
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
            backlog = db.execute(f"""SELECT count(*) FROM outbox o WHERE
                o.state IN ('PENDING','SENDING','RETRY_WAIT') OR ({_UNHANDLED_DEAD})""").fetchone()[0]
            for target in targets:
                cycles = tuple(sorted(set(target.cycle_keys or alert_cycle_ids(source_id, batch))))
                row = db.execute("""SELECT * FROM outbox WHERE source_id=? AND destination_id=?
                    AND content_hash=? ORDER BY generation DESC LIMIT 1""",
                    (source_id, target.destination_id, target.content_hash)).fetchone()
                intervened = False
                if row is not None:
                    placeholders = ",".join("?" for _ in cycles)
                    intervened = db.execute(f"""SELECT 1 FROM outbox later
                        JOIN delivery_order q ON q.delivery_id=later.delivery_id
                        JOIN delivery_members m ON m.delivery_id=later.delivery_id
                        WHERE later.source_id=? AND later.destination_id=? AND m.cycle_key IN ({placeholders})
                        AND q.seq>(SELECT seq FROM delivery_order WHERE delivery_id=?) LIMIT 1""",
                        (source_id, target.destination_id, *cycles, row["delivery_id"])).fetchone() is not None
                if row is not None and not intervened and (row["state"] != "SENT" or
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
                    db.execute("INSERT INTO delivery_order(delivery_id) VALUES (?)", (delivery_id,))
                    db.executemany("INSERT INTO delivery_members VALUES (?,?)", ((delivery_id, key) for key in cycles))
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
            row = db.execute("""SELECT o.* FROM outbox o JOIN delivery_order q ON q.delivery_id=o.delivery_id
                WHERE ((o.state IN ('PENDING','RETRY_WAIT') AND o.next_attempt_at<=?) OR
                (o.state='SENDING' AND o.lease_until<=?)) AND NOT EXISTS (
                    SELECT 1 FROM outbox prev JOIN delivery_order pq ON pq.delivery_id=prev.delivery_id
                    WHERE prev.source_id=o.source_id AND prev.destination_id=o.destination_id AND pq.seq<q.seq
                    AND prev.state IN ('PENDING','SENDING','RETRY_WAIT'))
                ORDER BY o.next_attempt_at,q.seq LIMIT 1""",
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
            unknown = db.execute(f"SELECT count(*) FROM outbox o WHERE {_UNRESOLVED_UNKNOWN}").fetchone()[0]
            dead = db.execute(f"SELECT count(*) FROM outbox o WHERE {_UNHANDLED_DEAD}").fetchone()[0]
            return StoreStats(counts, max(0, stamp - oldest) if oldest is not None else 0,
                              utc(commit), unknown, dead, dead + sum(counts[k] for k in ("PENDING", "SENDING", "RETRY_WAIT")))

    def probe_write(self, now: datetime) -> None:
        with self._transaction() as db:
            db.execute("""INSERT INTO store_health VALUES (1,?) ON CONFLICT(id)
                          DO UPDATE SET probed_at=excluded.probed_at""", (epoch(now),))

    def prune_sent(self, before: datetime) -> int:
        cutoff = epoch(before)
        with self._transaction() as db:
            count = db.execute(f"""DELETE FROM outbox WHERE delivery_id IN (
                SELECT o.delivery_id FROM outbox o WHERE o.state='SENT' AND o.sent_at<?
                AND NOT ({_UNRESOLVED_UNKNOWN}) AND NOT EXISTS (SELECT 1 FROM delivery_events e
                WHERE e.delivery_id=o.delivery_id AND e.at>=?))""", (cutoff, cutoff)).rowcount
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

    def _disposition(self, delivery_id: str, reason: str, now: datetime, *, event_type: str) -> None:
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 1000:
            raise ValueError("a non-empty maintenance reason of at most1000 characters is required")
        with self._transaction() as db:
            row = db.execute("SELECT * FROM outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
            allowed = {"DEAD"} if event_type == "dead_handled" else {"DEAD", "SENT"}
            if row is None or row["state"] not in allowed:
                raise ValueError("disposition requires a terminal delivery")
            if event_type == "unknown_acknowledged" and not row["result_unknown"]:
                raise ValueError("delivery has no unknown result to acknowledge")
            self._event(db, row, epoch(now), event_type, reason=reason.strip())

    def resolve_dead(self, delivery_id: str, reason: str, now: datetime) -> None:
        self._disposition(delivery_id, reason, now, event_type="dead_handled")

    def acknowledge_unknown(self, delivery_id: str, reason: str, now: datetime) -> None:
        self._disposition(delivery_id, reason, now, event_type="unknown_acknowledged")

    def events(self, delivery_id: str) -> tuple[dict, ...]:
        with self._connection() as db:
            return tuple(dict(r) for r in db.execute(
                "SELECT * FROM delivery_events WHERE delivery_id=? ORDER BY event_id", (delivery_id,)))

    def backup(self, destination: Path) -> Path:
        return _backup_sqlite(self.path, destination)

    @staticmethod
    def _job(row) -> DeliveryJob:
        fields = dict(row)
        fields["payload"] = json.loads(fields["payload"])
        for name in ("next_attempt_at", "lease_until", "created_at", "sent_at", "budget_started_at"):
            fields[name] = utc(fields[name])
        fields["result_unknown"] = bool(fields["result_unknown"])
        return DeliveryJob(**fields)
