"""Container-local maintenance; authorization is OS/container access, never HTTP."""
import argparse
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .bridge_store import DeliveryStore, StorageUnavailable, UnsupportedSchema


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="inspector-bridge-admin")
    parser.add_argument("--db", required=True)
    sub = parser.add_subparsers(dest="command", required=True)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--delivery-id")
    inspect.add_argument("--include-payload", action="store_true")
    requeue = sub.add_parser("requeue")
    requeue.add_argument("--delivery-id", required=True)
    requeue.add_argument("--reason", required=True)
    backup = sub.add_parser("backup")
    backup.add_argument("--destination", required=True)
    prune = sub.add_parser("prune")
    prune.add_argument("--retention-days", type=int, default=30)
    args = parser.parse_args(argv)
    now = datetime.now(timezone.utc)
    try:
        if not Path(args.db).is_file():
            raise ValueError("maintenance requires an existing delivery database")
        store = DeliveryStore(Path(args.db))
        if args.command == "inspect":
            jobs = (store.get_job(args.delivery_id),) if args.delivery_id else store.list_jobs()
            data = []
            for job in jobs:
                item = asdict(job)
                if not args.include_payload:
                    item.pop("payload")
                if args.delivery_id:
                    item["events"] = store.events(job.delivery_id)
                data.append(item)
            result = {"stats": asdict(store.stats(now)), "jobs": data}
        elif args.command == "requeue":
            store.requeue(args.delivery_id, args.reason, now)
            result = {"requeued": args.delivery_id}
        elif args.command == "backup":
            result = {"backup": str(store.backup(Path(args.destination)))}
        else:
            if args.retention_days < 30:
                raise ValueError("retention must be at least30 days")
            result = {"pruned_sent": store.prune_sent(now - timedelta(days=args.retention_days))}
        print(json.dumps(result, ensure_ascii=False, default=lambda value: value.isoformat()))
        return 0
    except (ValueError, KeyError, OSError, StorageUnavailable, UnsupportedSchema):
        print(json.dumps({"error": "maintenance failed; check database, arguments and local access"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
