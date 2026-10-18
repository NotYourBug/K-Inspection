"""Read-only rollback planning checks. Passing is not authorization to roll back."""
import argparse
import json


def validate_rollback(*,current_schema,target_schema,delivery_mode,unfinished_jobs):
    if any(type(v) is not int or v<0 for v in (current_schema,target_schema,unfinished_jobs)):
        raise ValueError('schema and unfinished counts must be nonnegative integers')
    if target_schema!=current_schema:
        raise ValueError('schema compatibility requires matching code or a verified backup restore')
    if delivery_mode not in ('sync','async'): raise ValueError('invalid delivery mode')
    if unfinished_jobs and delivery_mode!='async':
        raise ValueError('unfinished outbox must be preserved and drained with compatible async code')
    return {'preserve_outbox':True,'preserve_observation_state':True,'requires_backup':True,
            'requires_operator_approval':True,'external_validation_pending':True}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--current-schema',type=int,required=True)
    p.add_argument('--target-schema',type=int,required=True)
    p.add_argument('--delivery-mode',choices=['sync','async'],required=True)
    p.add_argument('--unfinished-jobs',type=int,required=True)
    args=p.parse_args(argv)
    try:
        result=validate_rollback(current_schema=args.current_schema,target_schema=args.target_schema,
                                delivery_mode=args.delivery_mode,unfinished_jobs=args.unfinished_jobs)
    except ValueError as exc:
        p.error(str(exc))
    print(json.dumps(result))
    return 0


if __name__=='__main__':raise SystemExit(main())
