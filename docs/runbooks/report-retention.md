# Report retention and scheduling

Generated jobs consume `k8s_inspector.profiles.PROFILES`; regenerate using `python -m tools.render_cronjobs`. Run Kustomize contract tests after generation. The scheduled profiles are5m/10m/hourly/daily; full is manual. Runner deadlines240/480/900/1800s receive30s Job margin:270/510/930/1830. Each job uses Forbid, backoffLimit0 and explicit Asia/Shanghai. Jobs start suspended; no sample is a release authorization.

CronJob timeZone requires a target Kubernetes version supporting the field (stable in1.27). Server dry-run and controller behavior must be verified before unsuspending; do not silently remove the timezone and assume Shanghai schedules on an older cluster. See [Kubernetes CronJob time zones](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#time-zones).

An inspector business FAIL normally exits2. Examples use always0 for ephemeral Job execution; quality/fault metrics and run-status remain authoritative. Job backoff0 prevents retry storms for business failures or program errors. Forbid limits one CronJob, not different profiles or manual jobs. Each scheduled profile has its own block-state PVC; state is separate from the RWX report claim. Verify actual CSI permissions, same-profile locking, reattachment and restart before relying on this arrangement.

Every run atomically refreshes latest/<profile>/report.json even when extra persistence is issue/none. Hourly/daily archives have UTC buckets and unique run directories. Retain hourly7 days/daily30 days; latest, observation state, Bridge SQLite/outbox and unknown/dead evidence are not report-cleanup targets.

The cleanup job defaults to --dry-run and remains suspended. Review its candidate list, then remove --dry-run only for the site-approved retention job. Permission/deletion errors must remain visible in logs and a nonzero exit; do not use always0 to conceal them. Test cleanup in a disposable archive with valid YYYYMMDD/ YYYYMMDDHH buckets. Monitor PVC free space/permissions through the site's independent storage monitoring; this repository does not yet prove deployed volume capacity coverage.

To inspect candidates locally:

```sh
python -m k8s_inspector.cli cleanup --root ./out/archive --hourly-days 7 --daily-days 30 --dry-run
```

Report cleanup does not prune the delivery database. Use the documented local Bridge maintenance procedure with preserved backups/audit and required reasons. Restoring a report does not resolve an incident or manufacture valid healthy observations.
