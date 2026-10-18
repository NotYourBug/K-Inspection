# Operations and ownership

Status: offline preparation. Site owners, actual credentials, deployed volume semantics, container/runtime matrix and shadow acceptance remain unverified. A passing source suite is not permission to deploy.

## Assign before enabling

Record the accountable owner and escalation backup for each cluster/profile, Alertmanager route/team, Bridge state volume/backup, report volume/retention, Prometheus storage and independent quality receiver. Set an explicit stable cluster_id. Keep actual app credentials, bearer source tokens and chat destination configuration in site-managed Secrets; never commit resolved Secret files or actual kubeconfig.

The default Kustomization contains suspended inspector jobs and storage declarations. `overlays/example` adds opt-in Bridge/async/example monitoring. `overlays/shadow` uses a dedicated namespace, report path, Pushgateway job and Secret reference. Placeholders, suspended jobs and syntactically valid claims do not prove a runnable site installation. Keep cluster-scoped RBAC names distinct across installations; the control-component observer still reads the configured control-plane namespace.

## Routine checks

- Check independent expected inventory, never-ran/stale/invalid observations, required sink failures and Pushgateway scrape health. Unknown or absent evidence is not recovery.
- Inspect latest report.json, run-manifest and run-status together. INFO is evidence, not a business fault; filter WARN/FAIL. Historical OOM/restarts and current Pod failures are separate.
- For Bridge backlog/dead/unknown, use local bridge_admin inspect. Health proves process liveness; readyz also depends on writable state and active delivery worker. Do not send the only failure notification through the failing Bridge.
- Backup before schema changes using the SQLite backup API and a pre-created durable directory. Retain delivery IDs/audit and pending/DEAD/unacknowledged unknown evidence. Verify restored schema with matching code.
- Rotate tokens by updating both the Alertmanager authorization mount and Bridge sources Secret through the site's controlled procedure. Bind HA replicas to one logical source. Verify the new path before removing the old credential; never print it in command logs.

Read-only/help command inventory:

```sh
python -m k8s_inspector.cli --help
python -m k8s_inspector.cli run --help
python -m k8s_inspector.cli cleanup --help
python -m k8s_inspector.alertmanager_bridge --help
python -m k8s_inspector.bridge_admin --help
python -m k8s_inspector.expected_targets --help
```

Local administrative actions require container/OS authorization. resolve-dead and acknowledge-unknown require reasons and preserve history; they do not change an unconfirmed send into SENT. Use documented requeue only after correcting the cause and accepting duplicate-send risk. Report cleanup does not manage Bridge jobs or incident state.

## What is implemented and what is not

Implemented locally: bounded existing inspection rules, v1/v2 evidence and output isolation, persistent aggregate evidence, expected inventory/shadow rules, fixed existing-chat routing, authenticated optional async inbox/outbox, retries/leases/audit/backup/schema3 maintenance, package/deployment validation helpers.

Optional explicit components: app/webhook provider, async Bridge, sample monitoring and shadow namespace. These are not installed by default. Image smoke and runtime support require actual CI/target evidence; skip/pending is not success.

Not implemented by this release preparation: D manual incident authorization/disposition/collaboration, F expanded coverage and repaired standalone MCP packaging, automatic remediation, automatic group creation, HA SQLite or guaranteed node-loss recovery. Existing MCP source is not a verified release extra and has no default deployment component. Ordinary alerts continue to existing fixed groups.

## Record real operation before setting SLOs

Capture version/image digest/config revision, cluster/profile coverage and last-valid age, current/historical findings, verified UID recovery, API requests/duration, sink outcomes, delivery success/retry/unknown/backlog/dead, independent quality alert delivery and backup/restore results. Choose SLOs only from these observations. No uptime, losslessness or latency SLO is asserted by offline tests.

For retention see report-retention.md; for queue recovery see bridge-delivery.md; for monitoring activation see inspection-monitoring.md; for controlled rollout and rollback see rollout.md. Keep the previous code/config and compatible state backup. Stop expansion on missed faults, false recovery, non-restorable state or sustained queue growth.
