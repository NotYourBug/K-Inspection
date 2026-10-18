# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project overview

A Kubernetes cluster automated inspection tool for SRE. Covers infrastructure, control plane, and data plane health checks with multi-format reporting and Feishu alerting.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests (inspection, models, config, metrics, notifications, routing and Bridge HTTP)
python -m pytest tests/ -v

# Run inspection locally
python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --formats json,html --output-dir ./out --profile full

# Run inside cluster (Pod/CronJob)
INSPECTOR_CLUSTER=prod python -m k8s_inspector.cli run --in-cluster --formats json --output-dir /data/out --profile 5m

# With Feishu webhook push (only FAIL/WARN items)
python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --feishu-webhook "<webhook_url>"

# With Pushgateway metrics push
python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --profile full --pushgateway-url http://127.0.0.1:9091

# With custom config file (JSON/YAML)
python -m k8s_inspector.cli run --config ./config.yaml

# Cleanup archived reports
python -m k8s_inspector.cli cleanup --root ./out/archive --hourly-days 7 --daily-days 30

# Start Alertmanager → Feishu bridge (long-running HTTP service)
python -m k8s_inspector.alertmanager_bridge --mode app --feishu-chat-id oc_your_existing_chat --listen-port 8000

# Durable async delivery (requires Secret sources and verified local/block state volume)
python -m k8s_inspector.alertmanager_bridge --mode app --feishu-chat-id oc_your_existing_chat --delivery-mode async --state-db /var/lib/inspector/bridge.db --sources-file /etc/bridge-auth/sources.json

# Local delivery maintenance (OS/container authorization; no HTTP admin API)
python -m k8s_inspector.bridge_admin --db /var/lib/inspector/bridge.db inspect

# Start MCP server for LLM troubleshooting
python -m k8s_inspector.mcp_server

# Docker build
docker build -t your-registry/k8s-inspector:0.1.0 .

# Deploy to cluster (Kustomize)
kubectl apply -k deploy
```

## Architecture

```text
k8s_inspector/
  __init__.py
  cli.py                      # Inspection/cleanup command and orchestration
  alertmanager_bridge.py      # Stable command wrapper → notifications.bridge.server
  bridge_admin.py             # Stable command wrapper → notifications.bridge.admin
  expected_targets.py         # Stable command wrapper → monitoring.inventory
  mcp_server.py               # MCP troubleshooting command; shares core.resources
  core/
    config.py, models.py, observations.py
    profiles.py, resources.py # Shared profile definitions and resource helpers
    files.py                  # Atomic durable publication for state and reports
  inspection/
    runner.py, catalog.py     # Finite rules → bounded snapshot → Report v2
    collector.py, snapshot.py, kube.py
    state.py                  # Profile locks, last-valid and confirmed positives
    checks.py, compatibility.py # Legacy result adapters
    rules/                    # Pure evaluations; no SDK/file/network I/O
      evaluation.py, nodes.py, pods.py, storage.py, events.py, control.py
  notifications/
    bridge/                   # HTTP server, authentication, routes and local admin
      server.py, auth.py, routing.py, admin.py, observability.py
      legacy_state.py         # Historical group/chat JSON state; not used by Bridge
    delivery/                 # Durable inbox/outbox, identities, attempts and retry
      store.py, models.py, identity.py, adapter.py, worker.py
    providers/                # Feishu app/webhook clients and sanitized errors
      feishu.py, webhook.py, errors.py
  reporting/
    writers.py, sinks.py       # JSON/CSV/HTML and isolated output attempts
  monitoring/
    metrics.py, inventory.py   # Pushgateway and independent expected inventory

deploy/                       # Kustomize, monitoring, storage and example overlays
```

Keep package `__init__.py` files free of service initialization. `core` has no
adapter/SDK dependencies; pure `inspection.rules` must not depend on outputs or
notification providers. Use canonical package imports for internal modules; only
the documented commands above retain the root paths. See
[package layout and migration map](docs/reference/package-layout.md).

## Key design decisions

- **Profile-based execution**: `5m` → data plane only (Node Ready + Pods + PVC, no metrics), `10m` → control plane, `hourly` → infra essentials, `daily`/`full` → all checks including events and kube-system addons. Controls API pressure at different frequencies.
- **Every check isolates collection errors**: pure collection failure returns WARN; already confirmed FAIL survives later optional collection errors. Details include collection_errors and unknown observations.
- **Pushgateway pattern** for CronJob metrics: inspection runs are ephemeral, so metrics are pushed to Pushgateway for Prometheus scrape rather than exposing a `/metrics` endpoint.
- **Exit codes**: 0 = no FAIL, 2 = has FAIL, 1 = WARN when --fail-on warn, or configuration failure. Explicit --output-policy strict also returns 1 on configured output failure, before business 2; default best-effort preserves business exit behavior. always0 overrides valid run exit codes only, not startup validation or observable sink/run failure.
- **Configuration validation**: finite ratios in [0,1], nonnegative integer counters, strict field/type validation. Grace defaults: Pod unready 120s, Pod Pending 300s, PVC Pending 300s. Ready uses condition transition time; historical OOM/restarts are separate evidence.
- **Two Feishu providers, fixed chat policy**: `webhook` sends text to an existing group; `app` sends cards to existing chat IDs with optional exact-label responsibility routing and a required fallback. No automatic create/invite/rename operations. Manual incident escalation remains D work.
- **Bridge delivery contract**: default sync200 confirms provider success; opt-in async202 confirms atomic persistent acceptance only. Async requires source authentication and SQLite on a verified local/block PVC, replicas1/Recreate; no NFS/HA SQLite. Stable delivery ID/UUID, lease60s, provider HTTP timeout10s, max6 attempts, retry5/15/45/120/300s + jitter,900s retry deadline from first claim. Unknown results remain auditable; no exactly-once claim. See docs/runbooks/bridge-delivery.md for cutover/recovery/release gates.
- **Bridge order and disposition**: schema3 persists source/destination FIFO and per-alert cycle membership; A→B→A is a new transition even while old A is pending, unrelated HA interleaving still deduplicates. FIFO can delay later same-channel notifications. Local resolve-dead/acknowledge-unknown append reasoned audit and distinguish historical facts from unresolved counts. Backup requires a prepared directory and durable non-overwriting publication.
- **The MCP server** remains at `mcp_server.py` with its existing client initialization and rate limiter, and imports shared resource helpers from `core.resources`. It is not a standalone single-file distribution. Optional MCP dependency/package work remains F scope; this package reorganization does not change that runtime contract.
- **Report storage layering**: every run atomically refreshes latest/<profile>/report.json, even with persist-level issue/none; extra output/archive respects persistence policy. Canonical deployed latest/archive paths remain supported. Archives use UTC buckets plus unique run directories. cleanup supports --dry-run, rejects symlink/reparse targets and reports deletion failures; hourly7/daily30 days retained.
- **Output evidence**: run-status.json records attempted sinks and execution quality independently of the legacy Report root. Prometheus run/sink metrics describe completed attempts before metrics push; that push result is in status/logs. Persistent last-valid and confirmed-positive state now preserve faults through unknown collection; independent expected inventory and primary-profile rules are generated under deploy/monitoring/generated in shadow mode. Real volume/runtime/receiver/cutover gates remain E work; see docs/runbooks/inspection-monitoring.md.

- C review closure: per-rule Node/control/Pod evaluators isolate siblings; SDK retries disabled in the shared loader; page/rule deadlines rechecked after response/evaluation. API health supports old/new SDK call interfaces. In-cluster requires explicit cluster identity; kubeconfig fallback binds selected source/context/server via hash.
- INFO evidence carries healthy utilization and ordinary Event cursors; only WARN/FAIL count as faults. Event deltas require a trustworthy recent baseline/window. Addon shortage requires complete selector membership. UID observations describe seen resources only.
- `inspection/compatibility.py` projects v2 evidence to legacy check groups and existing metric selectors, omitting unknown counters; CSV keeps independent rule_id/UID rows. Older positive writes/exports cannot override newer complete healthy evidence.


## Offline release preparation (E)

- pyproject.toml is dependency declaration source; requirements.txt installs constrained .[dev]. Direct runtime/tool constraints are not a full transitive lock. Actual Python3.9/3.11/3.14 CI and container results remain required.
- tools/verify_release.py builds an installed wheel and records source/tool/config checks. --offline shares preinstalled dependency paths only for source-isolated import smoke, never pulls Docker images and leaves clean install/runtime/storage/receiver acceptance pending; result success is false until every mandatory check passes.
- CI uses explicit --local-checks-only: exit status requires every local check, including composed monitoring configs and both rule suites; JSON local_checks_success and exit_gate remain distinct from full release success. Missing local checks fail. Dev extra explicitly includes non-isolated build backends.
- deploy/ is suspended inspector base only. Explicit overlays/example adds optional Bridge async/monitoring; overlays/shadow isolates namespace, outputs, job and Secret references. The control-plane Role stays in kube-system; namespace changes require matching RBAC.
- tools/render_cronjobs.py consumes PROFILES, adds30s deadline margin and explicit Asia/Shanghai (target API support must be verified). Forbid/backoffLimit0; full remains manual. Each scheduled profile uses a separate block-state claim; report storage is separate. Cleanup is dry-run by default.
- Dockerfile uses a verified official Python3.11 manifest digest and installs a wheel as nonroot. Building/smoking it, selecting an actual published image digest and validating volumes/CNI/separate real notification destinations remain external gates. No auto-publish or deployment.
- tools/rollout.py is a read-only planning guard; it does not inspect actual DB counts or authorize rollback. Preserve outbox/observation state and compatible schema. See rollout.md, operations.md and report-retention.md.
