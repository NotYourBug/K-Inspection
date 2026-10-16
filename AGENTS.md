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

```
k8s_inspector/
  cli.py              # Entry point: argparse, "run" and "cleanup" subcommands
  runner.py           # Finite catalog → bounded snapshot → isolated pure rules → Report v2
  rule_catalog.py     # Rule/profile/collection keys and one primary notification profile
  observation_models.py # Execution/health/coverage/UID evidence; legacy conversions
  collector.py, snapshot.py # Deadline-bounded pagination, error classification and per-run reuse
  rules_*.py          # Pure domain evaluations; no SDK/file/network I/O
  observation_state.py # OS profile locks, atomic last-valid and confirmed-positive state
  expected_targets.py # Independent read-only expected inventory HTTP exporter
  checks.py           # Legacy check_* compatibility adapters; run_all uses canonical runner. Historical rules: check_nodes, check_pods, check_pending_pvcs,
                      #   check_warning_events, check_control_plane, check_kube_system_addons
                      #   run_all(profile) dispatches by frequency tier
  models.py           # Status enum (PASS/WARN/FAIL/SKIP), CheckResult, Report
  config.py           # InspectorConfig + Thresholds dataclass; loads JSON/YAML override
  kube.py             # KubeContext: thin wrapper around kubernetes SDK
                      #   load_kube, apiserver_healthz, get_node_metrics, parse_quantity
  report.py           # Output writers: write_json, write_csv, write_html, format_failed_summary
  metrics.py          # Converts Report → Prometheus Gauge metrics → Pushgateway (push)
  sinks.py            # Isolated output attempts, redacted SinkResult, run-status.json
  notify.py           # send_feishu_text: bot webhook using stdlib urllib (no requests dep)
  alertmanager_bridge.py  # Long-running HTTP server receiving Alertmanager webhooks
                      #   Two delivery modes: "webhook" (fixed-group text) and "app" (existing-chat cards)
                      #   Fixed chat policy; never maps notification groupKey to a new chat
  alert_routing.py    # Payload validation, alert identity fields, exact-label routes and fallback chat
  bridge_auth.py      # Bearer credentials bind logical source IDs; Secret file/env
  bridge_store.py     # SQLite atomic inbox/outbox, leases, audit, retention, backup/migration
  delivery_models.py # Immutable delivery DTOs and StoreStats
  delivery_identity.py # Canonical alert-cycle/content identity and quality flags
  delivery_adapter.py # Confirmed app/webhook sends, sanitized provider outcomes
  delivery_worker.py # Single worker, bounded retry budget and lease recovery
  bridge_observability.py # DB/worker readiness and aggregate metrics
  bridge_admin.py    # Container-local inspect/requeue/backup/prune commands
                      #   resolve-dead/acknowledge-unknown require reasons, preserve history
  feishu_app.py       # Feishu app bot client (lark-oapi SDK): create_chat, rename_chat, send_card
  state_store.py      # Legacy JsonStateStore: historical group_key → chat_id mapping,
                      #   no longer used by the fixed-chat Bridge
  mcp_server.py       # Self-contained FastMCP server exposing K8s troubleshooting tools to LLMs
                      #   Tools: list_nodes, get_node_detail, get_node_resource_usage,
                      #     find_problem_nodes, list_node_pods, diagnose_pod_issues
                      #   Has its own K8s client init, parse_quantity, rate limiter
deploy/               # Kustomize manifests: RBAC, CronJobs (by frequency), Prometheus rules,
                      #   Alertmanager config, Pushgateway, Grafana dashboard JSON, storage PV/PVC
```

## Key design decisions

- **Profile-based execution**: `5m` → data plane only (Node Ready + Pods + PVC, no metrics), `10m` → control plane, `hourly` → infra essentials, `daily`/`full` → all checks including events and kube-system addons. Controls API pressure at different frequencies.
- **Every check isolates collection errors**: pure collection failure returns WARN; already confirmed FAIL survives later optional collection errors. Details include collection_errors and unknown observations.
- **Pushgateway pattern** for CronJob metrics: inspection runs are ephemeral, so metrics are pushed to Pushgateway for Prometheus scrape rather than exposing a `/metrics` endpoint.
- **Exit codes**: 0 = no FAIL, 2 = has FAIL, 1 = WARN when --fail-on warn, or configuration failure. Explicit --output-policy strict also returns 1 on configured output failure, before business 2; default best-effort preserves business exit behavior. always0 overrides valid run exit codes only, not startup validation or observable sink/run failure.
- **Configuration validation**: finite ratios in [0,1], nonnegative integer counters, strict field/type validation. Grace defaults: Pod unready 120s, Pod Pending 300s, PVC Pending 300s. Ready uses condition transition time; historical OOM/restarts are separate evidence.
- **Two Feishu providers, fixed chat policy**: `webhook` sends text to an existing group; `app` sends cards to existing chat IDs with optional exact-label responsibility routing and a required fallback. No automatic create/invite/rename operations. Manual incident escalation remains D work.
- **Bridge delivery contract**: default sync200 confirms provider success; opt-in async202 confirms atomic persistent acceptance only. Async requires source authentication and SQLite on a verified local/block PVC, replicas1/Recreate; no NFS/HA SQLite. Stable delivery ID/UUID, lease60s, provider HTTP timeout10s, max6 attempts, retry5/15/45/120/300s + jitter,900s retry deadline from first claim. Unknown results remain auditable; no exactly-once claim. See docs/runbooks/bridge-delivery.md for cutover/recovery/release gates.
- **Bridge order and disposition**: schema3 persists source/destination FIFO and per-alert cycle membership; A→B→A is a new transition even while old A is pending, unrelated HA interleaving still deduplicates. FIFO can delay later same-channel notifications. Local resolve-dead/acknowledge-unknown append reasoned audit and distinguish historical facts from unresolved counts. Backup requires a prepared directory and durable non-overwriting publication.
- **The MCP server** (`mcp_server.py`) is fully self-contained — it duplicates `parse_quantity`, `_BAD_POD_REASONS`, `_pod_issue`, and client init rather than importing from the shared modules. This is intentional so it can be deployed independently.
- **Report storage layering**: every run atomically refreshes latest/<profile>/report.json, even with persist-level issue/none; extra output/archive respects persistence policy. Canonical deployed latest/archive paths remain supported. Archives use UTC buckets plus unique run directories. cleanup supports --dry-run, rejects symlink/reparse targets and reports deletion failures; hourly7/daily30 days retained.
- **Output evidence**: run-status.json records attempted sinks and execution quality independently of the legacy Report root. Prometheus run/sink metrics describe completed attempts before metrics push; that push result is in status/logs. Persistent last-valid and confirmed-positive state now preserve faults through unknown collection; independent expected inventory and primary-profile rules are generated under deploy/monitoring/generated in shadow mode. Real volume/runtime/receiver/cutover gates remain E work; see docs/runbooks/inspection-monitoring.md.
