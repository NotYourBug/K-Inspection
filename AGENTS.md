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
  checks.py           # Core inspection rules: check_nodes, check_pods, check_pending_pvcs,
                      #   check_warning_events, check_control_plane, check_kube_system_addons
                      #   run_all(profile) dispatches by frequency tier
  models.py           # Status enum (PASS/WARN/FAIL/SKIP), CheckResult, Report
  config.py           # InspectorConfig + Thresholds dataclass; loads JSON/YAML override
  kube.py             # KubeContext: thin wrapper around kubernetes SDK
                      #   load_kube, apiserver_healthz, get_node_metrics, parse_quantity
  report.py           # Output writers: write_json, write_csv, write_html, format_failed_summary
  metrics.py          # Converts Report → Prometheus Gauge metrics → Pushgateway (push)
  notify.py           # send_feishu_text: bot webhook using stdlib urllib (no requests dep)
  alertmanager_bridge.py  # Long-running HTTP server receiving Alertmanager webhooks
                      #   Two delivery modes: "webhook" (fixed-group text) and "app" (existing-chat cards)
                      #   Fixed chat policy; never maps notification groupKey to a new chat
  alert_routing.py    # Payload validation, alert identity fields, exact-label routes and fallback chat
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
- **Every check function catches its own exceptions** and returns a `CheckResult` with status `WARN` on error — single check failure never crashes the entire inspection.
- **Pushgateway pattern** for CronJob metrics: inspection runs are ephemeral, so metrics are pushed to Pushgateway for Prometheus scrape rather than exposing a `/metrics` endpoint.
- **Exit codes**: 0 = no FAIL, 2 = has FAIL, 1 = has WARN only when `--fail-on warn`. `INSPECTOR_EXIT_MODE=always0` overrides to always return 0 (for use with Prometheus alerts).
- **Two Feishu delivery modes, fixed chat policy**: `webhook` sends text to an existing group; `app` sends cards to existing chat IDs with optional exact-label responsibility routing and a required fallback. No automatic create/invite/rename operations. Manual incident escalation and persistent delivery state are subsequent remediation steps.
- **The MCP server** (`mcp_server.py`) is fully self-contained — it duplicates `parse_quantity`, `_BAD_POD_REASONS`, `_pod_issue`, and client init rather than importing from the shared modules. This is intentional so it can be deployed independently.
- **Report storage layering**: high-frequency profiles write to `latest/<profile>/report.json` (overwrite), low-frequency write to `archive/<profile>/<timestamp>/` for retention. A cleanup CronJob prunes hourly archives after 7 days and daily after 30 days.
