# Inspection v2 monitoring cutover and recovery

Status: offline implementation and fixtures; no cluster deployment or external notification performed. Native C batch follows `docs/superpowers/plans/2026-10-02-inspection-foundations-and-monitoring.md`. Actual release still requires E storage, Linux/container, API-pressure and independent-receiver validation.

## Inputs and generation

Copy `deploy/monitoring/expected-targets.example.yaml` to a site-owned file, set the actual cluster identity, profiles, maximum ages and expected supported rule IDs. Inventory is configuration, independent of successful CronJob execution. Verify managed-control-plane capabilities before removing an unsupported rule from the expected list. Do not remove expectations merely to hide collection errors.

```sh
python -m tools.render_monitoring --targets deploy/monitoring/expected-targets.example.yaml --output deploy/monitoring/generated
python -m k8s_inspector.expected_targets --config deploy/monitoring/expected-targets.example.yaml
python -m pytest tests/test_monitoring_catalog.py tests/test_metrics_quality.py -q
promtool check rules deploy/monitoring/generated/inspection-rules.yaml
promtool test rules tests/prometheus/inspection-rules.test.yaml
amtool check-config deploy/monitoring/generated/alertmanager-snippet.yaml
```

The generated directory contains standalone rules, an inventory ConfigMap/Deployment/Service, a scrape snippet and a complete route example. The checked-in example is reproducible from the catalog. `rule_catalog` supplies the only primary notification profile mapping. The inventory process has no Kubernetes credentials, writable state or dependency on Pushgateway. Its read-only ConfigMap triggers a rollout through a content hash when regenerated.

The repository's Prometheus image is 2.53.0 and Alertmanager image is 0.27.0. Offline tool checks use those versions; Kustomize rendering uses 5.4.3. Pin and verify official release checksums. Query and fixture semantics follow [Prometheus query functions](https://prometheus.io/docs/prometheus/2.55/querying/functions/) and [rule unit testing](https://prometheus.io/docs/prometheus/2.55/configuration/unit_testing_rules/); executable 2.53.0 tests are the compatibility gate.

## State and evidence

Use a persistent verified local/block state volume with `--state-dir` / `INSPECTOR_STATE_DIR`; the report NFS example does not verify state locking. Every profile uses a run lock; updates also use a state-file lock. Test two processes, Pod replacement, node/volume interruption, restoration and actual permission failures on the deployed filesystem. Keep CronJob `Forbid`; set active deadlines with shutdown margin beyond the catalog's 240/480/900/1800-second budgets.

Complete valid aggregate observations alone update `inspector_last_valid_failures` and its timestamp. Incomplete positive findings are persisted separately as confirmed evidence, with no claim of complete coverage; `inspector_confirmed_failures` retains them through unknown runs. Only a fresh complete valid healthy aggregate clears these aggregate counters. Old resource UID recovery still requires same-resource evidence in reports/incident handling; a newly named/recreated object does not produce a healthy observation for the old UID.

Unreadable state or a missing initialized state file fails a required sink. Keep readable prior counters on a write failure. The `.initialized` marker cannot distinguish first install from simultaneous loss of the entire volume. Unknown without history has no default healthy sample; monitor missing evidence and restore the actual state volume. Keep state and v2 reports on rollback.

Business expressions retain positive evidence with `last_over_time(...[30d])` while a current sample is absent. This is bounded by available Prometheus history, retention and TSDB survival, and does not replace persistent inspection/incident state. The example Prometheus has 15-day retention and ephemeral TSDB storage; E must size and persist it. Missing telemetry, old valid times, Pushgateway failure and inventory failure have separate quality alerts. Alertmanager resolved due to disappearing telemetry is never verified incident recovery.

## Shadow integration

1. Render the site inventory and validate every generated file. Deploy only after E authorization and gates. The inventory resource is separate from the CronJobs and Pushgateway.
2. Add the inventory scrape job and copy `inspection-rules.yaml` into the Prometheus rule volume beside legacy rules. Reload/restart using the site's reviewed procedure.
3. Install the `shadow="true"` → `shadow-null` route **before** ordinary routes, with a receiver having no integrations. This must precede loading shadow rules. Merge the route into the existing Alertmanager configuration; preserve existing Bridge async `http_config.authorization`, Secret mounts, other receivers and operational inhibition rules.
4. Verify inventory up/absent behavior through an independent receiver, not solely the Bridge/Feishu path under test. In shadow mode the generated example deliberately discards notifications; this is not production failure coverage.
5. Observe at least a complete daily cycle, compare reports against both rule sets, and exercise never-ran, stopped-run, cleared Pushgateway, missing inventory, FAIL→ERROR, first partial FAIL→ERROR, stale positive evidence and complete healthy recovery. Confirm that hourly/full samples do not create 5m business notifications.

Offline base Kustomize rendering does not validate a merged shadow/async rollout. The generated Alertmanager file is an integration example, not a replacement for an existing async/credentialed configuration. Validate the **fully merged** configuration and mount paths with amtool and a real delivery test before activation.

## Activation

After shadow and E gates pass, render a separate candidate:

```sh
python -m tools.render_monitoring --targets SITE_TARGETS.yaml --output REVIEWED_OUTPUT --activate --quality-receiver-url https://YOUR_INDEPENDENT_RECEIVER/alert
```

This only writes local candidate files; it does not deploy or send. An independent receiver URL is required for active generation. Verify that receiver with a controlled end-to-end test before changing production routing. Keep it independent of the inspected Bridge, its SQLite volume and the normal Feishu destination.

Stop the old HasFail/HasWarn/CheckFail/PendingPVC/utilization equivalents before activating the new business rule set; keep quality alerts. Each rule notifies from its primary profile only. Do not silence business findings because collection quality degraded. New quality routes have their own receiver and no blanket business inhibition. After verifying the new job/cluster/profile grouping keys, explicitly remove only obsolete Pushgateway groups; PUT replaces current groups but does not delete renamed groups automatically.

## Release gates and rollback

- Actual cluster RBAC/managed capabilities, list pagination and request budgets at realistic scale.
- Verified state persistence, cross-process locking and restoration on the actual local/block volume; Linux/Python 3.9+ and built-image checks.
- Fully composed Kustomize manifests, actual Prometheus/Alertmanager reload and mounted Secret compatibility, including the async Bridge option.
- An independent inventory scrape/quality receiver, tested while the normal Bridge is unavailable.
- Shadow daily-cycle evidence, capacity/cardinality measurements, persistent Prometheus storage and retention sizing.

Rollback restores the previous rule/route configuration, stops the new active notification set and preserves state/reports/confirmed evidence. Do not delete initialization markers or fabricate zero to suppress an alert. Restore unreadable state from a verified backup and re-run inspection; require new complete healthy evidence before treating the business condition as recovered. Manual incident disposition remains D work.
