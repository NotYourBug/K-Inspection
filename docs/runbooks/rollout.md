# Controlled shadow rollout and rollback

Current authorization: offline preparation only. No kubectl apply, real Feishu sends, failure injection or production switching has been performed. A site owner must review a concrete candidate and authorize the actual test environment before any of those steps.

## Prepare the candidate locally

1. Run source tests, build/install the wheel outside source, render base/example/shadow and run promtool/amtool. `python -m tools.verify_release --offline --output-dir ./out/release-check` writes individual outcomes and artifacts; exit1 is expected while clean dependency installation, selected pins, Docker/runtime matrix or external acceptance is pending. Reuse requires a new output directory. Do not relabel an offline failure/pending report as release-ready.
2. CI is an unexecuted GitHub example until a repository/runner exists. It uses Python3.9/3.11/3.14, selected direct/tool constraints, real Kustomize/rule binaries and a separate Docker smoke job. The local Windows3.12 run does not establish that matrix. Save actual resolved versions; the constraints are not a full transitive lock.

   CI uses `--offline --local-checks-only` to require wheel/install/imports, the complete source suite, all three renders, composed Prometheus/Alertmanager configs and both inspection/Bridge rule suites. This explicit flag changes only the process exit gate: JSON still reports full `success=false` while external checks are pending, with a separate `local_checks_success` and `exit_gate=local`. Omitting the flag retains the full release gate and expected exit1. Missing local outcomes fail either local evaluation or release evaluation.
3. Build the nonroot image from the verified pinned official Python3.11 base manifest. Record local image metadata; a local image ID is not a published registry digest. After a separately authorized push, pin the actual deployment image digest in a site overlay. Example image tags and StorageClass names remain placeholders.
4. Render with `kustomize build deploy/overlays/shadow`. Review exact resources, ConfigMaps, Secret references, storage bindings and diffs. The shadow namespace is k8s-inspector-shadow; report root /data/shadow, Pushgateway job k8s-inspector-shadow, per-profile state PVCs and Bridge state PVC are namespace-isolated. Control-plane Role/RoleBinding stay in kube-system with shadow-prefixed names and shadow ServiceAccount subjects. Changing control_plane_namespace requires matching site RBAC patches.

The shadow overlay starts suspended. Its example monitoring scrapes its own Pushgateway and inventory, and keeps the legacy rules alongside new rules labelled shadow=true. The shadow-null route discards every new shadow notification while preserving authenticated legacy delivery into a dedicated test destination. This prevents new/old business rules from both notifying. Operators must independently verify the resolved chat IDs differ: distinct Secret names prove configuration isolation, not actual provider destination identity.

## Site inputs and authorization

Supply target Kubernetes version (timeZone support), actual image digest, cluster_id, workload/control namespaces, managed-control-plane capabilities, node/Pod scale, report RWX and per-profile/Bridge verified local/block StorageClasses, CNI/API/DNS/proxy/Feishu egress requirements, Alertmanager source Secret and a dedicated test Feishu Secret. Never post credentials to logs. The base ingress policy assumes monitoring Pods in the same namespace; cross-namespace existing monitoring needs a reviewed policy. Egress is intentionally site-specific and remains a release gate.

Do not treat ReadWriteOnce as proof of suitable SQLite storage. Verify actual CSI/PV filesystem, permissions/nonroot, lock exclusivity, Pod replacement, node/volume interruption, backup and restoration using disposable state. Prometheus/Alertmanager example data use separate persistent claims; the45-day Prometheus sample retention still needs actual capacity, persistence and restart validation. Their availability and the independent receiver remain separate acceptance items.

## Test scope and evidence

After explicit site authorization, server-dry-run the fully merged overlay first. Then use only the approved namespace, claims and test destination. Inspect capabilities/RBAC before interpreting missing resources. Unsuspend a small chosen profile; capture actual deadline/exit/requests, reports and metrics. Observe multiple daily runs and recoveries.7–14 days is an observation suggestion, not an automated wait or permission granted by elapsed time.

Exercise current Pod unready/init failure, expected WFFC and Lost fixtures, metrics403/timeout, FAIL→ERROR→fresh complete PASS, never-ran and cleared Pushgateway, inventory/Bridge outage, DB unavailable, provider429/timeouts and restart/retry. Do not mutate production resources to create fixtures. D creation-unknown drills apply only after D exists. Record expected vs observed results and reasons for false/missed alerts, coverage, valid observation ages, API load, queue oldest age/dead/unknown and actual independent quality delivery.

## Activation and stop conditions

Only the actual owner can approve activation after G2/G3 evidence. Review small-scope formal destinations and all Secret mounts. Confirm the independent quality receiver works while the ordinary Bridge is unavailable. Stop equivalent legacy business notifications before activating v2, retain quality coverage, then remove only obsolete Pushgateway grouping keys. Preserve previous code/config and a consistent state backup. Inhibition, silence, missing samples or Alertmanager resolved are not verified incident recovery.

Stop expansion on missed faults, false recovery, state/backup failures or sustained backlog growth. Keep the shadow recording path available for diagnosis. No publish/deploy action is part of the local verifier.

## Rollback

Freeze new input/cutover under the site's reviewed procedure, inspect/export pending/DEAD/unknown jobs and make a consistent backup through bridge_admin into a pre-created durable directory. Keep compatible async code running/draining retained outbox; do not switch to sync and abandon queued jobs. Keep observation state, confirmed evidence, latest/reports and maintenance audit. Verify both report and Bridge schema compatibility before selecting code or restoring a matching backup. Never overwrite an unverified new database with an old one.

The read-only helper `python -m tools.rollout --current-schema 3 --target-schema 3 --delivery-mode async --unfinished-jobs 4` rejects incompatible schema or sync with unfinished work. It neither reads the actual DB nor grants permission: the counts/schema must come from a fresh local inspect; a stale/incorrect input does not prove safe rollback. Its output always requires backup, preservation and operator review.

Reapply the reviewed former rule/route configuration only after authorization; preserve jobs/state rather than deleting PVCs, databases or initialization markers. Exercise backup restore and queue replay in disposable volumes first, verify original delivery IDs and historical audit, then require fresh complete healthy UID evidence before closing incidents. Manual dispositions require reasons; do not forge SENT or healthy zero.
