# CLAUDE.md

This Kubernetes inspection tool supports bounded inspection, structured reports,
Pushgateway metrics and fixed-chat Feishu notifications.

Read [AGENTS.md](AGENTS.md) for commands, current design decisions and release
gates. It is the shared repository guidance. The complete module tree and old to
new import mapping are in [Python package layout](docs/reference/package-layout.md).

## Working on Python code

- Install with `pip install -r requirements.txt`; run `python -m pytest tests/ -v`.
- Put shared config/models/helpers in `core/`, collection and pure rules in
  `inspection/`, reports in `reporting/`, metrics/inventory in `monitoring/`, and
  HTTP routing, delivery and providers in `notifications/`.
- Import canonical subpackage modules. Keep `__init__.py` files free of service
  initialization, and keep core and pure rules independent of output adapters.
- Preserve the documented root CLI/MCP/Bridge/admin/inventory commands. Root
  Bridge/admin/inventory modules are command wrappers, not import facades.
- Feishu notifications use existing chats with responsibility routing and a
  fallback. The Bridge does not create, invite members to or rename groups.
- Keep report, observation state and durable-delivery contracts described in
  AGENTS.md and the runbooks. Passing local tests is distinct from actual cluster,
  storage and real notification acceptance.
