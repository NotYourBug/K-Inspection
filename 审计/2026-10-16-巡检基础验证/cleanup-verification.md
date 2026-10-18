# Cleanup verification note

Audit publication commit: ab98915d761f2373c10ebb422a08fdf171db6d28. Before cleanup all90 working evidence files and committed blobs matched the manifest SHA256; Git tree was clean.

The cleanup command verified the resolved absolute target stayed at the exact expected plan directory inside the workspace and matched its ledger ownership header. Its additional reparse check used a nested PowerShell array. That check raised InvalidOperation (System.Object[] has no op_BitwiseAnd); default Continue allowed deletion of the already path-checked plan directory. The extra check therefore did not complete before deletion; absence of reparse entries inside the deleted directory is not retrospectively claimed.

Follow-up verification used ErrorActionPreference=Stop and a flat per-directory loop. The retained .superpowers, sdd and tools parent directories were ordinary non-reparse directories; the plan directory was absent; all3 shared validation executables matched their archived SHA256; the shared pip cache remained present; Git status was clean. No further deletion was attempted. The source/test commit ea2f6e3 remained unchanged after447/447 tests and30/30 installed-wheel checks.

This note preserves the cleanup error rather than counting it as a successful pre-delete check. The evidence manifest is refreshed for the added note and ledger entries; historical byte-exact evidence remains in ab98915. Final working and committed blob hashes are verified again after this documentation-only publication.
