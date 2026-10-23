-- Canonical CSV references remain unchanged; prepared execution receipts are append-only.
ALTER TABLE task_snapshots
 ADD COLUMN execution_plan_json JSON NULL,
 ADD COLUMN input_manifest_sha256 CHAR(64) NULL;
