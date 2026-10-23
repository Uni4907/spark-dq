CREATE TABLE datasets (
 id CHAR(36) PRIMARY KEY, name VARCHAR(255) NOT NULL, file_path VARCHAR(512) NOT NULL,
 sha256 CHAR(64) NOT NULL, size_bytes BIGINT NOT NULL, row_count BIGINT NOT NULL,
 columns_json JSON NOT NULL, preview_json JSON NOT NULL, created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);
CREATE TABLE tasks (
 id CHAR(36) PRIMARY KEY,dataset_id CHAR(36) NOT NULL,status VARCHAR(16) NOT NULL,
 total_rows BIGINT,spark_version VARCHAR(32),error TEXT,duration_ms BIGINT,
 created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),started_at TIMESTAMP(3) NULL,finished_at TIMESTAMP(3) NULL,
 FOREIGN KEY(dataset_id) REFERENCES datasets(id), INDEX(created_at)
);
CREATE TABLE rules (
 id CHAR(36) PRIMARY KEY,task_id CHAR(36) NOT NULL,type VARCHAR(32) NOT NULL,columns_json JSON NOT NULL,
 FOREIGN KEY(task_id) REFERENCES tasks(id)
);
CREATE TABLE results (
 id CHAR(36) PRIMARY KEY,rule_id CHAR(36) NOT NULL UNIQUE,metrics_json JSON NOT NULL,anomaly_path VARCHAR(512) NOT NULL,
 FOREIGN KEY(rule_id) REFERENCES rules(id)
);

-- Additive upgrade: existing datasets, rules and results remain untouched.
CREATE TABLE task_snapshots (
 task_id CHAR(36) PRIMARY KEY,parent_task_id CHAR(36) NULL,input_sha256 CHAR(64) NOT NULL,
 semantics_version VARCHAR(32) NOT NULL,rules_json JSON NOT NULL,
 created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 FOREIGN KEY(task_id) REFERENCES tasks(id),FOREIGN KEY(parent_task_id) REFERENCES tasks(id)
);

CREATE TABLE governance_runs (
 task_id CHAR(36) PRIMARY KEY,source_task_id CHAR(36) NOT NULL,policy_json JSON NOT NULL,
 output_dataset_id CHAR(36) NULL,input_rows BIGINT,output_rows BIGINT,removed_rows BIGINT,
 FOREIGN KEY(task_id) REFERENCES tasks(id),FOREIGN KEY(source_task_id) REFERENCES tasks(id),
 FOREIGN KEY(output_dataset_id) REFERENCES datasets(id)
);
CREATE TABLE dataset_versions (
 dataset_id CHAR(36) PRIMARY KEY,parent_dataset_id CHAR(36) NOT NULL,governance_task_id CHAR(36) NOT NULL UNIQUE,
 created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 FOREIGN KEY(dataset_id) REFERENCES datasets(id),FOREIGN KEY(parent_dataset_id) REFERENCES datasets(id),
 FOREIGN KEY(governance_task_id) REFERENCES tasks(id)
);

CREATE TABLE rule_parameters (
 rule_id CHAR(36) PRIMARY KEY,params_json JSON NOT NULL,
 FOREIGN KEY(rule_id) REFERENCES rules(id)
);
CREATE TABLE rule_template_heads (
 id CHAR(36) PRIMARY KEY,name VARCHAR(120) NOT NULL,revision INT NOT NULL,archived BOOLEAN NOT NULL DEFAULT false,
 updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);
CREATE TABLE rule_template_versions (
 template_id CHAR(36) NOT NULL,revision INT NOT NULL,name VARCHAR(120) NOT NULL,definitions_json JSON NOT NULL,
 created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 PRIMARY KEY(template_id,revision),FOREIGN KEY(template_id) REFERENCES rule_template_heads(id)
);
CREATE TABLE task_template_refs (
 task_id CHAR(36) PRIMARY KEY,template_id CHAR(36) NOT NULL,revision INT NOT NULL,
 FOREIGN KEY(task_id) REFERENCES tasks(id),FOREIGN KEY(template_id,revision) REFERENCES rule_template_versions(template_id,revision)
);

CREATE TABLE governance_audits (
 task_id CHAR(36) PRIMARY KEY,audit_json JSON NOT NULL,
 FOREIGN KEY(task_id) REFERENCES tasks(id)
);
