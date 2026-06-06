-- ATT SDP ontology objects (ex-Foundry Object Types)
-- Service Delivery Platform: incidents, orders, accounts, technicians

CREATE TABLE IF NOT EXISTS ${catalog}.${gold_schema}.customer_account (
  account_id        STRING NOT NULL COMMENT 'Foundry PK: account_id',
  account_name      STRING NOT NULL,
  market            STRING NOT NULL COMMENT 'e.g. DALLAS, ATLANTA, CHICAGO',
  tier              STRING NOT NULL COMMENT 'RESIDENTIAL, BUSINESS, ENTERPRISE',
  updated_at        TIMESTAMP NOT NULL,
  CONSTRAINT pk_customer_account PRIMARY KEY (account_id)
)
USING DELTA
COMMENT 'Migrated from Foundry object type CustomerAccount'
TBLPROPERTIES (
  'foundry.object_type_rid' = 'ri.ontology.att.object-type.customer-account'
);

CREATE TABLE IF NOT EXISTS ${catalog}.${gold_schema}.service_incident (
  incident_id           STRING NOT NULL,
  customer_account_id   STRING NOT NULL,
  market                STRING NOT NULL,
  severity              STRING NOT NULL COMMENT 'P1, P2, P3, P4',
  status                STRING NOT NULL COMMENT 'OPEN, IN_PROGRESS, DISPATCHED, RESOLVED, CLOSED',
  title                 STRING NOT NULL,
  description           STRING,
  service_type          STRING COMMENT 'FIBER, WIRELESS, UVERSE, ENTERPRISE',
  opened_at             TIMESTAMP NOT NULL,
  resolved_at           TIMESTAMP,
  assigned_technician_id STRING,
  ops_notes             STRING COMMENT 'Lakebase writeback field',
  updated_at            TIMESTAMP NOT NULL,
  CONSTRAINT pk_service_incident PRIMARY KEY (incident_id),
  CONSTRAINT fk_incident_account FOREIGN KEY (customer_account_id)
    REFERENCES ${catalog}.${gold_schema}.customer_account (account_id)
)
USING DELTA
COMMENT 'Migrated from Foundry object type ServiceIncident'
TBLPROPERTIES (
  'foundry.object_type_rid' = 'ri.ontology.att.object-type.service-incident',
  'lakebase.sync_enabled' = 'true',
  'lakebase.sync_table' = 'service_incident_ops'
);

CREATE TABLE IF NOT EXISTS ${catalog}.${gold_schema}.service_order (
  order_id              STRING NOT NULL,
  customer_account_id   STRING NOT NULL,
  service_type          STRING NOT NULL,
  status                STRING NOT NULL COMMENT 'PENDING, PROVISIONING, ACTIVE, CANCELLED',
  market                STRING NOT NULL,
  promised_date         DATE,
  updated_at            TIMESTAMP NOT NULL,
  CONSTRAINT pk_service_order PRIMARY KEY (order_id),
  CONSTRAINT fk_order_account FOREIGN KEY (customer_account_id)
    REFERENCES ${catalog}.${gold_schema}.customer_account (account_id)
)
USING DELTA
COMMENT 'Migrated from Foundry object type ServiceOrder'
TBLPROPERTIES (
  'foundry.object_type_rid' = 'ri.ontology.att.object-type.service-order'
);

CREATE TABLE IF NOT EXISTS ${catalog}.${gold_schema}.field_technician (
  technician_id     STRING NOT NULL,
  technician_name   STRING NOT NULL,
  market            STRING NOT NULL,
  status            STRING NOT NULL COMMENT 'AVAILABLE, DISPATCHED, OFF_DUTY',
  skill_level       STRING NOT NULL COMMENT 'L1, L2, L3',
  updated_at        TIMESTAMP NOT NULL,
  CONSTRAINT pk_field_technician PRIMARY KEY (technician_id)
)
USING DELTA
COMMENT 'Migrated from Foundry object type FieldTechnician'
TBLPROPERTIES (
  'foundry.object_type_rid' = 'ri.ontology.att.object-type.field-technician'
);

-- Agent audit log (ex-AIP query logging)
CREATE TABLE IF NOT EXISTS ${catalog}.${gold_schema}.agent_audit_log (
  audit_id          STRING NOT NULL,
  agent_name        STRING NOT NULL,
  tool_name         STRING NOT NULL,
  user_id           STRING,
  query_text        STRING,
  result_summary    STRING,
  created_at        TIMESTAMP NOT NULL,
  CONSTRAINT pk_agent_audit_log PRIMARY KEY (audit_id)
)
USING DELTA
COMMENT 'Audit trail for AgentBricks / Mosaic agent tool invocations';
