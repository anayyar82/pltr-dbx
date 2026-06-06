-- Lakebase writeback sync configuration for SDP Ops Console
-- Docs: https://docs.databricks.com/en/database/lakebase-sync.html
--
-- Pattern: App writes to Lakebase (low-latency Postgres) → syncs to Delta gold.
-- This replaces Foundry Actions / writeback on ServiceIncident objects.

-- Operational mirror table (Lakebase-synced from gold.service_incident)
-- Deploy Lakebase instance and enable sync via Databricks UI or API after DDL deploy.
CREATE TABLE IF NOT EXISTS ${catalog}.${gold_schema}.service_incident_ops (
  incident_id           STRING NOT NULL,
  status                STRING NOT NULL,
  assigned_technician_id STRING,
  ops_notes             STRING,
  updated_at            TIMESTAMP NOT NULL,
  updated_by            STRING COMMENT 'App user or service principal',
  CONSTRAINT pk_service_incident_ops PRIMARY KEY (incident_id),
  CONSTRAINT fk_ops_incident FOREIGN KEY (incident_id)
    REFERENCES ${catalog}.${gold_schema}.service_incident (incident_id)
)
USING DELTA
COMMENT 'Lakebase writeback target — ops-editable incident fields'
TBLPROPERTIES (
  'lakebase.sync_direction' = 'bidirectional',
  'lakebase.source_table' = 'service_incident',
  'lakebase.sync_columns' = 'status,assigned_technician_id,ops_notes,updated_at',
  'foundry.action_rid' = 'ri.foundry.att.action.update-incident-status'
);

-- Change-data feed enabled for downstream sync triggers
ALTER TABLE ${catalog}.${gold_schema}.service_incident_ops
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
