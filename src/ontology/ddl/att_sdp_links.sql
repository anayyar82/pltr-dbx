-- ATT SDP link types (ex-Foundry Links)

CREATE TABLE IF NOT EXISTS ${catalog}.${gold_schema}.bridge_incident_technician (
  incident_id           STRING NOT NULL,
  technician_id         STRING NOT NULL,
  assignment_status     STRING NOT NULL COMMENT 'PROPOSED, CONFIRMED, COMPLETED, CANCELLED',
  assigned_at           TIMESTAMP NOT NULL,
  assigned_by           STRING COMMENT 'user or agent id',
  CONSTRAINT pk_bridge_incident_technician PRIMARY KEY (incident_id, technician_id),
  CONSTRAINT fk_bit_incident FOREIGN KEY (incident_id)
    REFERENCES ${catalog}.${gold_schema}.service_incident (incident_id),
  CONSTRAINT fk_bit_technician FOREIGN KEY (technician_id)
    REFERENCES ${catalog}.${gold_schema}.field_technician (technician_id)
)
USING DELTA
COMMENT 'Foundry link: TechnicianAssignedToIncident (M:N dispatch history)'
TBLPROPERTIES (
  'foundry.link_type_rid' = 'ri.ontology.att.link-type.technician-assigned-to-incident'
);

-- Denormalized materialized view for dashboards, Genie, and Apps
-- Refreshed incrementally via serverless pipeline (requires row tracking on sources)
DROP VIEW IF EXISTS ${catalog}.${gold_schema}.mv_incident_dispatch_board;
DROP MATERIALIZED VIEW IF EXISTS ${catalog}.${gold_schema}.mv_incident_dispatch_board;

CREATE MATERIALIZED VIEW ${catalog}.${gold_schema}.mv_incident_dispatch_board
REFRESH POLICY INCREMENTAL
COMMENT 'Operational dispatch board — incident + account + technician (serverless incremental refresh)'
AS
SELECT
  i.incident_id,
  i.title,
  i.severity,
  COALESCE(o.status, i.status) AS incident_status,
  i.market,
  i.service_type,
  i.opened_at,
  COALESCE(o.ops_notes, i.ops_notes) AS ops_notes,
  a.account_name,
  a.tier AS account_tier,
  t.technician_id,
  t.technician_name,
  t.status AS technician_status,
  b.assignment_status,
  b.assigned_at
FROM ${catalog}.${gold_schema}.service_incident i
JOIN ${catalog}.${gold_schema}.customer_account a ON i.customer_account_id = a.account_id
LEFT JOIN ${catalog}.${gold_schema}.service_incident_ops o ON i.incident_id = o.incident_id
LEFT JOIN ${catalog}.${gold_schema}.bridge_incident_technician b
  ON i.incident_id = b.incident_id AND b.assignment_status IN ('PROPOSED', 'CONFIRMED')
LEFT JOIN ${catalog}.${gold_schema}.field_technician t ON b.technician_id = t.technician_id;
