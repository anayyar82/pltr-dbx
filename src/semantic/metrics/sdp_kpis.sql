-- SDP semantic layer for AI/BI Dashboards and Genie Space
-- Replaces Foundry Slate dashboards for service delivery KPIs

CREATE OR REPLACE VIEW ${catalog}.${gold_schema}.metric_open_incidents_by_market AS
SELECT
  i.market,
  i.severity,
  COUNT(*) AS open_incidents,
  MIN(i.opened_at) AS oldest_opened_at
FROM ${catalog}.${gold_schema}.service_incident i
LEFT JOIN ${catalog}.${gold_schema}.service_incident_ops o ON i.incident_id = o.incident_id
WHERE COALESCE(o.status, i.status) IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')
GROUP BY i.market, i.severity;

CREATE OR REPLACE VIEW ${catalog}.${gold_schema}.metric_incident_mttr AS
SELECT
  market,
  service_type,
  AVG(
    TIMESTAMPDIFF(HOUR, opened_at, COALESCE(resolved_at, current_timestamp()))
  ) AS avg_hours_to_resolve,
  COUNT(*) AS incident_count
FROM ${catalog}.${gold_schema}.service_incident
WHERE opened_at >= date_sub(current_date(), 30)
GROUP BY market, service_type;

CREATE OR REPLACE VIEW ${catalog}.${gold_schema}.metric_order_fulfillment_sla AS
SELECT
  market,
  service_type,
  status,
  COUNT(*) AS order_count,
  SUM(CASE WHEN promised_date >= current_date() THEN 1 ELSE 0 END) AS on_track_count
FROM ${catalog}.${gold_schema}.service_order
GROUP BY market, service_type, status;

CREATE OR REPLACE VIEW ${catalog}.${gold_schema}.metric_technician_utilization AS
SELECT
  t.market,
  t.status,
  t.skill_level,
  COUNT(*) AS technician_count,
  COUNT(DISTINCT b.incident_id) AS active_assignments
FROM ${catalog}.${gold_schema}.field_technician t
LEFT JOIN ${catalog}.${gold_schema}.bridge_incident_technician b
  ON t.technician_id = b.technician_id
  AND b.assignment_status IN ('PROPOSED', 'CONFIRMED')
GROUP BY t.market, t.status, t.skill_level;

CREATE OR REPLACE VIEW ${catalog}.${gold_schema}.metric_sdp_executive_summary AS
SELECT
  (SELECT COUNT(*) FROM ${catalog}.${gold_schema}.service_incident i
   LEFT JOIN ${catalog}.${gold_schema}.service_incident_ops o ON i.incident_id = o.incident_id
   WHERE COALESCE(o.status, i.status) IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')) AS total_open_incidents,
  (SELECT COUNT(*) FROM ${catalog}.${gold_schema}.service_incident i
   LEFT JOIN ${catalog}.${gold_schema}.service_incident_ops o ON i.incident_id = o.incident_id
   WHERE i.severity = 'P1'
     AND COALESCE(o.status, i.status) IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED')) AS open_p1_incidents,
  (SELECT COUNT(*) FROM ${catalog}.${gold_schema}.service_order
   WHERE status = 'PROVISIONING') AS orders_in_provisioning,
  (SELECT COUNT(*) FROM ${catalog}.${gold_schema}.field_technician
   WHERE status = 'AVAILABLE') AS available_technicians;
