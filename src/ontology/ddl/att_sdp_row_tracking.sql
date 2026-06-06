-- Enable row tracking on MV source tables (required for incremental refresh)
-- Docs: https://docs.databricks.com/en/optimizations/incremental-refresh

ALTER STREAMING TABLE ${catalog}.${gold_schema}.customer_account
SET TBLPROPERTIES ('delta.enableRowTracking' = 'true');

ALTER STREAMING TABLE ${catalog}.${gold_schema}.field_technician
SET TBLPROPERTIES ('delta.enableRowTracking' = 'true');

ALTER STREAMING TABLE ${catalog}.${gold_schema}.service_incident
SET TBLPROPERTIES ('delta.enableRowTracking' = 'true');

ALTER TABLE ${catalog}.${gold_schema}.bridge_incident_technician
SET TBLPROPERTIES ('delta.enableRowTracking' = 'true');
