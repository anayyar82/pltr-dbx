-- Refresh dispatch board MV (must run on SQL warehouse — created in DBSQL)
REFRESH MATERIALIZED VIEW users.ankur_nayyar.mv_incident_dispatch_board;
