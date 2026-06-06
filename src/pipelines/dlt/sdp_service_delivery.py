# Databricks notebook source
# MAGIC %md
# MAGIC # ATT SDP Service Delivery DLT pipeline (triggered refresh)
# MAGIC Migrated from Foundry Build `sdp-service-delivery-build`.
# MAGIC
# MAGIC **Write + refresh pattern:** append rows to `/Volumes/.../sdp_exports/`, then run job `sdp_refresh`.
# MAGIC Each pipeline update batch-reads bronze, dedupes by key, and reloads gold (no full refresh needed).

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------


def _conf(key: str, default: str) -> str:
    return spark.conf.get(key, default)


CATALOG = _conf("bundle.catalog", "users")
BRONZE = _conf("bundle.bronze_schema", "ankur_nayyar")
SILVER = _conf("bundle.silver_schema", "ankur_nayyar")
GOLD = _conf("bundle.gold_schema", "ankur_nayyar")


def volume_path(entity: str) -> str:
    return f"/Volumes/{CATALOG}/{BRONZE}/sdp_exports/{entity}"


def _read_bronze_snapshot(entity: str):
    """Batch read of bronze landing (append-only writes from notebooks 07/08)."""
    return (
        spark.read.format("delta")
        .load(volume_path(entity))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def _latest_by_key(df, key: str):
    return df.orderBy(F.col("updated_at").desc()).dropDuplicates([key])


# --- Bronze: land raw Foundry / OSS exports ---

@dlt.table(
    name="raw_customer_accounts",
    comment="Bronze: customer accounts from CRM export",
    table_properties={"foundry.dataset_rid": "ri.foundry.att.dataset.customer-accounts-raw"},
)
def raw_customer_accounts():
    return _read_bronze_snapshot("customer_accounts")


@dlt.table(
    name="raw_service_incidents",
    comment="Bronze: network incidents from NOC monitoring",
    table_properties={"foundry.dataset_rid": "ri.foundry.att.dataset.service-incidents-raw"},
)
def raw_service_incidents():
    return _read_bronze_snapshot("service_incidents")


@dlt.table(
    name="raw_service_orders",
    comment="Bronze: service fulfillment orders",
    table_properties={"foundry.dataset_rid": "ri.foundry.att.dataset.service-orders-raw"},
)
def raw_service_orders():
    return _read_bronze_snapshot("service_orders")


@dlt.table(
    name="raw_field_technicians",
    comment="Bronze: field technician roster",
    table_properties={"foundry.dataset_rid": "ri.foundry.att.dataset.field-technicians-raw"},
)
def raw_field_technicians():
    return _read_bronze_snapshot("field_technicians")


# --- Silver: conform + dedupe (latest row per key) ---

@dlt.table(name="stg_customer_accounts")
@dlt.expect_all({
    "valid_account_id": "account_id IS NOT NULL",
    "valid_tier": "tier IN ('RESIDENTIAL', 'BUSINESS', 'ENTERPRISE')",
})
def stg_customer_accounts():
    return (
        dlt.read("raw_customer_accounts")
        .select(
            F.col("account_id").cast("string").alias("account_id"),
            F.trim("account_name").alias("account_name"),
            F.upper("market").alias("market"),
            F.upper("tier").alias("tier"),
            F.coalesce("updated_at", F.current_timestamp()).alias("updated_at"),
        )
        .transform(lambda df: _latest_by_key(df, "account_id"))
    )


@dlt.table(name="stg_service_incidents")
@dlt.expect_all({
    "valid_incident_id": "incident_id IS NOT NULL",
    "valid_severity": "severity IN ('P1', 'P2', 'P3', 'P4')",
    "valid_status": "status IN ('OPEN', 'IN_PROGRESS', 'DISPATCHED', 'RESOLVED', 'CLOSED')",
})
def stg_service_incidents():
    return (
        dlt.read("raw_service_incidents")
        .select(
            F.col("incident_id").cast("string").alias("incident_id"),
            F.col("customer_account_id").cast("string").alias("customer_account_id"),
            F.upper("market").alias("market"),
            F.upper("severity").alias("severity"),
            F.upper("status").alias("status"),
            F.trim("title").alias("title"),
            F.col("description").alias("description"),
            F.upper("service_type").alias("service_type"),
            F.col("opened_at").cast("timestamp").alias("opened_at"),
            F.col("resolved_at").cast("timestamp").alias("resolved_at"),
            F.col("assigned_technician_id").cast("string").alias("assigned_technician_id"),
            F.col("ops_notes").alias("ops_notes"),
            F.coalesce("updated_at", F.current_timestamp()).alias("updated_at"),
        )
        .transform(lambda df: _latest_by_key(df, "incident_id"))
    )


@dlt.table(name="stg_service_orders")
@dlt.expect_all({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_order_status": "status IN ('PENDING', 'PROVISIONING', 'ACTIVE', 'CANCELLED')",
})
def stg_service_orders():
    return (
        dlt.read("raw_service_orders")
        .select(
            F.col("order_id").cast("string").alias("order_id"),
            F.col("customer_account_id").cast("string").alias("customer_account_id"),
            F.upper("service_type").alias("service_type"),
            F.upper("status").alias("status"),
            F.upper("market").alias("market"),
            F.col("promised_date").cast("date").alias("promised_date"),
            F.coalesce("updated_at", F.current_timestamp()).alias("updated_at"),
        )
        .transform(lambda df: _latest_by_key(df, "order_id"))
    )


@dlt.table(name="stg_field_technicians")
@dlt.expect_all({
    "valid_technician_id": "technician_id IS NOT NULL",
    "valid_tech_status": "status IN ('AVAILABLE', 'DISPATCHED', 'OFF_DUTY')",
})
def stg_field_technicians():
    return (
        dlt.read("raw_field_technicians")
        .select(
            F.col("technician_id").cast("string").alias("technician_id"),
            F.trim("technician_name").alias("technician_name"),
            F.upper("market").alias("market"),
            F.upper("status").alias("status"),
            F.upper("skill_level").alias("skill_level"),
            F.coalesce("updated_at", F.current_timestamp()).alias("updated_at"),
        )
        .transform(lambda df: _latest_by_key(df, "technician_id"))
    )


# --- Gold: ontology objects (batch snapshot per refresh — supports append bronze) ---

def _gold_table(table_name: str, source: str):
    @dlt.table(
        name=table_name,
        comment=f"Gold object: {table_name}",
        table_properties={
            "foundry.pipeline": "sdp_service_delivery",
            "delta.enableRowTracking": "true",
            "delta.enableChangeDataFeed": "true",
        },
    )
    def gold():
        return dlt.read(source)

    return gold


for _table_name, _source in [
    ("customer_account", "stg_customer_accounts"),
    ("service_incident", "stg_service_incidents"),
    ("service_order", "stg_service_orders"),
    ("field_technician", "stg_field_technicians"),
]:
    _gold_table(_table_name, _source)

# --- Gold: Lakebase writeback mirror ---

@dlt.table(name="stg_service_incident_ops")
def stg_service_incident_ops():
    return dlt.read("stg_service_incidents").select(
        "incident_id",
        "status",
        "assigned_technician_id",
        "ops_notes",
        "updated_at",
    ).withColumn("updated_by", F.lit("dlt_pipeline"))


@dlt.table(
    name="service_incident_ops",
    comment="Lakebase writeback mirror for incident ops fields",
    table_properties={
        "lakebase.sync_enabled": "true",
        "lakebase.sync_direction": "bidirectional",
        "delta.enableChangeDataFeed": "true",
    },
)
def service_incident_ops():
    return dlt.read("stg_service_incident_ops")
