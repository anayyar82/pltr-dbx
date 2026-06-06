# Databricks notebook source
# MAGIC %md
# MAGIC # Wait for continuous DLT ingest
# MAGIC Use between append feed and MV refresh when the streaming pipeline needs time to merge.

# COMMAND ----------

dbutils.widgets.text("wait_seconds", "90")

import time

wait_seconds = int(dbutils.widgets.get("wait_seconds"))
print(f"Waiting {wait_seconds}s for continuous DLT to ingest appended bronze events…")
time.sleep(wait_seconds)
print("Done — proceed to MV refresh / App board refresh.")
