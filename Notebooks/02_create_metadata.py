# Databricks notebook source
# MAGIC %md
# MAGIC # Create Metadata Configuration
# MAGIC
# MAGIC This notebook defines the ingestion configuration as a metadata table stored in Delta format.
# MAGIC
# MAGIC The metadata includes:
# MAGIC - Source name
# MAGIC - File format
# MAGIC - Source path (landing)
# MAGIC - Target path (raw)
# MAGIC - Data Hub target
# MAGIC - Activation flag
# MAGIC
# MAGIC This metadata drives the ingestion framework dynamically.

# COMMAND ----------

# MAGIC %run ./00_setup_storage

# COMMAND ----------

# DBTITLE 1,setting up the metadata
metadata_rows = [
    (
        1,
        "customers",
        "csv",
        "abfss://landing@armandoingestionpoc.dfs.core.windows.net/customers",
        "abfss://raw@armandoingestionpoc.dfs.core.windows.net/customers",
        "datahub.customers",
        "true"
    ),
    (
        2,
        "orders",
        "json",
        "abfss://landing@armandoingestionpoc.dfs.core.windows.net/orders",
        "abfss://raw@armandoingestionpoc.dfs.core.windows.net/orders",
        "datahub.orders",
        "true"
    )
]

metadata_columns = [
    "source_id",
    "source_name",
    "file_format",
    "source_path",
    "raw_target_path",
    "datahub_table",
    "is_active"
]

metadata_df = spark.createDataFrame(metadata_rows, metadata_columns)
display(metadata_df)

# COMMAND ----------

# DBTITLE 1,persisting dataframe to delta format in storage
metadata_path = "abfss://metadata@armandoingestionpoc.dfs.core.windows.net/ingestion_config"

metadata_df.write.format("delta").mode("overwrite").save(metadata_path)

# COMMAND ----------

# DBTITLE 1,validation of results
metadata_config_df = spark.read.format("delta").load(metadata_path)
display(metadata_config_df)