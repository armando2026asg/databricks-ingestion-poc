# Databricks notebook source
# MAGIC %md
# MAGIC # Data Hub Load (Delta Layer)
# MAGIC
# MAGIC This notebook loads data from the Raw layer into the Data Hub layer.
# MAGIC
# MAGIC For each source:
# MAGIC - Data is read from Raw
# MAGIC - Audit columns are added (ingestion timestamp, source name)
# MAGIC - Data is written in Delta format
# MAGIC
# MAGIC The Data Hub provides a structured and reliable layer for downstream analytical consumption.

# COMMAND ----------

# MAGIC %run ./00_setup_storage

# COMMAND ----------

# DBTITLE 1,fetching the metadata 
metadata_path = "abfss://metadata@armandoingestionpoc.dfs.core.windows.net/ingestion_config"

metadata_df = spark.read.format("delta").load(metadata_path)
display(metadata_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# DBTITLE 1,writing the raw data to the datahub
datahub_base_path = "abfss://raw@armandoingestionpoc.dfs.core.windows.net/datahub"

metadata_rows = metadata_df.collect()

for row in metadata_rows:
    source_name = row["source_name"]
    raw_source_path = row["raw_target_path"]
    datahub_table = row["datahub_table"]

    print(f"Loading {source_name} into Data Hub...")

    df = spark.read.parquet(raw_source_path)

    df = (
        df.withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("source_name", lit(source_name))
    )

    target_path = f"{datahub_base_path}/{source_name}"

    df.write.format("delta").mode("overwrite").save(target_path)

    print(f"{source_name} loaded into Data Hub successfully.")

# COMMAND ----------

# DBTITLE 1,validation of results
customers_datahub_path = "abfss://raw@armandoingestionpoc.dfs.core.windows.net/datahub/customers"

customers_dh_df = spark.read.format("delta").load(customers_datahub_path)
display(customers_dh_df)