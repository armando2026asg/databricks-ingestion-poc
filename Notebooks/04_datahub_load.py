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

def read_source(file_format, path):
    readers = {
        "csv": lambda p: spark.read.option("header", True).csv(p),
        "json": lambda p: spark.read.json(p),
        "parquet": lambda p: spark.read.parquet(p)
    }

    if file_format not in readers:
        raise Exception(f"Unsupported format: {file_format}")

    return readers[file_format](path)

# COMMAND ----------

# Data Hub layer:
# Reads raw data dynamically based on metadata and standardizes it into Delta format.
# This layer applies minimal transformations and adds audit columns.

datahub_base_path = "abfss://raw@armandoingestionpoc.dfs.core.windows.net/datahub"
metadata_rows = metadata_df.collect()

for row in metadata_rows:
    
    source_name = row["source_name"]
    raw_source_path = row["raw_target_path"]
    file_format = row["file_format"]

    print(f"Loading {source_name} into Data Hub...")

    df = read_source(file_format, raw_source_path)

    df = (
        df.withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("source_name", lit(source_name))
    )

    target_path = f"{datahub_base_path}/{source_name}"
    
    dbutils.fs.rm(target_path, recurse=True)
    df.write.format("delta").mode("overwrite").save(target_path)

    print(f"{source_name} loaded successfully.")

# COMMAND ----------

# DBTITLE 1,validation of results
customers_datahub_path = "abfss://raw@armandoingestionpoc.dfs.core.windows.net/datahub/customers"
oders_datahub_path = "abfss://raw@armandoingestionpoc.dfs.core.windows.net/datahub/orders"

orders_dh_df = spark.read.format("delta").load(oders_datahub_path)
customers_dh_df = spark.read.format("delta").load(customers_datahub_path)
display(orders_dh_df)
display(customers_dh_df)
