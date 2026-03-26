# Databricks notebook source
# MAGIC %md
# MAGIC # Metadata-Driven Ingestion Framework
# MAGIC
# MAGIC This notebook implements a dynamic ingestion process based on metadata.
# MAGIC
# MAGIC For each active source defined in the metadata table:
# MAGIC - The source is read dynamically based on its format (CSV or JSON)
# MAGIC - The data is ingested into the Raw layer in ADLS
# MAGIC
# MAGIC This approach avoids hardcoding logic and enables scalable onboarding of new sources.

# COMMAND ----------

# MAGIC %run ./00_setup_storage

# COMMAND ----------

# DBTITLE 1,Metada Load
metadata_path = "abfss://metadata@armandoingestionpoc.dfs.core.windows.net/ingestion_config"

metadata_df = spark.read.format("delta").load(metadata_path)

display(metadata_df)

# COMMAND ----------

# DBTITLE 1,writing source data in raw data storage
metadata_rows = metadata_df.collect()

for row in metadata_rows:
    
    source_name = row["source_name"]
    file_format = row["file_format"]
    source_path = row["source_path"]
    raw_target_path = row["raw_target_path"]

    print(f"Processing {source_name}...")
    
    # Dynamic read
    if file_format == "csv":
        df = spark.read.option("header", True).csv(source_path)
    elif file_format == "json":
        df = spark.read.json(source_path)
    else:
        raise Exception(f"Unsupported format: {file_format}")
    
    # Write to raw
    df.write.mode("overwrite").parquet(raw_target_path)
    
    print(f"{source_name} ingested successfully.")