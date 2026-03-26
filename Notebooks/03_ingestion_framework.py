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


# Ingestion strategy:
# The Raw layer stores data exactly as it comes from the source.
# No transformation is applied here — files are copied as-is to preserve original format.

metadata_rows = metadata_df.collect()

for row in metadata_rows:
    
    source_name = row["source_name"]
    source_path = row["source_path"]
    raw_target_path = row["raw_target_path"]

    print(f"Copying {source_name} to Raw layer...")
    
    dbutils.fs.rm(raw_target_path, recurse=True)
    dbutils.fs.cp(source_path, raw_target_path, recurse=True)

    print(f"{source_name} copied to Raw successfully.")