# Databricks notebook source
# MAGIC %md
# MAGIC # Storage Setup
# MAGIC
# MAGIC This notebook configures access from Databricks to Azure Data Lake Storage (ADLS Gen2).
# MAGIC
# MAGIC It sets the required Spark configuration to enable reading and writing data using the `abfss://` protocol.
# MAGIC
# MAGIC This notebook is reused across the project to centralize storage access configuration.

# COMMAND ----------

storage_account_name = "armandoingestionpoc"

storage_account_key = dbutils.secrets.get(
    scope="adls-scope",
    key="storage-account-key"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)