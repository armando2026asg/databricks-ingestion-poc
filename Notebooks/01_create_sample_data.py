# Databricks notebook source
# MAGIC %md
# MAGIC # Create Sample Source Data
# MAGIC
# MAGIC This notebook generates sample datasets to simulate source systems.
# MAGIC
# MAGIC The data is written to the `landing` container in ADLS, representing incoming data from external sources.
# MAGIC
# MAGIC Sources created:
# MAGIC - Customers (CSV format)
# MAGIC - Orders (JSON format)

# COMMAND ----------

# MAGIC
# MAGIC %run ./00_setup_storage

# COMMAND ----------

# DBTITLE 1,Setting up source origin data
#the first section is to set up the data
customers_data = [
    (1, "Alice", "Spain"),
    (2, "Bob", "France"),
    (3, "Carla", "Germany")
]

customers_columns = ["customer_id", "customer_name", "country"]

orders_data = [
    (1001, 1, "Laptop", 1200.0),
    (1002, 2, "Mouse", 25.5),
    (1003, 1, "Keyboard", 80.0)
]

orders_columns = ["order_id", "customer_id", "product", "amount"]


customers_df = spark.createDataFrame(customers_data, customers_columns)
orders_df = spark.createDataFrame(orders_data, orders_columns)

# COMMAND ----------

# DBTITLE 1,writing to the container
landing_base = "abfss://landing@armandoingestionpoc.dfs.core.windows.net"

customers_df.write.mode("overwrite").option("header", True).csv(f"{landing_base}/customers")
orders_df.write.mode("overwrite").json(f"{landing_base}/orders")

# COMMAND ----------

# DBTITLE 1,validation of results
display(dbutils.fs.ls(f"{landing_base}/customers"))
display(dbutils.fs.ls(f"{landing_base}/orders"))