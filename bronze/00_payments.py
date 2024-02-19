# Databricks notebook source
spark.sql(f"CREATE SCHEMA IF NOT EXISTS divvy")

# COMMAND ----------

BRONZE_TABLE = "divvy.bronze_payments"

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE};")
df = (
    spark.read.format("csv").option("sep", ",").load("/FileStore/tables/payments.csv")
)

# COMMAND ----------

df = df.withColumn("payment_id", df._c0)
df = df.withColumn("date", df._c1)
df = df.withColumn("amount", df._c2)
df = df.withColumn("rider_id", df._c3)

# COMMAND ----------

column_to_drop = ["_c0", "_c1", "_c2", "_c3"]
df = df.drop(*column_to_drop)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)
