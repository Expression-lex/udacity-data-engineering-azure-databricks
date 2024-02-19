# Databricks notebook source
SILVER_TABLE = "divvy.dim_riders"

spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE};")


# COMMAND ----------

df = spark.table("divvy.bronze_riders")

df = df.withColumn("ride_id", df.rider_id.cast("int"))
df = df.withColumn("birthday", df.birthday.cast("date"))
df = df.withColumn("account_start_date", df.account_start_date.cast("date"))
df = df.withColumn("account_end_date", df.account_end_date.cast("date"))
df = df.withColumn("is_member", df.is_member.cast("boolean"))

df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
