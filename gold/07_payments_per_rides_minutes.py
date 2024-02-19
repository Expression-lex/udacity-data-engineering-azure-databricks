# Databricks notebook source
from gold import *

GOLD_TABLE = "divvy.gold_payments_per_rides_minutes"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_payments")
date_df = spark.table("divvy.dim_date").select(
    F.col("date"),
    F.col("month"),
    F.col("year"),
)
df = df.join(date_df, df.date == date_df.date, "inner")

# COMMAND ----------

# Refactored code
trips_df = spark.table("divvy.fact_trips")
trips_df = trips_df.join(date_df, trips_df.start_at_date == date_df.date, "inner")
trips_df = (
    trips_df.groupby(trips_df.rider_id, trips_df.month, trips_df.year)
    .agg(
        F.sum(trips_df.time_spent).alias("sum_time_spent"),
    )
    .selectExpr("rider_id AS trips_rider_id", "month AS trips_month", "year AS trips_year", "sum_time_spent")
    .orderBy("rider_id", "year", "month")
)

# COMMAND ----------

df = (
    df.groupby(df.rider_id, df.month, df.year)
    .agg(
        F.sum(df.amount).alias("sum_amount"),
    )
    .orderBy("rider_id", "year", "month")
    .select(
        F.col("rider_id"),
        F.col("month"),
        F.col("year"),
        F.col("sum_amount"),
    )
)
conds = [
    df.rider_id == trips_df.trips_rider_id,
    df.month == trips_df.trips_month,
    df.year == trips_df.trips_year,
]
df = df.join(trips_df, conds, "inner").select(
    F.col("rider_id"),
    F.col("month"),
    F.col("year"),
    F.col("sum_amount"),
    F.col("sum_time_spent"),
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
