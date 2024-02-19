# Databricks notebook source
from gold import *


GOLD_TABLE = "divvy.gold_payments_per_date"

spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE};")

df = spark.table("divvy.fact_payments")
date_df = spark.table("divvy.dim_date")

df = df.join(date_df, df.date == date_df.date, "inner")


# COMMAND ----------

df = (
    df.groupby(df.year, df.quarter_of_year, df.month)
    .agg(
        F.avg(df.amount).alias("avg_amount"),
        F.sum(df.amount).alias("sum_amount"),
    )
    .orderBy("year", "quarter_of_year", "month")
    .select(
        F.col("year"),
        F.col("quarter_of_year"),
        F.col("month"),
        F.col("avg_amount"),
        F.col("sum_amount"),
    )
)

df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)
