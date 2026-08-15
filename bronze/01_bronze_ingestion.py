# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark

# COMMAND ----------

reviews_path = "/Volumes/amazon_lakehouse/raw/amazon_files/reviews/Video_Games.jsonl.gz"
metadata_path = "/Volumes/amazon_lakehouse/raw/amazon_files/metadata/meta_Video_Games.jsonl.gz"

# COMMAND ----------

from pyspark.sql import functions as F

reviews_bronze = (
    spark.read.json(reviews_path)
    .withColumn("_source_file", F.lit("Video_Games.jsonl.gz"))
    .withColumn("_ingested_at", F.current_timestamp())
)

reviews_bronze.printSchema()

# COMMAND ----------

reviews_bronze.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("amazon_lakehouse.bronze.reviews")

# COMMAND ----------

spark.table("amazon_lakehouse.bronze.reviews").show(5)

# COMMAND ----------

spark.table("amazon_lakehouse.bronze.reviews").count()

# COMMAND ----------

metadata_raw = spark.read.text(metadata_path)

# COMMAND ----------

metadata_bronze = (
    metadata_raw
    .withColumnRenamed("value", "raw_json")
    .withColumn("_source_file", F.lit("meta_Video_Games.jsonl.gz"))
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

metadata_bronze.printSchema()

# COMMAND ----------

(
    metadata_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("amazon_lakehouse.bronze.product_metadata_raw")
)

# COMMAND ----------

spark.table(
    "amazon_lakehouse.bronze.product_metadata_raw"
).show(2, truncate=False)