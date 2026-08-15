# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark

# COMMAND ----------

reviews_path = "/Volumes/amazon_lakehouse/raw/amazon_files/reviews/Video_Games.jsonl.gz"

reviews_df = spark.read.json(reviews_path)

reviews_df.printSchema()

# COMMAND ----------

reviews_df.show(5, truncate=False)

# COMMAND ----------

metadata_path = "/Volumes/amazon_lakehouse/raw/amazon_files/metadata/meta_Video_Games.jsonl.gz"

metadata_df = spark.read.json(metadata_path)

metadata_df.printSchema()

# COMMAND ----------

metadata_df.show(3, truncate=False)

# COMMAND ----------

metadata_raw = spark.read.text(metadata_path)

metadata_raw.show(2, truncate=False)

# COMMAND ----------

