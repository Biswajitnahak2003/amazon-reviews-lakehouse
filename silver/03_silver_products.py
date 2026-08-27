# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

# COMMAND ----------

bronze_products = spark.table(
    "amazon_lakehouse.bronze.product_metadata_raw"
)

# COMMAND ----------

bronze_products.printSchema()

# COMMAND ----------

display(
    bronze_products.select("raw_json").limit(5)
)

# COMMAND ----------

product_count = bronze_products.count()
print(f"Total product metadata records: {product_count}")

# COMMAND ----------

sample_json = (
    bronze_products
    .select("raw_json")
    .first()["raw_json"]
)

print(sample_json)

# COMMAND ----------

json_schema = F.schema_of_json(
    F.lit(sample_json)
)

# COMMAND ----------

schema_string = (
    bronze_products
    .select(json_schema.alias("schema"))
    .first()["schema"]
)

print(schema_string)

# COMMAND ----------

details_json = bronze_products.select(
    F.get_json_object(
        "raw_json",
        "$.details"
    ).alias("details_json")
)

display(details_json.limit(10))

# COMMAND ----------

details_key_json = bronze_products.select(
    F.json_object_keys(
        F.get_json_object("raw_json", "$.details")
    ).alias("detail_keys")
)

display(details_key_json.limit(20))

# COMMAND ----------

detail_key_counts = (
    details_key_json
    .select(
        F.explode("detail_keys").alias("detail_key")
    )
    .groupBy("detail_key")
    .count()
    .orderBy(F.desc("count"))
)

display(detail_key_counts)

# COMMAND ----------

key_frequency_summary = (
    detail_key_counts
    .select(
        F.when(F.col("count") >= 100000, ">= 100k")
         .when(F.col("count") >= 50000, "50k - 99,999")
         .when(F.col("count") >= 10000, "10k - 49,999")
         .when(F.col("count") >= 1000, "1k - 9,999")
         .when(F.col("count") >= 100, "100 - 999")
         .when(F.col("count") >= 10, "10 - 99")
         .otherwise("< 10")
         .alias("frequency_range")
    )
    .groupBy("frequency_range")
    .count()
    .orderBy(F.desc("count"))
)

display(key_frequency_summary)

# COMMAND ----------

details_missing = bronze_products.filter(
    F.get_json_object(
        "raw_json",
        "$.details"
    ).isNull()
).count()

print(f"Products without details: {details_missing}")

# COMMAND ----------

details_empty = bronze_products.filter(
    F.get_json_object(
        "raw_json",
        "$.details"
    ) == "{}"
).count()

print(f"Products with empty details: {details_empty}")

# COMMAND ----------

from pyspark.sql.types import *

product_schema = StructType([
    StructField("main_category", StringType(), True),
    StructField("title", StringType(), True),
    StructField("average_rating", DoubleType(), True),
    StructField("rating_number", LongType(), True),
    StructField("features", ArrayType(StringType()), True),
    StructField("description", ArrayType(StringType()), True),
    StructField("price", StringType(), True),
    StructField("images", ArrayType(
        StructType([
            StructField("thumb", StringType(), True),
            StructField("large", StringType(), True),
            StructField("variant", StringType(), True),
            StructField("hi_res", StringType(), True)
        ])
    ), True),
    StructField("videos", ArrayType(
        StructType([
            StructField("title", StringType(), True),
            StructField("url", StringType(), True)
        ])
    ), True),
    StructField("store", StringType(), True),
    StructField("categories", ArrayType(StringType()), True),
    StructField("parent_asin", StringType(), True),
    StructField("bought_together", StringType(), True)
])

# COMMAND ----------

products_parsed = bronze_products.withColumn(
    "product",
    F.from_json(
        F.col("raw_json"),
        product_schema
    )
)

# COMMAND ----------

products_parsed = products_parsed.select(
    "product.*",
    "_source_file",
    "_ingested_at"
)

# COMMAND ----------

# products_parsed.printSchema()

# COMMAND ----------

# display(products_parsed.limit(10))

# COMMAND ----------

products_working = products_parsed.select(
    "parent_asin",
    "title",
    "main_category",
    "average_rating",
    "rating_number",
    "features",
    "description",
    "price",
    "store",
    "categories",
    "images",
    "videos",
    "bought_together",
    "_source_file",
    "_ingested_at"
)

# COMMAND ----------

products_working.createOrReplaceTempView(
    "products_working"
)

# COMMAND ----------

products_null_counts = spark.sql("""
    SELECT
        COUNT_IF(parent_asin IS NULL) AS null_parent_asin,
        COUNT_IF(title IS NULL) AS null_title,
        COUNT_IF(main_category IS NULL) AS null_main_category,
        COUNT_IF(average_rating IS NULL) AS null_average_rating,
        COUNT_IF(rating_number IS NULL) AS null_rating_number,
        COUNT_IF(price IS NULL) AS null_price,
        COUNT_IF(store IS NULL) AS null_store,
        COUNT_IF(categories IS NULL) AS null_categories,
        COUNT_IF(features IS NULL) AS null_features,
        COUNT_IF(description IS NULL) AS null_description,
        COUNT_IF(images IS NULL) AS null_images,
        COUNT_IF(videos IS NULL) AS null_videos,
        COUNT_IF(bought_together IS NULL) AS null_bought_together
    FROM products_working
""")

display(products_null_counts)

# COMMAND ----------

products_core = products_working.drop(
    "bought_together"
)

# COMMAND ----------

products_core.createOrReplaceTempView(
    "products_core"
)

# COMMAND ----------

invalid_prices = spark.sql("""
    SELECT price
    FROM products_core
    WHERE price IS NOT NULL
      AND NOT regexp_like(price, '^[0-9]+(\\.[0-9]+)?$')
""")

display(invalid_prices.limit(50))

# COMMAND ----------

products_core = products_core.withColumn(
    "price_raw",
    F.col("price")
).withColumn(
    "price",
    F.when(
        F.col("price").rlike(r"^[0-9]+(\.[0-9]+)?$"),
        F.col("price").cast("double")
    ).when(
        F.col("price").rlike(r"^from [0-9]+(\.[0-9]+)?$"),
        F.regexp_extract(
            F.col("price"),
            r"([0-9]+(\.[0-9]+)?)",
            1
        ).cast("double")
    ).otherwise(
        F.lit(None).cast("double")
    )
)

# COMMAND ----------

