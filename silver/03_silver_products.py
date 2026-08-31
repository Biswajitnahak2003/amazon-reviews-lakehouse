# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Import Spark SQL functions
# Import Spark SQL functions for transformations
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Load bronze product metadata
# Load raw product metadata from the bronze layer
bronze_products = spark.table(
    "amazon_lakehouse.bronze.product_metadata_raw"
)

# COMMAND ----------

# DBTITLE 1,EDA: Print schema (commented out)
# EDA: Print bronze schema (commented out — not needed in production)
# bronze_products.printSchema()

# COMMAND ----------

# DBTITLE 1,EDA: Preview raw JSON (commented out)
# EDA: Preview raw JSON (commented out — not needed in production)
# display(
#     bronze_products.select("raw_json").limit(5)
# )

# COMMAND ----------

# DBTITLE 1,EDA: Count records (commented out)
# EDA: Count total product metadata records (commented out — not needed in production)
# product_count = bronze_products.count()
# print(f"Total product metadata records: {product_count}")

# COMMAND ----------

# DBTITLE 1,EDA: Sample JSON (commented out)
# EDA: Sample raw JSON for schema inference (commented out — not needed in production)
# sample_json = (
#     bronze_products
#     .select("raw_json")
#     .first()["raw_json"]
# )

# print(sample_json)

# COMMAND ----------

# DBTITLE 1,EDA: Infer JSON schema (commented out)
# EDA: Infer JSON schema from sample (commented out — not needed in production)
# json_schema = F.schema_of_json(
#     F.lit(sample_json)
# )

# COMMAND ----------

# DBTITLE 1,EDA: Print schema string (commented out)
# EDA: Print inferred schema string (commented out — not needed in production)
# schema_string = (
#     bronze_products
#     .select(json_schema.alias("schema"))
#     .first()["schema"]
# )

# print(schema_string)

# COMMAND ----------

# DBTITLE 1,EDA: Preview details field (commented out)
# EDA: Preview details field from raw_json (commented out — not needed in production)
# details_json = bronze_products.select(
#     F.get_json_object(
#         "raw_json",
#         "$.details"
#     ).alias("details_json")
# )

# display(details_json.limit(10))

# COMMAND ----------

# DBTITLE 1,EDA: Extract detail keys (commented out)
# EDA: Extract detail keys from raw_json (commented out — not needed in production)
# details_key_json = bronze_products.select(
#     F.json_object_keys(
#         F.get_json_object("raw_json", "$.details")
#     ).alias("detail_keys")
# )

# display(details_key_json.limit(20))

# COMMAND ----------

# DBTITLE 1,EDA: Count detail keys (commented out)
# EDA: Count detail keys by frequency (commented out — not needed in production)
# detail_key_counts = (
#     details_key_json
#     .select(
#         F.explode("detail_keys").alias("detail_key")
#     )
#     .groupBy("detail_key")
#     .count()
#     .orderBy(F.desc("count"))
# )

# display(detail_key_counts)

# COMMAND ----------

# DBTITLE 1,EDA: Key frequency summary (commented out)
# EDA: Summarize key frequency ranges (commented out — not needed in production)
# key_frequency_summary = (
#     detail_key_counts
#     .select(
#         F.when(F.col("count") >= 100000, ">= 100k")
#          .when(F.col("count") >= 50000, "50k - 99,999")
#          .when(F.col("count") >= 10000, "10k - 49,999")
#          .when(F.col("count") >= 1000, "1k - 9,999")
#          .when(F.col("count") >= 100, "100 - 999")
#          .when(F.col("count") >= 10, "10 - 99")
#          .otherwise("< 10")
#          .alias("frequency_range")
#     )
#     .groupBy("frequency_range")
#     .count()
#     .orderBy(F.desc("count"))
# )

# display(key_frequency_summary)

# COMMAND ----------

# DBTITLE 1,EDA: Missing details check (commented out)
# EDA: Count products without details (commented out — not needed in production)
# details_missing = bronze_products.filter(
#     F.get_json_object(
#         "raw_json",
#         "$.details"
#     ).isNull()
# ).count()

# print(f"Products without details: {details_missing}")

# COMMAND ----------

# DBTITLE 1,EDA: Empty details check (commented out)
# EDA: Count products with empty details (commented out — not needed in production)
# details_empty = bronze_products.filter(
#     F.get_json_object(
#         "raw_json",
#         "$.details"
#     ) == "{}"
# ).count()

# print(f"Products with empty details: {details_empty}")

# COMMAND ----------

# DBTITLE 1,Define product JSON schema
# Define the explicit schema for parsing product JSON from the bronze layer
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

# DBTITLE 1,Parse raw JSON into product struct
# Parse raw_json using the explicit product schema into a struct column
products_parsed = bronze_products.withColumn(
    "product",
    F.from_json(
        F.col("raw_json"),
        product_schema
    )
)

# COMMAND ----------

# DBTITLE 1,Flatten product struct columns
# Flatten the product struct into individual columns, keeping metadata columns
products_parsed = products_parsed.select(
    "product.*",
    "_source_file",
    "_ingested_at"
)

# COMMAND ----------

# DBTITLE 1,EDA: Print parsed schema (commented out)
# EDA: Print parsed schema (commented out — not needed in production)
# products_parsed.printSchema()

# COMMAND ----------

# DBTITLE 1,EDA: Preview parsed products (commented out)
# EDA: Preview parsed products (commented out — not needed in production)
# display(products_parsed.limit(10))

# COMMAND ----------

# DBTITLE 1,Select silver working columns
# Select the working set of columns for silver layer transformation
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

# DBTITLE 1,EDA: products_working temp view (commented out)
# --- EDA: products_working temp view (only used for null count checks, not needed in production) ---
# products_working.createOrReplaceTempView(
#     "products_working"
# )

# COMMAND ----------

# DBTITLE 1,EDA: Null count check (commented out)
# --- EDA: Null count check on products_working (not needed in production) ---
# products_null_counts = spark.sql("""
#     SELECT
#         COUNT_IF(parent_asin IS NULL) AS null_parent_asin,
#         COUNT_IF(title IS NULL) AS null_title,
#         COUNT_IF(main_category IS NULL) AS null_main_category,
#         COUNT_IF(average_rating IS NULL) AS null_average_rating,
#         COUNT_IF(rating_number IS NULL) AS null_rating_number,
#         COUNT_IF(price IS NULL) AS null_price,
#         COUNT_IF(store IS NULL) AS null_store,
#         COUNT_IF(categories IS NULL) AS null_categories,
#         COUNT_IF(features IS NULL) AS null_features,
#         COUNT_IF(description IS NULL) AS null_description,
#         COUNT_IF(images IS NULL) AS null_images,
#         COUNT_IF(videos IS NULL) AS null_videos,
#         COUNT_IF(bought_together IS NULL) AS null_bought_together
#     FROM products_working
# """)
#
# display(products_null_counts)

# COMMAND ----------

# DBTITLE 1,Drop bought_together column
# Drop bought_together column (137,269 nulls — not useful for analysis)
products_core = products_working.drop(
    "bought_together"
)

# COMMAND ----------

# DBTITLE 1,Create products_core temp view
# Register products_core as a temp view for SQL-based validation queries
products_core.createOrReplaceTempView(
    "products_core"
)

# COMMAND ----------

# DBTITLE 1,EDA: Check invalid prices (commented out)
# EDA: Check invalid price formats (commented out — not needed in production)
# invalid_prices = spark.sql("""
#     SELECT price
#     FROM products_core
#     WHERE price IS NOT NULL
#       AND NOT regexp_like(price, '^[0-9]+(\\.[0-9]+)?$')
# """)

# display(invalid_prices.limit(50))

# COMMAND ----------

# DBTITLE 1,Clean and cast price to double
# Clean price: preserve raw value in price_raw, cast numeric strings to double,
# handle "from X.XX" format, set non-numeric values to NULL.
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

# DBTITLE 1,EDA: Check invalid ratings (commented out)
# --- EDA: Check invalid ratings outside 1-5 range (not needed in production) ---
# invalid_ratings = spark.sql("""
#     SELECT
#         average_rating,
#         COUNT(*) AS product_count
#     FROM products_core
#     WHERE average_rating < 1
#        OR average_rating > 5
#     GROUP BY average_rating
#     ORDER BY average_rating
# """)
#
# display(invalid_ratings)

# COMMAND ----------

# DBTITLE 1,EDA: Check invalid rating numbers (commented out)
# --- EDA: Check invalid rating numbers below 0 (not needed in production) ---
# invalid_rating_numbers = spark.sql("""
#     SELECT
#         rating_number,
#         COUNT(*) AS product_count
#     FROM products_core
#     WHERE rating_number < 0
#     GROUP BY rating_number
#     ORDER BY rating_number
# """)
#
# display(invalid_rating_numbers)

# COMMAND ----------

# DBTITLE 1,REMOVED: Fill nulls with Unknown
# --- Removed: Do NOT replace missing main_category/store with 'Unknown' ---
# Missing source information should remain NULL — "Unknown" is an invented value.
# products_core = products_core.withColumn(
#     "main_category",
#     F.coalesce(F.col("main_category"), F.lit("Unknown"))
# ).withColumn(
#     "store",
#     F.coalesce(F.col("store"), F.lit("Unknown"))
# )

# COMMAND ----------

# DBTITLE 1,EDA: Re-validate nulls (commented out)
# --- EDA: Re-validate nulls (not needed in production) ---
# products_core.createOrReplaceTempView("products_core")
#
# null_counts_after = spark.sql("""
#     SELECT
#         COUNT_IF(parent_asin IS NULL) AS null_parent_asin,
#         COUNT_IF(title IS NULL) AS null_title,
#         COUNT_IF(main_category IS NULL) AS null_main_category,
#         COUNT_IF(average_rating IS NULL) AS null_average_rating,
#         COUNT_IF(rating_number IS NULL) AS null_rating_number,
#         COUNT_IF(price IS NULL) AS null_price,
#         COUNT_IF(store IS NULL) AS null_store,
#         COUNT_IF(categories IS NULL) AS null_categories,
#         COUNT_IF(features IS NULL) AS null_features,
#         COUNT_IF(description IS NULL) AS null_description,
#         COUNT_IF(images IS NULL) AS null_images,
#         COUNT_IF(videos IS NULL) AS null_videos
#     FROM products_core
# """)
#
# display(null_counts_after)

# COMMAND ----------

# DBTITLE 1,REMOVED: Fill null titles with Unknown
# --- Removed: Do NOT replace missing titles with 'Unknown' ---
# Missing source information should remain NULL.
# products_core = products_core.withColumn(
#     "title",
#     F.coalesce(F.col("title"), F.lit("Unknown"))
# )
# products_core.createOrReplaceTempView("products_core")

# COMMAND ----------

# DBTITLE 1,EDA: Comprehensive validation (commented out)
# --- EDA: Comprehensive validation summary (not needed in production) ---
# validation_summary = spark.sql("""
#     SELECT
#         COUNT(*) AS total_records,
#         COUNT(DISTINCT parent_asin) AS unique_asins,
#         COUNT_IF(parent_asin IS NULL) AS null_parent_asin,
#         COUNT_IF(title IS NULL) AS null_title,
#         COUNT_IF(main_category IS NULL) AS null_main_category,
#         COUNT_IF(average_rating IS NULL) AS null_average_rating,
#         COUNT_IF(rating_number IS NULL) AS null_rating_number,
#         COUNT_IF(price IS NULL) AS null_price,
#         COUNT_IF(store IS NULL) AS null_store,
#         COUNT_IF(average_rating < 1 OR average_rating > 5) AS invalid_ratings,
#         COUNT_IF(rating_number < 0) AS invalid_rating_numbers,
#         COUNT_IF(size(features) = 0) AS empty_features,
#         COUNT_IF(size(description) = 0) AS empty_description,
#         COUNT_IF(size(categories) = 0) AS empty_categories,
#         COUNT_IF(size(images) = 0) AS empty_images,
#         COUNT_IF(size(videos) = 0) AS empty_videos,
#         MIN(price) AS min_price,
#         MAX(price) AS max_price,
#         AVG(price) AS avg_price,
#         MIN(average_rating) AS min_rating,
#         MAX(average_rating) AS max_rating
#     FROM products_core
# """)
#
# display(validation_summary)

# COMMAND ----------

# DBTITLE 1,Explode images into separate table
# --- Explode images array into separate table ---
# Each product can have multiple images; posexplode preserves original array order.
# Filter out entries where all image URL fields are NULL.
product_images = (
    products_core
    .select("parent_asin", F.posexplode("images").alias("image_idx", "image"))
    .select(
        "parent_asin",
        "image_idx",
        F.col("image.thumb").alias("thumb"),
        F.col("image.large").alias("large"),
        F.col("image.variant").alias("variant"),
        F.col("image.hi_res").alias("hi_res")
    )
    .filter(
        F.col("thumb").isNotNull()
        | F.col("large").isNotNull()
        | F.col("variant").isNotNull()
        | F.col("hi_res").isNotNull()
    )
)

# COMMAND ----------

# DBTITLE 1,Explode videos into separate table
# --- Explode videos array into separate table ---
# Each product can have multiple videos; posexplode preserves original array order.
# Filter out entries where both title and URL are NULL.
product_videos = (
    products_core
    .select("parent_asin", F.posexplode("videos").alias("video_idx", "video"))
    .select(
        "parent_asin",
        "video_idx",
        F.col("video.title").alias("title"),
        F.col("video.url").alias("url")
    )
    .filter(
        F.col("title").isNotNull()
        | F.col("url").isNotNull()
    )
)

# COMMAND ----------

# DBTITLE 1,Extract and explode details key-value pairs
# --- Extract details from raw_json and explode into key-value pairs ---
# The "details" field in raw_json is a JSON object with varying keys per product.
# Values can be plain strings or nested objects (e.g. "Best Sellers Rank": {"Video Games": 137612}).
# Parsing as map<string,string> automatically stringifies nested objects to JSON strings.
# Products with empty details "{}" are excluded.
product_details = (
    bronze_products
    .select(
        F.get_json_object("raw_json", "$.parent_asin").alias("parent_asin"),
        F.get_json_object("raw_json", "$.details").alias("details_json")
    )
    .filter(
        F.col("details_json").isNotNull()
        & (F.col("details_json") != "{}")
    )
    .withColumn(
        "details_map",
        F.from_json(F.col("details_json"), "map<string, string>")
    )
    .select(
        "parent_asin",
        F.explode("details_map").alias("detail_key", "detail_value")
    )
)

# COMMAND ----------

# DBTITLE 1,EDA: Verify nested object handling (commented out)
# --- EDA: Verify nested object handling in details (not needed in production) ---
# nested_check = product_details.filter(
#     F.col("detail_key") == "Best Sellers Rank"
# ).limit(5)
#
# display(nested_check)

# COMMAND ----------

# DBTITLE 1,Validate: No duplicate parent_asin
# --- Validate: No duplicate parent_asin before writing to Delta ---
duplicate_count = products_core.count() - products_core.select("parent_asin").distinct().count()
print(f"Duplicate parent_asin records: {duplicate_count}")
assert duplicate_count == 0, f"Found {duplicate_count} duplicate parent_asin records — deduplication required before writing"

# COMMAND ----------

# DBTITLE 1,Validate: No duplicate parent_asin + detail_key
# --- Validate: No duplicate parent_asin + detail_key pairs in product_details ---
detail_dupes = (
    product_details
    .groupBy("parent_asin", "detail_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
print(f"Duplicate (parent_asin, detail_key) pairs: {detail_dupes}")
assert detail_dupes == 0, f"Found {detail_dupes} duplicate (parent_asin, detail_key) pairs"

# COMMAND ----------

# DBTITLE 1,Write products Delta table
# --- Write products table to silver Delta ---
# Drop images and videos columns (they go in separate tables).
# main_category, store, and title remain NULL where the source did not provide them.
products_silver = products_core.drop("images", "videos")

(
    products_silver
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("amazon_lakehouse.silver.products")
)

print("Written: amazon_lakehouse.silver.products")
print(f"Row count: {spark.table('amazon_lakehouse.silver.products').count()}")

# COMMAND ----------

# DBTITLE 1,Write product_images Delta table
# --- Write product_images table to silver Delta ---
(
    product_images
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("amazon_lakehouse.silver.product_images")
)

print("Written: amazon_lakehouse.silver.product_images")
print(f"Row count: {spark.table('amazon_lakehouse.silver.product_images').count()}")

# COMMAND ----------

# DBTITLE 1,Write product_videos Delta table
# --- Write product_videos table to silver Delta ---
(
    product_videos
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("amazon_lakehouse.silver.product_videos")
)

print("Written: amazon_lakehouse.silver.product_videos")
print(f"Row count: {spark.table('amazon_lakehouse.silver.product_videos').count()}")

# COMMAND ----------

# DBTITLE 1,Write product_details Delta table
# --- Write product_details table to silver Delta ---
(
    product_details
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("amazon_lakehouse.silver.product_details")
)

print("Written: amazon_lakehouse.silver.product_details")
print(f"Row count: {spark.table('amazon_lakehouse.silver.product_details').count()}")