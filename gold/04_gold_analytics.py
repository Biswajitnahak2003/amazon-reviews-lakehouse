# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Create Gold Schema
# Create the gold schema in Unity Catalog if it does not exist
spark.sql("CREATE SCHEMA IF NOT EXISTS amazon_lakehouse.gold")
print("Schema amazon_lakehouse.gold ready")

# COMMAND ----------

# DBTITLE 1,Load Silver Tables
from pyspark.sql import functions as F

# Load the cleaned Silver tables we need for building Gold
silver_reviews = spark.table("amazon_lakehouse.silver.reviews")
silver_products = spark.table("amazon_lakehouse.silver.products")
print("Silver tables loaded:")
print(f"  reviews: {silver_reviews.count():,} rows")
print(f"  products: {silver_products.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Build dim_product
# dim_product — one row per product with business-relevant columns only
# We drop pipeline metadata (_source_file, _ingested_at, price_raw) and
# array columns (features, description, categories) to keep it clean for analytics

dim_product = silver_products.select(
    "parent_asin",
    "title",
    "main_category",
    "average_rating",
    "rating_number",
    "price",
    "store"
)

# Write to Delta
dim_product.write.mode("overwrite").saveAsTable("amazon_lakehouse.gold.dim_product")
print(f"dim_product written: {dim_product.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Build dim_date
# dim_date — calendar dimension derived from distinct review dates
# Each row is one date with year, month, and day-of-week attributes
# This lets analysts filter or group by time without re-deriving every query

dim_date = (
    silver_reviews
    .select("review_date")
    .distinct()
    .withColumn("year", F.year("review_date"))
    .withColumn("quarter", F.quarter("review_date"))
    .withColumn("month", F.month("review_date"))
    .withColumn("month_name", F.date_format("review_date", "MMMM"))
    .withColumn("day_of_week", F.dayofweek("review_date"))
    .withColumn("day_name", F.date_format("review_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("review_date").isin(1, 7))
    .withColumnRenamed("review_date", "date")
)

# Write to Delta
dim_date.write.mode("overwrite").saveAsTable("amazon_lakehouse.gold.dim_date")
print(f"dim_date written: {dim_date.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Build fact_reviews
# fact_reviews — each review enriched with product context
# Joins silver.reviews with silver.products on parent_asin
# Carries only business-relevant columns (no pipeline metadata)
# This is the central fact table that aggregation tables derive from

fact_reviews = (
    silver_reviews.join(
        silver_products.select("parent_asin", "main_category", "price", "store"),
        on="parent_asin",
        how="left"
    )
    .select(
        "review_id",
        "parent_asin",
        "asin",
        "user_id",
        "rating",
        "text",
        "title",
        "review_timestamp",
        "review_date",
        "verified_purchase",
        "helpful_vote",
        "main_category",
        "price",
        "store"
    )
)

# Write to Delta
fact_reviews.write.mode("overwrite").saveAsTable("amazon_lakehouse.gold.fact_reviews")
print(f"fact_reviews written: {fact_reviews.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Build agg_product_ratings
# agg_product_ratings — product-level aggregation
# One row per product with review count, average rating, helpful votes, and verified %
# Pre-aggregating this saves dashboards from doing GROUP BY at query time

agg_product_ratings = (
    fact_reviews
    .groupBy("parent_asin", "main_category")
    .agg(
        F.count("review_id").alias("total_reviews"),
        F.round(F.avg("rating"), 2).alias("avg_review_rating"),
        F.sum("helpful_vote").alias("total_helpful_votes"),
        F.round(
            F.avg(F.when(F.col("verified_purchase") == True, 1).otherwise(0)) * 100,
            2
        ).alias("verified_purchase_pct")
    )
)

# Write to Delta
agg_product_ratings.write.mode("overwrite").saveAsTable("amazon_lakehouse.gold.agg_product_ratings")
print(f"agg_product_ratings written: {agg_product_ratings.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Build agg_category_monthly
# agg_category_monthly — category-level monthly trends
# One row per (category, year, month) with review count and average rating
# Useful for trend charts showing how ratings evolve over time per category

agg_category_monthly = (
    fact_reviews
    .groupBy(
        "main_category",
        F.year("review_date").alias("year"),
        F.month("review_date").alias("month")
    )
    .agg(
        F.count("review_id").alias("total_reviews"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum("helpful_vote").alias("total_helpful_votes")
    )
    .orderBy("main_category", "year", "month")
)

# Write to Delta
agg_category_monthly.write.mode("overwrite").saveAsTable("amazon_lakehouse.gold.agg_category_monthly")
print(f"agg_category_monthly written: {agg_category_monthly.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Build agg_review_summary
# agg_review_summary — high-level KPIs for executive dashboards
# Single-row table with overall platform metrics
# Designed for headline numbers on a dashboard (total reviews, avg rating, etc.)
# Using Spark SQL for this table to show both PySpark and SQL approaches

spark.sql("""
    CREATE OR REPLACE TABLE amazon_lakehouse.gold.agg_review_summary AS
    SELECT
        COUNT(review_id) AS total_reviews,
        ROUND(AVG(rating), 2) AS overall_avg_rating,
        COUNT(DISTINCT parent_asin) AS total_products_reviewed,
        SUM(helpful_vote) AS total_helpful_votes,
        ROUND(AVG(CASE WHEN verified_purchase = true THEN 1 ELSE 0 END) * 100, 2) AS verified_purchase_pct
    FROM amazon_lakehouse.gold.fact_reviews
""")

row_count = spark.table("amazon_lakehouse.gold.agg_review_summary").count()
print(f"agg_review_summary written: {row_count:,} rows")

# COMMAND ----------

# DBTITLE 1,Validate All Gold Tables
# Validation — print row counts for all Gold tables to confirm everything loaded

gold_tables = [
    "amazon_lakehouse.gold.dim_product",
    "amazon_lakehouse.gold.dim_date",
    "amazon_lakehouse.gold.fact_reviews",
    "amazon_lakehouse.gold.agg_product_ratings",
    "amazon_lakehouse.gold.agg_category_monthly",
    "amazon_lakehouse.gold.agg_review_summary"
]

print("=== Gold Layer Validation ===")
for table in gold_tables:
    count = spark.table(table).count()
    print(f"  {table}: {count:,} rows")
print("=== All Gold tables validated ===")