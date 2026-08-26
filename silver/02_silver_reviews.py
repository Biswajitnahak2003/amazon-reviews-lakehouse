# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

bronze_reviews_table = "amazon_lakehouse.bronze.reviews"

bronze_reviews = spark.table(bronze_reviews_table)

# COMMAND ----------

# display(bronze_reviews.limit(10))

# COMMAND ----------

# bronze_reviews.printSchema()

# COMMAND ----------

reviews_working = bronze_reviews.select(
    "asin",
    "parent_asin",
    "user_id",
    "rating",
    "text",
    "title",
    "timestamp",
    "verified_purchase",
    "helpful_vote",
    "images"
)

# COMMAND ----------

initial_count = reviews_working.count()

print(f"Initial review count: {initial_count}")

# COMMAND ----------

reviews_working = reviews_working.selectExpr(
    "asin",
    "parent_asin",
    "user_id",
    "rating",
    "text",
    "title",
    "CAST(from_unixtime(timestamp / 1000) AS TIMESTAMP) AS review_timestamp",
    "CAST(from_unixtime(timestamp / 1000) AS DATE) AS review_date",
    "verified_purchase",
    "helpful_vote",
    "images"
)

# COMMAND ----------

reviews_working.printSchema()

# COMMAND ----------

reviews_working.createOrReplaceTempView("reviews_working")

# COMMAND ----------

reviews_null_counts = spark.sql("""
    SELECT
        COUNT_IF(asin IS NULL) AS null_asin,
        COUNT_IF(parent_asin IS NULL) AS null_parent_asin,
        COUNT_IF(user_id IS NULL) AS null_user_id,
        COUNT_IF(rating IS NULL) AS null_rating,
        COUNT_IF(text IS NULL) AS null_text,
        COUNT_IF(title IS NULL) AS null_title,
        COUNT_IF(review_timestamp IS NULL) AS null_review_timestamp,
        COUNT_IF(review_date IS NULL) AS null_review_date,
        COUNT_IF(verified_purchase IS NULL) AS null_verified_purchase,
        COUNT_IF(helpful_vote IS NULL) AS null_helpful_vote
    FROM reviews_working
""")

display(reviews_null_counts)

# COMMAND ----------

rating_counts = spark.sql("""
    SELECT
        rating,
        COUNT(*) AS rating_count
    FROM reviews_working
    GROUP BY rating
    ORDER BY rating
""")

display(rating_counts)

# COMMAND ----------

invalid_helpful_votes = spark.sql("""
    SELECT
        helpful_vote,
        COUNT(*) AS count
    FROM reviews_working
    WHERE helpful_vote < 0
    GROUP BY helpful_vote
    ORDER BY helpful_vote
""")

display(invalid_helpful_votes)

# COMMAND ----------

reviews_working = reviews_working.withColumn(
    "helpful_vote",
    F.when(F.col("helpful_vote") < 0, 0)
     .otherwise(F.col("helpful_vote"))
)

# COMMAND ----------

before_dedup_count = reviews_working.count()

print(f"Before exact deduplication: {before_dedup_count}")

# COMMAND ----------

reviews_deduped = reviews_working.dropDuplicates([
    "asin",
    "parent_asin",
    "user_id",
    "rating",
    "text",
    "title",
    "review_timestamp",
    "review_date",
    "verified_purchase",
    "helpful_vote",
    "images"
])

# COMMAND ----------

after_dedup_count = reviews_deduped.count()

exact_duplicates_removed = before_dedup_count - after_dedup_count

print(f"Before exact deduplication: {before_dedup_count}")
print(f"After exact deduplication: {after_dedup_count}")
print(f"Exact duplicates removed: {exact_duplicates_removed}")

# COMMAND ----------

reviews_deduped.createOrReplaceTempView("reviews_deduped")

# COMMAND ----------

review_identity_check = spark.sql("""
    SELECT
        asin,
        user_id,
        review_timestamp,
        COUNT(*) AS occurrence_count
    FROM reviews_deduped
    GROUP BY
        asin,
        user_id,
        review_timestamp
    HAVING COUNT(*) > 1
    ORDER BY occurrence_count DESC
""")

display(review_identity_check.limit(20))

# COMMAND ----------

reviews_with_id = reviews_deduped.withColumn(
    "review_id",
    F.sha2(
        F.concat_ws(
            "|",
            F.col("asin"),
            F.col("user_id"),
            F.col("review_timestamp")
        ),
        256
    )
)

# COMMAND ----------

duplicate_review_ids = (
    reviews_with_id
    .groupBy("review_id")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_review_ids)

# COMMAND ----------

silver_reviews = reviews_with_id.select(
    "review_id",
    "asin",
    "parent_asin",
    "user_id",
    "rating",
    "text",
    "title",
    "review_timestamp",
    "review_date",
    "verified_purchase",
    "helpful_vote"
)

# COMMAND ----------

reviews_with_images = reviews_with_id.filter(
    F.size("images") > 0
)

# COMMAND ----------

print(f"Reviews with images: {reviews_with_images.count()}")

# COMMAND ----------

reviews_images_exploded = reviews_with_images.select(
    "review_id",
    F.posexplode("images").alias("image_index", "image")
)

# COMMAND ----------

silver_review_images = reviews_images_exploded.select(
    "review_id",
    "image_index",
    F.col("image.attachment_type").alias("attachment_type"),
    F.col("image.large_image_url").alias("large_image_url"),
    F.col("image.medium_image_url").alias("medium_image_url"),
    F.col("image.small_image_url").alias("small_image_url")
)

# COMMAND ----------

image_null_counts = silver_review_images.select(
    F.sum(F.col("large_image_url").isNull().cast("int")).alias("null_large"),
    F.sum(F.col("medium_image_url").isNull().cast("int")).alias("null_medium"),
    F.sum(F.col("small_image_url").isNull().cast("int")).alias("null_small")
)

display(image_null_counts)

# COMMAND ----------

invalid_image_urls = silver_review_images.filter(
    ~F.col("large_image_url").startswith("https://")
    | ~F.col("medium_image_url").startswith("https://")
    | ~F.col("small_image_url").startswith("https://")
)

display(invalid_image_urls.limit(20))

# COMMAND ----------

display(
    silver_review_images
    .groupBy("attachment_type")
    .count()
    .orderBy("attachment_type")
)

# COMMAND ----------

empty_image_urls = silver_review_images.select(
    F.sum((F.trim(F.col("large_image_url")) == "").cast("int")).alias("empty_large"),
    F.sum((F.trim(F.col("medium_image_url")) == "").cast("int")).alias("empty_medium"),
    F.sum((F.trim(F.col("small_image_url")) == "").cast("int")).alias("empty_small")
)

display(empty_image_urls)

# COMMAND ----------

duplicate_images = (
    silver_review_images
    .groupBy(
        "review_id",
        "large_image_url",
        "medium_image_url",
        "small_image_url"
    )
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_images.limit(20))

# COMMAND ----------

silver_reviews.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("amazon_lakehouse.silver.reviews")

# COMMAND ----------

silver_review_images.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("amazon_lakehouse.silver.review_images")

# COMMAND ----------

# spark.table("amazon_lakehouse.silver.reviews").printSchema()

# COMMAND ----------

# display(spark.table("amazon_lakehouse.silver.reviews").limit(5))

# COMMAND ----------

# spark.table("amazon_lakehouse.silver.review_images").printSchema()

# COMMAND ----------

# display(spark.table("amazon_lakehouse.silver.review_images").limit(5))