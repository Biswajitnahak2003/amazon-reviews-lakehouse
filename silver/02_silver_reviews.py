# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
spark

# COMMAND ----------

from pyspark.sql import functions as F

bronze_reviews_table = "amazon_lakehouse.bronze.reviews"
bronze_reviews = spark.table(bronze_reviews_table)


# COMMAND ----------

display(bronze_reviews.limit(10))

# COMMAND ----------

bronze_reviews.printSchema()

# COMMAND ----------

bronze_reviews_without_images = bronze_reviews.select(
    "asin",
    "parent_asin",
    "user_id",
    "rating",
    "text",
    "title",
    "timestamp",
    "verified_purchase",
    "helpful_vote"
)

# COMMAND ----------

initial_count = bronze_reviews_without_images.count()
print(f"we have {initial_count}no. of data")

# COMMAND ----------

bronze_reviews_without_images\
.selectExpr(
    "asin",
    "parent_asin",
    "user_id",
    "rating",
    "text",
    "title",
    "from_unixtime(timestamp / 1000) AS review_timestamp",
    "CAST(from_unixtime(timestamp / 1000) AS DATE) AS review_date",
    "verified_purchase",
    "helpful_vote"
)\
.createOrReplaceTempView("bronze_reviews_without_images")

# COMMAND ----------

bronze_reviews_without_images_null_counts = spark.sql(
    """
    select
        count_if(asin is null)as null_asin,
        count_if(parent_asin is null)as null_parent_asin,
        count_if(user_id is null)as null_user_id,
        count_if(rating is null)as null_rating,
        count_if(text is null)as null_text,
        count_if(title is null)as null_title,
        count_if(review_timestamp is null)as null_review_timestamp,
        count_if(review_date is null)as null_review_date,
        count_if(verified_purchase is null)as null_verified_purchase,
        count_if(helpful_vote is null)as null_helpful_vote
    from bronze_reviews_without_images
    """
)
display(bronze_reviews_without_images_null_counts)

# COMMAND ----------

validated_ratings = spark.sql(
    """
    select
        rating,
        count(*) as rating_count
    from bronze_reviews_without_images
    group by rating
    order by rating"""
)
display(validated_ratings)

# COMMAND ----------

# DBTITLE 1,Check helpful_vote for negative values
helpful_vote_negative = spark.sql(
    """
    select
        helpful_vote,
        count(*) as count
    from bronze_reviews_without_images
    where helpful_vote < 0
    group by helpful_vote
    order by helpful_vote
    """
)
display(helpful_vote_negative)

# COMMAND ----------

# DBTITLE 1,Handle helpful_vote: Convert negative to 0
# Since only 1 record has helpful_vote = -1, converting it to 0
reviews_cleaned = spark.sql(
    """
    SELECT
        asin,
        parent_asin,
        user_id,
        rating,
        text,
        title,
        review_timestamp,
        review_date,
        verified_purchase,
        CASE 
            WHEN helpful_vote < 0 THEN 0 
            ELSE helpful_vote 
        END AS helpful_vote
    FROM bronze_reviews_without_images
    """
)

reviews_cleaned.createOrReplaceTempView("reviews_cleaned")
print(f"Total records after helpful_vote handling: {reviews_cleaned.count()}")

# COMMAND ----------

# DBTITLE 1,Step 1: Remove exact duplicates using DISTINCT
reviews_distinct = spark.sql(
    """
    SELECT DISTINCT
        asin,
        parent_asin,
        user_id,
        rating,
        text,
        title,
        review_timestamp,
        review_date,
        verified_purchase,
        helpful_vote
    FROM reviews_cleaned
    """
)

reviews_distinct.createOrReplaceTempView("reviews_distinct")

initial_count = spark.sql("SELECT COUNT(*) as count FROM reviews_cleaned").collect()[0]['count']
distinct_count = reviews_distinct.count()
exact_duplicates_removed = initial_count - distinct_count

print(f"Initial count: {initial_count:,}")
print(f"After removing exact duplicates: {distinct_count:,}")
print(f"Exact duplicates removed: {exact_duplicates_removed:,}")

# COMMAND ----------

# DBTITLE 1,Step 2: Handle duplicates with window function
from pyspark.sql.window import Window

window_spec = Window.partitionBy(
    "user_id", "parent_asin", "rating", "text", "title", "verified_purchase", "helpful_vote"
).orderBy("review_timestamp")

reviews_with_row_num = spark.sql("""
    SELECT *
    FROM reviews_distinct
""").withColumn("row_num", F.row_number().over(window_spec))

reviews_deduped = reviews_with_row_num.filter(F.col("row_num") == 1).drop("row_num")

reviews_deduped.createOrReplaceTempView("reviews_deduped")

after_window_count = reviews_deduped.count()
window_duplicates_removed = distinct_count - after_window_count

print(f"After distinct: {distinct_count:,}")
print(f"After window function deduplication: {after_window_count:,}")
print(f"Duplicates removed by window function: {window_duplicates_removed:,}")

# COMMAND ----------

# DBTITLE 1,Step 3: Final quality check and summary
final_duplicate_check = spark.sql(
    """
    SELECT 
        user_id,
        parent_asin,
        rating,
        text,
        title,
        COUNT(*) as duplicate_count
    FROM reviews_deduped
    GROUP BY user_id, parent_asin, rating, text, title
    HAVING COUNT(*) > 1
    """
)

dup_count = final_duplicate_check.count()
print(f"\n=== Deduplication Summary ===")
print(f"Records remaining with duplicates: {dup_count}")

if dup_count == 0:
    print("✓ No duplicates found - data is clean!")
else:
    print(f" {dup_count} duplicate groups still exist")
    display(final_duplicate_check.limit(10))

final_count = reviews_deduped.count()
total_removed = initial_count - final_count
removal_percentage = (total_removed / initial_count) * 100

print(f"\n=== Final Statistics ===")
print(f"Original records: {initial_count:,}")
print(f"Final records: {final_count:,}")
print(f"Total duplicates removed: {total_removed:,} ({removal_percentage:.2f}%)")

# COMMAND ----------

# DBTITLE 1,Save to Silver Delta Table
silver_table_name = "amazon_lakehouse.silver.reviews"

print(f"Saving {final_count:,} records to {silver_table_name}...")

reviews_deduped.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_table_name)

print(f"✓ Successfully saved to {silver_table_name}")

silver_verify = spark.table(silver_table_name)
verify_count = silver_verify.count()

print(f"\n=== Verification ===")
print(f"Records in silver table: {verify_count:,}")
print(f"Match with deduped count: {verify_count == final_count}")

print("\nSample of silver data:")
display(silver_verify.limit(5))