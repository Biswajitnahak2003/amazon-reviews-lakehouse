# Silver Layer

The Silver layer cleans, standardizes, and deduplicates Bronze data — producing reliable, query-ready Delta tables for downstream analytics.

> **Notebook:** [`02_silver_reviews`](https://github.com/Biswajitnahak2003/amazon-reviews-lakehouse/blob/main/silver/02_silver_reviews.py)

```
┌──────────────────────────┐
│  Bronze Delta Table      │
│  (amazon_lakehouse.      │
│   bronze.reviews)        │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│  Silver Layer                                │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │  Drop images (handled separately)      │  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │  Parse timestamps                      │  │
│  │  (epoch → datetime/date)               │  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │  Validate columns                      │  │
│  │  (nulls, ratings, negative helpful_vote)│  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │  Deduplicate                           │  │
│  │  (DISTINCT + window function)          │  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │  silver.reviews (Delta)                │  │
│  └────────────────────────────────────────┘  │
│                                              │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
              Gold Layer
```

---

## Input Tables

| Table | Description |
|-------|-------------|
| `amazon_lakehouse.bronze.reviews` | Structured review records with nested images |

---

## Reviews Processing

### 1. Drop Images

The `images` column is excluded from this notebook — it will be processed separately later:

```python
bronze_reviews_without_images = bronze_reviews.select(
    "asin", "parent_asin", "user_id", "rating",
    "text", "title", "timestamp",
    "verified_purchase", "helpful_vote"
)
```

### 2. Parse Timestamps

Convert epoch milliseconds to readable timestamp and date:

```sql
from_unixtime(timestamp / 1000) AS review_timestamp,
CAST(from_unixtime(timestamp / 1000) AS DATE) AS review_date
```

### 3. Validate Columns

Check every column for nulls using `count_if` and validate rating distribution.

### 4. Handle Negative `helpful_vote`

Records with negative values (e.g., `-1`) are capped at `0`:

```sql
CASE WHEN helpful_vote < 0 THEN 0 ELSE helpful_vote END AS helpful_vote
```

### 5. Deduplicate

Two-step deduplication:

1. **`DISTINCT`** — removes exact duplicate rows
2. **Window function (`row_number`)** — keeps the earliest record per user + product + review content

```python
window_spec = Window.partitionBy(
    "user_id", "parent_asin", "rating", "text", "title",
    "verified_purchase", "helpful_vote"
).orderBy("review_timestamp")

reviews_deduped = reviews_with_row_num.filter(F.col("row_num") == 1).drop("row_num")
```

### 6. Save to Silver

Write cleaned data as a Delta table with `overwrite` mode:

```python
reviews_deduped.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("amazon_lakehouse.silver.reviews")
```

---

## Output Tables

| Table | Description |
|-------|-------------|
| `amazon_lakehouse.silver.reviews` | Cleaned, deduplicated reviews with parsed timestamps |

All lineage columns (`_source_file`, `_ingested_at`) from Bronze are preserved.

---

## Product Metadata

Processing for product metadata will follow a similar pattern — normalizing the variable `details` field from `bronze.product_metadata_raw`.
