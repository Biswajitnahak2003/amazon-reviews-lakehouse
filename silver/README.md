# Silver Layer

The Silver layer cleans, standardizes, validates, and deduplicates the Bronze review data before producing query-ready Delta tables for downstream analytics.

> **Notebook:** [`02_silver_reviews`](https://github.com/Biswajitnahak2003/amazon-reviews-lakehouse/blob/main/silver/02_silver_reviews.py)

```text
┌──────────────────────────┐
│  Bronze Delta Table      │
│  amazon_lakehouse.       │
│  bronze.reviews          │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│  Silver Review Processing                    │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │ Preserve nested images                 │  │
│  │ for separate image processing          │  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │ Parse timestamp                        │  │
│  │ epoch milliseconds → TIMESTAMP + DATE  │  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │ Validate review data                   │  │
│  │ nulls, ratings, helpful_vote           │  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │ Normalize negative helpful_vote        │  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │ Remove exact duplicate reviews         │  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │ Generate deterministic review_id       │  │
│  └──────────────────┬─────────────────────┘  │
│                     │                        │
│          ┌──────────┴──────────┐             │
│          ▼                     ▼             │
│  ┌──────────────────┐  ┌──────────────────┐ │
│  │ silver.reviews   │  │ review_images    │ │
│  │                  │  │ explode + clean  │ │
│  └──────────────────┘  └────────┬─────────┘ │
│                                 ▼           │
│                       ┌──────────────────┐  │
│                       │ silver.review_   │  │
│                       │ images           │  │
│                       └──────────────────┘  │
└──────────────────────────────────────────────┘
```

---

## Input

| Table | Description |
|---|---|
| `amazon_lakehouse.bronze.reviews` | Bronze review records containing review attributes and nested image data |

---

## Review Processing

### 1. Preserve Nested Images

Unlike the initial approach, the `images` column is **not dropped** during the review-cleaning stage.

It is carried through the Silver review pipeline so that image records can be derived from the same deduplicated review records later.

This prevents the image branch from accidentally going back to the raw Bronze dataset after review deduplication.

```python
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
```

---

### 2. Parse Timestamps

The source stores review timestamps as Unix epoch milliseconds.

They are converted into:

- `review_timestamp` — Spark `TIMESTAMP`
- `review_date` — Spark `DATE`

```sql
CAST(from_unixtime(timestamp / 1000) AS TIMESTAMP) AS review_timestamp,
CAST(from_unixtime(timestamp / 1000) AS DATE) AS review_date
```

Keeping both representations makes the data convenient for timestamp-level analysis as well as date-based aggregations.

---

### 3. Validate Review Columns

The review dataset was profiled before applying transformations.

Checks included:

- NULL counts for review columns
- Rating distribution
- Negative `helpful_vote` values
- Review identity collisions after exact deduplication

The NULL checks showed no missing values in the selected review fields.

The rating distribution contained only valid values from **1 through 5**.

---

### 4. Handle Negative `helpful_vote`

One review contained a `helpful_vote` value of `-1`.

Because `helpful_vote` represents a count, negative values are not meaningful as counts. For this project, the value is normalized to `0`.

```python
reviews_working = reviews_working.withColumn(
    "helpful_vote",
    F.when(F.col("helpful_vote") < 0, 0)
     .otherwise(F.col("helpful_vote"))
)
```

This is a project-level data-cleaning assumption and is documented rather than silently treating the source value as valid.

---

### 5. Exact Duplicate Removal

The source contains repeated copies of identical review records.

The pipeline removes exact duplicates using the review attributes together with the nested `images` field:

```python
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
```

### Result

| Metric | Count |
|---|---:|
| Initial Bronze reviews | 4,624,615 |
| Reviews after exact deduplication | 4,570,969 |
| Exact duplicate records removed | 53,646 |

A previous window-based deduplication approach was investigated during development but was not retained as the primary deduplication rule because matching several review attributes does not necessarily prove that two records are duplicates.

The final pipeline therefore uses **exact duplicate removal** as the defensible Silver-layer rule.

---

### 6. Generate Deterministic `review_id`

The source does not provide an explicit review identifier.

After exact deduplication, the combination of:

```text
asin + user_id + review_timestamp
```

was checked for duplicate combinations. No duplicate groups were found in the deduplicated review data.

A deterministic SHA-256 hash is therefore generated from these fields:

```python
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
```

This provides a reproducible identifier instead of assigning an order-dependent serial number.

---

# Image Processing

The nested `images` array is processed **after review-level cleaning and deduplication**.

This preserves the relationship:

```text
One review
   │
   ├── image 0
   ├── image 1
   └── image 2
```

### 1. Select Reviews Containing Images

```python
reviews_with_images = reviews_with_id.filter(
    F.size("images") > 0
)
```

After review deduplication, **202,241 reviews contained image data**.

---

### 2. Preserve Image Position with `posexplode`

Instead of using only `explode`, `posexplode` is used so that the original position of each image is retained:

```python
reviews_images_exploded = reviews_with_images.select(
    "review_id",
    F.posexplode("images").alias("image_index", "image")
)
```

This produces:

```text
review_id | image_index | image
----------|-------------|-------
R1        | 0           | {...}
R1        | 1           | {...}
R1        | 2           | {...}
```

`image_index` allows each image to remain distinguishable within its parent review.

---

### 3. Flatten the Image Struct

The nested image struct is converted into columns:

```python
silver_review_images = reviews_images_exploded.select(
    "review_id",
    "image_index",
    F.col("image.attachment_type").alias("attachment_type"),
    F.col("image.large_image_url").alias("large_image_url"),
    F.col("image.medium_image_url").alias("medium_image_url"),
    F.col("image.small_image_url").alias("small_image_url")
)
```

The resulting structure is:

| Column | Purpose |
|---|---|
| `review_id` | Parent review identifier |
| `image_index` | Position of the image within the review |
| `attachment_type` | Image attachment type |
| `large_image_url` | Large image URL |
| `medium_image_url` | Medium image URL |
| `small_image_url` | Small image URL |

---

### 4. Image Data Quality Checks

The image data was profiled before being written to Silver.

Checks included:

- NULL image URLs
- Empty image URLs
- Attachment type distribution
- URL format
- Duplicate image records

Results:

- No NULL image URLs were found.
- No empty image URLs were found.
- The dataset contained only the `IMAGE` attachment type.
- Image URLs passed the HTTPS format check.
- Duplicate image combinations were checked after the review-level deduplication.

The pipeline does not download the images. The URLs are retained as source references.

---

# Output Tables

### `amazon_lakehouse.silver.reviews`

Cleaned review-level Delta table containing:

```text
review_id
asin
parent_asin
user_id
rating
text
title
review_timestamp
review_date
verified_purchase
helpful_vote
```

### `amazon_lakehouse.silver.review_images`

Child Delta table containing:

```text
review_id
image_index
attachment_type
large_image_url
medium_image_url
small_image_url
```

The relationship is:

```text
silver.reviews
      │
      │ 1
      │
      └──────────────< silver.review_images
                           many
```

This keeps review attributes and image attributes separated while preserving their relationship through `review_id`.

---

## Design Decisions

### Why not keep images inside `silver.reviews`?

The Bronze source contains images as a nested array. Keeping that structure would make downstream SQL analysis more difficult.

Separating the images creates a cleaner parent-child model:

```text
reviews
  1
  │
  └── many review_images
```

This also allows image-specific transformations without repeatedly processing the entire review record.

### Why `posexplode` instead of `explode`?

`explode` creates one row per array element, but `posexplode` additionally preserves the element's original position.

That position becomes `image_index`, giving us a stable way to distinguish multiple images belonging to the same review.

### Why SHA-256 instead of a serial ID?

A serial ID depends on processing order. A deterministic hash produces the same identifier when the same identity attributes are processed again, making it more appropriate for a reproducible data pipeline.

---

## Important Development Findings

Several issues were discovered while developing this notebook:

1. The source contained **53,646 exact duplicate review records**.
2. A single `helpful_vote = -1` value was found and normalized to `0`.
3. The nested `images` data should not be processed directly from Bronze after review deduplication; doing so would reintroduce duplicate image records from duplicate source reviews.
4. `images` is therefore preserved during review cleaning and transformed into `silver.review_images` only after review-level deduplication.
5. The source does not provide an explicit review ID, so a deterministic `review_id` is generated.

---

## Next Layer

The cleaned Silver tables provide the foundation for the Gold layer, where review, product, rating, helpful-vote, temporal, and image-related data can be aggregated into business-ready analytical datasets.
