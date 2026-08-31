# Silver Layer

The Silver layer cleans, standardizes, validates, and deduplicates the Bronze review and product data before producing query-ready Delta tables for downstream analytics.

> **Notebooks:**
> - [`02_silver_reviews`](https://github.com/Biswajitnahak2003/amazon-reviews-lakehouse/blob/main/silver/02_silver_reviews.py)
> - [`03_silver_products`](https://github.com/Biswajitnahak2003/amazon-reviews-lakehouse/blob/main/silver/03_silver_products.py)

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
| `amazon_lakehouse.bronze.product_metadata_raw` | Bronze product metadata records stored as raw JSON with `_source_file` and `_ingested_at` metadata |

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

## Product Processing

> **Notebook:** [`03_silver_products`](https://github.com/Biswajitnahak2003/amazon-reviews-lakehouse/blob/main/silver/03_silver_products.py)

```text
┌──────────────────────────────┐
│  Bronze Delta Table          │
│  amazon_lakehouse.           │
│  bronze.product_metadata_raw │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│  Silver Product Processing                  │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │ Parse raw_json with explicit schema    │  │
│  │ → product struct → flattened columns   │  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │ Drop bought_together (100% NULL)       │  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │ Clean price                            │  │
│  │ string → double, preserve raw          │  │
│  └──────────────────┬─────────────────────┘  │
│                     ▼                        │
│  ┌────────────────────────────────────────┐  │
│  │ Validate parent_asin uniqueness        │  │
│  └──────────────────┬─────────────────────┘  │
│                     │                        │
│     ┌───────────────┼───────────────┐        │
│     ▼               ▼               ▼        │
│  ┌────────┐  ┌────────────┐  ┌─────────────┐  │
│  │ Images │  │ Videos     │  │ Details     │  │
│  │ posex-  │  │ posex-     │  │ map<string, │  │
│  │ plode   │  │ plode      │  │ string>     │  │
│  └───┬────┘  └─────┬──────┘  └──────┬──────┘  │
│      │             │                │         │
│      ▼             ▼                ▼         │
│  ┌────────┐  ┌────────────┐  ┌─────────────┐  │
│  │silver. │  │silver.     │  │silver.      │  │
│  │product │  │product_    │  │product_     │  │
│  │_images │  │videos      │  │details      │  │
│  └────────┘  └────────────┘  └─────────────┘  │
│                     │                        │
│                     ▼                        │
│              ┌──────────────────┐             │
│              │ silver.products  │             │
│              │ (normal cols)   │             │
│              └──────────────────┘             │
└──────────────────────────────────────────────┘
```

### 1. Define Product JSON Schema and Parse

The Bronze `product_metadata_raw` table stores each product as a raw JSON string. An explicit `StructType` schema is defined rather than relying on schema inference:

```python
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
```

The raw JSON is parsed using `from_json` and the product struct is flattened into individual columns.

---

### 2. Drop `bought_together`

The `bought_together` column was entirely NULL (137,269 out of 137,269 records). It is dropped from the working dataset.

```python
products_core = products_working.drop("bought_together")
```

---

### 3. Clean Price

The source `price` column is a string with mixed formats: plain numbers ("29.99"), prefixed values ("from 19.99"), and non-numeric text.

The cleaning logic:

- Numeric strings are cast directly to `double`
- "from X.XX" values are extracted via regex
- All other non-numeric values are set to `NULL`
- The original string is preserved in `price_raw`

```python
products_core = products_core.withColumn(
    "price_raw", F.col("price")
).withColumn(
    "price",
    F.when(
        F.col("price").rlike(r"^[0-9]+(\.[0-9]+)?$"),
        F.col("price").cast("double")
    ).when(
        F.col("price").rlike(r"^from [0-9]+(\.[0-9]+)?$"),
        F.regexp_extract(
            F.col("price"), r"([0-9]+(\.[0-9]+)?)", 1
        ).cast("double")
    ).otherwise(
        F.lit(None).cast("double")
    )
)
```

After cleaning: price ranges from $0 to $3,499.99 (avg $45.73), with 75,277 NULLs for products without a listed price.

---

### 4. Null Handling

Missing values in `main_category` (11,035), `store` (4,361), and `title` (9) are **left as NULL**.

Replacing NULLs with invented values like "Unknown" was considered and explicitly rejected — NULL accurately represents that the source did not provide the information.

---

### 5. Validate `parent_asin` Uniqueness

Before writing to Delta, an explicit deduplication check is run:

```python
duplicate_count = products_core.count() - products_core.select("parent_asin").distinct().count()
print(f"Duplicate parent_asin records: {duplicate_count}")
assert duplicate_count == 0
```

Result: **0 duplicate `parent_asin` records** — all 137,269 products are unique.

---

### 6. Explode Images into `product_images`

Each product can have multiple images. `posexplode` preserves the original array position:

```python
product_images = (
    products_core
    .select("parent_asin", F.posexplode("images").alias("image_idx", "image"))
    .select(
        "parent_asin", "image_idx",
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
```

Entries where all URL fields are NULL are filtered out. This produces **692,576 image records**.

---

### 7. Explode Videos into `product_videos`

Videos are exploded the same way as images:

```python
product_videos = (
    products_core
    .select("parent_asin", F.posexplode("videos").alias("video_idx", "video"))
    .select(
        "parent_asin", "video_idx",
        F.col("video.title").alias("title"),
        F.col("video.url").alias("url")
    )
    .filter(
        F.col("title").isNotNull() | F.col("url").isNotNull()
    )
)
```

This produces **145,608 video records**.

---

### 8. Extract Details into `product_details`

The `details` field in the raw JSON is a JSON object with varying keys per product. Values can be:

- Plain strings (e.g., `"Manufacturer": "Nintendo"`)
- Nested objects (e.g., `"Best Sellers Rank": {"Video Games": 51019}`)

Parsing as `map<string, string>` automatically stringifies nested objects to JSON string representation, so no data is lost:

```python
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
```

Products with empty details `{}` (3,422) are excluded. This produces **1,227,483 detail records**.

Example output:

| parent_asin | detail_key | detail_value |
|---|---|---|
| B00069EVOG | Best Sellers Rank | `{"Video Games":137612,"PC-compatible Games":6707}` |
| B00069EVOG | Manufacturer | Sierra |
| B00069EVOG | Rated | Mature |

---

### 9. Validate Detail Key Uniqueness

After exploding, a duplicate check ensures no `(parent_asin, detail_key)` pair appears more than once:

```python
detail_dupes = (
    product_details
    .groupBy("parent_asin", "detail_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
assert detail_dupes == 0
```

Result: **0 duplicate pairs** — all detail keys are unique per product.

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

### `amazon_lakehouse.silver.products`

Cleaned product-level Delta table containing:

```text
parent_asin
title
main_category
average_rating
rating_number
features
            description
price            (double)
store
categories
_source_file
_ingested_at
price_raw         (original string)
```

Missing values in `main_category`, `store`, and `title` remain NULL.

### `amazon_lakehouse.silver.product_images`

Child Delta table containing:

```text
parent_asin
image_idx
thumb
large
variant
hi_res
```

### `amazon_lakehouse.silver.product_videos`

Child Delta table containing:

```text
parent_asin
video_idx
title
url
```

### `amazon_lakehouse.silver.product_details`

Child Delta table containing:

```text
parent_asin
detail_key
detail_value
```

The relationship is:

```text
silver.products
      │
      │ 1
      │
      ├──────────────< silver.product_images    (many)
      ├──────────────< silver.product_videos     (many)
      └──────────────< silver.product_details    (many)
```

All child tables reference their parent product through `parent_asin`.

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

### Why leave NULLs instead of filling with "Unknown"?

Replacing missing `main_category`, `store`, or `title` with "Unknown" invents a value that does not exist in the source. NULL accurately communicates that the source did not provide the information. Downstream consumers can filter or handle NULLs explicitly rather than treating "Unknown" as a real category or store name.

### Why parse `details` as `map<string, string>`?

The `details` field is a JSON object where values can be plain strings or nested objects. Parsing as `map<string, string>` automatically converts nested objects to their JSON string representation (e.g., `{"Video Games": 137612}` becomes the string `"{"Video Games":137612}"`). This preserves all data in a uniform string format without losing nested values.

### Why explode images, videos, and details into separate tables?

Keeping arrays nested in the products table would make downstream SQL analysis more difficult. Separating them creates a clean parent-child model where each image, video, or detail entry is a single row, allowing efficient filtering, aggregation, and joining without repeatedly processing the parent product record.

---

## Important Development Findings

Several issues were discovered while developing these notebooks:

**Reviews:**

1. The source contained **53,646 exact duplicate review records**.
2. A single `helpful_vote = -1` value was found and normalized to `0`.
3. The nested `images` data should not be processed directly from Bronze after review deduplication; doing so would reintroduce duplicate image records from duplicate source reviews.
4. `images` is therefore preserved during review cleaning and transformed into `silver.review_images` only after review-level deduplication.
5. The source does not provide an explicit review ID, so a deterministic `review_id` is generated.

**Products:**

6. The `bought_together` column was **100% NULL** (137,269 records) and was dropped entirely.
7. The `price` column contained mixed string formats (plain numbers, "from X.XX", non-numeric text). 75,277 products had no parseable price after cleaning.
8. The `details` field contains a JSON object with **varying keys per product** and mixed value types (strings and nested objects). Parsing as `map<string, string>` preserves all values uniformly.
9. `main_category` (11,035), `store` (4,361), and `title` (9) contain NULLs — these are left as-is rather than inventing replacement values.
10. `average_rating` (1-5) and `rating_number` (>= 0) contained **zero invalid values** after validation.
11. `parent_asin` is unique across all 137,269 products — no deduplication was needed.
12. No duplicate `(parent_asin, detail_key)` pairs were found in the exploded details table.

---

## Next Layer

The cleaned Silver tables provide the foundation for the Gold layer, where review, product, rating, helpful-vote, temporal, image, and detail data can be aggregated into business-ready analytical datasets.

| Silver Table | Rows | Purpose |
|---|---:|---|
| `silver.reviews` | 4,570,969 | Cleaned, deduplicated review records |
| `silver.review_images` | — | Review-associated image URLs |
| `silver.products` | 137,269 | Cleaned product metadata |
| `silver.product_images` | 692,576 | Product image URLs |
| `silver.product_videos` | 145,608 | Product video URLs |
| `silver.product_details` | 1,227,483 | Exploded product detail key-value pairs |
