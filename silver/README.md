# Silver Layer

The Silver layer cleans, validates, and deduplicates Bronze review and product data into query-ready Delta tables.

> **Notebooks:**
> - [`02_silver_reviews`](https://github.com/Biswajitnahak2003/amazon-reviews-lakehouse/blob/main/silver/02_silver_reviews.py)
> - [`03_silver_products`](https://github.com/Biswajitnahak2003/amazon-reviews-lakehouse/blob/main/silver/03_silver_products.py)

## Architecture

```text
        BRONZE                                    SILVER
┌───────────────────────────┐   ┌──────────────────────────────────────┐
│ bronze.reviews            │   │ REVIEW PIPELINE                      │
│ (review attrs + images)  │──▶│                                      │
└───────────────────────────┘   │ Preserve images → Parse timestamps   │
                                │ → Validate → Normalize helpful_vote   │
                                │ → Deduplicate → Generate review_id   │
                                │    │                                 │
                                │    ├──▶ silver.reviews               │
                                │    └──▶ silver.review_images        │
                                └──────────────────────────────────────┘

┌───────────────────────────┐   ┌──────────────────────────────────────┐
│ bronze.product_metadata_  │   │ PRODUCT PIPELINE                     │
│ raw (raw_json product     │──▶│                                      │
│  metadata)                │   │ Parse JSON → Drop bought_together    │
└───────────────────────────┘   │ → Clean price → Validate parent_asin │
                                │    │                                 │
                                │    ├──▶ silver.products              │
                                │    ├──▶ silver.product_images      │
                                │    ├──▶ silver.product_videos       │
                                │    └──▶ silver.product_details      │
                                └──────────────────────────────────────┘
```

## Input

| Table | Description |
|---|---|
| `bronze.reviews` | Review attributes with nested image data |
| `bronze.product_metadata_raw` | Product metadata as raw JSON strings |

---

## Review Processing

### 1. Preserve Nested Images

The `images` column is carried through the pipeline (not dropped) so image records derive from the same deduplicated reviews later — this prevents re-reading Bronze after deduplication.

### 2. Parse Timestamps

Epoch milliseconds → `review_timestamp` (TIMESTAMP) + `review_date` (DATE) for both timestamp-level and date-based analysis.

### 3. Validate Review Columns

Profiling confirmed: no NULLs in selected fields, ratings 1-5 only, one negative `helpful_vote` found.

### 4. Handle Negative `helpful_vote`

A single `helpful_vote = -1` was normalized to `0`.

### 5. Exact Duplicate Removal

```python
reviews_deduped = reviews_working.dropDuplicates([
    "asin", "parent_asin", "user_id", "rating",
    "text", "title", "review_timestamp", "review_date",
    "verified_purchase", "helpful_vote", "images"
])
```

| Metric | Count |
|---|---:|
| Initial Bronze reviews | 4,624,615 |
| After deduplication | 4,570,969 |
| Duplicates removed | 53,646 |

### 6. Generate Deterministic `review_id`

SHA-256 hash of `asin + user_id + review_timestamp` — no duplicate combinations found, so the hash is safe as a unique identifier.

---

## Review Image Processing

Images are exploded **after** review deduplication to avoid reintroducing duplicates from Bronze. `posexplode` preserves each image's array position as `image_index`.

- Reviews with images: 202,241
- Data quality: No NULLs, no empty URLs, all HTTPS, `IMAGE` type only

---

## Product Processing

### 1. Define Schema and Parse

An explicit `StructType` parses `raw_json` into a product struct, which is flattened into individual columns.

### 2. Drop `bought_together`

100% NULL (137,269 records) — dropped entirely.

### 3. Clean Price

| Source format | Handling |
|---|---|
| Plain number ("29.99") | Cast to double |
| "from X.XX" | Extract number via regex |
| Non-numeric text | Set to NULL |

Original string preserved in `price_raw`. Result: $0-$3,499.99 range, 75,277 NULLs.

### 4. Null Handling

`main_category` (11,035), `store` (4,361), and `title` (9) NULLs are **left as-is** — not replaced with invented values.

### 5. Validate `parent_asin` Uniqueness

0 duplicates across 137,269 products — no deduplication needed.

### 6. Explode Images

`posexplode` on `images` array, filtering entries where all URL fields are NULL → **692,576 records**.

### 7. Explode Videos

`posexplode` on `videos` array, filtering entries where both title and URL are NULL → **145,608 records**.

### 8. Extract Details

The `details` field is a JSON object with varying keys per product. Values can be strings or nested objects. Parsing as `map<string, string>` auto-stringifies nested objects.

Products with empty `{}` details (3,422) excluded → **1,227,483 records**.

Example:

| parent_asin | detail_key | detail_value |
|---|---|---|
| B00069EVOG | Best Sellers Rank | `{"Video Games":137612,"PC-compatible Games":6707}` |
| B00069EVOG | Manufacturer | Sierra |

### 9. Validate Detail Key Uniqueness

0 duplicate `(parent_asin, detail_key)` pairs found.

---

## Output Tables

### Review Tables

| Table | Key Columns | Rows |
|---|---|---:|
| `silver.reviews` | review_id, asin, parent_asin, user_id, rating, text, title, review_timestamp, review_date, verified_purchase, helpful_vote | 4,570,969 |
| `silver.review_images` | review_id, image_index, attachment_type, large_image_url, medium_image_url, small_image_url | — |

### Product Tables

| Table | Key Columns | Rows |
|---|---|---:|
| `silver.products` | parent_asin, title, main_category, average_rating, rating_number, features, description, price, store, categories, price_raw | 137,269 |
| `silver.product_images` | parent_asin, image_idx, thumb, large, variant, hi_res | 692,576 |
| `silver.product_videos` | parent_asin, video_idx, title, url | 145,608 |
| `silver.product_details` | parent_asin, detail_key, detail_value | 1,227,483 |

```text
silver.reviews  ──1:N──▶  silver.review_images
silver.products ──1:N──▶  silver.product_images
                 ──1:N──▶  silver.product_videos
                 ──1:N──▶  silver.product_details
```

---

## Next Layer

These Silver tables feed the Gold layer for business-ready aggregations.

| Silver Table | Rows | Purpose |
|---|---:|---|
| `silver.reviews` | 4,570,969 | Cleaned, deduplicated reviews |
| `silver.review_images` | — | Review image URLs |
| `silver.products` | 137,269 | Cleaned product metadata |
| `silver.product_images` | 692,576 | Product image URLs |
| `silver.product_videos` | 145,608 | Product video URLs |
| `silver.product_details` | 1,227,483 | Product detail key-value pairs |
