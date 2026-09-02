# Gold Layer

The Gold layer builds business-ready Delta tables from the Silver layer using dimensional modeling and pre-aggregation.

> **Notebook:** [`04_gold_analytics`](https://github.com/Biswajitnahak2003/amazon-reviews-lakehouse/blob/main/gold/04_gold_analytics.py)

## Architecture

```text
                    SILVER
          ┌─────────────────────┐
          │ silver.reviews      │
          │ silver.products     │
          └──────────┬──────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────┐
│                      GOLD                        │
│                                                  │
│  ┌──────────────────┐   ┌──────────────────────┐ │
│  │  DIMENSIONS       │   │  AGGREGATIONS        │ │
│  │                  │   │                      │ │
│  │ Handled schema   │   │ Grouped by product   │ │
│  │ Dropped pipeline  │   │ Grouped by category  │ │
│  │   metadata       │   │   + month             │ │
│  │ Cast timestamps   │   │ Overall KPIs (SQL)   │ │
│  │ Derived date      │   │                      │ │
│  │   attributes     │   │                      │ │
│  │                   │   │                      │ │
│  │ ├── dim_product   │   │ ├── agg_product_     │ │
│  │ └── dim_date      │   │ │   ratings          │ │
│  │                   │   │ ├── agg_category_   │ │
│  │                   │   │ │   monthly          │ │
│  │                   │   │ └── agg_review_     │ │
│  │                   │   │     summary          │ │
│  └──────────────────┘   └──────────────────────┘ │
│                                                  │
│  ┌──────────────────────────────────────────┐    │
│  │  FACT                                     │    │
│  │ Joined reviews + products on parent_asin │    │
│  │ Enriched with product context             │    │
│  │ ├── fact_reviews                          │    │
│  └──────────────────────────────────────────┘    │
└──────────────────────────────────────────────────┘
```

## Input

| Table | Description |
|---|---|
| `silver.reviews` | Cleaned, deduplicated review records |
| `silver.products` | Cleaned product metadata |

---

## Processing

### 1. Build `dim_product`

Selected business-relevant columns from `silver.products` — dropped pipeline metadata (`_source_file`, `_ingested_at`, `price_raw`) and array columns (`features`, `description`, `categories`).

### 2. Build `dim_date`

Derived calendar dimension from distinct `review_date` values in `silver.reviews`. Each row is one date with year, quarter, month, month name, day-of-week, day name, and a weekend flag.

### 3. Build `fact_reviews`

Joined `silver.reviews` with `silver.products` on `parent_asin` (left join) to enrich each review with product context (`main_category`, `price`, `store`). Carries only business-relevant columns — no pipeline metadata.

### 4. Build `agg_product_ratings`

Grouped `fact_reviews` by `parent_asin` and `main_category`. Calculated total reviews, average rating, total helpful votes, and verified purchase percentage per product.

### 5. Build `agg_category_monthly`

Grouped `fact_reviews` by `main_category`, year, and month. Calculated total reviews, average rating, and total helpful votes per category-month combination. Ordered by category, year, month.

### 6. Build `agg_review_summary` (Spark SQL)

Single-row table with overall platform KPIs. Written using `CREATE OR REPLACE TABLE ... AS SELECT` (Spark SQL) to demonstrate both PySpark and SQL approaches in the same notebook.

---

## Output Tables

### Dimension Tables

| Table | Key Columns | Rows |
|---|---|---:|
| `gold.dim_product` | parent_asin, title, main_category, average_rating, rating_number, price, store | 137,269 |
| `gold.dim_date` | date, year, quarter, month, month_name, day_of_week, day_name, is_weekend | 8,731 |

### Fact Table

| Table | Key Columns | Rows |
|---|---|---:|
| `gold.fact_reviews` | review_id, parent_asin, asin, user_id, rating, text, title, review_timestamp, review_date, verified_purchase, helpful_vote, main_category, price, store | 4,570,969 |

### Aggregation Tables

| Table | Key Columns | Rows |
|---|---|---:|
| `gold.agg_product_ratings` | parent_asin, main_category, total_reviews, avg_review_rating, total_helpful_votes, verified_purchase_pct | 137,249 |
| `gold.agg_category_monthly` | main_category, year, month, total_reviews, avg_rating, total_helpful_votes | 5,514 |
| `gold.agg_review_summary` | total_reviews, overall_avg_rating, total_products_reviewed, total_helpful_votes, verified_purchase_pct | 1 |

---

## Star Schema

```text
           dim_product
               │
               │ 1
               │
  fact_reviews ──────<  (one review per row, enriched with product context)
               │
               │ N
               │
           dim_date
```

`fact_reviews` joins to `dim_product` on `parent_asin` and to `dim_date` on `review_date = date`.

Aggregation tables derive from `fact_reviews` and are ready for dashboard queries without further grouping.

---

## Notes

- `agg_product_ratings` has 137,249 rows vs 137,269 products in `dim_product` — 20 products had no reviews.
- `fact_reviews` uses a left join so all 4,570,969 reviews are retained even if product metadata is missing.
- `agg_review_summary` was built using Spark SQL (`CREATE OR REPLACE TABLE AS SELECT`) while all other tables use the PySpark DataFrame API.
- All tables are simple Delta tables with `mode("overwrite")` — no partitioning, liquid clustering, or materialized views yet.