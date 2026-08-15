# Bronze Layer

The Bronze layer ingests raw Amazon Reviews 2023 data into Delta tables with minimal transformation. The goal is to preserve the source data as-is while adding basic ingestion metadata for traceability.

> **Notebook:** [`01_bronze_ingestion`](https://github.com/Biswajitnahak2003/amazon-reviews-lakehouse/blob/main/bronze/01_bronze_ingestion.py)

```
┌──────────────────────┐
│  Unity Catalog       │
│  Raw .gz files       │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  Read + Audit Cols   │
│  (_source_file,      │
│   _ingested_at)      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  Delta Tables        │
│  (Bronze)            │
└──────────┬───────────┘
           │
           ▼
      Silver Layer
```

---

## Storage: Unity Catalog Volumes

Instead of setting up external cloud storage like S3 or GCS, we use **Unity Catalog Volumes** — Databricks' managed storage that sits on top of your cloud storage but handles the infrastructure for you. Raw files are uploaded directly to the Volume and accessed via a simple path:

```text
/Volumes/amazon_lakehouse/raw/amazon_files/
```

See the [project README](https://github.com/Biswajitnahak2003/amazon-reviews-lakehouse/blob/main/README.md) for steps on downloading and uploading the dataset.

---

## Source Data

Two files from the Video Games domain of the Amazon Reviews'23 dataset:

```text
/Volumes/amazon_lakehouse/raw/amazon_files/
├── reviews/
│   └── Video_Games.jsonl.gz
└── metadata/
    └── meta_Video_Games.jsonl.gz
```

---

## Ingestion Strategy

### Reviews

Reviews have a stable schema, so they're read directly with Spark's JSON reader:

```python
reviews_bronze = (
    spark.read.json(reviews_path)
    .withColumn("_source_file", F.lit("Video_Games.jsonl.gz"))
    .withColumn("_ingested_at", F.current_timestamp())
)
```

The result is written as a **Delta table** (`amazon_lakehouse.bronze.reviews`). Delta is chosen over raw JSONL because it supports ACID transactions, schema enforcement, and efficient upserts — making downstream reads faster and incremental updates possible.

### Product Metadata

The metadata has a `details` field that varies across products — sometimes a string, sometimes nested JSON. Spark's schema inference fails on this because of duplicate field names like `assembly required`.

So we read it as **raw text** instead:

```python
metadata_raw = spark.read.text(metadata_path)

metadata_bronze = (
    metadata_raw
    .withColumnRenamed("value", "raw_json")
    .withColumn("_source_file", F.lit("meta_Video_Games.jsonl.gz"))
    .withColumn("_ingested_at", F.current_timestamp())
)
```

Also written as a **Delta table** (`amazon_lakehouse.bronze.product_metadata_raw`). Storing the raw JSON string in Delta keeps the original data intact while gaining Delta's reliability guarantees — no data loss, consistent reads, and time-travel capability if we need to reprocess later.

---

## Output Tables

| Table | Description |
|-------|-------------|
| `bronze.reviews` | Structured review records with nested images |
| `bronze.product_metadata_raw` | Raw product metadata stored as JSON strings |

Both tables include `_source_file` and `_ingested_at` for lineage tracking.
