# DocStream Real-Time Data Pipeline

[![Architecture](https://img.shields.io/badge/Architecture-Streaming-blue)]() [![Kafka](https://img.shields.io/badge/Kafka-3%20Node%20Cluster-orange)]() [![Spark](https://img.shields.io/badge/Spark-Streaming-red)]() [![Iceberg](https://img.shields.io/badge/Iceberg-1.4.0-green)]()

## Overview

A real-time data pipeline that captures Change Data Capture (CDC) events from PostgreSQL, processes them through Apache Spark Structured Streaming, and stores results in Apache Iceberg tables with real-time indexing to OpenSearch.

**Pipeline Characteristics:**
- **Volume**: Handles 5,000+ historical documents + continuous real-time updates
- **Architecture**: Event-driven, fault-tolerant, horizontally scalable
- **Data Lineage**: OpenLineage integration for observability (partial - see limitations)

## Architecture
```
┌─────────────┐
│ PostgreSQL  │  Source: Document lifecycle tracking
│  (WAL/CDC)  │  (draft → sent → viewed → signed → completed)
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────┐
│  Kafka Cluster (3 Brokers - KRaft Mode)    │
│  + Schema Registry (Avro)                   │  Message Bus
│  + Debezium Connector                       │  - Reliable delivery
└──────┬──────────────────────────────────────┘  - Schema evolution
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│              Apache Spark Streaming (3 Jobs)                 │
│                                                               │
│  ┌────────────────────┐                                      │
│  │ 1. Enrichment      │                                      │
│  │ - Time deltas      │                                      │
│  │ - Business logic   │                                      │
│  │ - Data quality     │                                      │
│  └─────┬──────────────┘                                      │
│        │                                                      │
│        ▼                                                      │
│  ┌─────────────────┐                                         │
│  │ Apache Iceberg  │                                         │
│  │ events.documents│ ◄───────────────────┐                  │
│  │ (Enriched CDC)  │                     │                  │
│  └────┬────────────┘                     │                  │
│       │                                   │                  │
│       ├──────────────┬────────────────────┘                  │
│       │              │                                       │
│       ▼              ▼                                       │
│  ┌─────────────┐  ┌────────────────┐                       │
│  │ 2. Aggreg.  │  │ 3. Indexer     │                       │
│  │ - Hourly    │  │ - Search       │                       │
│  │ - Metrics   │  │ - Analytics    │                       │
│  │ - Windows   │  │ - ISO format   │                       │
│  └─────┬───────┘  └────────┬───────┘                       │
└────────┼──────────────────┼─────────────────────────────────┘
         │                   │
         ▼                   ▼
┌─────────────────┐   ┌──────────────┐
│ Apache Iceberg  │   │  OpenSearch  │
│ metrics.hourly  │   │   + Dashboards│
│ (Aggregations)  │   │              │
└─────────────────┘   └──────────────┘
         │
         └──────► MinIO (S3-compatible storage)
```

1. **PostgreSQL → Kafka**: Debezium captures CDC events
2. **Kafka → Spark Job 1**: Enrichment writes to `events.documents` (Iceberg)
3. **Iceberg → Spark Job 2**: Reads enriched events, computes hourly aggregations → `metrics.hourly` (Iceberg)
4. **Iceberg → Spark Job 3**: Reads enriched events, indexes to OpenSearch for search/dashboards

## Technologies

| Component      | Purpose |
|----------------|---------|
| Apache Kafka   | Message bus |
| Apache Spark   | Stream processing |
| Apache Iceberg | Table format |
| Debezium       | CDC connector |
| PostgreSQL     | Source database |
| OpenSearch     | Search & analytics |
| MinIO          | S3-compatible storage |
| OpenLineage    | Data lineage |

---

## Data Flow

### 0. **Data Producer**

**Purpose:** Simulates a document management system generating lifecycle events

**What it does:**
- **Historical load**: Inserts 5,000 documents spanning 6 months of activity on startup
  - Distributed across statuses (80% completed, 20% in-progress)
  - Realistic timestamps with proper lifecycle progression
- **Real-time updates**: Continuously generates new documents and status transitions
  - Creates 3 new documents per minute
  - Updates ~10 existing documents per minute (status progression)

**Status Flow:**
```
draft → sent → viewed → signed → completed
```
---

### 1. **Source Layer - PostgreSQL**
```sql
-- Documents table tracks complete lifecycle
CREATE TABLE documents (
    document_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    status VARCHAR(50) CHECK (status IN ('draft', 'sent', 'viewed', 'signed', 'completed')),
    template_category VARCHAR(100),  -- 'Sales', 'Legal', 'HR'
    created_at TIMESTAMP,
    sent_at TIMESTAMP,
    viewed_at TIMESTAMP,
    signed_at TIMESTAMP,
    completed_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

**What it provides:**
- Source of truth for document lifecycle
- PostgreSQL logical replication (WAL) enabled
- Every UPDATE captured as CDC event

---

### 2. **Ingestion Layer - Kafka + Debezium**

**Components:**
- **3-node Kafka cluster** (KRaft mode)
- **Schema Registry** (Confluent) for Avro schema management
- **Debezium PostgreSQL Connector** for CDC capture

**What it provides:**
- Captures every database change
- Guarantees at-least-once delivery
- Schema evolution with Avro
- Decouples source from consumers

**Topic:** `dbserver1.public.documents`

**Sample Event:**
```json
{
  "document_id": "abc-123",
  "title": "Sales Contract",
  "status": "signed",
  "template_category": "Sales",
  "created_at": 1699500000000,
  "sent_at": 1699501000000,
  "viewed_at": 1699502000000,
  "signed_at": 1699503000000,
  "__op": "u",
  "__source_ts_ms": 1699503000000
}
```

---

### 3. **Processing Layer - Spark Structured Streaming**

#### **Job 1: Event Enrichment** 

**Purpose:** Transform raw CDC events into analytics-ready records

**Transformations:**
- **Time deltas**: Calculate minutes between lifecycle stages
```python
  time_to_view_minutes = (viewed_at - sent_at) / 60
  time_to_sign_minutes = (signed_at - sent_at) / 60
  time_to_complete_minutes = (completed_at - sent_at) / 60
```
- **Temporal context**: Extract date, hour, business hours flag
- **Data quality**: Validate required fields (nullability checks)

**Output:** `events.documents` (Iceberg table)

**Trigger:** Every 30 seconds

---

#### **Job 2: Real-Time Aggregations**

**Purpose:** Compute business metrics for reporting

**Aggregations (1-hour windows by category):**
- Document counts by status (created, sent, viewed, signed, completed)
- Conversion rates:
  - `sent_to_viewed_rate` = viewed / sent
  - `viewed_to_signed_rate` = signed / viewed
  - `completion_rate` = completed / sent
- Average processing times (view, sign, complete)

**Key Features:**
- **Watermarking**: Handles 10 minutes of late-arriving data
- **Windowing**: Tumbling 1-hour windows
- **Append mode**: Metrics finalized after watermark passes

**Output:** `metrics.hourly` (Iceberg table)

**Trigger:** Every 1 minute

---

#### **Job 3: OpenSearch Indexer** (`opensearch_indexer.py`)

**Purpose:** Enable real-time search and dashboards

**What it does:**
- Reads enriched events from Iceberg
- Converts timestamps to ISO 8601 format
- Upserts to OpenSearch (idempotent writes)

**Output:** OpenSearch index `documents`

**Trigger:** Every 30 seconds

---

### 4. **Storage Layer - Apache Iceberg**

- **ACID transactions** on object storage (S3/MinIO)
- **Time travel**: Query historical snapshots
- **Schema evolution**: Add/modify columns without downtime
- **Incremental reads**: Efficient for downstream consumers

**Tables:**

| Table | Namespace | Purpose |
|-------|-----------|---------|
| `documents` | `events` | Enriched CDC events |
| `hourly` | `metrics` | Aggregated metrics |

**Catalog:** REST catalog backed by MinIO (S3-compatible)

---

### 5. **Analytics Layer - OpenSearch**

**Use Cases:**

#### **1. Full-Text Search**
```bash
# Search documents by title
curl -X GET "localhost:9200/documents/_search" -H 'Content-Type: application/json' -d'
{
  "query": { "match": { "title": "NDA" } }
}'

# Filter by status and category
curl -X GET "localhost:9200/documents/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "bool": {
      "must": [
        { "term": { "status": "signed" } },
        { "term": { "template_category.keyword": "Legal" } }
      ]
    }
  }
}'
```

#### **2. Real-Time Dashboards** (OpenSearch Dashboards on port 5601)
- **KPIs**: Documents by status (pie chart)
- **Time series**: Document creation rate over time
- **Conversion funnels**: sent → viewed → signed → completed
- **Heatmaps**: Activity by hour and category

#### **3. Business Analytics**
```json
// Average time to sign by category
{
  "aggs": {
    "by_category": {
      "terms": { "field": "template_category.keyword" },
      "aggs": {
        "avg_time_to_sign": { "avg": { "field": "time_to_sign_minutes" } }
      }
    }
  }
}

// Documents created during business hours
{
  "query": {
    "term": { "is_business_hours": true }
  }
}
```

---

## Repository Structure
```
docstream-pipeline/
├── docker/
│   ├── docker-compose.yml          # Full stack orchestration
│   ├── kafka-connect/
│   │   ├── Dockerfile              # Custom Debezium image with Avro converter
│   │   └── debezium/
│   │       ├── connectors/
│   │       │   └── postgres-connector.json
│   │       └── deploy-connector.sh
│   ├── marquez/                    # OpenLineage backend
│   ├── postgres/
│   │   └── init.sql                # Schema + CDC setup
│   └── producer/
│       ├── producer.py             # Simulates document lifecycle
│       └── Dockerfile
└── spark_jobs/
    ├── common/
    │   ├── config.py               # Centralized configuration
    │   ├── data_quality.py         # Validation rules
    │   ├── spark_session.py        # Session management
    │   └── transformations.py      # Reusable functions
    ├── conf/
    │   └── spark-defaults.conf     # Spark/Iceberg config
    ├── event_enrichment.py         # Job 1
    ├── realtime_aggregations.py    # Job 2
    └── opensearch_indexer.py       # Job 3
```

---

## Quick Start

### Prerequisites
- Docker Desktop (≥16GB RAM, ≥4 CPU cores)
- 20GB free disk space

### 1. Start the Pipeline
```bash
# Start infrastructure
docker-compose -f docker/docker-compose.yml up -d

# Wait a few minutes for all services to be healthy
docker-compose -f docker/docker-compose.yml ps
```

### 2. Monitor the Producer
```bash
# Watch producer logs to see data generation
docker logs -f docstream-producer

# You'll see:
# - Historical load: 5,000 documents in ~30 seconds
# - Real-time updates: Every minute (3 created + ~10 updated)
```

### 3. Verify Data Flow

**Check Kafka topics:**
```bash
docker exec -it kafka1 kafka-topics --list --bootstrap-server kafka1:9092
# Should see: dbserver1.public.documents
```

**Check Spark jobs:**
```bash
# View Spark UI
open http://localhost:8080

# Check job logs
docker logs docstream-spark-enrichment --tail 50
docker logs docstream-spark-aggregations --tail 50
docker logs docstream-spark-opensearch --tail 50
```

**Query Iceberg tables:**
```bash
# Connect to Spark master
docker exec -it docstream-spark-master /opt/spark/bin/spark-sql \
  --master local[*] \
  --conf spark.sql.catalog.docstream_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.docstream_catalog.type=rest \
  --conf spark.sql.catalog.docstream_catalog.uri=http://rest:8181

# Run queries
USE docstream_catalog.events;
SELECT status, COUNT(*) FROM documents GROUP BY status;

USE docstream_catalog.metrics;
SELECT * FROM hourly ORDER BY window_start DESC LIMIT 10;
```

**Check OpenSearch:**
```bash
# Document count
curl -s "localhost:9200/documents/_count?pretty"

# Sample documents
curl -s "localhost:9200/documents/_search?pretty&size=3"
```

**Access UIs:**
- Kafka UI: http://localhost:8082
- Spark Master: http://localhost:8080
- OpenSearch Dashboards: http://localhost:5601
- MinIO Console: http://localhost:9001 (admin/password)
- Marquez (Lineage): http://localhost:3001

---

## Design Decisions

### 1. **Kafka Cluster (3 Brokers)**
- **3 nodes**: Enables replication factor = 3, min ISR = 2 (fault tolerance)
- **KRaft mode**: Modern Kafka without Zookeeper dependency
- **Compression**: LZ4 for best CPU/compression tradeoff

### 2. **Avro with Schema Registry**
- Compact binary format (vs JSON)
- Schema evolution (forward/backward compatibility)

### 3. **Spark Structured Streaming**
- **Micro-batches** (30s-1min) balance latency vs throughput
- **Checkpointing**: S3 (MinIO) for fault tolerance
- **Dynamic allocation**: Scales executors based on load

### 4. **Apache Iceberg**
- **ACID guarantees** on object storage
- **Time travel**: `SELECT * FROM docstream_catalog.events.documents TIMESTAMP AS OF '2025-11-11 17:00:00';`
- **Snapshot isolation**: Concurrent reads/writes without conflicts

### 5. **OpenSearch**
- Pre-indexed for sub-second search queries
- Aggregations run on indexed data (not raw events)
- Dashboards update in real-time

---

## Monitoring & Observability

### 1. **Data Lineage (Partial - OpenLineage)**

**What's tracked:**
- Debezium connector emits lineage events to Marquez
- Iceberg table reads/writes (batch mode)

**Known Limitation:**
- Spark Structured Streaming writes to Iceberg **do not emit lineage events** due to `SparkMicroBatchStream` not being supported by OpenLineage Spark agent
- Warning in logs:
```
  WARN StreamingDataSourceV2RelationVisitor: The class NoOpStreamStrategy has been selected 
  because no rules have matched for SparkMicroBatchStream
```
- **Workaround**: Use batch jobs for lineage tracking or custom OpenLineage facets

### 2. **Metrics**
- **Kafka**: Consumer lag (Kafka UI: http://localhost:8082)
- **Spark**: Processing time, input rate (Spark UI: http://localhost:8080)
- **Iceberg**: Table snapshots, file count
```sql
  SELECT snapshot_id, committed_at, summary
  FROM docstream_catalog.events.documents.snapshots;
```
---

## Production

### ⚠️ Current Configuration: **Local Development**

This setup is optimized for running on a single machine with limited resources. **Production deployments require adjustments:**

#### **1. Debezium Connector**
```diff
# Current (demo):
"snapshot.fetch.size": "2000"
"snapshot.delay.ms": "0"

# Production:
+ "snapshot.fetch.size": "10000"           # Increase for faster initial load
+ "heartbeat.interval.ms": "5000"          # Detect connection issues
+ "max.batch.size": "2048"                 # Tune for throughput
+ "max.queue.size": "16384"
+ "tombstones.on.delete": "true"           # Kafka compaction
```

#### **2. Spark Cluster**
```diff
# Current (demo):
spark.executor.memory=1g
spark.executor.cores=1
spark.dynamicAllocation.maxExecutors=2

# Production:
+ spark.executor.memory=8g                  # Scale per node capacity
+ spark.executor.cores=4
+ spark.dynamicAllocation.maxExecutors=20   # Auto-scale based on load
+ spark.sql.shuffle.partitions=200          # Adjust for data volume
```

#### **3. Kafka**
```diff
# Current (demo):
KAFKA_HEAP_OPTS: '-Xmx512m -Xms512m'

# Production:
+ KAFKA_HEAP_OPTS: '-Xmx4g -Xms4g'
+ num.network.threads: 8
+ num.io.threads: 16
+ log.retention.hours: 168                  # Adjust retention policy
```

#### **4. Iceberg Compaction**
**Expected behavior:** Real-time streaming produces many small files
```bash
# Check file count
SELECT COUNT(file_path) FROM docstream_catalog.events.documents.files;
```

**Solution:** Run daily compaction:
```python
spark.sql("""
  CALL docstream_catalog.system.rewrite_data_files(
    table => 'docstream_catalog.events.documents',
    options => map('target-file-size-bytes', '536870912')  -- 512MB
  )
""")
```

#### **5. Security (Production)**
- Use Secrets Manager for credentials
- Enable Iceberg encryption at rest

#### **6. High Availability**
- Deploy Kafka across multiple AZs
- Use managed Schema Registry (Confluent Cloud)
- Run Spark on EMR/Databricks with auto-recovery
- Enable Iceberg table backups

---

## Next Steps for Production

### 1. **Observability**
- [ ] Integrate Prometheus/Grafana for metrics
- [ ] Add structured logging (JSON format)
- [ ] Complete OpenLineage integration (custom facets for streaming)
- [ ] Set up lag monitoring alerts (Kafka consumer lag < 5 minutes)

Make sure we Monitor:
- Debezium lag (how far behind CDC is)
- Schema registry health
- Kafka topic lag
- Connector status

### 2. **Data Quality**
- [ ] Implement data profiling on Iceberg tables
- [ ] Add anomaly detection (sudden spikes in null values)

### 3. **Performance**
- [ ] Tune Spark shuffle partitions based on production load
- [ ] Enable Iceberg Z-ordering on query columns (`template_category`, `status`)
- [ ] Run compaction jobs during off-peak hours

### 4. **Scalability**
- [ ] Migrate to managed Kafka (MSK)
- [ ] Use AWS Glue or EMR for Spark execution
- [ ] Replace MinIO with S3 (production object storage)

Scaling (> 1M rows):
```text
Step 1: Initial Snapshot
├─ Use Spark to copy table in parallel
└─ Write to "backfill" table

Step 2: Start Debezium CDC
├─ Reads changes from now onwards
└─ Writes to "realtime" table

Step 3: Merge Tables
├─ Create view combining backfill + realtime
└─ Deduplicate overlapping data
```
---

## Contact

For questions or improvements, open an issue or submit a PR.