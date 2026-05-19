# IoT Sensor Data Engineering Platform

End-to-end data pipeline for AeroSense IoT sensors.

## Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Java 11 (for Spark)

## Quick Start

1. **Start Kafka cluster**  
   `docker compose up -d`

2. **Create topic** (3 partitions, replication factor 3)  
   `docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --topic sensor-events --partitions 3 --replication-factor 3`

3. **Run producer** (send 1000 events at 10 msg/sec)  
   `python src/producer.py --count 1000 --rate 10 --source factory-42`

4. **Launch Spark pipeline**  
   `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_pipeline.py`

5. **Run analytics queries**  
   `spark-submit src/analytics.py`

6. **Start REST API**  
   `python api/app.py`

7. **Test API**  
   `./tests/test_curl_commands.sh`

## Components

- **Kafka**: 3‑broker cluster, topic `sensor-events`
- **Producer**: Simulates IoT devices, 10% anomalies (values within physical range but outside business thresholds)
- **Spark Streaming**: Watermark (2 min), 5‑minute windows, writes to raw/curated/consumption zones
- **Data Lake**: `datalake/` directory (relative to project root) with Hive‑style partitioning
- **Analytics**: Spark SQL with partition pruning demo
- **REST API**: Flask with 6 endpoints (health, sensors, latest, stats, anomalies, publish)

See `docs/architecture.md` for detailed design.

## Technical Choices

### 1. Partitioning strategy for curated zone
Curated zone is partitioned by `sensor_type/year/month/day`. This allows queries filtering on a specific sensor type to skip irrelevant partitions entirely, dramatically reducing I/O. It also supports time‑range queries without full scans. Alternatives considered: partitioning only by date (loses sensor‑type pruning) or by sensor_type alone (partitions become too large over time). The hybrid approach balances both dimensions.

### 2. Spark Structured Streaming outputMode
We use `append` mode. Because a 2‑minute watermark is applied on event time, late data beyond the watermark is discarded, and window results are final once emitted. Append mode is therefore safe and more efficient than update or complete mode, which would require maintaining state indefinitely. This choice also reduces memory pressure on the driver.

### 3. Replication factor (3) and min.insync.replicas (2)
With 3 brokers, replicating each partition 3 times ensures data durability even if one broker fails completely. Setting `min.insync.replicas=2` means the producer only needs acknowledgment from 2 brokers before considering the write successful; the cluster remains available during a single broker outage. This setting provides a strong fault‑tolerance guarantee without blocking writes when one node is temporarily slow or down.

### 4. event_time vs ingestion_time across zones
- **Raw zone** uses ingestion time (Kafka message timestamp) for partitioning. This makes it easy to audit when data actually entered the system, regardless of sensor clock skew.
- **Curated and consumption zones** use event time (sensor timestamp). Business analytics must align with when an event occurred, not when it arrived. The watermark also relies on event time to handle out‑of‑order data correctly. This distinction is a standard practice in Lambda and Kappa architectures.

### 5. End‑to‑end delivery semantics
The pipeline provides **at‑least‑once** semantics. The producer is configured with `acks=all` and retries, so messages are persisted durably. Spark checkpointing allows the streaming job to resume from the last committed offset after a failure. However, without idempotent sinks, a crash during write could cause duplicate output files. In practice, this is acceptable for the analytics use case; consumers can deduplicate if needed. Exactly‑once would require transactional Kafka producers and idempotent Parquet writers (e.g., `_delta_log`), which adds complexity beyond the current scope.

## Results

### Analytical queries (actual run on 1000 messages)
| Query | Result |
|-------|--------|
| Top 5 anomaly hours | hour 9 → 109 anomalies (only one hour present in data) |
| Per‑sensor anomaly rate | humidity 13.62%, pressure 10.66%, temperature 7.96% |
| Partition pruning speedup | 1.85x (full scan 0.16s vs pruned 0.09s) |

### API response samples (expected)
GET `/api/v1/health` → `{"status":"ok","timestamp":"..."}`  
GET `/api/v1/sensors/temperature/latest` → `{"status":"success","data":{...}}`  
POST `/api/v1/readings` → `{"status":"success","partition":0,"offset":1245}`

*See `docs/analytics.md` for full query results and CSV outputs.*

## Limitations and Improvements

- **Current limitations**: The Spark pipeline runs in local mode, limiting throughput. Some API endpoints (`/stats`, `/anomalies`) may return 500 errors if the Spark session is not correctly initialised – a known issue fixed in the current code (single‑session pattern).
- **If given 2 extra days**: I would (1) containerise the Spark job and API, (2) implement Kafka topic compaction for the latest‑values view, (3) add a simple alerting service that consumes the consumption zone and sends notifications for high anomaly rates, and (4) write integration tests that simulate broker failures and network partitions.# Yalin_Mo_exam
