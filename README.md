# Real-Time Search Re-Ranking Pipeline

A distributed, low-latency machine learning infrastructure engine designed to dynamically optimize search result relevance using real-time user click telemetry. This system captures live click streams, processes behavioral signals, corrects for position bias, and updates an in-memory cache to serve re-ranked results in sub-milliseconds.

## System Architecture

The pipeline implements a two-stage retrieval and re-ranking architecture:
1. **Ingestion Layer:** Python-based Kafka producers simulate high-throughput client click logs and stream them to an event broker.
2. **Processing Layer:** A streaming consumer reads events, computes time-decayed Click-Through Rates (CTR), and applies Inverse Propensity Scoring (IPS) to eliminate position bias.
3. **Storage & Serving Layer:** High-relevance document rankings are pushed to Redis Sorted Sets (`ZSET`), allowing the Search API to serve dynamically optimized results with sub-100ms P99 latency.

<img width="2853" height="3532" alt="User Interaction with API-2026-06-25-060228" src="https://github.com/user-attachments/assets/94ae1839-5b8d-413e-9ecc-09244f94f012" />

## 🛠️Infrastructure Setup

The local streaming infrastructure is fully containerized using Docker.

### Prerequisites
* Docker Desktop installed
* Python 3.10+

### Spin up Kafka & Redis
From the root directory, run the following command to start the message broker and cache layers in the background:

```bash
docker compose up -d
```

To verify the containers are running healthy:
```bash
docker ps
```
