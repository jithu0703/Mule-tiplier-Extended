# Database Showcase & Presentation Guide

**Project:** Chain Reaction — High-Dimensional Distributed AML Engine  
**Topic:** Wide-Column NoSQL Storage & Lakehouse Database Architecture  
**Key Technologies:** Apache Cassandra (CQL), Apache Spark SQL, Databricks Delta Lake / Parquet, PySpark

---

## 1. The 3-Minute Elevator Pitch

> *"In Anti-Money Laundering (AML) monitoring, financial institutions must process millions of continuous transactions every day while maintaining real-time risk profiles and running complex graph analytics.*  
>  
> *Traditional relational databases (PostgreSQL/MySQL) break down under this workload because B-Tree index locks throttle write throughput, recursive multi-table JOINs kill graph queries, and schema migrations lock tables for hours.*  
>  
> *To solve this, I designed a **Wide-Column NoSQL & Lakehouse Storage Model** using **Apache Cassandra CQL** principles implemented on **Databricks Delta Lake / Parquet**. It provides:*  
> 1. *$O(1)$ point lookups by hashing account partition keys across cluster nodes.*  
> 2. *Contiguous clustered disk slices for instant ego-subgraph reconstruction without scanning tables.*  
> 3. *High-dimensional wide rows holding 18+ graph and behavioral features with zero-byte NULL overhead.*  
> 4. *Zero-downtime schema evolution to deploy newly trained ML model scores instantly."*

---

## 2. How to Showcase Your Database Live

You have two ready-to-run ways to showcase your database:

### Option A: Interactive Command-Line Tour (Fastest, Terminal-Friendly)
Run the automated presentation script directly from your terminal:
```bash
python scripts/showcase_db.py
```
Or run in interactive mode (press `[Enter]` between steps to talk through each slide):
```bash
python scripts/showcase_db.py --interactive
```
Or target a specific account partition:
```bash
python scripts/showcase_db.py --account 100428660
```

### Option B: Jupyter Notebook Showcase (Visual, GitHub-Rendered)
Open and walk through:
- [`notebooks/07_database_showcase.ipynb`](file:///c:/Projects/Mule-tiplier%20Extended/notebooks/07_database_showcase.ipynb)
- Or [`notebooks/06e_wide_column_model.ipynb`](file:///c:/Projects/Mule-tiplier%20Extended/notebooks/06e_wide_column_model.ipynb)

Both notebooks are pre-executed with formatted tables, execution timings, and diagrams already rendered.

---

## 3. Four Core Architectural Highlights to Explain

### 1. Query-Driven Data Modeling (Cassandra Philosophy)
In relational databases, you model for normalization (3NF) and figure out queries later using JOINs.  
In **Wide-Column NoSQL**, you model tables **strictly around access patterns**:

| Access Pattern | Target Table | Partition Key | Clustering Key | Computational Complexity |
| :--- | :--- | :--- | :--- | :--- |
| **Point Profile Lookup** | `aml_account_profiles` | `account_id` | N/A | $O(1)$ Hash Partition Route |
| **Ego-Subgraph Slicing** | `aml_transaction_events` | `account_id` | `event_timestamp DESC` | $O(\log K)$ Local Clustered Scan |
| **High-Risk Analyst Triage** | Materialized View | `is_laundering, jurisdiction_risk` | `(total_in + total_out) DESC` | Pre-indexed sorted slice |

### 2. DDL Schema Definition (`docs/cassandra_schema.cql`)
Show the CQL file [`docs/cassandra_schema.cql`](file:///c:/Projects/Mule-tiplier%20Extended/docs/cassandra_schema.cql):
- Highlight `PRIMARY KEY ((account_id), event_timestamp, transaction_id)`:
  - Outer `(account_id)` is the **Partition Key** — guarantees all transactions for an account live on the exact same physical node.
  - `event_timestamp DESC` is the **Clustering Key** — physically orders rows inside the SSTable / Parquet partition so recent transactions are stored consecutively on disk.
- Highlight `default_time_to_live = 31536000` (1 year TTL): Cassandra automatically purges aged transaction data during background compaction with zero delete lock overhead.

### 3. Physical Storage Layer on Disk
Show the actual storage directories:
```
outputs/wide_column_store/
??? transaction_events/
?   ??? part-*.parquet (Immutable columnar financial events)
?   ??? _delta_log/ (ACID commit log & versioned checkpoints)
??? account_profiles/
    ??? part-*.parquet (High-dimensional wide rows)
    ??? _delta_log/
```
Explain that Parquet gives **columnar compression (Snappy)**, **dictionary encoding**, and **Bloom filters**, while Delta Lake provides **ACID transactions** and **time-travel rollback**.

### 4. Zero-Downtime Schema Evolution
Show how adding new ML predictions works without table rebuilds:
```sql
ALTER TABLE aml.account_profiles ADD spark_gbt_risk_score DOUBLE;
```
Because the storage is wide-column, new columns are dynamically attached as sparse metadata attributes without requiring full-table rewrites or locking active reads.

---

## 4. Cheat Sheet: Comparison with Relational Databases (RDBMS)

When asked: *"Why not just use PostgreSQL or MySQL?"*

| Evaluation Dimension | Relational DB (PostgreSQL / MySQL) | Wide-Column NoSQL (Cassandra / Delta Lake) | Why Wide-Column Wins in AML |
| :--- | :--- | :--- | :--- |
| **Write Scaling** | Single-master bottleneck; WAL locks | Masterless LSM Tree / append-only Parquet | Absorbs 100k+ TPS spikes from credit cards & wire networks |
| **Graph Traversal** | Recursive self-joins ($O(N^2)$) | Clustered partition key range slice ($O(\log K)$) | Immediate 1-hop & 2-hop money-flow reconstruction |
| **Schema Flexibility** | Schema migrations lock tables | Instant zero-downtime metadata evolution | Continuously update entity risk profiles as new ML models deploy |
| **Big Data Integration** | Requires Debezium/Kafka CDC connectors | Native Apache Spark / Databricks Delta engine | Batch, streaming, and ML algorithms query storage in-place |

---

## 5. Summary of Showcase Artifacts

1. **CLI Showcase Script:** [`scripts/showcase_db.py`](file:///c:/Projects/Mule-tiplier%20Extended/scripts/showcase_db.py)
2. **Visual Showcase Notebook:** [`notebooks/07_database_showcase.ipynb`](file:///c:/Projects/Mule-tiplier%20Extended/notebooks/07_database_showcase.ipynb)
3. **CQL Schema Specification:** [`docs/cassandra_schema.cql`](file:///c:/Projects/Mule-tiplier%20Extended/docs/cassandra_schema.cql)
4. **Full Architecture Blueprint:** [`docs/ARCHITECTURE.md`](file:///c:/Projects/Mule-tiplier%20Extended/docs/ARCHITECTURE.md)
