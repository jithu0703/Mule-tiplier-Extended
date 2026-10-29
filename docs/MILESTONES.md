# Chain Reaction — Money Mule Detection with Graph Neural Networks

A graph-based fraud detection project that models bank transactions as a graph to identify money mule accounts, using GraphSAGE for learned structural patterns, motif search for multi-hop laundering chains, GNNExplainer for interpretable fraud flags, and Apache Spark / NoSQL for distributed big data processing.

---

## Milestone 1: Data & Graph Foundation

**Goal:** Load real transaction data and represent it as an actual graph you can inspect and visualize.

**Concepts required:**
- What a graph is in this context (nodes = accounts, edges = transactions, edge weight = amount, edge attribute = timestamp)
- Directed vs undirected graphs (this is directed — money flows one way)
- Basic graph terms: degree, in-degree, out-degree, neighbors, path

**What gets built:**
- Load the IBM AML dataset (HI-Small_Trans.csv) into pandas, inspect schema
- Build a directed graph using `networkx`
- Visualize a small subgraph to see what a mule pattern looks like

**Done when:** You can pull up any account by ID and plot its immediate neighbors and transaction history, and visually contrast a laundering account's subgraph against a legitimate one.

---

## Milestone 2: Feature Engineering + Baseline Model

**Goal:** Turn graph structure into numeric features and get a working (non-GNN) fraud classifier as a benchmark.

**Concepts required:**
- Why row-level transaction data misses fraud (individually normal, collectively suspicious)
- Graph-based features: fan-in/fan-out ratio, holding time, account age, PageRank/centrality
- Class imbalance and why accuracy is misleading here — use precision, recall, and PR-AUC instead

**What gets built:**
- Per-account features: in-degree, out-degree, avg holding time, unique counterparties, PageRank
- A trained `XGBoost` / `RandomForestClassifier` baseline
- Evaluation using precision-recall AUC

**Done when:** You have a trained baseline model with a PR-AUC score to use as your comparison benchmark for every milestone after this.

---

## Milestone 3: Graph Neural Network (GraphSAGE)

**Goal:** Replace/augment the baseline with a model that learns directly from graph structure instead of hand-crafted stats.

**Concepts required:**
- Node embeddings — a learned numeric representation of each account capturing its role in the graph
- Message passing — nodes update their embedding by aggregating neighbor info, layer by layer
- Why GraphSAGE — inductive (works on unseen accounts) and uses neighbor sampling instead of needing the full graph in memory
- The over-smoothing problem — why you can't just stack many layers to "see further"

**What gets built:**
- Convert the graph into PyTorch Geometric format
- A 2–3 layer GraphSAGE model trained with `NeighborLoader` sampling
- A comparison table: baseline (Milestone 2) vs GraphSAGE PR-AUC

**Done when:** You have a trained GraphSAGE model and an honest, documented comparison against the baseline.

---

## Milestone 4: Multi-Hop Chain Detection + Explainability

**Goal:** Catch layered, multi-hop laundering chains, and make model output human-readable.

**Concepts required:**
- Motif/pattern matching in graphs — searching for a specific shape (a chain with amount decay and time proximity)
- Why this is separate from the GNN — structural pattern search vs learned embeddings solve different parts of the problem
- What GNNExplainer does — identifies which edges/neighbors drove a node's fraud score

**What gets built:**
- A `networkx` (or Spark GraphFrames) function to search for chains: A→B→C→D with decreasing amounts and timestamps within a window
- GNNExplainer applied to flagged accounts
- A combined account risk report: GNN score + chain membership + plain-language explanation

**Done when:** For any flagged account, you can output a short human-readable reason, not just a probability.

---

## Milestone 5: Scale + Demo

**Goal:** Show the pipeline works beyond toy scale and package it as something demoable.

**Concepts required:**
- Why distributed processing matters at scale (a billion-edge graph won't fit/process on one machine)
- Basic PySpark/GraphFrames concepts — distributed DataFrames, lazy evaluation, pattern matching with `.find()`

**What gets built:**
- Milestone 2's feature engineering re-implemented in PySpark + GraphFrames
- Re-run on a larger dataset size (HI-Medium/Large)
- A Streamlit dashboard: search an account, view its subgraph, view its risk score and explanation

**Done when:** You have a live, clickable demo and a README documenting the full pipeline, results, and honest limitations.

---

## Milestone 6: Distributed Big Data & Streaming Architecture (Spark, MLlib, Streaming & NoSQL)

**Goal:** Deliver an enterprise-grade distributed AML big data platform demonstrating Spark RDD, Spark SQL, Spark MLlib, Structured Streaming, and a Wide-Column NoSQL data model designed for Databricks micro-clusters.

**Concepts required:**
- Spark RDD core transformations (`map`, `flatMap`, `filter`, `reduceByKey`, `sortByKey`) and actions (`count`, `take`, `first`, `saveAsTextFile`)
- Spark SQL Temp Views, windowed aggregation (`LAG()`, `LEAD()`), and ANSI SQL rule-based risk classification
- Distributed classification pipelines with `pyspark.ml` (`GBTClassifier`, `RandomForestClassifier`, `LogisticRegression`) tuned for severe class imbalance using PR-AUC
- Real-time Structured Streaming with event-time watermarking, 5-minute tumbling windows, and anomaly filtering
- Wide-column NoSQL schema architecture (Cassandra CQL & Delta Lake) with dynamic schema evolution, ACID upserts, and time-travel audit trails
- Multi-source ETL pipelines with automated validation (5 deterministic rules), deduplication, sanctions screening, and referential integrity verification

**What gets built:**
- `src/spark_rdd_pipeline.py`: Low-level RDD transformation and feature extraction pipeline
- `src/spark_sql_analytics.py`: Advanced ANSI Spark SQL queries and windowed temporal analytics
- `src/spark_ml_pipeline.py`: Distributed MLlib classification suite with unified benchmark tables
- `src/stream_simulator.py` & `src/streaming_pipeline.py`: Continuous micro-batch stream simulator and Structured Streaming engine
- `src/wide_column_model.py` & `docs/cassandra_schema.cql`: NoSQL Cassandra schema specification and Delta Lake wide-row implementation
- `src/etl_pipeline.py`, `data/external/sanctions_list.csv`, `data/external/country_risk_scores.csv`: Multi-source ETL orchestrator with automated data quality audits
- Complete Databricks notebook execution flow: `notebooks/06a_spark_rdd_fundamentals.ipynb` through `06g_end_to_end_demo.ipynb`

**Done when:** All six sub-phases (6A–6F) and the capstone end-to-end integration demo run seamlessly with complete requirements traceability.
