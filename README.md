# Chain Reaction — Money Mule Detection with Graph Neural Networks

A graph-based fraud detection system that models bank transactions as a graph to identify money mule accounts. Combines a GraphSAGE graph neural network for learned structural patterns, motif/chain search for multi-hop laundering detection, and GNNExplainer for interpretable, investigator-ready fraud flags.

---

## Why graphs, not row-level classification

Individual money mule transactions often look completely normal in isolation — a deposit, then a withdrawal, both under reporting thresholds. The fraud signal only becomes visible in the **pattern of connections**: many first-time senders converging into one account (fan-in), followed by a rapid, near-complete forwarding of funds to a different set of accounts (fan-out), all within a short time window. Standard row-by-row transaction classifiers miss this because they never see the surrounding structure. This project treats the transaction data as a graph instead of a table, so the model can learn from *who is connected to whom, how fast, and how much* — not just individual transaction features.

---

## Dataset

[IBM Transactions for Anti-Money Laundering (AML)](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) — a synthetic dataset modeling realistic banking transactions across multiple currencies, with ground-truth laundering labels covering eight distinct laundering patterns (fan-in, fan-out, cycle, scatter-gather, and others).

Starting scale: **HI-Small** (~5M transactions). Larger sizes (HI-Medium, HI-Large) used in the scaling milestone.

> Raw data files are not committed to this repository. Download `HI-Small_Trans.csv` and `HI-Small_Patterns.txt` from Kaggle and place them in `data/raw/`.

---

## Project Structure

```
chain-reaction/
│
├── data/
│   ├── raw/                  # Untouched downloaded dataset files
│   └── processed/            # Cleaned data, computed features, saved graphs
│
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_baseline_model.ipynb
│   ├── 03_graphsage.ipynb
│   ├── 04_chain_detection_and_explainability.ipynb
│   └── 05_scaling_and_demo.ipynb
│
├── src/
│   ├── data_loader.py        # Load + clean transactions, currency standardization
│   ├── graph_builder.py      # Build the transaction graph (networkx)
│   ├── feature_engineering.py # Per-account graph feature computation
│   ├── splits.py             # Stratified 70/10/20 data splitting
│   ├── gnn_data.py           # PyG Data object constructor
│   ├── gnn_model.py          # GraphSAGE architecture & NeighborLoader
│   ├── chain_detector.py     # Multi-hop temporal motif chain detector
│   ├── explainer.py          # GNNExplainer feature & edge attribution
│   ├── risk_report.py        # Combined Analyst Risk Cards & ego-subgraphs
│   └── spark_features.py     # Distributed PySpark feature engineering
│
├── outputs/
│   ├── figures/              # Saved plots and visualizations
│   ├── cache/                # Serialized graphs
│   ├── models/               # XGBoost model (model.json) & GraphSAGE (graphsage.pt)
│   └── results/              # Evaluation benchmark tables
├── app.py                    # Interactive Streamlit AML Compliance Dashboard
│
├── requirements.txt
└── README.md
```

---

## Setup

```bash
python -m venv chain-reaction-env
source chain-reaction-env/bin/activate      # Windows: chain-reaction-env\Scripts\activate

pip install -r requirements.txt
```

### Launch Interactive AML Intelligence Dashboard
```bash
streamlit run app.py
```

**Requirements**
```
pandas
numpy
networkx
matplotlib
seaborn
scikit-learn
xgboost
torch
torchvision
torch_geometric
jupyterlab
streamlit
pyspark
```

---

## Data Preprocessing

Transaction amounts in the dataset span multiple currencies (USD, Euro, Bitcoin, and others). All amounts are converted to a standardized USD reference (`amount_paid_usd`, `amount_received_usd`) using static approximate exchange rates, since exact real-world historical rates aren't meaningful for this synthetic dataset — the goal is comparability across accounts, not financial precision. Original raw amount and currency columns are preserved alongside the converted ones.

---

## Milestones

### Milestone 1 — Data & Graph Foundation
Load transaction data, standardize currencies, and build a directed graph (nodes = accounts, edges = transactions). Visualize and contrast the local subgraph of a known laundering account against a legitimate one.

### Milestone 2 — Feature Engineering + Baseline Model
Compute per-account graph features (in/out-degree, total inflow/outflow, average holding time, has_outflow_after_inflow,  PageRank, unique counterparties) and train an XGBoost baseline classifier. Evaluated with precision-recall AUC due to severe class imbalance. This establishes the benchmark score for all later milestones.

### Milestone 3 — Graph Neural Network (GraphSAGE)
Train a GraphSAGE model using neighbor sampling to learn account embeddings directly from graph structure, rather than relying solely on hand-crafted features. Compared against the Milestone 2 baseline.

### Milestone 4 — Multi-Hop Chain Detection + Explainability
Search for layered laundering chains (A → B → C → D with amount decay and time proximity) that a single-node classifier can miss. Apply GNNExplainer to generate human-readable rationale for flagged accounts, combining GNN score, chain membership, and plain-language explanation.

### Milestone 5 — Scale + Demo
Reimplement feature engineering in PySpark + GraphFrames for larger dataset sizes, and build a Streamlit dashboard to search accounts, view their subgraph, and inspect their fraud score and explanation.

### Milestone 6 — Distributed Big Data, Streaming & NoSQL Architecture
Deliver an enterprise-grade distributed AML big data platform:
- Low-level **Spark RDD** transformations (map, flatMap, filter, reduceByKey, sortByKey) and actions (take, count, saveAsTextFile).
- Multi-source **ETL pipeline** integrating transaction ledgers, OFAC sanctions lists, and FATF country risk ratings with 5 deterministic data quality rules and automated audit reporting.
- **Wide-column NoSQL schema** design in Cassandra CQL (transaction_events, account_profiles, streaming_alerts, model_predictions) and Delta Lake implementation with dynamic schema evolution and ACID upserts.
- **Spark SQL** structured analytics with temp views, window functions (LAG()), and rule-based risk scoring.
- Distributed classification pipelines using **PySpark MLlib** (GBTClassifier, RandomForestClassifier, LogisticRegression) evaluated on PR-AUC.
- Real-time **Structured Streaming** with event-time watermarking, 5-minute tumbling windows, anomaly rules, and dual console/parquet sinks.
- Complete suite of 7 **Databricks Community Edition** notebooks (notebooks/06a to 06g_end_to_end_demo.ipynb).

---

## Results

| Model | Val PR-AUC | Test PR-AUC | Test Precision | Test Recall | Notes |
|---|---|---|---|---|---|
| Random Guessing Floor | 0.0123 | 0.0123 | 0.0123 | 1.0000 | Theoretical lower bound (1.23% mule prevalence) |
| XGBoost (Milestone 2 benchmark) | — | **0.0914** | 0.0500 | 0.6700 | 80% train, 8 graph features, default 0.5 threshold |
| XGBoost (Milestone 3 retrained) | 0.0855 | **0.0834** | 0.1178 | 0.1880 | 70% train, identical val-tuned max-F1 threshold |
| **GraphSAGE (Milestone 3 GNN)** | **0.2091** | **0.1884** | **0.2827** | **0.1495** | **+128% lift over baseline**; 2-hop neighbor sampling (15, 10) |

> **Key Takeaway:** GraphSAGE achieves a **2.28× lift** in PR-AUC over the tabular XGBoost baseline and a **15.3× lift** over the random baseline. Under the optimal F1 operating point, precision improves by **2.4×** (from 11.8% to 28.3%), cutting investigator alert fatigue significantly. Comparative precision-recall curves and training logs are available in `outputs/figures/pr_curve_comparison.png` and `outputs/figures/gnn_training_curves.png`.


---

## Known Limitations

- **Multi-hop depth**: GNN performance degrades with very deep laundering chains due to over-smoothing; deep chains are partially addressed via separate motif search rather than relying on the GNN alone.
- **Time dilution**: Slow, patient laundering (funds held for weeks between hops) is harder to detect than fast pass-through, since the strongest structural signals rely on short holding times.
- **Single-institution visibility**: The model only sees transactions within its own dataset; real-world laundering chains that cross institutions would be partially invisible without cross-bank data sharing.
- **Adaptive adversary**: Unlike static classification problems, laundering patterns can adapt once detection thresholds become known, which this project does not attempt to model.
- This project is framed as a **risk prioritization tool** (surfacing the most suspicious accounts for human review), not a standalone fraud determination system.

---

## Tech Stack

`Python` · `pandas` · `networkx` · `scikit-learn` · `XGBoost` · `PyTorch Geometric` (GraphSAGE, GNNExplainer) · `PySpark` (RDD, SQL, MLlib, Structured Streaming) · `Delta Lake` / `Cassandra CQL` · `Streamlit` · `FastAPI`
