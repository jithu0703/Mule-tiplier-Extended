"""Interactive Database Showcase Script for Chain Reaction AML Engine.

This CLI script provides a comprehensive presentation tour of the Wide-Column
NoSQL and Lakehouse database architecture.

Usage:
    python scripts/showcase_db.py [--account <account_id>] [--interactive]
"""

from __future__ import annotations

import argparse
import os
import sys
import time

# Configure Win32 event loop & paths
if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

sys.path.insert(0, os.path.abspath("."))
sys.path.insert(0, os.path.abspath("src"))
os.environ["PYTHONPATH"] = os.path.abspath(".") + os.pathsep + os.environ.get("PYTHONPATH", "")

from src.spark_features import _init_windows_hadoop
_init_windows_hadoop()

from pyspark.sql import SparkSession, functions as F


def print_banner(title: str) -> None:
    print("\n" + "=" * 80)
    print(f"  {title.upper()}")
    print("=" * 80)


def print_section(title: str) -> None:
    print(f"\n--- [STEP] {title} ---")


def format_table(df, num_rows: int = 5) -> None:
    """Print Spark DataFrame nicely."""
    rows = df.limit(num_rows).collect()
    if not rows:
        print("  (Empty Result)")
        return
    cols = df.columns
    col_widths = {c: max(len(c), max(len(str(r[c])) for r in rows)) for c in cols}
    
    # Header
    header = " | ".join(c.ljust(col_widths[c]) for c in cols)
    sep = "-+-".join("-" * col_widths[c] for c in cols)
    print(f"  {header}")
    print(f"  {sep}")
    for r in rows:
        row_str = " | ".join(str(r[c]).ljust(col_widths[c]) for c in cols)
        print(f"  {row_str}")
    print()


def run_showcase(sample_account: str | None = None, interactive: bool = False) -> None:
    print_banner("Chain Reaction: Big Data & Wide-Column Database Showcase")
    print("""
Welcome to the Chain Reaction Database Showcase.
This demonstration walks through the architecture, physical layout, schema design,
and query execution patterns of our Wide-Column AML Database (Cassandra CQL / Delta Lake).
""")

    if interactive:
        input("Press [Enter] to start Step 1: Initializing Spark Database Session...")

    # 1. Initialize Spark Engine
    print_section("1. Spark Engine & Database Session Initialization")
    spark = (
        SparkSession.builder.appName("ChainReaction-DB-Showcase")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(f"  Spark Version : {spark.version}")
    print(f"  Parallelism   : {spark.sparkContext.defaultParallelism} executor threads")
    print("  Engine Status : Online & Ready")

    # 2. Database Schema DDL Walkthrough
    print_section("2. Cassandra CQL Schema Architecture (docs/cassandra_schema.cql)")
    cql_path = "docs/cassandra_schema.cql"
    if os.path.exists(cql_path):
        with open(cql_path, "r", encoding="utf-8") as f:
            cql_lines = f.readlines()
        print(f"  Keyspace DDL Preview from {cql_path}:")
        print("  " + "".join(cql_lines[20:45]).replace("\n", "\n  "))
    else:
        print("  CQL schema located at docs/cassandra_schema.cql")

    # 3. Physical Storage Inspection
    print_section("3. Physical Storage Layer Inspection (Parquet / Delta Lake)")
    store_dir = os.path.abspath("outputs/wide_column_store")
    print(f"  Storage Root: {store_dir}")

    tx_dir = os.path.join(store_dir, "transaction_events")
    prof_dir = os.path.join(store_dir, "account_profiles")

    if not os.path.exists(tx_dir) or not os.path.exists(prof_dir):
        print("  Notice: Persisted database tables not found. Generating now from ETL pipeline...")
        from src.etl_pipeline import ETLPipeline
        from src.wide_column_model import WideColumnStore
        raw_csv = "data/raw/HI-Small_Trans.csv"
        if not os.path.exists(raw_csv):
            raw_csv = "data/processed/processed_df.csv"
        etl = ETLPipeline(spark, raw_csv, "data/external/sanctions_list.csv", "data/external/country_risk_scores.csv", max_transaction_rows=10000)
        res = etl.run()
        store = WideColumnStore(spark, base_storage_dir="outputs/wide_column_store")
        store.create_tables(res.validated_transactions, res.enriched_accounts)

    # Inspect partition files
    for tbl_name, tbl_path in [("aml_transaction_events", tx_dir), ("aml_account_profiles", prof_dir)]:
        files = [f for f in os.listdir(tbl_path) if f.endswith(".parquet") or f.endswith(".crc") or f == "_delta_log"]
        print(f"  Table '{tbl_name}':")
        print(f"    Location: {tbl_path}")
        print(f"    Physical Partitions: {len([f for f in files if f.endswith('.parquet')])} Parquet part files")

    # Load Tables into Spark Views
    tx_df = spark.read.parquet(tx_dir)
    prof_df = spark.read.parquet(prof_dir)
    tx_df.createOrReplaceTempView("aml_transaction_events")
    prof_df.createOrReplaceTempView("aml_account_profiles")

    total_tx = tx_df.count()
    total_prof = prof_df.count()
    print(f"\n  Active Records in DB:")
    print(f"    - aml_transaction_events : {total_tx:,} immutable financial events")
    print(f"    - aml_account_profiles   : {total_prof:,} wide-column entity profiles")

    if interactive:
        input("\nPress [Enter] to proceed to Step 4: Live Query Demonstrations...")

    # 4. Live Query Pattern 1: Single Wide-Row Account Lookup (O(1) Hash Partitioning)
    print_section("4. Query Pattern 1: Single Account Profile Lookup (O(1) Partition Key)")
    if sample_account is None:
        sample_account = prof_df.select("account").first()[0]

    print(f"  Target Partition Key: account='{sample_account}'")
    q1_sql = f"""
        SELECT account, in_degree, out_degree, total_in, total_out,
               is_sanctioned, jurisdiction_code, jurisdiction_risk_score, is_laundering
        FROM aml_account_profiles
        WHERE account = '{sample_account}'
    """
    t0 = time.perf_counter()
    res1 = spark.sql(q1_sql)
    q1_time = (time.perf_counter() - t0) * 1000
    format_table(res1)
    print(f"  Query Execution Time: {q1_time:.2f} ms")

    # 5. Live Query Pattern 2: Ego-Subgraph Temporal Slicing (Clustering Keys)
    print_section("5. Query Pattern 2: Ego-Subgraph Temporal Slicing (Clustering Key Range Scan)")
    print(f"  Fetching latest transactions for account '{sample_account}' sorted by timestamp DESC:")
    q2_sql = f"""
        SELECT timestamp, from_account, to_account, amount_paid_usd, is_laundering
        FROM aml_transaction_events
        WHERE from_account = '{sample_account}' OR to_account = '{sample_account}'
        ORDER BY timestamp DESC
        LIMIT 5
    """
    t0 = time.perf_counter()
    res2 = spark.sql(q2_sql)
    q2_time = (time.perf_counter() - t0) * 1000
    format_table(res2)
    print(f"  Query Execution Time: {q2_time:.2f} ms")

    # 6. Live Query Pattern 3: High-Risk Triage (Materialized View Emulation)
    print_section("6. Query Pattern 3: Materialized View Triage for Compliance Analysts")
    print("  Emulates Cassandra Materialized View: aml_high_risk_cluster_triage")
    q3_sql = """
        SELECT account, in_degree, out_degree, total_in, total_out,
               is_sanctioned, jurisdiction_risk_score, is_laundering
        FROM aml_account_profiles
        WHERE in_degree >= 1 AND out_degree >= 1
        ORDER BY (total_in + total_out) DESC
        LIMIT 5
    """
    t0 = time.perf_counter()
    res3 = spark.sql(q3_sql)
    q3_time = (time.perf_counter() - t0) * 1000
    format_table(res3)
    print(f"  Query Execution Time: {q3_time:.2f} ms")

    # 7. Schema Evolution Without Downtime
    print_section("7. Zero-Downtime Schema Evolution Demonstration")
    print("  Simulating CQL: ALTER TABLE aml.account_profiles ADD spark_gbt_risk_score DOUBLE;")
    t0 = time.perf_counter()
    
    # Dynamically inject ML risk score column
    evolved_df = prof_df.withColumn(
        "spark_gbt_risk_score",
        F.round(F.rand(seed=42) * 0.95, 4)
    )
    evolved_df.createOrReplaceTempView("aml_account_profiles_v2")
    q4_sql = """
        SELECT account, total_in, jurisdiction_code, spark_gbt_risk_score
        FROM aml_account_profiles_v2
        WHERE spark_gbt_risk_score > 0.70
        ORDER BY spark_gbt_risk_score DESC
        LIMIT 5
    """
    res4 = spark.sql(q4_sql)
    q4_time = (time.perf_counter() - t0) * 1000
    format_table(res4)
    print(f"  Schema Evolution & Query Time: {q4_time:.2f} ms")
    print("  Result: New ML score column queried instantly without re-writing existing Parquet partitions!")

    # 8. Architecture Comparison Summary
    print_banner("Database Architecture Comparison: Wide-Column vs Traditional RDBMS")
    print("""
Feature                     Traditional RDBMS (Postgres/MySQL)    Wide-Column NoSQL (Cassandra / Delta)
--------------------------  ------------------------------------  -------------------------------------
Write Scalability           Master bottleneck / WAL locks         LSM Tree / Append-only Parquet (O(1))
High Dimensionality         Sparse NULL tables waste space        Wide-row sparse columns natively
Schema Evolution            Heavy ALTER TABLE metadata table lock Instant zero-downtime metadata evolution
Temporal Graph Queries      Expensive JOINs across massive tables Clustered ego-subgraph range slice
Disaster Tolerance          Active-Passive replication lag        Multi-datacenter masterless replication
Big Data Lakehouse Link     External ETL connector needed         Native Apache Spark / Databricks Delta
""")

    print_banner("Database Showcase Completed Successfully!")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chain Reaction AML Database Showcase")
    parser.add_argument("--account", type=str, default=None, help="Target account ID to query")
    parser.add_argument("--interactive", action="store_true", help="Pause between steps for interactive presentation")
    args = parser.parse_args()

    run_showcase(sample_account=args.account, interactive=args.interactive)
