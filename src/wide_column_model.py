"""Milestone 6E: Wide-Column Data Model Implementation with Delta Lake & Cassandra Emulation.

Provides concrete implementations of wide-column database patterns for Anti-Money Laundering
(AML) systems, runnable on Databricks Community Edition or local Spark clusters via Delta Lake.

Maps Cassandra NoSQL wide-column concepts to Delta Lake operations:
- Partition Keys -> Delta Lake partitionBy (e.g. txn_date, alert_date)
- Clustering Keys -> Local Z-Ordering or sorted writes
- Wide Rows -> Arbitrary column expansion and embedded array/map struct types
- Dynamic Schema -> Delta Lake schema evolution (mergeSchema = True)
- High-throughput Upserts -> Delta Lake MERGE INTO (ACID Upserts)
- Audit Lineage -> Delta Lake Time Travel (VERSION AS OF, RESTORE)
"""

from __future__ import annotations

import logging
import os
import shutil
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


def create_spark_session(app_name: str = "ChainReaction-WideColumn") -> SparkSession:
    """Configures a Spark session equipped with Delta Lake capabilities."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    try:
        spark = builder.getOrCreate()
    except Exception as exc:
        # Fallback to standard session if Delta package jar is not on classpath in local test
        logger.warning("Delta Lake extension not detected on classpath, running with standard Parquet fallback: %s", exc)
        spark = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
    spark.sparkContext.setLogLevel("WARN")
    return spark


class WideColumnStore:
    """Manages wide-column physical storage tables, schema evolution, and queries."""

    def __init__(self, spark: SparkSession, base_storage_dir: str = "outputs/wide_column_store") -> None:
        self.spark = spark
        self.base_dir = os.path.abspath(base_storage_dir)
        self.tx_events_path = os.path.join(self.base_dir, "transaction_events")
        self.account_profiles_path = os.path.join(self.base_dir, "account_profiles")
        self.streaming_alerts_path = os.path.join(self.base_dir, "streaming_alerts")

        # Determine storage format (delta if available, otherwise parquet)
        try:
            test_df = self.spark.createDataFrame([(1,)], ["id"])
            test_df.write.format("delta").mode("overwrite").save(os.path.join(self.base_dir, "_format_test"))
            shutil.rmtree(os.path.join(self.base_dir, "_format_test"), ignore_errors=True)
            self.format = "delta"
        except Exception:
            self.format = "parquet"

        logger.info("Storage engine initialized using format='%s' at '%s'", self.format, self.base_dir)

    def create_tables(self, transactions_df: DataFrame, account_features_df: DataFrame) -> Dict[str, str]:
        """Creates physical tables adhering to wide-column schema standards."""
        logger.info("Persisting Wide-Column tables...")

        # 1. TRANSACTION EVENTS TABLE
        # Cassandra mapping: partition key = account_id, clustering = timestamp DESC
        # Delta mapping: partitioned by txn_date with sorted timestamp
        tx_prepared = (
            transactions_df
            .withColumn("txn_date", F.to_date(F.col("timestamp")))
            .sort("timestamp", ascending=False)
        )

        tx_prepared.write.format(self.format) \
            .mode("overwrite") \
            .partitionBy("txn_date") \
            .save(self.tx_events_path)

        # 2. ACCOUNT PROFILES (WIDE ROW PER ACCOUNT)
        # Cassandra mapping: 1 primary key (account_id) with 20+ feature columns
        profiles_prepared = account_features_df.withColumn(
            "last_updated", F.current_timestamp()
        )

        # Ensure collection-like columns exist to match Cassandra schema
        if "last_10_txn_amounts" not in profiles_prepared.columns:
            profiles_prepared = profiles_prepared.withColumn(
                "last_10_txn_amounts", F.array(F.lit(100.0), F.lit(250.0), F.lit(50.0))
            )

        profiles_prepared.write.format(self.format) \
            .mode("overwrite") \
            .save(self.account_profiles_path)

        # Register SQL Views
        self.spark.read.format(self.format).load(self.tx_events_path).createOrReplaceTempView("aml_transaction_events")
        self.spark.read.format(self.format).load(self.account_profiles_path).createOrReplaceTempView("aml_account_profiles")

        logger.info("Tables created and registered: aml_transaction_events, aml_account_profiles")
        return {
            "transaction_events": self.tx_events_path,
            "account_profiles": self.account_profiles_path,
        }

    def demonstrate_schema_evolution(self, new_feature_df: DataFrame) -> DataFrame:
        """Demonstrates adding new feature columns without modifying existing storage.

        Mimics Cassandra CQL:
            ALTER TABLE aml.account_profiles ADD spark_gbt_risk_score DOUBLE;
        """
        logger.info("Executing Wide-Column Schema Evolution...")

        # Load existing table
        current_profiles = self.spark.read.format(self.format).load(self.account_profiles_path)

        # Join new columns
        evolved_df = current_profiles.join(new_feature_df, on="account", how="left")

        # Save with schema merging enabled
        if self.format == "delta":
            evolved_df.write.format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(self.account_profiles_path)
        else:
            evolved_df.write.format("parquet") \
                .mode("overwrite") \
                .save(self.account_profiles_path)

        # Refresh temp view
        evolved_profiles = self.spark.read.format(self.format).load(self.account_profiles_path)
        evolved_profiles.createOrReplaceTempView("aml_account_profiles")

        logger.info("Schema evolved successfully. Current column count: %d", len(evolved_profiles.columns))
        return evolved_profiles

    def demonstrate_upsert(self, updates_df: DataFrame) -> DataFrame:
        """Performs ACID Upsert into the wide-row account profiles table.

        Mimics Cassandra CQL UPDATE:
            UPDATE aml.account_profiles SET graphsage_risk_score = 0.89 WHERE account_id = '8000EBD30';
        """
        logger.info("Performing Wide-Column Upsert (MERGE INTO)...")

        updates_df.createOrReplaceTempView("profile_updates")

        if self.format == "delta":
            try:
                from delta.tables import DeltaTable
                delta_table = DeltaTable.forPath(self.spark, self.account_profiles_path)

                delta_table.alias("target").merge(
                    updates_df.alias("source"),
                    "target.account = source.account"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
            except Exception as exc:
                logger.warning("DeltaTable API merge fallback to SQL/DataFrame: %s", exc)
                self._upsert_fallback(updates_df)
        else:
            self._upsert_fallback(updates_df)

        updated_view = self.spark.read.format(self.format).load(self.account_profiles_path)
        updated_view.createOrReplaceTempView("aml_account_profiles")
        return updated_view

    def _upsert_fallback(self, updates_df: DataFrame) -> None:
        """Fallback upsert using anti-join + union for standard environments."""
        current = self.spark.read.format(self.format).load(self.account_profiles_path)
        untouched = current.join(updates_df.select("account"), on="account", how="left_anti")
        merged = untouched.unionByName(updates_df, allowMissingColumns=True)
        merged.write.format(self.format).mode("overwrite").save(self.account_profiles_path)

    def demonstrate_time_travel(self) -> Optional[DataFrame]:
        """Queries historical versions of the account profile table.

        Provides complete regulatory auditability for AML Model Governance.
        """
        if self.format != "delta":
            logger.info("Time-travel history requires Delta Lake engine. Skipping on Parquet fallback.")
            return None

        logger.info("Querying Delta Lake Time-Travel History...")
        history_df = self.spark.sql("DESCRIBE HISTORY delta.`{}`".format(self.account_profiles_path))
        return history_df

    def simulate_cassandra_queries(self, sample_account_id: str) -> Dict[str, DataFrame]:
        """Demonstrates how typical Cassandra query patterns run on this data model."""
        logger.info("Executing Cassandra query emulation for account '%s'...", sample_account_id)

        # 1. Single Wide Row Lookup (Primary Key query)
        q1 = self.spark.sql(f"""
            SELECT * FROM aml_account_profiles
            WHERE account = '{sample_account_id}'
        """)

        # 2. Time-Series Partition Slice (Account Ego Subgraph, latest 10 events)
        q2 = self.spark.sql(f"""
            SELECT timestamp, from_account, to_account, amount_paid_usd, is_laundering
            FROM aml_transaction_events
            WHERE from_account = '{sample_account_id}' OR to_account = '{sample_account_id}'
            ORDER BY timestamp DESC
            LIMIT 10
        """)

        # 3. High-Risk Triage Query (Materialized View Emulation)
        q3 = self.spark.sql("""
            SELECT account, in_degree, out_degree, total_in, total_out, is_laundering
            FROM aml_account_profiles
            WHERE in_degree > 5 AND out_degree > 3
            ORDER BY total_in DESC
            LIMIT 10
        """)

        return {
            "single_account_lookup": q1,
            "account_recent_events": q2,
            "high_risk_cluster_triage": q3,
        }


# ---------------------------------------------------------------------------
# Verification & Self-Test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = create_spark_session()
    store = WideColumnStore(spark, base_storage_dir="outputs/wide_column_store")

    # Generate synthetic accounts and events for standalone demonstration
    sample_accounts = spark.createDataFrame([
        ("8000EBD30", 12, 4, 154000.0, 149000.0, 120.0, True, 16, 0.0042, 1),
        ("8000F4580", 1, 15, 5000.0, 4800.0, 3600.0, True, 16, 0.0011, 0),
        ("8000F5340", 25, 2, 850000.0, 840000.0, 45.0, True, 27, 0.0125, 1),
    ], [
        "account", "in_degree", "out_degree", "total_in", "total_out",
        "avg_holding_time", "has_outflow_after_inflow", "unique_counterparties",
        "pagerank", "is_laundering"
    ])

    sample_txns = spark.createDataFrame([
        ("8000EBD30", "ACCT_A", "2026-10-25 08:30:00", 50000.0, 50000.0, 1),
        ("8000EBD30", "ACCT_B", "2026-10-25 09:15:00", 45000.0, 45000.0, 1),
        ("ACCT_C", "8000EBD30", "2026-10-25 07:00:00", 95000.0, 95000.0, 1),
        ("8000F5340", "ACCT_D", "2026-10-25 10:00:00", 120000.0, 120000.0, 1),
    ], ["from_account", "to_account", "timestamp", "amount_paid_usd", "amount_received_usd", "is_laundering"])

    sample_txns = sample_txns.withColumn("timestamp", F.to_timestamp("timestamp"))

    print("\n--- 1. Creating Wide-Column Physical Tables ---")
    store.create_tables(sample_txns, sample_accounts)

    print("\n--- 2. Demonstrating Wide-Column Schema Evolution ---")
    # Add new ML risk scores without altering old records
    new_features = spark.createDataFrame([
        ("8000EBD30", 0.942, "CRITICAL"),
        ("8000F4580", 0.125, "LOW"),
        ("8000F5340", 0.988, "CRITICAL"),
    ], ["account", "spark_gbt_risk_score", "risk_tier"])

    evolved = store.demonstrate_schema_evolution(new_features)
    evolved.select("account", "in_degree", "total_in", "spark_gbt_risk_score", "risk_tier").show(truncate=False)

    print("\n--- 3. Demonstrating Query Emulation (Cassandra Semantics) ---")
    queries = store.simulate_cassandra_queries("8000EBD30")

    print("\n[A] Single Account Profile Wide-Row Lookup:")
    queries["single_account_lookup"].select("account", "in_degree", "total_in", "spark_gbt_risk_score", "risk_tier").show(truncate=False)

    print("\n[B] Account Ego-Subgraph Recent Events (Clustering Order):")
    queries["account_recent_events"].show(truncate=False)

    print("\n[C] High-Risk Alert Triage (Materialized View):")
    queries["high_risk_cluster_triage"].show(truncate=False)

    spark.stop()
    print("Wide-Column model demo completed successfully.")

