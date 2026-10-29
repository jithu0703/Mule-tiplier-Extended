"""Milestone 6B: Structured Big Data Analytics with Spark SQL.

Demonstrates advanced Spark SQL analytics on high-volume financial transaction networks:
1. SQL Temp Views registration across transaction ledgers and account feature stores.
2. Topological flow queries: Fan-in aggregation, Fan-out dispersion, Pass-through velocity.
3. Windowed temporal analytics: Hourly transaction rates, dwell times, and velocity spikes.
4. Multi-currency arbitrage detection and foreign exchange volume tracking.
5. Rule-based composite risk scoring and high-risk laundering cluster discovery.
6. Side-by-side comparison between ANSI Spark SQL queries and PySpark DataFrame DSL.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


def _get_short_path(path: str) -> str:
    """Returns 8.3 short path on Windows to avoid whitespace issues in worker spawn."""
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes
            buf = ctypes.create_unicode_buffer(260)
            needed = ctypes.windll.kernel32.GetShortPathNameW(path, buf, 260)
            if 0 < needed < 260:
                return buf.value
        except Exception:
            pass
    return path


def _init_windows_hadoop() -> None:
    """Configures HADOOP_HOME, PYTHONPATH, and PYSPARK_PYTHON for Windows environments."""
    if os.name == "nt":
        import sys
        short_py = _get_short_path(sys.executable)
        os.environ["PYSPARK_PYTHON"] = short_py
        os.environ["PYSPARK_DRIVER_PYTHON"] = short_py

        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, ".."))
        short_root = _get_short_path(project_root)
        os.environ["PYTHONPATH"] = (
            short_root + os.pathsep + project_root + os.pathsep + os.environ.get("PYTHONPATH", "")
        )

        if "HADOOP_HOME" not in os.environ:
            local_hadoop = os.path.join(project_root, "hadoop")
            if os.path.exists(os.path.join(local_hadoop, "bin", "winutils.exe")):
                short_hadoop = _get_short_path(local_hadoop)
                os.environ["HADOOP_HOME"] = short_hadoop
                os.environ["PATH"] = os.path.join(short_hadoop, "bin") + os.pathsep + os.environ.get("PATH", "")


def create_spark_session(app_name: str = "ChainReaction-SparkSQL") -> SparkSession:
    """Initializes or retrieves a PySpark session tuned for SQL analytics."""
    _init_windows_hadoop()
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


class SparkSQLAnalytics:
    """Executes structured AML analytics using Spark SQL engines and temp views."""

    def __init__(
        self,
        spark: SparkSession,
        transactions_df: Optional[DataFrame] = None,
        account_features_df: Optional[DataFrame] = None,
    ) -> None:
        self.spark = spark
        self.tx_df = transactions_df
        self.features_df = account_features_df

        if self.tx_df is not None:
            self.register_transaction_view(self.tx_df)
        if self.features_df is not None:
            self.register_features_view(self.features_df)

    def register_transaction_view(self, df: DataFrame, view_name: str = "transactions") -> None:
        """Registers a transaction DataFrame as a SQL temporary view."""
        # Normalize column names for convenient SQL syntax
        tx = df
        if "from_Account" in tx.columns:
            tx = tx.withColumnRenamed("from_Account", "from_account")
        if "to_Account" in tx.columns:
            tx = tx.withColumnRenamed("to_Account", "to_account")
        if "Timestamp" in tx.columns:
            tx = tx.withColumnRenamed("Timestamp", "timestamp")
        if "Payment Currency" in tx.columns:
            tx = tx.withColumnRenamed("Payment Currency", "payment_currency")
        if "Receiving Currency" in tx.columns:
            tx = tx.withColumnRenamed("Receiving Currency", "receiving_currency")
        if "Is Laundering" in tx.columns:
            tx = tx.withColumnRenamed("Is Laundering", "is_laundering")

        tx.createOrReplaceTempView(view_name)
        logger.info("Registered SQL temporary view: '%s'", view_name)

    def register_features_view(self, df: DataFrame, view_name: str = "account_features") -> None:
        """Registers an account feature DataFrame as a SQL temporary view."""
        # Ensure optional advanced graph/temporal columns exist so all queries succeed portably
        if "has_outflow_after_inflow" not in df.columns:
            df = df.withColumn(
                "has_outflow_after_inflow",
                F.when((F.col("in_degree") > 0) & (F.col("out_degree") > 0), 1).otherwise(0)
            )
        if "avg_holding_time" not in df.columns:
            df = df.withColumn("avg_holding_time", F.lit(1800.0))
        if "pagerank" not in df.columns:
            df = df.withColumn("pagerank", F.lit(0.001))

        df.createOrReplaceTempView(view_name)
        logger.info("Registered SQL temporary view: '%s'", view_name)

    # -----------------------------------------------------------------------
    # Query 1: Fan-in Topology Analysis (Aggregation & Filtering)
    # -----------------------------------------------------------------------
    def query_fan_in_hubs(self, min_counterparties: int = 5) -> Tuple[DataFrame, DataFrame]:
        """Identifies accumulator hubs collecting funds from numerous distinct originators.

        Returns:
            Tuple of (sql_result_df, dataframe_api_result_df).
        """
        sql_query = f"""
            SELECT
                to_account AS account,
                COUNT(DISTINCT from_account) AS unique_senders,
                COUNT(*) AS total_inflow_txns,
                ROUND(SUM(amount_received_usd), 2) AS total_inflow_usd,
                ROUND(AVG(amount_received_usd), 2) AS avg_txn_usd,
                MAX(is_laundering) AS ground_truth_laundering
            FROM transactions
            GROUP BY to_account
            HAVING unique_senders >= {min_counterparties}
            ORDER BY unique_senders DESC, total_inflow_usd DESC
        """
        sql_df = self.spark.sql(sql_query)

        # PySpark DataFrame API Equivalent for architectural benchmarking
        tx = self.spark.table("transactions")
        dsl_df = (
            tx.groupBy("to_account")
            .agg(
                F.countDistinct("from_account").alias("unique_senders"),
                F.count("*").alias("total_inflow_txns"),
                F.round(F.sum("amount_received_usd"), 2).alias("total_inflow_usd"),
                F.round(F.avg("amount_received_usd"), 2).alias("avg_txn_usd"),
                F.max("is_laundering").alias("ground_truth_laundering"),
            )
            .filter(F.col("unique_senders") >= min_counterparties)
            .withColumnRenamed("to_account", "account")
            .orderBy(F.col("unique_senders").desc(), F.col("total_inflow_usd").desc())
        )

        return sql_df, dsl_df

    # -----------------------------------------------------------------------
    # Query 2: Fan-out Topology Analysis (Dispersion Aggregation)
    # -----------------------------------------------------------------------
    def query_fan_out_dispersion(self, min_counterparties: int = 5) -> DataFrame:
        """Identifies distributor accounts rapidly spraying funds to multiple receivers."""
        sql_query = f"""
            SELECT
                from_account AS account,
                COUNT(DISTINCT to_account) AS unique_receivers,
                COUNT(*) AS total_outflow_txns,
                ROUND(SUM(amount_paid_usd), 2) AS total_outflow_usd,
                ROUND(AVG(amount_paid_usd), 2) AS avg_outflow_usd,
                MAX(is_laundering) AS ground_truth_laundering
            FROM transactions
            GROUP BY from_account
            HAVING unique_receivers >= {min_counterparties}
            ORDER BY unique_receivers DESC, total_outflow_usd DESC
        """
        return self.spark.sql(sql_query)

    # -----------------------------------------------------------------------
    # Query 3: Windowed Velocity Analytics (Window LAG / Acceleration)
    # -----------------------------------------------------------------------
    def query_hourly_velocity_bursts(self) -> DataFrame:
        """Uses SQL Window functions (LAG, OVER) to measure sudden transaction acceleration."""
        sql_query = """
            WITH hourly_summary AS (
                SELECT
                    from_account AS account,
                    DATE_TRUNC('hour', timestamp) AS txn_hour,
                    COUNT(*) AS txns_this_hour,
                    ROUND(SUM(amount_paid_usd), 2) AS volume_this_hour
                FROM transactions
                GROUP BY from_account, DATE_TRUNC('hour', timestamp)
            ),
            windowed_metrics AS (
                SELECT
                    account,
                    txn_hour,
                    txns_this_hour,
                    volume_this_hour,
                    LAG(txns_this_hour, 1, 0) OVER (
                        PARTITION BY account ORDER BY txn_hour
                    ) AS txns_prev_hour,
                    LAG(volume_this_hour, 1, 0.0) OVER (
                        PARTITION BY account ORDER BY txn_hour
                    ) AS volume_prev_hour
                FROM hourly_summary
            )
            SELECT
                account,
                txn_hour,
                txns_this_hour,
                txns_prev_hour,
                (txns_this_hour - txns_prev_hour) AS hourly_tx_acceleration,
                volume_this_hour,
                volume_prev_hour,
                ROUND(volume_this_hour - volume_prev_hour, 2) AS hourly_volume_surge_usd
            FROM windowed_metrics
            WHERE txns_this_hour >= 3 AND (txns_this_hour - txns_prev_hour) >= 2
            ORDER BY hourly_tx_acceleration DESC, hourly_volume_surge_usd DESC
        """
        return self.spark.sql(sql_query)

    # -----------------------------------------------------------------------
    # Query 4: Cross-Currency Arbitrage & Multi-Currency Flow
    # -----------------------------------------------------------------------
    def query_cross_currency_flows(self) -> DataFrame:
        """Identifies transactions involving currency hops and significant value shifts."""
        sql_query = """
            SELECT
                from_account,
                to_account,
                payment_currency,
                receiving_currency,
                ROUND(amount_paid_usd, 2) AS amount_paid_usd,
                ROUND(amount_received_usd, 2) AS amount_received_usd,
                ROUND(
                    ((amount_received_usd - amount_paid_usd) / amount_paid_usd) * 100.0,
                    2
                ) AS conversion_variance_pct,
                is_laundering
            FROM transactions
            WHERE payment_currency != receiving_currency
              AND amount_paid_usd > 1000.0
            ORDER BY conversion_variance_pct DESC
        """
        return self.spark.sql(sql_query)

    # -----------------------------------------------------------------------
    # Query 5: Money Mule Pass-Through Signature (Inflow ~ Outflow + Fast Holding)
    # -----------------------------------------------------------------------
    def query_pass_through_mules(self) -> DataFrame:
        """Detects classic money mule accounts: high volume, balanced flow, brief dwell time."""
        sql_query = """
            SELECT
                account,
                in_degree,
                out_degree,
                ROUND(total_in, 2) AS total_in_usd,
                ROUND(total_out, 2) AS total_out_usd,
                ROUND(ABS(total_in - total_out) / ((total_in + total_out) / 2.0) * 100.0, 2) AS flow_asymmetry_pct,
                ROUND(avg_holding_time, 1) AS avg_holding_sec,
                unique_counterparties,
                ROUND(pagerank, 6) AS pagerank,
                is_laundering AS ground_truth
            FROM account_features
            WHERE in_degree >= 2
              AND out_degree >= 1
              AND has_outflow_after_inflow = 1
              AND avg_holding_time <= 7200.0       -- Forwarded within 2 hours
              AND total_in >= 1000.0
              AND ABS(total_in - total_out) <= (0.25 * total_in) -- Within 25% balance
            ORDER BY avg_holding_sec ASC, total_in_usd DESC
        """
        return self.spark.sql(sql_query)

    # -----------------------------------------------------------------------
    # Query 6: Rule-Based Composite Risk Scoring
    # -----------------------------------------------------------------------
    def query_composite_risk_scores(self) -> DataFrame:
        """Computes a multi-tier regulatory risk assessment using CASE WHEN logic."""
        sql_query = """
            SELECT
                account,
                in_degree,
                out_degree,
                ROUND(total_in, 2) AS total_in_usd,
                ROUND(avg_holding_time, 1) AS avg_holding_sec,
                unique_counterparties,
                ROUND(pagerank, 6) AS pagerank,
                CASE
                    WHEN (avg_holding_time < 1800 AND in_degree >= 4 AND out_degree >= 2) THEN 'CRITICAL'
                    WHEN (avg_holding_time < 7200 AND in_degree >= 3) THEN 'HIGH'
                    WHEN (has_outflow_after_inflow = 1 AND unique_counterparties >= 5) THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS rule_risk_tier,
                ROUND(
                    (0.35 * (in_degree / (in_degree + out_degree + 1.0))) +
                    (0.35 * (1.0 / (1.0 + (avg_holding_time / 3600.0)))) +
                    (0.30 * (LEAST(unique_counterparties, 20) / 20.0)),
                    4
                ) AS composite_heuristic_score,
                is_laundering AS ground_truth
            FROM account_features
            ORDER BY composite_heuristic_score DESC, total_in_usd DESC
        """
        return self.spark.sql(sql_query)

    def run_all_analytics(self) -> Dict[str, DataFrame]:
        """Executes all analytical queries and compiles results dictionary."""
        logger.info("Executing full suite of Spark SQL analytics...")
        results = {
            "fan_in_hubs": self.query_fan_in_hubs(min_counterparties=3)[0],
            "fan_out_dispersion": self.query_fan_out_dispersion(min_counterparties=3),
            "hourly_velocity_bursts": self.query_hourly_velocity_bursts(),
            "cross_currency_flows": self.query_cross_currency_flows(),
            "pass_through_mules": self.query_pass_through_mules(),
            "composite_risk_scores": self.query_composite_risk_scores(),
        }
        logger.info("All Spark SQL queries completed successfully.")
        return results


# ---------------------------------------------------------------------------
# Standalone Execution & Verification
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = create_spark_session("ChainReaction-SQL-Demo")

    tx_path = "data/processed/processed_df.csv"
    features_path = "data/processed/account_features.csv"

    if os.path.exists(tx_path) and os.path.exists(features_path):
        print(f"Loading data from disk:\n  {tx_path}\n  {features_path}")
        tx_df = spark.read.option("header", "true").option("inferSchema", "true").csv(tx_path).limit(50000)
        features_df = spark.read.option("header", "true").option("inferSchema", "true").csv(features_path).limit(20000)
    else:
        print("Data files not found at specified paths. Generating sample datasets for verification...")
        tx_df = spark.createDataFrame([
            ("ACCT_01", "ACCT_02", "2026-10-25 08:00:00", 5000.0, 5000.0, "US Dollar", "US Dollar", 1),
            ("ACCT_03", "ACCT_02", "2026-10-25 08:15:00", 4500.0, 4500.0, "Euro", "US Dollar", 1),
            ("ACCT_04", "ACCT_02", "2026-10-25 08:30:00", 8000.0, 8000.0, "Bitcoin", "US Dollar", 1),
            ("ACCT_02", "ACCT_05", "2026-10-25 09:00:00", 17000.0, 17000.0, "US Dollar", "US Dollar", 1),
        ], ["from_account", "to_account", "timestamp", "amount_paid_usd", "amount_received_usd", "payment_currency", "receiving_currency", "is_laundering"])
        tx_df = tx_df.withColumn("timestamp", F.to_timestamp("timestamp"))

        features_df = spark.createDataFrame([
            ("ACCT_02", 3, 1, 17500.0, 17000.0, 1800.0, 1, 4, 0.0054, 1),
            ("ACCT_05", 1, 0, 17000.0, 0.0, 0.0, 0, 1, 0.0012, 1),
        ], ["account", "in_degree", "out_degree", "total_in", "total_out", "avg_holding_time", "has_outflow_after_inflow", "unique_counterparties", "pagerank", "is_laundering"])

    analytics = SparkSQLAnalytics(spark, tx_df, features_df)
    results = analytics.run_all_analytics()

    print("\n" + "=" * 65)
    print("SPARK SQL ANALYTICS: TOP FAN-IN HUBS")
    print("=" * 65)
    results["fan_in_hubs"].show(5, truncate=False)

    print("\n" + "=" * 65)
    print("SPARK SQL ANALYTICS: PASS-THROUGH MULE SIGNATURES")
    print("=" * 65)
    results["pass_through_mules"].show(5, truncate=False)

    print("\n" + "=" * 65)
    print("SPARK SQL ANALYTICS: COMPOSITE RISK SCORING")
    print("=" * 65)
    results["composite_risk_scores"].show(5, truncate=False)

    spark.stop()
    print("Spark SQL demonstration completed.")

