"""Milestone 6F: Multi-Source Distributed ETL Pipeline with Validation & Verification.

This module provides a production-grade Spark ETL orchestrator that:
1. EXTRACTS from heterogeneous data sources (transactions CSV, sanctions watchlists,
   and country risk indicators).
2. TRANSFORMS with schema mapping, multi-currency USD standardization, timestamp
   enrichment, temporal ordering, and feature aggregations.
3. VALIDATES data quality using 5 deterministic checks (null checks, range checks,
   self-transfer filters, timestamp constraints, statistical outlier flags).
4. VERIFIES data consistency via deduplication, referential integrity verification,
   and sanctions entity resolution.
5. ENRICHES account feature profiles with external FATF risk indices and velocity metrics.
6. LOADS verified datasets for downstream analytics, MLlib pipelines, and Delta Lake stores.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.data_loader import EXCHANGE_RATES_TO_USD

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


def create_spark_session(app_name: str = "ChainReaction-ETL") -> SparkSession:
    """Initializes or retrieves a PySpark session tuned for ETL processing."""
    _init_windows_hadoop()
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Schema Mapping Layer
# ---------------------------------------------------------------------------

class SchemaMapper:
    """Unifies and maps incoming schemas across distinct data providers."""

    @staticmethod
    def map_transactions(df: DataFrame) -> DataFrame:
        """Standardizes raw or pre-processed transaction column schemas."""
        columns = df.columns

        # Standardize sender and receiver account columns across all raw/processed schemas
        if "from_account" in columns and "to_account" in columns:
            pass
        elif "Account2" in columns and "Account4" in columns:
            df = df.withColumnRenamed("Account2", "from_account")
            df = df.withColumnRenamed("Account4", "to_account")
        elif "Account" in columns and "Account.1" in columns:
            df = df.withColumnRenamed("Account", "from_account")
            df = df.withColumnRenamed("Account.1", "to_account")
        elif "from_Account" in columns and "to_Account" in columns:
            df = df.withColumnRenamed("from_Account", "from_account")
            df = df.withColumnRenamed("to_Account", "to_account")
        elif "From Bank" in columns and "To Bank" in columns:
            from_idx = columns.index("From Bank") + 1
            to_idx = columns.index("To Bank") + 1
            df = df.withColumnRenamed(columns[from_idx], "from_account")
            df = df.withColumnRenamed(columns[to_idx], "to_account")

        if "Amount Paid" in columns:
            df = df.withColumnRenamed("Amount Paid", "amount_paid")
        if "Amount Received" in columns:
            df = df.withColumnRenamed("Amount Received", "amount_received")
        if "Payment Currency" in columns:
            df = df.withColumnRenamed("Payment Currency", "payment_currency")
        if "Receiving Currency" in columns:
            df = df.withColumnRenamed("Receiving Currency", "receiving_currency")
        if "Is Laundering" in columns:
            df = df.withColumnRenamed("Is Laundering", "is_laundering")
        elif "is_laundering" not in df.columns:
            df = df.withColumn("is_laundering", F.lit(0))

        # Ensure synthetic unique transaction id if not present
        if "transaction_id" not in df.columns:
            df = df.withColumn(
                "transaction_id",
                F.concat_ws("_", F.col("from_account"), F.col("to_account"), F.col("Timestamp"))
            )

        # Cast types
        df = df.withColumn(
            "timestamp",
            F.coalesce(
                F.to_timestamp(F.col("Timestamp"), "yyyy/MM/dd HH:mm"),
                F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("Timestamp"))
            )
        )
        if "Timestamp" in df.columns and "timestamp" != "Timestamp":
            df = df.drop("Timestamp")

        df = df.withColumn("from_account", F.col("from_account").cast(StringType()))
        df = df.withColumn("to_account", F.col("to_account").cast(StringType()))
        df = df.withColumn("is_laundering", F.col("is_laundering").cast(IntegerType()))

        return df

    @staticmethod
    def map_sanctions(df: DataFrame) -> DataFrame:
        """Standardizes sanctions entity schemas."""
        for col_name in df.columns:
            clean_name = col_name.strip().lower().replace(" ", "_")
            if clean_name != col_name:
                df = df.withColumnRenamed(col_name, clean_name)

        return (
            df.withColumn("entity_id", F.col("entity_id").cast(StringType()))
            .withColumn("risk_score", F.col("risk_score").cast(DoubleType()))
        )

    @staticmethod
    def map_country_risk(df: DataFrame) -> DataFrame:
        """Standardizes country risk rating schemas."""
        for col_name in df.columns:
            clean_name = col_name.strip().lower().replace(" ", "_")
            if clean_name != col_name:
                df = df.withColumnRenamed(col_name, clean_name)

        return (
            df.withColumn("country_code", F.upper(F.trim(F.col("country_code"))).cast(StringType()))
            .withColumn("fatf_risk_level", F.upper(F.trim(F.col("fatf_risk_level"))).cast(StringType()))
            .withColumn("corruption_perception_index", F.col("corruption_perception_index").cast(IntegerType()))
            .withColumn("aml_risk_score", F.col("aml_risk_score").cast(DoubleType()))
        )


# ---------------------------------------------------------------------------
# Data Standardization Layer
# ---------------------------------------------------------------------------

class DataStandardizer:
    """Normalizes transaction amounts and temporal dimensions."""

    @staticmethod
    def standardize_currencies(df: DataFrame, rates: Dict[str, float] = EXCHANGE_RATES_TO_USD) -> DataFrame:
        """Converts heterogeneous currencies into standard USD equivalents."""
        cols = df.columns
        has_usd_paid = "amount_paid_usd" in cols
        has_usd_received = "amount_received_usd" in cols

        if has_usd_paid and has_usd_received:
            return (
                df.withColumn("amount_paid_usd", F.col("amount_paid_usd").cast(DoubleType()))
                .withColumn("amount_received_usd", F.col("amount_received_usd").cast(DoubleType()))
            )

        # Build when/otherwise chains for currency conversion
        paid_expr = F.lit(None).cast(DoubleType())
        for curr, rate in rates.items():
            paid_expr = F.when(F.col("payment_currency") == curr, F.col("amount_paid") * F.lit(rate)).otherwise(paid_expr)

        recv_expr = F.lit(None).cast(DoubleType())
        for curr, rate in rates.items():
            recv_expr = F.when(F.col("receiving_currency") == curr, F.col("amount_received") * F.lit(rate)).otherwise(recv_expr)

        return (
            df.withColumn("amount_paid_usd", paid_expr)
            .withColumn("amount_received_usd", recv_expr)
        )

    @staticmethod
    def standardize_timestamps(df: DataFrame) -> DataFrame:
        """Extracts partitioned temporal features (date, hour, day of week)."""
        return (
            df.withColumn("txn_date", F.to_date(F.col("timestamp")))
            .withColumn("txn_hour", F.hour(F.col("timestamp")))
            .withColumn("day_of_week", F.dayofweek(F.col("timestamp")))
        )


# ---------------------------------------------------------------------------
# Data Quality Validation Layer
# ---------------------------------------------------------------------------

class TransactionValidator:
    """Applies strict data quality rules to isolate anomalies and invalid records."""

    @staticmethod
    def validate(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Evaluates rules and partitions records into valid and rejected sets.

        Returns:
            Tuple of (valid_transactions_df, rejected_transactions_df).
        """
        # Rule 1: Null required fields
        null_filter = (
            F.col("from_account").isNull()
            | F.col("to_account").isNull()
            | F.col("timestamp").isNull()
            | F.col("amount_paid_usd").isNull()
            | F.col("amount_received_usd").isNull()
        )

        # Rule 2: Non-positive transfer amounts
        non_positive_filter = (
            (F.col("amount_paid_usd") <= 0.0)
            | (F.col("amount_received_usd") <= 0.0)
        )

        # Rule 3: Direct self-transfers
        self_transfer_filter = (F.col("from_account") == F.col("to_account"))

        # Rule 4: Future timestamps
        future_filter = (F.col("timestamp") > F.current_timestamp())

        # Compile rejected subsets
        r1 = df.filter(null_filter).withColumn("rejection_reason", F.lit("NULL_REQUIRED_FIELD"))
        r2 = df.filter(~null_filter & non_positive_filter).withColumn("rejection_reason", F.lit("NON_POSITIVE_AMOUNT"))
        r3 = df.filter(~null_filter & ~non_positive_filter & self_transfer_filter).withColumn("rejection_reason", F.lit("SELF_TRANSFER"))
        r4 = df.filter(~null_filter & ~non_positive_filter & ~self_transfer_filter & future_filter).withColumn("rejection_reason", F.lit("FUTURE_TIMESTAMP"))

        rejected_df = r1.unionByName(r2, allowMissingColumns=True) \
                        .unionByName(r3, allowMissingColumns=True) \
                        .unionByName(r4, allowMissingColumns=True)

        # Valid set is the complement
        valid_filter = (~null_filter) & (~non_positive_filter) & (~self_transfer_filter) & (~future_filter)
        valid_df = df.filter(valid_filter)

        # Statistical outlier tagging (informational, not discarded)
        stats = valid_df.select(
            F.mean("amount_paid_usd").alias("mu"),
            F.stddev("amount_paid_usd").alias("sigma"),
        ).first()

        if stats and stats["mu"] is not None and stats["sigma"] is not None:
            threshold = float(stats["mu"] + 3.0 * stats["sigma"])
            valid_df = valid_df.withColumn(
                "is_amount_outlier",
                F.when(F.col("amount_paid_usd") > threshold, 1).otherwise(0)
            )
        else:
            valid_df = valid_df.withColumn("is_amount_outlier", F.lit(0))

        return valid_df, rejected_df


# ---------------------------------------------------------------------------
# Data Verification & Consistency Layer
# ---------------------------------------------------------------------------

class DataVerifier:
    """Validates cross-source referential integrity and integrity constraints."""

    @staticmethod
    def verify_deduplication(df: DataFrame) -> Tuple[DataFrame, int]:
        """Eliminates identical transaction events keeping the earliest occurrence."""
        window_spec = Window.partitionBy(
            "from_account", "to_account", "timestamp", "amount_paid_usd"
        ).orderBy("transaction_id")

        deduped = (
            df.withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )
        return deduped

    @staticmethod
    def verify_sanctions_match(accounts_df: DataFrame, sanctions_df: DataFrame) -> DataFrame:
        """Flags accounts cross-referenced with international sanctions lists."""
        sanctions_subset = sanctions_df.select(
            F.col("entity_id").alias("sanctioned_entity_id"),
            F.col("entity_name").alias("sanctions_entity_name"),
            F.col("sanctions_list").alias("sanctions_program"),
            F.col("risk_score").alias("sanctions_risk_score"),
        )

        matched = accounts_df.join(
            sanctions_subset,
            accounts_df["account"] == sanctions_subset["sanctioned_entity_id"],
            how="left",
        )

        return (
            matched.withColumn(
                "is_sanctioned",
                F.when(F.col("sanctioned_entity_id").isNotNull(), 1).otherwise(0),
            )
            .withColumn(
                "sanctions_risk_score",
                F.coalesce(F.col("sanctions_risk_score"), F.lit(0.0)),
            )
            .drop("sanctioned_entity_id")
        )

    @staticmethod
    def verify_referential_integrity(transactions_df: DataFrame, accounts_df: DataFrame) -> Dict[str, int]:
        """Validates that all transaction counterparties exist in the master accounts catalog."""
        distinct_tx_senders = transactions_df.select(F.col("from_account").alias("acct")).distinct()
        distinct_tx_receivers = transactions_df.select(F.col("to_account").alias("acct")).distinct()
        account_catalog = accounts_df.select(F.col("account").alias("acct")).distinct()

        orphan_senders = distinct_tx_senders.join(account_catalog, on="acct", how="left_anti").count()
        orphan_receivers = distinct_tx_receivers.join(account_catalog, on="acct", how="left_anti").count()

        return {
            "orphan_senders_count": orphan_senders,
            "orphan_receivers_count": orphan_receivers,
        }


# ---------------------------------------------------------------------------
# Data Enrichment Layer
# ---------------------------------------------------------------------------

class DataEnricher:
    """Enriches account profiles with macro country risks and velocity dynamics."""

    @staticmethod
    def enrich_with_country_risk(accounts_df: DataFrame, country_risk_df: DataFrame) -> DataFrame:
        """Simulates jurisdiction risk attribution using deterministic account hashing."""
        # Synthesize country mapping using modulo hash of account string to realistic country codes
        country_codes = [row["country_code"] for row in country_risk_df.select("country_code").distinct().collect()]
        num_countries = len(country_codes)

        # Hash account id to array index
        hash_expr = F.abs(F.hash(F.col("account"))) % num_countries
        codes_lit = F.array([F.lit(c) for c in country_codes])

        with_country = accounts_df.withColumn("jurisdiction_code", codes_lit.getItem(hash_expr))

        enriched = with_country.join(
            country_risk_df,
            with_country["jurisdiction_code"] == country_risk_df["country_code"],
            how="left",
        ).drop(country_risk_df["country_code"])

        return (
            enriched.withColumn(
                "jurisdiction_risk_score",
                F.coalesce(F.col("aml_risk_score"), F.lit(0.20)),
            )
            .drop("aml_risk_score")
        )

    @staticmethod
    def enrich_with_transaction_velocity(accounts_df: DataFrame, transactions_df: DataFrame) -> DataFrame:
        """Calculates hourly transaction rate and burstiness indicators."""
        hourly_counts = (
            transactions_df.groupBy("from_account", "txn_date", "txn_hour")
            .agg(F.count("*").alias("txns_in_hour"))
            .groupBy("from_account")
            .agg(
                F.max("txns_in_hour").alias("max_txns_per_hour"),
                F.avg("txns_in_hour").alias("avg_txns_per_hour"),
            )
            .withColumnRenamed("from_account", "account")
        )

        return accounts_df.join(hourly_counts, on="account", how="left").na.fill({
            "max_txns_per_hour": 0,
            "avg_txns_per_hour": 0.0,
        })


# ---------------------------------------------------------------------------
# ETL Pipeline Orchestrator
# ---------------------------------------------------------------------------

@dataclass
class ETLResult:
    """Container for validated outputs, metadata, and quality audits."""
    validated_transactions: DataFrame
    rejected_transactions: DataFrame
    enriched_accounts: DataFrame
    quality_report: Dict[str, Any] = field(default_factory=dict)


class ETLPipeline:
    """Orchestrates multi-source ingestion, validation, and enrichment."""

    def __init__(
        self,
        spark: SparkSession,
        transactions_path: str,
        sanctions_path: str,
        country_risk_path: str,
        max_transaction_rows: Optional[int] = None,
    ) -> None:
        self.spark = spark
        self.transactions_path = transactions_path
        self.sanctions_path = sanctions_path
        self.country_risk_path = country_risk_path
        self.max_rows = max_transaction_rows
        self._report: Dict[str, Any] = {}

    def run(self) -> ETLResult:
        """Executes full ETL lifecycle."""
        logger.info("Starting Multi-Source ETL Pipeline execution...")

        # 1. EXTRACT
        raw_tx_df = self.spark.read.option("header", "true").csv(self.transactions_path)
        if self.max_rows is not None:
            raw_tx_df = raw_tx_df.limit(self.max_rows)

        raw_sanctions_df = self.spark.read.option("header", "true").csv(self.sanctions_path)
        raw_country_df = self.spark.read.option("header", "true").csv(self.country_risk_path)

        total_tx_in = raw_tx_df.count()
        total_sanctions_in = raw_sanctions_df.count()
        total_countries_in = raw_country_df.count()

        logger.info(
            "Extracted: %d transactions, %d sanctions records, %d country risk profiles",
            total_tx_in, total_sanctions_in, total_countries_in,
        )

        # 2. SCHEMA MAPPING & STANDARDIZATION
        mapped_tx = SchemaMapper.map_transactions(raw_tx_df)
        mapped_sanctions = SchemaMapper.map_sanctions(raw_sanctions_df)
        mapped_country = SchemaMapper.map_country_risk(raw_country_df)

        std_tx = DataStandardizer.standardize_currencies(mapped_tx)
        std_tx = DataStandardizer.standardize_timestamps(std_tx)

        # 3. VALIDATION
        valid_tx, rejected_tx = TransactionValidator.validate(std_tx)
        valid_tx.cache()
        rejected_tx.cache()

        valid_count = valid_tx.count()
        rejected_count = rejected_tx.count()

        # Rejection metrics breakdown
        rejection_breakdown = (
            rejected_tx.groupBy("rejection_reason")
            .count()
            .rdd.collectAsMap()
        )

        logger.info("Validation Complete: %d valid, %d rejected", valid_count, rejected_count)

        # 4. DEDUPLICATION
        deduped_tx = DataVerifier.verify_deduplication(valid_tx)
        deduped_count = deduped_tx.count()
        duplicates_removed = valid_count - deduped_count

        # 5. AGGREGATE CORE ACCOUNT FEATURES
        inflow = deduped_tx.groupBy("to_account").agg(
            F.count("*").alias("in_degree"),
            F.sum("amount_received_usd").alias("total_in"),
            F.collect_set("from_account").alias("in_counterparties"),
            F.max("is_laundering").alias("in_laundering"),
        ).withColumnRenamed("to_account", "account")

        outflow = deduped_tx.groupBy("from_account").agg(
            F.count("*").alias("out_degree"),
            F.sum("amount_paid_usd").alias("total_out"),
            F.collect_set("to_account").alias("out_counterparties"),
            F.max("is_laundering").alias("out_laundering"),
        ).withColumnRenamed("from_account", "account")

        accounts = inflow.join(outflow, on="account", how="full_outer").na.fill({
            "in_degree": 0,
            "out_degree": 0,
            "total_in": 0.0,
            "total_out": 0.0,
            "in_laundering": 0,
            "out_laundering": 0,
        })

        accounts = accounts.withColumn(
            "is_laundering",
            F.when((F.col("in_laundering") == 1) | (F.col("out_laundering") == 1), 1).otherwise(0),
        ).drop("in_laundering", "out_laundering")

        accounts = accounts.withColumn(
            "unique_counterparties",
            F.size(F.array_distinct(F.concat(
                F.coalesce(F.col("in_counterparties"), F.array()),
                F.coalesce(F.col("out_counterparties"), F.array()),
            ))),
        ).drop("in_counterparties", "out_counterparties")

        # 6. VERIFICATION & ENRICHMENT
        sanctioned_accounts = DataVerifier.verify_sanctions_match(accounts, mapped_sanctions)
        sanctions_match_count = sanctioned_accounts.filter(F.col("is_sanctioned") == 1).count()

        enriched_accounts = DataEnricher.enrich_with_country_risk(sanctioned_accounts, mapped_country)
        enriched_accounts = DataEnricher.enrich_with_transaction_velocity(enriched_accounts, deduped_tx)
        enriched_accounts.cache()

        ref_integrity = DataVerifier.verify_referential_integrity(deduped_tx, enriched_accounts)

        # 7. ASSEMBLE QUALITY REPORT
        self._report = {
            "total_records_by_source": {
                "transactions": total_tx_in,
                "sanctions_watchlists": total_sanctions_in,
                "country_risk_ratings": total_countries_in,
            },
            "records_rejected_by_rule": dict(rejection_breakdown),
            "total_valid_records": deduped_count,
            "deduplication_removed": duplicates_removed,
            "sanctions_matches_found": sanctions_match_count,
            "referential_integrity": ref_integrity,
            "total_accounts_processed": enriched_accounts.count(),
        }

        logger.info("ETL Pipeline completed successfully.")
        return ETLResult(
            validated_transactions=deduped_tx,
            rejected_transactions=rejected_tx,
            enriched_accounts=enriched_accounts,
            quality_report=self._report,
        )

    def generate_quality_report(self) -> Dict[str, Any]:
        """Retrieves summary audit metrics from the latest execution."""
        return self._report


# ---------------------------------------------------------------------------
# Main Execution Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = create_spark_session("ChainReaction-ETL")

    tx_path = "data/processed/processed_df.csv"
    if not os.path.exists(tx_path):
        tx_path = "data/raw/HI-Small_Trans.csv"

    sanctions_path = "data/external/sanctions_list.csv"
    country_path = "data/external/country_risk_scores.csv"

    print(f"\nRunning ETL Pipeline with inputs:\n  Transactions: {tx_path}\n  Sanctions: {sanctions_path}\n  Countries: {country_path}")

    pipeline = ETLPipeline(
        spark=spark,
        transactions_path=tx_path,
        sanctions_path=sanctions_path,
        country_risk_path=country_path,
        max_transaction_rows=100000,  # verification sample
    )

    result = pipeline.run()
    report = pipeline.generate_quality_report()

    print("\n" + "=" * 65)
    print("DATA QUALITY AUDIT & ETL EXECUTION REPORT")
    print("=" * 65)
    print(f"1. Ingestion Volume:")
    for k, v in report["total_records_by_source"].items():
        print(f"   - {k}: {v:,}")

    print(f"\n2. Rejections Breakdown:")
    if report["records_rejected_by_rule"]:
        for k, v in report["records_rejected_by_rule"].items():
            print(f"   - {k}: {v:,}")
    else:
        print("   - No records rejected by validation rules.")

    print(f"\n3. Data Cleaning & Deduplication:")
    print(f"   - Duplicates removed: {report['deduplication_removed']:,}")
    print(f"   - Cleaned transactions: {report['total_valid_records']:,}")

    print(f"\n4. Multi-Source Integration & Enrichment:")
    print(f"   - Sanctions Watchlist Hits: {report['sanctions_matches_found']:,}")
    print(f"   - Total Unique Enriched Accounts: {report['total_accounts_processed']:,}")
    print(f"   - Referential Integrity: {report['referential_integrity']}")
    print("=" * 65)

    print("\nSample Enriched Account Profiles:")
    result.enriched_accounts.select(
        "account", "in_degree", "out_degree", "total_in", "is_sanctioned",
        "jurisdiction_code", "jurisdiction_risk_score", "max_txns_per_hour", "is_laundering"
    ).show(5, truncate=False)

    spark.stop()
    print("ETL complete. Spark session terminated.")

