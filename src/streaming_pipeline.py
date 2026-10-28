"""Milestone 6D: Real-Time Structured Streaming & Anomaly Detection Pipeline.

Implements real-time financial stream processing using PySpark Structured Streaming:
1. Micro-batch file stream ingestion monitoring continuous incoming transaction CSVs.
2. Tumbling event-time windows (5-minute windows with 10-minute watermark for late-arriving events).
3. Windowed aggregation capturing rapid fan-in velocity, cumulative volume, and sender dispersion.
4. Anomaly detection rule engine flagging accounts meeting high-velocity money mule signatures.
5. Dual streaming sinks:
   - Interactive Console Sink (for real-time dashboard inspection).
   - Partitioned Parquet Storage Sink with ACID checkpointing (for persistent audit logs).
"""

from __future__ import annotations

import logging
import os
import shutil
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
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


def create_spark_session(app_name: str = "ChainReaction-StructuredStreaming") -> SparkSession:
    """Creates a Spark session tuned for real-time structured streaming."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")  # Low partition count for low streaming latency
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Canonical transaction schema for streaming ingestion
TRANSACTION_STREAM_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("from_account", StringType(), True),
    StructField("to_account", StringType(), True),
    StructField("amount_usd", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("is_laundering", IntegerType(), True),
])


class StreamingAnomalyDetector:
    """Manages Structured Streaming queries, anomaly filters, and active sinks."""

    def __init__(
        self,
        spark: SparkSession,
        incoming_dir: str = "data/stream/incoming",
        output_dir: str = "data/stream/output/alerts",
        checkpoint_dir: str = "data/stream/checkpoints/alerts",
    ) -> None:
        self.spark = spark
        self.incoming_dir = os.path.abspath(incoming_dir)
        self.output_dir = os.path.abspath(output_dir)
        self.checkpoint_dir = os.path.abspath(checkpoint_dir)

        os.makedirs(self.incoming_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.checkpoint_dir, exist_ok=True)

    def read_transaction_stream(self) -> DataFrame:
        """Configures the streaming DataFrame source from directory micro-batches."""
        logger.info("Initializing Structured Stream reader from '%s'...", self.incoming_dir)
        return (
            self.spark.readStream
            .schema(TRANSACTION_STREAM_SCHEMA)
            .option("header", "true")
            .option("maxFilesPerTrigger", 1)  # Ingest 1 micro-batch file per trigger cycle
            .csv(self.incoming_dir)
        )

    def process_stream(self, stream_df: DataFrame) -> DataFrame:
        """Applies watermarking, tumbling window aggregation, and anomaly rules."""
        # 1. Watermark for late data handling (10 minutes tolerance)
        watermarked = stream_df.withWatermark("timestamp", "10 minutes")

        # 2. Tumbling 5-Minute Window Aggregation by Destination Account
        windowed = (
            watermarked.groupBy(
                F.window(F.col("timestamp"), "5 minutes"),
                F.col("to_account").alias("account"),
            )
            .agg(
                F.count("*").alias("window_txn_count"),
                F.round(F.sum("amount_usd"), 2).alias("total_inflow_usd"),
                F.approx_count_distinct("from_account").alias("unique_senders"),
                F.round(F.avg("amount_usd"), 2).alias("avg_txn_size_usd"),
                F.round(F.max("amount_usd"), 2).alias("max_txn_size_usd"),
                F.max("is_laundering").alias("ground_truth_flag"),
            )
        )

        # 3. Anomaly Detection Rules: Rapid Fan-In Mule Signature
        anomalies = windowed.filter(
            (F.col("unique_senders") >= 2)
            & (F.col("total_inflow_usd") >= 5000.0)
            & (F.col("window_txn_count") >= 3)
        )

        # 4. Regulatory Severity Classification
        classified_alerts = (
            anomalies.withColumn(
                "alert_level",
                F.when(
                    (F.col("unique_senders") >= 5) | (F.col("total_inflow_usd") >= 50000.0),
                    F.lit("CRITICAL"),
                )
                .when(
                    (F.col("unique_senders") >= 3) | (F.col("total_inflow_usd") >= 20000.0),
                    F.lit("HIGH"),
                )
                .otherwise(F.lit("MEDIUM")),
            )
            .withColumn("alert_timestamp", F.current_timestamp())
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window")
        )

        return classified_alerts

    def start_pipeline(
        self,
        enable_console: bool = True,
        enable_parquet: bool = True,
        trigger_interval: str = "5 seconds",
        timeout_seconds: Optional[int] = None,
    ) -> list[StreamingQuery]:
        """Launches Structured Streaming queries and registers sinks."""
        raw_stream = self.read_transaction_stream()
        alerts_stream = self.process_stream(raw_stream)

        active_queries: list[StreamingQuery] = []

        # Sink 1: Interactive Console Output (Update Mode)
        if enable_console:
            logger.info("Starting Console Sink query...")
            console_q = (
                alerts_stream.writeStream
                .outputMode("update")
                .format("console")
                .option("truncate", "false")
                .trigger(processingTime=trigger_interval)
                .start()
            )
            active_queries.append(console_q)

        # Sink 2: Persistent Parquet Sink (Append Mode)
        if enable_parquet:
            logger.info("Starting Parquet Storage Sink query at '%s'...", self.output_dir)
            parquet_q = (
                alerts_stream.writeStream
                .outputMode("append")
                .format("parquet")
                .option("path", self.output_dir)
                .option("checkpointLocation", self.checkpoint_dir)
                .trigger(processingTime=trigger_interval)
                .start()
            )
            active_queries.append(parquet_q)

        logger.info("Streaming pipeline launched with %d active queries.", len(active_queries))

        if timeout_seconds is not None and timeout_seconds > 0:
            logger.info("Running streaming queries for %d seconds...", timeout_seconds)
            self.spark.streams.awaitAnyTermination(timeout=timeout_seconds)
            logger.info("Timeout elapsed. Stopping queries...")
            for q in active_queries:
                if q.isActive:
                    q.stop()

        return active_queries


# ---------------------------------------------------------------------------
# Verification & Standalone Demonstration
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = create_spark_session()
    detector = StreamingAnomalyDetector(spark)

    print("\n" + "=" * 65)
    print("CHAIN REACTION: REAL-TIME STRUCTURED STREAMING ANOMALY DETECTOR")
    print("=" * 65)
    print(f"Monitoring folder: {detector.incoming_dir}")
    print(f"Alerts sink:       {detector.output_dir}")
    print("Run `python src/stream_simulator.py` in another terminal to feed transactions.")
    print("=" * 65 + "\n")

    # Start pipeline with a 20-second demonstration timeout
    queries = detector.start_pipeline(
        enable_console=True,
        enable_parquet=False,  # Console only for quick CLI verification
        trigger_interval="5 seconds",
        timeout_seconds=15,
    )

    spark.stop()
    print("Structured Streaming session finished.")

