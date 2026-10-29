"""Milestone 5: Distributed Feature Engineering with PySpark.

Re-implements Milestone 2's account-level graph feature extraction
using PySpark DataFrames, Window functions, and scalable aggregations
designed to scale to HI-Medium and HI-Large datasets (10M-100M+ edges).
"""

import os
import sys
from typing import Optional
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window


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


def create_spark_session(app_name: str = "ChainReaction-FeatureEngineering") -> SparkSession:
    """Initializes or retrieves a local PySpark session with memory tuning."""
    _init_windows_hadoop()
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def compute_spark_account_features(
    spark: SparkSession,
    transactions_csv_path: str,
    max_rows: Optional[int] = None,
) -> DataFrame:
    """Extracts per-account graph and flow features in PySpark.

    Features computed per account:
    - in_degree: Count of incoming transactions
    - out_degree: Count of outgoing transactions
    - total_in: Total USD received
    - total_out: Total USD forwarded
    - avg_holding_time: Average time in seconds between receiving and forwarding
    - has_outflow_after_inflow: Binary flag (1 if account forwarded funds after deposit)
    - unique_counterparties: Total distinct senders and receivers
    - is_laundering: Target label (1 if involved in any laundering transaction)
    """
    # 1. Ingest transactions
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(transactions_csv_path)
    )

    if max_rows is not None:
        df = df.limit(max_rows)

    # Standardize column types
    df = df.withColumn(
        "Timestamp",
        F.coalesce(
            F.to_timestamp(F.col("Timestamp"), "yyyy/MM/dd HH:mm"),
            F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col("Timestamp"))
        )
    )
    df = df.withColumn("amount_paid_usd", F.col("amount_paid_usd").cast("double"))
    df = df.withColumn("amount_received_usd", F.col("amount_received_usd").cast("double"))
    df = df.withColumn("Is Laundering", F.col("Is Laundering").cast("integer"))

    # 2. Inbound aggregations
    inflow = df.groupBy("to_Account").agg(
        F.count("*").alias("in_degree"),
        F.sum("amount_received_usd").alias("total_in"),
        F.collect_set("from_Account").alias("in_counterparties"),
        F.max("Is Laundering").alias("in_laundering"),
    ).withColumnRenamed("to_Account", "account")

    # 3. Outbound aggregations
    outflow = df.groupBy("from_Account").agg(
        F.count("*").alias("out_degree"),
        F.sum("amount_paid_usd").alias("total_out"),
        F.collect_set("to_Account").alias("out_counterparties"),
        F.max("Is Laundering").alias("out_laundering"),
    ).withColumnRenamed("from_Account", "account")

    # 4. Full outer join on account
    accounts = inflow.join(outflow, on="account", how="full_outer")
    accounts = accounts.na.fill({
        "in_degree": 0,
        "out_degree": 0,
        "total_in": 0.0,
        "total_out": 0.0,
        "in_laundering": 0,
        "out_laundering": 0,
    })

    # Combined laundering label
    accounts = accounts.withColumn(
        "is_laundering",
        F.when((F.col("in_laundering") == 1) | (F.col("out_laundering") == 1), 1).otherwise(0)
    ).drop("in_laundering", "out_laundering")

    # Distinct counterparties count
    accounts = accounts.withColumn(
        "unique_counterparties",
        F.size(F.array_distinct(F.concat(
            F.coalesce(F.col("in_counterparties"), F.array()),
            F.coalesce(F.col("out_counterparties"), F.array())
        )))
    ).drop("in_counterparties", "out_counterparties")

    # 5. Distributed Holding Time Computation
    # Unify inbound and outbound transaction events with timestamps
    in_events = df.select(
        F.col("to_Account").alias("account"),
        F.col("Timestamp").alias("in_time"),
    )
    out_events = df.select(
        F.col("from_Account").alias("account"),
        F.col("Timestamp").alias("out_time"),
    )

    # Join events where out_time >= in_time and find closest subsequent outflow
    events_joined = in_events.join(out_events, on="account", how="inner").filter(
        F.col("out_time") >= F.col("in_time")
    )

    # Window over account partitioned by in_time to get the earliest succeeding outflow
    window_spec = Window.partitionBy("account", "in_time").orderBy("out_time")
    first_out = events_joined.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)

    holding_times = first_out.withColumn(
        "dwell_seconds",
        F.col("out_time").cast("long") - F.col("in_time").cast("long")
    ).groupBy("account").agg(
        F.avg("dwell_seconds").alias("avg_holding_time"),
        F.lit(1).alias("has_outflow_after_inflow")
    )

    # Join holding time stats back to accounts
    final_features = accounts.join(holding_times, on="account", how="left").na.fill({
        "avg_holding_time": 0.0,
        "has_outflow_after_inflow": 0
    })

    return final_features


if __name__ == "__main__":
    print("Testing PySpark Feature Engineering Pipeline...")
    spark = create_spark_session()
    csv_file = "./data/processed/processed_df.csv"
    
    # Run on sample of 50,000 transactions to verify distributed logic
    features = compute_spark_account_features(spark, csv_file, max_rows=50000)
    print(f"Extracted features for {features.count()} unique accounts.")
    features.select("account", "in_degree", "out_degree", "total_in", "total_out", "avg_holding_time", "unique_counterparties", "is_laundering").show(5)
    spark.stop()
    print("PySpark execution completed successfully.")
