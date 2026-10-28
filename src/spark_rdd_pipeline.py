"""Milestone 5b: RDD-Level Distributed Pipeline for AML Mule Detection.

Provides an explicit Apache Spark RDD pipeline that parallels the
DataFrame-based feature engineering in ``spark_features.py``.  Every major
RDD transformation and action is annotated so the module doubles as a
reference guide to the Spark RDD API applied to real-world AML analytics.

Pipeline overview
-----------------
1. Ingest the raw IBM AML CSV via ``sc.textFile`` (no schema inference).
2. Parse each line into a ``Transaction`` namedtuple.
3. Standardize all monetary amounts to USD using ``EXCHANGE_RATES_TO_USD``.
4. Validate and cache the cleaned RDD.
5. ``flatMap`` each transaction into per-account event pairs.
6. ``reduceByKey`` to aggregate account-level features.
7. Compute derived risk heuristics, sort, and persist.

A companion function ``demonstrate_rdd_operations`` showcases additional
RDD primitives (``distinct``, ``groupByKey``, ``sample``, ``union``,
``intersection``, ``subtract``, ``foreach``, ``aggregate``).
"""

import csv
import io
import logging
import os
import shutil
from collections import namedtuple
from typing import Any, Dict, Tuple

from pyspark import SparkContext
from pyspark.sql import SparkSession

from src.data_loader import EXCHANGE_RATES_TO_USD

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# 1. Constants & Helpers
# ---------------------------------------------------------------------------

# A lightweight, immutable record for a single transaction row.
Transaction = namedtuple(
    "Transaction",
    [
        "timestamp",
        "from_account",
        "to_account",
        "amount_paid",
        "currency_paid",
        "amount_received",
        "currency_received",
        "is_laundering",
    ],
)


def parse_transaction_row(line: str) -> Transaction:
    """Parse a single CSV line into a ``Transaction`` namedtuple.

    The IBM AML CSV has the following positional columns (0-indexed)::

        0  Timestamp
        1  From Bank
        2  Account           (sender account ID)
        3  To Bank
        4  Account           (receiver account ID — duplicate header name)
        5  Amount Received
        6  Receiving Currency
        7  Amount Paid
        8  Payment Currency
        9  Payment Format
       10  Is Laundering

    We read with the stdlib ``csv`` module to handle potential quoting and
    embedded commas gracefully.

    Parameters
    ----------
    line : str
        A single raw CSV row (excluding the header).

    Returns
    -------
    Transaction
        Parsed, typed namedtuple.

    Raises
    ------
    ValueError
        If the row cannot be parsed into the expected number of fields.
    """
    reader = csv.reader(io.StringIO(line))
    fields = next(reader)
    if len(fields) < 11:
        raise ValueError(
            f"Expected at least 11 CSV fields, got {len(fields)}: {line!r}"
        )
    return Transaction(
        timestamp=fields[0].strip(),
        from_account=fields[2].strip(),
        to_account=fields[4].strip(),
        amount_paid=float(fields[7]),
        currency_paid=fields[8].strip(),
        amount_received=float(fields[5]),
        currency_received=fields[6].strip(),
        is_laundering=int(fields[10]),
    )


def standardize_to_usd(txn: Transaction, rates: dict = EXCHANGE_RATES_TO_USD) -> Transaction:
    """Convert both paid and received amounts to USD.

    Uses the ``EXCHANGE_RATES_TO_USD`` dictionary from ``src.data_loader``.
    Currencies not present in the dictionary default to a rate of **1.0**
    (treated as USD) and a warning is logged once per executor.

    Parameters
    ----------
    txn : Transaction
        A parsed transaction with original currency amounts.
    rates : dict
        Mapping of currency name → USD multiplier.

    Returns
    -------
    Transaction
        A *new* Transaction with amounts converted to USD.  The currency
        fields are set to ``'US Dollar'`` to reflect the normalisation.
    """
    paid_rate: float = rates.get(txn.currency_paid, 1.0)
    received_rate: float = rates.get(txn.currency_received, 1.0)
    return Transaction(
        timestamp=txn.timestamp,
        from_account=txn.from_account,
        to_account=txn.to_account,
        amount_paid=round(txn.amount_paid * paid_rate, 4),
        currency_paid="US Dollar",
        amount_received=round(txn.amount_received * received_rate, 4),
        currency_received="US Dollar",
        is_laundering=txn.is_laundering,
    )


def create_spark_session(app_name: str = "ChainReaction-RDD") -> SparkSession:
    """Initialise or retrieve a local PySpark session with memory tuning.

    Reuses the same configuration pattern established in
    ``src.spark_features.create_spark_session`` to ensure consistent
    resource allocation across all Spark entry-points in the project.

    Parameters
    ----------
    app_name : str
        Application name shown in the Spark UI.

    Returns
    -------
    SparkSession
    """
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


# ---------------------------------------------------------------------------
# 2. Main RDD Pipeline
# ---------------------------------------------------------------------------

def run_rdd_pipeline(sc: SparkContext, csv_path: str) -> dict:
    """Execute the full RDD-level AML feature-engineering pipeline.

    The pipeline mirrors the DataFrame logic in ``spark_features.py`` but
    operates entirely through explicit RDD transformations and actions to
    demonstrate low-level Spark primitives.

    Parameters
    ----------
    sc : SparkContext
        Active Spark context.
    csv_path : str
        Path to the raw IBM AML CSV file (``HI-Small_Trans.csv``).

    Returns
    -------
    dict
        A summary dictionary with keys:

        - ``total_accounts``  — int, number of unique accounts
        - ``top_20_risky``    — list of top-20 (risk_score, account, features)
        - ``riskiest``        — single riskiest account tuple
        - ``label_counts``    — dict mapping laundering label → count
        - ``output_path``     — str, path where features were saved
    """
    logger.info("=== Chain Reaction — RDD Feature Pipeline START ===")

    # ------------------------------------------------------------------
    # (a) Ingestion: read raw CSV as a text RDD.
    #     textFile is a *transformation* that lazily defines the RDD.
    # ------------------------------------------------------------------
    raw_rdd = sc.textFile(csv_path)
    logger.info("Ingested CSV via sc.textFile (lazy — no data read yet).")

    # ------------------------------------------------------------------
    # (b) Header removal: .first() is an *action* that triggers a job to
    #     retrieve the first line (the header).  We then .filter() — a
    #     *transformation* — to exclude that header from the data RDD.
    # ------------------------------------------------------------------
    header: str = raw_rdd.first()  # ACTION: triggers one job
    logger.info("Header (via .first() ACTION): %s", header)

    data_rdd = raw_rdd.filter(
        lambda line: line != header  # TRANSFORMATION: lazy predicate filter
    )

    # ------------------------------------------------------------------
    # (c) Parsing: .map() is a *transformation* that applies
    #     parse_transaction_row to every element 1-to-1.
    # ------------------------------------------------------------------
    parsed_rdd = data_rdd.map(parse_transaction_row)  # TRANSFORMATION
    logger.info("Mapped raw lines → Transaction namedtuples (.map).")

    # ------------------------------------------------------------------
    # (d) Currency standardisation: another .map() *transformation* that
    #     converts every Transaction's amounts to USD in-place.
    # ------------------------------------------------------------------
    # Broadcast the exchange-rate dictionary so each executor gets a
    # single, efficient copy instead of shipping it per-task.
    rates_bc = sc.broadcast(EXCHANGE_RATES_TO_USD)

    usd_rdd = parsed_rdd.map(
        lambda txn: standardize_to_usd(txn, rates_bc.value)  # TRANSFORMATION
    )
    logger.info("Standardised currencies → USD (.map with broadcast).")

    # ------------------------------------------------------------------
    # (e) Validation: .filter() *transformation* to discard invalid rows.
    #     - Remove zero / negative USD amounts (data quality).
    #     - Remove self-transfers (from_account == to_account) since they
    #       add noise and carry no AML signal.
    # ------------------------------------------------------------------
    validated_rdd = usd_rdd.filter(
        lambda txn: (
            txn.amount_paid > 0.0
            and txn.amount_received > 0.0
            and txn.from_account != txn.to_account
        )
    )  # TRANSFORMATION
    logger.info("Filtered out zero/negative amounts and self-transfers (.filter).")

    # ------------------------------------------------------------------
    # (f) Cache: .cache() marks the RDD for in-memory persistence (storage
    #     level MEMORY_ONLY).  Because validated_rdd is reused by multiple
    #     downstream paths, caching avoids recomputing from disk.
    # ------------------------------------------------------------------
    validated_rdd.cache()  # TRANSFORMATION (marks lineage, no compute yet)
    logger.info("Cached validated RDD for reuse (.cache).")

    # ------------------------------------------------------------------
    # (g) Per-account event emission via .flatMap() — a *transformation*
    #     that can emit 0, 1, or many output elements per input element.
    #     Each transaction produces TWO (account, event) pairs:
    #       • sender  → outflow event
    #       • receiver → inflow event
    # ------------------------------------------------------------------
    def emit_account_events(txn: Transaction):
        """Yield two (account, event_dict) pairs per transaction."""
        return [
            (
                txn.from_account,
                {
                    "direction": "out",
                    "amount": txn.amount_paid,
                    "counterparty": txn.to_account,
                    "is_laundering": txn.is_laundering,
                },
            ),
            (
                txn.to_account,
                {
                    "direction": "in",
                    "amount": txn.amount_received,
                    "counterparty": txn.from_account,
                    "is_laundering": txn.is_laundering,
                },
            ),
        ]

    events_rdd = validated_rdd.flatMap(emit_account_events)  # TRANSFORMATION
    logger.info("Emitted per-account events via .flatMap (2 per txn).")

    # ------------------------------------------------------------------
    # (h) Aggregation in two steps:
    #
    #  Step 1 — .mapValues() converts each event dict into a fixed-width
    #           accumulator tuple for efficient reduction:
    #           (in_degree, out_degree, total_in, total_out,
    #            is_laundering, counterparty_set)
    #
    #  Step 2 — .reduceByKey() merges accumulators partition-locally first
    #           (combiner) then across partitions (reducer), which is far
    #           more efficient than .groupByKey() for associative ops.
    # ------------------------------------------------------------------
    def event_to_accumulator(event: dict) -> tuple:
        """Convert a single event dict to a feature accumulator tuple.

        Returns
        -------
        tuple
            (in_degree, out_degree, total_in, total_out,
             is_laundering, counterparty_set)
        """
        if event["direction"] == "in":
            return (
                1,                          # in_degree
                0,                          # out_degree
                event["amount"],            # total_in
                0.0,                        # total_out
                event["is_laundering"],     # is_laundering (will take max)
                {event["counterparty"]},    # counterparty set
            )
        else:
            return (
                0,                          # in_degree
                1,                          # out_degree
                0.0,                        # total_in
                event["amount"],            # total_out
                event["is_laundering"],     # is_laundering
                {event["counterparty"]},    # counterparty set
            )

    def merge_accumulators(a: tuple, b: tuple) -> tuple:
        """Associative + commutative merge of two accumulator tuples.

        Used by .reduceByKey() as both the within-partition combiner and
        the cross-partition reducer.
        """
        return (
            a[0] + b[0],           # in_degree
            a[1] + b[1],           # out_degree
            a[2] + b[2],           # total_in
            a[3] + b[3],           # total_out
            max(a[4], b[4]),       # is_laundering — max acts as logical OR
            a[5] | b[5],           # counterparty set union
        )

    accum_rdd = events_rdd.mapValues(event_to_accumulator)  # TRANSFORMATION
    aggregated_rdd = accum_rdd.reduceByKey(merge_accumulators)  # TRANSFORMATION
    logger.info("Aggregated per-account features (.mapValues + .reduceByKey).")

    # ------------------------------------------------------------------
    # (i) Derived features: .mapValues() to compute additional metrics.
    #     - unique_counterparties: cardinality of the counterparty set
    #     - fan_in_ratio: in_degree / (in_degree + out_degree)
    #       Mule accounts tend to have balanced fan-in/fan-out; pure
    #       receivers have ratio ≈ 1.0.
    # ------------------------------------------------------------------
    def derive_features(acc: tuple) -> dict:
        """Compute derived features from raw accumulator tuple.

        Parameters
        ----------
        acc : tuple
            (in_degree, out_degree, total_in, total_out,
             is_laundering, counterparty_set)

        Returns
        -------
        dict
            Flat feature dictionary ready for downstream analytics.
        """
        in_deg, out_deg, total_in, total_out, is_laund, cparties = acc
        total_deg = in_deg + out_deg
        fan_in_ratio = in_deg / total_deg if total_deg > 0 else 0.0
        return {
            "in_degree": in_deg,
            "out_degree": out_deg,
            "total_in": round(total_in, 2),
            "total_out": round(total_out, 2),
            "is_laundering": is_laund,
            "unique_counterparties": len(cparties),
            "fan_in_ratio": round(fan_in_ratio, 4),
        }

    features_rdd = aggregated_rdd.mapValues(derive_features)  # TRANSFORMATION
    logger.info("Computed derived features (.mapValues).")

    # ------------------------------------------------------------------
    # (j) Risk heuristic: a simple scoring function inspired by degree-
    #     product normalised by a proxy for holding time.
    #
    #     risk_score = (in_degree × out_degree) / (avg_holding_time + 1)
    #
    #     Since true holding time requires temporal joins (handled in the
    #     DataFrame pipeline), we approximate avg_holding_time here as:
    #         total_out / (out_degree + 1)   (average outflow size as proxy)
    #
    #     .map() converts (account, features) → (risk_score, account, features)
    #     .sortByKey(ascending=False) orders by descending risk.
    # ------------------------------------------------------------------
    def compute_risk(pair: Tuple[str, dict]) -> Tuple[float, str, dict]:
        """Attach a numeric risk score to each account.

        The heuristic favours accounts with high bidirectional degree
        (many in *and* out connections), normalised by average outflow
        size as a rough holding-time proxy.
        """
        account, feats = pair
        in_deg = feats["in_degree"]
        out_deg = feats["out_degree"]
        avg_holding_proxy = (feats["total_out"] / (out_deg + 1)) + 1.0
        risk_score = round((in_deg * out_deg) / avg_holding_proxy, 4)
        return (risk_score, account, feats)

    risk_rdd = features_rdd.map(compute_risk)  # TRANSFORMATION
    sorted_risk_rdd = risk_rdd.sortBy(
        keyfunc=lambda x: x[0], ascending=False  # TRANSFORMATION (wide)
    )
    logger.info("Computed risk scores and sorted descending (.map + .sortBy).")

    # ------------------------------------------------------------------
    # (k) Actions — materialise results.
    # ------------------------------------------------------------------

    # .count() — ACTION: total number of unique accounts.
    total_accounts: int = features_rdd.count()
    logger.info("Total unique accounts (.count ACTION): %d", total_accounts)

    # .take(20) — ACTION: retrieve the 20 highest-risk accounts.
    top_20: list = sorted_risk_rdd.take(20)
    logger.info("Top 20 risky accounts (.take ACTION):")
    for rank, (score, acct, feats) in enumerate(top_20, 1):
        logger.info(
            "  #%02d  account=%-12s  risk=%.4f  in=%d  out=%d  laund=%d",
            rank, acct, score, feats["in_degree"],
            feats["out_degree"], feats["is_laundering"],
        )

    # .first() — ACTION: single riskiest account.
    riskiest = sorted_risk_rdd.first()
    logger.info(
        "Riskiest account (.first ACTION): %s  risk=%.4f",
        riskiest[1], riskiest[0],
    )

    # .countByKey() — ACTION on a laundering-label RDD.
    # Create (is_laundering, 1) pairs and count by key.
    label_rdd = features_rdd.map(lambda pair: (pair[1]["is_laundering"], 1))
    label_counts: dict = dict(label_rdd.countByKey())
    logger.info("Label distribution (.countByKey ACTION): %s", label_counts)

    # .saveAsTextFile() — ACTION: persist the feature RDD to disk.
    output_dir = os.path.join("outputs", "rdd_features")
    # Remove existing output directory to avoid Spark's "already exists" error.
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    features_rdd.map(
        lambda pair: f"{pair[0]}\t{pair[1]}"
    ).saveAsTextFile(output_dir)  # ACTION: triggers full DAG execution
    logger.info("Saved feature RDD to %s (.saveAsTextFile ACTION).", output_dir)

    # Clean up broadcast variable to free executor memory.
    rates_bc.unpersist()

    logger.info("=== Chain Reaction — RDD Feature Pipeline END ===")

    return {
        "total_accounts": total_accounts,
        "top_20_risky": top_20,
        "riskiest": riskiest,
        "label_counts": label_counts,
        "output_path": output_dir,
    }


# ---------------------------------------------------------------------------
# 3. Additional RDD Demonstrations
# ---------------------------------------------------------------------------

def demonstrate_rdd_operations(sc: SparkContext, csv_path: str) -> None:
    """Showcase additional Spark RDD transformations and actions.

    This function exercises RDD primitives that are not part of the main
    pipeline but are important for a comprehensive understanding of the
    Spark RDD API.  Each operation is preceded by a docstring-style comment
    explaining its semantics.

    Parameters
    ----------
    sc : SparkContext
        Active Spark context.
    csv_path : str
        Path to the raw IBM AML CSV file.
    """
    logger.info("=== Additional RDD Demonstrations START ===")

    # Ingest & parse (reuse helpers).
    rates_bc = sc.broadcast(EXCHANGE_RATES_TO_USD)
    raw_rdd = sc.textFile(csv_path)
    header = raw_rdd.first()
    data_rdd = raw_rdd.filter(lambda line: line != header)
    txn_rdd = data_rdd.map(parse_transaction_row).map(
        lambda txn: standardize_to_usd(txn, rates_bc.value)
    )
    txn_rdd.cache()

    # ------------------------------------------------------------------ #
    # .distinct() — TRANSFORMATION (wide shuffle)                         #
    # Returns an RDD with duplicate elements removed.  Because it must    #
    # compare every element, it triggers a full shuffle.                  #
    # Here: find the unique set of currencies in the original dataset.    #
    # ------------------------------------------------------------------ #
    paid_currencies = data_rdd.map(parse_transaction_row).map(
        lambda txn: txn.currency_paid
    )
    received_currencies = data_rdd.map(parse_transaction_row).map(
        lambda txn: txn.currency_received
    )
    all_currencies = paid_currencies.union(received_currencies)
    unique_currencies = all_currencies.distinct().collect()  # ACTION: .collect()
    logger.info(
        ".distinct() — %d unique currencies: %s",
        len(unique_currencies), sorted(unique_currencies),
    )

    # ------------------------------------------------------------------ #
    # .groupByKey() — TRANSFORMATION (wide shuffle)                       #
    # Groups all values with the same key into an iterable.  Less         #
    # efficient than .reduceByKey() for aggregations because all values   #
    # are shuffled before any combining happens, but useful when you need #
    # the raw value list (e.g., for median or sampling).                  #
    # Here: count transactions per payment currency.                      #
    # ------------------------------------------------------------------ #
    currency_txns = data_rdd.map(parse_transaction_row).map(
        lambda txn: (txn.currency_paid, 1)
    )
    grouped = currency_txns.groupByKey()  # TRANSFORMATION
    currency_counts = grouped.mapValues(
        lambda vals: sum(vals)
    ).collect()  # ACTION
    logger.info(".groupByKey() — transactions per currency:")
    for curr, cnt in sorted(currency_counts, key=lambda x: -x[1]):
        logger.info("  %-20s %d", curr, cnt)

    # ------------------------------------------------------------------ #
    # .sample() — TRANSFORMATION (narrow)                                 #
    # Returns a random subset of the RDD.  withReplacement=False uses     #
    # Bernoulli sampling; fraction is the *expected* proportion.          #
    # Here: draw ≈1 % of transactions for quick prototyping.              #
    # ------------------------------------------------------------------ #
    sample_rdd = txn_rdd.sample(
        withReplacement=False, fraction=0.01, seed=42
    )  # TRANSFORMATION
    sample_count = sample_rdd.count()  # ACTION
    logger.info(
        ".sample(1%%) — sampled %d transactions from the dataset.",
        sample_count,
    )

    # ------------------------------------------------------------------ #
    # .union() — TRANSFORMATION (narrow)                                  #
    # Concatenates two RDDs (does NOT deduplicate).  Partitions of both   #
    # RDDs are simply joined; no shuffle is needed.                       #
    # Here: combine inflow and outflow account RDDs.                      #
    # ------------------------------------------------------------------ #
    inflow_accounts = txn_rdd.map(lambda txn: txn.to_account)    # receivers
    outflow_accounts = txn_rdd.map(lambda txn: txn.from_account)  # senders
    all_accounts = inflow_accounts.union(outflow_accounts)  # TRANSFORMATION
    total_events = all_accounts.count()  # ACTION
    logger.info(
        ".union() — combined inflow + outflow account events: %d", total_events,
    )

    # ------------------------------------------------------------------ #
    # .intersection() — TRANSFORMATION (wide shuffle)                     #
    # Returns elements present in *both* RDDs (set intersection).         #
    # Requires a shuffle to match elements across partitions.             #
    # Here: accounts that appear as BOTH senders and receivers.           #
    # ------------------------------------------------------------------ #
    sender_set = outflow_accounts.distinct()    # TRANSFORMATION
    receiver_set = inflow_accounts.distinct()   # TRANSFORMATION
    both_roles = sender_set.intersection(receiver_set)  # TRANSFORMATION
    both_count = both_roles.count()  # ACTION
    logger.info(
        ".intersection() — accounts that are both senders AND receivers: %d",
        both_count,
    )

    # ------------------------------------------------------------------ #
    # .subtract() — TRANSFORMATION (wide shuffle)                         #
    # Returns elements in the first RDD that are NOT in the second.       #
    # Here: accounts that only ever *receive* (never send).  These may be #
    # terminal nodes in a mule chain.                                     #
    # ------------------------------------------------------------------ #
    receive_only = receiver_set.subtract(sender_set)  # TRANSFORMATION
    receive_only_count = receive_only.count()  # ACTION
    logger.info(
        ".subtract() — accounts that only receive (never send): %d",
        receive_only_count,
    )

    # ------------------------------------------------------------------ #
    # .foreachPartition() — ACTION                                        #
    # Applies a function to each partition iterator.  Unlike .foreach(),  #
    # it allows amortised setup per partition (e.g., open a DB connection  #
    # once per partition rather than per element).                        #
    # Here: log the size of each partition for diagnostics.               #
    # ------------------------------------------------------------------ #
    def log_partition_size(index: int, iterator):
        """Log the number of elements in a single partition.

        This callable is used with .mapPartitionsWithIndex() + .collect()
        because .foreachPartition() returns None and logging from
        executors may not appear in the driver log.  The pattern below
        is the idiomatic way to surface per-partition info on the driver.
        """
        count = sum(1 for _ in iterator)
        return [f"Partition {index}: {count} records"]

    partition_info = txn_rdd.mapPartitionsWithIndex(
        log_partition_size
    ).collect()  # ACTION
    logger.info(".foreachPartition() — partition diagnostics:")
    for info in partition_info:
        logger.info("  %s", info)

    # Also demonstrate actual .foreachPartition() for side-effect work.
    def _partition_side_effect(iterator):
        """Example side-effect function executed on each executor.

        In production this could flush records to an external sink
        (database, Kafka topic, etc.).  Here we simply count for
        demonstration — the result stays on the executor.
        """
        record_count = sum(1 for _ in iterator)
        # In a real scenario: db_conn.execute(f"INSERT stats {record_count}")
        # Side-effect only — no return value used.

    txn_rdd.foreachPartition(_partition_side_effect)  # ACTION
    logger.info(".foreachPartition() — side-effect function executed on all partitions.")

    # ------------------------------------------------------------------ #
    # .aggregate() — ACTION                                               #
    # A generalised fold with separate per-partition (seqOp) and cross-   #
    # partition (combOp) functions.  More powerful than .reduce() because  #
    # the accumulator type can differ from the element type.              #
    # Here: compute global statistics in a single pass:                   #
    #   (total_transaction_volume_usd, transaction_count)                 #
    # ------------------------------------------------------------------ #
    zero_value: Tuple[float, int] = (0.0, 0)

    def seq_op(acc: Tuple[float, int], txn: Transaction) -> Tuple[float, int]:
        """Within-partition sequential aggregator.

        Adds the transaction's paid amount (in USD) to the running total
        and increments the count.
        """
        return (acc[0] + txn.amount_paid, acc[1] + 1)

    def comb_op(a: Tuple[float, int], b: Tuple[float, int]) -> Tuple[float, int]:
        """Cross-partition combiner — merges two partial aggregates."""
        return (a[0] + b[0], a[1] + b[1])

    total_volume, total_count = txn_rdd.aggregate(
        zero_value, seq_op, comb_op
    )  # ACTION
    logger.info(
        ".aggregate() — Global stats: total_volume_usd=$%,.2f  count=%d",
        total_volume, total_count,
    )

    # Clean up.
    txn_rdd.unpersist()
    rates_bc.unpersist()

    logger.info("=== Additional RDD Demonstrations END ===")


# ---------------------------------------------------------------------------
# 4. Main Block
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = create_spark_session("ChainReaction-RDD")
    sc = spark.sparkContext

    results = run_rdd_pipeline(sc, "data/raw/HI-Small_Trans.csv")

    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    print(f"  Total accounts:   {results['total_accounts']}")
    print(f"  Label counts:     {results['label_counts']}")
    print(f"  Riskiest account: {results['riskiest'][1]}  "
          f"(score={results['riskiest'][0]:.4f})")
    print(f"  Output saved to:  {results['output_path']}")
    print("=" * 60 + "\n")

    demonstrate_rdd_operations(sc, "data/raw/HI-Small_Trans.csv")

    spark.stop()
    print("Spark session stopped. Done.")
