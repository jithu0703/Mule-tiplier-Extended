"""Milestone 6D: Real-Time Streaming Transaction Event Simulator.

Drip-feeds transaction events from historical CSV files into an active streaming directory
to emulate continuous real-time Kafka or banking network feeds.

Features:
- Micro-batch partitioning with configurable batch size and inter-batch delay.
- Automatic output directory creation and atomic file drops.
- CLI argument parsing for interactive testing and headless pipeline orchestration.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import uuid
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


def simulate_stream(
    source_csv_path: str,
    output_dir: str = "data/stream/incoming",
    batch_size: int = 500,
    delay_seconds: float = 2.0,
    max_batches: Optional[int] = 20,
) -> int:
    """Emits CSV micro-batches to the incoming stream directory.

    Args:
        source_csv_path: Path to raw or processed transaction CSV.
        output_dir: Target folder monitored by Spark Structured Streaming.
        batch_size: Number of transactions per micro-batch CSV file.
        delay_seconds: Sleep interval between emitting consecutive batches.
        max_batches: Maximum batches to emit (None for unbounded loop).

    Returns:
        Total number of batches emitted.
    """
    if not os.path.exists(source_csv_path):
        raise FileNotFoundError(f"Source transaction dataset not found: {source_csv_path}")

    os.makedirs(output_dir, exist_ok=True)
    logger.info("Reading source dataset from '%s'...", source_csv_path)

    # Read needed rows efficiently
    nrows = (max_batches * batch_size * 2) if (max_batches is not None and max_batches > 0) else None
    df = pd.read_csv(source_csv_path, nrows=nrows)

    # Standardize column naming if reading raw format
    col_map = {
        "Account": "from_account",
        "Account.1": "to_account",
        "from_Account": "from_account",
        "to_Account": "to_account",
        "Amount Paid": "amount_paid",
        "Amount Received": "amount_received",
        "amount_paid_usd": "amount_usd",
        "Payment Currency": "currency",
        "Is Laundering": "is_laundering",
        "Timestamp": "timestamp",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Ensure amount_usd exists
    if "amount_usd" not in df.columns and "amount_paid" in df.columns:
        df["amount_usd"] = df["amount_paid"]

    # Ensure transaction_id exists
    if "transaction_id" not in df.columns:
        df["transaction_id"] = [uuid.uuid4().hex[:12] for _ in range(len(df))]

    # Order standard columns
    standard_cols = ["transaction_id", "timestamp", "from_account", "to_account", "amount_usd", "currency", "is_laundering"]
    available_cols = [c for c in standard_cols if c in df.columns]
    df = df[available_cols]

    total_rows = len(df)
    logger.info("Source loaded: %d total rows. Emitting micro-batches of %d rows to '%s'", total_rows, batch_size, output_dir)

    batch_idx = 0
    start_pos = 0

    try:
        while start_pos < total_rows:
            if max_batches is not None and batch_idx >= max_batches:
                logger.info("Reached configured max_batches limit (%d). Simulation terminating.", max_batches)
                break

            end_pos = min(start_pos + batch_size, total_rows)
            batch_slice = df.iloc[start_pos:end_pos].copy()

            # Generate unique atomic filename with timestamp prefix
            file_name = f"txn_stream_{int(time.time())}_{uuid.uuid4().hex[:6]}.csv"
            tmp_path = os.path.join(output_dir, f".tmp_{file_name}")
            final_path = os.path.join(output_dir, file_name)

            # Write to temporary file first then rename for atomic file arrival in Spark
            batch_slice.to_csv(tmp_path, index=False)
            os.rename(tmp_path, final_path)

            batch_idx += 1
            logger.info(
                "[%s] Emitted Batch %03d: %d transactions -> %s",
                time.strftime("%H:%M:%S"), batch_idx, len(batch_slice), file_name,
            )

            start_pos = end_pos
            if start_pos < total_rows and delay_seconds > 0:
                time.sleep(delay_seconds)

    except KeyboardInterrupt:
        logger.info("Simulation halted by user.")

    logger.info("Simulation completed. Emitted %d total batches.", batch_idx)
    return batch_idx


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate real-time AML transaction stream")
    parser.add_argument(
        "--source",
        default="data/processed/processed_df.csv",
        help="Path to source CSV (default: data/processed/processed_df.csv)",
    )
    parser.add_argument(
        "--output-dir",
        default="data/stream/incoming",
        help="Target streaming directory (default: data/stream/incoming)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Transactions per micro-batch (default: 500)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Delay in seconds between micro-batches (default: 2.0)",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=20,
        help="Maximum micro-batches to emit (default: 20)",
    )

    args = parser.parse_args()

    # Fallback if processed_df doesn't exist
    source_path = args.source
    if not os.path.exists(source_path):
        source_path = "data/raw/HI-Small_Trans.csv"

    simulate_stream(
        source_csv_path=source_path,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        delay_seconds=args.delay,
        max_batches=args.max_batches,
    )


if __name__ == "__main__":
    main()
