-- ============================================================================
-- Chain Reaction: Money Mule Detection — Cassandra Wide-Column Data Model
-- ============================================================================
-- Design Architecture Document: Production NoSQL Cassandra CQL Schema
-- Target Workload: High-throughput ingestion, low-latency AML account lookups,
-- time-series transaction event queries, and audit-ready risk scoring.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. KEYSPACE DEFINITION
-- ----------------------------------------------------------------------------
-- In production, NetworkTopologyStrategy distributes replicas across multiple
-- availability zones or data centers. For local/development environments,
-- SimpleStrategy with replication_factor 3 is standard.
CREATE KEYSPACE IF NOT EXISTS aml
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
} AND durable_writes = true;

USE aml;


-- ----------------------------------------------------------------------------
-- 2. TABLE: transaction_events (Time-Series Partitioned Wide-Column Store)
-- ----------------------------------------------------------------------------
-- WIDE-COLUMN DESIGN EXPLANATION:
-- * Partition Key: account_id (TEXT)
--   Distributes the account transaction logs uniformly across cluster nodes
--   using Murmur3Partitioner. Every event where an account is either sender
--   or receiver resides on the exact same storage node for that account partition.
-- * Clustering Keys: event_timestamp DESC, transaction_id ASC
--   Within each account's physical partition on disk (SSTable), rows are stored
--   in strictly chronological order (latest event first). This makes queries like
--   "fetch the 10 most recent transactions for account X" a zero-hop contiguous
--   disk slice without table scanning.
-- * TTL: 31,536,000 seconds (1 year)
--   Cassandra automatically expires and cleans up old transactional events
--   during background compaction without running costly DELETE operations.
CREATE TABLE IF NOT EXISTS aml.transaction_events (
    account_id              TEXT,            -- Partition Key
    event_timestamp         TIMESTAMP,       -- Clustering Key 1 (sort order DESC)
    transaction_id          UUID,            -- Clustering Key 2 (uniqueness tiebreaker)
    direction               TEXT,            -- 'IN' (inflow) or 'OUT' (outflow)
    counterparty_account    TEXT,            -- Connected peer account in graph
    amount_usd              DOUBLE,          -- Standardized USD currency amount
    original_currency       TEXT,            -- Native currency identifier (e.g., EUR, BTC, INR)
    original_amount         DOUBLE,          -- Nominal amount in original currency
    payment_format          TEXT,            -- Wire, ACH, Cash, Cheque, Crypto
    is_laundering           INT,             -- Supervised ground-truth fraud indicator
    PRIMARY KEY (account_id, event_timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (event_timestamp DESC, transaction_id ASC)
  AND default_time_to_live = 31536000
  AND gc_grace_seconds = 864000
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': '1'
  }
  AND comment = 'Time-series transaction journal partitioned per account for instant subgraphs';


-- ----------------------------------------------------------------------------
-- 3. TABLE: account_profiles (Canonical Wide-Row Account Feature Store)
-- ----------------------------------------------------------------------------
-- WIDE-COLUMN DESIGN EXPLANATION:
-- * Primary Key: account_id (Single Partition Key)
--   Each account represents exactly one wide row. In Cassandra, a row can store
--   up to 2 billion cells.
-- * Collections & Maps:
--   - last_10_txn_amounts: LIST<DOUBLE> stores rolling recent amounts for instant ML inference
--   - last_10_txn_timestamps: LIST<TIMESTAMP> tracks transaction dwell times
--   - flagged_chain_ids: SET<TEXT> records multi-hop motif chains (A->B->C->D)
--   - counterparty_risk_map: MAP<TEXT, DOUBLE> maps peer account ID -> counterparty risk score
-- * Dynamic Column Support:
--   As new graph features or ML model scores are engineered (e.g., Spark MLlib GBT,
--   GraphSAGE, XGBoost), new columns can be added via ALTER TABLE instantly
--   with zero storage overhead for accounts that do not have values yet.
CREATE TABLE IF NOT EXISTS aml.account_profiles (
    account_id                  TEXT PRIMARY KEY,
    
    -- Topological & Flow Features (Computed in Batch/Spark ETL)
    in_degree                   INT,
    out_degree                  INT,
    total_in_usd                DOUBLE,
    total_out_usd               DOUBLE,
    avg_holding_time_sec        DOUBLE,
    has_outflow_after_inflow    BOOLEAN,
    unique_counterparties       INT,
    pagerank                    DOUBLE,
    
    -- Extended Behavioral Signals
    fan_in_count                INT,
    fan_out_count               INT,
    txn_velocity_per_hour       DOUBLE,
    cross_currency_ratio        DOUBLE,
    unique_currencies           INT,
    
    -- Multi-Source Regulatory & Jurisdiction Flags
    is_sanctioned               BOOLEAN,
    sanctions_program           TEXT,
    jurisdiction_code           TEXT,
    jurisdiction_risk_score     DOUBLE,
    
    -- Multi-Model Machine Learning Ensembles
    xgboost_risk_score          DOUBLE,          -- Baseline Gradient Boosted Trees (M2)
    graphsage_risk_score        DOUBLE,          -- Graph Neural Network Probability (M3)
    spark_gbt_risk_score        DOUBLE,          -- Distributed Spark MLlib GBT (M6C)
    combined_risk_score         DOUBLE,          -- Calibrated Risk Fusion
    risk_level                  TEXT,            -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    
    -- Audit & Lineage Timestamps
    first_seen                  TIMESTAMP,
    last_seen                   TIMESTAMP,
    last_model_update           TIMESTAMP,
    
    -- Wide-Column Rich Structured Collections
    last_10_txn_amounts         LIST<DOUBLE>,
    last_10_txn_timestamps      LIST<TIMESTAMP>,
    flagged_chain_ids           SET<TEXT>,
    counterparty_risk_map       MAP<TEXT, DOUBLE>
) WITH compaction = {
    'class': 'LeveledCompactionStrategy'
} AND comment = 'Wide-column account profile containing topological features and model scores';


-- ----------------------------------------------------------------------------
-- 4. TABLE: streaming_alerts (Time-Partitioned Alert Sinks)
-- ----------------------------------------------------------------------------
-- WIDE-COLUMN DESIGN EXPLANATION:
-- * Partition Key: alert_date (DATE)
--   Partitions alerts day-by-day. Compliance officers and investigation teams
--   can query all alerts generated for a specific calendar day in a single partition scan.
-- * Clustering Keys: alert_timestamp DESC, account_id ASC
--   Ensures the most critical, recent alerts appear first within each day's partition.
-- * TTL: 7,776,000 seconds (90 days)
--   Alert records are preserved for 90 days of regulatory compliance reporting.
CREATE TABLE IF NOT EXISTS aml.streaming_alerts (
    alert_date              DATE,            -- Partition Key (YYYY-MM-DD)
    alert_timestamp         TIMESTAMP,       -- Clustering Key 1 (sort order DESC)
    account_id              TEXT,            -- Clustering Key 2
    alert_level             TEXT,            -- 'CRITICAL', 'HIGH', 'MEDIUM'
    trigger_rule            TEXT,            -- e.g., 'RAPID_FAN_IN_BURST', 'VELOCITY_SPIKE'
    txn_count               INT,             -- Transactions detected in streaming window
    total_inflow_usd        DOUBLE,          -- Cumulative volume in window
    unique_senders          INT,             -- Count of distinct originators
    window_start            TIMESTAMP,       -- Micro-batch window start
    window_end              TIMESTAMP,       -- Micro-batch window end
    acknowledged            BOOLEAN,         -- Flag for AML triage resolution
    PRIMARY KEY (alert_date, alert_timestamp, account_id)
) WITH CLUSTERING ORDER BY (alert_timestamp DESC, account_id ASC)
  AND default_time_to_live = 7776000
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': '1'
  };


-- ----------------------------------------------------------------------------
-- 5. TABLE: model_predictions (Immutable Model Scoring Audit Log)
-- ----------------------------------------------------------------------------
-- Designed for complete Model Governance and regulatory explainability audits.
-- Partitioned by account_id so all predictions for an account form a historical trail.
CREATE TABLE IF NOT EXISTS aml.model_predictions (
    account_id              TEXT,            -- Partition Key
    prediction_timestamp    TIMESTAMP,       -- Clustering Key 1
    model_name              TEXT,            -- Clustering Key 2 (e.g., 'GraphSAGE-v1', 'SparkGBT-v2')
    model_version           TEXT,
    risk_score              DOUBLE,
    decision_threshold      DOUBLE,
    is_mule_predicted       BOOLEAN,
    explanation_summary     TEXT,            -- GNNExplainer top attributions summary
    features_snapshot       TEXT,            -- JSON serialization of features at inference time
    PRIMARY KEY (account_id, prediction_timestamp, model_name)
) WITH CLUSTERING ORDER BY (prediction_timestamp DESC, model_name ASC)
  AND comment = 'Full audit trail of ML predictions per account across all model versions';


-- ----------------------------------------------------------------------------
-- 6. SECONDARY INDEXES & MATERIALIZED VIEWS
-- ----------------------------------------------------------------------------

-- Secondary index on account_profiles risk level for triage querying:
CREATE INDEX IF NOT EXISTS account_risk_level_idx
ON aml.account_profiles (risk_level);

-- Materialized View for High-Risk and Critical Accounts triage:
-- Allows compliance analysts to query only accounts requiring immediate SAR filings.
CREATE MATERIALIZED VIEW IF NOT EXISTS aml.high_risk_accounts AS
    SELECT
        account_id,
        risk_level,
        combined_risk_score,
        graphsage_risk_score,
        avg_holding_time_sec,
        in_degree,
        out_degree,
        total_in_usd,
        is_sanctioned
    FROM aml.account_profiles
    WHERE account_id IS NOT NULL
      AND risk_level IS NOT NULL
      AND combined_risk_score IS NOT NULL
    PRIMARY KEY (risk_level, combined_risk_score, account_id)
    WITH CLUSTERING ORDER BY (combined_risk_score DESC, account_id ASC);


-- ----------------------------------------------------------------------------
-- 7. PRODUCTION CQL QUERY PATTERNS (REFERENCE)
-- ----------------------------------------------------------------------------

-- Pattern A: Fetch the 20 most recent transactions for an account (Ego Subgraph)
-- SELECT * FROM aml.transaction_events
-- WHERE account_id = '8000EBD30'
-- LIMIT 20;

-- Pattern B: Fetch current feature profile and model scores for account lookup
-- SELECT * FROM aml.account_profiles
-- WHERE account_id = '8000EBD30';

-- Pattern C: Real-time AML investigator alert feed for today
-- SELECT * FROM aml.streaming_alerts
-- WHERE alert_date = toDate(now())
--   AND alert_level IN ('CRITICAL', 'HIGH')
-- LIMIT 50;

-- Pattern D: Historical prediction lineage for SAR compliance narrative
-- SELECT model_name, prediction_timestamp, risk_score, is_mule_predicted, explanation_summary
-- FROM aml.model_predictions
-- WHERE account_id = '8000EBD30';

