"""Milestone 6C: Distributed Machine Learning Pipeline with PySpark MLlib.

Builds and evaluates distributed classification pipelines for money mule detection:
1. Feature vectorization via ``VectorAssembler`` across 8 graph/flow features.
2. Standardized scaling via ``StandardScaler`` fit strictly on training splits to prevent data leakage.
3. Multi-model comparisons across:
   - Spark Gradient-Boosted Trees (``GBTClassifier``)
   - Spark Random Forest (``RandomForestClassifier``)
   - Spark Regularized Logistic Regression (``LogisticRegression``)
4. Hyperparameter tuning using ``CrossValidator`` and ``ParamGridBuilder``.
5. Metric evaluation using ``BinaryClassificationEvaluator`` tuned for Precision-Recall AUC (PR-AUC)
   to handle the severe 1.23% money mule class imbalance.
6. Head-to-head benchmarking against existing Milestone 2 (XGBoost) and Milestone 3 (GraphSAGE) models.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    BinaryLogisticRegressionTrainingSummary,
    GBTClassificationModel,
    GBTClassifier,
    LogisticRegression,
    LogisticRegressionModel,
    RandomForestClassificationModel,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession, functions as F

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


def create_spark_session(app_name: str = "ChainReaction-SparkML") -> SparkSession:
    """Configures a Spark session optimized for distributed model training."""
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


# Default feature columns matching Milestone 2 and 3 feature specifications
DEFAULT_FEATURE_COLS = [
    "in_degree",
    "out_degree",
    "total_in",
    "total_out",
    "avg_holding_time",
    "has_outflow_after_inflow",
    "unique_counterparties",
    "pagerank",
]


@dataclass
class ModelBenchmarkResult:
    """Encapsulates evaluation metrics for a single model candidate."""
    model_name: str
    framework: str
    train_pr_auc: float
    val_pr_auc: float
    test_pr_auc: float
    best_params: Dict[str, Any]
    model_instance: Any


class SparkMLMuleClassifier:
    """End-to-end distributed ML pipeline builder, trainer, and evaluator."""

    def __init__(
        self,
        spark: SparkSession,
        feature_cols: Optional[List[str]] = None,
        label_col: str = "is_laundering",
        seed: int = 42,
    ) -> None:
        self.spark = spark
        self.feature_cols = feature_cols or DEFAULT_FEATURE_COLS
        self.label_col = label_col
        self.seed = seed

        self.evaluator_pr = BinaryClassificationEvaluator(
            labelCol=self.label_col,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderPR",
        )
        self.evaluator_roc = BinaryClassificationEvaluator(
            labelCol=self.label_col,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )

    def prepare_data(
        self,
        features_df: DataFrame,
        split_df: Optional[DataFrame] = None,
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """Prepares features and partitions into consistent Train/Val/Test sets.

        If split_df is provided (from split_accounts.csv), it ensures exact split
        parity with the GraphSAGE and XGBoost benchmarks.
        """
        # Ensure correct cast of features and label
        cleaned = features_df
        for col_name in self.feature_cols:
            cleaned = cleaned.withColumn(col_name, F.col(col_name).cast("double"))
        cleaned = cleaned.withColumn(self.label_col, F.col(self.label_col).cast("integer"))

        # Impute missing values with 0.0
        cleaned = cleaned.na.fill({col: 0.0 for col in self.feature_cols})

        if split_df is not None:
            joined = cleaned.join(split_df.select("account", "split"), on="account", how="inner")
            train_df = joined.filter(F.col("split") == "train").drop("split")
            val_df = joined.filter(F.col("split") == "val").drop("split")
            test_df = joined.filter(F.col("split") == "test").drop("split")
            logger.info("Partitioned data using split_accounts mapping.")
        else:
            # Random split: 70% Train, 10% Validation, 20% Test
            train_df, val_df, test_df = cleaned.randomSplit([0.70, 0.10, 0.20], seed=self.seed)
            logger.info("Partitioned data using random 70/10/20 split.")

        train_df.cache()
        val_df.cache()
        test_df.cache()

        logger.info(
            "Splits ready — Train: %d, Val: %d, Test: %d",
            train_df.count(), val_df.count(), test_df.count(),
        )
        return train_df, val_df, test_df

    def build_preprocessing_stages(self) -> Tuple[VectorAssembler, StandardScaler]:
        """Constructs vector assembly and standard scaler stages."""
        assembler = VectorAssembler(
            inputCols=self.feature_cols,
            outputCol="raw_features",
            handleInvalid="keep",
        )
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withStd=True,
            withMean=False,  # Sparsity preserving
        )
        return assembler, scaler

    def train_gbt_classifier(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        test_df: DataFrame,
        tune_hyperparams: bool = False,
    ) -> ModelBenchmarkResult:
        """Trains a Gradient-Boosted Trees classifier using PySpark ML."""
        logger.info("Training Spark MLlib GBTClassifier...")
        assembler, scaler = self.build_preprocessing_stages()

        gbt = GBTClassifier(
            labelCol=self.label_col,
            featuresCol="features",
            maxIter=30 if not tune_hyperparams else 50,
            maxDepth=5,
            seed=self.seed,
        )

        pipeline = Pipeline(stages=[assembler, scaler, gbt])

        if tune_hyperparams:
            param_grid = (
                ParamGridBuilder()
                .addGrid(gbt.maxDepth, [3, 5, 7])
                .addGrid(gbt.maxIter, [20, 50])
                .build()
            )
            cv = CrossValidator(
                estimator=pipeline,
                estimatorParamMaps=param_grid,
                evaluator=self.evaluator_pr,
                numFolds=3,
                seed=self.seed,
            )
            cv_model = cv.fit(train_df)
            best_model = cv_model.bestModel
            best_params = {"maxDepth": best_model.stages[-1].getMaxDepth(), "maxIter": best_model.stages[-1].getMaxIter()}
        else:
            best_model = pipeline.fit(train_df)
            best_params = {"maxDepth": 5, "maxIter": 30}

        # Evaluate across splits
        train_preds = best_model.transform(train_df)
        val_preds = best_model.transform(val_df)
        test_preds = best_model.transform(test_df)

        train_pr = float(self.evaluator_pr.evaluate(train_preds))
        val_pr = float(self.evaluator_pr.evaluate(val_preds))
        test_pr = float(self.evaluator_pr.evaluate(test_preds))

        logger.info("GBT Complete — Train PR-AUC: %.4f, Val: %.4f, Test: %.4f", train_pr, val_pr, test_pr)

        return ModelBenchmarkResult(
            model_name="Spark GBT",
            framework="pyspark.ml",
            train_pr_auc=train_pr,
            val_pr_auc=val_pr,
            test_pr_auc=test_pr,
            best_params=best_params,
            model_instance=best_model,
        )

    def train_random_forest(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        test_df: DataFrame,
    ) -> ModelBenchmarkResult:
        """Trains a Random Forest classifier using PySpark ML."""
        logger.info("Training Spark MLlib RandomForestClassifier...")
        assembler, scaler = self.build_preprocessing_stages()

        rf = RandomForestClassifier(
            labelCol=self.label_col,
            featuresCol="features",
            numTrees=50,
            maxDepth=6,
            seed=self.seed,
        )

        pipeline = Pipeline(stages=[assembler, scaler, rf])
        model = pipeline.fit(train_df)

        train_pr = float(self.evaluator_pr.evaluate(model.transform(train_df)))
        val_pr = float(self.evaluator_pr.evaluate(model.transform(val_df)))
        test_pr = float(self.evaluator_pr.evaluate(model.transform(test_df)))

        logger.info("Random Forest Complete — Test PR-AUC: %.4f", test_pr)

        return ModelBenchmarkResult(
            model_name="Spark Random Forest",
            framework="pyspark.ml",
            train_pr_auc=train_pr,
            val_pr_auc=val_pr,
            test_pr_auc=test_pr,
            best_params={"numTrees": 50, "maxDepth": 6},
            model_instance=model,
        )

    def train_logistic_regression(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        test_df: DataFrame,
    ) -> ModelBenchmarkResult:
        """Trains an ElasticNet Logistic Regression classifier using PySpark ML."""
        logger.info("Training Spark MLlib LogisticRegression...")
        assembler, scaler = self.build_preprocessing_stages()

        lr = LogisticRegression(
            labelCol=self.label_col,
            featuresCol="features",
            maxIter=50,
            regParam=0.01,
            elasticNetParam=0.5,
        )

        pipeline = Pipeline(stages=[assembler, scaler, lr])
        model = pipeline.fit(train_df)

        train_pr = float(self.evaluator_pr.evaluate(model.transform(train_df)))
        val_pr = float(self.evaluator_pr.evaluate(model.transform(val_df)))
        test_pr = float(self.evaluator_pr.evaluate(model.transform(test_df)))

        logger.info("Logistic Regression Complete — Test PR-AUC: %.4f", test_pr)

        return ModelBenchmarkResult(
            model_name="Spark Logistic Regression",
            framework="pyspark.ml",
            train_pr_auc=train_pr,
            val_pr_auc=val_pr,
            test_pr_auc=test_pr,
            best_params={"regParam": 0.01, "elasticNetParam": 0.5},
            model_instance=model,
        )

    def run_benchmark_suite(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        test_df: DataFrame,
    ) -> List[ModelBenchmarkResult]:
        """Trains all models and produces a consolidated benchmark list."""
        results = [
            self.train_gbt_classifier(train_df, val_df, test_df),
            self.train_random_forest(train_df, val_df, test_df),
            self.train_logistic_regression(train_df, val_df, test_df),
        ]
        return results

    def print_comparison_table(self, spark_results: List[ModelBenchmarkResult]) -> None:
        """Prints a unified Markdown comparison table across all project milestones."""
        print("\n" + "=" * 80)
        print("HEAD-TO-HEAD MODEL BENCHMARK COMPARISON TABLE")
        print("=" * 80)
        print(f"{'Model Architecture':<30} | {'Framework':<16} | {'Val PR-AUC':<12} | {'Test PR-AUC':<12}")
        print("-" * 80)
        print(f"{'Random Guessing Floor':<30} | {'Theoretical':<16} | {'0.0123':<12} | {'0.0123':<12}")
        print(f"{'XGBoost (Milestone 2)':<30} | {'xgboost/sklearn':<16} | {'—':<12} | {'0.0914':<12}")
        print(f"{'XGBoost (Milestone 3)':<30} | {'xgboost/sklearn':<16} | {'0.0855':<12} | {'0.0834':<12}")
        print(f"{'GraphSAGE GNN (Milestone 3)':<30} | {'PyTorch Geometric':<16} | {'0.2091':<12} | {'0.1884':<12}")

        for res in spark_results:
            print(f"{res.model_name:<30} | {res.framework:<16} | {res.val_pr_auc:<12.4f} | {res.test_pr_auc:<12.4f}")
        print("=" * 80 + "\n")

    def save_model(self, model: PipelineModel, path: str = "outputs/models/spark_gbt_model") -> None:
        """Serializes the Spark ML pipeline model to disk."""
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        model.write().overwrite().save(path)
        logger.info("Saved Spark ML model pipeline to '%s'", path)

    def load_model(self, path: str = "outputs/models/spark_gbt_model") -> PipelineModel:
        """Loads a persisted Spark ML pipeline model."""
        model = PipelineModel.load(path)
        logger.info("Loaded Spark ML model pipeline from '%s'", path)
        return model


# ---------------------------------------------------------------------------
# Standalone Execution Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = create_spark_session()

    features_path = "data/processed/account_features.csv"
    split_path = "data/processed/split_accounts.csv"

    if os.path.exists(features_path):
        logger.info("Loading account features from %s", features_path)
        raw_features = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(features_path)
        )
        split_df = None
        if os.path.exists(split_path):
            split_df = spark.read.option("header", "true").csv(split_path)

        classifier = SparkMLMuleClassifier(spark)
        train_df, val_df, test_df = classifier.prepare_data(raw_features, split_df)

        # Train models (use sample to speed up local execution)
        sample_train = train_df.sample(False, 0.10, seed=42)
        sample_val = val_df.sample(False, 0.20, seed=42)
        sample_test = test_df.sample(False, 0.20, seed=42)

        results = classifier.run_benchmark_suite(sample_train, sample_val, sample_test)
        classifier.print_comparison_table(results)

        # Save best GBT model
        gbt_res = [r for r in results if r.model_name == "Spark GBT"][0]
        classifier.save_model(gbt_res.model_instance, "outputs/models/spark_gbt_pipeline")
    else:
        logger.warning("File %s not found. Run previous milestones or data generation first.", features_path)

    spark.stop()
    print("Spark MLlib training workflow completed.")

