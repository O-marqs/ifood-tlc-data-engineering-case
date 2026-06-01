"""Answer mandatory iFood case questions from gold yellow datasets."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from ifood_tlc.paths import get_gold_path
from ifood_tlc.spark import get_spark_session


MONTHLY_KPIS = "gold_monthly_yellow_kpis"
HOURLY_PASSENGER_KPIS = "gold_hourly_passenger_kpis"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "analysis" / "outputs"


def read_gold_dataset(spark: SparkSession, dataset_name: str, gold_root: Path | None = None) -> DataFrame:
    """Read a gold dataset from parquet."""
    root = gold_root or get_gold_path()
    path = root / dataset_name
    print(f"Reading {dataset_name} from: {path}")
    return spark.read.parquet(str(path))


def answer_monthly_avg_total_amount(monthly_kpis: DataFrame) -> DataFrame:
    """Answer question 1 using monthly yellow KPIs."""
    return (
        monthly_kpis.select(
            F.col("reference_year"),
            F.col("reference_month"),
            F.round("avg_total_amount", 2).alias("avg_total_amount"),
            F.col("total_trips"),
            F.round("total_revenue", 2).alias("total_revenue"),
        )
        .orderBy("reference_year", "reference_month")
    )


def answer_may_hourly_avg_passengers(hourly_kpis: DataFrame) -> DataFrame:
    """Answer question 2 using hourly yellow passenger KPIs for May 2023."""
    return (
        hourly_kpis.filter(
            (F.col("reference_year") == 2023)
            & (F.col("reference_month") == "05")
        )
        .select(
            F.col("pickup_hour"),
            F.round("avg_passenger_count", 2).alias("avg_passenger_count"),
            F.col("total_trips"),
        )
        .orderBy("pickup_hour")
    )


def register_views(monthly_kpis: DataFrame, hourly_kpis: DataFrame) -> None:
    """Register temp views for equivalent Spark SQL queries."""
    monthly_kpis.createOrReplaceTempView(MONTHLY_KPIS)
    hourly_kpis.createOrReplaceTempView(HOURLY_PASSENGER_KPIS)


def write_output(df: DataFrame, output_path: Path) -> None:
    """Write a small CSV output for execution evidence."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_path))


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Answer mandatory iFood case questions.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where small CSV outputs will be written.",
    )
    parser.add_argument(
        "--skip-write",
        action="store_true",
        help="Print answers without writing CSV outputs.",
    )
    return parser.parse_args()


def main() -> int:
    """Print the mandatory case answers."""
    args = parse_args()
    try:
        spark = get_spark_session("ifood-tlc-mandatory-questions")
    except Exception as exc:
        print("ERROR: Could not start Spark. PySpark local execution requires Java/JDK.")
        print("Install Java and set JAVA_HOME, or run this script in Databricks.")
        print(f"Original error: {exc}")
        return 1

    monthly_kpis = read_gold_dataset(spark, MONTHLY_KPIS)
    hourly_kpis = read_gold_dataset(spark, HOURLY_PASSENGER_KPIS)
    register_views(monthly_kpis, hourly_kpis)

    question_1 = answer_monthly_avg_total_amount(monthly_kpis)
    question_2 = answer_may_hourly_avg_passengers(hourly_kpis)

    print("\nQuestion 1 - Monthly average total amount")
    print("Spark AVG ignores null total_amount values in the gold aggregation.")
    question_1.show(truncate=False)

    print("\nQuestion 2 - May 2023 average passengers by pickup hour")
    print("Spark AVG ignores null passenger_count values in the gold aggregation.")
    question_2.show(24, truncate=False)

    if not args.skip_write:
        write_output(question_1, args.output_dir / "question_1_monthly_avg_total_amount")
        write_output(question_2, args.output_dir / "question_2_may_hourly_avg_passengers")
        print(f"\nSmall CSV outputs written to: {args.output_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
