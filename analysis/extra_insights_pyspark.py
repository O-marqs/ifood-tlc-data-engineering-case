"""Run extra analytical insights from gold yellow taxi datasets."""

from __future__ import annotations

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


GOLD_YELLOW_TRIPS = "gold_yellow_trips"


def read_gold_yellow_trips(spark: SparkSession, gold_root: Path | None = None) -> DataFrame:
    """Read the gold yellow detail dataset."""
    root = gold_root or get_gold_path()
    path = root / GOLD_YELLOW_TRIPS
    print(f"Reading {GOLD_YELLOW_TRIPS} from: {path}")
    return spark.read.parquet(str(path))


def revenue_by_hour(df: DataFrame) -> DataFrame:
    """Calculate average and total revenue by pickup hour."""
    return (
        df.groupBy("reference_year", "reference_month", "pickup_hour")
        .agg(
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.count(F.lit(1)).alias("total_trips"),
        )
        .orderBy("reference_year", "reference_month", "pickup_hour")
    )


def trips_by_day(df: DataFrame) -> DataFrame:
    """Calculate daily trip volume."""
    return df.groupBy("pickup_date").agg(F.count(F.lit(1)).alias("total_trips")).orderBy("pickup_date")


def payment_distribution(df: DataFrame) -> DataFrame:
    """Calculate trip distribution by payment type."""
    monthly_totals = df.groupBy("reference_year", "reference_month").agg(
        F.count(F.lit(1)).alias("month_total_trips")
    )
    by_payment = df.groupBy("reference_year", "reference_month", "payment_type").agg(
        F.count(F.lit(1)).alias("total_trips")
    )
    return (
        by_payment.join(monthly_totals, ["reference_year", "reference_month"])
        .withColumn(
            "trip_share_percentage",
            F.round((F.col("total_trips") / F.col("month_total_trips")) * F.lit(100.0), 2),
        )
        .select("reference_year", "reference_month", "payment_type", "total_trips", "trip_share_percentage")
        .orderBy("reference_year", "reference_month", F.desc("total_trips"))
    )


def distance_amount_buckets(df: DataFrame) -> DataFrame:
    """Aggregate trips and revenue by distance buckets."""
    bucketed = df.withColumn(
        "distance_bucket",
        F.when(F.col("trip_distance").isNull(), F.lit("unknown"))
        .when(F.col("trip_distance") < 1, F.lit("00_<1_mile"))
        .when(F.col("trip_distance") < 3, F.lit("01_1_to_3_miles"))
        .when(F.col("trip_distance") < 5, F.lit("02_3_to_5_miles"))
        .when(F.col("trip_distance") < 10, F.lit("03_5_to_10_miles"))
        .otherwise(F.lit("04_10_plus_miles")),
    )
    return (
        bucketed.groupBy("distance_bucket")
        .agg(
            F.count(F.lit(1)).alias("total_trips"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy("distance_bucket")
    )


def top_revenue_hours(df: DataFrame) -> DataFrame:
    """Return top pickup hours by total revenue."""
    return (
        df.groupBy("reference_year", "reference_month", "pickup_hour")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.count(F.lit(1)).alias("total_trips"),
        )
        .orderBy(F.desc("total_revenue"))
        .limit(20)
    )


def average_duration_by_month(df: DataFrame) -> DataFrame:
    """Calculate average trip duration by month."""
    return (
        df.groupBy("reference_year", "reference_month")
        .agg(
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
            F.count(F.lit(1)).alias("total_trips"),
        )
        .orderBy("reference_year", "reference_month")
    )


def show_insight(title: str, df: DataFrame, rows: int = 20) -> None:
    """Print one insight result."""
    print(f"\n{title}")
    df.show(rows, truncate=False)


def main() -> int:
    """Run all extra insights and print the results."""
    try:
        spark = get_spark_session("ifood-tlc-extra-insights")
    except Exception as exc:
        print("ERROR: Could not start Spark. PySpark local execution requires Java/JDK.")
        print("Install Java and set JAVA_HOME, or run this script in Databricks.")
        print(f"Original error: {exc}")
        return 1

    gold_yellow_trips = read_gold_yellow_trips(spark)
    gold_yellow_trips.createOrReplaceTempView(GOLD_YELLOW_TRIPS)

    print("Scope: yellow taxi only, using gold_yellow_trips.")
    print("Spark AVG and SUM ignore null values in the aggregated column by default.")

    show_insight("1. Receita media por hora do dia", revenue_by_hour(gold_yellow_trips))
    show_insight("2. Volume de corridas por dia", trips_by_day(gold_yellow_trips))
    show_insight("3. Distribuicao de corridas por forma de pagamento", payment_distribution(gold_yellow_trips))
    show_insight("4. Distancia x valor total por faixas de distancia", distance_amount_buckets(gold_yellow_trips))
    show_insight("5. Top horarios por receita total", top_revenue_hours(gold_yellow_trips))
    show_insight("6. Duracao media das corridas por mes", average_duration_by_month(gold_yellow_trips))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
