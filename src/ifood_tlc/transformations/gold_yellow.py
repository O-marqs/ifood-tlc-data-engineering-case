"""Build gold consumption datasets for yellow taxi trips."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from ..config import load_pipeline_config
from ..paths import format_month, get_gold_path, get_silver_path
from ..spark import get_spark_session


SERVICE_TYPE = "yellow"
SILVER_TABLE_NAME = "silver_yellow_trips"
GOLD_DETAIL_NAME = "gold_yellow_trips"
GOLD_MONTHLY_KPIS_NAME = "gold_monthly_yellow_kpis"
GOLD_HOURLY_PASSENGER_KPIS_NAME = "gold_hourly_passenger_kpis"

REQUIRED_DETAIL_COLUMNS = (
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
)

OPTIONAL_DETAIL_COLUMNS = (
    "vendor_id",
    "pickup_date",
    "pickup_hour",
    "reference_year",
    "reference_month",
    "trip_duration_minutes",
    "trip_distance",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "source_file",
    "ingestion_run_id",
)

DETAIL_COLUMNS = REQUIRED_DETAIL_COLUMNS + OPTIONAL_DETAIL_COLUMNS
PARTITION_COLUMNS = ("reference_year", "reference_month")
LOGGER = logging.getLogger(__name__)

REQUIRED_GOLD_INPUT_COLUMNS = REQUIRED_DETAIL_COLUMNS + (
    "reference_year",
    "reference_month",
    "pickup_hour",
)


def build_default_input_path() -> Path:
    """Return the default silver yellow input path."""
    return get_silver_path() / SILVER_TABLE_NAME


def build_gold_dataset_path(dataset_name: str) -> Path:
    """Return a gold dataset path and create gold root if needed."""
    return get_gold_path(create=True) / dataset_name


def read_silver_yellow(
    spark: SparkSession,
    input_path: str | Path | None = None,
    year: int | None = None,
    months: list[int] | None = None,
) -> DataFrame:
    """Read silver yellow trips and optionally filter reference period."""
    resolved_input_path = Path(input_path) if input_path else build_default_input_path()
    if _is_local_path(resolved_input_path) and not resolved_input_path.exists():
        raise FileNotFoundError(f"Silver yellow parquet path not found: {resolved_input_path}")

    LOGGER.info("Reading silver yellow trips from: %s", resolved_input_path)
    try:
        df = spark.read.parquet(str(resolved_input_path))
    except AnalysisException as exc:
        raise RuntimeError(f"Could not read silver yellow parquet path: {resolved_input_path}") from exc

    if "service_type" in df.columns:
        df = df.filter(F.col("service_type") == SERVICE_TYPE)
    if "reference_month" in df.columns:
        df = normalize_reference_month(df)
    if year is not None:
        df = df.filter(F.col("reference_year").cast("int") == int(year))
    if months:
        formatted_months = [format_month(month) for month in months]
        df = df.filter(F.col("reference_month").isin(formatted_months))

    return df


def validate_required_columns(df: DataFrame, required_columns: tuple[str, ...], dataset_name: str) -> None:
    """Fail fast when required columns are missing."""
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        missing_text = ", ".join(missing_columns)
        raise ValueError(f"Missing required columns for {dataset_name}: {missing_text}")


def ensure_optional_columns(df: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    """Ensure optional output columns exist, using nulls when absent."""
    result = df
    for column_name in columns:
        if column_name not in result.columns:
            LOGGER.warning("Missing optional silver column '%s'. Creating null column in gold.", column_name)
            result = result.withColumn(column_name, F.lit(None))

    return result


def normalize_reference_month(df: DataFrame) -> DataFrame:
    """Normalize reference_month as a two-digit string."""
    return df.withColumn("reference_month", F.lpad(F.col("reference_month").cast("string"), 2, "0"))


def build_gold_yellow_trips(df: DataFrame) -> DataFrame:
    """Build the gold detail dataset for analytical consumption."""
    validate_required_columns(df, REQUIRED_DETAIL_COLUMNS, GOLD_DETAIL_NAME)
    return ensure_optional_columns(df, OPTIONAL_DETAIL_COLUMNS).select(*DETAIL_COLUMNS)


def build_gold_monthly_yellow_kpis(df: DataFrame) -> DataFrame:
    """Build monthly yellow taxi KPIs."""
    return (
        normalize_reference_month(df).groupBy("reference_year", "reference_month")
        .agg(
            F.avg("total_amount").alias("avg_total_amount"),
            F.count(F.lit(1)).alias("total_trips"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .select(
            F.col("reference_year").cast("int").alias("reference_year"),
            F.col("reference_month").cast("string").alias("reference_month"),
            F.col("avg_total_amount"),
            F.col("total_trips"),
            F.col("total_revenue"),
        )
    )


def build_gold_hourly_passenger_kpis(df: DataFrame) -> DataFrame:
    """Build hourly passenger KPIs for yellow taxi trips."""
    return (
        normalize_reference_month(df).groupBy("reference_year", "reference_month", "pickup_hour")
        .agg(
            F.avg("passenger_count").alias("avg_passenger_count"),
            F.count(F.lit(1)).alias("total_trips"),
        )
        .select(
            F.col("reference_year").cast("int").alias("reference_year"),
            F.col("reference_month").cast("string").alias("reference_month"),
            F.col("pickup_hour").cast("int").alias("pickup_hour"),
            F.col("avg_passenger_count"),
            F.col("total_trips"),
        )
    )


def register_gold_views(
    gold_yellow_trips: DataFrame,
    monthly_kpis: DataFrame,
    hourly_passenger_kpis: DataFrame,
) -> None:
    """Register temp views for Spark SQL consumption."""
    gold_yellow_trips.createOrReplaceTempView(GOLD_DETAIL_NAME)
    monthly_kpis.createOrReplaceTempView(GOLD_MONTHLY_KPIS_NAME)
    hourly_passenger_kpis.createOrReplaceTempView(GOLD_HOURLY_PASSENGER_KPIS_NAME)
    LOGGER.info(
        "Registered temp views: "
        f"{GOLD_DETAIL_NAME}, {GOLD_MONTHLY_KPIS_NAME}, {GOLD_HOURLY_PASSENGER_KPIS_NAME}"
    )


def write_dataset(
    df: DataFrame,
    output_path: Path,
    write_mode: str,
    partition_columns: tuple[str, ...] = PARTITION_COLUMNS,
) -> int:
    """Write a parquet dataset and return row count."""
    if _is_local_path(output_path):
        output_path.mkdir(parents=True, exist_ok=True)

    row_count = df.count()
    LOGGER.info("Writing %s rows to: %s", row_count, output_path)

    writer = df.write.mode(write_mode)
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(str(output_path))

    return row_count


def build_gold(
    spark: SparkSession,
    input_path: str | Path | None = None,
    output_root: str | Path | None = None,
    year: int | None = None,
    months: list[int] | None = None,
    write_mode: str = "overwrite",
) -> dict[str, int]:
    """Build all gold yellow consumption datasets."""
    gold_root = Path(output_root) if output_root else get_gold_path(create=True)
    silver_df = read_silver_yellow(spark, input_path=input_path, year=year, months=months)
    validate_required_columns(silver_df, REQUIRED_GOLD_INPUT_COLUMNS, "gold yellow input")

    gold_yellow_trips = build_gold_yellow_trips(silver_df)
    monthly_kpis = build_gold_monthly_yellow_kpis(silver_df)
    hourly_passenger_kpis = build_gold_hourly_passenger_kpis(silver_df)
    register_gold_views(gold_yellow_trips, monthly_kpis, hourly_passenger_kpis)

    return {
        GOLD_DETAIL_NAME: write_dataset(
            gold_yellow_trips,
            gold_root / GOLD_DETAIL_NAME,
            write_mode=write_mode,
        ),
        GOLD_MONTHLY_KPIS_NAME: write_dataset(
            monthly_kpis,
            gold_root / GOLD_MONTHLY_KPIS_NAME,
            write_mode=write_mode,
        ),
        GOLD_HOURLY_PASSENGER_KPIS_NAME: write_dataset(
            hourly_passenger_kpis,
            gold_root / GOLD_HOURLY_PASSENGER_KPIS_NAME,
            write_mode=write_mode,
        ),
    }


def _is_local_path(path: Path) -> bool:
    """Return whether pathlib can validate the path locally."""
    path_text = str(path)
    return not (
        path_text.startswith("dbfs:")
        or path_text.startswith("s3:")
        or path_text.startswith("s3a:")
        or path_text.startswith("abfss:")
        or path_text.startswith("gs:")
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    config = load_pipeline_config()
    parser = argparse.ArgumentParser(description="Build gold yellow taxi consumption datasets.")
    parser.add_argument("--input-path", type=Path, help="Optional explicit silver yellow input path.")
    parser.add_argument("--output-root", type=Path, help="Optional explicit gold output root.")
    parser.add_argument("--year", type=int, default=int(config["default_year"]))
    parser.add_argument("--month", type=int, help="Optional single reference month to process.")
    parser.add_argument(
        "--all-config-months",
        action="store_true",
        help="Process months listed in configs/pipeline.yml.",
    )
    parser.add_argument(
        "--write-mode",
        choices=("append", "overwrite"),
        default="overwrite",
        help="Spark write mode. Overwrite is the default for reproducible gold outputs.",
    )
    return parser.parse_args()


def resolve_months(args: argparse.Namespace) -> list[int] | None:
    """Resolve month filters requested by CLI."""
    if args.month and args.all_config_months:
        raise ValueError("Use either --month or --all-config-months, not both.")
    if args.month:
        return [args.month]
    if args.all_config_months:
        return [int(month) for month in load_pipeline_config()["months"]]

    return None


def main() -> int:
    """Run gold build from the CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    args = parse_args()

    try:
        months = resolve_months(args)
    except ValueError as exc:
        LOGGER.error("%s", exc)
        return 1

    try:
        spark = get_spark_session("ifood-tlc-gold-yellow")
    except Exception as exc:
        LOGGER.error("Could not start Spark. PySpark local execution requires Java/JDK.")
        LOGGER.error("Install Java and set JAVA_HOME, or run this command in Databricks.")
        LOGGER.error("Original error: %s", exc)
        return 1

    try:
        results = build_gold(
            spark=spark,
            input_path=args.input_path,
            output_root=args.output_root,
            year=args.year,
            months=months,
            write_mode=args.write_mode,
        )
    except FileNotFoundError as exc:
        LOGGER.error("%s", exc)
        return 1
    except ValueError as exc:
        LOGGER.error("%s", exc)
        return 1
    except RuntimeError as exc:
        LOGGER.error("%s", exc)
        return 1
    except Exception as exc:
        LOGGER.error("Gold build failed.")
        LOGGER.error("Original error: %s", exc)
        return 1

    LOGGER.info("Gold build summary")
    for dataset_name, row_count in results.items():
        LOGGER.info("- %s: %s rows", dataset_name, row_count)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
