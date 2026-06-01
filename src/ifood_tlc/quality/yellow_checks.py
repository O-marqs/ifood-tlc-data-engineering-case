"""Data quality report for silver yellow taxi trips."""

from __future__ import annotations

import argparse
import logging
import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from ..config import load_pipeline_config
from ..paths import format_month
from ..paths import get_gold_path, get_silver_path
from ..spark import get_spark_session


SERVICE_TYPE = "yellow"
SILVER_TABLE_NAME = "silver_yellow_trips"
QUALITY_REPORT_NAME = "quality/quality_yellow_trips_report"
GROUP_COLUMNS = ("reference_year", "reference_month")
LOGGER = logging.getLogger(__name__)

REQUIRED_QUALITY_COLUMNS = (
    "reference_year",
    "reference_month",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pickup_date",
    "total_amount",
    "passenger_count",
    "trip_distance",
    "trip_duration_minutes",
)


QUALITY_RULE_NAMES = (
    "null_pickup_datetime",
    "null_dropoff_datetime",
    "dropoff_before_pickup",
    "null_total_amount",
    "negative_total_amount",
    "zero_total_amount",
    "null_passenger_count",
    "negative_passenger_count",
    "zero_passenger_count",
    "negative_trip_distance",
    "invalid_trip_duration",
    "records_outside_reference_month",
)


def build_quality_rules() -> dict[str, F.Column]:
    """Return quality rule conditions as Spark columns."""
    return {
        "null_pickup_datetime": F.col("tpep_pickup_datetime").isNull(),
        "null_dropoff_datetime": F.col("tpep_dropoff_datetime").isNull(),
        "dropoff_before_pickup": F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"),
        "null_total_amount": F.col("total_amount").isNull(),
        "negative_total_amount": F.col("total_amount") < 0,
        "zero_total_amount": F.col("total_amount") == 0,
        "null_passenger_count": F.col("passenger_count").isNull(),
        "negative_passenger_count": F.col("passenger_count") < 0,
        "zero_passenger_count": F.col("passenger_count") == 0,
        "negative_trip_distance": F.col("trip_distance") < 0,
        "invalid_trip_duration": F.col("trip_duration_minutes").isNull()
        | (F.col("trip_duration_minutes") <= 0),
        "records_outside_reference_month": (
            F.col("pickup_date").isNull()
            | (F.year("pickup_date") != F.col("reference_year").cast("int"))
            | (F.date_format("pickup_date", "MM") != F.col("reference_month"))
        ),
    }


def build_default_input_path() -> Path:
    """Return the default silver yellow input path."""
    return get_silver_path() / SILVER_TABLE_NAME


def build_default_output_path() -> Path:
    """Return the default data quality report output path."""
    return get_gold_path(create=True) / QUALITY_REPORT_NAME


def read_silver_yellow(spark: SparkSession, input_path: str | Path | None = None) -> DataFrame:
    """Read silver yellow trips."""
    resolved_input_path = Path(input_path) if input_path else build_default_input_path()
    if _is_local_path(resolved_input_path) and not resolved_input_path.exists():
        raise FileNotFoundError(f"Silver yellow parquet path not found: {resolved_input_path}")

    LOGGER.info("Reading silver yellow trips from: %s", resolved_input_path)
    try:
        return normalize_reference_month(spark.read.parquet(str(resolved_input_path)))
    except AnalysisException as exc:
        raise RuntimeError(f"Could not read silver yellow parquet path: {resolved_input_path}") from exc


def validate_required_columns(df: DataFrame, required_columns: tuple[str, ...], dataset_name: str) -> None:
    """Fail fast when required columns are missing."""
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        missing_text = ", ".join(missing_columns)
        raise ValueError(f"Missing required columns for {dataset_name}: {missing_text}")


def normalize_reference_month(df: DataFrame) -> DataFrame:
    """Normalize reference_month as a two-digit string."""
    if "reference_month" not in df.columns:
        return df

    return df.withColumn("reference_month", F.lpad(F.col("reference_month").cast("string"), 2, "0"))


def build_quality_report(df: DataFrame, run_id: str | None = None) -> DataFrame:
    """Calculate quality checks by reference year and month."""
    validate_required_columns(df, REQUIRED_QUALITY_COLUMNS, "quality_yellow_trips_report")
    df = normalize_reference_month(df)
    resolved_run_id = run_id or str(uuid.uuid4())
    quality_rules = build_quality_rules()
    grouped_df = df.groupBy(*GROUP_COLUMNS).agg(
        F.count(F.lit(1)).alias("total_records"),
        *[
            F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).alias(rule_name)
            for rule_name, condition in quality_rules.items()
        ],
    )

    rule_frames = []
    for rule_name in QUALITY_RULE_NAMES:
        rule_frames.append(
            grouped_df.select(
                F.lit(resolved_run_id).alias("run_id"),
                F.lit(SERVICE_TYPE).alias("service_type"),
                F.col("reference_year").cast("int").alias("reference_year"),
                F.col("reference_month").cast("string").alias("reference_month"),
                F.lit(rule_name).alias("rule_name"),
                F.col("total_records").cast("long").alias("total_records"),
                F.col(rule_name).cast("long").alias("failed_records"),
                F.when(
                    F.col("total_records") > 0,
                    F.round((F.col(rule_name) / F.col("total_records")) * F.lit(100.0), 4),
                )
                .otherwise(F.lit(0.0))
                .alias("failed_percentage"),
                F.current_timestamp().alias("created_at"),
            )
        )

    report_df = rule_frames[0]
    for rule_df in rule_frames[1:]:
        report_df = report_df.unionByName(rule_df)

    return report_df


def write_quality_report(
    report_df: DataFrame,
    output_path: str | Path | None = None,
    write_mode: str = "overwrite",
) -> int:
    """Write the quality report to gold and return row count."""
    resolved_output_path = Path(output_path) if output_path else build_default_output_path()
    if _is_local_path(resolved_output_path):
        resolved_output_path.mkdir(parents=True, exist_ok=True)

    row_count = report_df.count()
    LOGGER.info("Writing quality report to: %s", resolved_output_path)
    LOGGER.info("Rows to write: %s", row_count)

    (
        report_df.write.mode(write_mode)
        .partitionBy("service_type", "reference_year", "reference_month")
        .parquet(str(resolved_output_path))
    )

    return row_count


def run_quality_checks(
    spark: SparkSession,
    input_path: str | Path | None = None,
    output_path: str | Path | None = None,
    run_id: str | None = None,
    write_mode: str = "overwrite",
    year: int | None = None,
    months: list[int] | None = None,
) -> int:
    """Run quality checks for silver yellow trips."""
    resolved_run_id = run_id or str(uuid.uuid4())
    silver_df = read_silver_yellow(spark, input_path=input_path)

    if "service_type" in silver_df.columns:
        silver_df = silver_df.filter(F.col("service_type") == SERVICE_TYPE)
    if year is not None:
        silver_df = silver_df.filter(F.col("reference_year").cast("int") == int(year))
    if months:
        formatted_months = [format_month(month) for month in months]
        silver_df = silver_df.filter(F.col("reference_month").isin(formatted_months))

    LOGGER.info("Starting yellow quality checks, run_id=%s", resolved_run_id)
    report_df = build_quality_report(silver_df, run_id=resolved_run_id)
    row_count = write_quality_report(report_df, output_path=output_path, write_mode=write_mode)
    LOGGER.info("Finished yellow quality checks, report_rows=%s", row_count)

    return row_count


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
    parser = argparse.ArgumentParser(description="Generate quality report for silver yellow trips.")
    parser.add_argument("--input-path", type=Path, help="Optional explicit silver yellow input path.")
    parser.add_argument("--output-path", type=Path, help="Optional explicit quality report output path.")
    parser.add_argument("--year", type=int, default=int(config["default_year"]))
    parser.add_argument("--month", type=int, help="Optional single reference month to evaluate.")
    parser.add_argument(
        "--all-config-months",
        action="store_true",
        help="Evaluate all months listed in configs/pipeline.yml.",
    )
    parser.add_argument("--run-id", help="Optional run id for reproducible reporting.")
    parser.add_argument(
        "--write-mode",
        choices=("append", "overwrite"),
        default="overwrite",
        help="Spark write mode. Overwrite is the default for reproducible case outputs.",
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
    """Run quality checks from the CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    args = parse_args()
    try:
        months = resolve_months(args)
    except ValueError as exc:
        LOGGER.error("%s", exc)
        return 1

    try:
        spark = get_spark_session("ifood-tlc-quality-yellow")
    except Exception as exc:
        LOGGER.error("Could not start Spark. PySpark local execution requires Java/JDK.")
        LOGGER.error("Install Java and set JAVA_HOME, or run this command in Databricks.")
        LOGGER.error("Original error: %s", exc)
        return 1

    try:
        rows = run_quality_checks(
            spark=spark,
            input_path=args.input_path,
            output_path=args.output_path,
            run_id=args.run_id,
            write_mode=args.write_mode,
            year=args.year,
            months=months,
        )
        LOGGER.info("Quality report summary")
        LOGGER.info("- report rows written: %s", rows)
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
        LOGGER.error("Quality report generation failed.")
        LOGGER.error("Original error: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
