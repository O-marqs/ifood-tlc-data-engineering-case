"""Transform bronze yellow taxi trips into the silver layer."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, DoubleType, IntegerType, StringType, TimestampType
from pyspark.sql.utils import AnalysisException

from ..config import load_pipeline_config
from ..paths import format_month, get_bronze_path, get_silver_path
from ..spark import get_spark_session


SERVICE_TYPE = "yellow"
SILVER_TABLE_NAME = "silver_yellow_trips"
PARTITION_COLUMNS = ("reference_year", "reference_month")
LOGGER = logging.getLogger(__name__)

REQUIRED_YELLOW_COLUMNS = (
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
)

EXPECTED_COLUMNS: dict[str, DataType] = {
    "VendorID": IntegerType(),
    "tpep_pickup_datetime": TimestampType(),
    "tpep_dropoff_datetime": TimestampType(),
    "passenger_count": DoubleType(),
    "trip_distance": DoubleType(),
    "PULocationID": IntegerType(),
    "DOLocationID": IntegerType(),
    "payment_type": IntegerType(),
    "fare_amount": DoubleType(),
    "tip_amount": DoubleType(),
    "tolls_amount": DoubleType(),
    "total_amount": DoubleType(),
    "congestion_surcharge": DoubleType(),
    "airport_fee": DoubleType(),
    "reference_year": IntegerType(),
    "reference_month": StringType(),
    "service_type": StringType(),
    "source_file": StringType(),
    "ingestion_run_id": StringType(),
}

SILVER_COLUMNS = (
    "vendor_id",
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "pickup_date",
    "pickup_hour",
    "trip_duration_minutes",
    "reference_year",
    "reference_month",
    "service_type",
    "source_file",
    "ingestion_run_id",
)


def build_default_output_path() -> Path:
    """Return the default silver yellow output path."""
    return get_silver_path(create=True) / SILVER_TABLE_NAME


def build_bronze_partition_path(year: int, month: int) -> Path:
    """Return the expected bronze partition path for yellow taxi."""
    return (
        get_bronze_path()
        / f"service_type={SERVICE_TYPE}"
        / f"reference_year={year}"
        / f"reference_month={format_month(month)}"
    )


def union_dataframes_by_name(dataframes: list[DataFrame]) -> DataFrame:
    """Union dataframes by column name, allowing optional schema differences."""
    if not dataframes:
        raise ValueError("No silver dataframes available to union.")

    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)

    return combined_df


def ensure_expected_columns(df: DataFrame) -> DataFrame:
    """Add missing expected columns as typed nulls and cast existing columns."""
    validate_required_columns(df, REQUIRED_YELLOW_COLUMNS, "silver_yellow_trips")

    result = df
    for column_name, data_type in EXPECTED_COLUMNS.items():
        if column_name in result.columns:
            result = result.withColumn(column_name, F.col(column_name).cast(data_type))
        else:
            LOGGER.warning("Missing optional bronze column '%s'. Creating typed null.", column_name)
            result = result.withColumn(column_name, F.lit(None).cast(data_type))

    return normalize_reference_month(result)


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


def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """Create the silver yellow taxi dataframe without destructive filters."""
    typed_df = ensure_expected_columns(df)

    enriched_df = (
        typed_df.withColumn("vendor_id", F.col("VendorID").cast(IntegerType()))
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime").cast(IntegerType()))
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            )
            / F.lit(60.0),
        )
    )

    return enriched_df.select(*SILVER_COLUMNS)


def read_bronze_yellow(
    spark: SparkSession,
    year: int,
    month: int,
    input_path: str | Path | None = None,
) -> DataFrame:
    """Read bronze yellow data for one reference month."""
    resolved_input_path = Path(input_path) if input_path else build_bronze_partition_path(year, month)

    if _is_local_path(resolved_input_path) and not resolved_input_path.exists():
        raise FileNotFoundError(f"Bronze parquet path not found: {resolved_input_path}")

    LOGGER.info("Reading bronze yellow data from: %s", resolved_input_path)
    try:
        df = spark.read.parquet(str(resolved_input_path))
    except AnalysisException as exc:
        raise RuntimeError(f"Could not read bronze parquet path: {resolved_input_path}") from exc

    if "service_type" in df.columns:
        df = df.filter(F.col("service_type") == SERVICE_TYPE)
    else:
        df = df.withColumn("service_type", F.lit(SERVICE_TYPE))

    if "reference_year" not in df.columns:
        df = df.withColumn("reference_year", F.lit(int(year)))
    if "reference_month" not in df.columns:
        df = df.withColumn("reference_month", F.lit(format_month(month)))

    return normalize_reference_month(df)


def write_silver(df: DataFrame, output_path: str | Path | None = None, write_mode: str = "overwrite") -> int:
    """Write silver yellow trips as parquet and return row count."""
    resolved_output_path = Path(output_path) if output_path else build_default_output_path()
    if _is_local_path(resolved_output_path):
        resolved_output_path.mkdir(parents=True, exist_ok=True)

    row_count = df.count()
    LOGGER.info("Writing silver yellow trips to: %s", resolved_output_path)
    LOGGER.info("Rows to write: %s", row_count)

    (
        df.write.mode(write_mode)
        .partitionBy(*PARTITION_COLUMNS)
        .parquet(str(resolved_output_path))
    )

    return row_count


def collect_reference_month_counts(df: DataFrame) -> dict[str, int]:
    """Return row counts by two-digit reference month."""
    return {
        row["reference_month"]: int(row["count"])
        for row in df.groupBy("reference_month").count().collect()
    }


def transform_month(
    spark: SparkSession,
    year: int,
    month: int,
    input_path: str | Path | None = None,
    output_path: str | Path | None = None,
    write_mode: str = "overwrite",
) -> int:
    """Transform one bronze yellow month into silver."""
    LOGGER.info(
        "Starting silver transformation: service_type=%s, year=%s, month=%s",
        SERVICE_TYPE,
        year,
        format_month(month),
    )
    LOGGER.info(
        "Single-month silver transformation writes only the requested month. "
        "Use --all-config-months to rebuild the complete configured period."
    )
    bronze_df = read_bronze_yellow(spark, year=year, month=month, input_path=input_path)
    silver_df = transform_bronze_to_silver(bronze_df)
    row_count = write_silver(silver_df, output_path=output_path, write_mode=write_mode)
    LOGGER.info("Finished silver transformation: year=%s, month=%s, rows=%s", year, format_month(month), row_count)
    return row_count


def transform_config_months(
    spark: SparkSession,
    year: int | None = None,
    output_path: str | Path | None = None,
    write_mode: str = "overwrite",
) -> dict[str, int]:
    """Transform all months configured in pipeline.yml into silver."""
    config = load_pipeline_config()
    resolved_year = int(year or config["default_year"])
    dataframes: list[DataFrame] = []
    missing_months: list[str] = []

    if write_mode != "overwrite":
        LOGGER.warning("--all-config-months forces write_mode=overwrite to keep reruns idempotent.")
        write_mode = "overwrite"

    for month in config["months"]:
        month_key = format_month(month)
        try:
            bronze_df = read_bronze_yellow(spark=spark, year=resolved_year, month=int(month))
            dataframes.append(transform_bronze_to_silver(bronze_df))
        except FileNotFoundError as exc:
            LOGGER.error("%s", exc)
            missing_months.append(month_key)

    if missing_months:
        missing_text = ", ".join(missing_months)
        raise FileNotFoundError(
            f"Missing bronze parquet paths for configured months: {missing_text}. "
            "Silver output was not overwritten."
        )

    combined_df = union_dataframes_by_name(dataframes)
    results = collect_reference_month_counts(combined_df)
    write_silver(combined_df, output_path=output_path, write_mode=write_mode)

    return {format_month(month): results.get(format_month(month), 0) for month in config["months"]}


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
    parser = argparse.ArgumentParser(description="Transform bronze yellow taxi trips to silver.")
    parser.add_argument("--year", type=int, default=int(config["default_year"]))
    parser.add_argument("--month", type=int, help="Month to transform, for example 1 for January.")
    parser.add_argument(
        "--all-config-months",
        action="store_true",
        help="Transform all months listed in configs/pipeline.yml.",
    )
    parser.add_argument("--input-path", type=Path, help="Optional explicit bronze input path.")
    parser.add_argument("--output-path", type=Path, help="Optional explicit silver output path.")
    parser.add_argument(
        "--write-mode",
        choices=("append", "overwrite"),
        default="overwrite",
        help="Spark write mode. Overwrite is the default; --all-config-months forces overwrite.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the silver yellow transformation from the CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    args = parse_args()

    if args.all_config_months and args.month:
        LOGGER.error("Use either --month or --all-config-months, not both.")
        return 1
    if args.all_config_months and args.input_path:
        LOGGER.error("--input-path can only be used with a single --month transformation.")
        return 1
    if not args.all_config_months and not args.month:
        LOGGER.error("Provide --month or --all-config-months.")
        return 1

    try:
        spark = get_spark_session("ifood-tlc-silver-yellow")
    except Exception as exc:
        LOGGER.error("Could not start Spark. PySpark local execution requires Java/JDK.")
        LOGGER.error("Install Java and set JAVA_HOME, or run this command in Databricks.")
        LOGGER.error("Original error: %s", exc)
        return 1

    try:
        if args.all_config_months:
            results = transform_config_months(
                spark=spark,
                year=args.year,
                output_path=args.output_path,
                write_mode=args.write_mode,
            )
            LOGGER.info("Silver transformation summary")
            for month, row_count in results.items():
                LOGGER.info("- yellow %s-%s: %s rows", args.year, month, row_count)
            LOGGER.info("Months transformed: %s", len(results))
        else:
            row_count = transform_month(
                spark=spark,
                year=args.year,
                month=args.month,
                input_path=args.input_path,
                output_path=args.output_path,
                write_mode=args.write_mode,
            )
            LOGGER.info("Silver transformation summary")
            LOGGER.info("- yellow %s-%s: %s rows", args.year, format_month(args.month), row_count)
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
        LOGGER.error("Silver transformation failed.")
        LOGGER.error("Original error: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
