"""Landing to bronze ingestion for NYC TLC parquet files."""

from __future__ import annotations

import argparse
import logging
import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from ..config import load_pipeline_config
from ..paths import format_month, get_bronze_path, get_landing_path
from ..spark import get_spark_session


LOGGER = logging.getLogger(__name__)

BRONZE_SERVICE_TYPES = ("yellow",)
PARTITION_COLUMNS = ("service_type", "reference_year", "reference_month")


def validate_service_type(service_type: str) -> str:
    """Validate service types accepted by bronze ingestion."""
    normalized_service_type = service_type.lower()
    if normalized_service_type not in BRONZE_SERVICE_TYPES:
        supported = ", ".join(BRONZE_SERVICE_TYPES)
        raise ValueError(f"Unsupported service_type '{service_type}'. Supported: {supported}.")

    return normalized_service_type


def add_bronze_metadata(
    df: DataFrame,
    service_type: str,
    year: int,
    month: int,
    ingestion_run_id: str,
) -> DataFrame:
    """Add technical columns required in bronze."""
    return (
        df.withColumn("service_type", F.lit(service_type))
        .withColumn("reference_year", F.lit(int(year)))
        .withColumn("reference_month", F.lit(format_month(month)).cast("string"))
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_run_id", F.lit(ingestion_run_id))
    )


def union_dataframes_by_name(dataframes: list[DataFrame]) -> DataFrame:
    """Union dataframes by column name, allowing optional schema differences."""
    if not dataframes:
        raise ValueError("No bronze dataframes available to union.")

    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)

    return combined_df


def build_landing_input_path(
    service_type: str,
    year: int,
    month: int,
) -> Path:
    """Build the expected landing path for any TLC service type."""
    resolved_service_type = validate_service_type(service_type)
    resolved_month = format_month(month)
    return (
        get_landing_path()
        / "tlc"
        / resolved_service_type
        / f"year={year}"
        / f"month={resolved_month}"
        / f"{resolved_service_type}_tripdata_{year}-{resolved_month}.parquet"
    )


def build_bronze_dataframe(
    spark: SparkSession,
    service_type: str,
    year: int,
    month: int,
    input_path: str | Path | None = None,
    ingestion_run_id: str | None = None,
) -> DataFrame:
    """Read one landing parquet file and add bronze metadata."""
    resolved_service_type = validate_service_type(service_type)
    resolved_input_path = Path(input_path) if input_path else build_landing_input_path(
        year=year,
        month=month,
        service_type=resolved_service_type,
    )
    resolved_run_id = ingestion_run_id or str(uuid.uuid4())

    if _is_local_path(resolved_input_path) and not resolved_input_path.exists():
        raise FileNotFoundError(f"Landing parquet file not found: {resolved_input_path}")

    LOGGER.info(
        f"Starting bronze ingestion: service_type={resolved_service_type}, "
        f"year={year}, month={format_month(month)}, run_id={resolved_run_id}"
    )
    LOGGER.info("Input: %s", resolved_input_path)

    try:
        landing_df = spark.read.parquet(str(resolved_input_path))
    except AnalysisException as exc:
        raise RuntimeError(f"Could not read landing parquet file: {resolved_input_path}") from exc

    return add_bronze_metadata(
        landing_df,
        service_type=resolved_service_type,
        year=year,
        month=month,
        ingestion_run_id=resolved_run_id,
    )


def write_bronze(
    df: DataFrame,
    output_path: str | Path | None = None,
    write_mode: str = "overwrite",
) -> int:
    """Write bronze data once and return the row count."""
    resolved_output_path = Path(output_path) if output_path else get_bronze_path(create=True)
    if _is_local_path(resolved_output_path):
        resolved_output_path.mkdir(parents=True, exist_ok=True)

    row_count = df.count()
    LOGGER.info("Writing bronze data to: %s", resolved_output_path)
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


def ingest_month(
    spark: SparkSession,
    service_type: str,
    year: int,
    month: int,
    input_path: str | Path | None = None,
    output_path: str | Path | None = None,
    ingestion_run_id: str | None = None,
    write_mode: str = "overwrite",
) -> int:
    """Ingest one landing parquet file into bronze."""
    resolved_service_type = validate_service_type(service_type)
    resolved_run_id = ingestion_run_id or str(uuid.uuid4())

    LOGGER.info(
        "Single-month bronze ingestion writes only the requested month. "
        "Use --all-config-months to rebuild the complete configured period."
    )

    bronze_df = build_bronze_dataframe(
        spark=spark,
        service_type=resolved_service_type,
        year=year,
        month=month,
        input_path=input_path,
        ingestion_run_id=resolved_run_id,
    )
    row_count = write_bronze(bronze_df, output_path=output_path, write_mode=write_mode)

    LOGGER.info(
        f"Finished bronze ingestion: service_type={resolved_service_type}, "
        f"year={year}, month={format_month(month)}, rows={row_count}"
    )
    return row_count


def ingest_config_months(
    spark: SparkSession,
    service_type: str,
    year: int | None = None,
    output_path: str | Path | None = None,
    write_mode: str = "overwrite",
) -> dict[str, int]:
    """Ingest all months configured in pipeline.yml."""
    config = load_pipeline_config()
    resolved_year = int(year or config["default_year"])
    resolved_service_type = validate_service_type(service_type)
    resolved_run_id = str(uuid.uuid4())
    dataframes: list[DataFrame] = []
    missing_months: list[str] = []

    if write_mode != "overwrite":
        LOGGER.warning("--all-config-months forces write_mode=overwrite to keep reruns idempotent.")
        write_mode = "overwrite"

    for month in config["months"]:
        month_key = format_month(month)
        try:
            dataframes.append(
                build_bronze_dataframe(
                    spark=spark,
                    service_type=resolved_service_type,
                    year=resolved_year,
                    month=int(month),
                    ingestion_run_id=resolved_run_id,
                )
            )
        except FileNotFoundError as exc:
            LOGGER.error("%s", exc)
            missing_months.append(month_key)

    if missing_months:
        missing_text = ", ".join(missing_months)
        raise FileNotFoundError(
            f"Missing landing parquet files for configured months: {missing_text}. "
            "Bronze output was not overwritten."
        )

    combined_df = union_dataframes_by_name(dataframes)
    results = collect_reference_month_counts(combined_df)
    write_bronze(combined_df, output_path=output_path, write_mode=write_mode)

    return {format_month(month): results.get(format_month(month), 0) for month in config["months"]}


def _is_local_path(path: Path) -> bool:
    """Return whether a path should be checked with pathlib locally."""
    path_text = str(path)
    return not (
        path_text.startswith("dbfs:")
        or path_text.startswith("s3:")
        or path_text.startswith("s3a:")
        or path_text.startswith("abfss:")
        or path_text.startswith("gs:")
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for bronze ingestion."""
    config = load_pipeline_config()
    parser = argparse.ArgumentParser(description="Ingest NYC TLC landing parquet files to bronze.")
    parser.add_argument(
        "--service-type",
        default=config.get("primary_service_type", "yellow"),
        help="TLC service type to ingest. Yellow is the mandatory case focus.",
    )
    parser.add_argument("--year", type=int, default=int(config["default_year"]))
    parser.add_argument("--month", type=int, help="Month to ingest, for example 1 for January.")
    parser.add_argument(
        "--all-config-months",
        action="store_true",
        help="Ingest all months listed in configs/pipeline.yml.",
    )
    parser.add_argument("--input-path", type=Path, help="Optional explicit input parquet path.")
    parser.add_argument("--output-path", type=Path, help="Optional explicit bronze output root.")
    parser.add_argument(
        "--write-mode",
        choices=("append", "overwrite"),
        default="overwrite",
        help="Spark write mode. Overwrite is the default; --all-config-months forces overwrite.",
    )
    return parser.parse_args()


def main() -> int:
    """Run bronze ingestion from the CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    args = parse_args()

    try:
        args.service_type = validate_service_type(args.service_type)
    except ValueError as exc:
        LOGGER.error("%s", exc)
        return 1

    if args.all_config_months and args.month:
        LOGGER.error("Use either --month or --all-config-months, not both.")
        return 1

    if args.all_config_months and args.input_path:
        LOGGER.error("--input-path can only be used with a single --month ingestion.")
        return 1

    if not args.all_config_months and not args.month:
        LOGGER.error("Provide --month or --all-config-months.")
        return 1

    try:
        spark = get_spark_session("ifood-tlc-bronze-ingestion")
    except Exception as exc:
        LOGGER.error("Could not start Spark. PySpark local execution requires Java/JDK.")
        LOGGER.error("Install Java and set JAVA_HOME, or run this command in Databricks.")
        LOGGER.error("Original error: %s", exc)
        return 1

    try:
        if args.all_config_months:
            results = ingest_config_months(
                spark=spark,
                service_type=args.service_type,
                year=args.year,
                output_path=args.output_path,
                write_mode=args.write_mode,
            )
            LOGGER.info("Bronze ingestion summary")
            for month, row_count in results.items():
                LOGGER.info("- %s %s-%s: %s rows", args.service_type, args.year, month, row_count)
            LOGGER.info("Months ingested: %s", len(results))
        else:
            row_count = ingest_month(
                spark=spark,
                service_type=args.service_type,
                year=args.year,
                month=args.month,
                input_path=args.input_path,
                output_path=args.output_path,
                write_mode=args.write_mode,
            )
            LOGGER.info("Bronze ingestion summary")
            LOGGER.info("- %s %s-%s: %s rows", args.service_type, args.year, format_month(args.month), row_count)
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
        LOGGER.error("Bronze ingestion failed.")
        LOGGER.error("Original error: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
