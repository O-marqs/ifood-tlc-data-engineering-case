"""Inspect schemas of NYC TLC parquet files available in landing."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from ..config import load_pipeline_config
from ..paths import format_month, get_landing_path
from ..spark import get_spark_session


LOGGER = logging.getLogger(__name__)

EXPLORATORY_SERVICE_TYPES = ("yellow",)
REQUIRED_COLUMNS = (
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
)


@dataclass(frozen=True)
class SchemaInspection:
    service_type: str
    year: int
    month: str
    path: Path
    row_count: int
    column_count: int
    columns: list[str]
    missing_required_columns: list[str]
    schema: StructType

    @property
    def contains_required_columns(self) -> bool:
        """Return whether all mandatory case columns are present."""
        return not self.missing_required_columns


def build_landing_path(
    service_type: str,
    year: int,
    month: int,
    data_dir: str | Path | None = None,
) -> Path:
    """Build a landing parquet path for schema exploration."""
    month_value = format_month(month)
    return (
        get_landing_path(data_dir=data_dir)
        / "tlc"
        / service_type
        / f"year={year}"
        / f"month={month_value}"
        / f"{service_type}_tripdata_{year}-{month_value}.parquet"
    )


def inspect_parquet(
    spark: SparkSession,
    path: Path,
    service_type: str,
    year: int,
    month: int,
) -> SchemaInspection:
    """Read a parquet file and collect schema metadata."""
    df = spark.read.parquet(str(path))
    columns = df.columns
    missing_required_columns = [column for column in REQUIRED_COLUMNS if column not in columns]
    row_count = df.count()

    LOGGER.info("Schema for %s %s-%s", service_type, year, format_month(month))
    LOGGER.info("Path: %s", path)
    df.printSchema()
    LOGGER.info("Rows: %s", row_count)
    LOGGER.info("Columns: %s", len(columns))
    LOGGER.info("Column list: %s", ", ".join(columns))

    return SchemaInspection(
        service_type=service_type,
        year=year,
        month=format_month(month),
        path=path,
        row_count=row_count,
        column_count=len(columns),
        columns=columns,
        missing_required_columns=missing_required_columns,
        schema=df.schema,
    )


def compare_schemas(inspections: list[SchemaInspection]) -> list[str]:
    """Compare schemas across inspected files."""
    lines = ["## Schema Comparison", ""]
    if len(inspections) < 2:
        lines.append("Not enough available files to compare schemas.")
        return lines

    base = inspections[0]
    base_fields = {(field.name, field.dataType.simpleString()) for field in base.schema.fields}
    lines.append(f"Reference schema: `{base.service_type}` {base.year}-{base.month}")
    lines.append("")

    for inspection in inspections[1:]:
        fields = {(field.name, field.dataType.simpleString()) for field in inspection.schema.fields}
        only_in_base = sorted(base_fields - fields)
        only_in_current = sorted(fields - base_fields)

        lines.append(f"### {inspection.service_type} {inspection.year}-{inspection.month}")
        if not only_in_base and not only_in_current:
            lines.append("Schema matches the reference schema.")
        else:
            lines.append(f"Fields only in reference: {format_field_diff(only_in_base)}")
            lines.append(f"Fields only in this file: {format_field_diff(only_in_current)}")
        lines.append("")

    return lines


def format_field_diff(fields: list[tuple[str, str]]) -> str:
    """Format schema field differences for Markdown output."""
    if not fields:
        return "none"

    return ", ".join(f"{name}:{data_type}" for name, data_type in fields)


def build_markdown_summary(
    inspections: list[SchemaInspection],
    missing_files: list[Path],
) -> str:
    """Build a Markdown summary for the landing schema inspection."""
    lines = [
        "# Landing Schema Inspection",
        "",
        "Yellow taxi is the primary dataset for the mandatory case analyses.",
        "Green, FHV and FHVHV are future extensions and are not processed in this version.",
        "",
        "## Required Columns",
        "",
    ]
    lines.extend(f"- `{column}`" for column in REQUIRED_COLUMNS)
    lines.extend(["", "## Inspected Files", ""])

    if not inspections:
        lines.append("No parquet files were available for inspection.")
    for inspection in inspections:
        lines.extend(
            [
                f"### {inspection.service_type} {inspection.year}-{inspection.month}",
                "",
                f"- Path: `{inspection.path}`",
                f"- Rows: {inspection.row_count}",
                f"- Columns: {inspection.column_count}",
                f"- Contains required columns: {inspection.contains_required_columns}",
                f"- Missing required columns: {format_missing_columns(inspection)}",
                f"- Column list: {', '.join(f'`{column}`' for column in inspection.columns)}",
                "",
            ]
        )

    lines.extend(["## Missing Expected Files", ""])
    if missing_files:
        lines.extend(f"- `{path}`" for path in missing_files)
    else:
        lines.append("No expected files were missing for the requested parameters.")
    lines.append("")

    lines.extend(compare_schemas(inspections))
    lines.append("")

    return "\n".join(lines)


def format_missing_columns(inspection: SchemaInspection) -> str:
    """Format missing required columns for one inspected file."""
    if inspection.contains_required_columns:
        return "none"

    return ", ".join(f"`{column}`" for column in inspection.missing_required_columns)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    config = load_pipeline_config()
    parser = argparse.ArgumentParser(
        description="Inspect schemas of parquet files available in the landing zone."
    )
    parser.add_argument("--year", type=int, default=int(config["default_year"]))
    parser.add_argument(
        "--months",
        type=int,
        nargs="+",
        default=[int(month) for month in config["months"]],
    )
    parser.add_argument(
        "--service-types",
        nargs="+",
        default=list(EXPLORATORY_SERVICE_TYPES),
        choices=EXPLORATORY_SERVICE_TYPES,
        help="Service types to inspect. This version supports only yellow.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Optional data root. Defaults to DATA_DIR or data/.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("analysis/landing_schema_summary.md"),
        help="Markdown summary output path.",
    )
    return parser.parse_args()


def main() -> int:
    """Inspect available landing files and write a Markdown summary."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    args = parse_args()
    try:
        spark = get_spark_session("ifood-tlc-landing-schema-inspection")
    except Exception as exc:
        LOGGER.error("Could not start Spark. PySpark local execution requires Java/JDK.")
        LOGGER.error("Install Java and set JAVA_HOME, or run this script in Databricks.")
        LOGGER.error("Original error: %s", exc)
        return 1

    inspections: list[SchemaInspection] = []
    missing_files: list[Path] = []

    LOGGER.info("Yellow taxi is the primary dataset for the mandatory case analyses.")

    for service_type in args.service_types:
        for month in args.months:
            path = build_landing_path(service_type, args.year, month, args.data_dir)
            if not path.exists():
                LOGGER.warning("Expected file not found, skipping: %s", path)
                missing_files.append(path)
                continue

            inspections.append(inspect_parquet(spark, path, service_type, args.year, month))

    summary = build_markdown_summary(inspections, missing_files)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(summary, encoding="utf-8")

    LOGGER.info("Markdown summary written to: %s", args.output)
    LOGGER.info("Files inspected: %s", len(inspections))
    LOGGER.info("Missing expected files: %s", len(missing_files))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
