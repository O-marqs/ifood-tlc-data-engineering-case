"""Export small evidence files for the mandatory iFood case answers."""

from __future__ import annotations

import argparse
import logging
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


LOGGER = logging.getLogger(__name__)
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "analysis" / "outputs"


def read_gold_dataset(spark: SparkSession, dataset_name: str) -> DataFrame:
    """Read one gold dataset from the configured gold path."""
    path = get_gold_path() / dataset_name
    if _is_local_path(path) and not path.exists():
        raise FileNotFoundError(
            f"Required gold dataset not found: {path}. Run the gold pipeline before exporting outputs."
        )

    LOGGER.info("Reading %s from %s", dataset_name, path)
    return spark.read.parquet(str(path))


def build_question_1(monthly_kpis: DataFrame) -> DataFrame:
    """Build monthly average total amount answer."""
    return (
        monthly_kpis.select(
            "reference_year",
            "reference_month",
            F.round("avg_total_amount", 2).alias("avg_total_amount"),
            "total_trips",
            F.round("total_revenue", 2).alias("total_revenue"),
        )
        .orderBy("reference_year", "reference_month")
    )


def build_question_2(hourly_kpis: DataFrame) -> DataFrame:
    """Build May 2023 hourly passenger average answer."""
    return (
        hourly_kpis.filter(
            (F.col("reference_year") == 2023)
            & (F.col("reference_month") == "05")
        )
        .select(
            "pickup_hour",
            F.round("avg_passenger_count", 2).alias("avg_passenger_count"),
            "total_trips",
        )
        .orderBy("pickup_hour")
    )


def write_csv(df: DataFrame, output_path: Path) -> None:
    """Write a small CSV folder with header."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_path))
    LOGGER.info("Wrote %s", output_path)


def write_markdown_table(df: DataFrame, output_path: Path, title: str) -> None:
    """Write a small DataFrame as a Markdown table."""
    rows = df.collect()
    columns = df.columns
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [f"# {title}", ""]
    if not rows:
        lines.append("Nenhum registro retornado.")
    else:
        lines.append("| " + " | ".join(columns) + " |")
        lines.append("| " + " | ".join(["---"] * len(columns)) + " |")
        for row in rows:
            lines.append("| " + " | ".join(str(row[column]) for column in columns) + " |")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    LOGGER.info("Wrote %s", output_path)


def quality_summary_available() -> bool:
    """Return whether the quality report path exists locally or should be attempted remotely."""
    path = get_gold_path() / "quality" / "quality_yellow_trips_report"
    return not _is_local_path(path) or path.exists()


def build_quality_summary(spark: SparkSession) -> DataFrame | None:
    """Build a compact quality report summary when the quality report exists."""
    if not quality_summary_available():
        LOGGER.warning("Quality report not found. Skipping quality summary export.")
        return None

    quality_df = read_gold_dataset(spark, "quality/quality_yellow_trips_report")
    return (
        quality_df.groupBy("reference_year", "reference_month", "rule_name")
        .agg(
            F.max("total_records").alias("total_records"),
            F.sum("failed_records").alias("failed_records"),
            F.round(F.max("failed_percentage"), 4).alias("max_failed_percentage"),
        )
        .filter(F.col("failed_records") > 0)
        .orderBy("reference_year", "reference_month", F.desc("failed_records"), "rule_name")
    )


def write_quality_not_available(output_path: Path) -> None:
    """Write a small note when quality report is not available."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "# Quality report summary\n\n"
        "O relatório de qualidade não estava disponível no momento da exportação.\n",
        encoding="utf-8",
    )


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
    parser = argparse.ArgumentParser(description="Export small case answer outputs.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for small CSV evidence files.",
    )
    return parser.parse_args()


def main() -> int:
    """Export mandatory case answers from gold datasets."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    args = parse_args()

    try:
        spark = get_spark_session("ifood-tlc-export-case-outputs")
    except Exception as exc:
        LOGGER.error("Could not start Spark. PySpark local execution requires Java/JDK.")
        LOGGER.error("Install Java and set JAVA_HOME, use Docker, or run this command in Databricks.")
        LOGGER.error("Original error: %s", exc)
        return 1

    try:
        monthly_kpis = read_gold_dataset(spark, "gold_monthly_yellow_kpis")
        hourly_kpis = read_gold_dataset(spark, "gold_hourly_passenger_kpis")
    except FileNotFoundError as exc:
        LOGGER.error("%s", exc)
        return 1

    question_1 = build_question_1(monthly_kpis)
    question_2 = build_question_2(hourly_kpis)
    quality_summary = build_quality_summary(spark)

    LOGGER.info("Question 1 preview")
    question_1.show(truncate=False)
    LOGGER.info("Question 2 preview")
    question_2.show(24, truncate=False)

    write_csv(question_1, args.output_dir / "question_1_monthly_avg_total_amount")
    write_csv(question_2, args.output_dir / "question_2_may_hourly_avg_passengers")
    write_markdown_table(
        question_1,
        args.output_dir / "question_1_monthly_avg_total_amount.md",
        "Pergunta 1 - Média mensal de total_amount",
    )
    write_markdown_table(
        question_2,
        args.output_dir / "question_2_may_hourly_avg_passengers.md",
        "Pergunta 2 - Média de passageiros por hora em maio de 2023",
    )

    if quality_summary is not None:
        write_csv(quality_summary, args.output_dir / "quality_report_summary")
        write_markdown_table(
            quality_summary,
            args.output_dir / "quality_report_summary.md",
            "Resumo do quality report",
        )
    else:
        write_quality_not_available(args.output_dir / "quality_report_summary.md")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
