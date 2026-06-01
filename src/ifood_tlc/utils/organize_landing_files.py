"""Organize raw NYC TLC parquet files into the landing zone layout."""

from __future__ import annotations

import argparse
import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path


LOGGER = logging.getLogger(__name__)

SUPPORTED_SERVICE_TYPES = ("yellow",)
FILE_PATTERN = re.compile(
    r"^(?P<service_type>yellow)_tripdata_"
    r"(?P<year>\d{4})-(?P<month>\d{2})\.parquet$"
)


@dataclass(frozen=True)
class LandingFile:
    source_path: Path
    service_type: str
    year: str
    month: str

    def target_path(self, target_root: Path) -> Path:
        return (
            target_root
            / self.service_type
            / f"year={self.year}"
            / f"month={self.month}"
            / self.source_path.name
        )


def parse_landing_file(path: Path) -> LandingFile | None:
    match = FILE_PATTERN.match(path.name)
    if not match:
        return None

    month = match.group("month")
    if not 1 <= int(month) <= 12:
        return None

    return LandingFile(
        source_path=path,
        service_type=match.group("service_type"),
        year=match.group("year"),
        month=month,
    )


def organize_files(source_dir: Path, target_root: Path, move_files: bool) -> dict[str, int]:
    summary = {"organized": 0, "ignored": 0, "existing": 0}
    action = "Moving" if move_files else "Copying"

    if not source_dir.exists():
        LOGGER.warning("Source directory does not exist: %s", source_dir)
        return summary

    for source_path in sorted(source_dir.iterdir()):
        if not source_path.is_file():
            LOGGER.warning("Ignoring non-file entry: %s", source_path)
            summary["ignored"] += 1
            continue

        landing_file = parse_landing_file(source_path)
        if landing_file is None:
            LOGGER.warning("Ignoring file with unexpected name pattern or unsupported service type: %s", source_path.name)
            summary["ignored"] += 1
            continue

        target_path = landing_file.target_path(target_root)
        if target_path.exists():
            LOGGER.warning("Target already exists, skipping: %s", target_path)
            summary["existing"] += 1
            continue

        target_path.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info("%s: %s -> %s", action, source_path, target_path)

        if move_files:
            shutil.move(str(source_path), str(target_path))
        else:
            shutil.copy2(source_path, target_path)

        summary["organized"] += 1

    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Organize NYC TLC parquet files into the project landing zone."
    )
    parser.add_argument(
        "--source-dir",
        default="data/landing/raw_downloads",
        type=Path,
        help="Directory containing downloaded raw parquet files.",
    )
    parser.add_argument(
        "--target-root",
        default="data/landing/tlc",
        type=Path,
        help="Root directory for organized TLC landing files.",
    )
    parser.add_argument(
        "--move",
        action="store_true",
        help="Move files instead of copying them. Copy is the default for safety.",
    )
    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    args = build_parser().parse_args()
    summary = organize_files(args.source_dir, args.target_root, args.move)

    LOGGER.info("Summary")
    LOGGER.info("- Organized: %s", summary["organized"])
    LOGGER.info("- Ignored: %s", summary["ignored"])
    LOGGER.info("- Already existing: %s", summary["existing"])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
