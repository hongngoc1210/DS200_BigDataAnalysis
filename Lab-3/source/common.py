"""Shared PySpark RDD helpers for Lab 3.

All scripts in this folder use the low-level RDD API only.  No Spark SQL
DataFrame operations are used, because assignments.ipynb explicitly asks for RDD.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from typing import Iterable, Optional, Tuple, List


def parse_csv_line(line: str) -> List[str]:
    """Parse one CSV-like input line safely.

    The provided lab files are comma-separated and mostly unquoted.  Using
    csv.reader keeps the code correct if a future title/occupation is quoted.
    """
    return next(csv.reader([line.strip()]))


def is_blank_or_header(line: str, first_column_name: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    return stripped.lower().startswith(first_column_name.lower())


def parse_movie(line: str) -> Optional[Tuple[str, str, List[str]]]:
    """Return (movie_id, title, genres) from movies.txt."""
    if is_blank_or_header(line, "MovieID"):
        return None

    parts = parse_csv_line(line)
    if len(parts) < 3:
        return None

    movie_id = parts[0].strip()
    # Robust to unquoted commas in titles by treating the final column as Genres.
    title = ",".join(parts[1:-1]).strip()
    genres = [g.strip() for g in parts[-1].strip().split("|") if g.strip()]

    if not movie_id or not title:
        return None
    return movie_id, title, genres


def parse_rating(line: str) -> Optional[Tuple[str, str, float, int]]:
    """Return (user_id, movie_id, rating, timestamp) from ratings_*.txt."""
    if is_blank_or_header(line, "UserID"):
        return None

    parts = parse_csv_line(line)
    if len(parts) < 4:
        return None

    try:
        return (
            parts[0].strip(),
            parts[1].strip(),
            float(parts[2].strip()),
            int(parts[3].strip()),
        )
    except (TypeError, ValueError):
        return None


def parse_user(line: str) -> Optional[Tuple[str, str, int, str, str]]:
    """Return (user_id, gender, age, occupation_id, zip_code) from users.txt."""
    if is_blank_or_header(line, "UserID"):
        return None

    parts = parse_csv_line(line)
    if len(parts) < 5:
        return None

    try:
        return (
            parts[0].strip(),
            parts[1].strip(),
            int(parts[2].strip()),
            parts[3].strip(),
            parts[4].strip(),
        )
    except (TypeError, ValueError):
        return None


def parse_occupation(line: str) -> Optional[Tuple[str, str]]:
    """Return (occupation_id, occupation_name) from occupation.txt."""
    if is_blank_or_header(line, "ID") or line.strip().lower().startswith("occupationid"):
        return None

    parts = parse_csv_line(line)
    if len(parts) < 2:
        return None

    occupation_id = parts[0].strip()
    occupation_name = ",".join(parts[1:]).strip()
    if not occupation_id or not occupation_name:
        return None
    return occupation_id, occupation_name


def add_rating_stats(left: Tuple[float, int], right: Tuple[float, int]) -> Tuple[float, int]:
    """Reduce helper for (sum_rating, count)."""
    return left[0] + right[0], left[1] + right[1]


def average(stats: Tuple[float, int]) -> float:
    rating_sum, count = stats
    return rating_sum / count if count else 0.0


def format_avg(value: Optional[float]) -> str:
    return "NA" if value is None else f"{value:.2f}"


def timestamp_to_year(unix_seconds: int) -> int:
    return datetime.fromtimestamp(unix_seconds, tz=timezone.utc).year


def get_age_group(age: int) -> str:
    if age < 18:
        return "0-18"
    if age < 35:
        return "18-35"
    if age < 50:
        return "35-50"
    return "50+"


def delete_output_if_exists(sc, output_path: str) -> None:
    """Delete existing output path for both local filesystem and HDFS paths."""
    hadoop_conf = sc._jsc.hadoopConfiguration()
    path = sc._jvm.org.apache.hadoop.fs.Path(output_path)
    fs = path.getFileSystem(hadoop_conf)
    if fs.exists(path):
        fs.delete(path, True)


def save_text_rdd(rdd, output_path: str, single_file: bool = True) -> None:
    """Save an RDD[str] to text output, replacing the old output directory."""
    delete_output_if_exists(rdd.context, output_path)
    target_rdd = rdd.coalesce(1) if single_file else rdd
    target_rdd.saveAsTextFile(output_path)
