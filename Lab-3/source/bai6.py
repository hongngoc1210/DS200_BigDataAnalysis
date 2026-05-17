#!/usr/bin/env python3
"""Bài 6 - Phân tích tổng số lượt đánh giá và rating trung bình theo năm bằng PySpark RDD.

Usage:
    spark-submit source/bai6.py <ratings_1> <ratings_2> <output>
"""

from __future__ import annotations

import sys
from pyspark import SparkConf, SparkContext

from common import parse_rating, add_rating_stats, average, timestamp_to_year, save_text_rdd


def build_result(sc: SparkContext, ratings1_path: str, ratings2_path: str):
    ratings = (
        sc.textFile(ratings1_path)
        .union(sc.textFile(ratings2_path))
        .map(parse_rating)
        .filter(lambda row: row is not None)
    )

    return (
        ratings
        .map(lambda row: (timestamp_to_year(row[3]), (row[2], 1)))
        .reduceByKey(add_rating_stats)
        .mapValues(lambda stats: (average(stats), stats[1]))
        .sortBy(lambda item: item[0])
        .map(lambda item: f"{item[0]}\tAverage Rating: {item[1][0]:.2f}\tTotal Ratings: {item[1][1]}")
    )


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("Usage: bai6.py <ratings_1> <ratings_2> <output>", file=sys.stderr)
        return 1

    ratings1_path, ratings2_path, output_path = argv[1:4]
    conf = SparkConf().setAppName("Lab3-Bai6-Time-Rating-RDD")
    sc = SparkContext(conf=conf)
    try:
        result = build_result(sc, ratings1_path, ratings2_path)
        save_text_rdd(result, output_path)
    finally:
        sc.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
