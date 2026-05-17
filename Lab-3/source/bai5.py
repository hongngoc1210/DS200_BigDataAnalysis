#!/usr/bin/env python3
"""Bài 5 - Phân tích rating trung bình và tổng lượt đánh giá theo occupation bằng PySpark RDD.

Usage:
    spark-submit source/bai5.py <ratings_1> <ratings_2> <users> <occupation> <output>
"""

from __future__ import annotations

import sys
from pyspark import SparkConf, SparkContext

from common import parse_rating, parse_user, parse_occupation, add_rating_stats, average, save_text_rdd


def build_result(sc: SparkContext, ratings1_path: str, ratings2_path: str, users_path: str, occupations_path: str):
    occupation_names = (
        sc.textFile(occupations_path)
        .map(parse_occupation)
        .filter(lambda row: row is not None)
    )  # occupation_id -> occupation_name

    # First build user_id -> occupation_name using only RDD joins.
    user_id_by_occupation_id = (
        sc.textFile(users_path)
        .map(parse_user)
        .filter(lambda row: row is not None)
        .map(lambda row: (row[3], row[0]))
    )

    user_occupation_name = (
        user_id_by_occupation_id
        .join(occupation_names)  # occupation_id -> (user_id, occupation_name)
        .map(lambda item: (item[1][0], item[1][1]))
    )

    ratings_by_user = (
        sc.textFile(ratings1_path)
        .union(sc.textFile(ratings2_path))
        .map(parse_rating)
        .filter(lambda row: row is not None)
        .map(lambda row: (row[0], row[2]))
    )

    return (
        ratings_by_user
        .join(user_occupation_name)  # user_id -> (rating, occupation_name)
        .map(lambda item: (item[1][1], (item[1][0], 1)))
        .reduceByKey(add_rating_stats)
        .mapValues(lambda stats: (average(stats), stats[1]))
        .sortBy(lambda item: item[0])
        .map(lambda item: f"{item[0]}\tAverage Rating: {item[1][0]:.2f}\tTotal Ratings: {item[1][1]}")
    )


def main(argv: list[str]) -> int:
    if len(argv) != 6:
        print("Usage: bai5.py <ratings_1> <ratings_2> <users> <occupation> <output>", file=sys.stderr)
        return 1

    ratings1_path, ratings2_path, users_path, occupations_path, output_path = argv[1:6]
    conf = SparkConf().setAppName("Lab3-Bai5-Occupation-Rating-RDD")
    sc = SparkContext(conf=conf)
    try:
        result = build_result(sc, ratings1_path, ratings2_path, users_path, occupations_path)
        save_text_rdd(result, output_path)
    finally:
        sc.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
