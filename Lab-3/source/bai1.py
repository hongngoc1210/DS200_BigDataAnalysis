#!/usr/bin/env python3
"""Bài 1 - Tính rating trung bình và số lượt đánh giá cho mỗi phim bằng PySpark RDD.

Usage:
    spark-submit source/bai1.py <ratings_1> <ratings_2> <movies> <output> [min_ratings]

Notes:
    assignments.ipynb có một điểm chưa thống nhất: mục tiêu nói lọc phim có ít
    nhất 50 lượt đánh giá, phần giải pháp lại ghi ít nhất 5 lượt.  Script này
    dùng mặc định 5 để phù hợp với dữ liệu lab nhỏ, nhưng có thể truyền tham số
    cuối là 50 nếu muốn bám sát mục tiêu gốc.
"""

from __future__ import annotations

import sys
from pyspark import SparkConf, SparkContext

from common import parse_movie, parse_rating, add_rating_stats, average, save_text_rdd


def build_result(sc: SparkContext, ratings1_path: str, ratings2_path: str, movies_path: str, min_ratings: int):
    movies = (
        sc.textFile(movies_path)
        .map(parse_movie)
        .filter(lambda row: row is not None)
    )
    movie_titles = sc.broadcast(movies.map(lambda row: (row[0], row[1])).collectAsMap())

    ratings = (
        sc.textFile(ratings1_path)
        .union(sc.textFile(ratings2_path))
        .map(parse_rating)
        .filter(lambda row: row is not None)
    )

    stats_by_movie = (
        ratings
        .map(lambda row: (row[1], (row[2], 1)))
        .reduceByKey(add_rating_stats)
        .mapValues(lambda stats: (average(stats), stats[1]))
    )

    detail_lines = (
        stats_by_movie
        .map(lambda item: (
            movie_titles.value.get(item[0], item[0]),
            item[1][0],
            item[1][1],
        ))
        .sortBy(lambda row: row[0])
        .map(lambda row: f"{row[0]}\tAverage Rating: {row[1]:.2f}\tTotal Ratings: {row[2]}")
    )

    top_candidates = (
        stats_by_movie
        .filter(lambda item: item[1][1] >= min_ratings)
        .takeOrdered(1, key=lambda item: (-item[1][0], -item[1][1], movie_titles.value.get(item[0], item[0])))
    )

    if top_candidates:
        movie_id, (avg_rating, count) = top_candidates[0]
        top_title = movie_titles.value.get(movie_id, movie_id)
        top_message = (
            f">>> TOP RATED >>>\t{top_title} is the highest rated movie "
            f"with an average rating of {avg_rating:.2f} among movies with at least {min_ratings} ratings "
            f"(Total Ratings: {count})."
        )
    else:
        top_message = f">>> TOP RATED >>>\tNo movie has at least {min_ratings} ratings."

    return detail_lines.union(sc.parallelize([top_message]))


def main(argv: list[str]) -> int:
    if len(argv) not in {5, 6}:
        print("Usage: bai1.py <ratings_1> <ratings_2> <movies> <output> [min_ratings]", file=sys.stderr)
        return 1

    ratings1_path, ratings2_path, movies_path, output_path = argv[1:5]
    min_ratings = int(argv[5]) if len(argv) == 6 else 5

    conf = SparkConf().setAppName("Lab3-Bai1-Average-Movie-Rating-RDD")
    sc = SparkContext(conf=conf)
    try:
        result = build_result(sc, ratings1_path, ratings2_path, movies_path, min_ratings)
        save_text_rdd(result, output_path)
    finally:
        sc.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
