#!/usr/bin/env python3

from __future__ import annotations

import sys
from pyspark import SparkConf, SparkContext

from common import parse_movie, parse_rating, add_rating_stats, average, save_text_rdd


def build_result(sc: SparkContext, ratings1_path: str, ratings2_path: str, movies_path: str):
    movie_genres = (
        sc.textFile(movies_path)
        .map(parse_movie)
        .filter(lambda row: row is not None)
        .map(lambda row: (row[0], row[2]))
    )

    ratings_by_movie = (
        sc.textFile(ratings1_path)
        .union(sc.textFile(ratings2_path))
        .map(parse_rating)
        .filter(lambda row: row is not None)
        .map(lambda row: (row[1], row[2]))
    )

    genre_rating_pairs = (
        ratings_by_movie
        .join(movie_genres)  # movie_id -> (rating, [genres])
        .flatMap(lambda item: [(genre, (item[1][0], 1)) for genre in item[1][1]])
    )

    return (
        genre_rating_pairs
        .reduceByKey(add_rating_stats)
        .mapValues(lambda stats: (average(stats), stats[1]))
        .sortBy(lambda item: item[0])
        .map(lambda item: f"{item[0]}\tAverage Rating: {item[1][0]:.2f}\tTotal Ratings: {item[1][1]}")
    )


def main(argv: list[str]) -> int:
    if len(argv) != 5:
        print("Usage: bai2.py <ratings_1> <ratings_2> <movies> <output>", file=sys.stderr)
        return 1

    ratings1_path, ratings2_path, movies_path, output_path = argv[1:5]
    conf = SparkConf().setAppName("Lab3-Bai2-Genre-Rating-RDD")
    sc = SparkContext(conf=conf)
    try:
        result = build_result(sc, ratings1_path, ratings2_path, movies_path)
        save_text_rdd(result, output_path)
    finally:
        sc.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
