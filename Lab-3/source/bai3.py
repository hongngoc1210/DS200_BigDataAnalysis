#!/usr/bin/env python3

from __future__ import annotations

import sys
from pyspark import SparkConf, SparkContext

from common import parse_movie, parse_rating, parse_user, add_rating_stats, average, save_text_rdd


GENDERS = ("M", "F")


def build_result(sc: SparkContext, ratings1_path: str, ratings2_path: str, movies_path: str, users_path: str):
    movie_titles = sc.broadcast(
        sc.textFile(movies_path)
        .map(parse_movie)
        .filter(lambda row: row is not None)
        .map(lambda row: (row[0], row[1]))
        .collectAsMap()
    )

    user_gender = (
        sc.textFile(users_path)
        .map(parse_user)
        .filter(lambda row: row is not None)
        .map(lambda row: (row[0], row[1]))
    )

    ratings_by_user = (
        sc.textFile(ratings1_path)
        .union(sc.textFile(ratings2_path))
        .map(parse_rating)
        .filter(lambda row: row is not None)
        .map(lambda row: (row[0], (row[1], row[2])))
    )

    avg_by_movie_gender = (
        ratings_by_user
        .join(user_gender)  # user_id -> ((movie_id, rating), gender)
        .map(lambda item: ((item[1][0][0], item[1][1]), (item[1][0][1], 1)))
        .reduceByKey(add_rating_stats)
        .mapValues(lambda stats: (average(stats), stats[1]))
    )

    grouped_by_movie = (
        avg_by_movie_gender
        .map(lambda item: (item[0][0], (item[0][1], item[1][0], item[1][1])))
        .groupByKey()
    )

    def format_movie_gender(item):
        movie_id, rows_iter = item
        rows = {gender: (avg_rating, count) for gender, avg_rating, count in rows_iter}
        fields = []
        for gender in GENDERS:
            if gender in rows:
                fields.append(f"{gender}: {rows[gender][0]:.2f} (n={rows[gender][1]})")
            else:
                fields.append(f"{gender}: NA (n=0)")
        title = movie_titles.value.get(movie_id, movie_id)
        return title, f"{title}\t" + "\t".join(fields)

    return grouped_by_movie.map(format_movie_gender).sortBy(lambda row: row[0]).map(lambda row: row[1])


def main(argv: list[str]) -> int:
    if len(argv) != 6:
        print("Usage: bai3.py <ratings_1> <ratings_2> <movies> <users> <output>", file=sys.stderr)
        return 1

    ratings1_path, ratings2_path, movies_path, users_path, output_path = argv[1:6]
    conf = SparkConf().setAppName("Lab3-Bai3-Gender-Rating-RDD")
    sc = SparkContext(conf=conf)
    try:
        result = build_result(sc, ratings1_path, ratings2_path, movies_path, users_path)
        save_text_rdd(result, output_path)
    finally:
        sc.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
