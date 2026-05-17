#!/usr/bin/env bash
set -euo pipefail

# Run from the Lab-3 root directory:
#   bash source/run_all_pyspark.sh

RATINGS_1="movies/ratings_1.txt"
RATINGS_2="movies/ratings_2.txt"
MOVIES="movies/movies.txt"
USERS="movies/users.txt"
OCCUPATIONS="movies/occupation.txt"
OUT_DIR="output_pyspark"

spark-submit --py-files source/common.py source/bai1.py "$RATINGS_1" "$RATINGS_2" "$MOVIES" "$OUT_DIR/bai1" 5
spark-submit --py-files source/common.py source/bai2.py "$RATINGS_1" "$RATINGS_2" "$MOVIES" "$OUT_DIR/bai2"
spark-submit --py-files source/common.py source/bai3.py "$RATINGS_1" "$RATINGS_2" "$MOVIES" "$USERS" "$OUT_DIR/bai3"
spark-submit --py-files source/common.py source/bai4.py "$RATINGS_1" "$RATINGS_2" "$MOVIES" "$USERS" "$OUT_DIR/bai4"
spark-submit --py-files source/common.py source/bai5.py "$RATINGS_1" "$RATINGS_2" "$USERS" "$OCCUPATIONS" "$OUT_DIR/bai5"
spark-submit --py-files source/common.py source/bai6.py "$RATINGS_1" "$RATINGS_2" "$OUT_DIR/bai6"
