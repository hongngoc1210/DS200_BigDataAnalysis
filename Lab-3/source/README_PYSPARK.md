# Lab 3 - PySpark RDD Version

Các file `bai1.py` đến `bai6.py` là bản chuyển từ Hadoop MapReduce Java sang PySpark **RDD API** theo yêu cầu trong `assignments.ipynb`. Không sử dụng Spark SQL/DataFrame.

## Chạy từng bài

Chạy từ thư mục gốc `Lab-3`:

```bash
spark-submit source/bai1.py movies/ratings_1.txt movies/ratings_2.txt movies/movies.txt output_pyspark/bai1 5
spark-submit source/bai2.py movies/ratings_1.txt movies/ratings_2.txt movies/movies.txt output_pyspark/bai2
spark-submit source/bai3.py movies/ratings_1.txt movies/ratings_2.txt movies/movies.txt movies/users.txt output_pyspark/bai3
spark-submit source/bai4.py movies/ratings_1.txt movies/ratings_2.txt movies/movies.txt movies/users.txt output_pyspark/bai4
spark-submit source/bai5.py movies/ratings_1.txt movies/ratings_2.txt movies/users.txt movies/occupation.txt output_pyspark/bai5
spark-submit source/bai6.py movies/ratings_1.txt movies/ratings_2.txt output_pyspark/bai6
```

Hoặc chạy toàn bộ:

```bash
bash source/run_all_pyspark.sh
```

## Ghi chú quan trọng

- `assignments.ipynb` của bài 1 ghi mục tiêu là lọc phim có ít nhất **50** lượt đánh giá, nhưng phần giải pháp lại ghi **5** lượt. Vì dataset lab khá nhỏ, `bai1.py` mặc định dùng `5` để tạo được kết quả top-rated hữu ích. Nếu muốn bám đúng mục tiêu 50, truyền tham số cuối là `50`.
- Các script tự xóa output directory cũ trước khi `saveAsTextFile`, tránh lỗi Spark báo output đã tồn tại.
- Output được `coalesce(1)` để dễ đọc trong một `part-*` file.
