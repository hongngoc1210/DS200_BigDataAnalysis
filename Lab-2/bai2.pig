-- Tải dữ liệu từ file CSV với dấu phân cách là ';'
data = LOAD '/hotel-review.csv' USING PigStorage(';') AS (
    id: int, 
    review: chararray, 
    topic: chararray, 
    aspect: chararray, 
    sentiment: chararray
);

-- Kiểm tra cấu trúc schema của dữ liệu
DESCRIBE data;

-- Chuyển toàn bộ review sang chữ thường
data = FOREACH data GENERATE 
    LOWER(review) AS review, 
    topic         AS topic, 
    aspect        AS aspect, 
    sentiment     AS sentiment;

-- Loại bỏ các ký tự dấu câu đặc biệt (@, !, ^, ?, ., ,)
data = FOREACH data GENERATE 
    REPLACE(review, '[\\@\\!\\^\\!\\?\\.\\,]', '') AS review, 
    topic                                          AS topic, 
    aspect                                         AS aspect, 
    sentiment                                      AS sentiment;

-- Loại bỏ số và các ký tự toán học/ký hiệu khác
data = FOREACH data GENERATE 
    REPLACE(review, '[0-9%\\\\-?+/&:=~<>]', ' ') AS review, 
    topic                                         AS topic, 
    aspect                                        AS aspect, 
    sentiment                                     AS sentiment;

-- Tách chuỗi review thành các từ riêng biệt và trải phẳng
words = FOREACH data GENERATE 
    FLATTEN(TOKENIZE(review)) AS word, 
    topic                     AS topic, 
    aspect                    AS aspect, 
    sentiment                 AS sentiment;

-- Tải danh sách từ dừng
stopwords = LOAD '/stopwords.txt' USING PigStorage() AS (word: chararray);

-- LEFT OUTER JOIN để kết hợp từ với bảng từ dừng
words = JOIN words BY word LEFT OUTER, stopwords BY word;

-- Giữ lại những từ KHÔNG nằm trong danh sách từ dừng
words = FILTER words BY stopwords::word IS NULL;

-- Chọn lại các cột cần thiết sau khi Join
words = FOREACH words GENERATE 
    words::word      AS word, 
    words::topic     AS topic, 
    words::aspect    AS aspect, 
    words::sentiment AS sentiment;


-- Thống kê tần suất xuất hiện các từ (500 lần)

-- Nhóm tất cả các dòng theo từng từ
word_groups = GROUP words BY word;

-- Đếm số lần xuất hiện của mỗi từ
word_count = FOREACH word_groups GENERATE 
    group        AS word, 
    COUNT(words) AS freq;

-- Lọc chỉ giữ lại các từ xuất hiện trên 500 lần
word_count_filtered = FILTER word_count BY freq > 500L;

-- Sắp xếp theo tần số giảm dần
word_count_sorted = ORDER word_count_filtered BY freq DESC;

-- Lưu kết quả thống kê tần số từ
STORE word_count_sorted INTO '/output/word_frequency' USING PigStorage(',');

-- (Tùy chọn) In kết quả ra màn hình để kiểm tra
DUMP word_count_sorted;

-- Thống kê theo phân loại

-- Tải lại dữ liệu gốc để thống kê theo dòng bình luận (không dùng words đã tách)
raw_data = LOAD '/hotel-review.csv' USING PigStorage(';') AS (
    id: int, 
    review: chararray, 
    topic: chararray, 
    aspect: chararray, 
    sentiment: chararray
);

-- Loại bỏ các bình luận trùng lặp cùng id và topic (1 bình luận có thể có nhiều aspect)
-- Dùng DISTINCT để tránh đếm trùng cùng 1 review
unique_by_topic = FOREACH raw_data GENERATE 
    id    AS id, 
    topic AS topic;

unique_by_topic = DISTINCT unique_by_topic;

-- Nhóm theo topic (category)
topic_groups = GROUP unique_by_topic BY topic;

-- Đếm số bình luận theo từng topic
topic_count = FOREACH topic_groups GENERATE 
    group                    AS topic, 
    COUNT(unique_by_topic)   AS num_comments;

-- Sắp xếp theo số lượng giảm dần
topic_count_sorted = ORDER topic_count BY num_comments DESC;

-- Lưu kết quả thống kê theo topic
STORE topic_count_sorted INTO '/output/comments_by_topic' USING PigStorage(',');

-- In kết quả ra màn hình
DUMP topic_count_sorted;

-- Thống kê theo từng khía cạnh

-- Loại bỏ trùng lặp theo id và aspect
unique_by_aspect = FOREACH raw_data GENERATE 
    id     AS id, 
    aspect AS aspect;

unique_by_aspect = DISTINCT unique_by_aspect;

-- Nhóm theo aspect
aspect_groups = GROUP unique_by_aspect BY aspect;

-- Đếm số bình luận theo từng aspect
aspect_count = FOREACH aspect_groups GENERATE 
    group                    AS aspect, 
    COUNT(unique_by_aspect)  AS num_comments;

-- Sắp xếp theo số lượng giảm dần
aspect_count_sorted = ORDER aspect_count BY num_comments DESC;

-- Lưu kết quả thống kê theo aspect
STORE aspect_count_sorted INTO '/output/results_b2' USING PigStorage(',');

-- In kết quả ra màn hình
DUMP aspect_count_sorted;