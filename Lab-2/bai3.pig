raw_data = LOAD '/hotel-review.csv' USING PigStorage(';') 
    AS (id: int, comment: chararray, topic: chararray, aspect: chararray, sentiment: chararray);

-- Bỏ dòng header (nếu có, kiểm tra tên cột đầu tiên thực tế trong file)
data_no_header = FILTER raw_data BY comment != 'comment';

-- Lấy các cột cần thiết
aspect_sentiment = FOREACH data_no_header GENERATE
    aspect    AS aspect,
    sentiment AS sentiment;

-- ===== XỬ LÝ NEGATIVE =====
negative_reviews = FILTER aspect_sentiment BY sentiment == 'negative';
negative_groups  = GROUP negative_reviews BY aspect;
negative_count   = FOREACH negative_groups GENERATE
    group                   AS aspect,
    COUNT(negative_reviews) AS neg_count;
negative_sorted  = ORDER negative_count BY neg_count DESC;
top_negative     = LIMIT negative_sorted 1;

top_negative_labeled = FOREACH top_negative GENERATE
    'MOST_NEGATIVE' AS label,
    aspect          AS aspect,
    neg_count       AS count;

-- ===== XỬ LÝ POSITIVE =====
positive_reviews = FILTER aspect_sentiment BY sentiment == 'positive';
positive_groups  = GROUP positive_reviews BY aspect;
positive_count   = FOREACH positive_groups GENERATE
    group                   AS aspect,
    COUNT(positive_reviews) AS pos_count;
positive_sorted  = ORDER positive_count BY pos_count DESC;
top_positive     = LIMIT positive_sorted 1;

top_positive_labeled = FOREACH top_positive GENERATE
    'MOST_POSITIVE' AS label,
    aspect          AS aspect,
    pos_count       AS count;

-- ===== GỘP & LƯU =====
combined_results = UNION top_negative_labeled, top_positive_labeled;

STORE combined_results INTO '/output/results_b3' USING PigStorage(',');