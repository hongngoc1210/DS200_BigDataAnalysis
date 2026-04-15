-- Load data
raw_data = LOAD '/hotel-review.csv' USING PigStorage(';')
    AS (id: int, comment: chararray, topic: chararray, aspect: chararray, sentiment: chararray);

-- Bỏ header
data = FILTER raw_data BY id != 0 AND comment != 'comment';

-- Tách từ trong comment thành từng dòng riêng
tokenized = FOREACH data GENERATE
    topic     AS topic,
    sentiment AS sentiment,
    TOKENIZE(LOWER(comment)) AS words_bag;

-- Flatten để mỗi từ thành 1 dòng riêng -> tạo ra relation 'words'
words = FOREACH tokenized GENERATE
    topic      AS topic,
    sentiment  AS sentiment,
    FLATTEN(words_bag) AS word;

-- ===== POSITIVE =====
words_positive     = FILTER words BY sentiment == 'positive';
words_pos_grouped  = GROUP words_positive BY (topic, word);
words_pos_count    = FOREACH words_pos_grouped GENERATE
    FLATTEN(group)        AS (topic, word),
    COUNT(words_positive) AS freq;

words_pos_by_topic = GROUP words_pos_count BY topic;

top5_positive_per_topic = FOREACH words_pos_by_topic {
    sorted = ORDER words_pos_count BY freq DESC;  -- dùng tên bag nội bộ
    top5   = LIMIT sorted 5;
    GENERATE
        FLATTEN(top5);  -- flatten nested bag trước khi lưu
};

-- ===== NEGATIVE =====
words_negative     = FILTER words BY sentiment == 'negative';
words_neg_grouped  = GROUP words_negative BY (topic, word);
words_neg_count    = FOREACH words_neg_grouped GENERATE
    FLATTEN(group)        AS (topic, word),
    COUNT(words_negative) AS freq;

words_neg_by_topic = GROUP words_neg_count BY topic;

top5_negative_per_topic = FOREACH words_neg_by_topic {
    sorted = ORDER words_neg_count BY freq DESC;
    top5   = LIMIT sorted 5;
    GENERATE
        FLATTEN(top5);
};

-- ===== GỘP & LƯU 1 FILE =====
combined = UNION top5_positive_per_topic, top5_negative_per_topic;
STORE combined INTO '/output/results_b4' USING PigStorage(',');