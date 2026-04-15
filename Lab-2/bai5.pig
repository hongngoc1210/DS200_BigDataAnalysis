-- Load data
raw_data = LOAD '/hotel-review.csv' USING PigStorage(';')
    AS (id: int, comment: chararray, topic: chararray, aspect: chararray, sentiment: chararray);

-- Bỏ header
data = FILTER raw_data BY comment != 'comment';

-- Tách từ
tokenized = FOREACH data GENERATE
    topic AS topic,
    TOKENIZE(LOWER(comment)) AS words_bag;

-- Flatten mỗi từ thành 1 dòng
words = FOREACH tokenized GENERATE
    topic AS topic,
    FLATTEN(words_bag) AS word;

-- Nhóm theo (topic, word) và đếm tần số
all_words_grouped = GROUP words BY (topic, word);
all_words_count   = FOREACH all_words_grouped GENERATE
    FLATTEN(group) AS (topic, word),
    COUNT(words)   AS freq;

-- Nhóm theo topic để lấy TOP 5
all_words_by_topic = GROUP all_words_count BY topic;

top5_words_per_topic = FOREACH all_words_by_topic {
    sorted = ORDER all_words_count BY freq DESC;
    top5   = LIMIT sorted 5;
    GENERATE FLATTEN(top5);  -- flatten nested bag
};

-- Lưu 1 file duy nhất
STORE top5_words_per_topic INTO '/output/results_b5' USING PigStorage(',');