-- Tải dataset chính (giả sử file CSV có các cột: comment, topic, aspect, sentiment)
raw_data = LOAD '/hotel-review.csv' USING PigStorage(',') 
    AS (comment: chararray, topic: chararray, aspect: chararray, sentiment: chararray);

lowercased = FOREACH raw_data GENERATE 
    LOWER(comment) AS comment,
    topic          AS topic,
    aspect         AS aspect,
    sentiment      AS sentiment;

-- TOKENIZE tách câu thành bag of words theo khoảng trắng
tokenized = FOREACH lowercased GENERATE 
    FLATTEN(TOKENIZE(comment)) AS word,
    topic                      AS topic,
    aspect                     AS aspect,
    sentiment                  AS sentiment;

stopwords = LOAD '/stopwords.txt' USING PigStorage('\n') AS (word: chararray);


-- LEFT OUTER JOIN: giữ tất cả từ bên trái, khớp với stopwords bên phải
joined = JOIN tokenized BY word LEFT OUTER, stopwords BY word;

-- Giữ lại những từ KHÔNG nằm trong danh sách stop word
-- (những từ không khớp sẽ có stopwords::word = NULL)
filtered = FILTER joined BY stopwords::word IS NULL;

words = FOREACH filtered GENERATE 
    tokenized::word      AS word,
    tokenized::topic     AS topic,
    tokenized::aspect    AS aspect,
    tokenized::sentiment AS sentiment;


STORE words INTO '/output/result_b1' USING PigStorage(',');