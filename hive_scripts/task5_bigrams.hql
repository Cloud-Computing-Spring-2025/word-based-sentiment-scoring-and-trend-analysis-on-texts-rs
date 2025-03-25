-- Step 1: Remove any leftover table named 'lemma_freqs_raw' to avoid duplicates.
--         Create an external table that points to raw data (Task 2 output) in HDFS.
DROP TABLE IF EXISTS lemma_freqs_raw;
CREATE EXTERNAL TABLE lemma_freqs_raw (
  raw_line STRING
)
LOCATION 'hdfs://namenode:8020/outputs/task_2_output';

-- Step 2: Parse each line from 'lemma_freqs_raw' to populate a managed table called 'lemma_freqs'.
--         The line is split by commas and tabs to extract book_id, lemma, year, and freq.
DROP TABLE IF EXISTS lemma_freqs;
CREATE TABLE lemma_freqs AS
SELECT
  CAST(SPLIT(raw_line, ',')[0] AS INT) AS book_id,
  SPLIT(raw_line, ',')[1] AS lemma,
  CAST(SPLIT(SPLIT(raw_line, ',')[2], '\\t')[0] AS INT) AS year,
  CAST(SPLIT(SPLIT(raw_line, ',')[2], '\\t')[1] AS INT) AS freq
FROM lemma_freqs_raw;

-- Step 3: Add the custom UDF JAR (already copied to /opt/jar_file/bigram-udf.jar)
--         and register the function as 'pseudo_bigrams'.
ADD JAR /opt/jar_file/bigram-udf.jar;
CREATE TEMPORARY FUNCTION pseudo_bigrams AS 'com.example.task_5.BigramUDF';

-- Step 4: Clean up any existing 'bigram_output' table.
DROP TABLE IF EXISTS bigram_output;

-- Step 5: Create the final bigram output table.
--         First group lemmas by book_id and year, collect them into an array,
--         then explode the UDF's result into rows with bigrams and co_freq.
CREATE TABLE bigram_output AS
WITH lemma_grouped AS (
  SELECT
    book_id,
    year,
    COLLECT_LIST(CONCAT_WS(':', lemma, CAST(freq AS STRING))) AS lemma_freq_arr
  FROM lemma_freqs
  GROUP BY book_id, year
)
SELECT
  lg.book_id,
  lg.year,
  ex.key   AS bigram,
  ex.value AS co_freq
FROM lemma_grouped lg
LATERAL VIEW explode(pseudo_bigrams(lg.lemma_freq_arr)) ex AS key, value;

-- Step 6: Export the results to HDFS if needed.
--         This overwrites data in the specified directory.
INSERT OVERWRITE DIRECTORY 'hdfs://namenode:8020/outputs/task5_output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT book_id, year, bigram, co_freq
FROM bigram_output;
