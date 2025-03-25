# multi-stage-sentiment-analysis-Mapreduce_hive
This is Multi-Stage Sentiment Analysis on Historical Literature application is developed with map reduce and hive
Here’s your cleaned-up and properly formatted `README.md` (Markdown) for GitHub, including fenced code blocks for commands, headers, and organized formatting:


##  Task 1: Preprocessing MapReduce Job



### Objective

The objective of Task 1 is to **clean and standardize raw text data** from multiple historical books while retaining only the essential metadata—specifically:
```
- `book_id`
- `title`
- `year`
```
This preprocessed output will be used for downstream NLP tasks such as sentiment scoring and trend analysis.



##  Dataset Overview

We used English books sourced from [Project Gutenberg](https://www.gutenberg.org/) using the **Gutendex API**.  
The dataset comprises **public domain books** published between the 1700s and 1800s.

Each book has the following metadata:

- `book_id`: The Project Gutenberg ID of the book  
- `title`: Title of the book  
- `author`: Author of the book  
- `year`: Birth year of the author (used as a proxy for publication year)

**Books and metadata saved as:**

- `gutenberg_books/` — Folder with raw `.txt` books  
- `gutenberg_metadata.csv` — Metadata CSV file

---

##  Implementation Notes

- **Scope**: Implemented using Hadoop MapReduce (Java)
- **Initial Preprocessing**: Done in Python before MapReduce
- **File Formats**:
  - Raw Books → `.txt` files inside `gutenberg_books/`
  - Preprocessed Text → `cleaned_books.tsv`
  - Metadata → `gutenberg_metadata.csv`
  - Final Output → `final_cleaned_books.csv`



##  Implementation Steps

###  1. Python Preprocessing (`formatBooks.py`)

- Extracts `book_id` from filenames like `book_1342.txt` → `1342`
- Lowercases text  
- Removes punctuation with regex  
- Removes a basic set of stopwords  
- Outputs TSV format:  
  ```
  book_id<TAB>cleaned_text
  ```

>  Output saved as: `cleaned_books.tsv`

---

###  2. Upload to HDFS

```bash
hdfs dfs -mkdir /input
hdfs dfs -put cleaned_books.tsv /input
hdfs dfs -put gutenberg_metadata.csv /input
```

---

###  3. Java MapReduce

#### Mapper: `PreprocessingMapper.java`
- Input: each line of `cleaned_books.tsv`
- Processing:
  - Split by tab
  - Lowercase, strip punctuation, remove stopwords
- Output:  
  ```
  (bookID, cleaned_text)
  ```

#### Reducer: `PreprocessingReducer.java`
- Aggregates all `cleaned_text` by `bookID`
- Output:  
  ```
  (bookID, final_cleaned_text)
  ```

#### Driver: `PreprocessingDriver.java`
- Runs the job:
  ```bash
  hadoop jar target/sentiment-scoring-trend-analysis-0.0.1-SNAPSHOT.jar com.example.PreprocessingDriver /input /output
  ```



###  4. Merge Output with Metadata

```bash
hdfs dfs -get /output/part-r-00000 output_local.txt
python merge_metadata.py
```

This script merges:

- `output_local.txt` (from HDFS)
- `gutenberg_metadata.csv`

→ Saves final file:
```
final_cleaned_books.csv
```

Columns:

```csv
book_id,title,year,cleaned_text
```

---

##  Final Output Example

```csv
book_id,title,year,cleaned_text
1342,Pride and Prejudice,1775,"pride prejudice jane elizabeth darcy ..."
2701,Moby Dick,1819,"call ishmael years ago never mind ..."
```

---

##  Challenges Encountered

- Some books had missing or malformed metadata
- Some download links returned unusable data
- HDFS output required post-processing to match with local metadata

---

##  Insights

- Java MapReduce is powerful but verbose for preprocessing
- Python is more effective for initial data wrangling
- Combining Hadoop + Python offers a scalable and flexible pipeline

---

##  Contributors

- `@ramisha99`
  



## How to Run (Quick Start)

```bash
# 1. Install requirements
pip install -r requirements.txt

# 2. Download and clean data
python get_books.py
python formatBooks.py

# 3. Upload files to HDFS
hdfs dfs -mkdir /input
hdfs dfs -put cleaned_books.tsv /input
hdfs dfs -put gutenberg_metadata.csv /input

# 4. Build and run MapReduce job
mvn clean package
hadoop jar target/sentiment-scoring-trend-analysis-0.0.1-SNAPSHOT.jar com.example.PreprocessingDriver /input /output

# 5. Download result & merge
hdfs dfs -get /output/part-r-00000 output_local.txt
python merge_metadata.py
```
# Task 2: Word Frequency Analysis with Lemmatization (MapReduce Job)

## Objective
The objective of Task 2 is to compute the word frequency of lemmatized words for each book while preserving the publication year. Lemmatization ensures different word forms (e.g., "running" → "run") are treated as the same word to improve accuracy in trend analysis.

## Dataset Overview
The dataset for Task 2: Word Frequency Analysis with Lemmatization is derived from the cleaned book texts in `final_cleaned_books.csv`, processed in Task 1. It contains:
- `book_id` – Unique identifier for each book
- `title` – Book title
- `year` – Year of publication
- `cleaned_text` – Preprocessed text  

This task extracts lemmatized word frequencies while retaining metadata for trend and sentiment analysis.

## Implementation Notes
- **Scope:** Implemented using Hadoop MapReduce (Java) with Stanford NLP for lemmatization.
- **Preprocessing Method:** Utilized Stanford CoreNLP for tokenization, part-of-speech tagging, and lemmatization.
- **File Formats:**  
  - **Input:** `final_cleaned_books.csv` (book metadata + cleaned text)  
  - **Output:** Lemmatized word counts.

## Implementation Steps

### 1. Upload Input Data to HDFS
```sh
hadoop fs -mkdir -p /input/task2
hadoop fs -put ./task_1_output/ /input/task2
```

### 2. Run the MapReduce Job
#### Mapper: `WordFrequencyMapper.java`
- Reads each line from `final_cleaned_books.csv`
- Extracts `book_id`, `year`, and `cleaned_text`
- Uses Stanford CoreNLP for tokenization and lemmatization
- Emits: `<book_id, lemma, year> → 1` (word frequency count per book)

#### Reducer: `WordFrequencyReducer.java`
- Aggregates word counts per `book_id`, `lemma`, and `year`
- Emits: `book_id, lemma, year frequency`

#### Driver: `WordFrequencyDriver.java`
- Sets up the MapReduce job and specifies input/output paths

Run the MapReduce job with:
```sh
hadoop jar sentiment-scoring-trend-analysis-0.0.1-SNAPSHOT.jar com.example.task_2.WordFrequencyDriver /task_1_output/ /output_2
```

### 3. Retrieve the Output
```sh
hadoop fs -cat /output_2/*
```

### Final Output Format
Each line in `/output_2` contains:
```
book_id,lemma,year frequency
```
#### Sample Output:
```
1080,90,1667	2
1080,American,1667	2
1080,English,1667	1
1080,French,1667	1
1080,I,1667	60
1080,Protestant,1667	1
```

## Challenges Encountered
- Processing large text datasets efficiently with Stanford NLP in Java MapReduce.
- Handling punctuation and non-word tokens during lemmatization.
- Ensuring HDFS-friendly output formats for future tasks.

## Insights
- Lemmatization helps standardize words, improving accuracy in trend analysis.
- Efficient text processing ensures better scalability for large corpora.
- Structured output makes downstream tasks (e.g., sentiment analysis) easier.

## Contributors
**Bhanu Saisree Thanniru**

---

## How to Run (Quick Start)

### Setup and Execution

#### 1. Start the Hadoop Cluster
```sh
docker compose up -d
```

#### 2. Build the Code
```sh
mvn install
```

#### 3. Move JAR File to Shared Folder
```sh
mv target/sentiment-scoring-trend-analysis-0.0.1-SNAPSHOT.jar jar_file/task_2
```

#### 4. Copy JAR to Docker Container
```sh
docker cp /workspaces/word-based-sentiment-scoring-and-trend-analysis-on-texts-rs/jar_file/task_2/sentiment-scoring-trend-analysis-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

#### 5. Move Dataset to Docker Container
```sh
docker cp /workspaces/word-based-sentiment-scoring-and-trend-analysis-on-texts-rs/outputs/task_1_output/ resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

#### 6. Connect to Docker Container
```sh
docker exec -it resourcemanager /bin/bash
cd /opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

#### 7. Set Up HDFS
```sh
hadoop fs -mkdir -p /input/task2
hadoop fs -put ./task_1_output/ /input/task2
```

#### 8. Execute the MapReduce Job
```sh
hadoop jar sentiment-scoring-trend-analysis-0.0.1-SNAPSHOT.jar com.example.task_2.WordFrequencyDriver /input/task2/task_1_output /output_2
```

#### 9. View the Output
```sh
hadoop fs -cat /output_2/*
```

#### 10. Copy Output from HDFS to Local OS
To copy the output from HDFS to your local machine:
```sh
# Copy from HDFS to container filesystem
hdfs dfs -get /output_2 /opt/hadoop-2.7.4/share/hadoop/mapreduce/

# Exit the container
exit  

# Copy from Docker container to local machine
docker cp resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/output_2/ outputs/task_2_output
```


# Task 3: Sentiment Scoring and Trend Analysis MapReduce Job

## Objective
The objective of Task 3 is to perform sentiment analysis on historical texts by leveraging word-based sentiment scoring. The output will be a dataset containing the sentiment score for each book, which can be used for further trend analysis.

## Dataset Overview
The input dataset is the output of Task 2, which contains each lemma along with its frequency, associated book ID, and year. This dataset is referred to as `output2` and serves as the input for Task 3.

### Input File: `output2`
Each line in `output2` follows the format:

```plaintext
book_id,year\tword_frequency
```

#### Example:
```plaintext
1080,1667\t112
11,1832\t120
1259,1802\t1986
```

## Implementation Notes
- **Scope:** Implemented using Hadoop MapReduce (Java)
- **Sentiment Scoring:** AFINN lexicon is used for word-based sentiment analysis
- **Preprocessing:** The `AFINN-111.txt` lexicon is copied into the container using `docker cp` and used in the Mapper

## Implementation Steps

### 1. Copy the AFINN Lexicon to the Hadoop Container
```sh
docker-compose up -d
docker cp AFINN-111.txt resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/AFINN-111.txt
```

### 2. Upload Input Data to HDFS
```sh
docker exec -it resourcemanager /bin/bash
hadoop fs -mkdir -p /input/task3
hadoop fs -put ./output2 /input/task3
```

### 3. Run the MapReduce Job

#### Mapper: `SentimentMapper.java`
- Reads each line from `output2`
- Extracts `book_id`, `year`, and `word_frequency`
- Looks up each word in `AFINN-111.txt` for sentiment score
- Computes sentiment contribution per word
- Emits: `<book_id,year sentiment_score>`

#### Reducer: `SentimentReducer.java`
- Aggregates sentiment scores per book
- Emits: `<book_id, year, total_sentiment_score>`

#### Driver: `SentimentDriver.java`
- Sets up the MapReduce job and specifies input/output paths

Run the MapReduce job with:
```sh
hadoop jar target/sentiment-scoring-trend-analysis-0.0.1-SNAPSHOT.jar com.example.SentimentDriver /input/task3 /output_3
```

### 4. Retrieve the Output
```sh
hadoop fs -cat /output_3/*
```

## Final Output Format
Each line in `/output_3` contains:

```plaintext
book_id,year\tsentiment_score
```

### Sample Example:
```plaintext
1080,1667\t112
11,1832\t120
1259,1802\t1986
1260,1816\t1516
1342,1775\t4045
```

## Challenges Encountered
- Handling words not present in `AFINN-111.txt` required assigning a neutral score (0)
- Some words had ambiguous sentiment scores, requiring careful lookup handling
- Large-scale processing using Java MapReduce was efficient but required memory optimization for lexicon lookup

## Insights
- Combining word frequency with sentiment scoring allows historical trend analysis
- Sentiment variations across different centuries can indicate cultural and linguistic shifts
- Efficient file handling and format consistency ensure seamless integration with future tasks

## Contributors
- **Navya Vejalla**

## How to Run (Quick Start)
```bash
# 1. Copy the AFINN lexicon to Hadoop container
docker cp AFINN-111.txt resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/AFINN-111.txt

# 2. Upload input files to HDFS
hadoop fs -mkdir -p /input/task3
hadoop fs -put ./output2 /input/task3

# 3. Build and run MapReduce
mvn clean package
hadoop jar target/sentiment-scoring-trend-analysis-0.0.1-SNAPSHOT.jar com.example.SentimentDriver /input/task3 /output_3

# 4. Export and view results
hadoop fs -cat /output_3/*
```

# task 4 step by step guide 


## Prerequisites
- Docker
- Maven
- Hadoop 2.7.4 (inside Docker container)

## Steps to Run the Analysis

### 1. Start Docker Containers
Run the following command to start the necessary containers:
```sh
docker compose up -d
```

### 2. Build the JAR File
Navigate to the project directory and use Maven to build the JAR file:
```sh
mvn install
```

### 3. Copy the JAR File to Hadoop
Copy the compiled JAR file to the Hadoop directory inside the Docker container:
```sh
docker cp target/sentiment-scoring-trend-analysis-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

### 4. Move Dataset Outputs to Hadoop
Copy the dataset outputs from previous tasks into the Hadoop directory:
```sh
docker cp outputs/task_2_output/output2 resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/
docker cp outputs/task3_output/output_3/output3 resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

### 5. Connect to Docker Container
Access the Hadoop resourcemanager container:
```sh
docker exec -it resourcemanager /bin/bash
```

### 6. Navigate to Hadoop Directory
```sh
cd /opt/hadoop-2.7.4/share/hadoop/mapreduce/
```

### 7. Resolve SLF4J Dependency Issues (If Any)
If you encounter multiple SLF4J bindings, locate and remove duplicate files:
```sh
find /opt/hadoop-2.7.4/share/hadoop -name "slf4j*.jar"
```

### 8. Prepare Input Directory in HDFS
Create the input directory in Hadoop File System:
```sh
hadoop fs -mkdir -p /input/dataset
```
Copy the dataset files into the input directory:
```sh
hadoop fs -put ./output2 /input4
hadoop fs -put ./output3 /input4
```

### 9. Execute Hadoop Job
Run the Hadoop MapReduce job with the input data:
```sh
hadoop jar /opt/hadoop-2.7.4/share/hadoop/mapreduce/sentiment-scoring-trend-analysis-0.0.1-SNAPSHOT.jar com.hadoop.analysis.TrendAnalysisDriver /input4 /t4output
```

### 10. View the Output
Check the results in HDFS:
```sh
hadoop fs -cat /t4output/*
```
Retrieve the output files to the local Hadoop directory:
```sh
hdfs dfs -get /t4output /opt/hadoop-2.7.4/share/hadoop/mapreduce/
exit
```

### 11. Copy Output Files to Local Machine
Copy the output files from the container to the local workspace:
```sh
docker cp resourcemanager:/opt/hadoop-2.7.4/share/hadoop/mapreduce/t4output/ outputs/task4_output
```



# Task 5: Bigram Analysis Using Hive UDF

## Objective
Extract and analyze bigrams from lemmatized text using a custom Hive UDF. The UDF takes an array of "lemma:freq" strings and returns a map of bigrams to their co-occurrence frequencies.

## Implementation Steps

### 1. Copy Output Files to Local Machine
Copy the output files from the container to the local workspace:
```sh
docker cp target/sentiment-scoring-trend-analysis-0.0.1-SNAPSHOT.jar hive-server:/opt/jar_file/bigram-udf.jar
```
### 2.  Copy Hive Script  (on mac I had to individually query hive -- see below)
Copy the output files from the container to the local workspace:
```sh
docker cp hive_scripts/task5_bigrams.hql hive-server:/opt/task5_bigrams.hql
```

### 3. Open the Hive Container
Copy the output files from the container to the local workspace:
```sh
docker exec -it hive-server /bin/bash
```

### 4. Run Hive 
Copy the output files from the container to the local workspace:
```sh
hive -f /opt/task5_bigrams.hql
```

### 1. Create an External Table for Lemmatized Data
```sql
DROP TABLE IF EXISTS lemma_freqs_raw;
CREATE EXTERNAL TABLE lemma_freqs_raw (
  raw_line STRING
)
LOCATION 'hdfs://namenode:8020/outputs/task_2_output';
```



### 2. Create a Parsed Table for Lemma Data
```sql
DROP TABLE IF EXISTS lemma_freqs;
CREATE TABLE lemma_freqs AS
SELECT
  CAST(SPLIT(raw_line, ',')[0] AS INT) AS book_id,
  SPLIT(raw_line, ',')[1] AS lemma,
  CAST(SPLIT(SPLIT(raw_line, ',')[2], '\\t')[0] AS INT) AS year,
  CAST(SPLIT(SPLIT(raw_line, ',')[2], '\\t')[1] AS INT) AS freq
FROM lemma_freqs_raw;
```

### 3. Register the Bigram Hive UDF
- Make sure the compiled JAR is located in /opt/jar_file/.
```sql
ADD JAR /opt/jar_file/bigram-udf.jar;
CREATE TEMPORARY FUNCTION pseudo_bigrams AS 'com.example.task_5.BigramUDF';
```
### 4. Group Lemmas by Book and Year
```sql
DROP TABLE IF EXISTS lemma_grouped;
CREATE TABLE lemma_grouped AS
SELECT
  book_id,
  year,
  COLLECT_LIST(CONCAT_WS(':', lemma, CAST(freq AS STRING))) AS lemma_freq_arr,
  COUNT(*) AS cnt
FROM lemma_freqs
GROUP BY book_id, year;
```
### 5. Create an External Table for Lemmatized Data
```sql
DROP TABLE IF EXISTS lemma_grouped_filtered;
CREATE TABLE lemma_grouped_filtered AS
SELECT * FROM lemma_grouped WHERE cnt > 1;
```

### 6. Create an External Table for Lemmatized Data
```sql
DROP TABLE IF EXISTS bigram_output;
CREATE TABLE bigram_output AS
SELECT
  lgf.book_id,
  lgf.year,
  ex.key AS bigram,
  ex.value AS co_freq
FROM lemma_grouped_filtered lgf
LATERAL VIEW explode(pseudo_bigrams(lgf.lemma_freq_arr)) ex AS key, value;
```


### 7. Export Bigram Results
```sql
INSERT OVERWRITE DIRECTORY 'hdfs://namenode:8020/outputs/task5_output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT book_id, year, bigram, co_freq
FROM bigram_output;
```

### Output Example 

```
book_id | year | bigram           | co_freq
--------|------|------------------|--------
1342    | 1775 | pride prejudice  | 6
1342    | 1775 | elizabeth darcy  | 9
...
```
