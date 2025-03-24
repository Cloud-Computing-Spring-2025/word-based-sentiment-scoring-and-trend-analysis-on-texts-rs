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



