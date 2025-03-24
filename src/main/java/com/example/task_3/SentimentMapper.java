// package com.example.task_3;

// import org.apache.hadoop.io.*;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;

// import java.io.*;
// import java.util.*;

// public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

//     private Map<String, Integer> sentimentLexicon = new HashMap<>();
//     private final static IntWritable scoreWritable = new IntWritable();
//     private Text compositeKey = new Text();

//     @Override
//     protected void setup(Context context) throws IOException {
//         FileSystem fs = FileSystem.get(context.getConfiguration());
//         Path path = new Path("/user/hadoop/input/AFINN-111.txt");

//         // Load sentiment lexicon (Assuming AFINN format: "word\tscore")
//         try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
//             String line;
//             while ((line = reader.readLine()) != null) {
//                 String[] parts = line.split("\t");
//                 if (parts.length == 2) {
//                     sentimentLexicon.put(parts[0].toLowerCase().trim(), Integer.parseInt(parts[1]));
//                 }
//             }
//         }
//     }

//     @Override
//     protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//         // Split input based on commas (allow for multiple commas in titles)
//         String[] parts = value.toString().split(",", -1);

//         // Skip rows that do not have at least 3 parts (book_id, title, year)
//         if (parts.length < 3) return;

//         // Extract book_id and year correctly
//         String bookID = parts[0].trim(); // Book ID
//         String year = parts[parts.length - 1].trim(); // Year is the last part of the data

//         // Skip rows that do not have valid year
//         if (year.isEmpty() || !year.matches("\\d+")) return;

//         // Handle title with commas and quotes properly by removing leading/trailing quotes
//         String title = String.join(",", Arrays.copyOfRange(parts, 1, parts.length - 1)).trim();

//         if (title.startsWith("\"") && title.endsWith("\"")) {
//             title = title.substring(1, title.length() - 1);
//         }

//         // Create composite key: "bookID, year"
//         compositeKey.set(bookID + "\t" + year);

//         // Process the title text: convert to lowercase, remove non-alphanumeric characters, and tokenize.
//         String text = title.toLowerCase().replaceAll("[^a-z0-9\\s]", " ");
//         String[] words = text.split("\\s+");

//         int totalScore = 0;
//         for (String word : words) {
//             if (word.isEmpty()) continue;
//             // Look up the sentiment score for the word
//             totalScore += sentimentLexicon.getOrDefault(word, 0);
//         }

//         // Emit the composite key (bookID, year) and the calculated sentiment score
//         scoreWritable.set(totalScore);
//         context.write(compositeKey, scoreWritable);
//     }
// }

package com.example.task_3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class SentimentMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Map<String, Integer> sentimentDictionary = new HashMap<>();
    private Text bookYearKey = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        // Load sentiment lexicon from distributed cache or local file
        BufferedReader reader = new BufferedReader(new FileReader("/opt/hadoop-2.7.4/share/hadoop/mapreduce/AFINN-111.txt"));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                sentimentDictionary.put(parts[0].toLowerCase(), Integer.parseInt(parts[1]));
            }
        }
        reader.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input: book_id,lemma,year  count
        String[] parts = value.toString().split("\t");
        if (parts.length < 2) return;

        String[] keyParts = parts[0].split(",");
        if (keyParts.length < 3) return;

        String bookId = keyParts[0];
        String lemma = keyParts[1];
        String year = keyParts[2];
        int count = Integer.parseInt(parts[1]);

        // Get sentiment score from dictionary
        int sentimentScore = sentimentDictionary.getOrDefault(lemma, 0);
        int totalSentiment = sentimentScore * count;

        bookYearKey.set(bookId + "," + year);
        context.write(bookYearKey, new IntWritable(totalSentiment));  // Emit (book_id, year) -> sentiment score
    }
}