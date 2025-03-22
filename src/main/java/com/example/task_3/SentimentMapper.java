package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.*;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Map<String, Integer> sentimentLexicon = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        // Load sentiment lexicon (Assuming AFINN format: "word\tscore")
        try (BufferedReader reader = new BufferedReader(new FileReader("/AFINN-111.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    sentimentLexicon.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Expected input: bookID \t year \t processed text
        String[] parts = value.toString().split("\t");
        if (parts.length < 3) return;

        String bookID = parts[0];
        String year = parts[1];
        String[] words = parts[2].split(" ");

        int totalScore = 0;
        for (String word : words) {
            totalScore += sentimentLexicon.getOrDefault(word, 0);  // Get sentiment score if available
        }

        context.write(new Text(bookID + "\t" + year), new IntWritable(totalScore));
    }
}