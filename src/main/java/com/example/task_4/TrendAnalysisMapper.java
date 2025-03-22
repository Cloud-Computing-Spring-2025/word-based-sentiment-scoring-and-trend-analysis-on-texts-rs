package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class TrendAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable scoreWritable = new IntWritable();
    private Text compositeKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input by comma
        String[] parts = value.toString().split(",", -1);

        // Skip rows that do not have at least 3 parts (book_id, title, year)
        if (parts.length < 3) return;

        // Extract book_id and year
        String bookID = parts[0].trim();
        String year = parts[parts.length - 1].trim();

        // Skip rows with invalid year format
        if (year.isEmpty() || !year.matches("\\d+")) return;

        // Calculate the decade from the year (e.g., 1823 → 1820s)
        int yearInt = Integer.parseInt(year);
        int decadeStart = (yearInt / 10) * 10;
        String decade = decadeStart + "s";

        // Create composite key: "bookID, decade"
        compositeKey.set(bookID + "\t" + decade);

        // Compute the sentiment score (or use precomputed sentiment score)
        int sentimentScore = calculateSentimentScore(parts[1]); // Example: assume score in title is 2nd column
        scoreWritable.set(sentimentScore);

        // Emit the composite key (bookID, decade) and the sentiment score
        context.write(compositeKey, scoreWritable);

        // Alternatively, for overall decade trends:
        // compositeKey.set(decade);
        // context.write(compositeKey, scoreWritable);
    }

    // Helper method to calculate sentiment score for the title (simplified version)
    private int calculateSentimentScore(String title) {
        // Implement sentiment calculation logic here
        return title.length() % 5;  // Dummy example: length-based sentiment score
    }
}
