package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrendAnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final static IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        // Sum the sentiment scores or word frequencies for each key (bookID, decade) or decade
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }

        // Calculate the average sentiment score or word frequency
        int average = sum / count;

        // Output the aggregated result
        result.set(average);
        context.write(key, result);
    }
}
