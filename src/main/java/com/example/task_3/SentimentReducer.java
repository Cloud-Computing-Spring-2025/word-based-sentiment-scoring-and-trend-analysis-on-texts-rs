package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalSentiment = 0;

        for (IntWritable value : values) {
            totalSentiment += value.get();
        }

        context.write(key, new IntWritable(totalSentiment));
    }
}