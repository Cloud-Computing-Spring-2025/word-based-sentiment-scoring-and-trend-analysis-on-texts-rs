package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

public class PreprocessingReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder aggregatedText = new StringBuilder();
        for (Text value : values) {
            aggregatedText.append(value.toString()).append(" ");
        }
        context.write(key, new Text(aggregatedText.toString().trim()));
    }
}