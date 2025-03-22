
package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.*;

public class PreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "a", "an", "and", "the", "is", "in", "to", "of", "for", "on", "with", "as", "by", "at"));

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length < 2) return;  // Ensure correct format

        String bookID = parts[0];
        String text = parts[1].toLowerCase().replaceAll("[^a-z0-9 ]", "");

        StringBuilder cleanedText = new StringBuilder();
        for (String word : text.split(" ")) {
            if (!STOP_WORDS.contains(word)) {
                cleanedText.append(word).append(" ");
            }
        }

        context.write(new Text(bookID), new Text(cleanedText.toString().trim()));
    }
}