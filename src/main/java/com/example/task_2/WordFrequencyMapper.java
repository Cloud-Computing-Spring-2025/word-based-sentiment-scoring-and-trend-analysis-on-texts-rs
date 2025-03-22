package com.example;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.StringTokenizer;
import edu.stanford.nlp.simple.Sentence; // Import Stanford NLP Library

public class WordFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private Text wordText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split input line (bookID, title, year)
        String[] parts = value.toString().split(",", 3);
        if (parts.length < 3) return;  // Skip malformed lines

        String bookID = parts[0].trim();
        String title = parts[1].trim();
        String year = parts[2].trim();

        // Tokenize and lemmatize title words
        Sentence sentence = new Sentence(title);
        for (String lemma : sentence.lemmas()) {
            wordText.set(bookID + "_" + lemma.toLowerCase() + "_" + year);
            context.write(wordText, ONE);
        }
    }
}
