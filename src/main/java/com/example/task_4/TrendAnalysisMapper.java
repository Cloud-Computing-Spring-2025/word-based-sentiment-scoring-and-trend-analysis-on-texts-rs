package com.hadoop.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TrendAnalysisMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length != 2) return;

        String[] fields = parts[0].split(",");

        if (fields.length == 3) {  // Lemma frequency dataset: bookID, lemma, year
            String bookID = fields[0];
            String year = fields[2];

            String decade = getDecade(year);
            String wordFrequency = "word:" + parts[1];

            context.write(new Text(decade + "," + bookID), new Text(wordFrequency));
            context.write(new Text(decade + ",ALL"), new Text(wordFrequency));

        } else if (fields.length == 2) {  // Sentiment dataset: bookID, year
            String bookID = fields[0];
            String year = fields[1];

            String decade = getDecade(year);
            String sentimentScore = "sentiment:" + parts[1];

            context.write(new Text(decade + "," + bookID), new Text(sentimentScore));
            context.write(new Text(decade + ",ALL"), new Text(sentimentScore));
        }
    }

    private String getDecade(String yearStr) {
        try {
            int year = Integer.parseInt(yearStr);
            return (year / 10) * 10 + "s";
        } catch (NumberFormatException e) {
            return "Unknown";
        }
    }
}
