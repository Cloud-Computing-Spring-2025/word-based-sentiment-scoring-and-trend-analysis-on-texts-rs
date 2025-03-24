package com.hadoop.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrendAnalysisReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int totalWords = 0;
        int wordCount = 0;
        double totalSentiment = 0.0;
        int sentimentCount = 0;

        for (Text val : values) {
            String value = val.toString();
            if (value.startsWith("word:")) {
                totalWords += Integer.parseInt(value.split(":")[1]);
                wordCount++;
            } else if (value.startsWith("sentiment:")) {
                totalSentiment += Double.parseDouble(value.split(":")[1]);
                sentimentCount++;
            }
        }

        double avgSentiment = (sentimentCount > 0) ? (totalSentiment / sentimentCount) : 0.0;

        context.write(key, new Text("TotalWords=" + totalWords + ", AvgSentiment=" + avgSentiment));
    }
}
