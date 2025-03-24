package com.hadoop.analysis;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TrendAnalysisMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] parts = value.toString().split("\t");
        if (parts.length != 2) return; 
        
        String[] fields = parts[0].split(",");
       

        if (fields.length == 3) {  // Lemma frequency dataset: bookID, frequency, lemma, year
            String bookID = fields[0];;
          
           
         
            String decade = getDecade(fields[2]);


            String wordFrequency = "word:" + parts[1];

            context.write(new Text(decade + "," + bookID), new Text(wordFrequency));  // Book-level
            context.write(new Text(decade + ",ALL"), new Text(wordFrequency));        // Overall
        } else if (fields.length == 2) {  // Sentiment dataset: bookID, year, sentiment
            String bookID = fields[0];
                 
     
           

            String decade = getDecade(fields[0]);
            String sentimentScore = "sentiment:" + parts[1];

            context.write(new Text(decade + "," + bookID), new Text(sentimentScore));  // Book-level
            context.write(new Text(decade + ",ALL"), new Text(sentimentScore));        // Overall
        }
    }

    private String getDecade(String year) {
        int yearInt = Integer.parseInt(year);
        return (yearInt / 10) * 10 + "s";  // Convert to decade format
    }
}