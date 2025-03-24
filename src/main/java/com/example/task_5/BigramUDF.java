package com.example.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class BigramUDF extends UDF {

    // Input: List of "lemma:freq" strings
    public MapWritable evaluate(List<Text> lemmaFreqList) {
        MapWritable result = new MapWritable();

        //parse lemma-freq pairs
        List<String> lemmas = new ArrayList<>();
        List<Integer> freqs = new ArrayList<>();

        for (Text item : lemmaFreqList) {
            String[] parts = item.toString().split(":");
            if (parts.length != 2) continue;

            String lemma = parts[0].trim();
            int freq;

            try {
                freq = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException e) {
                continue;
            }

            lemmas.add(lemma);
            freqs.add(freq);
        }

        // Generate bigrams (unordered and non-repeating)
        for (int i = 0; i < lemmas.size(); i++) {
            for (int j = i + 1; j < lemmas.size(); j++) {
                String bigram = lemmas.get(i) + " " + lemmas.get(j);
                int coFreq = freqs.get(i) * freqs.get(j);

                Text key = new Text(bigram);
                IntWritable value = new IntWritable(coFreq);

                if (result.containsKey(key)) {
                    IntWritable existing = (IntWritable) result.get(key);
                    existing.set(existing.get() + coFreq);
                } else {
                    result.put(key, value);
                }
            }
        }

        return result;
    }
}
