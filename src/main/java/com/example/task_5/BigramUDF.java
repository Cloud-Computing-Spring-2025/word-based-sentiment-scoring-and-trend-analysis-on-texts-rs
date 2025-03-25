package com.example.task_5;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class BigramUDF extends UDF {
    public MapWritable evaluate(List<String> lemmaFreqList) {
        MapWritable result = new MapWritable();
        //loop over the list of strings (each of the format "lemma:freq")
        for (int i = 0; i < lemmaFreqList.size(); i++) {
            String item1 = lemmaFreqList.get(i);
            String[] parts1 = item1.split(":");
            if (parts1.length != 2) continue;
            String lemma1 = parts1[0].trim();
            int freq1;
            try {
                freq1 = Integer.parseInt(parts1[1].trim());
            } catch (NumberFormatException e) {
                continue;
            }
            for (int j = i + 1; j < lemmaFreqList.size(); j++) {
                String item2 = lemmaFreqList.get(j);
                String[] parts2 = item2.split(":");
                if (parts2.length != 2) continue;
                String lemma2 = parts2[0].trim();
                int freq2;
                try {
                    freq2 = Integer.parseInt(parts2[1].trim());
                } catch (NumberFormatException e) {
                    continue;
                }
                String bigram = lemma1 + " " + lemma2;
                IntWritable coFreq = new IntWritable(freq1 * freq2);
                Text bigramKey = new Text(bigram);
                if (result.containsKey(bigramKey)) {
                    IntWritable existing = (IntWritable) result.get(bigramKey);
                    coFreq.set(existing.get() + coFreq.get());
                }
                result.put(bigramKey, coFreq);
            }
        }
        return result;
    }
}
    