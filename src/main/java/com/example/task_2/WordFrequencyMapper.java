package com.example.task_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.stanford.nlp.pipeline.*;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class WordFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // Initialize the Stanford NLP pipeline
    private StanfordCoreNLP pipeline;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma");
        pipeline = new StanfordCoreNLP(props);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split CSV using regex to handle commas inside quotes
        String[] parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        // Ensure we have enough fields (book_id, title, year, cleaned_text)
        if (parts.length < 4) return;

        String bookId = parts[0].trim();
        String year = parts[2].trim();
        String cleanedText = parts[3].toLowerCase();  // Extract "cleaned_text"

        // Split into sentences using Stanford NLP
        CoreDocument document = new CoreDocument(cleanedText);
        pipeline.annotate(document);
        List<CoreSentence> sentences = document.sentences();

        for (CoreSentence sentence : sentences) {
            List<String> lemmas = sentence.lemmas();
            for (String lemma : lemmas) {
                // Check if the lemma is valid (avoid empty strings or punctuation)
                if (!lemma.isEmpty() && !isPunctuation(lemma)) {
                    word.set(bookId + "," + lemma + "," + year);
                    context.write(word, one);  // Emit (book_id, lemma, year) -> 1
                }
            }
        }
    }

    private boolean isPunctuation(String word) {
        return word.matches("[.,!?;:(){}\\[\\]]");
    }
}