package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrendAnalysisDriver {

    public static void main(String[] args) throws Exception {
        // Check arguments
        if (args.length != 2) {
            System.err.println("Usage: TrendAnalysisDriver <input path> <output path>");
            System.exit(-1);
        }

        // Set up the job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sentiment Trend Analysis");

        // Set the input/output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the Mapper and Reducer classes
        job.setJarByClass(TrendAnalysisDriver.class);
        job.setMapperClass(TrendAnalysisMapper.class);
        job.setReducerClass(TrendAnalysisReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
