package com.home.hadoop.wordcounts;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        int i = 0;
        for (IntWritable each : values) {
            ++i;
        }
        context.write(key, new LongWritable(i));
    }
}
