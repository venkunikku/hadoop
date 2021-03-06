package com.home.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private TemperatureInputParser parser = new TemperatureInputParser();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        parser.parse(value);
        if (parser.isValidTemperature()) {
            context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
        }
    }


}
