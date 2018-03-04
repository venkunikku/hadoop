package com.home.hadoop.test;

import com.home.hadoop.MaxTemperatureMapper;
import com.home.hadoop.MaxTemperatureMapper2;
import com.home.hadoop.MaxTemperatureReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;


public class MaxTemperatureTest {

    @Test
    public void processValidRecords() throws IOException, InterruptedException{
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                "99999V0203201N00261220001CN9999999N9-00111+99999999999");
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("1950"),new IntWritable(-11))
                .runTest();

    }

    //@Test
    public void ignoreMissingValueRecords() throws IOException, InterruptedException{
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .runTest();
    }

    @Test
    public void processValidRecordsForMaxTempMap2() throws IOException, InterruptedException{
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                "99999V0203201N00261220001CN9999999N9-00111+99999999999");
        new MapDriver<LongWritable,Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper2())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("1950"), new IntWritable(-11))
                .runTest();
    }

    @Test
    public void returnsMaximumIntegerValues() throws IOException, InterruptedException{

        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
                .withOutput(new Text("1950"), new IntWritable(10))
                .runTest();
    }
}
