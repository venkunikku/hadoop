package com.home.hadoop.mp2.orphanpagesTitles.orphanPages.topPopularLinks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.Integer;
import java.util.*;

public class TopPopularLinks extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("./tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Top Popular Links");

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setJarByClass(TopPopularLinks.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.waitForCompletion(true);


        Job jobB = Job.getInstance(this.getConf(), " Top Popular Links Job B");
        fs.delete(new Path(args[1]), true);

        jobB.setMapperClass(TopLinksMap.class);
        jobB.setReducerClass(TopLinksReduce.class);

        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setNumReduceTasks(1);

// this missing created issues for me
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));


        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer token = new StringTokenizer(value.toString(), " :");

            while (token.hasMoreElements()) {
                String tokenValue = token.nextToken();
                context.write(new IntWritable(Integer.parseInt(tokenValue)), new IntWritable(1));
            }
        }

    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable each : values) {
                count += each.get();
            }
            context.write(key, new IntWritable(count));

        }
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        private TreeSet<Pair<Integer, Integer>> countToPagesMap = new TreeSet<Pair<Integer, Integer>>();
        int outputRows;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.outputRows = conf.getInt("outputRows", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer pageId = Integer.parseInt(key.toString());
            Integer pageOccuranceCount = Integer.parseInt(value.toString());

            countToPagesMap.add(new Pair<Integer, Integer>(pageOccuranceCount, pageId));

            if (countToPagesMap.size() > outputRows) {
                countToPagesMap.remove(countToPagesMap.first());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, Integer> each : countToPagesMap) {

                Integer[] intArray = {each.second, each.first,};
                IntArrayWritable intArrayWrite = new IntArrayWritable(intArray);
                context.write(NullWritable.get(), intArrayWrite);
            }
        }

    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        private TreeSet<Pair<Integer, Integer>> countToWordMap = new TreeSet<Pair<Integer, Integer>>();
        int outputRows;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.outputRows = conf.getInt("outputRows", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) {
            for (IntArrayWritable each : values) {
                IntWritable[] allValues = (IntWritable[]) each.toArray();
                Integer pageId = allValues[0].get();
                Integer counts = allValues[1].get();

                countToWordMap.add(new Pair<Integer, Integer>(counts, pageId));

                if (countToWordMap.size() > outputRows) {
                    countToWordMap.remove(countToWordMap.first());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Pair<Integer, Integer>> li = new ArrayList<Pair<Integer, Integer>>();
            for (Pair<Integer, Integer> each : countToWordMap) {
                li.add(each);
            }

            Collections.sort(li, new Comparator<Pair<Integer, Integer>>() {
                public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
                    int numberSort = (o1.first).compareTo(o2.first);
                    return numberSort == 0 ? (o2.second).compareTo(o1.second) : numberSort;
                }
            });

            for (Pair<Integer, Integer> each : li) {
                IntWritable pageId = new IntWritable(each.second);
                IntWritable pageIdOccuranceCounts = new IntWritable(each.first);
                context.write(pageId, pageIdOccuranceCounts);
            }
        }

    }
}


class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
