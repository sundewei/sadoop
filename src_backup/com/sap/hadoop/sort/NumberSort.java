package com.sap.hadoop.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: Frank
 * Date: 2011/1/20
 * Time: ¤U¤È 09:32:14
 * To change this template use File | Settings | File Templates.
 */
public class NumberSort {
    public static class SortMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private final static IntWritable numberToAdd = new IntWritable(1);
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] strNumbers = line.split(",");
            for (String strNumber : strNumbers) {
                int number = Integer.parseInt(strNumber);
                numberToAdd.set(number);
                context.write(numberToAdd, one);
            }
        }
    }

    public static class SortReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, key);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "sort1");
        job.setJarByClass(NumberSort.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(NumberSort.SortMap.class);
        job.setReducerClass(NumberSort.SortReducer.class);
        job.setCombinerClass(NumberSort.SortReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("home/sort/source2/"));
        FileOutputFormat.setOutputPath(job, new Path("home/sort/output2/"));

        job.waitForCompletion(true);
    }
}
