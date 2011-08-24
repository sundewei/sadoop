package com.sap.hadoop.index;

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
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount2 {
    private static Pattern META_PATTERN =
            Pattern.compile("<meta[\\s]+[^>]*?name[\\s]?=[\\s\\\"\\']+(.*?)[\\s\\\"\\']+content[\\s]?=[\\s\\\"\\']+(.*?)[\\\"\\']+.*?>", Pattern.CASE_INSENSITIVE);

    private static final Pattern DELIMITER_PATTERN = Pattern.compile("[,]+");


    private static Set<String> getDistinctStringSet(String[] strings) {
        Set<String> set = new HashSet<String>();
        for (String keyword : strings) {
            if (!set.contains(keyword.trim())) {
                set.add(keyword.trim());
            }
        }
        return set;
    }

    private static String[] getKeywords(String line) {
        Matcher metaMatcher = META_PATTERN.matcher(line);
        while (metaMatcher.find()) {
            String name = metaMatcher.group(1);
            String content = metaMatcher.group(2);
            if (name.equalsIgnoreCase("keywords")) {
                return DELIMITER_PATTERN.split(content);
            }
        }
        return null;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] keywords = getKeywords(line);
            if (keywords != null) {
                Set<String> set = getDistinctStringSet(keywords);
                for (String keyword : set) {
                    word.set(keyword);
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount2");
        job.setJarByClass(WordCount2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCount2.Map.class);
        job.setReducerClass(WordCount2.IntSumReducer.class);
        job.setCombinerClass(WordCount2.IntSumReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("home/index/source/"));
        FileOutputFormat.setOutputPath(job, new Path("home/index/output2/"));

        job.waitForCompletion(true);
    }

}