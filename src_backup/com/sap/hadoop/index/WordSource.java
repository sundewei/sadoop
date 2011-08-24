package com.sap.hadoop.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordSource {

    private static Pattern META_PATTERN =
            Pattern.compile("<meta[\\s]+[^>]*?name[\\s]?=[\\s\\\"\\']+(.*?)[\\s\\\"\\']+content[\\s]?=[\\s\\\"\\']+(.*?)[\\\"\\']+.*?>", Pattern.CASE_INSENSITIVE);

    private static final Pattern DELIMITER_PATTERN = Pattern.compile("[,]+");

    private static final String BASE_INPUT_DIRECTORY = "home/index/source3/";
    private static final String BASE_OUTPUT_DIRECTORY = "home/index/output3/";

    private static Set<String> getDistinct(String[] strings) {
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

    public static class IndexMapper extends Mapper<Text, Text, Text, Text> {

        private Text metaWordText = new Text();

        public void map(Text filename, Text fileContent, Context context) throws IOException, InterruptedException {
            String fileContentString = fileContent.toString();
            String[] metaWordArray = getKeywords(fileContentString);
            if (metaWordArray != null) {
                Set<String> metaWordSet = getDistinct(metaWordArray);
                for (String metaWord : metaWordSet) {
                    metaWordText.set(metaWord);
                    // Each meta word will result a key-value pair
                    context.write(metaWordText, new Text(filename));
                }
            }
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text metaWordText, Iterable<Text> filenames, Context context)
                throws IOException, InterruptedException {
            StringBuilder filenameStringBuilder = new StringBuilder();
            for (Text text : filenames) {
                filenameStringBuilder.append(text.toString()).append(",");
            }
            context.write(metaWordText, new Text(filenameStringBuilder.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordsource");
        job.setJarByClass(WordSource.class);

        // Delete the output path
        Path outputPath = new Path(BASE_OUTPUT_DIRECTORY);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setCombinerClass(IndexReducer.class);

        job.setInputFormatClass(RecursiveFileInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(LuceneIndexFileOutputFormat.class);

        job.getConfiguration().set(KeywordsFileRecordReader.class.getName() + KeywordsFileRecordReader.KEYWORDS,
                "meta,keywords");
        job.getConfiguration().set(KeywordsFileRecordReader.class.getName() + KeywordsFileRecordReader.START_READING_KEYWORDS,
                "<head");
        job.getConfiguration().set(KeywordsFileRecordReader.class.getName() + KeywordsFileRecordReader.STOP_READING_KEYWORDS,
                "</head>");

        FileInputFormat.addInputPath(job, new Path(BASE_INPUT_DIRECTORY));
        FileOutputFormat.setOutputPath(job, outputPath);
        job.getConfiguration().set(LuceneIndexFileOutputFormat.class.getName() + LuceneIndexFileOutputFormat.OUTPUT_DIR,
                "home/index/lucene_index");

        job.waitForCompletion(true);
    }

}
