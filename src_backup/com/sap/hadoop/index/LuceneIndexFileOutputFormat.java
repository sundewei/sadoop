package com.sap.hadoop.index;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Jan 18, 2011
 * Time: 4:00:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class LuceneIndexFileOutputFormat<K, V> extends FileOutputFormat<K, V> {
    public static final String OUTPUT_DIR = "outputDir";

    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        LuceneIndexRecordWriter<K, V> luceneRecordWriter = new LuceneIndexRecordWriter<K, V>();
        FileSystem fs = Utility.getFileSystem(job);
        luceneRecordWriter.setFileSystem(fs);
        luceneRecordWriter.setIndexHdfsDirectory(job.getConfiguration().get(LuceneIndexFileOutputFormat.class.getName() + LuceneIndexFileOutputFormat.OUTPUT_DIR));
        return luceneRecordWriter;
    }
}
