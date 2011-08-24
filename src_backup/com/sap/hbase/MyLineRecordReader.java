package com.sap.hbase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 24, 2011
 * Time: 11:28:07 AM
 * To change this template use File | Settings | File Templates.
 */
public class MyLineRecordReader extends RecordReader<LongWritable, Text> {
    private Text line;
    private boolean hasReturned;

    public MyLineRecordReader() {
    }

    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        CsvLineInputSplit split = (CsvLineInputSplit) genericSplit;
        line = new Text(split.line);
    }

    public boolean nextKeyValue() throws IOException {
        return !hasReturned;
    }

    @Override
    public LongWritable getCurrentKey() {
        return new LongWritable(System.currentTimeMillis());
    }

    @Override
    public Text getCurrentValue() {
        hasReturned = true;
        return line;
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() {
        return hasReturned ? 1 : 0;
    }

    public synchronized void close() throws IOException {
    }
}
