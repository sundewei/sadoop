package com.sap.hbase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 24, 2011
 * Time: 11:24:41 AM
 * To change this template use File | Settings | File Templates.
 */
public class LineInputFormat<LongWritable, Text> extends InputFormat<LongWritable, Text> {
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> list = new ArrayList<InputSplit>();
        String filename = job.getConfiguration().get(LineInputFormat.class.getName() + "_filename");
        Path path = new Path(filename);
        FileSystem fs = path.getFileSystem(job.getConfiguration());
        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = in.readLine();
        while (line != null) {
            InputSplit split = new CsvLineInputSplit(line);
            list.add(split);
            line = in.readLine();
        }
        return list;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return (RecordReader<LongWritable, Text>) new MyLineRecordReader();
    }
}
