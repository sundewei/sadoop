package com.sap.hadoop.index;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Frank
 * Date: 2011/1/16
 * Time: ¤W¤È 10:46:07
 * To change this template use File | Settings | File Templates.
 */
public class RecursiveFileInputFormat extends FileInputFormat {
    /**
     * Generate the list of files and make them into FileSplits.
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        // generate splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (Object fileObj : this.listStatus(job)) {
            FileStatus file = (FileStatus) fileObj;
            Path path = file.getPath();

            List<String> filenames = new ArrayList<String>();
            getFilenames(job, filenames, path);

            for (String filename : filenames) {
                PathNameInputSplit pathIs = new PathNameInputSplit(filename);
                splits.add(pathIs);
            }

        }
        return splits;
    }

    private List<String> getFilenames(JobContext job, List<String> filenames, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(job.getConfiguration());
        if (fs.isFile(path)) {
            String pathString = path.toString();
            filenames.add(pathString);
        } else {
            for (FileStatus fss : fs.listStatus(path)) {
                getFilenames(job, filenames, fss.getPath());
            }
        }
        return filenames;
    }


    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new KeywordsFileRecordReader();
    }

    private static boolean include(Set<String> excludes, String lowerPathString) {

        for (String ext : excludes) {
            if (lowerPathString.endsWith(ext)) {
                return false;
            }
        }
        return true;
    }
}
