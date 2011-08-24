package com.sap.hadoop.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Jan 19, 2011
 * Time: 10:13:25 AM
 * To change this template use File | Settings | File Templates.
 */
public class Utility {
    private Utility() {
    }

    public static FileSystem getFileSystem(Job job) throws IOException {
        return job.getWorkingDirectory().getFileSystem(job.getConfiguration());
    }

    public static FileSystem getFileSystem(TaskAttemptContext job) throws IOException {
        return job.getWorkingDirectory().getFileSystem(job.getConfiguration());
    }

    public static FileSystem getFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }


}
