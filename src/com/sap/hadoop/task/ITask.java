package com.sap.hadoop.task;

import org.apache.hadoop.mapreduce.Job;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 4/25/11
 * Time: 9:35 AM
 * To change this template use File | Settings | File Templates.
 */
public interface ITask {
    public Job getMapReduceJob() throws Exception;
}
