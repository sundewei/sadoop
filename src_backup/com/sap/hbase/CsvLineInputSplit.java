package com.sap.hbase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 24, 2011
 * Time: 12:25:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class CsvLineInputSplit extends InputSplit implements Writable {
    String line;

    public CsvLineInputSplit() {
    }

    public CsvLineInputSplit(String line) {
        this.line = line;
    }

    public long getLength() {
        return line == null ? 0 : line.length();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, line);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        line = Text.readString(in);
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[]{};
    }

}