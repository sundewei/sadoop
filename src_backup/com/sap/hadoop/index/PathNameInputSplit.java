package com.sap.hadoop.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: Frank
 * Date: 2011/1/16
 * Time: ¤W¤È 10:52:42
 * To change this template use File | Settings | File Templates.
 */
public class PathNameInputSplit extends InputSplit implements Writable {
    String pathString;

    public PathNameInputSplit() {
    }

    public PathNameInputSplit(String pathString) {
        this.pathString = pathString;
    }

    public long getLength() {
        return pathString == null ? 0 : pathString.length();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, pathString);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pathString = Text.readString(in);
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[]{};
    }

}
