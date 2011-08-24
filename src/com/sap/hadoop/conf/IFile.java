package com.sap.hadoop.conf;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 4/21/11
 * Time: 12:45 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IFile {
    public String getName();

    public long getModificationTime();

    public String getOwner();

    public long getLen();

    public String getFilename();

    public boolean isDir();
}
