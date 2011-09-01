package com.sap.hadoop.conf;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 4/21/11
 * Time: 12:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileImpl implements IFile, Comparable<IFile> {
    private String name;
    private long modificationTime;
    private long len;
    private String owner;
    private String filename;
    private boolean dir;

    public int compareTo(IFile ifile) {
        return this.filename.compareTo(ifile.getFilename());
    }

    public FileImpl(String name, String owner, long modificationTime, long len, String filename) {
        this.name = name;
        this.owner = owner;
        this.modificationTime = modificationTime;
        this.len = len;
        int doubleSlashIdx = filename.indexOf("//");
        if (doubleSlashIdx > 0) {
            filename = filename.substring(filename.indexOf("/", doubleSlashIdx + 2), filename.length());
        }
        this.filename = filename;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public long getLen() {
        return len;
    }

    public String getOwner() {
        return owner;
    }

    public String getName() {
        return name;
    }

    public String getFilename() {
        return filename;
    }

    public boolean isDir() {
        return dir;
    }

    public void setDir(boolean isD) {
        dir = isD;
    }
}
