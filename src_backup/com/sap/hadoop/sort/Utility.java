package com.sap.hadoop.sort;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HftpFileSystem;

import java.io.*;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Jan 20, 2011
 * Time: 4:29:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class Utility {

    public static void writeTo(final String fullFilename, final String content, boolean append) throws IOException {
        OutputStream out = null;
        Writer writer = null;
        try {
            File file = new File(fullFilename);
            if (!file.exists()) {
                file.createNewFile();
            } else {
                if (!append) {
                    file.delete();
                    file.createNewFile();
                }
            }
            out = new BufferedOutputStream(new DataOutputStream(new FileOutputStream(fullFilename, append)));
            writer = new OutputStreamWriter(out, "UTF-8");
            writer.write(content);
            writer.flush();
        } finally {
            if (writer != null) {
                // indirectly closes the OutputStream
                writer.close();
            } else if (out != null) {
                out.close();
            }
        }
    }

    public static Collection<Integer> getNumbers(String line, String delimiter) {
        String[] strs = line.split(delimiter);
        Set<Integer> ints = new HashSet<Integer>(strs.length);
        for (String i : strs) {
            ints.add(Integer.parseInt(i));
        }
        return ints;
    }

    public static Collection<Path> getPaths(HftpFileSystem hftpFileSystem, Collection<Path> paths, Path dir)
            throws IOException {
        FileStatus[] fss = hftpFileSystem.listStatus(dir);
        for (FileStatus f : fss) {
            if (hftpFileSystem.isFile(dir)) {
                System.out.println("Add File: " + paths.size());
                paths.add(f.getPath());
            } else {
                System.out.println("Skip Path: " + paths.size());
                getPaths(hftpFileSystem, paths, f.getPath());
            }
        }
        if (paths.size() > 0 && paths.size() % 1000 == 0) {
            System.out.println("Added " + paths.size() + " files...");
        }
        return paths;
    }

    public static InputStream getInputStream(HftpFileSystem hftpFileSystem, Path path) throws IOException {
        return hftpFileSystem.open(path);
    }

    public static Collection<File> getFiles(Collection<File> files, File baseDir) throws IOException {
        if (baseDir.isFile()) {
            files.add(baseDir);
        } else {
            for (File file : baseDir.listFiles()) {
                getFiles(files, file);
            }
        }
        return files;
    }
}
