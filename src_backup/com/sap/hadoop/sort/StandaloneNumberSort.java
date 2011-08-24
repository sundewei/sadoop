package com.sap.hadoop.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HftpFileSystem;

import java.io.*;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by IntelliJ IDEA.
 * User: Frank
 * Date: 2011/1/20
 * Time: ¤U¤È 10:56:06
 * To change this template use File | Settings | File Templates.
 */
public class StandaloneNumberSort {

    public static void main(String[] arg) throws Exception {
        sortInHdfs();
        //sortInLocalhost(arg[0]);
    }

    public static void sortInLocalhost(String baseDir) throws Exception {
        long start = System.currentTimeMillis();
        Collection<Integer> tree = new TreeSet<Integer>();
        Collection<File> files = new HashSet<File>();
        files = Utility.getFiles(files, new File(baseDir));
        for (File file : files) {
            System.out.println("Try to read: " + file.getAbsolutePath());
            FileInputStream in = new FileInputStream(file);
            Collection<Integer> sourceNumbers = getNumbers(in);
            System.out.println("sourceNumbers.size()" + sourceNumbers.size());
            tree.addAll(sourceNumbers);
            in.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("Size = " + tree.size());
        System.out.println("Took: " + (end - start) + " ms.");
    }

    public static void sortInHdfs() throws Exception {
        long start = System.currentTimeMillis();
        Collection<Integer> tree = new TreeSet<Integer>();
        HftpFileSystem hftpFileSystem = new HftpFileSystem();
        hftpFileSystem.initialize(new URI("http://dewei0:50070"), new Configuration());
        Path path = new Path("/user/ccc/home/sort/source3/");
        Collection<Path> subPathes = new HashSet<Path>();
        subPathes = Utility.getPaths(hftpFileSystem, subPathes, path);
        int count = 1;
        for (Path subPath : subPathes) {
            System.out.println(count + "/" + subPathes.size() + "... Working on: " + subPath.toString());
            InputStream in = Utility.getInputStream(hftpFileSystem, subPath);
            Collection<Integer> numbers = getNumbers(in);
            in.close();
            tree.addAll(numbers);
            count++;
        }
        long end = System.currentTimeMillis();
        for (int num : tree) {
            System.out.println(num);
        }
        System.out.println("Took: " + (end - start) + " ms.");
    }

    private static Collection<Integer> getNumbers(InputStream is) throws IOException {
        Set<Integer> set = new HashSet<Integer>();
        BufferedReader in =
                new BufferedReader(new InputStreamReader(is));
        String line = in.readLine();
        while (line != null) {
            set.addAll(Utility.getNumbers(line, ","));
            line = in.readLine();
        }
        return set;
    }
}
