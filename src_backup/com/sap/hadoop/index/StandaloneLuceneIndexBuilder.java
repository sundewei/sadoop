package com.sap.hadoop.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HftpFileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Jan 19, 2011
 * Time: 5:06:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class StandaloneLuceneIndexBuilder {
    private static Pattern META_PATTERN =
            Pattern.compile("<meta[\\s]+[^>]*?name[\\s]?=[\\s\\\"\\']+(.*?)[\\s\\\"\\']+content[\\s]?=[\\s\\\"\\']+(.*?)[\\\"\\']+.*?>", Pattern.CASE_INSENSITIVE);

    private static final Pattern DELIMITER_PATTERN = Pattern.compile("[,]+");

    private static Map<String, Set<String>> getKeywordIndex(HftpFileSystem hftpFileSystem, Path[] paths) throws IOException {
        Map<String, Set<String>> keywordIndex = new TreeMap<String, Set<String>>();
        System.out.println("Found " + paths.length + " files.");
        for (Path path : paths) {
            setKeywordIndex(hftpFileSystem, path, keywordIndex);
        }
        return keywordIndex;
    }

    private static void setKeywordIndex(HftpFileSystem hftpFileSystem, Path path, Map<String, Set<String>> keywordIndex)
            throws IOException {
        if (!hftpFileSystem.isFile(path)) {
            return;
        }
        BufferedReader in = null;
        int tryCount = 0;
        for (int i = 0; i < tryCount; i++) {
            if (i > 0) {
                System.out.println("Trying " + (i + 1) + " times on " + path.toString());
            }
            try {
                in = new BufferedReader(new InputStreamReader(hftpFileSystem.open(path)));
                String line = in.readLine();
                while (line != null) {
                    String lowerLine = line.toLowerCase();
                    Matcher matcher = META_PATTERN.matcher(lowerLine);
                    while (matcher.find()) {
                        String metaTagName = matcher.group(1);
                        String content = matcher.group(2);
                        if (metaTagName.equalsIgnoreCase("keywords")) {
                            String[] keywords = DELIMITER_PATTERN.split(content);
                            for (String keyword : keywords) {
                                Set<String> filenames = keywordIndex.get(keyword);
                                if (filenames == null) {
                                    filenames = new HashSet<String>();
                                }
                                filenames.add(path.toString());
                                keywordIndex.put(keyword, filenames);
                            }
                            return;
                        }
                    }
                    line = in.readLine();
                }
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }

    public static void main(String[] arg) throws Exception {
        long start = System.currentTimeMillis();
        HftpFileSystem hftpFileSystem = new HftpFileSystem();
        hftpFileSystem.initialize(new URI("http://dewei0:50070"), new Configuration());
        Path path = new Path("/user/ccc/home/index/source3/");
        Collection<Path> paths = new HashSet<Path>();
        paths = com.sap.hadoop.sort.Utility.getPaths(hftpFileSystem, paths, path);
        Map<String, Set<String>> map = getKeywordIndex(hftpFileSystem, paths.toArray(new Path[paths.size()]));

        for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
            Set<String> values = entry.getValue();
            StringBuilder sb = new StringBuilder();
            for (String value : values) {
                sb.append(value).append("\n");
            }
            System.out.println(entry.getKey() + "\t" + sb.toString());
        }
        long end = System.currentTimeMillis();
        System.out.println("Took :" + (end - start) + " ms.");
    }
}
