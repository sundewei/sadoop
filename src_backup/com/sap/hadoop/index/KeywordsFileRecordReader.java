package com.sap.hadoop.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Frank
 * Date: 2011/1/16
 * Time: ¤W¤È 11:13:27
 * To change this template use File | Settings | File Templates.
 */
public class KeywordsFileRecordReader extends RecordReader<Text, Text> {
    public static final String START_READING_KEYWORDS = "startReadingKeywords";
    public static final String KEYWORDS = "keywords";
    public static final String STOP_READING_KEYWORDS = "stopReadingKeywords";

    boolean hasFetched = false;
    String pathString = null;
    private String validContent;
    private boolean startReading = true;
    private boolean stopReading = false;
    private Set<String> neededKeywords;
    private Set<String> startReadingKeywords;
    private Set<String> stopReadingKeywords;

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        initKeywords(context.getConfiguration());
        PathNameInputSplit textIs = (PathNameInputSplit) genericSplit;
        this.pathString = textIs.pathString;
        Path path = new Path(pathString);
        validContent = readFileContentAsString(path.getFileSystem(context.getConfiguration()), path);
    }

    private void initKeywords(Configuration conf) {
        neededKeywords = parseStrings(conf.get(KeywordsFileRecordReader.class.getName() + "keywords"), ",");
        startReadingKeywords =
                parseStrings(conf.get(KeywordsFileRecordReader.class.getName() + "startReadingKeywords"), ",");
        stopReadingKeywords =
                parseStrings(conf.get(KeywordsFileRecordReader.class.getName() + "stopReadingKeywords"), ",");
    }


    public boolean nextKeyValue() {
        if (validContent == null) {
            return false;
        } else {
            return !hasFetched;
        }
    }

    @Override
    public Text getCurrentKey() {
        hasFetched = true;
        return new Text(pathString);
    }

    @Override
    public Text getCurrentValue() throws IOException {
        hasFetched = true;
        return new Text(validContent);
    }

    private String readFileContentAsString(FileSystem fs, Path path)
            throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(path)));
        fs.open(path);
        String line = in.readLine();
        while (line != null) {
            if (!startReading && hasAllKeywords(line, startReadingKeywords, true)) {
                startReading = true;
            }

            if (!stopReading && hasAllKeywords(line, stopReadingKeywords, false)) {
                stopReading = true;
            }

            if (startReading && hasAllKeywords(line, neededKeywords, false)) {
                sb.append(line).append("\n");
                in.close();
                return sb.toString();
            }

            if (stopReading) {
                in.close();
                return null;
            }
            line = in.readLine();
        }
        in.close();
        return null;
    }

    public void close() {
    }

    public float getProgress() {
        return 1.0f;
    }

    private static boolean hasAllKeywords(String line, Set<String> allNeededKeywords, boolean nullSetReturn) {
        // If no keyword is specified, then return true
        if (allNeededKeywords == null) {
            return nullSetReturn;
        } else {
            String lowerLine = line.toLowerCase();
            for (String keyword : allNeededKeywords) {
                if (!lowerLine.contains(keyword)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static Set<String> parseStrings(String line, String delimiterRegex) {
        if (line != null && delimiterRegex != null) {
            Set<String> set = new HashSet<String>();
            String[] strings = line.split(delimiterRegex);
            for (String string : strings) {
                set.add(string.trim().toLowerCase());
            }
            return set;
        }
        return null;
    }
}
