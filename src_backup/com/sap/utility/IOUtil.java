package com.sap.utility;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 14, 2011
 * Time: 4:10:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class IOUtil {
    public static BufferedReader getBufferedReader(String filename) throws IOException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
    }

    public static BufferedReader getBufferedReader(File f) throws IOException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(f)));
    }

    public static BufferedWriter getBufferedWriter(String filename) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename)));
    }

    public static BufferedReader getUrlBufferedReader(String url) throws Exception {
        return new BufferedReader(new InputStreamReader(new URI(url).toURL().openStream()));
    }

    public static void toFile(String content, String filename) throws IOException {
        if (content != null) {
            BufferedWriter out = getBufferedWriter(filename);
            out.write(content);
            out.close();
        } else {
            throw new IOException("content is null");
        }
    }

    public static String getFileContent(String filename) throws IOException {
        BufferedReader in = getBufferedReader(new File(filename));
        StringBuilder sb = new StringBuilder();
        String line = in.readLine();
        while (line != null) {
            sb.append(line).append("\n");
            line = in.readLine();
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static Collection<String> getFileLineContent(String filename) throws IOException {
        Collection<String> lines = new ArrayList<String>();
        BufferedReader in = getBufferedReader(new File(filename));
        String line = in.readLine();
        while (line != null) {
            lines.add(line);
            line = in.readLine();
        }
        return lines;
    }

    public static void switchColumn(String inFilename, String outFilename, List<String> newCmds) throws IOException {
        if (newCmds == null) {
            throw new IOException("newCmds is null");
        }

        BufferedReader in = getBufferedReader(inFilename);
        BufferedWriter out = getBufferedWriter(outFilename);
        String line = in.readLine();
        String[] cmds = line.trim().split(",");
        int count = 1;
        while (line != null) {
            StringBuilder sb = new StringBuilder();
            List<String> dataRow = Arrays.asList(line.split(","));

            for (String newCmd : newCmds) {
                for (int i = 0; i < cmds.length; i++) {
                    if (newCmd.equalsIgnoreCase(cmds[i])) {
                        sb.append(dataRow.get(i)).append(",");
                    }
                }
            }
            sb.deleteCharAt(sb.length() - 1);
            out.write(sb.toString() + "\n");
            if (count % 1000000 == 0) {
                System.out.println("Done " + count + " lines.");
            }
            line = in.readLine();
            count++;
        }
        out.close();
        in.close();
    }

    public static void generateNumberCsv(String filename, int min, int max, int numPerRow) throws IOException {
        BufferedWriter out = getBufferedWriter(filename);
        int start = min;
        int end = min + numPerRow;

        while (end < max + 2) {
            List<String> list = new ArrayList<String>();
            for (int i = start; i < end; i++) {
                list.add(String.valueOf(i));
            }
            start = end;
            end = end + numPerRow;
            String line = getDelimitedLine(list.toArray(new String[list.size()]), ',');
            out.write(line);
            out.write("\n");
        }
        out.close();
    }

    public static void generateRandomNumberCsv(String filename, int min, int max, int numPerRow, int total) throws IOException {
        BufferedWriter out = getBufferedWriter(filename);
        int[] numArr = new int[total];
        Set<Integer> numSet = new HashSet<Integer>();

        for (int i=0; i<numArr.length; i++) {
            int random = (int)(Math.random() * (max-min)) + min + 1;
            while (numSet.contains(random)) {
                random = (int)(Math.random() * (max-min)) + min + 1;
            }
            numSet.add(random);
            numArr[i] = random;
        }

        int count = 1;
        StringBuilder sb = new StringBuilder();
        for (int num: numArr) {
            sb.append(num).append(",");
            if (count % numPerRow == 0) {
                sb.deleteCharAt(sb.length()-1).append("\n");
                out.write(sb.toString());
                sb = new StringBuilder();
            }
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length()-1).append("\n");
            out.write(sb.toString());
        }
        out.close();
    }

    public static String getDelimitedLine(String[] cells, char delimiter) {
        StringBuilder sb = new StringBuilder();
        for (String cell : cells) {
            sb.append(cell).append(delimiter);
        }
        return sb.deleteCharAt(sb.length() - 1).toString();
    }

    public static String getStringValue(KeyValue[] kvs, String columnName) throws IOException {
        for (KeyValue kv : kvs) {
            if (columnName.equals(Bytes.toString(kv.split().getFamily()))) {
                return Bytes.toString(kv.split().getValue());
            }
        }
        return null;
    }

    public static Integer getIntegerValue(KeyValue[] kvs, String columnName) throws IOException {
        for (KeyValue kv : kvs) {
            if (columnName.equals(Bytes.toString(kv.split().getFamily()))) {
                return Bytes.toInt(kv.split().getValue());
            }
        }
        return null;
    }

    public static Float getFloatValue(KeyValue[] kvs, String columnName) throws IOException {
        for (KeyValue kv : kvs) {
            if (columnName.equals(Bytes.toString(kv.split().getFamily()))) {
                return Bytes.toFloat(kv.split().getValue());
            }
        }
        return null;
    }

    public static void divideFile() throws Exception {
        //List<String> newCmds = new ArrayList<String>();
        //newCmds.add("FID");
        //newCmds.add("ID");
        //switchColumn(NameCollector.PERSON_FRIENDS_FILE, NameCollector.FRIENDS_PERSON_FILE, newCmds);

        //generateNumberCsv("c:\\projects\\sadoop\\data\\Ids.csv", 1, 1000 * 1000, 1);
        //generateRandomNumberCsv("c:\\projects\\sadoop\\data\\Ids.csv", 1, 1000 * 1000, 1, 1000);

        int fileIndex = 1;
        BufferedReader in = getBufferedReader(NameCollector.PERSON_FRIENDS_FILE);
        BufferedWriter out = getBufferedWriter(NameCollector.PERSON_FRIENDS_FILE + "_" + fileIndex);
        String cmd = in.readLine();
        out.write(cmd + "\n");
        String line = in.readLine();
        String oldId = null;
        int count = 1;
        while(line != null) {
            boolean inserted = false;
            String[] idFid = line.split(",");
            String nowId = idFid[0];
            if (oldId == null) {
                oldId = nowId;
            }
            if (!oldId.equals(nowId)) {
                oldId = nowId;
                if (count % 100000 == 0) {
                    out.close();
                    fileIndex++;
                    inserted = true;
                    out = getBufferedWriter(NameCollector.PERSON_FRIENDS_FILE + "_" + fileIndex);
                    out.write(cmd+"\n");
                }
                count++;
            }
            out.write(line + "\n");
            line = in.readLine();
            if (line == null && !inserted) {
                out.close();
            }
        }
        
    }

    public static void main(String[] args) throws Exception {
        BufferedReader in = IOUtil.getBufferedReader("C:\\projects\\sadoop\\data\\person_friends_1m-200-20.csv");
        BufferedWriter out = IOUtil.getBufferedWriter("C:\\projects\\sadoop\\data\\person_friends_1m-200-20_noCmd.csv");

        String line = in.readLine();
        line = in.readLine();
        while (line != null) {
            out.write(line + "\n");
            out.flush();
            line = in.readLine();
        }
        in.close();
        out.close();
    }
}
