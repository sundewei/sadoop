package com.sap.hbase;


import com.sap.utility.IOUtil;
import com.sap.utility.NameCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.*;


/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 9, 2011
 * Time: 10:08:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseInserter {

    private static HTable createTable(HBaseAdmin hbase, String tableName, Collection<String> columns, boolean dropIfExist) throws Exception {
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (String column : columns) {
            System.out.println("Adding " + column + " as a column...");
            HColumnDescriptor colDesc = new HColumnDescriptor(column.getBytes());
            desc.addFamily(colDesc);
        }
        if (dropIfExist && hbase.tableExists(tableName)) {
            System.out.println("About to drop table: " + tableName + "...Sleeping for 10 second before continuing");
            Thread.sleep(10 * 1000);
            if (!hbase.isTableDisabled(tableName)) {
                hbase.disableTable(tableName);
            }
            hbase.deleteTable(tableName);
        }

        if (!hbase.tableExists(tableName)) {
            hbase.createTable(desc);
        }
        return new HTable(hbase.getConfiguration(), tableName);
    }

    private static HTable createTable(HBaseAdmin hbase, String tableName, String[] columns, boolean dropIfExist) throws Exception {
        return createTable(hbase, tableName, Arrays.asList(columns), dropIfExist);
    }

    private static void insertRows(HTable htable, String[] rowKeyColumns, String[] cmdArray,
                                   Object[] dataArray)
            throws IOException {
        List<Object> rowKeyValues = new ArrayList<Object>();
        byte[] rowkey = null;
        for (int i = 0; i < rowKeyColumns.length; i++) {
            if (rowKeyColumns[i] != null) {
                rowKeyValues.add(dataArray[i]);
//System.out.println("Adding "+dataArray[i]+" as rowkey");
            }
        }
        rowkey = getRowKey(rowKeyValues.toArray(new Object[rowKeyValues.size()]));
//System.out.println("rowKey = "+rowkey.toString());
//System.out.println("cmdArray.length="+cmdArray.length);        
        Put put = new Put(rowkey);
        for (int i = 0; i < cmdArray.length; i++) {
            if (dataArray[i] instanceof String) {
                put.add(Bytes.toBytes(cmdArray[i]), rowkey, Bytes.toBytes((String) dataArray[i]));
            } else if (dataArray[i] instanceof Integer) {
                put.add(Bytes.toBytes(cmdArray[i]), rowkey, Bytes.toBytes((Integer) dataArray[i]));
            }
        }
        htable.put(put);
//System.out.println("\n\n\n");
    }

    private static void insertRow(HTable htable, byte[] rowKey, byte[] family, Map<byte[], byte[]> qualifierValueMap)
            throws IOException {
//System.out.println("\n\n\n\nAbout to Insert row...");
        Put put = new Put(rowKey);
        for (Map.Entry<byte[], byte[]> entry : qualifierValueMap.entrySet()) {
            int key = Bytes.toInt(entry.getKey());
            int value = Bytes.toInt(entry.getValue());
//System.out.println("In Map, the key = "+key+", the value = "+value);
            put.add(family, entry.getKey(), entry.getValue());
        }
        for (Map.Entry<byte[], List<KeyValue>> entry : (Set<Map.Entry<byte[], List<KeyValue>>>) put.getFamilyMap().entrySet()) {
            byte[] keyBytes = entry.getKey();
            String familyString = Bytes.toString(keyBytes);
//System.out.println("Family: "+familyString+"\n");
            List<KeyValue> list = entry.getValue();
            for (KeyValue kv : list) {
                byte[] kkk = kv.getQualifier();
                byte[] vvv = kv.getValue();
                int index = Bytes.toInt(kkk);
                int fid = Bytes.toInt(vvv);
//System.out.println("      ("+index+"th FID) = "+fid);
            }
        }
        htable.put(put);
    }

    private static byte[] getRowKey(Object... rowKeyValues) throws IOException {
        if (rowKeyValues == null) {
            throw new IOException("rowKeyValues is null");
        }

        byte[] rowkey = null;
        for (Object rowKeyValue : rowKeyValues) {
            if (rowkey != null) {
                if (rowKeyValue instanceof String) {
//System.out.println("Found String RowKey2");
                    rowkey = Bytes.add(rowkey, Bytes.toBytes((String) rowKeyValue));
                } else if (rowKeyValue instanceof Integer) {
//System.out.println("Found Integer RowKey2");
                    rowkey = Bytes.add(rowkey, Bytes.toBytes((Integer) rowKeyValue));
                }
            } else {
                if (rowKeyValue instanceof String) {
//System.out.println("Found String RowKey1");
                    rowkey = Bytes.toBytes((String) rowKeyValue);
                } else if (rowKeyValue instanceof Integer) {
//System.out.println("Found Integer RowKey1");
                    rowkey = Bytes.toBytes((Integer) rowKeyValue);
                }
            }
        }

        return rowkey;
    }

    private static void createAndUpsertTable(HBaseAdmin hbase, String tableName, String[] rowKeyColumns, InputStream is)
            throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String line = in.readLine();
        HTable htable = null;
        List<String> columns = new ArrayList<String>();
        int lineCount = 1;
        while (line != null) {
            if (line.length() > 0) {
                // Find the cmds if it has not been found yet
                if (columns.size() == 0) {
                    StringTokenizer st = new StringTokenizer(line.replaceAll("\"", ""), ",");
                    Collection<String> cmdList = new ArrayList<String>();
                    while (st.hasMoreTokens()) {
                        String cmdToken = st.nextToken();
                        if (cmdToken.length() > 0) {
                            System.out.println("Adding " + cmdToken + " as the column name...");
                            cmdList.add(cmdToken);
                        }
                    }
                    String[] cmdArray = cmdList.toArray(new String[cmdList.size()]);
                    columns = Arrays.asList(cmdArray);
                    htable = createTable(hbase, tableName, columns, true);
                } else {
                    String[] dataArray = line.replaceAll("\"", "").trim().split(",", -1);
                    Object[] dataObjArray = getObjectArray(dataArray);
                    insertRows(htable, rowKeyColumns, columns.toArray(new String[columns.size()]), dataObjArray);
                }
            }
            line = in.readLine();
            lineCount++;
            if (lineCount % 10000 == 0) {
                System.out.println("lineCount=" + lineCount);
            }
        }
        in.close();
    }

    private static Object[] getObjectArray(String[] strArray) {
        List<Object> objList = new ArrayList<Object>();
        for (String str : strArray) {
            Integer intValue = null;
            try {
                intValue = Integer.parseInt(str);
            } catch (NumberFormatException nfe) {
                intValue = null;
            }
            if (intValue != null) {
                objList.add(intValue);
            } else {
                if (str == null || str.length() == 0) {
                    objList.add(null);
                } else {
                    objList.add(str);
                }
            }
        }
        return objList.toArray(new Object[objList.size()]);
    }

    public static void loadRepotext(String csvFilename) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hbase = new HBaseAdmin(conf);
        conf.set("hbase.master", "dewei0:60000");
        String[] rowKeyColumns = new String[3];
        rowKeyColumns[0] = "PROGNAME";
        rowKeyColumns[1] = "R3STATE";
        rowKeyColumns[2] = "LANGUAGE";
        createAndUpsertTable(hbase, "REPOTEXT", rowKeyColumns, new FileInputStream(csvFilename));
    }

    public static void loadCityIpBlocks(String csvFilename) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hbase = new HBaseAdmin(conf);
        conf.set("hbase.master", "dewei0:60000");
        String[] rowKeyColumns = new String[2];
        rowKeyColumns[0] = "START_IP_NUM";
        rowKeyColumns[1] = "END_IP_NUM";
        createAndUpsertTable(hbase, "CITY_IP_BLOCKS", rowKeyColumns, new FileInputStream(csvFilename));
    }

    public static void loadCityLocations(String csvFilename) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hbase = new HBaseAdmin(conf);
        conf.set("hbase.master", "dewei0:60000");
        String[] rowKeyColumns = new String[1];
        rowKeyColumns[0] = "LOC_ID";
        createAndUpsertTable(hbase, "CITY_LOCATIONS", rowKeyColumns, new FileInputStream(csvFilename));
    }

    public static void main(String[] arg) throws Exception {
        //loadCityLocations("/usr/local/projects/sadoop/data/GeoLiteCity-Location.csv");
        //System.out.println("\n\n\n\n");
        //loadCityIpBlocks("/usr/local/projects/sadoop/data/GeoLiteCity-Blocks.csv");
        loadPersonFriends(arg[0], Boolean.valueOf(arg[1]));
    }

    public static void loadPersonFriends(String filename, boolean dropIfExist) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin hbase = new HBaseAdmin(conf);
        conf.set("hbase.master", "dewei0:60000");
        String[] rowKeyColumns = new String[2];
        rowKeyColumns[0] = "ID";
        rowKeyColumns[1] = "FID";
        HTable table = createTable(hbase, "PERSON_FRIENDS_TEMP", rowKeyColumns, dropIfExist);
        BufferedReader in = null;
        if (filename != null) {
            in = IOUtil.getBufferedReader(filename);
        } else {
            in = IOUtil.getBufferedReader(NameCollector.PERSON_FRIENDS_FILE);
        }
        String line = in.readLine();
        // Skip the cmd line
        line = in.readLine();
        Collection<Integer> fids = new ArrayList<Integer>();
        String oldId = null;
        int count = 1;
        while (line != null) {
            String[] idFid = line.split(",");
            String nowId = idFid[0];
            String nowFid = idFid[1];
            if (oldId == null) {
                oldId = nowId;
            }
            boolean inserted = false;
            if (!oldId.equals(nowId)) {
                insertRow(table, Bytes.toBytes(Integer.parseInt(oldId)), Bytes.toBytes("FID"), getIndexedIntegerMap(fids));
                if (count % 10000 == 0) {
                    System.out.println(count + ", About to flush ID=" + oldId + ", he has " + fids.size() + " friends...");
                    table.flushCommits();
                }
                inserted = true;
                fids = new ArrayList<Integer>();
                oldId = nowId;
                count++;
            }
            fids.add(Integer.parseInt(nowFid));
            line = in.readLine();

            if (line == null && !inserted) {
                insertRow(table, Bytes.toBytes(Integer.parseInt(oldId)), Bytes.toBytes("FID"), getIndexedIntegerMap(fids));
                System.out.println("Last person, about to flush ID=" + oldId + ", he has " + fids.size() + " friends...");
                table.flushCommits();
            }
        }
    }

    private static Map<byte[], byte[]> getIndexedIntegerMap(Collection<Integer> ids) {
        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
        Iterator<Integer> iterator = ids.iterator();
        int i = 1;
        while (iterator.hasNext()) {
            map.put(Bytes.toBytes(i), Bytes.toBytes(iterator.next().intValue()));
            i++;
        }
        return map;
    }
}
