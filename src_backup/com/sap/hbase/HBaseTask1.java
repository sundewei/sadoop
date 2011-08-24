package com.sap.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 16, 2011
 * Time: 4:12:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseTask1 {

    public static class Reducer extends TableReducer<Text, Put, Text> {
        public void reduce(Text key, Iterable<Put> values, Context context)
                throws IOException, InterruptedException {
            Put put = null;
            while (values.iterator().hasNext()) {
                put = values.iterator().next();
            }
            context.write(key, put);
        }
    }

    private static void createJoinTable(HBaseAdmin hbase, String tableName, String[] columns) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (String column : columns) {
            System.out.println("Adding " + column + " as a column...");
            HColumnDescriptor colDesc = new HColumnDescriptor(column.getBytes());
            desc.addFamily(colDesc);
        }
        if (hbase.tableExists(tableName)) {
            if (!hbase.isTableDisabled(tableName)) {
                hbase.disableTable(tableName);
            }
            hbase.deleteTable(tableName);
        }
        hbase.createTable(desc);
    }

    public static void main1(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "dewei0:60000");
        HBaseAdmin hbase = new HBaseAdmin(conf);
        String[] joinColumns = new String[7];
        joinColumns[0] = "LOC_ID";
        joinColumns[1] = "START_IP_NUM";
        joinColumns[2] = "END_IP_NUM";
        joinColumns[3] = "COUNTRY";
        joinColumns[4] = "CITY";
        joinColumns[5] = "LATITUDE";
        joinColumns[6] = "LONGITUDE";

        createJoinTable(hbase, "IP_LOCATIONS_JOIN", joinColumns);

        Job job = new Job(conf, "IpJoinTask");
        job.setJarByClass(HBaseTask1.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("START_IP_NUM"));
        scan.addFamily(Bytes.toBytes("END_IP_NUM"));
        scan.addFamily(Bytes.toBytes("LOC_ID"));
        //scan.setFilter(new FirstKeyOnlyFilter());
        scan.setCaching(1000);

        TableMapReduceUtil.initTableMapperJob("CITY_IP_BLOCKS", scan, HBaseMapper1.class, Text.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob("IP_LOCATIONS_JOIN", Reducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "dewei0:60000");
        HBaseAdmin hbase = new HBaseAdmin(conf);
        String[] joinColumns = new String[7];
        joinColumns[0] = "LOC_ID";
        joinColumns[1] = "START_IP_NUM";
        joinColumns[2] = "END_IP_NUM";
        joinColumns[3] = "COUNTRY";
        joinColumns[4] = "CITY";
        joinColumns[5] = "LATITUDE";
        joinColumns[6] = "LONGITUDE";

        createJoinTable(hbase, "IP_LOCATIONS_JOIN", joinColumns);

        Job job = new Job(conf, "IpJoinTask");
        job.setJarByClass(HBaseTask1.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("START_IP_NUM"));
        scan.addFamily(Bytes.toBytes("END_IP_NUM"));
        scan.addFamily(Bytes.toBytes("LOC_ID"));
        //scan.setFilter(new FirstKeyOnlyFilter());
        scan.setCaching(1000);

        TableMapReduceUtil.initTableMapperJob("CITY_IP_BLOCKS", scan, HBaseMapper1.class, Text.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob("IP_LOCATIONS_JOIN", Reducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
