package com.sap.utility;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.TreeSet;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 25, 2011
 * Time: 8:38:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseUtil {
     private static Collection<Integer> getFids(int id, HTable table) throws IOException {
        Get get = new Get(Bytes.toBytes(id));
        KeyValue[] kvs = table.get(get).raw();
        Collection<Integer> set = new TreeSet<Integer>();
        for (KeyValue kv : kvs) {
            int fid = Bytes.toInt(kv.getValue());
            set.add(fid);
        }
        return set;
    }

    public static void main(String[] arg) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "dewei0:60000");
        HTable table = new HTable(conf, "PERSON_FRIENDS_TEMP");
        Collection<Integer> found = getFids(Integer.parseInt(arg[0]), table);
        System.out.println("Found "+found.size()+" fids...");
        for (int fid: found) {
            System.out.println("Fid: "+fid);
        }
    }
}
