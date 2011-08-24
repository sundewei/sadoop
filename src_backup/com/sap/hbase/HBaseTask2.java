package com.sap.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 23, 2011
 * Time: 10:28:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseTask2 {
    private static HTable personFriends;

    private static Collection<Integer> getFids(int id, Collection<Integer> excludedList, int excludedId1, int excludeId2) throws IOException {
        Get get = new Get(Bytes.toBytes(id));
        KeyValue[] kvs = personFriends.get(get).raw();
        Collection<Integer> set = new HashSet<Integer>();
        for (KeyValue kv : kvs) {
            int fid = Bytes.toInt(kv.getValue());
            if (fid != excludedId1 && fid != excludeId2 && (excludedList == null || !excludedList.contains(fid))) {
                set.add(fid);
            }
        }
        return set;
    }

    public static class IdMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int commonFriendLimit = 2;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (personFriends == null) {
                personFriends = new HTable(context.getConfiguration(), "PERSON_FRIENDS_TEMP");
            }
            // This is the file name itself
            String content = value.toString();
            String[] ids = content.split(",");

            // 1
            for (String id : ids) {
                Integer intId = new Integer(id);
                Text textId = new Text(id);
                Collection<Integer> fids = getFids(intId, null, intId, -1);
                Set<Integer> deDup = new HashSet<Integer>();
                // 8,9,10
                for (Integer fid : fids) {
                    Collection<Integer> ffids = getFids(fid, fids, intId, -1);
                    for (Integer ffid: ffids) {
                        if (commonFriendLimit > 1 && !deDup.contains(ffid)) {
                            Collection<Integer> fffids = getFids(ffid, null, -1, -1);
                            fffids.retainAll(fids);
                            if (fffids.size() >= commonFriendLimit) {
                                context.write(textId, new Text(ffid.toString()));
                            }
                        } else {
                            context.write(textId, new Text(ffid.toString()));
                        }
                        deDup.add(ffid);
                    }
                }
                context.progress();
            }
        }
    }

    public static class IdReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            String outKey = key.toString() + " : ";
            int count = 0;
            for (Text text : values) {
                sb.append(text.toString()).append(",");
                count++;
            }
            sb.deleteCharAt(sb.length() - 1);
            context.write(new Text(outKey+count), new Text(sb.toString()));
        }
    }

    public static void main(String[] arg) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "dewei0:60000");
        conf.set("common_friend_limit", "2");
        Job job = new Job(conf, "FindPossibleFriendsLineByLine");
        job.setJarByClass(HBaseTask2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().set(LineInputFormat.class.getName() + "_filename", "/home/ccc/hbase/source1/Ids.csv");

        job.setInputFormatClass(LineInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/home/ccc/hbase/output1/"));

        job.setMapperClass(HBaseTask2.IdMapper.class);
        job.setReducerClass(HBaseTask2.IdReducer.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
