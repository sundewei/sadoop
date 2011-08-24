package com.sap.hbase;

import com.sap.utility.IOUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 17, 2011
 * Time: 11:48:06 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseMapper1 extends TableMapper<Text, Put> {
    private HTable cityLocations;

    @Override
    public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
        if (cityLocations == null) {
            cityLocations = new HTable(context.getConfiguration(), "CITY_LOCATIONS");
            //Pair<byte[][], byte[][]> keys = cityLocations.getStartEndKeys();
            //throw new IOException("\n\n\n\n\nRegion Count: keys.getFirst().length="+keys.getFirst().length+"\n\n\n\n\n");
        }
        int ipStartNum = IOUtil.getIntegerValue(values.raw(), "START_IP_NUM");
        int ipEndNum = IOUtil.getIntegerValue(values.raw(), "END_IP_NUM");
        int locId = IOUtil.getIntegerValue(values.raw(), "LOC_ID");
        Text outKey = new Text(ipStartNum + "_" + ipEndNum);
        Put put = new Put(row.get());
        put.add(Bytes.toBytes("LOC_ID"), Bytes.toBytes(""), Bytes.toBytes(locId));
        put.add(Bytes.toBytes("START_IP_NUM"), Bytes.toBytes(""), Bytes.toBytes(ipStartNum));
        put.add(Bytes.toBytes("END_IP_NUM"), Bytes.toBytes(""), Bytes.toBytes(ipEndNum));
        Result lookupResult = getResult(locId);

        String country = IOUtil.getStringValue(lookupResult.raw(), "COUNTRY");
        String city = IOUtil.getStringValue(lookupResult.raw(), "CITY");
        String latitude = IOUtil.getStringValue(lookupResult.raw(), "LATITUDE");
        String longitude = IOUtil.getStringValue(lookupResult.raw(), "LONGITUDE");
/*
if (true) {
    throw new IOException("\nfoundLocId="+foundLocId+"\n"+
                          "country ="+country+"\n" +
                          "city ="+city+"\n" +
                          "latitude ="+latitude+"\n" +
                          "longitude ="+longitude+"\n" +
    );
}
*/

        put.add(Bytes.toBytes("COUNTRY"), Bytes.toBytes(""), Bytes.toBytes(country));
        if (city != null) {
            put.add(Bytes.toBytes("CITY"), Bytes.toBytes(""), Bytes.toBytes(city));
        }
        put.add(Bytes.toBytes("LATITUDE"), Bytes.toBytes(""), Bytes.toBytes(latitude));
        put.add(Bytes.toBytes("LONGITUDE"), Bytes.toBytes(""), Bytes.toBytes(longitude));

        try {
            context.write(outKey, put);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }


    private Result getResult(int locId) throws IOException {
        return cityLocations.get(new Get(Bytes.toBytes(locId)));
    }

    public static void main(String[] arg) {
        System.out.println(Bytes.toInt(Bytes.toBytes("\\x00\\x00\\x00\\x01")));

    }

}
