package com.sap.hana;

import com.sap.utility.IOUtil;
import com.sap.utility.NameCollector;

import java.sql.*;
import java.io.BufferedReader;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 16, 2011
 * Time: 10:43:13 AM
 * To change this template use File | Settings | File Templates.
 */
public class HanaTask {
    private static Connection getConnection() throws Exception {
        Class.forName("com.sap.db.jdbc.Driver").newInstance();

        String url = "jdbc:sap://10.79.0.21:34015/I827779";
        //String url = "jdbc:sap://pmauhana.dhcp.syd.sap.corp:30015/I827779";

        //Connection conn = DriverManager.getConnection(url, "SYSTEM", "manager");
        return DriverManager.getConnection(url, "I827779", "Yahoo6377");
    }

    private static String getQuery() {
        return "SELECT blocks.LOC_ID, blocks.START_IP_NUM, blocks.END_IP_NUM, \n" +
                "       locs.COUNTRY, locs.CITY, locs.LATITUDE, locs.LONGITUDE  \n" +
                "FROM   I827779.CITY_IP_BLOCKS blocks  LEFT OUTER JOIN I827779.CITY_LOCATIONS locs   \n" +
                "          ON blocks.LOC_ID = locs.LOC_ID";

        /*
        return " SELECT blocks.LOC_ID, blocks.START_IP_NUM, blocks.END_IP_NUM, \n" +
                "       locs.COUNTRY, locs.CITY, locs.LATITUDE, locs.LONGITUDE  \n" +
                " FROM I827779.CITY_IP_BLOCKS blocks, \n" +
                "      I827779.CITY_LOCATIONS locs \n" +
                " WHERE blocks.LOC_ID = locs.LOC_ID \n" ;

        */

    }

    private static String getQuery2() {
        return  "select me.ID, f.FID\n" +
                "from   I827779.PERSON_FRIENDS me,\n" +
                "       I827779.PERSON_FRIENDS f\n" +
                "where me.ID = ? \n" +
                "and me.FID = f.ID\n" +
                "and f.FID not in ( select meagain.FID from I827779.PERSON_FRIENDS meagain where meagain.ID = ?)\n" +
                "and f.FID != me.ID";
    }

    private static void run() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        long startExecution = System.currentTimeMillis();
        ResultSet rs = stmt.executeQuery(getQuery());
        int rowCount = 0;
        long endExecution = 0;
        while (rs.next()) {
            if (rowCount == 0) {
                endExecution = System.currentTimeMillis();
                System.out.println("Took " + (endExecution - startExecution) + " ms to start looping the result set.");
            }
            String locID = rs.getString(1);
            rowCount++;
            if (rowCount % 100000 == 0) {
                System.out.println("rowCount = " + rowCount + ", locID =" + locID);
            }
        }
        long endLooping = System.currentTimeMillis();
        System.out.println("Took " + (endLooping - endExecution) + " ms to finish looping the result set.");
        rs.close();
        stmt.close();
        conn.close();
    }

    public static void run2() throws Exception {
        long start = System.currentTimeMillis();
        BufferedReader in = IOUtil.getBufferedReader("C:\\projects\\sadoop\\data\\Ids.csv");
        StringBuilder sb = new StringBuilder();
        Collection<String> ids = new ArrayList<String>();
        String line = in.readLine();
        while (line != null) {
            ids.add(line);
            line = in.readLine();
        }
        in.close();
        Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(getQuery2());
        int idLineCount = 1;
        Map<String, Integer> friendCount = new LinkedHashMap<String, Integer>();
        for (String id: ids) {

System.out.println(idLineCount+", Checking id:"+id+" with this query \n"+getQuery2());
            pstmt.setString(1, id);
            pstmt.setString(2, id);
            ResultSet rs = pstmt.executeQuery();            

            while (rs.next()) {
                String key = rs.getInt(1) + "===>" + rs.getInt(2);
                Integer nowCount = friendCount.get(key);
                if (nowCount == null) {
                    nowCount = new Integer(0);
                }
                nowCount = nowCount + 1;
                friendCount.put(key, nowCount);
            }
            rs.close();
            idLineCount++;
        }
        conn.close();
        pstmt.close();

        for (Map.Entry<String, Integer> entry: friendCount.entrySet()) {
            int fc = entry.getValue();
            if (fc > 1) {
                sb.append(entry.getKey()).append(", commonFriend: ").append(fc).append("\n");
            }
        }
        IOUtil.toFile(sb.toString(), "C:\\projects\\sadoop\\data\\possibleFriends.csv");
        long end = System.currentTimeMillis();
        System.out.println("Took "+ (end-start)+" ms.");
    }

    public static void main(String[] arg) throws Exception {
        run2();
        /*
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
                stmt.executeQuery(
                        " select distinct me.ID, f.FID\n" +
                        "from \tI827779.PERSON_FRIENDS me,\n" +
                        "\t\tI827779.PERSON_FRIENDS f\n" +
                        "where me.ID = 1\n" +
                        "and me.FID = f.ID\n" +
                        "and f.FID not in ( select meagain.FID from I827779.PERSON_FRIENDS meagain where meagain.ID = me.ID)\n" +
                        "and f.FID != me.ID");

        while (rs.next()) {
            int meId = rs.getInt(1);
            int meFid = rs.getInt(2);

            String key = meId + " ====> "+ meFid ;
            System.out.println(key);
        }
        rs.close();
        stmt.close();
        conn.close();
        */
    }
}
