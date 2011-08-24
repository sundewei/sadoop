package com.sap.hana;

import com.sap.utility.IOUtil;
import com.sap.utility.NameCollector;

import java.io.BufferedReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.StringTokenizer;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 11, 2011
 * Time: 11:25:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class HanaInserter {
    public static void load(String[] arg) throws Exception {
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        //String url = "jdbc:sap://10.79.0.21:34015/I827779";
        String url = "jdbc:sap://pmauhana.dhcp.syd.sap.corp:30015/I827779";
        //Connection conn = DriverManager.getConnection(url, "SYSTEM", "manager");
        Connection conn = DriverManager.getConnection(url, "I827779", "Yahoo6377");
        PreparedStatement stmt;
        //PreparedStatement stmt = conn.prepareStatement(" delete from CITY_LOCATION");
        //stmt.executeUpdate();

        conn.setAutoCommit(false);
        //stmt = conn.prepareStatement(" insert into CITY_LOCATIONS values (?, ?, ?, ?, ?,  ?, ?, ?, ?) ");
        stmt = conn.prepareStatement(" insert into CITY_IP_BLOCKS values (?, ?, ?) ");
        //stmt = conn.prepareStatement(" INSERT INTO REPOTEXT values (?,?,?,?,?,?,?) ");
        BufferedReader in = IOUtil.getBufferedReader(arg[0]);
        // Skip thh fist 2 lines
        String line = in.readLine();
        int lineCount = 1;
        SimpleDateFormat formatter5 = new SimpleDateFormat("dd.MM.yyyy");
        line = in.readLine();
        while (line != null) {
//System.out.println("Working on line: "+line);
            String[] values = line.split(",", -1);
            int i = 1;
            for (String value : values) {
                String cleanedValue = value.trim().replaceAll("\"", "");
                if (cleanedValue.length() == 0) {
                    cleanedValue = null;
                }
                /*
                if (i == 5) {
                    if (cleanedValue == null) {
                        stmt.setDate(i, null);
                    } else {
                        stmt.setDate(i, new java.sql.Date(formatter5.parse(cleanedValue).getTime()));
                    }
                } else if (i == 6) {
                    if (cleanedValue == null) {
                        stmt.setTime(i, null);
                    } else {
                        java.sql.Time jsqlT = java.sql.Time.valueOf(cleanedValue);
                        stmt.setTime(i, jsqlT);
                    }
                } else if (i == 7) {
                    if (cleanedValue == null) {
                        stmt.setString(i, null);
                    } else {
                        stmt.setFloat(i, Float.parseFloat(cleanedValue));
                    }
                } else*/
                {
                    stmt.setString(i, cleanedValue);
                }
                i++;
            }
            stmt.addBatch();
            boolean doneBatch = false;

            if (lineCount % 1000000 == 0) {
                System.out.println("lineCount=" + lineCount);
                stmt.executeBatch();
                doneBatch = true;
                conn.commit();
                System.out.println("Commit add batch...:" + lineCount);
            }
            line = in.readLine();
            lineCount++;
            if (line == null && !doneBatch) {
                System.out.println("lineCount=" + lineCount);
                stmt.executeBatch();
                conn.commit();
                System.out.println("Commit add batch...:" + lineCount);
            }
        }
        /*
        ResultSet rsSchema = conn.getMetaData().getSchemas();
        int columnCount = rsSchema.getMetaData().getColumnCount();
        for (int i=0; i<columnCount; i++) {
            System.out.print(rsSchema.getMetaData().getColumnName(i+1));
            System.out.print(",");
        }
        System.out.println("\n");
        while (rsSchema.next()) {
            for (int i=0; i<columnCount; i++) {
                System.out.print(rsSchema.getString(i+1));
                System.out.print(",");
            }
            System.out.println("\n");
        }
        */
        in.close();
        stmt.close();
        conn.close();
        //System.out.println("conn.getMetaData().getDatabaseProductName()="+conn.getMetaData().getDatabaseProductName());
        //conn.close();
    }

    public static void loadName() throws Exception {
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        //String url = "jdbc:sap://pmauhana.dhcp.syd.sap.corp:30015/I827779";
        String url = "jdbc:sap://10.79.0.21:34015/I827779";
        Connection conn = DriverManager.getConnection(url, "I827779", "Yahoo6377");
        PreparedStatement stmt;
        conn.setAutoCommit(false);
        stmt = conn.prepareStatement(" insert into People values (?, ?) ");

        Collection<String[]> names = NameCollector.getIdNameCollection();

        for (String[] name : names) {
            stmt.setString(1, name[0]);
            stmt.setString(2, name[1]);
            stmt.addBatch();
        }
        stmt.executeBatch();
        conn.commit();
        stmt.close();
        conn.close();
    }


    public void loadFriends(String filename) throws Exception {
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        //String url = "jdbc:sap://pmauhana.dhcp.syd.sap.corp:30015/I827779";
        String url = "jdbc:sap://10.79.0.21:34015/I827779";
        Connection conn = DriverManager.getConnection(url, "I827779", "Yahoo6377");
        PreparedStatement stmt;
        conn.setAutoCommit(false);
        stmt = conn.prepareStatement(" insert into  PERSON_FRIENDS values (?, ?) ");

        BufferedReader in = IOUtil.getBufferedReader(filename);

        String line = in.readLine();

        int count = 1;
        // skipped the first line
        line = in.readLine();
        long start = System.currentTimeMillis();
        while (line != null) {
            StringTokenizer st = new StringTokenizer(line, ",");
            String id1 = st.nextToken();
            String id2 = st.nextToken();
            stmt.setString(1, id1);
            stmt.setString(2, id2);
            stmt.addBatch();

            boolean hasUpdated = false;
            if (count % 100000 == 0) {
                stmt.executeBatch();
                conn.commit();
                System.out.println("Commited line at " + count + " and took " + (System.currentTimeMillis() - start) / 1000 + " seconds");
                start = System.currentTimeMillis();
                hasUpdated = true;
            }
            line = in.readLine();

            if (line == null && !hasUpdated) {
                stmt.executeBatch();
                conn.commit();
                System.out.println("Last line, Commited line at " + count);
            }
            count++;
        }
        stmt.close();
        conn.close();
        in.close();
    }

    static class InsertThread extends Thread {
        String filename;
        public InsertThread(String filename) {
            this.filename = filename;
        }
        public void run() {
            HanaInserter hi = new HanaInserter();
            try {
                hi.loadFriends(filename);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] arg) throws Exception {
        //InsertThread it1 = new InsertThread("C:\\projects\\sadoop\\data\\person_friends_1m-200-20.csv_1");
        //InsertThread it2 = new InsertThread("C:\\projects\\sadoop\\data\\person_friends_1m-200-20.csv_2");
        //InsertThread it3 = new InsertThread("C:\\projects\\sadoop\\data\\person_friends_1m-200-20.csv_3");
        //InsertThread it4 = new InsertThread("C:\\projects\\sadoop\\data\\person_friends_1m-200-20.csv_4");
        //InsertThread it5 = new InsertThread("C:\\projects\\sadoop\\data\\person_friends_1m-200-20.csv_5");
        InsertThread it6 = new InsertThread("C:\\projects\\sadoop\\data\\person_friends_1m-200-20.csv_6");
        InsertThread it7 = new InsertThread("C:\\projects\\sadoop\\data\\person_friends_1m-200-20.csv_7");
        //InsertThread it8 = new InsertThread("C:\\projects\\sadoop\\data\\person_friends_1m-200-20.csv_8");
        //InsertThread it9 = new InsertThread("C:\\projects\\sadoop\\data\\person_friends_1m-200-20.csv_9");
        //InsertThread it10 = new InsertThread("C:\\projects\\sadoop\\data\\person_friends_1m-200-20.csv_10");
        //it1.start();
        //it2.start();
        //it3.start();
        //it4.start();
        //it5.start();
        it6.start();
        it7.start();
        //it8.start();
        //it9.start();
        //it10.start();
    }
}

