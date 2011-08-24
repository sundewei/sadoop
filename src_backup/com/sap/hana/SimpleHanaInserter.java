package com.sap.hana;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.StringTokenizer;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Feb 27, 2011
 * Time: 5:44:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimpleHanaInserter {
    private static void loadData(String filename, String dbUsername, String dbPassword) throws Exception {
        Class.forName("com.sap.db.jdbc.Driver").newInstance();
        String url = "jdbc:sap://pmauhana.dhcp.syd.sap.corp:30015/I827779";

        Connection conn = DriverManager.getConnection(url, dbUsername, dbPassword);
        PreparedStatement stmt;
        conn.setAutoCommit(false);
        stmt = conn.prepareStatement(" insert into PERSON_FRIENDS values (?, ?) ");

        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
        String line = in.readLine();
        int count = 1;
        // skipped the first line since it's the column anmes
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
            if (count % 1000000 == 0) {
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

    public static void main(String[] args) throws Exception {
        loadData(args[0], args[1], args[2]);
    }
}
