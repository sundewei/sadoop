package com.sap.etl;

import com.sap.io.IFileSystem;
import com.sap.io.FileSystemImpl;
import com.sap.io.IOManager;
import com.sap.hadoop.conf.ConfigManager;
import com.sap.hadoop.hive.ConnectionManager;

import java.util.*;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import org.apache.log4j.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Mar 24, 2011
 * Time: 2:54:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class CsvReadStep extends BaseStepHelper {

    private static final Logger LOG = Logger.getLogger(CsvReadStep.class.getName());

    private Properties prop;

    private ICsvLineFilter filter;

    public void setup(Properties p) throws IllegalArgumentException {
        super.setup(p);
        prop = p;
        if (prop.getProperty(OUTPUT_FILENAME) == null && prop.getProperty(OUTPUT_TABLENAME) == null
                && prop.getProperty(OUTPUT_SPECIFIED) == null) {
            throw new IllegalArgumentException("Data destination is needed");
        }
        if (prop.getProperty(INPUT_FILENAME) == null) {
            throw new IllegalArgumentException("Input CSV file name is needed");
        }
    }

    @Override
    public void runStep() throws Exception {
        if (outFilename == null && tableName == null) {
            throw new IllegalArgumentException("runStep() can not be called whithout OUTPUT_FILENAME or OUTPUT_TABLENAME specified");
        }

        IFileSystem fs = IOManager.getFileSystem();
        BufferedReader in = new BufferedReader(new InputStreamReader(fs.getInputStream(csvFilename)));
        Collection<String> lines = createNewCollection(dedup, ordered);
        // The line in csv file
        String line = in.readLine();
        // skip the first line
        if (skipFirstLine) {
            line = in.readLine();
        }
        
        while (line != null) {
            if (filter != null && filter.keepLine(line)) {
                lines.add(line);
            }

            boolean processed = false;
            if (lines.size() > 0 && lines.size() % batch == 0) {
                if (insertIntoTable) {
                    processLines(ConnectionManager.getConnection(), tableName, lines);
                } else {
                    processLines(outFilename, lines);
                }
                lines = createNewCollection(dedup, ordered);
                processed = true;
            }
            line = in.readLine();
            if (line == null && !processed) {
                if (insertIntoTable) {
                    processLines(ConnectionManager.getConnection(), tableName, lines);
                } else {
                    processLines(outFilename, lines);
                }
            }
        }
    }

    private Collection<String> createNewCollection(boolean dedup, boolean ordered) {
        if (dedup && !ordered) {
            return new HashSet<String>();
        } else if (dedup && ordered) {
            return new LinkedHashSet<String>();
        } else {
            return new ArrayList<String>();
        }
    }

    private void processLines(Connection conn, String tableName, Collection<String> lines) throws SQLException {
        Iterator<String> lineIterator = lines.iterator();
        String firstLine = lineIterator.next();
        String[] values = firstLine.split(",");
        conn.setAutoCommit(false);
        PreparedStatement pstmt = conn.prepareStatement(getInsertQuery(tableName, values.length));

        for (String line: lines) {
            values = line.split(",");
            for (int i = 0; i < values.length; i++) {
                pstmt.setString((i + 1), values[i]);
            }
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        pstmt.close();
        conn.commit();
        conn.setAutoCommit(true);
    }

    private void processLines(String filename, Collection<String> lines) throws IOException {
        BufferedWriter out =
                new BufferedWriter(new OutputStreamWriter(IOManager.getFileSystem().getOutputStream(filename, false)));
        for (String line: lines) {
            out.write(line);
            out.write("\n");
        }
        out.close();
    }

    private String getInsertQuery(String tableName, int columns) {
        StringBuilder sb = new StringBuilder();
        sb.append(" INSERT INTO ").append(tableName).append(" VALUES ");
        sb.append(" ( ");

        for (int i = 0; i < columns; i++) {
            sb.append(" ?,");
        }

        if (sb.charAt(sb.length()-1) == ',') {
            sb.deleteCharAt(sb.length()-1);
        }

        sb.append(" ) ");
        return sb.toString();
    }

    public void setCsvLineFilter(ICsvLineFilter filter) {
        this.filter = filter;
    }
}
