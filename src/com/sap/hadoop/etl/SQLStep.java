package com.sap.hadoop.etl;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Apr 4, 2011
 * Time: 3:36:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class SQLStep extends StepBase {

    private String sql;

    public SQLStep(String stepName) {
        super(stepName);
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void run() {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            LOG.info("\nAbout to run SQL: \n" + sql);
            this.successfullyFinished = stmt.execute(sql);
        } catch (SQLException sqle) {
            this.successfullyFinished = false;
            this.hasErrorOrException = true;
            RuntimeException re = new RuntimeException(sqle);
            re.setStackTrace(sqle.getStackTrace());
            sqle.printStackTrace();
            LOG.error(sqle);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException sqle) {
                LOG.error(sqle);
            }
        }
    }
}
