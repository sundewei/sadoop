package com.sap.etl;



import org.apache.log4j.Logger;

import java.util.Properties;

import java.sql.ResultSet;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Mar 24, 2011
 * Time: 2:54:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class BaseStepHelper implements IStep {

    private static final Logger LOG = Logger.getLogger(BaseStepHelper.class.getName());

    protected String tableName;
    protected String outFilename;
    String csvFilename;
    protected boolean dedup;
    protected boolean ordered;
    protected boolean insertIntoTable;
    protected int batch;
    boolean skipFirstLine;

    public void setup(Properties p) throws IllegalArgumentException {
        tableName = p.getProperty(OUTPUT_TABLENAME);
        outFilename = p.getProperty(OUTPUT_FILENAME);
        dedup = "true".equalsIgnoreCase(p.getProperty(CSV_DEDUP));
        ordered = "true".equalsIgnoreCase(p.getProperty(CSV_ORDERED));
        insertIntoTable = tableName != null;
        skipFirstLine = "true".equalsIgnoreCase(p.getProperty(CSV_SKIP_FIRST_LINE));
        csvFilename = p.getProperty(INPUT_FILENAME);
        try {
            batch = Integer.parseInt(p.getProperty(TABLE_INSERT_BATCH));
        } catch (Exception e) {
            LOG.info("Unable to parse '" + TABLE_INSERT_BATCH + "' from properties, using " + batch + " as default value", e);
        }
    }

    public void runStep() throws Exception {
        // do nothing
    }
    public ResultSet runQueryStep() throws Exception {
        // return null since this is just a helper class
        return null;
    }
}
