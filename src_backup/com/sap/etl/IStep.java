package com.sap.etl;

import java.util.Properties;
import java.util.Collection;
import java.sql.ResultSet;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Mar 24, 2011
 * Time: 2:51:48 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IStep {
    public static final String OUTPUT_FILENAME = "outputFilename";
    public static final String OUTPUT_TABLENAME = "outputTablename";
    public static final String OUTPUT_SPECIFIED = "outputSpecified";

    public static final String INPUT_FILENAME = "inputFilename";
    public static final String INPUT_TABLENAME = "inputTablename";
    public static final String INPUT_SPECIFIED = "inputSpecified";

    public static final String CSV_DEDUP = "csvDedup";
    public static final String CSV_ORDERED = "csvOrdered";
    public static final String TABLE_INSERT_BATCH = "tableInsertBatch";
    public static final String DELETE_ON_EXIST = "deleteOnExist";
    public static final String CSV_SKIP_FIRST_LINE = "csvSkipFirstLine";

    public void setup(Properties p) throws IllegalArgumentException;
    public void runStep() throws Exception;
    public ResultSet runQueryStep() throws Exception;
}
