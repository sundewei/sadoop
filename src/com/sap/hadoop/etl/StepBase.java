package com.sap.hadoop.etl;

import com.sap.hadoop.conf.IFileSystem;
import org.apache.log4j.Logger;

import java.sql.Connection;


/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Apr 1, 2011
 * Time: 9:59:24 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class StepBase implements Runnable {

    protected boolean successfullyFinished;
    protected IFileSystem ifs;
    protected Connection conn;
    protected boolean hasErrorOrException = false;

    protected static final Logger LOG = Logger.getLogger(StepBase.class.getName());

    private String stepName;

    public StepBase(String stepName) {
        this.stepName = stepName;
    }

    public String getStepName() {
        return stepName;
    }

    protected void setAttribute(Object object) {
        if (object instanceof IFileSystem) {
            ifs = (IFileSystem) object;
        } else if (object instanceof Connection) {
            conn = (Connection) object;
        }
    }

    protected void sleepQuite(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException iee) {
            LOG.warn(iee);
        }
    }

    public boolean hasErrorOrException() {
        return hasErrorOrException;
    }
}
