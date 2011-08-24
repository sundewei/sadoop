package com.sap.hadoop.etl;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Apr 1, 2011
 * Time: 2:30:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class ETLStepContextException extends Exception {
    public ETLStepContextException(String message) {
        super(message);
    }

    public ETLStepContextException(String message, Throwable cause) {
        super(message, cause);
    }

    public ETLStepContextException(Throwable cause) {
        super(cause);
    }
}
