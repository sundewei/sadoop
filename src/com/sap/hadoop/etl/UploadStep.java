package com.sap.hadoop.etl;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Apr 4, 2011
 * Time: 3:52:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class UploadStep extends StepBase {

    private String remoteFilename;
    private String localFilename;

    public UploadStep(String stepName) {
        super(stepName);
    }

    public void setRemoteFilename(String remoteFilename) {
        this.remoteFilename = remoteFilename;
    }

    public void setLocalFilename(String localFilename) {
        this.localFilename = localFilename;
    }

    public void run() {
        try {
            ifs.uploadFromLocalFile(remoteFilename, localFilename);
            this.successfullyFinished = true;
        } catch (Exception ioe) {
            this.successfullyFinished = false;
            this.hasErrorOrException = true;
            LOG.error(ioe);
            throw new RuntimeException(ioe);
        }
    }
}
