package com.sap.hadoop.etl;

import org.apache.commons.io.IOUtils;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 6/2/11
 * Time: 2:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class DownloadFileStep extends StepBase {

    private String remoteFilename;
    private String localFilename;

    public DownloadFileStep(String stepName) {
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
            InputStream in = ifs.getInputStream(remoteFilename);
            OutputStream out = new FileOutputStream(localFilename);
            IOUtils.copyLarge(in, out);
            in.close();
            out.close();
            this.successfullyFinished = true;
        } catch (Exception ioe) {
            this.successfullyFinished = false;
            LOG.error(ioe);
            throw new RuntimeException(ioe);
        }
    }
}
