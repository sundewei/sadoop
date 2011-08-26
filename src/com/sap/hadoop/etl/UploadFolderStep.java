package com.sap.hadoop.etl;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 6/2/11
 * Time: 3:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class UploadFolderStep extends StepBase {
    private String remoteFolderName;
    private String localFolderName;
    private boolean replace;
    public UploadFolderStep(String stepName) {
        super(stepName);
    }

    public void setRemoteFolderName(String remoteFolderName) {
        this.remoteFolderName = remoteFolderName;
    }

    public void setLocalFolderName(String localFolderName) {
        this.localFolderName = localFolderName;
    }

    public void setReplace(boolean replace) {
        this.replace = replace;
    }

    public void run() {
        try {
            uploadFolder(remoteFolderName, localFolderName);
            this.successfullyFinished = true;
        } catch (Exception ioe) {
            this.successfullyFinished = false;
            this.hasErrorOrException = true;
            LOG.error(ioe);
            throw new RuntimeException(ioe);
        }
    }

    private void uploadFolder(String remoteFolder, String localFolder) throws Exception {
        if (!remoteFolder.endsWith("/")) {
            remoteFolder = remoteFolder + "/";
        }

        if (!localFolder.endsWith(File.separator)) {
            localFolder = localFolder + File.separator;
        }

        if (ifs.exists(remoteFolder)) {
            if (replace) {
                LOG.info("replace = " + replace);
                LOG.info("About to delete the existing " + remoteFolder);
                if(!ifs.deleteDirectory(remoteFolder)) {
                    throw new ETLStepContextException("Unable to delete '" + remoteFolder + "' when replace = true, can not proceed!");
                }
            } else {
                throw new ETLStepContextException(remoteFolder + " exists...but replace = " + false + ", can not proceed!");
            }
        }

        String remoteFolderParent = getParentFolder(remoteFolder);
        ifs.uploadFromLocalFile(localFolder, remoteFolderParent);
    }

    private static String getParentFolder(String remoteFolder) {
        StringBuilder sb = new StringBuilder(remoteFolder);
        sb.deleteCharAt(sb.length() - 1);
        return sb.substring(0, sb.lastIndexOf("/") + 1);
    }
}
