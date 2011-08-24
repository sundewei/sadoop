package com.sap.hadoop.etl;

import com.sap.hadoop.conf.IFile;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 6/2/11
 * Time: 3:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class DownloadFolderStep extends StepBase {
    private String remoteFolderName;
    private String localFolderName;

    public DownloadFolderStep(String stepName) {
        super(stepName);
    }

    public void setRemoteFolderName(String remoteFolderName) {
        this.remoteFolderName = remoteFolderName;
    }

    public void setLocalFolderName(String localFolderName) {
        this.localFolderName = localFolderName;
    }

    public void run() {
        try {
            downloadFolder(remoteFolderName, localFolderName);
            this.successfullyFinished = true;
        } catch (Exception ioe) {
            this.successfullyFinished = false;
            LOG.error(ioe);
            throw new RuntimeException(ioe);
        }
    }

    private void downloadFolder(String remoteFolder, String localFolder) throws Exception {
        if (!remoteFolder.endsWith("/")) {
            remoteFolder = remoteFolder + "/";
        }

        if (!localFolder.endsWith(File.separator)) {
            localFolder = localFolder + File.separator;
        }

        File lFolder = new File(localFolder);
        boolean localFolderExist = lFolder.exists();
        if (!localFolderExist) {
            localFolderExist = lFolder.mkdirs();
        }

        if (!localFolderExist) {
            throw new IOException("Unable to create " + localFolder);
        }

        IFile[] files = ifs.listFiles(remoteFolder);
        for (IFile file : files) {
            String localFilename = localFolder + file.getFilename().substring(file.getFilename().indexOf(remoteFolder) + remoteFolder.length());
            System.out.println("\n\n" + file.getFilename() + ", is Dir = " + file.isDir());
            System.out.println(localFilename);
            if (file.isDir()) {
                downloadFolder(file.getFilename(), localFolder);
            } else {
                File localFile = new File(localFilename);
                File nowFolder = new File(localFile.getParent());
                if (!nowFolder.exists()) {
                    nowFolder.mkdirs();
                }
                FileOutputStream out = new FileOutputStream(localFilename);
                InputStream in = ifs.getInputStream(file.getFilename());
                IOUtils.copyLarge(in, out);
                out.close();
                in.close();
            }
        }
    }
}
