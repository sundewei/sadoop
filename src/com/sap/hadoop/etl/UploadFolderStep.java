package com.sap.hadoop.etl;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

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

    public UploadFolderStep(String stepName) {
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

        boolean remoteFolderReady = false;
        remoteFolderReady = ifs.exists(remoteFolder);

        if (!remoteFolderReady) {
            remoteFolderReady = ifs.mkdirs(remoteFolder);
        }

        if (!remoteFolderReady) {
            throw new IOException(remoteFolder + " can not be created.");
        }

        File localFolderFile = new File(localFolder);
        File[] files = localFolderFile.listFiles();
        LOG.info("In " + localFolder + ", found " + files.length + " to upload.");
        for (File file : files) {
            String remoteFilename = remoteFolder + file.getAbsolutePath().substring(file.getAbsolutePath().indexOf(localFolder) + localFolder.length()).replaceAll("\\\\", "/");
            int lastSlashIdx = remoteFilename.lastIndexOf("/");
            String nowFolder = remoteFilename.substring(0, lastSlashIdx);
            if (file.isDirectory()) {
                uploadFolder(remoteFilename, file.getAbsolutePath());
            } else {
                if (!ifs.exists(nowFolder)) {
                    ifs.mkdirs(nowFolder);
                }
                LOG.info("Uploading " + file.getAbsolutePath() + " : " + file.length() + "bytes");
                FileInputStream in = new FileInputStream(file);
                OutputStream out = ifs.getOutputStream(remoteFilename);
                IOUtils.copyLarge(in, out);
                out.close();
                in.close();
            }
        }

        /*

        IFile[] files = ifs.listFiles(remoteFolder);
        for (IFile file: files) {
            String localFilename = localFolder + File.separator + file.getFilename().substring(file.getFilename().indexOf(remoteFolder) + remoteFolder.length() + 1);
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
        */
    }
}
