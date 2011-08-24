package com.sap.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 5/4/11
 * Time: 10:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class DFSImpl implements IFileSystem {

    private FileSystem fileSystem;

    private Configuration hadoopConfiguration;

    private String ownerName;
    private String password;

    protected static final FsPermission FS_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.NONE);

    private static final Logger LOG = Logger.getLogger(IFileSystem.class.getName());

    DFSImpl(Configuration conf) throws Exception {
        hadoopConfiguration = conf;
        String ugiString = hadoopConfiguration.get("hadoop.job.ugi");
        if (ugiString == null || ugiString.length() == 0 || ugiString.split(",").length != 2) {
            throw new Exception("No \"hadoop.job.ugi\" defined, unable to get IFileSystem object.");
        }
        String[] ugiArray = ugiString.split(",");
        ownerName = ugiArray[0];
        password = ugiArray[1];
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(ownerName);
        ugi.doAs(
                new PrivilegedExceptionAction<Void>() {
                    public Void run() throws Exception {
                        //OR access hdfs
                        fileSystem = FileSystem.get(hadoopConfiguration);
                        return null;
                    }
                }
        );
    }

    public boolean deleteFile(String filename) throws IOException {
        return fileSystem.delete(new Path(filename), true);
    }

    public boolean mkdirs(String foldername) throws IOException {
        Path folder = new Path(foldername);
        if (fileSystem.exists(folder)) {
            return true;
        } else {
            boolean createOk = fileSystem.mkdirs(folder, FS_PERMISSION);
            if (ownerName != null) {
                fileSystem.setOwner(folder, ownerName, ownerName);
            }
            return createOk;
        }
    }

    public boolean deleteDirectory(String foldername) throws IOException {
        Path folder = new Path(foldername);
        return fileSystem.delete(folder, true);
    }

    public void copyFromLocalFile(final String from, final String to) throws Exception {
        fileSystem.copyFromLocalFile(new Path(from), new Path(to));
    }

    public boolean exists(String filename) throws IOException {
        return fileSystem.exists(new Path(filename));
    }

    public void uploadFromLocalFile(String remoteFilename, String localFilename) throws IOException {
        fileSystem.copyFromLocalFile(new Path(localFilename), new Path(remoteFilename));
    }

    public IFile[] listFiles(String folder) throws IOException {
        FileStatus[] fss = fileSystem.listStatus(new Path(folder));
        List<IFile> files = new ArrayList<IFile>();
        if (fss != null) {
            for (FileStatus fs : fss) {
                IFile file = new FileImpl(fs.getPath().getName(),
                        fs.getOwner(),
                        fs.getModificationTime(),
                        fs.getLen(),
                        fs.getPath().toUri().toASCIIString());
                if (fs.isDir()) {
                    ((FileImpl) file).setDir(true);
                    IFile[] subFiles = listFiles(fs.getPath().toUri().toASCIIString());
                    files.addAll(Arrays.asList(subFiles));
                } else {
                    files.add(file);
                }
            }
        } else {
            throw new IOException("Folder '" + folder + "' not found on HDFS");
        }
        return files.toArray(new IFile[files.size()]);
    }

    public long getSize(String filename) throws IOException {
        return fileSystem.getFileStatus(new Path(filename)).getLen();
    }

    public InputStream getInputStream(String remoteFile) throws IOException {
        return fileSystem.open(new Path(remoteFile));
    }

    public OutputStream getOutputStream(String remoteFile) throws IOException {
        return fileSystem.create(new Path(remoteFile));
    }
    /*
    public static void main(String[] arg) throws Exception {
        ConfigurationManager cm = new ConfigurationManager("I123456", "hadoopsap");
        IContext context = ContextFactory.createContext(cm);
        DFSImpl dfs = new DFSImpl(cm.getConfiguration());
        dfs.copyFromLocalFile("c:\\temp\\small_category.tsv", "/user/I123456/small_category.tsv");
    }
    */
}
