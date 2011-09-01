package com.sap.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Mar 18, 2011
 * Time: 2:36:54 PM
 * To change this template use File | Settings | File Templates.
 */
public final class ConfigurationManager {

    private static String groupName = "lroot";

    private static String DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";

    private static final Logger LOG = Logger.getLogger(ConfigurationManager.class.getName());

    public static final String CORE_SITE_XML = "core-site.xml";
    public static final String CORE_SITE_DEFAULT_XML = "core-default.xml";

    public static final String HDFS_SITE_XML = "hdfs-site.xml";
    public static final String HDFS_SITE_DEFAULT_XML = "hdfs-default.xml";

    public static final String MAPRED_SITE_XML = "mapred-site.xml";
    public static final String MAPRED_SITE_DEFAULT_XML = "mapred-default.xml";

    public static final String HBASE_SITE_XML = "hbase-site.xml";

    public static final String SAP_XML = "sap.xml";

    private Configuration configuration;
    private boolean isInited = false;
    private String username;

    private static URL getResource(String name) throws IOException {
        LOG.info("Adding resource as " + name);
        return ConfigurationManager.class.getResource(name);
    }

    public Connection getConnection() throws SQLException {
        readyToGo();
        try {
            Class.forName(DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            LOG.error(e);
            throw new SQLException(e);
        }
        return DriverManager.getConnection("jdbc:hive://" + getNameNode() + ":10000/default", "", "");
    }

    public ConfigurationManager(String username, String password) {
        this.username = username;
        init(username, password);
    }

    public String getUsername() {
        return username;
    }

    public String getRemoteFolder() {
        return "/user/" + getUsername() + "/";
    }

    public Configuration getConfiguration() {
        return new Configuration(configuration);
    }

    private void init(String username, String password) {
        configuration = new Configuration();
        LOG.info("About to load config xml files...");
        try {
            configuration.addResource(getResource(CORE_SITE_DEFAULT_XML));
            configuration.addResource(getResource(HDFS_SITE_DEFAULT_XML));
            configuration.addResource(getResource(MAPRED_SITE_DEFAULT_XML));
            configuration.addResource(getResource(CORE_SITE_XML));
            configuration.addResource(getResource(HDFS_SITE_XML));
            configuration.addResource(getResource(MAPRED_SITE_XML));
            configuration.addResource(getResource(HBASE_SITE_XML));
            configuration.addResource(getResource(SAP_XML));
            configuration.set("hadoop.job.ugi", username + ", " + password);
            isInited = true;
        } catch (IOException ioe) {
            LOG.error(ioe);
            ioe.printStackTrace();
            throw new RuntimeException(ioe);
        }
        LOG.info("Done loading config xml files...");
        configuration.reloadConfiguration();
    }

    private String getNameNode() {
        String nameNode = configuration.get("fs.default.name");
        String prefix = "hdfs://";
        int prefixIdx = nameNode.indexOf(prefix);
        return nameNode.substring(prefixIdx + prefix.length(), nameNode.lastIndexOf(":"));
    }

    private void readyToGo() {
        if (!isInited) {
            throw new RuntimeException("ConfigurationManager has not been initialized!");
        }
    }

    public IFileSystem getFileSystem() throws Exception {
        readyToGo();
        return new DFSImpl(configuration);
    }

    public static void main(String[] args) throws Exception {
        ConfigurationManager cm = new ConfigurationManager(null, null);
        //System.out.println(cm.getNameNode());
    }
}
