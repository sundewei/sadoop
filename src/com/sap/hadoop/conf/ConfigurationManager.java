package com.sap.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Mar 18, 2011
 * Time: 2:36:54 PM
 * To change this template use File | Settings | File Templates.
 */
public final class ConfigurationManager {

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

    private static String CONF_URL;

    private Configuration configuration;

    private static Configuration SAVED_CONFIGURATION;
    private static long SAVED_TIME;

    private boolean isInited = false;
    private String username;

    private URL getResource(String name) throws IOException {
        if (name.equals(SAP_XML)) {
            LOG.info("Adding resource as " + name);
            return ConfigurationManager.class.getResource(name);
        }

        if (CONF_URL == null) {
            CONF_URL =
                    getConfiguration().get("com.sap.hadoop.conf.ConfigurationManager.confUrl",
                                           "http://llnpal55:80/shc/conf/");
        }
        String urlAddress = CONF_URL + name;
        LOG.info("Adding resource as " + urlAddress);
        return new URL(urlAddress);
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
        configuration = getSavedConfiguration();
        try {
            if (configuration == null) {
                LOG.info("About to load config xml files...");
                configuration = new Configuration();
                configuration.addResource(getResource(SAP_XML));
                configuration.addResource(getResource(CORE_SITE_DEFAULT_XML));
                configuration.addResource(getResource(HDFS_SITE_DEFAULT_XML));
                configuration.addResource(getResource(MAPRED_SITE_DEFAULT_XML));
                configuration.addResource(getResource(CORE_SITE_XML));
                configuration.addResource(getResource(HDFS_SITE_XML));
                configuration.addResource(getResource(MAPRED_SITE_XML));
                configuration.addResource(getResource(HBASE_SITE_XML));
                configuration.set("hadoop.job.ugi", username + ", " + password);
                LOG.info("Done loading config xml files, saving it now...");
                saveConfiguration(configuration);
            } else {
                LOG.info("Using saved Configuration loaded at " + new Timestamp(SAVED_TIME));
            }
            isInited = true;
        } catch (IOException ioe) {
            LOG.error(ioe);
            ioe.printStackTrace();
            throw new RuntimeException(ioe);
        }

        configuration.reloadConfiguration();
    }

    private synchronized static Configuration getSavedConfiguration() {
        // One hour
        if (SAVED_CONFIGURATION != null && System.currentTimeMillis() - SAVED_TIME < 3600000) {
            return SAVED_CONFIGURATION;
        } else {
            return null;
        }
    }

    private synchronized static void saveConfiguration(Configuration configuration) {
        SAVED_CONFIGURATION = configuration;
        SAVED_TIME = System.currentTimeMillis();
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
        ConfigurationManager cm1 = new ConfigurationManager(null, null);
        ConfigurationManager cm2 = new ConfigurationManager(null, null);
        ConfigurationManager cm3 = new ConfigurationManager(null, null);
        Thread.sleep(5000);
        ConfigurationManager cm4 = new ConfigurationManager(null, null);
        ConfigurationManager cm5 = new ConfigurationManager(null, null);
    }
}
