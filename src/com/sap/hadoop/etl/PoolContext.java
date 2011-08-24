package com.sap.hadoop.etl;

import com.sap.hadoop.conf.ConfigurationManager;
import com.sap.hadoop.conf.IFileSystem;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 8/3/11
 * Time: 12:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class PoolContext implements IContext {

    private static final Logger LOG = Logger.getLogger(PoolContext.class.getName());

    private ConfigurationManager configurationManager;

    private ExecutorService executor;

    private static int DEFAULT_NUM_THREADS;

    private Map<String, List<String>> dependencyMap;

    private Map<String, StepBase> threadNameMap;

    private Collection<String> submittedStepNames;

    private IFileSystem filesystem;

    // Prevent the default constructor to be called
    private PoolContext() {
    }

    static IContext getInstance(ConfigurationManager cm) {
        PoolContext context = new PoolContext();
        context.configurationManager = cm;
        DEFAULT_NUM_THREADS = cm.getConfiguration().getInt("com.sap.hadoop.etl.PoolContext.thread_count", 10);
        return context;
    }

    private void setAttribute(StepBase sb) throws Exception {
        sb.setAttribute(filesystem);
        if (sb instanceof SQLStep) {
            // Only pass the HiveConnection to SQL steps
            sb.setAttribute(configurationManager.getConnection());
        }
    }

    public void addStep(StepBase step) throws Exception {
        init();
        threadNameMap.put(step.getStepName(), step);
        dependencyMap.put(step.getStepName(), null);
        setAttribute(step);
    }

    public void addStep(StepBase step, StepBase... dependency) throws Exception {
        init();
        threadNameMap.put(step.getStepName(), step);
        List<String> dependencyNames = new ArrayList<String>(dependency.length);

        for (StepBase sb : dependency) {
            dependencyNames.add(sb.getStepName());
        }
        dependencyMap.put(step.getStepName(), dependencyNames);
        setAttribute(step);
    }

    private void init() throws Exception {
        if (executor == null) {
            executor = Executors.newFixedThreadPool(DEFAULT_NUM_THREADS);
        }

        if (dependencyMap == null) {
            dependencyMap = new HashMap<String, List<String>>();
        }

        if (threadNameMap == null) {
            threadNameMap = new HashMap<String, StepBase>();
        }

        if (submittedStepNames == null) {
            submittedStepNames = new HashSet<String>();
        }

        if (filesystem == null) {
            filesystem = configurationManager.getFileSystem();
        }
    }

    public String getRemoteWorkingFolder() {
        return configurationManager.getRemoteFolder();
    }

    private void validateSteps() throws ETLStepContextException {
        for (Map.Entry<String, List<String>> entry : dependencyMap.entrySet()) {
            if (entry != null && entry.getValue() != null) {
                for (String depStepName : entry.getValue()) {
                    if (threadNameMap.get(depStepName) == null) {
                        throw new ETLStepContextException("Unknown dependency \"" + depStepName + "\" for step \"" + entry.getKey() + "\"");
                    }

                    if (depStepName.equals(entry.getKey())) {
                        throw new ETLStepContextException("Step \"" + entry.getKey() + "\" can not depend on itself");
                    }
                }
            }
        }
    }

    public void runSteps() throws Exception {
        doInitBeforeRun();
        validateSteps();
        try {
            LOG.info("\n\nAbout to run " + threadNameMap.size() + " steps.");
            int loopCount = 0;
            while (canContinue()) {
                LOG.info("Loop #" + loopCount + ":");
                LOG.info("Totally there are " + threadNameMap.size() + " steps.");

                Collection<String> namesToRun = getRunnableStepNameStrings();
                for (String name : namesToRun) {
                    StepBase sb = threadNameMap.get(name);
                    LOG.info("Submitting '" + sb.getStepName() + "' to execution pool.");
                    executor.submit(sb);
                    submittedStepNames.add(sb.getStepName());
                }
                LOG.info("Total task submission so far: " + submittedStepNames.size());
                // Wait one second per loop
                Thread.sleep(1000);
                loopCount++;
            }
            LOG.info("Finished submitting all steps, now waiting for remaining steps to finish.");
            // Close the thread pool so it will not accpet new tasks
            executor.shutdown();

            // Wait for all remaining tasks to finish
            while (!executor.isTerminated()) {
                for (StepBase sb : threadNameMap.values()) {
                    if (!sb.successfullyFinished) {
                        LOG.info("'" + sb.getStepName() + "' is still running, waiting for it to finish.");
                    }
                }
                // Wait one second per loop
                Thread.sleep(1000);
            }
            LOG.info("...Execution completed!");
        } catch (Exception ee) {
            ETLStepContextException etle = new ETLStepContextException(ee);
            etle.setStackTrace(ee.getStackTrace());
            throw etle;
        } finally {
            shutdownAndAwaitTermination(executor);
        }
    }

    void shutdownAndAwaitTermination(ExecutorService pool) {
        // Disable new tasks from being submitted
        pool.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                pool.shutdownNow();
                // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(10, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    private Collection<String> getRunnableStepNameStrings() {
        Collection<String> stepNames = new HashSet<String>();
        for (Map.Entry<String, List<String>> entry : dependencyMap.entrySet()) {
            List<String> dependencies = entry.getValue();
            if ((dependencies == null || dependencies.size() == 0) && !submittedStepNames.contains(entry.getKey())) {
                stepNames.add(entry.getKey());
            } else {
                if (dependencies != null) {
                    boolean depAllDone = true;
                    for (String depName : dependencies) {
                        if (!threadNameMap.get(depName).successfullyFinished) {
                            depAllDone = false;
                        }
                    }
                    if (depAllDone && !submittedStepNames.contains(entry.getKey())) {
                        stepNames.add(entry.getKey());
                    }
                }
            }
        }
        return stepNames;
    }

    private boolean canContinue() throws ETLStepContextException {
        for (StepBase sb : threadNameMap.values()) {
            if (sb.hasErrorOrException()) {
                throw new ETLStepContextException("Found exception in '" + sb.getStepName() + "'");
            }
        }

        boolean canGoOn = false;
        for (StepBase sb : threadNameMap.values()) {
            if (!sb.successfullyFinished) {
                canGoOn = true;
            }
        }
        return canGoOn;
    }

    private boolean doInitBeforeRun() throws Exception {
        String remoteFolder = getRemoteWorkingFolder();
        if (!filesystem.exists(remoteFolder)) {
            if (!filesystem.mkdirs(remoteFolder)) {
                throw new ETLStepContextException("Unable to create remote work folder: " + remoteFolder);
            }
        }
        return false;
    }

    public static void main(String[] arg) throws Exception {
        ConfigurationManager cm = new ConfigurationManager("I827779", "hadoopsap");
        IContext context = ContextFactory.createContext(cm);

        UploadFolderStep uploadFolder = new UploadFolderStep("Upload a BIG Folder");
        uploadFolder.setLocalFolderName("c:\\data\\");
        uploadFolder.setRemoteFolderName(context.getRemoteWorkingFolder() + "data/");


        UploadStep uploadStep1 = new UploadStep("UploadCategory");
        uploadStep1.setLocalFilename("c:\\data\\small_category.tsv");
        uploadStep1.setRemoteFilename(context.getRemoteWorkingFolder() + "small_category.tsv");

        UploadStep uploadStep2 = new UploadStep("UploadSection");
        uploadStep2.setLocalFilename("c:\\data\\small_sections.tsv");
        uploadStep2.setRemoteFilename(context.getRemoteWorkingFolder() + "small_sections.tsv");


        ///////////////////////////////////////////////////////////////////////
        // <1> Now create "category" table
        ///////////////////////////////////////////////////////////////////////
        SQLStep createCategoryTable = new SQLStep("CREATE TABLE category");
        createCategoryTable.setSql(" CREATE EXTERNAL TABLE IF NOT EXISTS category " +
                " ( article_wpid INT, name STRING ) " +
                "   ROW FORMAT DELIMITED " +
                "   FIELDS TERMINATED BY '\t' " +
                "   LINES TERMINATED BY '\n'" +
                "   STORED AS TEXTFILE ");

        ///////////////////////////////////////////////////////////////////////
        // <2> Load the TSV to "category" table
        ///////////////////////////////////////////////////////////////////////
        SQLStep loadCategoryTable = new SQLStep("LOAD TABLE category");
        loadCategoryTable.setSql(" LOAD DATA INPATH '" + context.getRemoteWorkingFolder() + "small_category.tsv' " +
                " OVERWRITE INTO TABLE category");

        ///////////////////////////////////////////////////////////////////////
        // <3> Now create "sections" table
        ///////////////////////////////////////////////////////////////////////
        SQLStep createSectionsTable = new SQLStep("CREATE TABLE sections");
        createSectionsTable.setSql(" CREATE EXTERNAL TABLE IF NOT EXISTS sections " +
                " ( id BIGINT, " +
                "   parent_id BIGINT, " +
                "   ordinal INT, " +
                "   article_wpid INT, " +
                "   name STRING, " +
                "   xml STRING ) " +
                "   ROW FORMAT DELIMITED " +
                "   FIELDS TERMINATED BY '\t' " +
                "   LINES TERMINATED BY '\n'" +
                "   STORED AS TEXTFILE ");

        ///////////////////////////////////////////////////////////////////////
        // <4> Load the TSV to "sections" table
        ///////////////////////////////////////////////////////////////////////
        SQLStep loadSectionsTable = new SQLStep("LOAD TABLE sections");
        loadSectionsTable.setSql(" LOAD DATA INPATH '" + context.getRemoteWorkingFolder() + "small_sections.tsv' " +
                " OVERWRITE INTO TABLE sections");


        context.addStep(uploadStep1);
        context.addStep(uploadStep2);

        context.addStep(createCategoryTable, uploadStep1);                      // Add <1>
        context.addStep(createSectionsTable, uploadStep2);                      // Add <3>

        context.addStep(loadCategoryTable, createCategoryTable);   // Add <2> and make it depend on <1>
        context.addStep(loadSectionsTable, createSectionsTable);   // Add <4> and make it depend on <3>

        context.runSteps();
    }
}
