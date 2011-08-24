package com.sap.hadoop.etl;

import org.apache.log4j.Logger;


/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Apr 4, 2011
 * Time: 2:44:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class SleepStep extends StepBase {

    private static final Logger LOG = Logger.getLogger(SleepStep.class.getName());

    public SleepStep(String stepName) {
        super(stepName);
    }

    public void run() {
        LOG.info("In " + this.getStepName() + " run()");
        int msToSleep = 5000;
        try {
            msToSleep = Integer.parseInt(this.getStepName());
        } catch (NumberFormatException ne) {
            LOG.info("Unable to parse the sleep duration from step name: " + this.getStepName());
        }

        try {
            Thread.sleep(msToSleep);
        } catch (InterruptedException ie) {
            LOG.info("About to sleep " + msToSleep + " ms");
        }
        LOG.info("Done sleeping " + msToSleep + " ms");
        this.successfullyFinished = true;
        this.hasErrorOrException = false;
    }
}
