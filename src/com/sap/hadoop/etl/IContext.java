package com.sap.hadoop.etl;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 5/9/11
 * Time: 2:56 PM
 * To change this template use File | Settings | File Templates.
 */
public interface IContext {
    void addStep(StepBase step) throws Exception;

    void addStep(StepBase step, StepBase... dependency) throws Exception;

    String getRemoteWorkingFolder();

    void runSteps() throws ETLStepContextException, InterruptedException, Exception;
}
