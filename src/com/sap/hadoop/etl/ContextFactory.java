package com.sap.hadoop.etl;

import com.sap.hadoop.conf.ConfigurationManager;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: 5/9/11
 * Time: 3:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class ContextFactory {

    public static IContext createContext(ConfigurationManager cm) {
        return PoolContext.getInstance(cm);
    }
}
