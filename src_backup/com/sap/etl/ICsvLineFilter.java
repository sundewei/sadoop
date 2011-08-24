package com.sap.etl;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Mar 24, 2011
 * Time: 3:49:06 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ICsvLineFilter {
    public boolean keepLine(String line);    
}
