package com.sap.etl;

import java.util.Set;
import java.util.Collection;
import java.util.HashSet;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Mar 24, 2011
 * Time: 3:51:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class CsvDedupFilter implements ICsvLineFilter {
    Collection<String> distinctIds = new HashSet<String>();
    private int dedupColumnIndex;
    public CsvDedupFilter(int dedupColumnIndex) {
        this.dedupColumnIndex = dedupColumnIndex;
    }
    public boolean keepLine(String line) {
        String[] values = line.split(",");
        if (distinctIds.contains(values[dedupColumnIndex])) {
            return false;
        } else {
            distinctIds.add(values[dedupColumnIndex]);
            return true;
        }
    }

}
