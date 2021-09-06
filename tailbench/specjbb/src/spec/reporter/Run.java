/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 2000-2005 Hewlett-Packard All rights reserved.
 * Copyright (c) 1997-2005 Sun Microsystems, Inc. All rights reserved.
 * 
 * This source code is provided as is, without any express or implied warranty.
 *  
 */
package spec.reporter;

import spec.reporter.Result;

public class Run {
    // This goes right after each class/interface statement
    static final String         COPYRIGHT     = "SPECjbb2005,"
                                                      + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                                      + "All rights reserved,"
                                                      + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                                      + "All rights reserved,"
                                                      + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                                      + "All rights reserved,"
                                                      + "Licensed Materials - Property of SPEC";

    // /////////////////////////////////////
    // class variable field declarations
    // /////////////////////////////////////
    private final static double MB            = 1024 * 1024;

    // /////////////////////////////////////
    // instance variable field declarations
    // /////////////////////////////////////
    public int                  count;

    public double               totaltime;

    public double               minimumtime;

    public double               maximumtime;

    public double               averagetime;

    public double               usedMemoryEnd;

    public double               usedMemoryStart;

    private ReportProps         props;

    public Result               result;

    public double               time;

    public double               totalMemoryEnd;

    public double               totalMemoryStart;

    public static String        transaction[] = {
            "new_order", "payment", "order_status", "delivery", "stock_level",
            "cust_report"
                                              };

    public Run(ReportProps props, int runNumber, double time, Result result) {
        this.props = props;
        this.time = time;
        this.result = result;
        String prefix = "result." + result.name + ".company."
                + transaction[runNumber];
        count = props.getInt(prefix + ".count");
        totaltime = props.getDouble(prefix + ".totaltime");
        minimumtime = props.getDouble(prefix + ".minimumtime");
        maximumtime = props.getDouble(prefix + ".maximumtime");
        averagetime = props.getDouble(prefix + ".averagetime");
    }

    public double ratio() {
        double ref = result.referenceTime;
        if (time <= 0)
            return 0;
        else
            return ref / time;
    }
}
