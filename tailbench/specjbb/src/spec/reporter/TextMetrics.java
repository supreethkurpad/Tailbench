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

import java.text.*;

public class TextMetrics extends Metrics {
    // This goes right after each class/interface statement
    static final String          COPYRIGHT = "SPECjbb2005,"
                                                   + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                                   + "All rights reserved,"
                                                   + "Copyright (c) 2005 Hewlett-Packard,"
                                                   + "All rights reserved,"
                                                   + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                                   + "All rights reserved,"
                                                   + "Licensed Materials - Property of SPEC";

    // /////////////////////////////////////
    // class variable field declarations
    // /////////////////////////////////////
    private static int           digits    = 3;

    private static DecimalFormat memFormat = new DecimalFormat("#0");

    public TextMetrics(ReportProps props, boolean SPECInternalUse) {
        super(props, SPECInternalUse);
    }

    public String wrap(String s) {
        return s + "\n";
    }

    public String detail() {
        StringBuffer buf = new StringBuffer();
        if (!valid) {
            buf.append("*** Invalid Result\n");
            buf
                    .append("This result is not valid for the following reason(s):\n");
            buf.append(this.invalid_buf.toString());
            buf.append("\n");
        }
        buf.append("\n* Details of Runs\n\n");
        TextBlock c1 = new TextBlock(5);
        TextBlock c2 = new TextBlock(8);
        TextBlock c3 = new TextBlock(9);
        TextBlock c4 = new TextBlock(9);
        TextBlock c5 = new TextBlock(8);
        TextBlock c6 = new TextBlock(8);
        TextBlock c7 = new TextBlock(12, "Trans.");
        TextBlock c8 = new TextBlock(10);
        TextBlock c9 = new TextBlock(6, "Resp.");
        TextBlock c10 = new TextBlock(8, "Time (s)");
        c1.addRight("Wh.");
        c1.add("");
        c2.addRight("Score");
        c2.add("");
        c3.addRight("Tot.");
        c3.addRight("Heap");
        c4.addRight("Used");
        c4.addRight("Heap");
        c5.addRight("Thread");
        c5.addRight("spread");
        c6.addRight("elapsed");
        c6.addRight("> 120s");
        c7.add("Type");
        c8.addRight("Count");
        c8.add("");
        c9.add("total");
        c10.add("max");
        c1.add("");
        c2.add("");
        c3.addRight("(MB)");
        c4.addRight("(MB)");
        c5.add("%");
        c6.add("%");
        c7.add("");
        c8.add("");
        c9.add("");
        c10.add("");
        for (int i = 0; i < result.length; i++) {
            Result r = result[i];
            if (r.run.length <= 0) {
                c3.add("not run");
                c4.add("");
                c5.add("");
                c6.add("");
                c7.add("");
                c8.add("");
                c9.add("");
                c10.add("");
            }
            else {
                for (int j = 0; j < r.run.length; j++) {
                    if (j > 0) {
                        c1.add("");
                        c2.add("");
                        c3.add("");
                        c4.add("");
                        c5.add("");
                        c6.add("");
                    }
                    else {
                        c1.addRight("" + (r.warehouses()));
                        c2.addRight(Metrics.format(r.score()));
                        c3.addRight(memFormat.format(r.heapsize));
                        c4.addRight(memFormat.format(r.heapused));
                        c5
                                .addRight(Metrics
                                        .format((100 * (r.max_thread_transactions - r.min_thread_transactions) / r.max_thread_transactions)));
                        c6
                                .addRight(Metrics.format(0.1
                                        * (r.elapsed - 1000 * r.meas_int)
                                        / r.meas_int));
                    }
                    c7.add(Run.transaction[j]);
                    c8.addRight(Metrics.format(r.run[j].count));
                    c9.add(Metrics.format(r.run[j].totaltime));
                    c10.add(Metrics.format(r.run[j].maximumtime));
                }
            }
        }
        buf.append(c1.join(c2, "").join(c3, "").join(c4, "").join(c5, "").join(
                c6, "").join(c7, " ").join(c8, "").join(c9, " ").join(c10, "")
                .toString());
        return buf.toString();
    }

    public String ratioTable() {
        StringBuffer buf = new StringBuffer();
        buf.append("\nValidity\n");
        String error_messages = check_all_buf.toString();
        // if (error_messages.equals(""))
        if (error_messages == null)
            buf.append("Valid run; no errors");
        else
            buf.append(error_messages);
        TextBlock messages = new TextBlock(119);
        TextBlock wh = new TextBlock(15);
        wh.addRight("Warehouses");
        TextBlock thrp = new TextBlock(20);
        thrp.addRight("Thrput");
        // messages.add("\n");
        messages.add("\n                   SPEC scores");
        for (int i = 0; i < result.length; i++) {
            if ((expectedPeakWh <= (result[i].warehouses()))
                    && ((result[i].warehouses()) <= 2 * expectedPeakWh))
                wh.addRight(" * " + (result[i].warehouses()));
            else
                wh.addRight("   " + (result[i].warehouses()));
            thrp.addRight(Metrics.format(result[i].score()));
        }
        wh.add("");
        thrp.add("");
        wh.add("Throughput");
        thrp.add(this.metric());
        messages.add(wh.join(thrp, " "));
        buf.append(messages.toString());
        return buf.toString();
    }
}
