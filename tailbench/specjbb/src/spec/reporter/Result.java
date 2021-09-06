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
import java.util.Vector;
import spec.reporter.Metrics;
import spec.reporter.Run;

public class Result {
    // This goes right after each class/interface statement
    static final String          COPYRIGHT     = "SPECjbb2005,"
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
    private static double        maxBarWidth   = 0;

    private static DecimalFormat memFormat     = new DecimalFormat("##.0");

    // /////////////////////////////////////
    // instance variable field declarations
    // /////////////////////////////////////
    private int                  best          = -1;

    private int                  digits        = 3;

    public StringBuffer          invalidReason = new StringBuffer();

    public String                name;

    // private DecimalFormat ratioFormat;
    public double                referenceTime;

    public Run[]                 run;

    public boolean               valid         = true;

    public double                weight;

    private int                  worst         = -1;

    private double               score;

    private int                  warehouses;

    public double                heapsize;

    public double                heapused;

    public double                min_thread_transactions;

    public double                max_thread_transactions;

    public double                elapsed;

    public double                meas_int;

    public double                expected_peak;

    public int                   jvm_instances;

    public double                ramp_up_int;

    public long                  start_rampup_time_milliseconds;

    public long                  end_rampdown_time_milliseconds;

    public long                  start_time_milliseconds;

    public long                  end_time_milliseconds;

    // /////////////////////////////////////
    // constructor declarations
    // /////////////////////////////////////
    public Result(String name, ReportProps props) {
        this.name = name;
        weight = 1;
        referenceTime = props.getDouble("config." + name + ".ref");
        if (maxBarWidth <= 0)
            maxBarWidth = props.getDouble("config.report.maxBarWidth");
        Vector<Run> rv = new Vector<Run>(8);
        score = props.getDouble("result." + name + ".company.score");
        warehouses = props.getInt("result." + name + ".warehouses");
        elapsed = props.getDouble("result." + name + ".elapsed_milliseconds");
        meas_int = props.getDouble("input.measurement_seconds");
        jvm_instances = props.getInt("input.jvm_instances");
        expected_peak = props.getDouble("input.expected_peak_warehouse");
        ramp_up_int = props.getDouble("input.ramp_up_seconds");
        heapsize = props.getDouble("result." + name + ".company.heapsize");
        heapused = props.getDouble("result." + name + ".company.heapused");
        if (jvm_instances > 1) {
            start_rampup_time_milliseconds = props.getLong("result." + name
                    + ".start_rampup_time_milliseconds");
            end_rampdown_time_milliseconds = props.getLong("result." + name
                    + ".end_rampdown_time_milliseconds");
            start_time_milliseconds = props.getLong("result." + name
                    + ".start_time_milliseconds");
            end_time_milliseconds = props.getLong("result." + name
                    + ".end_time_milliseconds");
        }
        min_thread_transactions = props.getDouble("result." + name
                + ".company.min_thread_transactions");
        max_thread_transactions = props.getDouble("result." + name
                + ".company.max_thread_transactions");
        Run r;
        for (int i = 0; i < 6; i++) {
            r = new Run(props, i, 17.0, this); // 17 is fake -- amt
            rv.addElement(r);
        }
        best = 2;
        worst = 1;
        run = new Run[rv.size()];
        if (run.length == 0) {
            valid = false;
            invalidReason.append(name + " was not run\n");
        }
        else {
            for (int i = 0; i < run.length; i++)
                run[i] = (Run) rv.elementAt(i);
        }
    }

    // /////////////////////////////////////
    // class method declarations
    // /////////////////////////////////////
    // /////////////////////////////////////
    // instance method declarations
    // /////////////////////////////////////
    public double score() {
        return score;
    }

    public int warehouses() {
        return warehouses;
    }

    public double elapsed() {
        return elapsed;
    }

    public double meas_int() {
        return meas_int;
    }

    public double ramp_up_int() {
        return ramp_up_int;
    }

    public double expected_peak() {
        return expected_peak;
    }

    public long rampup_millis() {
        return start_rampup_time_milliseconds;
    }

    public long rampdown_millis() {
        return end_rampdown_time_milliseconds;
    }

    public long start_millis() {
        return start_time_milliseconds;
    }

    public long end_millis() {
        return end_time_milliseconds;
    }

    public String toDetailRows() {
        StringBuffer buf = new StringBuffer("<TR>\n");
        if (run.length == 0) {
            buf.append("  <TD>" + name + "</TD>\n");
            buf.append("  <TD COLSPAN=8><I>not run</I></TD>\n");
            buf.append("</TABLE>");
        }
        else {
            // buf.append("<TABLE BORDER=1 CELLSPACING=2 CELLPADDING=10
            // WIDTH=200>");
            buf.append("  <TD VALIGN=TOP ALIGN=RIGHT ROWSPAN=" + run.length
                    + ">" + ((int) warehouses) + "</TD>\n");
            buf.append("  <TD VALIGN=TOP ALIGN=RIGHT ROWSPAN=" + run.length
                    + ">" + Metrics.format(score) + "</TD>\n");
            buf.append("  <TD VALIGN=TOP ALIGN=RIGHT ROWSPAN=" + run.length
                    + ">" + Metrics.format(heapsize) + "</TD>\n");
            buf.append("  <TD VALIGN=TOP ALIGN=RIGHT ROWSPAN=" + run.length
                    + ">" + Metrics.format(heapused) + "</TD>\n");
            buf
                    .append("  <TD VALIGN=TOP ALIGN=CENTER ROWSPAN="
                            + run.length
                            + ">"
                            + Metrics
                                    .format((100 * (max_thread_transactions - min_thread_transactions) / max_thread_transactions))
                            + "% </TD>\n");
            buf
                    .append("  <TD VALIGN=TOP ALIGN=RIGHT ROWSPAN="
                            + run.length
                            + ">"
                            + Metrics
                                    .format(0.1 * ((elapsed - 1000 * meas_int) / meas_int))
                            + "</TD>\n");
            buf.append(detailRow(0));
            buf.append("</TR>\n");
            for (int i = 1; i < run.length; i++) {
                buf.append("<TR>\n");
                buf.append(detailRow(i));
                buf.append("</TR>\n");
            }
            // buf.append ("</TABLE>");
        }
        return buf.toString();
    }

    private String detailRow(int n) {
        StringBuffer buf = new StringBuffer();
        buf.append("  <TD ALIGN=RIGHT>" + Run.transaction[n] + "</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>" + (run[n].count) + "</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>" + Metrics.format(run[n].totaltime)
                + "</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>" + Metrics.format(run[n].maximumtime)
                + "</TD>\n");
        return buf.toString();
    }

    public String toRow(int whAtPeak) {
        StringBuffer buf = new StringBuffer("<TR>\n");
        // buf.append (" <TD>" + name + "</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>" + ((int) warehouses()) + "</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>" + Metrics.format(score()) + "</TD>\n");
        if ((whAtPeak <= (int) warehouses())
                && ((int) warehouses() <= 2 * whAtPeak)) {
            buf.append("  <TD ALIGN=RIGHT> *  </TD>\n");
        }
        else {
            buf.append("  <TD ALIGN=RIGHT>    </TD>\n");
        }
        buf.append("</TR>\n");
        return buf.toString();
    }

    public String toFirstRow(int whAtPeak) {
        StringBuffer buf = new StringBuffer("<TR>\n");
        // buf.append (" <TD>" + name + "</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>" + ((int) warehouses()) + "</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>" + Metrics.format(score()) + "</TD>\n");
        if ((whAtPeak <= (int) warehouses())
                && ((int) warehouses() <= 2 * whAtPeak)) {
            buf.append("  <TD ALIGN=RIGHT> *  </TD>\n");
        }
        else {
            buf.append("  <TD ALIGN=RIGHT>  &nbsp   </TD>\n");
        }
        buf.append("  <TD ALIGN=RIGHT>  &nbsp </TD>\n");
        buf.append("  <TD ALIGN=RIGHT>  &nbsp   </TD>\n");
        buf.append("</TR>\n");
        return buf.toString();
    }

    public String shifted_toRow(int whAtPeak) {
        StringBuffer buf = new StringBuffer("<TR>\n");
        // buf.append (" <TD>" + name + "</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>" + ((int) warehouses()) + "</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>&nbsp</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>&nbsp</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>" + Metrics.format(score()) + "</TD>\n");
        if ((whAtPeak <= (int) warehouses())
                && ((int) warehouses() <= 2 * whAtPeak)) {
            buf.append("  <TD ALIGN=RIGHT> *  </TD>\n");
        }
        else {
            buf.append("  <TD ALIGN=RIGHT>  &nbsp   </TD>\n");
        }
        buf.append("</TR>\n");
        return buf.toString();
    }

    // 08/30/00 WRR -- v 1.01
    // Add metrics_2 as a parameter.
    public String TwoRow(Result result_2, Metrics metrics_2, int whAtPeak,
            int whAtPeak2) {
        StringBuffer buf = new StringBuffer("<TR>\n");
        buf.append("  <TD ALIGN=RIGHT>" + ((int) warehouses()) + "</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>" + Metrics.format(score()) + "</TD>\n");
        if ((whAtPeak <= (int) warehouses())
                && ((int) warehouses() <= 2 * whAtPeak)) {
            buf.append("  <TD ALIGN=RIGHT> *  </TD>\n");
        }
        else {
            buf.append("  <TD ALIGN=RIGHT>  &nbsp   </TD>\n");
        }
        buf.append("  <TD ALIGN=RIGHT>" +
        // 08/30/00 WRR -- v 1.01
                // And now use metrics_2.
                Metrics.format(metrics_2.score_at_wh((int) warehouses()))
                + "</TD>\n");
        if ((whAtPeak2 <= warehouses()) && (warehouses() <= 2 * whAtPeak2)) {
            buf.append("  <TD ALIGN=RIGHT> *  </TD>\n");
        }
        else {
            buf.append("  <TD ALIGN=RIGHT>   &nbsp  </TD>\n");
        }
        buf.append("</TR>\n");
        return buf.toString();
    }
}
