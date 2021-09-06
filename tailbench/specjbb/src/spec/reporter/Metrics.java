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

import java.awt.*;
import java.io.*;
import java.util.Vector;
import java.util.Random;
import java.util.logging.Level;
import spec.jbb.JBButil;

public class Metrics {
    // This goes right after each class/interface statement
    static final String         COPYRIGHT             = "SPECjbb2005,"
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
    private static int          maxBarWidth           = 0;

    private static int          digits                = 3;

    private final static int    widthAdj1             = 80;                                             // SPEC

    // ratio
    // graph
    private final static int    widthAdj2             = -80;                                            // memory

    // utilization
    // graph
    private final static int    MAXINT                = 2000000000;

    private static final String IMAGE_PIX             = "pix.jpg";

    // 06/28/00 WRR -- v 1.01
    // Another pixel color for drawing distinguished lines.
    private static final String IMAGE_PIX_2           = "pix2.jpg";

    private static final String IMAGE_INVALID         = "invalid.gif";

    private static final String IMAGE_RESOURCE_PATH   = "images/";

    // /////////////////////////////////////
    // instance variable field declarations
    // /////////////////////////////////////
    public double               highestScore;

    public int                  maxWarehouses;

    public int                  measuredPeakWarehouse = 0;

    public int                  expectedPeakWh        = 0;

    public int                  jvm_instances         = 1;

    public Result               check;

    public String[]             name;

    public ReportProps          props;

    public Result[]             result;

    public double               score_at[];

    private long                rampup_millis[];

    private long                rampdown_millis[];

    private long                start_millis[];

    private long                end_millis[];

    public int                  maximum_warehouses;

    private int                 warehousesTested      = 0;

    public boolean              valid;

    public boolean              print_score;

    // 06/28/00 WRR -- v 1.01
    // New parameter allowing special mode.
    public boolean              SPECInternalUse;

    private String              o_directory;

    public StringBuffer         buf;

    public StringBuffer         invalid_buf;

    public StringBuffer         results_buf;

    public StringBuffer         check_all_buf;

    // New parameters for checking instance timing in multi-jvm mode
    // /////////////////////////////////////
    // constructor declarations
    // /////////////////////////////////////
    // 06/28/00 WRR -- v 1.01
    // Add new parameter for special internal mode.
    public Metrics(ReportProps props, boolean SPECInternalUse) {
        // String s = props.get ("spec.initial.cbase");
        this.props = props;
        // 06/28/00 WRR -- v 1.01
        // Save value of new parameter.
        this.SPECInternalUse = SPECInternalUse;
        // 06/28/00 WRR -- v 1.01
        // Initialize validity Boolean; it will be set to false if necessary.
        this.valid = true;
        this.buf = new StringBuffer();
        this.invalid_buf = new StringBuffer();
        this.results_buf = new StringBuffer();
        name = benchmarkList(props);
        result = new Result[name.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new Result(name[i], props);
        }
        String val = (String) props.get("input.jvm_instances");
        jvm_instances = Integer.parseInt(val.trim());
        warehousesTested = name.length;
        create_warehouse_array();
        calculate();
        check_all_buf = new StringBuffer();
        check_all_buf.append(check_all());
    }

    public String wrap(String s) {
        return ("<h1>" + s + "</h1>");
    }

    public String check_all() {
        // check various validation checks
        buf = new StringBuffer();
        String val =  props.get("result.validity.200_check");
        if (val != null) {
            // 06/28/00 WRR -- v 1.01
            // Removed logic that set this.valid to true since that would
            // override previous error discovery that had set this.valid
            // to false.
            if (!val.equals("true")) {
                this.valid = false;
            }
        }
        else {
            // 06/28/00 WRR -- v 1.01
            // Indicate error if no result has been recorded for 200_check;
            // this used to be set to true to accommodate running against
            // old versions of result files. Now that we're official this
            // is not a valid thing to do.
            this.valid = false;
        }
        this.print_score = true;
        if (!this.valid) {
            output_properly("INVALID because conformance checking returned negative.\n");
        }
        val = props.get("result.validity.999_checkit");
        if (val != null) {
            if (val.equals("false")) {
                this.valid = false;
                output_properly("INVALID because benchmark recompiled or jbb.jar not first in CLASSPATH.");
            }
        }
        boolean checkit;
        val = props.get("result.validity.jbb_operation");
        if (val != null) {
            if (val.equals("true")) {
                checkit = true;
            }
            else {
                checkit = false;
            }
        }
        else {
            checkit = true;
        }
        this.valid = this.valid && checkit;
        if (!checkit) {
            String s = "INVALID: JVM failed operational validity check.\n";
            output_properly(s);
        }
        boolean elapsedOK = checkElapsed();
        if (!elapsedOK) {
            String se = "INVALID: measurement interval failing to end in close enough time limits.";
            output_properly(se);
            this.valid = false;
        }
        // elian111004 - thread spread requirements fix
        /*
         * boolean threadSpread = checkThreadSpread(); if ( ! threadSpread) {
         * String s = "INVALID: thread starvation > 30%; see detail chart";
         * output_properly(s); this.valid = false; }
         */
        boolean points_before_metric = check_points_before_metric();
        // 06/28/00 WRR -- v 1.01
        // Enabled this check (was commented out); also changed error wording.
        if (!points_before_metric) {
            output_properly("INVALID: all points up to the peak are required");
            this.valid = false;
        }
        if (maximum_warehouses < 8) {
            String s8 = "INVALID: at least the points from 1 to 8 are needed for a publishable run.";
            output_properly(s8);
            // 06/28/00 WRR -- v 1.01
            // Flag this condition as unacceptable.
            this.valid = false;
        }
        //if (measuredPeakWarehouse < expectedPeakWh) {
        //    String s9 = "INVALID: measured peak warehouse must be >= expected_peak_warehouse.";
        //    output_properly(s9);
        //    this.valid = false;
        //}
        boolean points_for_metric = check_points_for_metric();
        /*
         * if ( ! points_for_metric) { output_properly("Warning: 0's will be
         * averaged in for points not measured."); }
         */
        // 06/28/00 WRR -- v 1.01
        // If it's special mode (SPECInternalUse) don't report it as valid.
        if (this.valid && !SPECInternalUse) {
            output_properly("No errors.  Valid run.");
        }
        return buf.toString();
    }

    public void output_properly(String s) {
        buf.append(this.wrap(s));
        this.invalid_buf.append(this.wrap(s));
        this.results_buf.append(s + "\n");
    }

    // /////////////////////////////////////
    // class method declarations
    // /////////////////////////////////////
    public int get_whAtPeak() {
        return expectedPeakWh;
    }

    public double getHighestScore() {
        return highestScore;
    }

    public int getWarehousesTested() {
        return warehousesTested;
    }

    // 08/30/00 WRR -- v 1.01
    // A new number formatter for scores.
    // AMT v1.01
    // wanted to use format for scores, but "< 0.01" looked funny
    /*
     * Simplistic number formatter to avoid bug in Linux JDK with NumberFormat
     */
    public static String score_format(double x) {
        int n;
        String s;
        String formatted;
        try {
            if (x < 0.01) {
                formatted = "0";
            }
            else if (x < 0.1) {
                n = (int) (1000 * x + 0.5);
                s = Integer.toString(n);
                formatted = ".0" + s;
            }
            else if (x < 1) {
                n = (int) (1000 * x + 0.5);
                s = Integer.toString(n);
                formatted = "." + s;
            }
            else if (x < 10) {
                n = (int) (100 * x + 0.5);
                s = Integer.toString(n);
                formatted = s.substring(0, 1) + "." + s.substring(1, 3);
            }
            else if (x < 100) {
                n = (int) (10 * x + 0.5);
                s = Integer.toString(n);
                formatted = s.substring(0, 2) + "." + s.substring(2, 3);
            }
            else {
                n = (int) (x + 0.5);
                formatted = Integer.toString(n);
            }
        }
        catch (StringIndexOutOfBoundsException e) {
            formatted = "n/a";
        }
        return formatted;
    }

    /*
     * Simplistic number formatter to avoid bug in Linux JDK with NumberFormat
     */
    public static String format(double x) {
        int n;
        String s;
        String formatted;
        try {
            if (x < 0.01) {
                formatted = "<0.01";
            }
            else if (x < 0.1) {
                n = (int) (1000 * x + 0.5);
                s = Integer.toString(n);
                formatted = ".0" + s;
            }
            else if (x < 1) {
                n = (int) (1000 * x + 0.5);
                s = Integer.toString(n);
                formatted = "." + s;
            }
            else if (x < 10) {
                n = (int) (100 * x + 0.5);
                s = Integer.toString(n);
                formatted = s.substring(0, 1) + "." + s.substring(1, 3);
            }
            else if (x < 100) {
                n = (int) (10 * x + 0.5);
                s = Integer.toString(n);
                formatted = s.substring(0, 2) + "." + s.substring(2, 3);
            }
            else {
                n = (int) (x + 0.5);
                formatted = Integer.toString(n);
            }
        }
        catch (StringIndexOutOfBoundsException e) {
            formatted = "n/a";
        }
        return formatted;
    }

    // /////////////////////////////////////
    // instance method declarations
    // /////////////////////////////////////
    // 7/25/2000 AMT v1.01
    // used for comparison graphs in order to get to routines and data in Result
    // from both Metrics at once
    //
    public Result[] getResults() {
        return result;
    }

    public boolean checkElapsed() {
        for (int i = 0; i < result.length; i++) {
            double e = result[i].elapsed();
            double mi = result[i].meas_int();
            double ri = result[i].ramp_up_int();
            double ep = result[i].expected_peak();
           
            if (i >= (ep - 1)) {
                if (e / mi > 1150) {
                    output_properly(" INVALID: for point " + i
                            + ", elapsed ms =" + e
                            + " specified measurement interval seconds = " + mi
                            + ".\n");
                    output_properly(" elapsed / intended =" + e / (10 * mi)
                            + " > 115% \n");
                    return false;
                }
                if (e / mi < 995) {
                    output_properly(" INVALID: for point " + i
                            + ", elapsed ms =" + e
                            + " specified measurement interval seconds = " + mi
                            + ".\n");
                    output_properly(" elapsed / intended =" + e / (10 * mi)
                            + " < 99.5% \n");
                    return false;
                }
            }
        }
        return true;
    }

    // elian111004 - thread spread requirements fix
    /*
     * public boolean checkThreadSpread() { boolean check_ok = true; for (int
     * i=0; i < result.length; i++) { int x = result[i].warehouses(); double y =
     * result[i].score(); if (x > whAtPeak ) return check_ok; // Otherwise, we
     * have to check double min_warehouse_transactions = props.getDouble(
     * "result.test"+i+".company.min_thread_transactions"); double
     * max_warehouse_transactions = props.getDouble(
     * "result.test"+i+".company.max_thread_transactions"); if
     * ((max_warehouse_transactions - min_warehouse_transactions) > (0.30 *
     * max_warehouse_transactions)) { output_properly("Problem at point " + i + "
     * where max_warehouse_transactions - min_warehouse_transactions is " +
     * (int) (100 * (max_warehouse_transactions -
     * min_warehouse_transactions)/max_warehouse_transactions) + "% of
     * max_warehouse_transactions \n"); check_ok = false; } } return check_ok; }
     */
    public static String[] benchmarkList(ReportProps props) {
        Vector<String> v = new Vector<String>();
        for (int j = 1;; j++) {
            String name = props.get("result.test" + j + ".warehouses");
            if (name == null)
                break;
            v.addElement("test" + j);
        }
        int nel = v.size();
        if (nel == 0)
            return new String[0];
        String[] names = new String[nel];
        for (int i = 0; i < nel; i++)
            names[i] = (String) v.elementAt(i);
        return names;
    }

    // sets maxWarehouses and highestScore and whAtPeak
    private void calculate() {
        double productWorst = 1;
        double productBest = 1;
        double weights = 0;
        int warehouses;
        double score;
        int j;
        highestScore = 0;
        maxWarehouses = 0;
        // get whAtPeak from properties expected_peak_warehouse
        expectedPeakWh = props.getInt("input.expected_peak_warehouse");
        for (j = 1;; j++) {
            score = props.getDouble("result.test" + j + ".company.score");
            if (score == 0)
                break;
            if (score > highestScore) {
                highestScore = score;
                measuredPeakWarehouse = props.getInt("result.test" + j
                        + ".warehouses");
            }
            warehouses = props.getInt("result.test" + j + ".warehouses");
            if (warehouses > maxWarehouses) {
                maxWarehouses = warehouses;
            }
        }
        // amt-end
    }

    public String detail() {
        // String[] name = Result.benchmarkList(props);
        StringBuffer buf = new StringBuffer();
        if (!valid) {
            buf.append("<H4>Invalid Result</H4>\n");
            buf
                    .append("This result is not valid for the following reason(s):\n");
            buf.append("<PRE>\n");
            buf.append(this.invalid_buf);
            // for (int i=0; i < result.length; i++){
            // buf.append (result[i].invalidReason.toString());
            // }
            buf.append("</PRE>\n");
        }
        else {
            buf.append(this.invalid_buf);
        }
        buf.append("<H4>Details of Runs</H4>\n");
        // 06/28/00 WRR -- v 1.01
        // Add WIDTH parameter.
        buf.append("<TABLE BORDER WIDTH=100%>\n");
        buf.append("<TR><TH ROWSPAN=2>Warehouses</TH>\n");
        buf.append("  <TH ROWSPAN=2>Thrput</TH>\n");
        buf.append("  <TH COLSPAN=2>Total heap (MB)</TH>\n");
        buf.append("  <TH ROWSPAN=2>Thread spread %</TH>\n");
        buf.append("  <TH ROWSPAN=2>% > 120s</TH>\n");
        buf.append("  <TH ROWSPAN=2>transaction type</TH>\n");
        buf.append("  <TH ROWSPAN=2>Count</TH>\n");
        buf.append("  <TH COLSPAN=4>Time (in seconds)</TH>\n");
        buf.append("</TR>\n");
        buf.append("<TR><TH>Size</TH><TH>Used</TH>\n");
        buf.append("<TH>total</TH><TH>max</TH></TR>\n");
        // buf.append ("</TABLE BORDER>\n");
        for (int i = 0; i < result.length; i++) {
            buf.append(result[i].toDetailRows());
        }
        buf.append("</TABLE>\n");
        return buf.toString();
    }

    private String JasonGraph(String label) {
        GraphImage graph = new GraphImage(400, 400);
        graph.setPixelSize(6);
        graph.setPixelType(GraphImage.PIXEL_TYPE_CIRCLE);
        graph.setForeground(Color.red);
        graph.setBackground(Color.white);
        graph.setConnectPointsEnabled(true);
        graph.setXAxisLabelsPrecision(1, 0);
        graph.setYAxisLabelsPrecision(3, 0);
        double maxx = maxWarehouses * 1.15;
        double maxy = highestScore * 1.15;
        graph.setScale(new double[] {
                graph.getSuggestedMinXScale(maxx, maxy), maxx,
                graph.getSuggestedMinYScale(maxx, maxy), maxy
        });
        graph.plotXYAxes(Color.gray);
        // 06/28/00 WRR -- v 1.01
        // Parameter changes in both X and Y axis tick computations.
        graph.plotXAxisTicks(4.0, Color.gray, "warehouses");
        graph.plotYAxisTicks(graph.getSuggestedYAxisTickInterval(10, maxy),
                Color.gray, "scores");
        double maximumWarehouse = 0;
        // graph.setForeground(Color.red);
        for (int i = 0; i < result.length; i++) {
            double x = (double) result[i].warehouses();
            if (x > maximumWarehouse) {
                maximumWarehouse = x;
            }
            double y = result[i].score();
            if ((x >= expectedPeakWh) && (x <= 2 * expectedPeakWh))
                graph.plot(x, y, Color.red);
            else
                graph.plot(x, y, Color.orange);
        }
        if (maximumWarehouse > 2 * expectedPeakWh - 1) {
            graph.plotFlatLine(expectedPeakWh, 2 * expectedPeakWh, Integer
                    .parseInt(metric()), (Color.red));
        }
        graph.drawLegend("Included in score calculation", 2, 4, Color.red,
                Color.gray, "Dot");
        graph.drawLegend("Not included in score calculation", 2, 6,
                Color.orange, Color.gray, "Dot");
        graph.setImageQuality(0.85f);
        if (label == null) {
            Random random = new Random();
            label = "" + (10000 + random.nextInt() % 10000);
        }
        String file_name;
        if (SPECInternalUse) {
            file_name = label + ".jpg";
        }
        else {
            file_name = "SPECjbb." + label + ".jpg";
        }
        graph.make(o_directory + java.io.File.separator + file_name);
        return (" <UL><IMG SRC=\"" + file_name + "\" WIDTH=400 HEIGHT=400></UL>");
    }

    // amt for comparison graphs
    private String JasonGraph(Metrics metrics_2, String label) {
        GraphImage graph = new GraphImage(400, 400);
        graph.setPixelSize(8);
        graph.setPixelType(GraphImage.PIXEL_TYPE_CIRCLE);
        graph.setForeground(Color.blue);
        graph.setBackground(Color.white);
        graph.setConnectPointsEnabled(true);
        graph.setXAxisLabelsPrecision(1, 0);
        graph.setYAxisLabelsPrecision(3, 0);
        int mw = Math.max(maxWarehouses, metrics_2.maxWarehouses);
        double hs = Math.max(highestScore, metrics_2.highestScore);
        double maxx = mw * 1.15;
        double maxy = hs * 1.15;
        graph.setScale(new double[] {
                graph.getSuggestedMinXScale(maxx, maxy), maxx,
                graph.getSuggestedMinYScale(maxx, maxy), maxy
        });
        graph.plotXYAxes(Color.gray);
        graph.plotXAxisTicks(4.0, Color.gray, "warehouses");
        graph.plotYAxisTicks(graph.getSuggestedYAxisTickInterval(10, maxy),
                Color.gray, "scores");
        for (int i = 0; i < result.length; i++) {
            double x = result[i].warehouses();
            double y = result[i].score();
            graph.plot(x, y, Color.red);
        }
        Result[] result_2 = metrics_2.getResults();
        graph.setForeground(Color.white);
        for (int i = 0; i < result_2.length; i++) {
            if (1 == i)
                graph.setForeground(Color.green);
            double x = result_2[i].warehouses();
            double y = result_2[i].score();
            graph.plot(x, y, Color.cyan);
        }
        graph.setImageQuality(0.85f);
        if (label == null) {
            Random random = new Random();
            label = "" + (10000 + random.nextInt() % 10000);
        }
        String file_name;
        if (SPECInternalUse) {
            file_name = label + ".jpg";
        }
        else {
            file_name = "SPECjbb." + label + ".jpg";
        }
        graph.make(o_directory + java.io.File.separator + file_name);
        return (" <UL><IMG SRC=\"" + file_name + "\" WIDTH=400 HEIGHT=400></UL>");
    }

    public static String multiCurvesGraph(Metrics[] metrics, String label,
            boolean SPECInternalUse, String o_directory) {
        GraphImage graph = new GraphImage(400, 400);
        graph.setPixelSize(6);
        graph.setPixelType(GraphImage.PIXEL_TYPE_CIRCLE);
        graph.setForeground(Color.blue);
        graph.setBackground(Color.white);
        graph.setConnectPointsEnabled(true);
        graph.setXAxisLabelsPrecision(1, 0);
        graph.setYAxisLabelsPrecision(3, 0);
        double maxX = 0;
        double maxY = 0;
        int maxWh = 0;
        for (int i = 0; i < metrics.length; i++) {
            if (maxX < metrics[i].maxWarehouses)
                maxX = metrics[i].maxWarehouses;
            maxY += metrics[i].highestScore;
            if (maxWh < metrics[i].maximum_warehouses)
                maxWh = metrics[i].maximum_warehouses;
        }
        double maxx = maxX * 1.15;
        double maxy = maxY * 1.15;
        graph.setScale(new double[] {
                graph.getSuggestedMinXScale(maxx, maxy), maxx,
                graph.getSuggestedMinYScale(maxx, maxy), maxy
        });
        graph.plotXYAxes(Color.gray);
        graph.plotXAxisTicks(4.0, Color.gray, "warehouses");
        graph.plotYAxisTicks(graph.getSuggestedYAxisTickInterval(10, maxy),
                Color.gray, "scores");
        double sumX[] = new double[maxWh];
        double sumY[] = new double[maxWh];
        for (int j = 0; j < metrics.length; j++) {
            int expPeak = metrics[j].expectedPeakWh;
            Result[] result = metrics[j].getResults();
            Color color = Color.green;
            for (int i = 0; i < result.length; i++) {
                if (1 == i)
                    graph.setForeground(color.darker());
                double x = result[i].warehouses();
                double y = result[i].score();
                if ((x >= expPeak) && (x <= 2 * expPeak))
                    graph.plot(x, y, Color.red);
                else
                    graph.plot(x, y, Color.orange);
                sumX[i] = x;
                sumY[i] += y;
            }
            graph.setForeground(Color.white);
        }
        for (int i = 0; i < sumX.length; i++) {
            if (i == 1)
                graph.setForeground(Color.magenta);
            graph.plot(sumX[i], sumY[i], Color.blue);
        }
        graph.drawLegend("Instance result", 2, 4, Color.green, Color.gray,
                "Line");
        graph.drawLegend("Aggregated result", 2, 6, Color.magenta, Color.gray,
                "Line");
        graph.setImageQuality(0.85f);
        if (label == null) {
            Random random = new Random();
            label = "" + (10000 + random.nextInt() % 10000);
        }
        String file_name;
        if (SPECInternalUse) {
            file_name = label + ".jpg";
        }
        else {
            file_name = "SPECjbb." + label + ".jpg";
        }
        graph.make(o_directory + java.io.File.separator + file_name);
        return (" <UL><IMG SRC=\"" + file_name + "\" WIDTH=400 HEIGHT=400></UL>");
    }

    private String dotsGraph(Metrics metrics_2) {
        StringBuffer buf = new StringBuffer();
        StringBuffer bufLabels = new StringBuffer();
        int[][] xy = new int[101][101];
        int mw;
        double hs;
        if (metrics_2 != null) {
            mw = Math.max(maxWarehouses, metrics_2.maxWarehouses);
            hs = Math.max(highestScore, metrics_2.highestScore);
        }
        else {
            mw = maxWarehouses;
            hs = highestScore;
        }
        int x_width = Math.max(50, mw);
        int y_width = 50;
        double x_factor = ((double) x_width) / (double) (mw);
        double y_factor = y_width * 0.9 / hs;
        for (int i = 0; i < result.length; i++) {
            // 08/25/2000 AMT v1.01
            // "-1" because the last point sometimes fell off the
            // right edge of the graph.
            int x = (int) ((double) result[i].warehouses() * x_factor) - 1;
            int y = (int) (result[i].score() * y_factor);
            xy[x][y] = 1;
        }
        if (metrics_2 != null) {
            for (int i = 0; i < metrics_2.result.length; i++) {
                // 08/25/2000 AMT v1.01
                // "-1" because the last point sometimes fell off the
                // right edge of the graph.
                int x = (int) ((double) metrics_2.result[i].warehouses() * x_factor) - 1;
                int y = (int) (metrics_2.result[i].score() * y_factor);
                xy[x][y] = 2;
            }
        }
        // 08/25/2000 JG v1.01
        // y-axis tick spacing made to look nice. Extensive code changes here.
        int unit = getSuggestedYAxisTickInterval(6, 1.1 * hs);
        buf.append("<TABLE BORDER=0 CELLSPACING=0 CELLPADDING=0>");
        buf.append("<TR><TD NOWRAP>");
        buf.append("<TABLE BORDER=0 CELLSPACING=0 CELLPADDING=0 HEIGHT=100%>");
        buf.append("<TR><TD ALIGN=center VALIGN=middle NOWRAP>");
        buf
                .append("<FONT FACE=courier>&nbsp;o&nbsp;<BR>&nbsp;p&nbsp;<BR>&nbsp;s&nbsp;<BR>&nbsp;/&nbsp;<BR>&nbsp;s&nbsp;</FONT>");
        buf.append("</TD>");
        buf.append("<TD VALIGN=top>");
        buf.append("<TABLE BORDER=0 CELLSPACING=2 CELLPADDING=0 HEIGHT=100%>");
        int count_down = (int) (1.1 * (hs / unit));
        for (int y = y_width; y > 0; y--) {
            buf.append("<TR>");
            if (y / (y_factor) < unit * count_down) {
                // buf.append("<TD ALIGN=center VALIGN=middle ROWSPAN=50
                // BGCOLOR=#ffffff> ");
                buf
                        .append("<TD BGCOLOR=white NOWRAP VALIGN=top ALIGN=right VALIGN=top><FONT SIZE=1 COLOR=#ff0000>");
                buf.append(unit * count_down);
                count_down--;
                buf.append("</FONT></TD>");
            }
            else {
                buf.append("<td></td>");
            }
            buf.append("</TR>");
        }
        buf.append("</TABLE></TD></TR></TABLE></TD><TD>");
        buf.append("<TABLE BORDER=1 CELLSPACING=2 CELLPADDING=0 WIDTH=100%>");
        for (int y = y_width; y > 0; y--) {
            buf.append("<TR>");
            // 08/25/2000 AMT v 1.01
            // Starting x at 1 rather than 0 to line chart up better
            // Makes it narrower, but points shifted left to fit on graph;
            // blank column removed.
            // BGCOLOR = black added by Jason to make pixels look better.
            for (int x = 1; x < x_width; x++) {
                if (xy[x][y] == 1)
                    buf.append("<TD BGCOLOR=#000000 WIDTH=2><IMG SRC="
                            + IMAGE_PIX + " WIDTH=2 HEIGHT=2></TD>\n");
                else if (xy[x][y] == 2)
                    buf.append("<TD BGCOLOR=#000000 WIDTH=2><IMG SRC="
                            + IMAGE_PIX_2 + " WIDTH=2 HEIGHT=2></TD>\n");
                else
                    buf.append("<TD WIDTH=2></TD>\n");
            }
            buf.append("</TR>");
        }
        buf.append("</TABLE>");
        buf.append("</td></TR><TR><TD></TD><TD>");
        // 08/25/2000 AMT v 1.01
        // Made x-axis spacing scalable. Fewer ticks, better spaced.
        buf.append("<TABLE BORDER=0 CELLSPACING=0 CELLPADDING=0 WIDTH=100%>");
        buf.append("<TR>\n");
        int x_unit; // How many warehouses ticks go up by
        if (mw < 8) {
            x_unit = 1;
        }
        else if (mw < 120) {
            x_unit = 4;
        }
        else // mw > 120
        {
            x_unit = 10;
        }
        // AMT: tick for 0 improves spacing
        buf
                .append("<TD BGCOLOR=white ALIGN=left><FONT SIZE=1 COLOR=red>0</FONT></TD>\n");
        for (int i = 1; i <= mw; i++) {
            if (mw <= 8 || (mw > 8 && (i % x_unit == 0))) {
                // AMT: Right-align last tick, otherwise center the tick.
                if (i + x_unit > mw)
                    buf
                            .append("<TD BGCOLOR=white ALIGN=right><FONT SIZE=1 COLOR=red>");
                else
                    buf
                            .append("<TD BGCOLOR=white ALIGN=center><FONT SIZE=1 COLOR=red>");
                // AMT: all tick labels are 2-character: improves spacing
                // (less confusing to html.)
                if (i < 10) {
                    buf.append("0" + i);
                }
                else {
                    buf.append("" + i);
                }
                buf.append("</FONT></TD>\n");
            }
            else {
                // AMT: all tick labels are 2-character, even the blanks
                buf.append("<TD>&nbsp&nbsp</TD>\n");
            }
        }
        buf.append("</TR><TR>");
        buf.append("<TD COLSPAN=" + mw + " ALIGN=center BGCOLOR=white>"
                + "<FONT FACE=courier>warehouses</FONT></TD></TR>");
        buf.append("</TABLE>");
        buf.append("</TD></TR></TABLE>");
        return buf.toString();
    }

    // 08/30/00 WRR -- v 1.01
    // New method. Added for comparison table purposes.
    // Since we create the warehouse array here (in the next method)
    // here is the best object to answer the question, what was the
    // score at n warehouses? even though Result might otherwise seem
    // better.
    double score_at_wh(int n) {
        if (n < score_at.length) {
            return score_at[n];
        }
        else {
            return 0;
        }
    }

    public long rampup_millis_at_wh(int n) {
        if (n < rampup_millis.length) {
            return rampup_millis[n];
        }
        else {
            return 0;
        }
    }

    public long rampdown_millis_at_wh(int n) {
        if (n < rampdown_millis.length) {
            return rampdown_millis[n];
        }
        else {
            return 0;
        }
    }

    public long start_millis_at_wh(int n) {
        if (n < start_millis.length) {
            return start_millis[n];
        }
        else {
            return 0;
        }
    }

    public long end_millis_at_wh(int n) {
        if (n < end_millis.length) {
            return end_millis[n];
        }
        else {
            return 0;
        }
    }

    // this as well as calculate finds the maximum warehouses; which is first?
    // this creates an array so that score_at[n] is the score at n warehouses
    private void create_warehouse_array() {
        for (int i = 0; i < result.length; i++) {
            if (result[i].warehouses() > maximum_warehouses)
                maximum_warehouses = result[i].warehouses();
        }
        score_at = new double[maximum_warehouses + 2]; // reads better without
        rampup_millis = new long[maximum_warehouses + 2];
        rampdown_millis = new long[maximum_warehouses + 2];
        start_millis = new long[maximum_warehouses + 2];
        end_millis = new long[maximum_warehouses + 2];
        for (int i = 0; i <= maximum_warehouses + 1; i++) {
            score_at[i] = 0;
            rampup_millis[i] = 0;
            rampdown_millis[i] = 0;
            start_millis[i] = 0;
            end_millis[i] = 0;
        }
        for (int i = 0; i < result.length; i++) {
            // 06/28/00 WRR -- v 1.01
            // Add logic to mark repeated points invalid.
            if (score_at[(int) (result[i].warehouses())] != 0) {
                output_properly("INVALID: cannot repeat points");
                this.valid = false;
            }
            else {
                score_at[(int) (result[i].warehouses())] = result[i].score();
                if (jvm_instances > 1) {
                    rampup_millis[(int) (result[i].warehouses())] = result[i]
                            .rampup_millis();
                    rampdown_millis[(int) (result[i].warehouses())] = result[i]
                            .rampdown_millis();
                    start_millis[(int) (result[i].warehouses())] = result[i]
                            .start_millis();
                    end_millis[(int) (result[i].warehouses())] = result[i]
                            .end_millis();
                }
            }
        }
    }

    private boolean check_points_before_metric() {
        boolean value = true;
        String s;
        for (int i = 1; i < expectedPeakWh; i++) {
            if (getScoreAt(i) == 0) {
                output_properly("INVALID: Missing point: "
                        + i
                        + " required for publication; all points up to the peak, "
                        + expectedPeakWh + ", are required.");
                value = false;
            }
        }
        return value;
    }

    private boolean check_points_for_metric() {
        boolean value = true;
        String s;
        if (maximum_warehouses < 2 * expectedPeakWh) {
            s = "Warning: all points from "
                    + expectedPeakWh
                    + " to "
                    + 2
                    * expectedPeakWh
                    + " are averaged for metric; 0's will be used for missing points.";
            output_properly(s);
            return false;
        }
        for (int i = expectedPeakWh; i <= 2 * expectedPeakWh; i++) {
            if (score_at[i] == 0) {
                output_properly("Warning: Missing point: 0 will used for point "
                        + i + ".");
                value = false;
            }
        }
        return value;
    }

    public String metric() {
        int n = expectedPeakWh;
        double total = 0;
        int up_to = 2 * n;
        if (maximum_warehouses < 2 * n) // should be maximum warehouse
        {
            up_to = maximum_warehouses;
            // valid = false;
            // print_score = false;
        }
        for (int i = n; i <= up_to; i++) {
            total += score_at[i];
        }
        return (ratioOrNa(total / (n + 1)));
    }

    public String ratioOrNa(double x) {
        if (!print_score || x <= 0)
            return "n/a";
        if (valid && print_score)
            return format(x);
        // else
        // return "Invalid, but estimate is " + format(x);
        return format(x); // print_score true and valid false
    }

    // 08/25/2000 AMT v 1.01
    // Made tableAndGraph be specific to single result case;
    // new routine compareTableAndGraph handles comparison table.
    // This is because the logic for listing all the points is
    // a simple walk through result array in single case.
    // Comparison requires checking every possible number of
    // warehouses to see if either result used it.
    // This changes the number of arguments to tableAndGraph.
    // tableAndGraph creates the chart and calls appropriate routine
    // to make jpg or html graph.
    public String tableAndGraph(String label, boolean useHtml,
            String output_directory) {
        StringBuffer buf = new StringBuffer();
        o_directory = output_directory;
        buf.append(check_all_buf);
        int columns = 5;
        buf.append("<TABLE BORDER WIDTH=\"100%\">\n" + "    <TH COLSPAN="
                + columns + "></TH>\n");
        // "<TR><TH ALIGN=LEFT ROWSPAN=2>Benchmark</TH>\n" +
        int span = result.length + 3;
        buf.append("    <TH ROWSPAN=" + span + ">\n");
        if (useHtml) {
            // 06/28/00 WRR -- v 1.01
            // Put the pixel images where they're needed.
            relocateImageResource(IMAGE_PIX, output_directory);
            relocateImageResource(IMAGE_PIX_2, output_directory);
            // 06/28/00 WRR -- v 1.01
            // AMT: Passing null to dotsGraph means no second result to compare
            buf.append(dotsGraph(null));
        }
        else {
            try {
                Class.forName("java.awt.image.BufferedImage");
                // 08/30/00 WRR -- v 1.01
                // Don't use metrics_2 here.
                buf.append(JasonGraph(label));
            }
            catch (java.lang.InternalError e) {
                buf.append(dotsGraph(null));
            }
            catch (Exception e) {
                buf.append(dotsGraph(null));
            }
        }
        buf.append("</TH></TR>\n");
        buf.append("<TR><TH ALIGN=RIGHT>Warehouses</TH>\n"
                + "    <TH ALIGN=RIGHT>bops</TH>\n"
                + "    <TH ALIGN=RIGHT>Incl. in metric</TH></TR>\n");
        for (int i = 0; i < result.length; i++)
            buf.append(result[i].toRow(expectedPeakWh));
        // 06/28/00 WRR -- v 1.01
        // Say SPECjbb2005 instead of just SPECjbb.
        buf.append("<TR><TH ALIGN=LEFT>SPECjbb2005 </TH>\n"
                + "    <TH ALIGN=RIGHT>  (from " + expectedPeakWh + " to " + 2
                * expectedPeakWh + ") " + "</TH>\n" + "    <TH ALIGN=RIGHT>"
                + metric() + " bops</TH>\n" + "</TR>\n</TABLE>\n");
        buf.append("</TR>\n</TABLE>\n");
        return buf.toString();
    }

    // 08/25/2000 AMT v 1.01
    // Added separate function compareTableAndGraph for comparing
    // two results.
    // Comparison requires checking every possible number of
    // warehouses to see if either result used it.
    public String compareTableAndGraph(Metrics metrics_2, String label,
            boolean useHtml, String output_directory) {
        StringBuffer buf = new StringBuffer();
        int columns = 5;
        if (metrics_2 == null) {
            // AMT: metrics2 null! Wrong function!
            return tableAndGraph(label, useHtml, output_directory);
        }
        o_directory = output_directory;
        buf.append(check_all_buf);
        buf.append("<TABLE BORDER WIDTH=\"100%\">\n" + "    <TH COLSPAN="
                + columns + "></TH>\n");
        // "<TR><TH ALIGN=LEFT ROWSPAN=2>Benchmark</TH>\n" +
        int span = result.length + 3;
        buf.append("    <TH ROWSPAN=" + span + ">\n");
        if (useHtml) {
            // 06/28/00 WRR -- v 1.01
            // Put the pixel images where they're needed.
            relocateImageResource(IMAGE_PIX, output_directory);
            relocateImageResource(IMAGE_PIX_2, output_directory);
            // 06/28/00 WRR -- v 1.01
            // Provide the expected parameter
            buf.append(dotsGraph(metrics_2));
        }
        else {
            try {
                Class.forName("java.awt.image.BufferedImage");
                if (metrics_2 != null)
                    buf.append(JasonGraph(metrics_2, label));
                else
                    buf.append(JasonGraph(label));
            }
            catch (java.lang.InternalError e) {
                // 06/28/00 WRR -- v 1.01
                // Provide the expected parameter
                buf.append(dotsGraph(metrics_2));
            }
            catch (Exception e) {
                // 06/28/00 WRR -- v 1.01
                // Provide the expected parameter
                buf.append(dotsGraph(metrics_2));
            }
        }
        buf.append("</TH></TR>\n");
        buf.append("<TR><TH ALIGN=RIGHT>Warehouses</TH>\n"
                + "    <TH ALIGN=RIGHT>bops</TH>\n"
                + "    <TH ALIGN=RIGHT>Incl. in metric</TH>\n"
                + "    <TH ALIGN=RIGHT>bops</TH>\n"
                + "    <TH ALIGN=RIGHT>Incl. in metric</TH></TR>\n");
        Result[] result_2 = metrics_2.getResults();
        int mw = Math.max(maxWarehouses, metrics_2.maxWarehouses);
        double hs = Math.max(highestScore, metrics_2.highestScore);
        int shorter = Math.min(result.length, result_2.length);
        int longer = Math.max(result.length, result_2.length);
        for (int i = 1; i <= mw; i++) {
            buf.append(BothScore(i, score_at_wh(i), metrics_2.score_at_wh(i),
                    expectedPeakWh, metrics_2.get_whAtPeak()));
        }
        // 06/28/00 WRR -- v 1.01
        // Add the second metric, and move this inside the else.
        buf.append("<TR><TH ALIGN=LEFT>SPECjbb2005 = </TH>\n"
                + "    <TH ALIGN=RIGHT>" + metric()
                + " bops</TH>\n<TH> &nbsp </TH>" + "    <TH ALIGN=RIGHT>"
                + metrics_2.metric() + " bops</TH>\n<TH> &nbsp </TH>"
                + "<P>\n");
        buf.append("</TR>\n</TABLE>\n");
        return buf.toString();
    }

    public void relocateImageResource(String image, String relocateTo) {
        /*
         * For reporter, we need to relocate some images... This method
         * relocates (copies) a system resource binary (image) file to either a
         * relative or absolute path. This hack assumes the image file resides
         * under the spec.reporter.images package, and will work whether the
         * package is jar'd or not. Example usage: relocateImageResource(
         * "invalid.gif", "/var/tmp" ); will copy invalid.gif from
         * spec.reporter.images package to /var/tmp -Jason Glick, 03/24/2000
         */
        String imageResource = IMAGE_RESOURCE_PATH + image;
        InputStream is = getClass().getResourceAsStream(imageResource);
        if (is == null)
            return;
        try {
            int b = -1;
            // 07/18/00 WRR -- v 1.01^M
            // Change "/" to File.separator for generality.
            File file = new File(relocateTo + File.separator + image);
            FileOutputStream fos = new FileOutputStream(file);
            while ((b = is.read()) > -1) {
                fos.write(b);
            }
            fos.flush();
            fos.close();
            is.close();
        }
        catch (IOException ioe) {
            JBButil.getLog().log(Level.WARNING,
                    "IOException:" + ioe.getMessage(), ioe);
        }
    }

    // 08/25/2000 AMT v 1.01
    // Mirror copy of GraphImage version, for html/non-AWT system
    // New method for computing Y axis tick interval
    public int getSuggestedYAxisTickInterval(int suggestedNumTicks, double maxy) {
        // as a starting point, aim for more-or-less suggestedNumTicks...
        double t = maxy / suggestedNumTicks;
        // now, let's massage the tick interval t, shape it up...
        int ll = (int) (Math.log(t) / Math.log(10));
        int lu = ll + 1;
        int[] ti = new int[10];
        ti[0] = (int) Math.pow(10, ll);
        ti[9] = (int) Math.pow(10, lu);
        for (int i = 0; i < ti.length - 1; i++) {
            ti[i + 1] = ti[i] + ti[0];
        }
        int index = 9;
        int delta = Integer.MAX_VALUE;
        for (int i = ti.length - 1; i >= 0; i--) {
            double ddiff = (ti[i] - t);
            if (ddiff < 0.0)
                break;
            int diff = (int) ddiff;
            if (diff <= delta || delta < 0) {
                delta = diff;
                index = i;
            }
        }
        // this is our sensible and aesthetically round tick interval...
        return ti[index];
    }

    // 08/25/2000 AMT v 1.01
    // BothScore handles the score chart line for comparison chart
    public String BothScore(int w, double score_1, double score_2,
            int whAtPeak_1, int whAtPeak2) {
        StringBuffer buf = new StringBuffer("<TR>\n");
        buf.append("  <TD ALIGN=RIGHT>" + w + "</TD>\n");
        buf.append("  <TD ALIGN=RIGHT>" + score_format(score_1) + "</TD>\n");
        if ((whAtPeak_1 <= w) && (w <= 2 * whAtPeak_1)) {
            buf.append("  <TD ALIGN=RIGHT> *  </TD>\n");
        }
        else {
            buf.append("  <TD ALIGN=RIGHT>  &nbsp   </TD>\n");
        }
        buf.append("  <TD ALIGN=RIGHT>" + score_format(score_2) + "</TD>\n");
        if ((whAtPeak2 <= w) && (w <= 2 * whAtPeak2)) {
            buf.append("  <TD ALIGN=RIGHT> *  </TD>\n");
        }
        else {
            buf.append("  <TD ALIGN=RIGHT>   &nbsp  </TD>\n");
        }
        buf.append("</TR>\n");
        return buf.toString();
    }

    private double getScoreAt(int i) {
        if (i < score_at.length) {
            return score_at[i];
        }
        else
            return 0;
    }
}
