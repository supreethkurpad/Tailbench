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

import java.io.*;
import java.util.Enumeration;
import java.util.logging.Level;
import spec.jbb.JBButil;

public class Report {
    // This goes right after each class/interface statement
    static final String         COPYRIGHT = "SPECjbb2005,"
                                                  + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                                  + "All rights reserved,"
                                                  + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                                  + "All rights reserved,"
                                                  + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                                  + "All rights reserved,"
                                                  + "Licensed Materials - Property of SPEC";

    private static String       logo      = "<IMG SRC=\"spec-sm.gif\" WIDTH=\"57\" HEIGHT=\"72\"\n"
                                                  + "ALT=\"[SPEC logo]\" ALIGN=MIDDLE>";

    private final static String version   = "@(#)Report.java	1.18 08/03/98";

    public String               graphTable, hardware, metric, name, software;

    public String               base;

    private String              category;

    public boolean              SPECInternalUse;

    public boolean              echoRaw;

    public String               issue;

    private String              memory;

    private String              memoryDescription;

    public Metrics              metrics;

    public Metrics              metrics_2;

    public ReportProps          results   = new ReportProps();

    public ReportProps          results_2 = new ReportProps();

    public Template             template;

    public ReportProps          titles    = new ReportProps();

    public boolean              useJavaGraph;

    public boolean              useHtml;

    public boolean              verbose;

    public String               label;

    public String               output_directory;

    public Report(boolean echoRaw, boolean SPECInternalUse, String newsletter,
            String resultFile, boolean verbose, String resultFile_2,
            String label, boolean useHtml, String output_directory) {
        this.echoRaw = echoRaw;
        this.SPECInternalUse = SPECInternalUse;
        this.issue = newsletter;
        this.verbose = verbose;
        this.label = label;
        this.output_directory = output_directory;
        try {
            StringReader tis = new StringReader(new Title().getTitleString());
            titles.load(tis);
        }
        catch (IOException e) {
            JBButil.getLog().log(Level.WARNING,
                    "Error loading titles for report\n", e);
            System.exit(1);
        }
        try {
            if (resultFile == null)
                results.load(new InputStreamReader(System.in));
            else
                results.load(resultFile);
        }
        catch (IOException e) {
            JBButil.getLog().log(Level.WARNING,
                    "Error reading results file " + resultFile + ": " + e, e);
            System.exit(1);
        }
        metrics = new Metrics(results, SPECInternalUse);
        try {
            if (resultFile_2 != null) {
                results_2.load(resultFile_2);
                metrics_2 = new Metrics(results_2, SPECInternalUse);
            }
            else
                metrics_2 = null;
        }
        catch (IOException e) {
            JBButil.getLog().log(
                    Level.WARNING,
                    "Error reading comparison results file " + resultFile_2
                            + ": " + e, e);
            System.exit(1);
        }
        if (metrics_2 == null) {
            this.graphTable = metrics.tableAndGraph(label, useHtml,
                    output_directory);
        }
        else {
            this.graphTable = metrics.compareTableAndGraph(metrics_2, label,
                    useHtml, output_directory);
        }
        this.name = results.get("input.suite");
        this.hardware = results.get("config.hw.vendor") + " "
                + results.get("config.hw.model");
        if (metric == null)
            metric = " " + metrics.metric();
        if (metrics.valid) {
            if (!SPECInternalUse) {
                metrics.results_buf.append("\nValid run, Score is " + metric
                        + "\n");
            }
        }
        else {
            if (metrics.print_score) {
                metrics.results_buf.append("INVALID run; Score is " + metric
                        + "\n");
            }
            else {
                metrics.results_buf
                        .append("INVALID run preventing score calculation.\n");
            }
        }
        this.software = results.get("config.sw.vendor") + " "
                + results.get("config.sw.JVM");
        this.memory = results.get("config.hw.memory");
        this.memoryDescription = results.get("config.hw.memory_description");
    }

    public void print(String outFile) {
        try {
            print(new PrintStream(new BufferedOutputStream(
                    new FileOutputStream(outFile))));
        }
        catch (IOException e) {
            JBButil.getLog().log(Level.WARNING, "Error creating output: " + e,
                    e);
        }
    }

    public void print(PrintStream out) {
        try {
            out.println("<HTML><HEAD>\n"
                    + "<META NAME=\"GENERATOR\" CONTENT=\""
                    + "SPEC Java Reporter\">\n" + "<TITLE> " + name
                    + "</TITLE>\n" + "</HEAD>");
            out.print("<BODY ");
            if (metrics.valid)
                out.print(results.get("spec.report.bodyAttributes", ""));
            else {
                metrics.relocateImageResource("invalid.gif", output_directory);
                out.print("BACKGROUND=\"invalid.gif\"");
            }
            out.print(">\n");
            out.print("<TABLE WIDTH=100%>\n" + "<TR><TD COLSPAN=2>");
            if (issue != null && !issue.equals("")) {
                metrics.relocateImageResource("spec-sm.gif", output_directory);
                out.print(logo);
            }
            else
                out.print("<FONT SIZE=+2></FONT>&nbsp;");
            if (!metrics.valid)
                out.print("<FONT SIZE=+2> Invalid</FONT>");
            out.print(
            // 06/28/00 WRR -- v 1.01
                    // Add 2000 to input.suite to make SPECjbb2005.
                    "<FONT SIZE=+2>" + name + "2005</FONT></TD>\n"
                            + "<TD COLSPAN=2 ALIGN=RIGHT bgcolor=FFFF00>" +
                            // Say SPECjbb2005 instead of just SPECjbb.
                            "    <FONT SIZE=+2 COLOR=000000><B> SPECjbb2005 = "
                            + metric + "        bops</B></FONT></TD></TR>\n"
                            + "<TR><TD COLSPAN=3><FONT SIZE=+1>" + hardware
                            + "</FONT></TD></TR>\n");
            if (software != null)
                out.print("<TR><TD COLSPAN=3><FONT SIZE=+1>" + software
                        + "</FONT></TD></TR>\n");
            out.println("</TABLE>");
            out.println(graphTable);
            testBar(out);
            out.print("<P>\n");
            BufferedReader in;
            BufferedReader in_2;
            TableGroupCompare m;
            if (metrics_2 != null) {
                in = new BufferedReader(new StringReader(new TemplateCompare()
                        .getTemplateString()));
                in_2 = new BufferedReader(new StringReader(
                        new TemplateCompare().getTemplateString()));
                m = new TableGroupCompare(in, in_2, results, results_2, titles);
            }
            else {
                in = new BufferedReader(new StringReader(new Template()
                        .getTemplateString()));
                in_2 = new BufferedReader(new StringReader(new Template()
                        .getTemplateString()));
                m = new TableGroupCompare(in, in_2, results, null, titles);
            }
            out.println(m.toString());
            if (metrics_2 == null)
                out.println(metrics.detail());
            if (verbose)
                printOtherProperties(out);
            out.println("<HR><FONT SIZE=-1>\n");
            // Say SPECjbb2005 instead of just SPECjbb; add brackets.
            out.println("SPECjbb2005 Version: ["
                    + results.get("config.benchmark_version", "N/A") + ", "
                    + results.get("config.benchmark_versionDate", "-") + "]");
            out.println("<BR><I>\n");
            if (issue != null && !issue.equals("") && metrics.valid)
                out.println("Published by SPEC " + issue + "<br>");
            out.println("Reporting page, Copyright &copy; 2005 SPEC. "
                    + "All rights reserved\n" + "</I></FONT>");
            out.println("<!-- ");
            out.println("Reporting Page Generator " + version);
            if (echoRaw)
                results.store(out, "Raw Results");
            out.println(" -->");
            out.print("</BODY></HTML>\n");
            out.close();
        }
        catch (Exception e) {
            JBButil.getLog().log(Level.WARNING, "Error: " + e, e);
        }
    }

    public String messages() {
        return metrics.results_buf.toString();
    }

    private void printOtherProperties(PrintStream out) {
        out
                .println("<H4>Optional Information</H4>\n"
                        + "<I>This information is for internal documentation purposes\n"
                        + "and is not required for SPEC reporting\n"
                        + "</I><BR>");
        for (Enumeration e = results.propertyNames(); e.hasMoreElements();) {
            String key = (String) e.nextElement();
            if (key.startsWith("config.testx.")
                    || key.startsWith("config.initial.")) {
                String value = results.get(key);
                if (key.equals("config.testx.internalReference")
                        && (value.startsWith("http://") || value
                                .startsWith("file:/")))
                    value = "<A HREF=\"" + value + "\">" + value + "</A>";
                out.println("<BR>" + key + "=" + value);
            }
        }
    }

    public void testBar(PrintStream out) throws IOException {
        String license = results.get("config.test.specLicense", "MISSING");
        String testedBy = results.get("config.test.testedBy", "MISSING");
        String testDate = results.get("config.test.date", "MISSING");
        out
                .println("<TABLE WIDTH=\"100%\" BORDER=1 CELLSPACING=2 CELLPADDING=0><tr>\n"
                        + "<td width=33%><b>SPEC license # </b>"
                        + license
                        + "</td>\n"
                        + "<td width=33%><b>Tested by: </b>"
                        + testedBy
                        + "</td>\n"
                        + "<td><b>Test date: </b>"
                        + testDate + "</td>\n" + "</tr></TABLE>");
    }
}// end class
