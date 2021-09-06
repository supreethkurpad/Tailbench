/*
 *
 * Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 * Copyright (c) 2000-2005 Hewlett-Packard        All rights reserved.
 * Copyright (c) 1997-2005 Sun Microsystems, Inc. All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 *
 */
package spec.reporter;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import spec.jbb.JBButil;

class JBBReportFilenameFilter implements FilenameFilter {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                          + "All rights reserved"
                                          + "Copyright (c) 1997-2005 Sun Microsystems, Inc ,"
                                          + "All rightts reserved "
                                          + "Licensed Materials - Property of SPEC";

    String              filePrefix;

    String              fileSuffix;

    int                 filePrefixLen;

    int                 fileSuffixStart;

    public JBBReportFilenameFilter(String prefix, String suffix) {
        filePrefix = prefix;
        filePrefixLen = filePrefix.length();
        fileSuffix = suffix;
        fileSuffixStart = filePrefixLen + 3;
    }

    public boolean accept(File dir, String name) {
        return ((name.startsWith(filePrefix)) && ((name
                .substring(fileSuffixStart).equals(fileSuffix))));
    }
}

public class MultiVMReport {
    // This goes right after each class/interface statement
    static final String         COPYRIGHT            = "SPECjbb2005,"
                                                             + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                                             + "All rights reserved,"
                                                             + "Copyright (c) 2005 Hewlett-Packard,"
                                                             + "All rights reserved,"
                                                             + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                                             + "All rights reserved,"
                                                             + "Licensed Materials - Property of SPEC";

    private static String       logo                 = "<IMG SRC=\"spec-sm.gif\" WIDTH=\"57\" HEIGHT=\"72\"\n"
                                                             + "ALT=\"[SPEC logo]\" ALIGN=MIDDLE>";

    private final static String version              = "@(#)Report.java	1.18 08/03/98";

    public String               hardware, name, software;

    public boolean              SPECInternalUse;

    public String               issue;

    public boolean              echoRaw;

    public String               metric1;

    public String               metric2;

    public String               graphTable[];

    public String               metric[];

    public Metrics              metrics[];

    public ReportProps          results[];

    public File                 htmlFiles[];

    public Template             template;

    public ReportProps          titles               = new ReportProps();

    public boolean              useHtml;

    public String               label[];

    public String               output_directory;

    public int                  instances            = 0;

    public boolean              instanceChecksPassed = true;

    public MultiVMReport(String output_directory, boolean SPECInternalUse,
            String newsletter, boolean useHtml, boolean echoRaw) {
        this.SPECInternalUse = SPECInternalUse;
        this.issue = newsletter;
        this.output_directory = output_directory;
        String prefix = "SPECjbb.";
        String suffix;
        File dir = new File(output_directory);
        if (!dir.exists()) {
            System.out.println("Error: file " + output_directory
                    + " doesn't exist.");
            System.exit(1);
        }
        if (!dir.isDirectory()) {
            System.out.println("Error: file " + output_directory
                    + " is not a directory.");
            System.exit(1);
        }
        // Get .raw and .html files from the output_direcory
        suffix = ".raw";
        FilenameFilter filter = new JBBReportFilenameFilter(prefix, suffix);
        File[] resultFiles = dir.listFiles(filter);
        instances = resultFiles.length;
        suffix = ".html";
        filter = new JBBReportFilenameFilter(prefix, suffix);
        htmlFiles = dir.listFiles(filter);
        results = new ReportProps[resultFiles.length];
        metrics = new Metrics[resultFiles.length];
        graphTable = new String[resultFiles.length];
        metric = new String[resultFiles.length];
        label = new String[resultFiles.length];
        try {
            StringReader tis = new StringReader(new Title().getTitleString());
            titles.load(tis);
        }
        catch (IOException e) {
            JBButil.getLog().log(Level.WARNING,
                    "Error loading titles for report\n", e);
            System.exit(1);
        }
        double sum = 0;
        long rampup_millis = 0;
        long rampdown_millis = 0;
        boolean isMetricsDouble = true;
        for (int i = 0; i < resultFiles.length; i++) {
            results[i] = new ReportProps();
            label[i] = resultFiles[i].getName().substring(prefix.length(),
                    prefix.length() + 3);
            try {
                results[i].load(resultFiles[i].getPath());
            }
            catch (IOException e) {
                JBButil.getLog().log(
                        Level.WARNING,
                        "Error reading results file " + resultFiles[i] + ": "
                                + e, e);
                System.exit(1);
            }
            // Create metrics for each JVM run
            metrics[i] = new Metrics(results[i], SPECInternalUse);
            graphTable[i] = metrics[i].tableAndGraph(label[i], useHtml,
                    output_directory);
            metric[i] = metrics[i].metric();
            try {
                sum += Double.parseDouble(metric[i]);
            }
            catch (NumberFormatException e) {
                isMetricsDouble = false;
                metric1 = "n/a";
                metric2 = "n/a";
            }
            try {
                // sum += Double.parseDouble(metric[i]);
            }
            catch (NumberFormatException e) {
                isMetricsDouble = false;
                metric1 = "n/a";
                metric2 = "n/a";
            }
        }
        checkInstanceTiming();
        if (isMetricsDouble) {
            metric1 = Metrics.format(sum);
            metric2 = Metrics.format(sum / metrics.length);
        }
        this.name = results[0].get("input.suite");
        this.hardware = results[0].get("config.hw.vendor") + " "
                + results[0].get("config.hw.model");
        this.software = results[0].get("config.sw.vendor") + " "
                + results[0].get("config.sw.JVM");
        // Create single Raw file
        generateSingleRawFile();
    }

    public void checkInstanceTiming() {
        // verify that all JVMs are running the same number of warehouses
        int count = 0;
        int instancesChecked = 0;
        instanceChecksPassed = true;
        long last_rampup_millis = 0;
        long first_start_millis = Long.MAX_VALUE;
        long first_rampdown_millis = Long.MAX_VALUE;
        long last_end_millis = 0;
        long tmpRampdown = 0;
        long tmpEnd = 0;
        long tmpStart = 0;
        long tmpRampup = 0;
        count = metrics[0].getWarehousesTested();
        instancesChecked++;
        for (int n = 1; n < instances; n++) {
            if (count != metrics[n].getWarehousesTested())
                break;
            instancesChecked++;
        }
        if (instancesChecked != instances) {
            System.out
                    .println("INVALID: Instances do not have identical test counts");
            instanceChecksPassed = false;
        }
       else {
            // check instance timings at every warehouse tested
            for (int i = 1; i <= count && instanceChecksPassed; i++) {
                last_rampup_millis = 0;
                first_start_millis = Long.MAX_VALUE;
                first_rampdown_millis = Long.MAX_VALUE;
                last_end_millis = 0;
                for (int j = 0; j < instances; j++) {
                    //Only check warehouses at expected_peak and above
                    if(i >= metrics[j].get_whAtPeak()){
                        // get first start_millis
                        tmpStart = metrics[j].start_millis_at_wh(i);
                        if (tmpStart < first_start_millis)
                            first_start_millis = tmpStart;
                        // get last end_millis
                        tmpRampup = metrics[j].rampup_millis_at_wh(i);
                        if (tmpRampup > last_rampup_millis)
                            last_rampup_millis = tmpRampup;
                        // get first rampdown_millis
                        tmpRampdown = metrics[j].rampdown_millis_at_wh(i);
                        if (tmpRampdown < first_rampdown_millis)
                            first_rampdown_millis = tmpRampdown;
                        // get last end_millis
                        tmpEnd = metrics[j].end_millis_at_wh(i);
                        if (tmpEnd > last_end_millis)
                            last_end_millis = tmpEnd;
                    }
                }
                // The earliest end Ramdown must be after the latest end to the
                // measurement interval
                if ((first_rampdown_millis > last_end_millis)
                        && (first_start_millis > last_rampup_millis)) {
                    instanceChecksPassed = true;
                }
                else {
                    System.out.println("INVALID MULTI-JVM Run: Test Interval "
                            + i + ": Instance Timing Checks: FAILED");
                    System.out
                            .println("(first_rampdown_millis > last_end_millis) && (first_start_millis > last_rampup_millis))");
                    System.out.println("first_start_millis    = "
                            + first_start_millis);
                    System.out.println("last_rampup_millis    = "
                            + last_rampup_millis);
                    System.out.println("first_rampdown_millis = "
                            + first_rampdown_millis);
                    System.out.println("last_end_millis       = "
                            + last_end_millis);
                    instanceChecksPassed = false;
                }
            }
        }
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
        boolean validRun = true;
        String msg = "";
        for (int i = 0; i < metrics.length; i++) {
            if (!metrics[i].valid)
                validRun = false;
        }
        if (!instanceChecksPassed) {
            validRun = false;
            msg = "JVM Instance Timing Checks Failed";
        }
        try {
            out.println("<HTML><HEAD>\n"
                    + "<META NAME=\"GENERATOR\" CONTENT=\""
                    + "SPEC Java Reporter\">\n" + "<TITLE> " + name
                    + "</TITLE>\n" + "</HEAD>");
            out.print("<BODY ");
            if (validRun)
                out.print(results[0].get("spec.report.bodyAttributes", ""));
            else {
                metrics[0].relocateImageResource("invalid.gif",
                        output_directory);
                out.print("BACKGROUND=\"invalid.gif\"");
            }
            out.print(">\n");
            out.print("<TABLE WIDTH=100%>\n" + "<TR><TD COLSPAN=2>");
            if (issue != null && !issue.equals("")) {
                metrics[0].relocateImageResource("spec-sm.gif",
                        output_directory);
                out.print(logo);
            }
            else
                out.print("<FONT SIZE=+2></FONT>&nbsp;");
            if (!validRun)
                out.print("<FONT SIZE=+2> Invalid Run: </FONT>&nbsp");
            out.print("<FONT SIZE=+2>" + name + "2005</FONT></TD>\n"
                    + "<TD COLSPAN=2 ALIGN=RIGHT bgcolor=FFFF00>"
                    + "    <FONT SIZE=+2 COLOR=000000><B> bops = " + metric1
                    + ",  bops/JVM = " + metric2 + "</B></FONT></TD></TR>\n"
                    + "<TR><TD COLSPAN=3><FONT SIZE=+1>" + hardware
                    + "</FONT></TD></TR>\n");
            if (software != null)
                out.print("<TR><TD COLSPAN=3><FONT SIZE=+1>" + software
                        + "</FONT></TD></TR>\n");
	    if (msg != "") {
		out.print("<TR><TD COLSPAN=3><font size=+2>INVALID because " 
			  + msg + "</TD><TR>");
	    }
            out.println("</TABLE>");
            // Table with scores per each JVM and the graph-------------
            out
                    .println("<P> <TABLE WIDTH=100%><TR><TD ALIGN=TOP>"
                            + "<CENTER>"
                            + "<P><TABLE BORDER=1><TR><TH> JVM run </TH><TH> JVM Scores </TH></TR>");
            for (int i = 0; i < metrics.length; i++) {
                out.println("<TR><TD>" + (i + 1) + "</TD><TD>" + metric[i]
                        + "</TD></TR>");
            }
            out.println("<TR><TH COLSPAN=2> bops = "
                    + metric1
                    + ", bops/JVM = "
                    + metric2
                    + "</TABLE></TD><TD>"
                    + Metrics.multiCurvesGraph(metrics, "000", SPECInternalUse,
                            output_directory) + "</TD></TABLE>");
            // Software and hardware information ------------------------------
            out.print("<P>\n");
            BufferedReader in;
            BufferedReader in_2;
            TableGroupCompare m;
            in = new BufferedReader(new StringReader(new Template()
                    .getTemplateString()));
            in_2 = new BufferedReader(new StringReader(new Template()
                    .getTemplateString()));
            m = new TableGroupCompare(in, in_2, results[0], null, titles);
            out.println(m.toString());
            // Details of each VM run ------------------------------------------
            for (int i = 0; i < metrics.length; i++) {
                out.println("<p><p><hr><center><b><font size=+2><a href="
                        + htmlFiles[i].getName() + "> JVM " + (i + 1)
                        + " Scores: </a></font></b></center>");
                // out.println(metrics[i].detail());
                out.println(graphTable[i]);
                testBar(out);
            }
            // SPECjbb version
            // --------------------------------------------------
            out.println("<HR><FONT SIZE=-1>\n");
            out
                    .println("SPECjbb2005 Version: ["
                            + results[0].get("config.benchmark_version", "N/A")
                            + ", "
                            + results[0].get("config.benchmark_versionDate",
                                    "-") + "]");
            out.println("<BR><I>\n");
            if (issue != null && !issue.equals("") && validRun)
                out.println("Published by SPEC " + issue + "<br>");
            out.println("Reporting page, Copyright &copy; 2005 SPEC. "
                    + "All rights reserved\n" + "</I></FONT>");
            out.println("<!-- ");
            out.println("Reporting Page Generator " + version);
            if (echoRaw)
                for (int i = 0; i < results.length; i++) {
                    results[i]
                            .store(out, "Raw Results for " + (i + 1) + " JVM");
                }
            out.println(" -->");
            out.print("</BODY></HTML>\n");
            out.close();
        }
        catch (Exception e) {
            JBButil.getLog().log(Level.WARNING, "Error: " + e, e);
        }
    }

    public void testBar(PrintStream out) throws IOException {
        String license = results[0].get("config.test.specLicense", "MISSING");
        String testedBy = results[0].get("config.test.testedBy", "MISSING");
        String testDate = results[0].get("config.test.date", "MISSING");
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

    public void generateSingleRawFile() {
        Enumeration propNames;
        PrintStream outRawFile;
        ReportProps totalProps = new ReportProps();
        String name, value;
        String singleRawFileName = output_directory + File.separator
                + "SPECjbb.raw";
        File singleRawFile = new File(singleRawFileName);
        if (singleRawFile.exists()) {
            singleRawFile.delete();
        }
        for (Enumeration e = results[0].propertyNames(); e.hasMoreElements();) {
            name = (String) e.nextElement();
            if (!name.startsWith("result.")) {
                value = results[0].get(name);
                totalProps.put("global." + name, value);
            }
        }
        for (int i = 0; i < results.length; i++) {
            for (Enumeration e = results[i].propertyNames(); e
                    .hasMoreElements();) {
                name = (String) e.nextElement();
                value = results[i].get(name);
                if (name.startsWith("result.")) {
                    totalProps.put((i+1) + "." + name, value);
                }
            }
        }
        try {
            outRawFile = new PrintStream(singleRawFileName);
            Set keys = totalProps.keySet();
            Vector keyvec = new Vector(keys);
            Collections.sort(keyvec);
            for (int i = 0; i < keyvec.size(); i++) {
                String propsKey = (String) keyvec.elementAt(i);
                String svalue = totalProps.getProperty(propsKey);
                outRawFile.println(propsKey + "=" + svalue);
            }
        }
        catch (FileNotFoundException e) {
            JBButil.getLog().log(Level.WARNING,
                    "ERROR: file not found " + singleRawFileName, e);
            System.exit(1);
        }
    }
}
