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
import java.util.logging.Level;
import spec.jbb.JBButil;

public class MultiVMTxtReport {
    // This goes right after each class/interface statement
    static final String      COPYRIGHT = "SPECjbb2005,"
                                               + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                               + "All rights reserved,"
                                               + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                               + "All rights reserved,"
                                               + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                               + "All rights reserved,"
                                               + "Licensed Materials - Property of SPEC";

    public String            hardware, metric1, metric2, software;

    public String            base;

    private TextColumn       columnTitle;

    private TextColumn       columnValue;

    public String            issue;

    private String           memory;

    public String            memoryDescription;

    public TextMetrics       metrics[];

    public String            metric[];

    private final static int pageWidth = 120;

    public ReportProps       results[];

    public boolean           SPECInternalUse;

    public boolean           validRun  = true;

    public String            output_directory;

    public int               instances            = 0;

    public boolean           instanceChecksPassed = true;

    public MultiVMTxtReport(String newsletter, String output_directory,
            boolean SPECInternalUse) {
        this.issue = newsletter;
        this.SPECInternalUse = SPECInternalUse;
        this.output_directory = output_directory;
        String prefix = "SPECjbb.";
        String suffix = ".raw";
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
        // Get .raw files from the output_direcory
        FilenameFilter filter = new JBBReportFilenameFilter(prefix, suffix);
        File[] resultFiles = dir.listFiles(filter);
        instances = resultFiles.length;
        results = new ReportProps[resultFiles.length];
        metrics = new TextMetrics[resultFiles.length];
        metric = new String[resultFiles.length];
        double sum = 0;
        boolean isMetricsDouble = true;
        for (int i = 0; i < resultFiles.length; i++) {
            results[i] = new ReportProps();
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
            metrics[i] = new TextMetrics(results[i], SPECInternalUse);
            metric[i] = metrics[i].metric();
            try {
                sum += Double.parseDouble(metric[i]);
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
        this.hardware = get("config.hw.vendor") + " " + get("config.hw.model");
        for (int i = 0; i < metrics.length; i++) {
            if (!metrics[i].valid)
                validRun = false;
        }
        /*
         * if (validRun) { metrics.output_properly("Valid run, Score is " +
         * metric + "\n"); } else { if (metrics.print_score) {
         * metrics.output_properly("INVALID run; Score is " + metric + "\n"); }
         * else metrics .output_properly("INVALID run, preventing score
         * calculation."); }
         */
        this.software = get("config.sw.vendor") + " " + get("config.sw.JVM");
        this.memory = results[0].get("config.hw.memory");
        this.memoryDescription = results[0].get("config.hw.memory_description");
    }

    private String configSection() {
        /*
         * Create title and value columns for the left side of the page and join
         * them, linked horizontally by logical elements Then flatten it into a
         * single block of text
         */
        TextBlock left = new TextBlock(37, "HARDWARE");
        columnTitle = new TextColumn(16);
        columnValue = new TextColumn(20);
        cput("config.hw.vendor", "Vendor");
        cput("config.hw.vendor.url", "Vendor URL");
        cput("config.hw.model", "Model");
        cput("config.hw.processor", "Processor");
        cput("config.hw.MHz", "MHz");
        cput("config.hw.nchips", "# of Chips");
        cput("config.hw.ncores", "# of Cores");
        cput("config.hw.ncoresperchip", "# of Cores/Chip");
        cput("config.hw.hwThreads", "HW Thresads");
        cput("config.sw.procsAvailtoJava", "Procs avail to Java");
        cput("config.hw.memory", "Memory (MB)");
        cput("config.hw.memory_description", "Memory Details");
        cput("config.hw.primaryCache", "L1 Cache");
        cput("config.hw.secondaryCache", "L2 Cache");
        cput("config.hw.otherCache", "Other Cache");
        cput("config.hw.fileSystem", "Filesystem");
        cput("config.hw.disk", "Disks");
        cput("config.hw.other", "Other H/W");
        left.add(columnTitle.join(columnValue, " ").merge());
        /*
         * Create title and value columns for the right side of the page and
         * join them, linked horizontally by logical elements Then flatten it
         * into a single block of text
         */
        TextBlock right = new TextBlock(37, "SOFTWARE");
        columnTitle = new TextColumn(16);
        columnValue = new TextColumn(20);
        cput("config.sw.vendor", "Vendor");
        cput("config.sw.vendor.url", "Vendor URL");
        cput("config.sw.JVM", "JVM Version");
        cput("config.sw.command_line", "Command Line");
        cput("config.sw.JVMheapInitial", "Initial Heap Mem (MB)");
        cput("config.sw.JVMheapMax", "Max Heap Mem (MB)");
        cput("config.sw.JVMbitness", "JVM addressing bits");
        // Report classpaths
        cput("java.class.path", "JVM CLASSPATH");
        cput("java.library.path", "JVM BOOTCLASSPATH");
        cput("config.sw.OS", "OS Version");
        cput("config.sw.other", "Other S/W");
        right.add(columnTitle.join(columnValue, " ").merge());
        right.add("");
        right.add("AOT Compilation Details");
        if ((results[0].get("config.sw.aot")) != null)
            right.add(results[0].get("config.sw.aot"));
        right.add("");
        right.add("TUNING AND NOTES");
        if ((results[0].get("config.sw.tuning")) != null)
            right.add(results[0].get("config.sw.tuning"));
        if ((results[0].get("config.sw.notes")) != null)
            right.add(results[0].get("config.sw.notes"));
        right.add("");
        /*
         * Save these two columns, and then start again top justifying a new set
         * of text, so that it looks like the HTML.
         */
        TextBlock top = left.join(right);
        top.add("");
        /*
         * Create title and value columns for the left side of the page and join
         * them, linked horizontally by logical elements Then flatten it into a
         * single block of text
         */
        /*
         * Create title and value columns for the right side of the page and
         * join them, linked horizontally by logical elements Then flatten it
         * into a single block of text
         */
        right = new TextBlock(37, "TEST INFORMATION");
        columnTitle = new TextColumn(18);
        columnValue = new TextColumn(18);
        cput("config.test.testedBy", "Tested by");
        cput("config.test.specLicense", "SPEC License");
        cput("config.test.location", "Test Location");
        cput("config.test.date", "Test Date");
        cput("config.sw.JVMavailable", "JVM available");
        cput("config.hw.available", "H/w available");
        cput("config.sw.OSavailable", "OS available");
        cput("config.sw.otherAvailable", "Other s/w available");
        right.add(columnTitle.join(columnValue, " ").merge());
        /*
         * Finally, add onto the top section, and return the resulting string
         */
        top.add(right);
        return top.toString();
    }

    private void cput(String name, String title) {
        columnTitle.add(title);
        columnValue.add(get(name));
    }

    private String get(String name) {
        return get(name, "MISSING");
    }

    private String get(String name, String def) {
        String s = results[0].get(name, def);
        if (s.indexOf('<') >= 0) {
            int i = s.indexOf("<i>");
            if (i >= 0)
                s = s.substring(0, i) + s.substring(i + 3);
            i = s.indexOf("<I>");
            if (i >= 0)
                s = s.substring(0, i) + s.substring(i + 3);
            i = s.indexOf("</i>");
            if (i >= 0)
                s = s.substring(0, i) + s.substring(i + 4);
            i = s.indexOf("</I>");
            if (i >= 0)
                s = s.substring(0, i) + s.substring(i + 4);
        }
        return s;
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
        String msg = "";
        if (!instanceChecksPassed) {
            validRun = false;
            msg = "JVM Instance Timing Checks Failed";
        }
        try {
            String notValid = "";
            if (!validRun)
                notValid = "Invalid";
            out.println(notValid + " SPECjbb2005                bops = "
                    + metric1 + ", bops/JVM = " + metric2);
            // out.println (TextBlock.justifyRight (metric, pageWidth));
            out.println();
	    if (msg != "") {
		out.print("INVALID because " + msg);
		out.println();
		out.println();
	    }
            out.println(hardware);
            out.println(software);
            testBar(out);
            out.println();
            out.println("              Multi JVM Mode");
            out.println();
            out.println("     JVM run                  JVM Scores");
            for (int i = 0; i < metrics.length; i++) {
                out.println("        " + (i + 1) + "                        "
                        + metric[i]);
            }
            out.println("     bops=" + metric1 + ", bops/JVM=" + metric2);
            out.println();
            out.println(configSection());
            for (int i = 0; i < metrics.length; i++) {
                out.println();
                out.println("                   JVM " + (i + 1) + " Scores");
                out.println(metrics[i].ratioTable());
                out.println();
            }
            out
                    .println("\nSPECjbb2005 Version: ["
                            + results[0].get("config.benchmark_version", "N/A")
                            + ", "
                            + results[0].get("config.benchmark_versionDate",
                                    "-") + "]");
            if (issue != null && !issue.equals("") && validRun)
                out.println("Published by SPEC " + issue);
            out.println("Reporting page (C) Copyright SPEC, 2005. "
                    + "All rights reserved\n");
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
        out.println("SPEC license # " + license + ",  " + "Tested by: "
                + testedBy + ",  " + "Test date: " + testDate);
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
                // The earliest end Ramdown must be after the latest end to the
                // measurement interval
                if ((first_rampdown_millis > last_end_millis)
                        && (first_start_millis > last_rampup_millis)) {
                    instanceChecksPassed = true;
                }
                else {
                    instanceChecksPassed = false;
                }
            }
        }
    }

}// end class
