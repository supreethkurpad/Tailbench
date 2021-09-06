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

public class TextiReport {
    // This goes right after each class/interface statement
    static final String      COPYRIGHT = "SPECjbb2005,"
                                               + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                               + "All rights reserved,"
                                               + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                               + "All rights reserved,"
                                               + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                               + "All rights reserved,"
                                               + "Licensed Materials - Property of SPEC";

    public String            hardware, metric, software;

    public String            base;

    private TextColumn       columnTitle;

    private TextColumn       columnValue;

    public String            issue;

    private String           memory;

    private String           memoryDescription;

    public TextMetrics       metrics;

    private final static int pageWidth = 120;

    public ReportProps       results   = new ReportProps();

    public boolean           SPECInternalUse;

    public TextiReport(String newsletter, String resultFile,
            boolean SPECInternalUse) {
        this.issue = newsletter;
        this.SPECInternalUse = SPECInternalUse;
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
        metrics = new TextMetrics(results, SPECInternalUse);
        this.hardware = get("config.hw.vendor") + " " + get("config.hw.model");
        if (metric == null)
            metric = metrics.metric();
        if (metrics.valid) {
            metrics.output_properly("Valid run, Score is " + metric + "\n");
        }
        else {
            if (metrics.print_score) {
                metrics.output_properly("INVALID run; Score is " + metric
                        + "\n");
            }
            else
                metrics
                        .output_properly("INVALID run, preventing score calculation.");
        }
        this.software = get("config.sw.vendor") + " " + get("config.sw.JVM");
        this.memory = results.get("config.hw.memory");
        this.memoryDescription = results.get("config.hw.memory_description");
    }

    private String configSection() {
        /*
         * Create title and value columns for the left side of the page and join
         * them, linked horizontally by logical elements Then flatten it into a
         * single block of text
         */
        TextBlock left = new TextBlock(51, "HARDWARE");
        columnTitle = new TextColumn(25);
        columnValue = new TextColumn(25);
        cput("config.hw.vendor", "Vendor");
        cput("config.hw.vendor.url", "Vendor URL");
        cput("config.hw.model", "Model");
        cput("config.hw.processor", "Processor");
        cput("config.hw.MHz", "MHz");
        cput("config.hw.nchips", "# of Chips");
        cput("config.hw.ncores", "# of Cores");
        cput("config.hw.ncoresperchip", "# of Cores/Chip");
        cput("config.hw.hwThreads", "HW Threading Enabled");
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
        TextBlock right = new TextBlock(65, "SOFTWARE");
        columnTitle = new TextColumn(20);
        columnValue = new TextColumn(44);
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
        if ((results.get("config.sw.aot")) != null)
            right.add(results.get("config.sw.aot"));
        right.add("");
        right.add("TUNING AND NOTES");
        if ((results.get("config.sw.tuning")) != null)
            right.add(results.get("config.sw.tuning"));
        if ((results.get("config.sw.notes")) != null)
            right.add(results.get("config.sw.notes"));
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
        String s = results.get(name, def);
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
        try {
            out.println("SPECjbb2005                     " + metric + " bops");
            // out.println (TextBlock.justifyRight (metric, pageWidth));
            out.println();
            out.println(hardware);
            out.println(software);
            testBar(out);
            out.println(metrics.ratioTable());
            out.println();
            out.println(configSection());
            out.println(metrics.detail());
            out.println("\nSPECjbb2005 Version: ["
                    + results.get("config.benchmark_version", "N/A") + ", "
                    + results.get("config.benchmark_versionDate", "-") + "]");
            if (issue != null && !issue.equals("") && metrics.valid)
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
        String license = results.get("config.test.specLicense", "MISSING");
        String testedBy = results.get("config.test.testedBy", "MISSING");
        String testDate = results.get("config.test.date", "MISSING");
        out.println("SPEC license # " + license + ",  " + "Tested by: "
                + testedBy + ",  " + "Test date: " + testDate);
    }
}// end class
