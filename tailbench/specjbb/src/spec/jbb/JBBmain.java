/*
 *
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 *
 * 2005/03/22 Veeru: Multi-JVM changes
 */
package spec.jbb;

import spec.jbb.validity.Check;
import spec.jbb.validity.digest;
import java.io.*;
import java.util.*;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import java.text.DateFormat;
import spec.reporter.*;
import java.net.Socket;

import tbench.*;

public class JBBmain implements Runnable {
    // This goes right after each class/interface statement
    static final String          COPYRIGHT             = "SPECjbb2005,"
                                                               + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                                               + "All rights reserved,"
                                                               + "(C) Copyright IBM Corp., 1996 - 2005"
                                                               + "All rights reserved,"
                                                               + "Licensed Materials - Property of SPEC";

    public static final String   Version               = "SPECjbb2005 1.04";

    public static final String   VersionDate           = "June 13, 2005";

    public static final String[] Header                = {
            "",
            "Licensed Materials - Property of SPEC",
            "SPECjbb2005",
            "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                    + "All rights reserved,"
                                                       };

    public static final String[] TPC_FAIR_USE          = {
            "",
            "Licensed Materials - Property of SPEC",
            "SPECjbb2005",
            "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                    + "All rights reserved,"
                    + "(C) Copyright IBM Corp., 1996 - 2005"
                    + "All rights reserved," + "",
            "This source code is provided as is, without any express or implied warranty.",
            "",
            "TPC Fair Use policy:",
            "",
            "SPECjbb2005 is not a TPC Benchmark. SPECjbb2005 results are not comparable with",
            "any TPC Benchmark results. The workload used by SPECjbb2005 is inspired",
            "by the TPC-C specification, TPC Benchmark C , Standard Specification,",
            "Revision 3.2, August 27 1996. TPC Benchmark is a trademark of the Transaction",
            "Processing Performance Council."
                                                       };

    private static long          deterministic_seed    = 2108417082252868L;

    public static final short    NON_NUMERIC_ENTRY     = -99;

    public static Company        myCompany             = null;

    public Vector<Thread>        threadList;

    public Vector<Short>         whIdStack;

    private int                  testnum               = 0;

    private int                  warehousePopulationBase;

    private int                  orderlinesPerOrder;

    private int                  waitTimePercent;

    private boolean              forceGC;

    private boolean              screenWriteFlag;

    private boolean              steadyStateFlag;

    public static boolean        uniformRandomItems;

    public static int            overrideItemTableSize = 0;

    public static int            maxWh                 = 2;

    static private PrintStream   outResultsFile        = null;

    static private PrintStream   outRawFile            = null;

    static private PrintStream   outDeliveriesFile     = null;

    // multi-JVM variables
    static public boolean        multiJVMMode          = false;

    static public int            instanceId            = -1;

    static public int            port                  = 1500;

    static public BufferedReader socIn;

    static public PrintWriter    socOut;

    static public String         defaultOutputDir      = "results";

    static public String         defaultPropsFileName  = "SPECjbb.props";

    JBBProperties                prop;

    public void run() {
        TransactionManager transMgr = null;
        // Vector.remove(n) method returns n-element and deletes it
        short wId = ((Short) whIdStack.remove(0)).shortValue();
        int maxwh = myCompany.getMaxWarehouses();
        if ((wId > 0) && (wId <= myCompany.getMaxWarehouses())) {
            transMgr = new TransactionManager();
            transMgr.initTransactionManager(myCompany, wId);
            // add transMgr instance to ArrayList in Company
            myCompany.addWarehouseThread(transMgr);
            transMgr.go();
        }
        else {
            JBButil.getLog().warning(
                    "IMPOSSIBLE ERROR: Invalid Warehouse passed in.  Value was "
                            + wId + " and should be between 1 and "
                            + myCompany.getMaxWarehouses());
            JBButil.SecondsToSleep(15);
        }
    }

    public synchronized void startJBBthread(short whID) {
        if (threadList == null) {
            threadList = new Vector<Thread>();
        }
        ;
        if (whIdStack == null) {
            whIdStack = new Vector<Short>();
        }
        Thread whThread = new Thread(this);
        threadList.add(whThread);
        whIdStack.add(whID);
        whThread.start();
    };

    public synchronized void stopJBBthread() {
        while (!threadList.isEmpty()) {
            try {
                while (((Thread) threadList.firstElement()).isAlive()) {
                    JBButil.SecondsToSleep(1);
                }
                threadList.remove(0);
            }
            catch (NoSuchElementException e) {
                System.out.println("No first element in the thread list: ");
                e.printStackTrace();
            }
        }
    }

    public void DoARun(Company myCompany, short number_of_warehouses,
            int rampup_time, int measurement_time) {
        JBButil.getLog().entering("spec.jbb.JBBmain", "DoARun");
        short whID;
        testnum++;
        myCompany.setPropOutputPrefix("result.test" + testnum + ".");
        System.out.println("Start User Threads");
        JBButil.getLog().info("Start User Threads");
        myCompany.prepareForStart();
        tbench.tBenchServerInit(number_of_warehouses);
        try {
            for (whID = 1; whID <= number_of_warehouses; whID++) {
                myCompany.startAutomated(whID, rampup_time, measurement_time);
                startJBBthread(whID);
                String msg = "  started user thread for Warehouse " + whID;
                System.out.println(msg);
                JBButil.getLog().info(msg);
                // JBButil.SecondsToSleep(1);
            }
        }
        catch (java.lang.ThreadDeath e) {
            try {
                // Be careful that we do not run out of memory trying to log a
                // problem that may have been caused by running out of memory.
                JBButil.getLog().log(
                        Level.WARNING,
                        "ERROR:  A thread died, probably out of memory."
                                + "  Increase the heap size and run again", e);
            }
            catch (OutOfMemoryError oome) {
                // Ok, the logging did not work, so just print a message and
                // stack trace.
                System.out
                        .println("ERROR:  A thread died, probably out of memory."
                                + "  Increase the heap size and run again");
                e.printStackTrace();
            }
        }
        catch (java.lang.OutOfMemoryError e) {
            try {
                // Be careful that we do not run out of memory trying to log a
                // problem caused by running out of memory.
                JBButil.getLog().log(
                        Level.WARNING,
                        "ERROR:  Out of memory error caught! "
                                + "  Increase the heap size and run again.", e);
            }
            catch (OutOfMemoryError oome) {
                // Ok, the logging did not work, so just print a message and
                // stack trace.
                System.out.println("ERROR:  Out of memory error caught! "
                        + "  Increase the heap size and run again.");
                e.printStackTrace();
            }
        }
        myCompany.displayResultTotals(prop.showWarehouseDetail);
        stopJBBthread();
        JBButil.getLog().exiting("spec.jbb.JBBmain", "DoARun");
        // Push all of the logged messages out after each run
        flushLog();
        tbench.tBenchServerFinish();
    }

    public boolean DoAValidationRun(Company myCompany) {
        short whID;
        myCompany.prepareForStart();
        myCompany.startValidation((short) 1); // one warehouse
        TransactionManager transMgr = new TransactionManager();
        transMgr.initTransactionManager(myCompany, (short) 1); // one warehouse
        return transMgr.goValidate();
    }

    public void doIt() {
        JBButil.getLog().entering("spec.jbb.JBBmain", "DOIT");
        // min_btps = min_tpmc *
        // (2.3 = all_transactions / new_order_transactions) /
        // (60 = seconds / minute)
        float min_btps = (float) ((prop.minBTPSRatio * 100.0 / prop.waitTimePercent) * 2.3 / 60.0);
        Transaction.setOrderLineCount(prop.orderlinesPerOrder);
        myCompany = new Company();
        // handle deterministic_random_seed
        if (prop.deterministicRandomSeed == true) {
            JBButil.set_random_seed(deterministic_seed);
        }
        myCompany.setJVMInstanceValues(prop.jvm_instances);
        myCompany.setMultiJVMRampingValues(prop.per_jvm_warehouse_rampup,prop.per_jvm_warehouse_rampdown);
        myCompany.setPopulationValues(prop.warehousePopulationBase,
                JBBProperties.overrideItemTableSize);
        Transaction.setLogWrite(prop.screenWriteFlag);
        Transaction.setSteadyState(prop.steadyStateFlag);
        int cur_warehouses = 0;
        int num_wh;
        if (prop.sequenceOfWarehouses == null) {
            for (num_wh = prop.startingNumberWarehouses; num_wh <= prop.endingNumberWarehouses; num_wh += prop.incrementNumberWarehouses) {
                if (!runWarehouse(cur_warehouses, num_wh, min_btps))
                    break;
                cur_warehouses = num_wh;
            }
        }
        else {
            for (int seqndx = 0; seqndx < prop.sequenceOfWarehouses.length; seqndx++) {
                num_wh = prop.sequenceOfWarehouses[seqndx];
                if (!runWarehouse(cur_warehouses, num_wh, min_btps))
                    break;
                cur_warehouses = num_wh;
            }
        }
        JBButil.getLog().exiting("spec.jbb.JBBmain", "DOIT");
    }

    public boolean doItForValidation() {
        Transaction.setOrderLineCount(orderlinesPerOrder);
        myCompany = new Company();
        long validationSeed = 528562479389981L;
        JBButil.set_random_seed(validationSeed);
        myCompany.setPopulationValues(warehousePopulationBase);
        Transaction.setLogWrite(screenWriteFlag);
        Transaction.setSteadyState(steadyStateFlag);
        Transaction.setValidation(true);
        increaseNumWarehouses(0, 1, waitTimePercent);
        boolean runValid = DoAValidationRun(myCompany);
        Transaction.setValidation(false);
        return runValid;
    }

    private void increaseNumWarehouses(int current, int next,
            int waitTimePercent) {
        JBButil.getLog().entering("spec.jbb.JBBmain", "increaseNumWarehouses");
        for (int i = current + 1; i <= next; i++) {
            myCompany.primeWithDummyData((short) i, 0);
            myCompany.getTimerDataPtr((short) i).useWaitTimesPercentage(
                    waitTimePercent);
        }
        JBButil.getLog().exiting("spec.jbb.JBBmain", "increaseNumWarehouses");
    }

    public boolean runWarehouse(int cur_warehouses, int num_wh, float min_btps) {
        JBButil.getLog().entering("spec.jbb.JBBmain", "runWarehouse");
        System.out
                .println("\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        increaseNumWarehouses(cur_warehouses, num_wh, prop.waitTimePercent);
        int measurementSeconds = 0;
        if (num_wh < prop.expectedPeakWarehouse) {
            DoARun(myCompany, (short) myCompany.getMaxWarehouses(), 0,
                    prop.rampupSeconds);
        }
        else {
            DoARun(myCompany, (short) myCompany.getMaxWarehouses(), 0,
                    prop.measurementSeconds);
        }
        if (prop.checkThroughput) {
            TimerData companyTimerDataPtr = myCompany
                    .getTimerDataPtr((short) 0);
            double result = companyTimerDataPtr.getBTPS();
            if (result < (min_btps * num_wh)) {
                System.out.println("result below min for warehouse");
                JBButil.getLog().exiting("spec.jbb.JBBmain", "runWarehouse");
                return false;
            }
        }
        JBButil.getLog().exiting("spec.jbb.JBBmain", "runWarehouse");
        return true;
    }

    public String commandLineParser(String args[]) {
        String s = null;
        if (args.length == 0) {
            s = defaultPropsFileName;
        }
        else if (args[0].equals("-id")) {
            JBBmain.instanceId = Integer.parseInt(args[1]);
        }
        else if (args[0].equals("-propfile")) {
            if (args.length == 2) {
                s = args[1];
            }
            else {
                if (args.length == 1) {
                    System.out.println("Missing properties file name");
                    System.out
                            .println("   Parameters:  -propfile <properties_file_name> [-id <instance_id>]");
                }
                else if (args.length == 4 && args[2].equals("-id")) {
                    s = args[1];
                    JBBmain.instanceId = Integer.parseInt(args[3]);
                }
                else {
                    System.out.println("Too many parameters");
                    System.out
                            .println("   Parameters:  -propfile <properties_file_name> [-id <instance_id>]");
                }
            }
        }
        else {
            System.out.println("Unrecognized command line parameter:  "
                    + args[0]);
            System.out
                    .println("   Parameters:  -propfile <properties_file_name>");
        }
        return s;
    }

    public boolean initOutputDir(String outputDir) {
        if (outputDir == null) {
            // output_directory not specified -- take default
            outputDir = defaultOutputDir;
        }
        File output_directory_file = new File(outputDir);
        if (output_directory_file.exists()) {
            // File exists -- is it a directory?
            if (!output_directory_file.isDirectory()) {
                System.out
                        .println("ERROR:  Specified input.output_directory is not a directory:  "
                                + outputDir);
                return false;
            }
        }
        else { // Specified directory does not exist -- try to create
            if (!output_directory_file.mkdirs()) {
                System.out
                        .println("ERROR:  Cannot create input.output_directory:  "
                                + outputDir);
                return false;
            }
        }
        return true;
    }

    public void callReporter(String output_directory, String outRawFile_name,
            String outRawPrefix, String sequenceNumber) {
        // amt: call Reporter
        String msg = "Calling Reporter";
        JBButil.getLog().info(msg);
        System.out.println();
        System.out.println();
        System.out.println(msg);
        boolean opte = true;
        boolean opth = false;
        String optr = outRawFile_name;
        String optl = sequenceNumber; // seq #
        boolean opts = false;
        String optn = null;
        String opto = output_directory + File.separator + outRawPrefix
                + sequenceNumber + ".html";
        String file_Ascii = output_directory + File.separator + outRawPrefix
                + sequenceNumber + ".txt";
        String optc = null;
        String optS = null;
        boolean optv = false;
        spec.reporter.Report r;
        try {
            TextiReport ar = new spec.reporter.TextiReport(optn, optr, opts);
            ar.print(file_Ascii);
        }
        catch (Exception e) {
        }
        try {
            r = new spec.reporter.Report(opte, opts, optn, optr, optv, optc,
                    optl, opth, output_directory);
        }
        catch (Exception e) {
            opth = true;
            JBButil
                    .getLog()
                    .warning(
                            "Producing html chart in report instead of JPEG; see Users' Guide");
            r = new spec.reporter.Report(opte, opts, optn, optr, optv, optc,
                    optl, opth, output_directory);
        }
        catch (java.lang.InternalError e) {
            opth = true;
            JBButil
                    .getLog()
                    .warning(
                            "Producing html chart in report instead of JPEG; see Users' Guide");
            r = new spec.reporter.Report(opte, opts, optn, optr, optv, optc,
                    optl, opth, output_directory);
        }
        catch (java.lang.UnsatisfiedLinkError e) {
            opth = true;
            JBButil
                    .getLog()
                    .warning(
                            "Producing html chart in report instead of JPEG; see Users' Guide");
            r = new spec.reporter.Report(opte, opts, optn, optr, optv, optc,
                    optl, opth, output_directory);
        }
        catch (java.lang.Error e) {
            opth = true;
            JBButil
                    .getLog()
                    .warning(
                            "Producing html chart in report instead of JPEG; see Users' Guide");
            r = new spec.reporter.Report(opte, opts, optn, optr, optv, optc,
                    optl, opth, output_directory);
        }
        r.print(opto);
        try {
            BufferedReader AscBr = new BufferedReader(
                    new FileReader(file_Ascii));
            String s;
            while ((s = AscBr.readLine()) != null) {
                System.out.println(s);
            }
        }
        catch (IOException e) {
            JBButil.getLog().log(Level.WARNING,
                    "Error opening ASCII output file", e);
        }
        System.out.println("Output files: " + file_Ascii + ", "
                + outRawFile_name + ", " + opto);
        System.out.println();
        System.out.println();
        System.out.println("Reporter messages:");
        System.out.println(r.messages());
    }

    public static void main(String args[]) {
        JBBmain main;
        TransactionManager transMgr;
        short whID = 0;
        byte tID = 0;
        String outRawPrefix = "SPECjbb.";
        String outRawSuffix = ".raw";
        boolean passed_200_check = !(Check.doCheck());
        main = new JBBmain();
        main.warehousePopulationBase = 30;
        main.orderlinesPerOrder = 10;
        main.waitTimePercent = 0;
        main.forceGC = false;
        main.screenWriteFlag = false;
        main.steadyStateFlag = true;
        JBBmain.uniformRandomItems = true;
        // adding java.awt.headless=true to the system properties
        String oldprop = System.setProperty("java.awt.headless", "true");
        // System.out.println(java.awt.GraphicsEnvironment.isHeadless());
        PrintStream scratch1 = null;
        PrintStream scratch2 = null;
        // The DOIT_validation method is going to do some transactions before we
        // are
        // ready to initialize the application logger. So for now we will use
        // the
        // default global logger until we are ready to create the real one.
        JBButil.setLog(Logger.getLogger("global"));
        JBButil.getLog().setLevel(Level.WARNING);
        // Enclose the entire "main" execution of the application in a try
        // region
        // so that if there is an unhandled exception we can flush and close the
        // logging file stream before ending the application.
        try {
            try {
                scratch1 = new PrintStream(new FileOutputStream(
                        "JBB.temp.scratch1"));
                scratch2 = new PrintStream(new FileOutputStream(
                        "JBB.temp.scratch2"));
            }
            catch (IOException e) {
                JBButil.getLog().log(Level.WARNING,
                        "VALIDATION ERROR:  IOException: " + e.getMessage(), e);
            }
            Company.setOutputs(scratch1, scratch2);
            boolean passed_validation = main.doItForValidation();
            main = null;
            scratch1.close();
            scratch2.close();
            File tfile = new File("JBB.temp.scratch1");
            tfile.delete();
            tfile = new File("JBB.temp.scratch2");
            tfile.delete();
            main = new JBBmain();
            main.prop = new JBBProperties(main.commandLineParser(args));
            if (!main.prop.getProps()) {
                System.out
                        .println("ERROR:  Properties File error; please start again");
                return;
            }
            overrideItemTableSize = JBBmain.overrideItemTableSize;
            // elian: Single and Multi JVM run
            // String output_directory = main.prop
            // .getOptionalProperty("input.output_directory");
            // if (!main.initOutputDir(output_directory)) {
            // return;
            // }
            String output_directory;
            if (main.prop.jvm_instances > 1) {
                JBBmain.multiJVMMode = true;
            }
            if (!multiJVMMode) {
                output_directory = main.prop
                        .getOptionalProperty("input.output_directory")
                        + File.separator + "SPECjbbSingleJVM";
                if (!main.initOutputDir(output_directory)) {
                    return;
                }
            }
            else {
                String parentDir = main.prop
                        .getOptionalProperty("input.output_directory");
                RunSequencer currentDirRS = new RunSequencer(parentDir,
                        "SPECjbbMultiJVM.", "");
                output_directory = parentDir + File.separator
                        + "SPECjbbMultiJVM."
                        + (currentDirRS.padNumber(currentDirRS.getSeq() - 1));
            }
            String sequenceNumber;
            if (main.prop.jvm_instances > 1) {
                String n = sequenceNumber = String.valueOf(instanceId);
                sequenceNumber = "" + n;
                int returnStringLength = sequenceNumber.length();
                if (returnStringLength == 1) {
                    sequenceNumber = "00" + sequenceNumber;
                }
                if (returnStringLength == 2) {
                    sequenceNumber = "0" + sequenceNumber;
                }
            }
            else {
                RunSequencer rs = new RunSequencer(output_directory,
                        outRawPrefix, outRawSuffix);
                sequenceNumber = rs.getSeqString();
            }
            String outResultsFile_name = output_directory + File.separator
                    + "SPECjbb." + sequenceNumber + ".results";
            String outRawFile_name = output_directory + File.separator
                    + outRawPrefix + sequenceNumber + outRawSuffix;
            String outDeliveriesFile_name = output_directory + File.separator
                    + "SPECjbb." + sequenceNumber + ".deliveries";
            String logFile_name = output_directory + File.separator
                    + "SPECjbb." + sequenceNumber + ".log";
            System.out.println("The results will be in: " + outRawFile_name);
            try {
                SaveOutput.start(outResultsFile_name);
                System.out.println("Opened " + outResultsFile_name);
                outRawFile = new PrintStream(new FileOutputStream(
                        outRawFile_name));
                System.out.println("Opened " + outRawFile_name);
                outDeliveriesFile = new PrintStream(new FileOutputStream(
                        outDeliveriesFile_name));
            }
            catch (IOException e) {
                // WRR: Added ": " for better readability.
                JBButil.getLog().log(Level.WARNING,
                        "ERROR:  IOException: " + e.getMessage(), e);
            }
            Company.setOutputs(outRawFile, outDeliveriesFile);
            // WRR: Converted to array to avoid use of \n.
            for (String str : Header) {
                System.out.println(str);
            }
            System.out.println("");
            System.out.println("Benchmark " + Version + " now Opening");
            System.out.println("");
            // Define maximum number of warehouses
            if (main.prop.sequenceOfWarehouses == null) {
                maxWh = main.prop.endingNumberWarehouses + 1;
            }
            else {
                maxWh = main.prop.sequenceOfWarehouses[main.prop.sequenceOfWarehouses.length - 1] + 1;
            }
            // Instantiate and initialize logging utilities for this application
            if (!main.initApplicationLogging(logFile_name)) {
                System.out.println("ERROR:  Logging initialization failed!");
                return;
            }
            // Logging has now been initialized. Log this event.
            JBButil.getLog().info("Logging started");
            main.prop.setProp("config.benchmark_version", Version);
            main.prop.setProp("config.benchmark_versionDate", VersionDate);
            main.prop.setProp("config.test.date", DateFormat.getDateInstance()
                    .format(new Date()));
            if (!main.prop.copyPropsToOutput(outRawFile)) {
                System.out
                        .println("ERROR:  Properties File error; please start again");
                return;
            }
            String value = main.prop.setProp("result.validity.200_check",
                    new Boolean(passed_200_check).toString());
            outRawFile.println("result.validity.200_check" + "=" + value);
            value = main.prop.setProp("result.validity.jbb_operation",
                    new Boolean(passed_validation).toString());
            outRawFile.println("result.validity.jbb_operation" + "=" + value);
            digest d = new digest();
            boolean _999_checkit = d.crunch_jar("jbb.jar");
            value = main.prop.setProp("result.validity.999_checkit",
                    new Boolean(_999_checkit).toString());
            outRawFile.println("result.validity.999_checkit" + "=" + value);
            main.prop.checkCompliance();
            // Before starting the main benchmark flush the logs
            main.flushLog();
            // set up socket communication for multi-jvm mode
            if (JBBmain.multiJVMMode) {
                String msg = "Running Multi-JVM Test: socket "
                        + (port + JBBmain.instanceId);
                JBButil.getLog().info(msg);
                System.out.println(msg);
                Socket soc = null;
                boolean trySucceeded = false;
                int tries = 0;
                while (!trySucceeded && (tries < 10)) {
                    try {
                        tries++;
                        if (tries > 1) {
                            Thread.sleep(5000);
                        }
                        soc = new Socket("localhost", port + JBBmain.instanceId);
                        trySucceeded = true;
                    }
                    catch (Exception e) {
                        /*
                         * JBButil.getLog().log(Level.WARNING, e + ": error in
                         * creating sockets, try again.", e);
                         */
                        trySucceeded = false;
                    }
                }
                if (tries == 10) {
                    JBButil.getLog().warning(
                            "10 failed socket connection attempts. Exiting..");
                    return;
                }
                try {
                    socIn = new BufferedReader(new InputStreamReader(soc
                            .getInputStream()));
                }
                catch (Exception e) {
                    JBButil.getLog().log(Level.WARNING,
                            e + ": error in setting socket input", e);
                    return;
                }
                try {
                    socOut = new PrintWriter(new OutputStreamWriter(soc
                            .getOutputStream()));
                }
                catch (Exception e) {
                    JBButil.getLog().log(Level.WARNING,
                            e + ": error in setting socket out", e);
                    return;
                }
            }
            main.doIt();
            // And again right after we are finished
            main.flushLog();
            if (JBBProperties.printPropertiesAndArgs) {
                // Puts sorted System Properties into Raw file
                Properties props = System.getProperties();
                Set keys = props.keySet();
                Vector keyvec = new Vector(keys);
                Collections.sort(keyvec);
                for (int i = 0; i < keyvec.size(); i++) {
                    String propsKey = (String) keyvec.elementAt(i);
                    String svalue = props.getProperty(propsKey);
                    outRawFile.println(propsKey + "=" + svalue);
                }
                // Puts command line args into raw file
                outRawFile.print("input.cmdline=");
                for (int i = 0; i < args.length; i++) {
                    outRawFile.print(args[i] + " ");
                }
                outRawFile.println();
            }
            main.callReporter(output_directory, outRawFile_name, outRawPrefix,
                    sequenceNumber);
            outDeliveriesFile.close();
            outRawFile.close();
            // elian: sending FINISHED state
            if (multiJVMMode) {
                String msg = JBBmain.instanceId + ":FINISHED";
                String exitMsg = JBBmain.instanceId + ":EXIT";
                JBButil.getLog().info(msg);
                System.out.println(msg);
                JBBmain.socOut.println(msg);
                System.out.println("Sent FINISHED message");
                JBBmain.socOut.flush();
                String mesg = "NULL";
                try {
                while ((mesg != null) && !mesg.matches(exitMsg))
                    mesg = JBBmain.socIn.readLine();
                }
                catch (NullPointerException e){
                    //do nothing
                }
                catch(java.net.SocketException se){
                    //do nothing here too
                }
                catch (Exception e){
                    JBButil.getLog().log(Level.WARNING,
                           e + ": error awaiting final exit message", e);
                   return;
                }
                System.out.println("Final EXIT reached");
            }
            SaveOutput.stop();
        }
        finally {
            // Logging has now been initialized. Log this event.
            JBButil.getLog().info("Logging ended");
            Handler[] handlers = JBButil.getLog().getHandlers();
            // Close all logging handlers
            for (Handler logHandler : handlers) {
                logHandler.close();
            }
        }
        //outResultsFile.close();
        System.exit(0);
    }

    private boolean initApplicationLogging(String logFileName) {
        StreamHandler handler = null;
        try {
            handler = new StreamHandler(new FileOutputStream(logFileName),
                    new JBBLogFormatter());
        }
        catch (FileNotFoundException fnfe) {
            JBButil.getLog().log(Level.WARNING,
                    "ERROR:  Unable to open logging file " + logFileName, fnfe);
            return false;
        }
        handler.setLevel(prop.getApplicationLoggingLevel());
        // Also create a handler for application error logging. Note that
        // we purposely ignore the application logging level, which is a
        // configurable property. This is done so that regardless of what
        // the user requests for logging, all errors will still be logged
        // (as they should be).
        ConsoleHandler errHandler = new ConsoleHandler();
        errHandler.setLevel(Level.WARNING);
        // Now create and initialize a specific logger for SPECjbb
        Logger appLog = Logger.getLogger("spec.jbb");
        appLog.setLevel(prop.getApplicationLoggingLevel());
        appLog.setUseParentHandlers(false);
        appLog.addHandler(handler);
        appLog.addHandler(errHandler);
        // Before replacing the existing (temporary) logger with the
        // official application logger, flush all handlers of the
        // temporary logger.
        flushLog();
        // Now we can store the application logger in a place where all
        // the other classes can easily (and quickly) access it.
        JBButil.setLog(appLog);
        return true;
    }

    private void flushLog() {
        Handler[] handlers = JBButil.getLog().getHandlers();
        // Close all logging handlers
        for (Handler logHandler : handlers) {
            logHandler.flush();
        }
    }
}
