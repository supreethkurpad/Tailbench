/*
 * Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved.
 * 
 * 2005/03/01 Veeru: Created first version - Multi-jvm Controller
 */
package spec.jbb;

import java.io.*;
import java.net.*;

public class Controller {
    // This goes right after each class/interface statement
    static final String   COPYRIGHT        = "SPECjbb2005,"
                                                   + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                                   + "All rights reserved,"
                                                   + "Licensed Materials - Property of SPEC"; ;

    private static int    port             = 1500;

    private static String host             = "localhost";

    private static String defaultOutputDir = JBBmain.defaultOutputDir;

    private static String outputDir        = null;

    private static String propFile         = JBBmain.defaultPropsFileName;

    private static String runOutputSubDir  = null;

    private static int[]  sequenceOfWarehouses;

    private static int    numInst          = 1;

    private static void readProperties() {
        JBBProperties prop = new JBBProperties(propFile);
        if (!prop.getProps()) {
            System.out
                    .println("ERROR:  Properties File error; please start again");
            return;
        }
        // output directory
        outputDir = prop.getOptionalProperty("input.output_directory");
        if (outputDir == null) {
            outputDir = defaultOutputDir;
        }
        // sequence of warehouses
        int i = 0;
        int seqLen;
        if (prop.sequenceOfWarehouses == null) {
            seqLen = (prop.endingNumberWarehouses - prop.startingNumberWarehouses)
                    / prop.incrementNumberWarehouses + 1;
            sequenceOfWarehouses = new int[seqLen];
            for (int num_wh = prop.startingNumberWarehouses; num_wh <= prop.endingNumberWarehouses; num_wh += prop.incrementNumberWarehouses) {
                sequenceOfWarehouses[i] = num_wh;
                i++;
            }
        }
        else {
            seqLen = prop.sequenceOfWarehouses.length;
            sequenceOfWarehouses = new int[seqLen];
            sequenceOfWarehouses = prop.sequenceOfWarehouses;
        }
        // number of JVM instances
        numInst = prop.jvm_instances;
    }

    private static void setOutputDirectory() {
        RunSequencer subdirRs = new RunSequencer(outputDir, "SPECjbbMultiJVM.",
                null);
        runOutputSubDir = outputDir + File.separator + "SPECjbbMultiJVM."
                + subdirRs.getSeqString();
        if (!initOutputDir(runOutputSubDir)) {
            return;
        }
    }

    private static boolean initOutputDir(String dir) {
        File output_directory_file = new File(dir);
        if (output_directory_file.exists()) {
            // File exists -- is it a directory?
            if (!output_directory_file.isDirectory()) {
                System.out
                        .println("ERROR:  Specified input.output_directory is not a directory:  "
                                + dir);
                return false;
            }
        }
        else { // Specified directory does not exist -- try to create
            if (!output_directory_file.mkdirs()) {
                System.out
                        .println("ERROR:  Cannot create input.output_directory:  "
                                + dir);
                return false;
            }
        }
        return true;
    }

    public static void main(String args[]) {
        int argIdx = 0;
        while (argIdx != args.length) {
            if (args[argIdx].matches("-port")) {
                port = Integer.parseInt(args[argIdx + 1]);
                argIdx += 2;
            }
            else if (args[argIdx].matches("-propfile")) {
                propFile = args[argIdx + 1];
                argIdx += 2;
            }
        }
        readProperties();
        setOutputDirectory();
        ServerSocket server_soc[] = new ServerSocket[numInst + 1];
        Socket soc[] = new Socket[numInst + 1];
        BufferedReader socIn[] = new BufferedReader[numInst + 1];
        PrintWriter socOut[] = new PrintWriter[numInst + 1];
        try {
            // Create a socket for each inst and do accept
            for (int i = 1; i <= numInst; ++i) {
                server_soc[i] = new ServerSocket(port + i);
                // System.out.println("Waiting for instance " + i + " on port "
                // +
                // server_soc[i].getLocalPort());
                soc[i] = server_soc[i].accept();
                System.out.println("Accepted client " + soc[i].getInetAddress()
                        + ":" + soc[i].getPort());
                socIn[i] = new BufferedReader(new InputStreamReader(soc[i]
                        .getInputStream()));
                socOut[i] = new PrintWriter(new OutputStreamWriter(soc[i]
                        .getOutputStream()));
            }
            // Now do the cycle for wh sequence
            for (int seqndx = 0; seqndx < sequenceOfWarehouses.length; seqndx++) {
                int wh = sequenceOfWarehouses[seqndx];
                // System.out.println(wh
                // + " wh: Reading READY state from the jbb instances");
                for (int i = 1; i <= numInst; ++i) {
                    // System.out.println("Waiting for READY from " + i);
                    String mesg = "NULL";
                    String needMsg = i + ":READY";
                    while (!mesg.matches(needMsg))
                        mesg = socIn[i].readLine();
                }
                // System.out.println("Received READY from all, now send
                // START");
                for (int i = 1; i <= numInst; ++i) {
                    // System.out.println("Sending START to " + i);
                    String mesg = i + ":START";
                    socOut[i].println(mesg);
                    socOut[i].flush();
                }
                // Read the FINISHED state only for the last warehouse
                if (seqndx == sequenceOfWarehouses.length - 1) {
                   System.out.println(wh + " wh: Reading FINISHED state from the jbb instances");
                    for (int i = 1; i <= numInst; ++i) {
                        System.out.println("Waiting for FINISHED from " + i);
                        String mesg = "NULL";
                        String needMsg = i + ":FINISHED";
                        String exitMsg = i + ":EXIT";
                        while (!mesg.matches(needMsg))
                            mesg = socIn[i].readLine();
                        socOut[i].println(exitMsg);
                        System.out.println("Got"+ mesg);
                                            }
                }
            }
            // Now when all the instances have sent FINISHED,
            // summarize the reports
            System.out.println("Waiting 20 seconds for all jvm instance to exit");
            //Thread.sleep(10000);
            System.out
                    .println("Received FINISHED from all jvm instances, now summarize the reports...");
            // Names of thouse parameters are same as in JBBmain
            boolean opts = false; // SPECInternalUse
            String optn = null; // newsletter
            boolean opth = false; // useHtml
            boolean opte = true; // echoRaw
            spec.reporter.MultiVMReport r = new spec.reporter.MultiVMReport(
                    runOutputSubDir, opts, optn, opth, opte);
            r.print(runOutputSubDir + File.separator + "index.html");
            spec.reporter.MultiVMTxtReport ar = new spec.reporter.MultiVMTxtReport(
                    optn, runOutputSubDir, opts);
            ar.print(runOutputSubDir + File.separator + "MultiVMReport.txt");
            System.out.println("benchmark run complete.");
            System.exit(0);
        }
        catch (Exception excep) {
            System.err.println(excep + ": error");
            System.exit(1);
        }
    }
}
