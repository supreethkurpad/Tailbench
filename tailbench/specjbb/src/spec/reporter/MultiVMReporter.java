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

import java.io.File;

public class MultiVMReporter {
    // This goes right after each class/interface statement
    static final String    COPYRIGHT = "SPECjbb2005,"
                                             + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                             + "All rights reserved,"
                                             + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                             + "All rights reserved,"
                                             + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                             + "All rights reserved,"
                                             + "Licensed Materials - Property of SPEC";

    private static boolean opta      = false;

    private static boolean opte      = true;

    private static boolean opth      = false;

    private static boolean opts      = false;

    private static String  optn;

    private static String  opto;

    private static String  optr;

    public static void main(String[] args) {
        if (!getOpt(args))
            return;
        if (opta) {
            if (opto == null)
                opto = optr + File.separator + "MultiVMReport.txt";
            MultiVMTxtReport r = new MultiVMTxtReport(optn, optr, opts);
            r.print(opto);
        }
        else {
            if (opto == null)
                opto = optr + File.separator + "index.html";
            MultiVMReport r = new MultiVMReport(optr, opts, optn, opth, opte);
            r.print(opto);
        }
    }

    private static boolean getOpt(String[] args) {
        try {
            for (int i = 0; i < args.length; i++)
                if (args[i].equals("-a"))
                    opta = true;
                else if (args[i].equals("-e"))
                    opte = false;
                else if (args[i].equals("-s"))
                    opts = true;
                else if (args[i].equals("-n"))
                    optn = args[++i];
                else if (args[i].equals("-o"))
                    opto = args[++i];
                else if (args[i].equals("-r"))
                    optr = args[++i];
                else
                    throw new ArrayIndexOutOfBoundsException();
        }
        catch (ArrayIndexOutOfBoundsException e) {
            usage();
            return false;
        }
        if (optr != null)
            return true;
        else
            usage();
        return false;
    }

    private static void usage() {
        System.out
                .println("Usage: java spec.reporter.MultiVMReporter [options]\n"
                        + "Required options are:\n"
                        + "-r ResultsDir     Directory with results files\n"
                        + "Other options are:\n"
                        + "-a                Plain ASCII text output\n"
                        + "                    default: generate HTML output with JPG graph\n"
                        + "-e                Do NOT echo raw results properties in HTML output\n"
                        + "                    default: raw results inserted as HTML comments\n"
                        + "-o Output         Output file for generated HTML or TXT\n"
                        + "                    default: index.html for HTML, MultiVMReport.txt for TXT\n"
                        + "                    (NB: html links are generated as relative to the\n"
                        + "                    directory with results files.)");
    }
}
