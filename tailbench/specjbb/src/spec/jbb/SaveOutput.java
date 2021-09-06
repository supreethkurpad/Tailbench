/*
 /*
 *
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 * Copyright (c) 1997-2005 Sun Microsystems, Inc. All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 *
 */
package spec.jbb;

import java.io.*;
import java.util.logging.Level;

class SaveOutput extends PrintStream {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    static OutputStream logfile;

    static PrintStream  oldStdout;

    static PrintStream  oldStderr;

    SaveOutput(PrintStream ps) {
        super(ps);
    }

    // Starts copying stdout and stderr to the file f.
    public static void start(String f) throws IOException {
        // Save old settings.
        oldStdout = System.out;
        oldStderr = System.err;
        // Create/Open logfile.
        logfile = new PrintStream(new BufferedOutputStream(
                new FileOutputStream(f)));
        // Start redirecting the output.
        System.setOut(new SaveOutput(System.out));
        System.setErr(new SaveOutput(System.err));
    }

    // Restores the original settings.
    public static void stop() {
        System.setOut(oldStdout);
        System.setErr(oldStderr);
        try {
            logfile.close();
        }
        catch (Exception e) {
            JBButil.getLog().log(Level.WARNING, "Exception: " + e.getMessage(),
                    e);
        }
    }

    public void write(int b) {
        try {
            logfile.write(b);
        }
        catch (Exception e) {
            JBButil.getLog().log(Level.WARNING, "Exception: " + e.getMessage(),
                    e);
            setError();
        }
        super.write(b);
    }

    public void write(byte buf[], int off, int len) {
        try {
            logfile.write(buf, off, len);
        }
        catch (Exception e) {
            JBButil.getLog().log(Level.WARNING, "Exception: " + e.getMessage(),
                    e);
            setError();
        }
        super.write(buf, off, len);
    }
}
