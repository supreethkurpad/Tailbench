/*
 *
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 * Copyright (c) 2000-2005 Hewlett-Packard        All rights reserved.
 * Copyright (c) 1997-2005 Sun Microsystems, Inc. All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 *
 */
// This is not a properties file. Sequence of lines and blocks of lines
// is significant. It is used to generate a properties object
// Format of Table entries is: group column
// It is expected that browsers will break pages between groups
// Version @(#)report	1.5 04/14/98
package spec.reporter;

public class Template {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                          + "All rights reserved,"
                                          + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    String              t;

    public Template() {
        t = "Title Hardware\n" + "Table 0 0 0\n" + "config.hw.vendor\n"
                + "config.hw.vendor.url\n" + "config.hw.model\n"
                + "config.hw.processor\n" + "config.hw.MHz\n"
                + "config.hw.nchips\n" + "config.hw.ncores\n"
                + "config.hw.ncoresperchip\n" + "config.hw.hwThreads\n"
                + "config.sw.procsAvailtoJava\n" + "config.hw.memory\n"
                + "config.hw.memory_description\n" + "config.hw.primaryCache\n"
                + "config.hw.secondaryCache\n" + "config.hw.otherCache\n"
                + "config.hw.fileSystem\n" + "config.hw.disk\n"
                + "config.hw.other\n" + "\n" + "Title Software\n"
                + "Table 0 1 0\n" + "config.sw.vendor\n"
                + "config.sw.vendor.url\n" + "config.sw.JVM\n"
                + "config.sw.command_line\n" + "config.sw.JVMheapInitial\n"
                + "config.sw.JVMheapMax\n" + "config.sw.JVMbitness\n"
                + "java.class.path\n" + "sun.boot.class.path\n"
                + "config.sw.OS\n" + "config.sw.other\n" + "\n"
                + "Title AOT Compilation\n" + "Table 1 1 1\n"
                + "config.sw.aot\n" + "\n" + "Title Tuning\n" + "Table 1 1 1\n"
                + "config.sw.tuning\n" + "\n" + "Title Test Information\n"
                + "Table 1 0 0\n" + "config.test.testedBy\n"
                + "config.test.specLicense\n" + "config.test.location\n"
                + "config.test.date\n" + "config.hw.available\n"
                + "config.sw.JVMavailable\n" + "config.sw.OSavailable\n"
                + "config.sw.otherAvailable\n" + "\n" + "Title Notes\n"
                + "Table 1 1 1\n" + "config.sw.notes\n" + "\n";
    }

    public String getTemplateString() {
        return t;
    }
}
