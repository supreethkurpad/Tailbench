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
// titles for properties - @(#)title	1.10 07/28/98
// The fields below are recorded to document the run
// Titles enclosed in parentheses indicate information saved in the raw file
// but not printed on the reporting page.  E.g., the name of the person
// who ran the test This information is optional, and intended for the
// convenience of the tester to facilitate recording and reproducing test
// results.
package spec.reporter;

public class Title {
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

    public Title() {
        t =
        // property editor parameters
        "spec.initial.clientFile=Property file\n"
                + "spec.initial.serverFile=Server property file\n"
                + "spec.initial.maxRows=# of fields per edit panel\n"
                + "config.hw.vendor=Hardware Vendor\n"
                + "config.hw.vendor.url=Vendor URL\n"
                + "config.hw.model=Model\n" + "config.hw.processor=Processor\n"
                + "config.hw.MHz=MHz\n" + "config.hw.nchips=# of Chips\n"
                + "config.hw.ncores=# of Cores\n"
                + "config.hw.ncoresperchip=# of Cores/Chip\n"
                + "config.hw.hwThreads=HW Threading Enabled?\n"
                + "config.sw.procsAvailtoJava=Procs Avail to Java\n"
                + "config.hw.memory=Memory (MB)\n"
                + "config.hw.memory_description=Memory Details\n"
                + "config.hw.primaryCache=Primary cache\n"
                + "config.hw.secondaryCache=Secondary cache\n"
                + "config.hw.otherCache=Other cache\n"
                + "config.hw.fileSystem=Filesystem\n"
                + "config.hw.disk=Disks\n" + "config.hw.other=Other hardware\n"
                + "config.hw.available=H/w available\n"
                + "config.sw.vendor=Software Vendor\n"
                + "config.sw.vendor.url=Vendor URL\n"
                + "config.sw.JVM=JVM Version\n"
                + "config.sw.command_line=JVM Command Line\n"
                + "config.sw.JVMavailable=JVM available\n"
                + "config.sw.JVMheapInitial=JVM Initial Heap Memory (MB)\n"
                + "config.sw.JVMheapMax=JVM Maximum Heap Memory (MB) \n"
                + "config.sw.JVMbitness=JVM Address bits \n"
                + "java.class.path=JVM CLASSPATH\n"
                + "sun.boot.class.path=JVM BOOTCLASSPATH\n"
                + "config.sw.OS=OS Version\n"
                + "config.sw.OSavailable=OS available\n"
                + "config.sw.other=Other software\n"
                + "config.sw.otherAvailable=Other s/w available\n"
                + "config.sw.notes=Other Notes\n"
                + "config.test.testedBy=Tested by\n"
                + "config.test.specLicense=SPEC license #\n"
                + "config.test.location=Test location\n"
                + "config.test.date=Test date\n" +
                // test information not printed on reporting page
                "config.testx.emailTo=Mail test results to\n"
                + "config.testx.emailReturn=Your return email address\n"
                + "config.testx.mailHost=Outgoing SMTP mail server\n"
                + "config.testx.testedByName=Tested by (person)\n"
                + "config.testx.internalReference=Internal Reference\n" +
                // other information, not normally printed on results page
                "spec.other.testTime=Test time\n"
                + "spec.other.version=SPEC version\n"
                + "spec.other.versionDate=SPEC release date\n"
                + "spec.other.reportDate=Date report generated\n";
    }

    public String getTitleString() {
        return t;
    }
}
