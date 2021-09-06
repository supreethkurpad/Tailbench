/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 2000-2005 Hewlett-Packard All rights reserved.
 * Copyright (c) 1997-2005 Sun Microsystems, Inc. All rights reserved.
 * 
 * This source code is provided as is, without any express or implied warranty.
 *  
 */
package spec.jbb.validity;

import java.io.*;
import java.security.*;

public class digest {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "Copyright (c) 2005 Hewlett-Packard,"
                                          + "All rights reserved,"
                                          + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    boolean             debug;

    public digest() {
        debug = false;
    };

    public boolean crunch_jar(String name) {
        boolean correct = true;
        String path = System.getProperty("java.class.path");
        // find jbb.jar
        int index_jar = path.indexOf("jbb.jar");
        // check that either it's at char 1
        String jar_name = "jbb.jar";
        if (index_jar < 0) {
            correct = false;
            System.out.println("jbb.jar not in CLASSPATH");
            return false;
        }
        if (index_jar > 0) {
            // or there's a File.separator before it by 1
            if (path.charAt(index_jar - 1) != File.separatorChar) {
                correct = false;
                return false;
            }
            // AND no path.separator before it
            int index_separator = path.indexOf(File.pathSeparatorChar);
            if (index_separator < index_jar) {
                System.out
                        .println("fails validation because something is before jbb.jar in CLASSPATH");
                correct = false;
                return false;
            }
            // fill in full name 0 - end of ".jar"
            jar_name = path.substring(0, index_jar + 7);
        }
        // open that File
        try {
            FileInputStream the_jar = new FileInputStream(jar_name);
            // DigestInputStream digestible_jar = new
            // DigestibleInputStream(the_jar);
            digestExpected e = new digestExpected();
            byte[] expected = e.getArray();
            MessageDigest md = MessageDigest.getInstance("SHA");
            DigestInputStream dis = new DigestInputStream(the_jar, md);
            int count = (int) (new File(jar_name).length());
            for (int i = 0; i < count; i++)
                dis.read();
            byte a[] = md.digest();
            for (int i = 0; i < 10; i++) {
                if (debug)
                    System.out.println(", " + a[i]);
                if (a[i] != expected[i])
                    correct = false;
            }
            System.out.println("jar validity is " + correct);
            return correct;
        }
        catch (Exception e) {
            System.out.println("digest:  caught exception " + e);
        }
        return false;
    }
}
