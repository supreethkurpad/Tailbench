/*
 *
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 * Copyright (c) 2000-2005 Hewlett-Packard        All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 *
 */
package spec.jbb;

import java.io.*;

class ResFilter implements FilenameFilter {
    // This goes right after each class/interface statement
    static final String COPYRIGHT    = "SPECjbb2005,"
                                             + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                             + "All rights reserved,"
                                             + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                             + "All rights reserved,"
                                             + "Licensed Materials - Property of SPEC";

    String              resultPrefix;                                                  // =

    // "SPECjbb.";
    String              resultSuffix = ".results";

    int                 resultPrefixLen;                                               // =

    // resultPrefix.length();
    int                 resultSuffixStart;

    public ResFilter(String prefix, String suffix) {
        resultPrefix = prefix;
        resultPrefixLen = resultPrefix.length();
        String resultSuffix = suffix;
        resultSuffixStart = resultPrefixLen + 3;
    }

    public boolean accept(File dir, String name) {
        return ((name.startsWith(resultPrefix)) && ((name
                .substring(resultSuffixStart).equals(resultSuffix)) || (resultSuffixStart == name
                .length())));
    }
}

class PrintLastSeq {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "Copyright (c) 2005 Hewlett-Packard,"
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    public static void main(String[] args) {
        String dir = ".";
        if (args.length == 1)
            dir = args[0];
        String resultPrefix = "SPECjbb.";
        String resultSuffix = ".raw";
        // int resultPrefixLen = resultPrefix.length();
        RunSequencer rs = new RunSequencer(dir, resultPrefix, resultSuffix);
        System.out.println(rs.padNumber(rs.getSeq() - 1));
    }
}

public class RunSequencer {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "Copyright (c) 2005 Hewlett-Packard,"
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    String              resultPrefix;

    String              resultSuffix;

    int                 resultPrefixLen;

    File                f1;

    public RunSequencer(String dir, String prefix, String suffix) {
        f1 = new File(dir);
        resultPrefix = prefix;
        resultPrefixLen = resultPrefix.length();
        resultSuffix = suffix;
    }

    public int getSeq() {
        int i;
        String[] ls;
        FilenameFilter filter = new ResFilter(resultPrefix, resultSuffix);
        int foo = 0;
        int max = 0;
        String fooString;
        int fooLen;
        int j;
        boolean skip = false;
        for (ls = f1.list(filter), i = 0; ls != null && i < ls.length; i++) {
            fooString = ls[i].substring(resultPrefixLen, resultPrefixLen + 3);
            fooLen = fooString.length();
            skip = false;
            for (j = 0; j < fooLen; j++) {
                if (!Character.isDigit(fooString.charAt(j))) {
                    skip = true;
                }
            }
            if (!skip) {
                foo = Integer.parseInt(fooString);
                if (foo > max)
                    max = foo;
            }
        }
        return (max + 1);
    }

    public String getSeqString() {
        return padNumber(getSeq());
    }

    public String padNumber(int n) {
        String returnString = "" + n;
        int returnStringLength = returnString.length();
        if (returnStringLength == 1) {
            returnString = "00" + returnString;
        }
        if (returnStringLength == 2) {
            returnString = "0" + returnString;
        }
        return returnString;
    }
}
