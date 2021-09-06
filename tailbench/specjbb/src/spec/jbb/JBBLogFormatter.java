/*
 * 
 * Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. 
 * Created on Nov 7, 2004
 */
package spec.jbb;

import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

public class JBBLogFormatter extends SimpleFormatter {
    static final String           COPYRIGHT       = "SPECjbb2005,"
                                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                                          + "All rights reserved,"
                                                          + "(C) Copyright IBM Corp., 1996 - 2005"
                                                          + "All rights reserved,"
                                                          + "Licensed Materials - Property of SPEC";

    protected static final String ARROW_SEPARATOR = " -> ";

    protected static final String NEW_LINE_STRING = System
                                                          .getProperty("line.separator");

    public String format(LogRecord record) {
        StringBuffer sb = new StringBuffer(super.format(record));
        int pos = sb.indexOf(JBBLogFormatter.NEW_LINE_STRING);
        if (pos != -1) {
            int end = pos + JBBLogFormatter.NEW_LINE_STRING.length();
            sb.replace(pos, end, JBBLogFormatter.ARROW_SEPARATOR);
        }
        return (sb.toString());
    }
}
