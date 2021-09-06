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
import java.util.Vector;

public class Table {
    // This goes right after each class/interface statement
    static final String    COPYRIGHT   = "SPECjbb2005,"
                                               + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                               + "All rights reserved,"
                                               + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                               + "All rights reserved,"
                                               + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                               + "All rights reserved,"
                                               + "Licensed Materials - Property of SPEC";

    // /////////////////////////////////////
    // class variable field declarations
    // /////////////////////////////////////
    private static String  tablePrefix = "<TABLE WIDTH=\"100%\" BORDER=1 CELLSPACING=2 CELLPADDING=0>"
                                               + "<TR><TH COLSPAN=2>\n";

    // /////////////////////////////////////
    // instance variable field declarations
    // /////////////////////////////////////
    private boolean        free;                                                         // free

    // format
    // table.
    // newline
    // substitution.
    // no
    // title
    private String         heading;

    private Vector<String> items;

    private ReportProps    props;

    private ReportProps    titles;

    // /////////////////////////////////////
    // constructor declarations
    // /////////////////////////////////////
    Table(String h, BufferedReader in, ReportProps props, ReportProps titles,
            boolean free) {
        heading = h;
        this.props = props;
        this.titles = titles;
        this.free = free;
        items = new Vector<String>();
        String line;
        try {
            while ((line = in.readLine()) != null) {
                if (line.equals(""))
                    break;
                items.addElement(line);
            }
        }
        catch (IOException e) {
        }
    }

    private String expandN(String s) {
        StringBuffer sb = new StringBuffer();
        char[] nb = new char[s.length()];
        s.getChars(0, nb.length, nb, 0);
        boolean sub = false;
        for (int i = 0; i < nb.length - 1; i++)
            if (nb[i] == '\\' && nb[i + 1] == 'n') {
                sb.append('\n');
                i++;
                sub = true;
            }
            else {
                sb.append(nb[i]);
                sub = false;
            }
        if (!sub)
            sb.append(nb[nb.length - 1]);
        return sb.toString();
    }

    private String insertBreakAfterColons(String s) {
        StringBuffer sb = new StringBuffer();
        char[] c = new char[s.length()];
        s.getChars(0, c.length, c, 0);
        for (int i = 0; i < c.length; i++) {
            sb.append(c[i]);
            if (i > 1 && (c[i] == ':' || c[i] == ';')) {
                if (c[i - 2] != ':' && c[i - 2] != ';') {
                    sb.append("<BR>");
                }
            }
        }
        return sb.toString();
    }

    public String toString() {
        StringBuffer buf = new StringBuffer(tablePrefix);
        buf.append(heading);
        buf.append("</TH></TR>\n");
        for (int i = 0; i < items.size(); i++) {
            String name = (String) items.elementAt(i);
            String label = titles.get(name);
            if (label == null)
                label = name + "=null";
            String val = props.get(name);
            if (val == null || val.equals("")) {
                val = "&nbsp;";
            }
            // Better display of path info?
            else if (name.endsWith(".path")) {
                val = insertBreakAfterColons(val);
            }
            else if (val.startsWith("http://")) {
                val = "<a href=\"" + val + "\">" + val + "</a>";
            }
            if (free) {
                buf.append("<TR><TD>");
                buf.append(expandN(val));
            }
            else {
                buf.append("<TR><TD><B>");
                buf.append(label);
                buf.append("</B></TD>\n<TD>");
                buf.append(val);
            }
            buf.append("</TD></TR>\n");
        }
        buf.append("</TABLE>\n");
        return buf.toString();
    }
}
