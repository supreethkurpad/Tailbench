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
package spec.reporter;

import java.io.*;
import java.util.StringTokenizer;
import java.util.logging.Level;
import spec.jbb.JBButil;

public class TableGroup {
    // This goes right after each class/interface statement
    static final String     COPYRIGHT   = "SPECjbb2005,"
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
    public final static int MAXGROUPS   = 10;

    public final static int MAXROWS     = 10;

    public final static int MAXCOLUMNS  = 3;

    Table[][][]             tables      = new Table[MAXGROUPS][MAXCOLUMNS][MAXROWS];

    int[][]                 rows        = new int[MAXGROUPS][MAXCOLUMNS];

    boolean[][]             anyEntry    = new boolean[MAXGROUPS][MAXCOLUMNS];

    private ReportProps     props;

    private static String   tableBegin  = "<TABLE WIDTH=\"100%\" BORDER=0 CELLSPACING=0 CELLPADDING=2>\n";

    private static String   firstColumn = "<TR><TD WIDTH=\"50%\" VALIGN=\"TOP\">\n";

    private static String   otherColumn = "</TD><TD WIDTH=\"50%\" VALIGN=\"TOP\">\n";

    private static String   tableEnd    = "</TD></TR></TABLE>";

    // /////////////////////////////////////
    // instance variable field declarations
    // /////////////////////////////////////
    // /////////////////////////////////////
    // constructor declarations
    // /////////////////////////////////////
    TableGroup(BufferedReader in, ReportProps props, ReportProps titles)
            throws IllegalArgumentException {
        this.props = props;
        String line;
        String tableTitle = "Untitled";
        try {
            while ((line = in.readLine()) != null) {
                if (line.startsWith("Title")) {
                    tableTitle = line.substring(5).trim();
                }
                else if (line.startsWith("Table")) {
                    StringTokenizer token = new StringTokenizer(line
                            .substring(5));
                    int group = Integer.parseInt(token.nextToken());
                    int column = Integer.parseInt(token.nextToken());
                    boolean freeForm = (Integer.parseInt(token.nextToken()) == 1);
                    if (group < 0 || group >= MAXGROUPS || column < 0
                            || column >= MAXCOLUMNS) {
                        throw new IllegalArgumentException(tableTitle);
                    }
                    int row = rows[group][column]++;
                    if (row < 0 || row >= MAXROWS) {
                        throw new IllegalArgumentException(tableTitle);
                    }
                    tables[group][column][row] = new Table(tableTitle, in,
                            props, titles, freeForm);
                    anyEntry[group][column] = true;
                }
            }
        }
        catch (IOException e) {
            JBButil.getLog().log(Level.WARNING, "Error: " + e, e);
        }
    }

    public String toString() {
        StringBuffer buf = new StringBuffer(tableBegin);
        Table t;
        for (int ig = 0; ig < MAXGROUPS; ig++) {
            if (!anyEntry[ig][0])
                continue;
            buf.append(firstColumn);
            for (int ir = 0; ir < MAXROWS; ir++) {
                t = tables[ig][0][ir];
                if (t != null)
                    buf.append(t.toString());
            }
            for (int ic = 1; ic < MAXCOLUMNS; ic++) {
                if (!anyEntry[ig][ic])
                    continue;
                buf.append(otherColumn);
                for (int ir = 0; ir < MAXROWS; ir++) {
                    t = tables[ig][ic][ir];
                    if (t != null)
                        buf.append(t.toString());
                }
            }
        }
        buf.append(tableEnd);
        return buf.toString();
    }
}
