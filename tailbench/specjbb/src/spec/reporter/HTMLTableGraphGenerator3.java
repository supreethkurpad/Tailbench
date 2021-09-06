/*
 *
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 * Copyright (c) 2000-2005 Hewlett-Packard        All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 *
 */
// HTMLTableGraphGenerator.java - 11/06/1999 - By Jason Glick, SPEC.
package spec.reporter;

import java.io.*;
import java.awt.*;
import java.util.*;
import java.util.logging.Level;
import java.text.*;
import spec.jbb.JBButil;

public class HTMLTableGraphGenerator3 {
    // This goes right after each class/interface statement
    static final String           COPYRIGHT         = "SPECjbb2005,"
                                                            + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                                            + "All rights reserved,"
                                                            + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                                            + "All rights reserved,"
                                                            + "Licensed Materials - Property of SPEC";

    public static final int       X                 = 0;

    public static final int       Y                 = 1;

    public static final int       DIM               = 2;

    public static final int       MINX              = 0;

    public static final int       MAXX              = 1;

    public static final int       MINY              = 2;

    public static final int       MAXY              = 3;

    public static final String    DEFAULT_XAXIS     = "x";

    public static final String    DEFAULT_YAXIS     = "y";

    public static final int       DEFAULT_PADSIZE   = 2;

    public static final int       DEFAULT_PIXELSIZE = 2;

    public static final Color     DEFAULT_FGCOLOR   = Color.red;

    public static final Color     DEFAULT_BGCOLOR   = Color.black;

    public static final Dimension DEFAULT_SIZE      = new Dimension(50, 50);

    public static final double[]  DEFAULT_SCALE     = new double[] {
            -5, 5, -5, 5
                                                    };

    public static final String    DEFAULT_HTMLFILE  = "HTMLTableGraph.html";

    private int                   pad;

    private String                pixel;

    private int                   pixelsize;

    private Dimension             size;

    private String                xaxis;

    private String                yaxis;

    private String                fgcolor;

    private String                bgcolor;

    private boolean               bgcolorEnabled;

    private String                pixelcolor;

    private boolean               pixelcolorEnabled;

    private double                alphax;

    private double                alphay;

    private final Hashtable       xypairs           = new Hashtable();

    private final double[]        scale             = new double[DEFAULT_SCALE.length];

    private final DecimalFormat   dfx               = new DecimalFormat("#.##");

    private final DecimalFormat   dfy               = new DecimalFormat("#.##");

    public HTMLTableGraphGenerator3() {
        setTableSize(DEFAULT_SIZE);
        setPixelSize(DEFAULT_PIXELSIZE);
        setPixelPadSize(DEFAULT_PADSIZE);
        setForegroundColor(DEFAULT_FGCOLOR);
        setBackgroundColor(DEFAULT_BGCOLOR);
        setAxesLabels(DEFAULT_XAXIS, DEFAULT_YAXIS);
        setAxesLabelsPrecision(0, 0);
    }

    private String toHexString(Color c) {
        return "#" + toHexString(c.getRed()) + toHexString(c.getGreen())
                + toHexString(c.getBlue());
    }

    private String toHexString(int value) {
        String s = Integer.toHexString(value);
        return (s.length() == 1) ? "0" + s : s;
    }

    public void setBackgroundColorEnabled(boolean bgcolorEnabled) {
        this.bgcolorEnabled = bgcolorEnabled;
    }

    public boolean isBackgroundColorEnabled() {
        return bgcolorEnabled;
    }

    public void setPixelBorderColorEnabled(boolean pixelcolorEnabled) {
        this.pixelcolorEnabled = pixelcolorEnabled;
    }

    public boolean isPixelBorderColorEnabled() {
        return pixelcolorEnabled;
    }

    public void setPixelBorderColor(Color c) {
        if (c == null)
            return;
        pixelcolor = toHexString(c);
        setPixelBorderColorEnabled(true);
    }

    public void setBackgroundColor(Color c) {
        if (c == null)
            return;
        bgcolor = toHexString(c);
        setBackgroundColorEnabled(true);
    }

    public void setForegroundColor(Color c) {
        if (c == null)
            return;
        fgcolor = toHexString(c);
    }

    public void setTableSize(Dimension size) {
        if (size == null)
            return;
        this.size = size;
        setScale(scale);
    }

    public void setPixelSize(int pixelsize) {
        this.pixelsize = pixelsize;
        setPixelImage(pixel);
    }

    public void setPixelPadSize(int pad) {
        this.pad = pad;
    }

    public void setPixelImage(String pixel) {
        pixel = "<IMG SRC=" + pixel + " WIDTH=" + pixelsize + " HEIGHT="
                + pixelsize + ">";
        this.pixel = pixel;
    }

    public void setScale(double[] scale) {
        if (scale == null || scale.length < this.scale.length)
            return;
        System.arraycopy(scale, 0, this.scale, 0, this.scale.length);
        alphax = (this.scale[MAXX] - this.scale[MINX]) / size.width;
        alphay = (this.scale[MAXY] - this.scale[MINY]) / size.height;
    }

    private int getLinearMapX(double x) {
        return (int) Math.round((x - scale[MINX]) / alphax);
    }

    private int getLinearMapY(double y) {
        return (int) Math.round((scale[MAXY] - y) / alphay);
    }

    private double getInverseLinearMapX(int x) {
        return (scale[MINX] + x * alphax);
    }

    private double getInverseLinearMapY(int y) {
        return (scale[MAXY] - y * alphay);
    }

    public void setAxesLabels(String xaxis, String yaxis) {
        this.xaxis = xaxis;
        if (yaxis.length() > 1) {
            char[] c = yaxis.toCharArray();
            yaxis = "";
            for (int i = 0; i < c.length; i++) {
                yaxis += "&nbsp;" + c[i] + "&nbsp;<BR>";
            }
        }
        this.yaxis = yaxis;
    }

    public void setAxesLabelsPrecision(int x, int y) {
        dfx.setMinimumIntegerDigits(1);
        dfx.setMinimumFractionDigits(x);
        dfx.setMaximumFractionDigits(x);
        dfy.setMinimumIntegerDigits(1);
        dfy.setMinimumFractionDigits(y);
        dfy.setMaximumFractionDigits(y);
    }

    public void clearXYPairs() {
        xypairs.clear();
    }

    public void addXYPair(double[] xypair) {
        if (xypair == null || xypair.length != DIM)
            return;
        String lmx = Integer.toString(getLinearMapX(xypair[X]));
        String lmy = Integer.toString(getLinearMapY(xypair[Y]));
        if (!xypairs.containsKey(lmx)) {
            xypairs.put(lmx, new Vector());
        }
        ((Vector) xypairs.get(lmx)).addElement(lmy);
    }

    public void setXYPairs(double[][] xypairs) {
        if (xypairs == null)
            return;
        for (int i = 0; i < xypairs.length; i++) {
            addXYPair(xypairs[i]);
        }
    }

    public void setXYPairs(Vector xypairs) {
        if (xypairs == null)
            return;
        Enumeration e = xypairs.elements();
        while (e.hasMoreElements()) {
            addXYPair((double[]) e.nextElement());
        }
    }

    public void generate() {
        generate(DEFAULT_HTMLFILE);
    }

    public void generate(String filename) {
        if (filename == null)
            return;
        try {
            FileWriter fw = new FileWriter(filename);
            fw.write(generateString());
            fw.flush();
            fw.close();
        }
        catch (IOException ioe) {
            JBButil.getLog().log(Level.WARNING,
                    "IOException: " + ioe.getMessage(), ioe);
        }
        catch (Exception e) {
            JBButil.getLog().log(Level.WARNING, "Exception: " + e.getMessage(),
                    e);
            e.printStackTrace();
        }
    }

    public String generateString() {
        StringBuffer buf = new StringBuffer();
        buf.append("<TABLE BORDER=1 CELLSPACING=0 CELLPADDING=0><TR><TD>");
        buf.append("<TABLE BORDER=0 CELLSPACING=" + pad + " CELLPADDING=" + pad
                + ((bgcolorEnabled) ? " BGCOLOR=" + bgcolor : "") + " WIDTH="
                + (size.width * pixelsize) + ">\n");
        for (int y = 0; y < size.height; y++) {
            buf.append("<TR>");
            if (y == 0) {
                buf.append("<TD ALIGN=center VALIGN=middle ROWSPAN="
                        + size.height + " BGCOLOR=#ffffff>" + yaxis + "</TD>");
            }
            if (y % 10 == 0) {
                double d = getInverseLinearMapY(y);
                String label = dfy.format(d).replace('-', ' ').trim();
                String color = (d < 0) ? "COLOR=#ff0000" : "COLOR=#000000";
                buf
                        .append("<TD BGCOLOR=white NOWRAP ROWSPAN=10 ALIGN=right VALIGN=top>"
                                + "<FONT SIZE=1 "
                                + color
                                + ">"
                                + label
                                + "</FONT></TD>\n");
            }
            for (int x = 0; x < size.width; x++) {
                String plot = "";
                String sx = Integer.toString(x);
                String sy = Integer.toString(y);
                if (xypairs.containsKey(sx)) {
                    Vector v = (Vector) xypairs.get(sx);
                    Enumeration e = v.elements();
                    while (e.hasMoreElements()) {
                        String svy = e.nextElement().toString();
                        if (svy.equals(sy)) {
                            plot = pixel;
                            // v.removeElement( svy );
                            break;
                        }
                    }
                }
                buf
                        .append("<TD WIDTH="
                                + pixelsize
                                + ((pixelcolorEnabled && !plot.equals("")) ? " BGCOLOR="
                                        + pixelcolor
                                        : "") + ">" + plot + "</TD>");
            }
            buf.append("</TR>\n");
        }
        buf.append("<TR><TD BGCOLOR=#ffffff COLSPAN=2 ROWSPAN=2>&nbsp;</TD>\n");
        for (int x = 0; x < size.width; x++) {
            if (x % 10 == 0) {
                double d = getInverseLinearMapX(x);
                String label = dfx.format(d).replace('-', ' ').trim();
                String color = (d < 0) ? "COLOR=#ff0000" : "COLOR=#000000";
                buf
                        .append("<TD ALIGN=left BGCOLOR=white NOWRAP COLSPAN=10 VALIGN=top>"
                                + "<FONT SIZE=1 "
                                + color
                                + ">"
                                + label
                                + "</FONT></TD>\n");
            }
        }
        buf.append("</TR><TR><TD COLSPAN=" + size.width
                + " ALIGN=center BGCOLOR=#ffffff>" + xaxis + "</TD></TR>");
        buf.append("</TABLE></TD></TR></TABLE>");
        return buf.toString();
    }
}
