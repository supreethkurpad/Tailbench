/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 2000-2005 Hewlett-Packard All rights reserved.
 * 
 * This source code is provided as is, without any express or implied warranty.
 *  
 */
// GraphImage.java - 11/09/1999 - By Jason Glick, SPEC.
/*
 * A graphable subclass of java.awt.image.BufferedImage that knows how to save
 * itself as a JPEG image file.
 * 
 * 
 *  
 */
package spec.reporter;

import java.awt.*;
import java.awt.image.*;
import java.io.*;
import java.text.*;
import java.util.logging.Level;
import spec.jbb.JBButil;
import javax.imageio.*;

public class GraphImage extends BufferedImage {
    // This goes right after each class/interface statement
    static final String          COPYRIGHT                = "SPECjbb2005,"
                                                                  + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                                                  + "All rights reserved,"
                                                                  + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                                                  + "All rights reserved,"
                                                                  + "Licensed Materials - Property of SPEC";

    private double               prevx;

    private double               prevy;

    private boolean              prevSet;

    private double               alphax;

    private double               alphay;

    private int                  pixelsize;

    private int                  pixeltype;

    private Color                pixelcolor;

    private Graphics             graphics;

    private float                quality;

    private boolean              connectPointsEnabled;

    public static final int      MINX                     = 0;

    public static final int      MAXX                     = 1;

    public static final int      MINY                     = 2;

    public static final int      MAXY                     = 3;

    public static final int      DASH_LENGTH              = 10;

    public static final int      AXIS_TICK_RADIUS         = 4;

    public static final int      DEFAULT_WIDTH            = 250;

    public static final int      DEFAULT_HEIGHT           = 250;

    public static final int      PIXEL_TYPE_CIRCLE        = 0;

    public static final int      PIXEL_TYPE_SQUARE        = 1;

    public static final int      DEFAULT_PIXEL_SIZE       = 1;

    public static final int      DEFAULT_PIXEL_TYPE       = PIXEL_TYPE_CIRCLE;

    public static final Color    DEFAULT_FOREGROUND_COLOR = Color.red;

    public static final Color    DEFAULT_BACKGROUND_COLOR = Color.black;

    public static final String   DEFAULT_FILENAME         = "GraphImage.jpg";

    public static final float    DEFAULT_JPEG_QUALITY     = 1.0f;

    public static final Font     FONT                     = new Font(
                                                                  "Monospaced",
                                                                  Font.PLAIN,
                                                                  10);

    public static final double[] DEFAULT_SCALE            = new double[] {
            -5.0, 5.0, -5.0, 5.0
                                                          };

    private final double[]       scale                    = new double[DEFAULT_SCALE.length];

    private final DecimalFormat  dfx                      = new DecimalFormat(
                                                                  "#.##");

    private final DecimalFormat  dfy                      = new DecimalFormat(
                                                                  "#.##");

    public GraphImage() {
        this(DEFAULT_WIDTH, DEFAULT_HEIGHT);
    }

    public GraphImage(int width, int height) {
        super(width, height, TYPE_3BYTE_BGR);
        init();
    }

    private void init() {
        graphics = getGraphics();
        graphics.setFont(FONT);
        setScale(DEFAULT_SCALE);
        setPixelSize(DEFAULT_PIXEL_SIZE);
        setPixelType(DEFAULT_PIXEL_TYPE);
        setBackground(DEFAULT_BACKGROUND_COLOR);
        setForeground(DEFAULT_FOREGROUND_COLOR);
        setImageQuality(DEFAULT_JPEG_QUALITY);
        setConnectPointsEnabled(false);
        setXYAxesLabelsPrecision(2, 2);
    }

    public void setImageQuality(float quality) {
        if (quality < 0.0f || quality > 1.0f)
            return;
        this.quality = quality;
    }

    public void setPixelSize(int pixelsize) {
        this.pixelsize = pixelsize;
    }

    public void setPixelType(int pixeltype) {
        this.pixeltype = pixeltype;
    }

    public void setBackground(Color color) {
        graphics.setColor(color);
        graphics.fillRect(0, 0, getWidth(), getHeight());
    }

    public void setForeground(Color color) {
        pixelcolor = color;
    }

    // 06/28/00 WRR -- v 1.01
    // New method for computing Y axis tick interval
    public int getSuggestedYAxisTickInterval(int suggestedNumTicks, double maxy) {
        // as a starting point, aim for more-or-less suggestedNumTicks...
        double t = maxy / suggestedNumTicks;
        // now, let's massage the tick interval t, shape it up...
        int ll = (int) (Math.log(t) / Math.log(10));
        int lu = ll + 1;
        int[] ti = new int[10];
        ti[0] = (int) Math.pow(10, ll);
        ti[9] = (int) Math.pow(10, lu);
        for (int i = 0; i < ti.length - 1; i++) {
            ti[i + 1] = ti[i] + ti[0];
        }
        int index = 9;
        int delta = Integer.MAX_VALUE;
        for (int i = ti.length - 1; i >= 0; i--) {
            double ddiff = (ti[i] - t);
            if (ddiff < 0.0)
                break;
            int diff = (int) ddiff;
            if (diff <= delta || delta < 0) {
                delta = diff;
                index = i;
            }
        }
        // this is our sensible and aesthetically round tick interval...
        return ti[index];
    }

    public synchronized double getSuggestedMinXScale(double maxx, double maxy) {
        double[] scale = new double[this.scale.length];
        System.arraycopy(this.scale, 0, scale, 0, scale.length);
        setScale(new double[] {
                0, maxx, 0, maxy
        });
        FontMetrics fm = graphics.getFontMetrics();
        int w = fm.stringWidth(dfy.format(maxy)) + AXIS_TICK_RADIUS + 20;
        double minx = -(this.scale[MINX] + w * alphax);
        setScale(scale);
        return minx;
    }

    public synchronized double getSuggestedMinYScale(double maxx, double maxy) {
        double[] scale = new double[this.scale.length];
        System.arraycopy(this.scale, 0, scale, 0, scale.length);
        setScale(new double[] {
                0, maxx, 0, maxy
        });
        FontMetrics fm = graphics.getFontMetrics();
        int h = AXIS_TICK_RADIUS + fm.getHeight() + 40;
        double miny = (this.scale[MAXY] - h * alphay) - this.scale[MAXY];
        setScale(scale);
        return miny;
    }

    public void setScale(double[] scale) {
        if (scale == null || scale.length < this.scale.length)
            return;
        System.arraycopy(scale, 0, this.scale, 0, this.scale.length);
        alphax = (this.scale[MAXX] - this.scale[MINX]) / getWidth();
        alphay = (this.scale[MAXY] - this.scale[MINY]) / getHeight();
    }

    private int getLinearMapX(double x) {
        return (int) Math.round((x - scale[MINX]) / alphax);
    }

    private int getLinearMapY(double y) {
        return (int) Math.round((scale[MAXY] - y) / alphay);
    }

    private void setAxisLabelsPrecision(DecimalFormat df, int i, int f) {
        df.setMinimumIntegerDigits(i);
        df.setMinimumFractionDigits(f);
        df.setMaximumFractionDigits(f);
    }

    public void setXAxisLabelsPrecision(int i, int f) {
        setAxisLabelsPrecision(dfx, i, f);
    }

    public void setYAxisLabelsPrecision(int i, int f) {
        setAxisLabelsPrecision(dfy, i, f);
    }

    public void setXYAxesLabelsPrecision(int x, int y) {
        setAxisLabelsPrecision(dfx, 1, x);
        setAxisLabelsPrecision(dfy, 1, y);
    }

    public void plotXYAxes(Color c) {
        graphics.setColor(c);
        int pixelOriginX = getLinearMapX(0.0);
        int pixelOriginY = getLinearMapY(0.0);
        graphics.drawLine(0, pixelOriginY, getWidth(), pixelOriginY);
        graphics.drawLine(pixelOriginX, 0, pixelOriginX, getHeight());
    }

    public void plotXAxisTicks(double step, Color c, String label) {
        graphics.setColor(c);
        int py = getLinearMapY(0.0);
        FontMetrics fm = graphics.getFontMetrics();
        for (double x = 0.0; x <= scale[MAXX]; x += step) {
            int px = getLinearMapX(x);
            graphics.drawLine(px, py - AXIS_TICK_RADIUS * ((x == 0.0) ? 2 : 1),
                    px, py + AXIS_TICK_RADIUS * ((x == 0.0) ? 2 : 1));
            if (x == 0.0)
                continue;
            String sx = dfx.format(x);
            int sw = fm.stringWidth(sx) / 2;
            graphics.drawString(sx, px - sw, py + AXIS_TICK_RADIUS
                    + fm.getAscent());
        }
        for (double x = 0.0; x >= scale[MINX]; x -= step) {
            int px = getLinearMapX(x);
            graphics.drawLine(px, py - AXIS_TICK_RADIUS, px, py
                    + AXIS_TICK_RADIUS);
        }
        graphics.drawString(label, getLinearMapX(scale[MAXX] - 2), py + 3
                * AXIS_TICK_RADIUS + fm.getAscent());
    }

    public void plotYAxisTicks(double step, Color c, String label) {
        graphics.setColor(c);
        int px = getLinearMapX(0.0);
        FontMetrics fm = graphics.getFontMetrics();
        for (double y = 0.0; y <= scale[MAXY] - step; y += step) {
            int py = getLinearMapY(y);
            graphics.drawLine(px - AXIS_TICK_RADIUS * ((y == 0.0) ? 2 : 1), py,
                    px + AXIS_TICK_RADIUS * ((y == 0.0) ? 2 : 1), py);
            if (y == 0.0)
                continue;
            String sy = dfy.format(y);
            graphics.drawString(sy, px - AXIS_TICK_RADIUS - fm.stringWidth(sy)
                    - 2, py + fm.getDescent());
        }
        for (double y = 0.0; y >= scale[MINY]; y -= step) {
            int py = getLinearMapY(y);
            graphics.drawLine(px - AXIS_TICK_RADIUS, py, px + AXIS_TICK_RADIUS,
                    py);
        }
        graphics.drawString(label, px - AXIS_TICK_RADIUS
                - fm.stringWidth(label), getLinearMapY(scale[MAXY]) + 4
                + fm.getDescent());
    }

    public void drawLegend(String legend, double XOffset, double YOffset,
            Color symbolColor, Color legendColor, String legendType) {
        FontMetrics fm = graphics.getFontMetrics();
        int tx = getLinearMapX(scale[MINX] + XOffset);
        int ty = getLinearMapY(0) + (int) (YOffset * AXIS_TICK_RADIUS)
                + fm.getAscent();
        int p = pixelsize;
        int r = (int) Math.round(p / 2.0);
        graphics.setColor(symbolColor);
        if (legendType.equals("Line")) {
            graphics.drawLine(tx, ty - fm.getAscent() / 2, tx + 20, ty
                    - fm.getAscent() / 2);
            graphics.setColor(legendColor);
            graphics.drawString(" - " + legend, tx + 22, ty);
        }
        else if (legendType.equals("Dot")) {
            graphics.fillArc(tx - r, ty - r - fm.getAscent() / 2, p, p, 0, 360);
            graphics.setColor(legendColor);
            graphics.drawString(" - " + legend, tx, ty);
        }
    }

    public void setConnectPointsEnabled(boolean connectPointsEnabled) {
        this.connectPointsEnabled = connectPointsEnabled;
    }

    public void resetConnectPoints() {
        prevSet = false;
    }

    public void plotLine(double x1, double y1, double x2, double y2) {
        graphics.setColor(pixelcolor);
        graphics.drawLine(getLinearMapX(x1), getLinearMapY(y1),
                getLinearMapX(x2), getLinearMapY(y2));
    }

    public void plotFlatLine(double x1, double x2, double y, Color c) {
        int py = getLinearMapY(y);
        int px = getLinearMapX(x1);
        int ex = getLinearMapX(x2);
        graphics.setColor(c);
        for (int x = px; x < ex; x += 2 * DASH_LENGTH) {
            graphics.drawLine(x, py, x + DASH_LENGTH, py);
        }
    }

    public void plotFlatLine(double y, Color c) {
        int w = getWidth();
        int py = getLinearMapY(y);
        int px = getLinearMapX(0.0);
        graphics.setColor(c);
        for (int x = px; x < w; x += 2 * DASH_LENGTH) {
            graphics.drawLine(x, py, x + DASH_LENGTH, py);
        }
    }

    public void plot(double x, double y, int p, Color c) {
        if (connectPointsEnabled) {
            if (prevSet) {
                plotLine(prevx, prevy, x, y);
            }
            prevSet = true;
            prevx = x;
            prevy = y;
        }
        else {
            prevSet = false;
        }
        graphics.setColor(c);
        int px = getLinearMapX(x);
        int py = getLinearMapY(y);
        int r = (int) Math.round(p / 2.0);
        switch (pixeltype) {
            case PIXEL_TYPE_CIRCLE:
                graphics.fillArc(px - r, py - r, p, p, 0, 360);
                break;
            case PIXEL_TYPE_SQUARE:
                graphics.fillRect(px - r, py - r, p, p);
                break;
        }
    }

    public void plot(double x, double y, Color c) {
        plot(x, y, pixelsize, c);
    }

    public void plot(double x, double y) {
        plot(x, y, pixelsize, pixelcolor);
    }

    public void make() {
        make(DEFAULT_FILENAME);
    }

    public void make(String filename) {
        if (filename == null)
            return;
        try {
            File outfile = new File(filename);
            ImageIO.write(this, "jpeg", outfile);
        }
        catch (Exception e) {
            JBButil.getLog().log(Level.WARNING, "Exception: " + e.getMessage(),
                    e);
        }
    }
}
