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

import java.util.Enumeration;
import java.util.Vector;

public class TextBlock {
    // This goes right after each class/interface statement
    static final String         COPYRIGHT       = "SPECjbb2005,"
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
    private final static char   NL              = (char) Character.LINE_SEPARATOR;

    private final static String columnSeparator = " | ";

    // /////////////////////////////////////
    // instance variable field declarations
    // /////////////////////////////////////
    private Vector<String>      lines           = new Vector<String>();

    private String              spaces;

    private int                 width;

    // /////////////////////////////////////
    // constructor declarations
    // /////////////////////////////////////
    public TextBlock(int width) {
        this.width = width;
        spaces = rep(" ", width);
    }

    public TextBlock(int width, String text) {
        this(width);
        add(text);
        spaces = rep(" ", width);
    }

    // /////////////////////////////////////
    // class method declarations
    // /////////////////////////////////////
    public static void main(String[] args) {
        int width = 40;
        if (args.length > 0)
            try {
                width = Integer.parseInt(args[0]);
            }
            catch (NumberFormatException e) {
            }
        TextBlock t = new TextBlock(width);
        t
                .add("Now is the time for all good men to come to the aid of their country");
        t.add("Mary had a little lamb. Its fleece was white as snow.");
        t.add("A veryverylongwordalmomstlikeaGermanwordinlength added here");
        System.out.println("*** left");
        System.out.println(t.toString());
        TextBlock t1 = new TextBlock(20);
        t1.add("The quick brown fox jumped over the lazy white dog.");
        t1.add("She sells sea shells by the sea shore.");
        System.out.println("*** right");
        System.out.println(t1.toString());
        TextBlock t2 = t.join(t1);
        System.out.println("*** together");
        System.out.println(t2.toString());
        t2.add(t);
        System.out.println("*** appended");
        System.out.println(t2.toString());
    }

    private static String rep(String s, int n) {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < n; i++)
            buf.append(s);
        return buf.toString();
    }

    public static String justifyLeft(String s, int width) {
        int slen = s.length();
        if (slen < width)
            return s + rep(" ", width - slen);
        else
            return s.substring(0, width);
    }

    public static String justifyRight(String s, int width) {
        int slen = s.length();
        if (slen < width)
            return rep(" ", width - slen) + s;
        else
            return s.substring(slen - width);
    }

    // /////////////////////////////////////
    // instance method declarations
    // /////////////////////////////////////
    public void add(String text) {
        int len = text.length();
        int i = 0;
        Outer: while (len - i > width) {
            // System.out.println ("Top. i=" + i);
            while (text.charAt(i) == ' ')
                if (len - ++i > width)
                    break Outer;
            // System.out.println ("Remaining: " + text.substring(i));
            int j = text.lastIndexOf(' ');
            while (j > i + width)
                j = text.lastIndexOf(' ', j - 1);
            // System.out.println ("i=" + i + ", j=" + j);
            if (j < i) {
                // System.out.println ("use " + i + " to " + i + " + " + width);
                lines.addElement(text.substring(i, i + width));
                i += width;
            }
            else {
                // System.out.println ("use " + i + " to " + j);
                lines.addElement(justifyLeft(text.substring(i, j)));
                i = j + 1;
            }
            // System.out.println (toString());
        }
        lines.addElement(justifyLeft(text.substring(i)));
    }

    public void addRight(String text) {
        int len = text.length();
        int i = 0;
        Outer: while (len - i > width) {
            // System.out.println ("Top. i=" + i);
            while (text.charAt(i) == ' ')
                if (len - ++i > width)
                    break Outer;
            // System.out.println ("Remaining: " + text.substring(i));
            int j = text.lastIndexOf(' ');
            while (j > i + width)
                j = text.lastIndexOf(' ', j - 1);
            // System.out.println ("i=" + i + ", j=" + j);
            if (j < i) {
                // System.out.println ("use " + i + " to " + i + " + " + width);
                lines.addElement(text.substring(i, i + width));
                i += width;
            }
            else {
                // System.out.println ("use " + i + " to " + j);
                lines.addElement(justifyRight(text.substring(i, j)));
                i = j + 1;
            }
            // System.out.println (toString());
        }
        lines.addElement(justifyRight(text.substring(i)));
    }

    public void add(TextBlock block) {
        for (Enumeration<String> e = block.lines.elements(); e
                .hasMoreElements();)
            lines.addElement(e.nextElement());
        width = Math.max(width, block.width);
    }

    public TextBlock join(TextBlock right) {
        return join(right, columnSeparator);
    }

    public TextBlock join(TextBlock right, String columnSeparator) {
        int nleft = size();
        int nright = right.size();
        int n = Math.min(nleft, nright);
        TextBlock combined = new TextBlock(width + columnSeparator.length()
                + right.width);
        for (int i = 0; i < n; i++)
            combined.lines.addElement((String) lines.elementAt(i)
                    + columnSeparator + (String) right.lines.elementAt(i));
        if (nleft < nright) {
            for (int i = n; i < nright; i++)
                combined.lines.addElement(spaces + columnSeparator
                        + (String) right.lines.elementAt(i));
        }
        else {
            for (int i = n; i < nleft; i++)
                combined.lines.addElement((String) lines.elementAt(i)
                        + columnSeparator + spaces);
        }
        return combined;
    }

    private String justifyLeft(String s) {
        s = s + spaces;
        return s.substring(0, width);
    }

    private String justifyRight(String s) {
        s = spaces + s;
        int slen = s.length();
        return s.substring(slen - width);
    }

    public int size() {
        return lines.size();
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        for (Enumeration e = lines.elements(); e.hasMoreElements();) {
            buf.append((String) e.nextElement());
            if (e.hasMoreElements())
                buf.append("\n");
        }
        return buf.toString();
    }
}// end class
