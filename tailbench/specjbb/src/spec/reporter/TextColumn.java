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

public class TextColumn {
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
    private Vector<TextBlock>   blocks          = new Vector<TextBlock>();

    private int                 width;

    // /////////////////////////////////////
    // constructor declarations
    // /////////////////////////////////////
    public TextColumn(int width) {
        this.width = width;
    }

    public TextColumn(int width, String text) {
        this(width);
        add(text);
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
        TextColumn t = new TextColumn(width);
        t
                .add("Now is the time for all good men to come to the aid of their country");
        t.add("Mary had a little lamb. Its fleece was white as snow.");
        t.add("A veryverylongwordalmomstlikeaGermanwordinlength added here");
        System.out.println("*** left");
        System.out.println(t.toString());
        TextColumn t1 = new TextColumn(20);
        t1.add("The quick brown fox jumped over the lazy white dog.");
        t1.add("She sells sea shells by the sea shore.");
        System.out.println("*** right");
        System.out.println(t1.toString());
        TextColumn t2 = t.join(t1);
        System.out.println("*** together");
        System.out.println(t2.toString());
    }

    // /////////////////////////////////////
    // instance method declarations
    // /////////////////////////////////////
    public void add(String text) {
        blocks.addElement(new TextBlock(width, text));
    }

    public TextColumn join(TextColumn right) {
        return join(right, columnSeparator);
    }

    public TextColumn join(TextColumn right, String columnSeparator) {
        int nleft = size();
        int nright = right.size();
        int n = Math.min(nleft, nright);
        TextColumn combined = new TextColumn(width + columnSeparator.length()
                + right.width);
        for (int i = 0; i < n; i++)
            combined.blocks.addElement(((TextBlock) blocks.elementAt(i)).join(
                    ((TextBlock) right.blocks.elementAt(i)), columnSeparator));
        if (nleft < nright) {
            TextBlock empty = new TextBlock(width);
            for (int i = n; i < nright; i++)
                combined.blocks.addElement(empty.join(((TextBlock) right.blocks
                        .elementAt(i)), columnSeparator));
        }
        else {
            TextBlock empty = new TextBlock(right.width);
            for (int i = n; i < nleft; i++)
                combined.blocks.addElement(((TextBlock) blocks.elementAt(i))
                        .join(empty, columnSeparator));
        }
        return combined;
    }

    public TextBlock merge() {
        TextBlock merged = new TextBlock(width);
        for (Enumeration e = blocks.elements(); e.hasMoreElements();)
            merged.add((TextBlock) e.nextElement());
        return merged;
    }

    public int size() {
        return blocks.size();
    }

    public String toString() {
        return merge().toString();
    }
}// end class
