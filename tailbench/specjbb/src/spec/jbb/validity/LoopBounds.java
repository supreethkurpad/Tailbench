/*
 *
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 * Copyright (c) 1997-2005 Sun Microsystems, Inc. All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 *
 */
/*
 * Check whether the JVM makes unwarranted assumptions about non-final
 * methods. This test case is derived from a hot spot in DeltaBlue
 * where it seems tempting to make the loop bounds constant, but
 * you cannot be certain that the class is not subclassed, etc.
 *
 * Walter Bays
 */
package spec.jbb.validity;

import java.util.Vector;

class LoopBounds {
    // This goes right after each class/interface statement
    static final String   COPYRIGHT = "SPECjbb2005,"
                                            + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                            + "All rights reserved,"
                                            + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                            + "All rights reserved,"
                                            + "Licensed Materials - Property of SPEC";

    protected Vector      v;

    public static boolean gotError  = false;

    public LoopBounds() {
        v = new Vector();
        int f0 = 0;
        int f1 = 1;
        for (int i = 0; i < 20; i++) {
            v.addElement(f1);
            int f = f0 + f1;
            f0 = f1;
            f1 = f;
        }
    }

    public int size() {
        return v.size();
    }

    public int constraintAt(int index) {
        Object o = v.elementAt(index);
        if (o instanceof Integer)
            return ((Integer) o).intValue();
        else
            return 666;
    }

    public void execute() {
        for (int i = 0; i < size(); ++i) {
            // System.out.println ("v.size()=" + v.size() + " size()=" +
            // size());
            System.out.print(constraintAt(i) + " ");
        }
        System.out.println();
    }

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        (new LoopBounds()).execute();
        String name = "spec.jbb.validity.LoopBounds2";
        try {
            Class c = Class.forName(name);
            Object o = c.newInstance();
            if (!(o instanceof LoopBounds)) {
                System.out.println(name + " is not a LoopBounds\n");
                gotError = true;
                return;
            }
            ((LoopBounds) o).execute();
        }
        catch (Exception e) {
            System.out.println("Error " + e);
            gotError = true;
        }
    }
}// end LoopBounds

class LoopBounds2 extends LoopBounds {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    private int         n         = 0;

    public int size() {
        if (n > 4 && n % 2 == 1)
            v.insertElementAt(new Double(1.0 / n), 6);
        return v.size() - n++;
    }
}// end LoopBounds2
