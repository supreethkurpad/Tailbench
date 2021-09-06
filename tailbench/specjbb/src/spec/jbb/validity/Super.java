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
 * See what happens when we make a subclass of this
 */
package spec.jbb.validity;

public class Super {
    // This goes right after each class/interface statement
    static final String   COPYRIGHT    = "SPECjbb2005,"
                                               + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                               + "All rights reserved,"
                                               + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                               + "All rights reserved,"
                                               + "Licensed Materials - Property of SPEC";

    // /////////////////////////////////////
    // class variable field declarations
    // /////////////////////////////////////
    private static String name         = "Super";

    private static int    psi          = 10;

    public static int     publicStatic = psi - 2;

    // /////////////////////////////////////
    // instance variable field declarations
    // /////////////////////////////////////
    private int           priv         = 2;

    protected int         prot         = 3;

    public int            pub          = 4;

    // /////////////////////////////////////
    // constructor declarations
    // /////////////////////////////////////
    public Super(int magic) {
        priv += psi * magic;
        prot += psi * magic;
        pub += psi * magic;
    }

    // /////////////////////////////////////
    // class method declarations
    // /////////////////////////////////////
    // /////////////////////////////////////
    // instance method declarations
    // /////////////////////////////////////
    public String getName() {
        return name;
    }

    public int getPrivate() {
        return priv;
    }

    public int getProtected() {
        return prot;
    }

    public String toString() {
        return "Class " + name + ", public=" + pub + ", protected=" + prot
                + ", private=" + priv;
    }
}
