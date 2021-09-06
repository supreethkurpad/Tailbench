/*
 *
 * Copyright (c) 2000 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 * Copyright (c) 1997-2000 Sun Microsystems, Inc. All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 *
 */
/*
 * See what happens here where we subclass Super
 */
package spec.jbb.validity;

public class Sub extends Super {
    // This goes right after each class/interface statement
    static final String   COPYRIGHT = "SPECjbb2005,"
                                            + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                            + "All rights reserved,"
                                            + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                            + "All rights reserved,"
                                            + "Licensed Materials - Property of SPEC";

    // /////////////////////////////////////
    // class variable field declarations
    // /////////////////////////////////////
    private static String name      = "Sub";

    private static int    psi       = publicStatic + 7;

    // /////////////////////////////////////
    // instance variable field declarations
    // /////////////////////////////////////
    private int           priv      = 5;

    protected int         prot      = 11;

    public int            pub       = 13;

    // /////////////////////////////////////
    // constructor declarations
    // /////////////////////////////////////
    public Sub(int black) {
        super(black + 77);
        pub += black * 2;
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
        return priv + 100;
    }

    public int getProtected() {
        return prot + 100;
    }
}
