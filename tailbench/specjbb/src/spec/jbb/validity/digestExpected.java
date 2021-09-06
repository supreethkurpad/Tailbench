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
package spec.jbb.validity;

public class digestExpected {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "Copyright (c) 2005 Hewlett-Packard,"
                                          + "All rights reserved,"
                                          + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    byte                a[]       = {
            -120, 38, -16, -122, 8, -70, -108, 33, -112, 125
                                  };

    public digestExpected() {
    }

    public byte[] getArray() {
        return a;
    }
}
