/*
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.lang.String;
import java.io.PrintStream;

class DeliveryHandler {
    // This goes right after each class/interface statement
    static final String        COPYRIGHT = "SPECjbb2005,"
                                                 + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                                 + "All rights reserved,"
                                                 + "(C) Copyright IBM Corp., 1996 - 2005"
                                                 + "All rights reserved,"
                                                 + "Licensed Materials - Property of SPEC";

    static private PrintStream outFile;

    public DeliveryHandler(PrintStream ps) {
        synchronized (getClass()) {
            outFile = ps;
        }
    }

    public void handleDelivery(DeliveryTransaction deliveryTransaction) {
        // this needs to be asynchronous
        deliveryTransaction.preprocess();
        // send record of delivery to file
        deliveryTransaction.display(outFile);
    }
}
