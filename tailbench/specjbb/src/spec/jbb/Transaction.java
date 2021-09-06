/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.lang.reflect.Constructor;
import java.util.logging.Level;

abstract class Transaction {
    // This goes right after each class/interface statement
    static final String         COPYRIGHT          = "SPECjbb2005,"
                                                           + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                                           + "All rights reserved,"
                                                           + "(C) Copyright IBM Corp., 1996 - 2005"
                                                           + "All rights reserved,"
                                                           + "Licensed Materials - Property of SPEC";

    protected static final byte new_order          = 0;

    protected static final byte payment            = 1;

    protected static final byte order_status       = 2;

    protected static final byte delivery           = 3;

    protected static final byte stock_level        = 4;

    protected static final byte cust_report        = 5;

    // sjm092398
    public static int           aveOrderlines      = 10;

    public static int           minOrderlines      = aveOrderlines - 5;

    public static int           maxOrderlines      = aveOrderlines + 5;

    static boolean              enableLogWrite     = false;

    static boolean              steadyStateMem     = true;

    static boolean              validationFlag     = false;

    private static boolean      validRun           = true;

    static String               transactionNames[] = {
            "NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel",
            "CustomerReport",
                                                   };

    protected static final byte maxTxnTypes        = (byte) transactionNames.length;

    static Class                transactionClasses[];
    static {
        transactionClasses = new Class[maxTxnTypes];
        for (int i = 0; i < maxTxnTypes; i++) {
            try {
                transactionClasses[i] = Class.forName("spec.jbb."
                        + transactionNames[i] + "Transaction");
            }
            catch (ClassNotFoundException e) {
                System.out.println("Transaction.transactionClasses - "
                        + "ClassNotFoundException");
            }
        }
    }

    public abstract String getMenuName();

    public abstract void init();

    public abstract boolean process();

    public abstract void initializeTransactionLog();

    public abstract void processTransactionLog();

    //
    public static void invalidateRun() {
        validRun = false;
    }

    public static void validateRun() {
        validRun = true;
    }

    public static boolean isRunValid() {
        return validRun;
    }

    public static void setOrderLineCount(int count) {
        aveOrderlines = count;
        minOrderlines = aveOrderlines - 5;
        maxOrderlines = aveOrderlines + 5;
    }

    public static void setSteadyState(boolean onoff) {
        steadyStateMem = onoff;
    }

    public static void setLogWrite(boolean onoff) {
        enableLogWrite = onoff;
    }

    public static void setValidation(boolean onoff) {
        validationFlag = onoff;
    }

    static Class[] transactionConstructorSignature = {
            Company.class, short.class,
                                                   };

    static Transaction getInstance(Class transactionClass, Company company,
            short warehouseID) {
        Transaction t = null;
        try {
            Constructor ctor = transactionClass
                    .getConstructor(transactionConstructorSignature);
            Object[] args = new Object[2];
            args[0] = company;
            args[1] = new Short(warehouseID);
            t = (Transaction) ctor.newInstance(args);
        }
        catch (Exception e) {
            JBButil.getLog().log(Level.WARNING, "Exception: " + e.getMessage(),
                    e);
        }
        return t;
    }
}
