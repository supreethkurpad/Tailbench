/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.io.*;
import java.util.logging.Level;

import tbench.*;

class TransactionManager {
    // This goes right after each class/interface statement
    static final String       COPYRIGHT       = "SPECjbb2005,"
                                                      + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                                      + "All rights reserved,"
                                                      + "(C) Copyright IBM Corp., 1996-2005"
                                                      + "All rights reserved,"
                                                      + "Licensed Materials - Property of SPEC";

    private Company           company;

    private short             warehouseId;                                                      // W_ID

    public Company.runModes   mode            = Company.runModes.DEFAULT_MODE;

    private static final byte new_order       = Transaction.new_order;

    private static final byte payment         = Transaction.payment;

    private static final byte order_status    = Transaction.order_status;

    private static final byte delivery        = Transaction.delivery;

    private static final byte stock_level     = Transaction.stock_level;

    private static final byte cust_report     = Transaction.cust_report;

    private static final byte maxTxnTypes     = Transaction.maxTxnTypes;

    private static final byte multiple_orders = maxTxnTypes;

    private static final byte pgm_exit        = (byte) (multiple_orders + 2);

    private Transaction       transactionInstance[];

    public void initTransactionManager(Company inCompany, short warehouseId) {
        company = inCompany;
        this.warehouseId = warehouseId;
        // This is necessary to keep sequence of random calls
        JBButil.random(1, company.getMaxDistrictsPerWarehouse(), warehouseId);
    }

    // elian061004: transactions creation via reflection
    private void createTxnInstances() {
        transactionInstance = new Transaction[maxTxnTypes];
        for (int i = 0; i < maxTxnTypes; ++i) {
            transactionInstance[i] = Transaction.getInstance(
                    Transaction.transactionClasses[i], company, warehouseId);
        }
    }

    private void manualSelection() {
        int i;
        System.out.println("Select transaction type");
        System.out.println();
        for (i = 0; i < maxTxnTypes; ++i) {
            try {
                System.out.println((i + 1)
                        + ". "
                        + Transaction.transactionClasses[i].getMethod(
                                "getMenuName", (Class[]) null).invoke(null,
                                (Object[]) null));
            }
            catch (Exception e) {
                JBButil
                        .getLog()
                        .log(
                                Level.WARNING,
                                "TransactionManager.manualSelection - "
                                        + "NoSuchMethodException, or IllegalAccessException",
                                e);
            }
        }
        System.out.println(multiple_orders + 1 + ". Create NewOrders");
        System.out.println(pgm_exit + 1 + ". Exit");
        System.out.println();
        System.out.println("Enter selection here: ");
    }

    private String readUserValue() {
        BufferedReader keyboard_input;
        String s = "";
        try {
            keyboard_input = new BufferedReader(
                    new InputStreamReader(System.in));
            s = keyboard_input.readLine();
        }
        catch (IOException exception) {
        }
        return s;
    }

    // elian061004: runs processes for the given transaction type
    // with the defined pauses
    private long runTxn(Transaction txn, long menuWaitTime,
            long typingWaitTime, double thinkingWaitTime) {
        long start;
        long end;
        txn.init();
        if (menuWaitTime > 0) {
            JBButil.milliSecondsToSleep(menuWaitTime);
        }
        txn.initializeTransactionLog();
        if (typingWaitTime > 0) {
            JBButil.milliSecondsToSleep(typingWaitTime);
        }
        start = System.currentTimeMillis(); // get start time
        txn.process();
        txn.processTransactionLog();
        end = System.currentTimeMillis(); // get end time
        if (thinkingWaitTime > 0.0) {
            JBButil.SecondsToSleep(thinkingWaitTime);
        }
        return (end - start);
    }

    // elian061004: does different actions based on manual selection value
    private long goManual(int selection, TimerData myTimerData) {
        long menuWaitTime = 0;
        long typingWaitTime = 0;
        double thinkingWaitTime = 0.0;
        if (isMultiple(selection)) {
            // special case processing for multiple ...
            long numOrders = 0;
            int i;
            System.out.println("How many orders to be created? ");
            numOrders = new Integer(readUserValue()).intValue();
            System.out.println("Creating New Orders...");
            for (i = 0; i < numOrders; ++i) {
                // Instance of NewOrderTransaction
                transactionInstance[0].init();
                transactionInstance[0].process();
            }
            return 0;
        }
        else {
            if (selection < maxTxnTypes) {
                menuWaitTime = myTimerData.getMenuWaitTime(selection);
                typingWaitTime = myTimerData.getTypingWaitTime(selection);
                thinkingWaitTime = JBButil
                        .negativeExpDistribution(((double) myTimerData
                                .getThinkingWaitTime(selection)) / 1000.0D,
                                warehouseId);
            }
            return runTxn(transactionInstance[selection], menuWaitTime,
                    typingWaitTime, thinkingWaitTime);
        }
    }

    boolean isMultiple(int selection) {
        if (selection == multiple_orders) {
            return true;
        }
        else {
            return false;
        }
    }

    public synchronized Company.runModes getrunMode() {
        return mode;
    }

    public synchronized void setrunMode(Company.runModes inmode) {
        mode = inmode;
    }

    public void go() {
        byte co = 0;
        int deck[] = new int[33];
        long elapsed_time;
        long txntime;
        int txntype;
        boolean timed = false;
        boolean signaled_done = false;
        int i = 0;
        TimerData warehouseTimerDataPtr = company.getTimerDataPtr(warehouseId);
        long rampup_time = warehouseTimerDataPtr.getRampUpTime();
        long measurement_time = warehouseTimerDataPtr.getMeasurementTime();
        // create object to store timer data for this process
        TimerData myTimerData = new TimerData();
        // copy wait times from warehouseTimerData to myTimerData
        myTimerData.setWaitTimes(warehouseTimerDataPtr.getWaitTimes());
        deck = buildDeck();
        Warehouse warehousePtr = company.getWarehousePtr(warehouseId, false);
        // create transaction objects near their warehouse
        createTxnInstances();
        synchronized (company.initThreadsCountMonitor) {
            synchronized (company.initThreadsStateChange) {
                company.initThreadsCount++;
                company.initThreadsStateChange.notify();
            }
            try {
                company.initThreadsCountMonitor.wait();
            }
            catch (InterruptedException e) {
            }
        }
        if ((rampup_time > 0) || (measurement_time > 0)) {
            timed = true;
        }
        if (JBButil.getLog().isLoggable(Level.FINEST)) {
            JBButil.getLog().finest(
                    "Benchmark " + JBBmain.Version + ": warehouse "
                            + warehouseId);
        }

        tbench.tBenchServerThreadStart();

        while (this.getrunMode() != Company.runModes.STOP) {
            if ((!timed)
                    && (this.getrunMode() == Company.runModes.DEFAULT_MODE)) {
                manualSelection();
                txntype = (new Integer(readUserValue()).intValue()) - 1;
            }
            else {
                txntype = deck[i];
                i++;
                if (i == 33) {
                    deck = buildDeck();
                    i = 0;
                }
            }

            byte[] req = tbench.tBenchRecvReq();

            txntime = goManual(txntype, myTimerData);

            byte[] resp = new byte[32];
            tbench.tBenchSendResp(resp, resp.length);

            if (this.getrunMode() == Company.runModes.RECORDING)
                myTimerData.updateTimerData(txntype, txntime);
            if (timed) {
                if ((this.getrunMode() == Company.runModes.RAMP_DOWN)
                        && (!signaled_done)) {
                    synchronized (company.threadsDoneCountMonitor) {
                        company.threadsDoneCount++;
                        company.threadsDoneCountMonitor.notify();
                        signaled_done = true;
                    }
                }
            }
            else {
                if (txntype == pgm_exit) {
                    break;
                }
            }
        }
        if (timed && (this.getrunMode() == Company.runModes.STOP)) {
            elapsed_time = company.getElapsedTime();
            myTimerData.calculateResponseTimeStats();
            double tpmc = myTimerData.updateTPMC(elapsed_time);
            double btps = myTimerData.updateBTPS(elapsed_time);
            // roll up totals to warehouse and company
            long totalTransactions = 0;
            for (txntype = 0; txntype < maxTxnTypes; txntype++) {
                warehouseTimerDataPtr.rollupTimerData(txntype, myTimerData
                        .getTransactionCount(txntype), myTimerData
                        .getTotalTime(txntype), myTimerData
                        .getTotalTimeSquare(txntype), myTimerData
                        .getMinimumTime(txntype), myTimerData
                        .getMaximumTime(txntype));
                company.getTimerDataPtr(co).rollupTimerData(txntype,
                        myTimerData.getTransactionCount(txntype),
                        myTimerData.getTotalTime(txntype),
                        myTimerData.getTotalTimeSquare(txntype),
                        myTimerData.getMinimumTime(txntype),
                        myTimerData.getMaximumTime(txntype));
                totalTransactions += myTimerData.getTransactionCount(txntype);
            }
            company.getTimerDataPtr(co).accumulateTransactionStats(
                    totalTransactions);
            warehouseTimerDataPtr.updateTPMC(tpmc);
            warehouseTimerDataPtr.updateBTPS(btps);
            company.getTimerDataPtr(co).updateTPMC(tpmc);
            company.getTimerDataPtr(co).updateBTPS(btps);
            synchronized (company.stopThreadsCountMonitor) {
                company.stopThreadsCount++;
                company.stopThreadsCountMonitor.notify();
            }
        }

    }

    public boolean goValidate() {
        Transaction t[];
        t = new Transaction[maxTxnTypes];
        int i;
        Transaction.validateRun(); // Start off assuming the run is valid.
        for (i = 0; i < maxTxnTypes; ++i) {
            t[i] = Transaction.getInstance(Transaction.transactionClasses[i],
                    company, warehouseId);
        }
        for (i = 0; i < maxTxnTypes; ++i) {
            t[i].init();
            t[i].initializeTransactionLog();
            t[i].process();
            t[i].processTransactionLog();
        }
        return Transaction.isRunValid();
    }

    public int[] buildDeck() {
        int real_deck[] = new int[33];
        int cross_deck[] = new int[33];
        int rand_val;
        int i;
        // set up cross_deck
        for (i = 0; i < 33; i++)
            cross_deck[i] = i;
        // assign new-order
        for (i = 0; i < 10; i++) {
            rand_val = (int) JBButil.random(0, 33 - 1 - i, warehouseId);
            real_deck[cross_deck[rand_val]] = new_order;
            cross_deck[rand_val] = cross_deck[33 - 1 - i];
        }
        // assign payment
        for (i = 0; i < 10; i++) {
            rand_val = (int) JBButil.random(0, 23 - 1 - i, warehouseId);
            real_deck[cross_deck[rand_val]] = payment;
            cross_deck[rand_val] = cross_deck[23 - 1 - i];
        }
        // order status
        rand_val = (int) JBButil.random(0, 13 - 1, warehouseId);
        real_deck[cross_deck[rand_val]] = order_status;
        cross_deck[rand_val] = cross_deck[13 - 1];
        // delivery
        rand_val = (int) JBButil.random(0, 12 - 1, warehouseId);
        real_deck[cross_deck[rand_val]] = delivery;
        cross_deck[rand_val] = cross_deck[12 - 1];
        // stock-level
        rand_val = (int) JBButil.random(0, 11 - 1, warehouseId);
        real_deck[cross_deck[rand_val]] = stock_level;
        cross_deck[rand_val] = cross_deck[11 - 1];
        // customer-report
        for (i = 0; i < 10; i++) {
            rand_val = (int) JBButil.random(0, 10 - 1 - i, warehouseId);
            real_deck[cross_deck[rand_val]] = cust_report;
        }
        return real_deck;
    }
}
