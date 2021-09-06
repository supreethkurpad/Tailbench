/*
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved. Copyright (c) 2001-2005 Compaq Computer Corp. All rights reserved.
 * 2001/10/23 CJB: Updated to stop printing of per-warehouse statistics.
 * 2001/11/01 CJB: Updated to make per-warehouse statistics optional.
 * 2005/03/22 Veeru: Multi-jvm changes
 */
package spec.jbb;

import java.lang.Integer;
import java.text.DecimalFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.PrintStream;

public class Company {
    // This goes right after each class/interface statement
    static final String        COPYRIGHT                  = "SPECjbb2005,"
                                                                  + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                                                  + "All rights reserved,"
                                                                  + "(C) Copyright IBM Corp., 1996 - 2005"
                                                                  + "All rights reserved,"
                                                                  + "Licensed Materials - Property of SPEC";

    static private PrintStream outPropFile;

    static private PrintStream outDeliveriesFile;

    private String             propPrefix                 = null;

    private Object[]           warehouseTable;

    private JBBDataStorage     customerTable;

    private JBBSortedStorage   lastNameCustomerTable;

    private JBBDataStorage     itemTable;

    private Object[]           timerdataTable;

    private Object[]           warehouseContainers;

    // timing variables:
    private long               rampup_time;

    private long               measurement_time;

    private TimerData          companyTimerData;

    private long               elapsed_time;

    // population variables:
    private short              PreviousMaxWarehouses;                                                       // holds

    // number of warehouses for last run
    private short              MaxWarehouses;                                                               // this

    private short              MaxDistrictsPerWarehouse;                                                    // should

    private int                MaxCustomersPerDistrict;                                                     // should

    private int                MaxItems;                                                                    // should

    private int                MaxStock;                                                                    // should

    private int                InitialOrders;                                                               // should

    private int                InitialNewOrders;                                                            // should

    private int                InitialHistories;                                                            // should

    private int                warehouseCapacity          = JBBmain.maxWh;                                   ;

    private ArrayList          warehouseThreads;

    private long               jvm_instances              = 0;

    private float              per_jvm_warehouse_rampup   = 0.5f;

    private float              per_jvm_warehouse_rampdown = 0.5f;

    // Should really make these private and provide "getters".
    Object                     initThreadsStateChange     = null;

    Object                     initThreadsCountMonitor    = null;

    int                        initThreadsCount           = 0;

    Object                     threadsDoneCountMonitor    = null;

    int                        threadsDoneCount           = 0;

    Object                     stopThreadsCountMonitor    = null;

    int                        stopThreadsCount           = 0;

    static void setOutputs(PrintStream oPropFile, PrintStream oDeliveriesFile) {
        outPropFile = oPropFile;
        outDeliveriesFile = oDeliveriesFile;
    }

    public enum runModes {
        DEFAULT_MODE, MULTI_RAMP, RAMP_UP, RECORDING, RAMP_DOWN, STOP
    };

    private volatile runModes mode = runModes.DEFAULT_MODE;

    public Company() {
        JBButil.random_init(warehouseCapacity);
        System.out.println("Constructing the company now   Hang....on");
        System.out.println("");
        warehouseContainers = new Object[warehouseCapacity];
        warehouseTable = new Object[warehouseCapacity]; // was new Hashtable();
        customerTable = Infrastructure.createStorage();
        lastNameCustomerTable = Infrastructure.createSortedStorage();
        itemTable = Infrastructure.createStorage();
        timerdataTable = new Object[warehouseCapacity];
        companyTimerData = new TimerData();
        MaxWarehouses = 0;
        initThreadsStateChange = new Object();
        initThreadsCountMonitor = new Object();
        initThreadsCount = 0;
        threadsDoneCountMonitor = new Object();
        threadsDoneCount = 0;
        stopThreadsCountMonitor = new Object();
        stopThreadsCount = 0;
        // add ArrayList to save pointers to warehouse threads
        warehouseThreads = new ArrayList(warehouseCapacity);
    }

    PrintStream getOutDeliveriesFile() {
        return outDeliveriesFile;
    }

    public short getMaxWarehouses() {
        return MaxWarehouses;
    }

    public short getMaxDistrictsPerWarehouse() {
        return MaxDistrictsPerWarehouse;
    }

    public int getMaxCustomersPerDistrict() {
        return MaxCustomersPerDistrict;
    }

    public int getMaxItems() {
        return MaxItems;
    }

    public int getInitialOrders() {
        return InitialOrders;
    }

    public int getInitialNewOrders() {
        return InitialNewOrders;
    }

    public synchronized void primeWithDummyData(short number_of_warehouses,
            int choice) {
        switch (choice) {
            case 0: {
                PreviousMaxWarehouses = MaxWarehouses;
                if (PreviousMaxWarehouses == 0)
                    MaxWarehouses = number_of_warehouses;
                else
                    ++MaxWarehouses;
                String msg = "Loading Warehouse " + MaxWarehouses + "...";
                System.out.println(msg);
                JBButil.getLog().info(msg);
                // Item Table must be loaded first since the warehouses use it
                // for
                // construction
                if (PreviousMaxWarehouses == 0) {
                    loadItemTable();
                }
                loadWarehouseTable();
                loadCustomerTable();
                loadWarehouseHistoryTable();
                loadInitialOrders();
                System.out.println("");
                System.out.println("");
            }
                break;
            case 1: {
                PreviousMaxWarehouses = MaxWarehouses;
                if (PreviousMaxWarehouses == 0)
                    MaxWarehouses = number_of_warehouses;
                else
                    ++MaxWarehouses;
                String msg = "Loading Warehouse " + MaxWarehouses + "...";
                System.out.println(msg);
                JBButil.getLog().info(msg);
                // Item Table must be loaded first since the warehouses use it
                // for
                // construction
                if (PreviousMaxWarehouses == 0) {
                    loadItemTable();
                }
            }
                break;
            case 2: {
                loadWarehouseTable();
            }
                break;
            case 3: {
                loadCustomerTable();
            }
                break;
            case 4: {
            }
                break;
            case 5: {
                loadInitialOrders();
                System.out.println("");
                System.out.println("");
            }
                break;
        }
    }

    public synchronized void startAutomated(short inWarehouseId,
            int rampup_time, int measurement_time) {
        companyTimerData.zeroTimerData();
        this.rampup_time = rampup_time;
        this.measurement_time = measurement_time;
        TimerData warehouseTimerData = getTimerDataPtr(inWarehouseId);
        warehouseTimerData.zeroTimerData();
        warehouseTimerData.setRampUpTime(rampup_time);
        warehouseTimerData.setMeasurementTime(measurement_time);
    }

    public synchronized void startValidation(short inWarehouseId) {
        companyTimerData.zeroTimerData();
        TimerData warehouseTimerData = getTimerDataPtr(inWarehouseId);
        warehouseTimerData.zeroTimerData();
    }

    public synchronized void addWarehouseThread(TransactionManager tm) {
        warehouseThreads.add(tm);
    }

    public synchronized void prepareForStart() {
        initThreadsCount = 0;
        threadsDoneCount = 0;
        stopThreadsCount = 0;
        mode = runModes.DEFAULT_MODE;
        warehouseThreads.clear();
    }

    long getElapsedTime() {
        return elapsed_time;
    }

    // return with write lock if lockFlag is True
    public Warehouse getWarehousePtr(short warehouseId, boolean lockFlag) {
        // index lookup get write lock if lockFlag is True
        Warehouse result;
        result = (Warehouse) warehouseTable[warehouseId];
        return result;
    }

    // return with write lock if lockFlag is True
    public Customer getCustomer(long customerId, boolean lockflag) {
        // index lookup gets write lock
        Customer result;
        result = (Customer) customerTable.get(customerId);
        if (JBButil.getLog().isLoggable(Level.FINEST)) {
            Logger log = JBButil.getLog();
            log.finest("Company::getCustomer");
            log.finest("  customerId=" + customerId);
            log.finest("  Customer=" + result);
        }
        ;
        return result;
    }

    public boolean isCustomer(long customerId) {
        return customerTable.containsKey(customerId);
    }

    public long buildUniqueCustomerKey(short warehouseId, byte districtId,
            short customerId) {
        // warehouseId=1:12, districtId=13:24, customerId=25-64
        long key = warehouseId;
        key = key << 12;
        key += districtId;
        key = key << 40;
        key += customerId;
        return key;
    }

    // return customer with a write lock if lockFlag is True
    public Customer getCustomerByLastName(short warehouseId, byte districtId,
            String last_name) {
        String custKey = Integer.toString((int) warehouseId) + "_"
                + Integer.toString((int) districtId) + "_" + last_name;
        String lastCustKey = custKey + "_~";
        Long custId = (Long) lastNameCustomerTable.getMedianValue(custKey,
                lastCustKey);
        return (Customer) customerTable.get(custId);
    }

    public synchronized void setJVMInstanceValues(long instanceCount) {
        this.jvm_instances = instanceCount;
    }

    public synchronized void setMultiJVMRampingValues(float rampup, float rampdown) {
        this.per_jvm_warehouse_rampup = rampup;
        this.per_jvm_warehouse_rampdown = rampdown;
    }
    
    public synchronized void setPopulationValues(int population_base) {
        if (population_base > 0) {
            MaxDistrictsPerWarehouse = 10;
            MaxCustomersPerDistrict = population_base;
            if (JBBmain.overrideItemTableSize > 0) {
                MaxItems = JBBmain.overrideItemTableSize;
            }
            else {
                MaxItems = 20000;
            }
        }
        else {
            // minimally populated databases (used for quicker startup &
            // turnaround during testing)
            MaxDistrictsPerWarehouse = 5;
            MaxCustomersPerDistrict = 30;
            MaxItems = 100;
        }
        MaxStock = MaxItems;
        InitialOrders = MaxCustomersPerDistrict;
        InitialNewOrders = (short) (MaxCustomersPerDistrict * 0.30);
        InitialHistories = MaxCustomersPerDistrict;
    }

    public synchronized void setPopulationValues(int population_base,
            int itemtable_size) {
        if (population_base > 0) {
            MaxDistrictsPerWarehouse = 10;
            MaxCustomersPerDistrict = population_base;
            MaxItems = itemtable_size;
        }
        else {
            // minimally populated databases (used for quicker startup &
            // turnaround during testing)
            MaxDistrictsPerWarehouse = 5;
            MaxCustomersPerDistrict = 30;
            MaxItems = 100;
        }
        MaxStock = MaxItems;
        InitialOrders = MaxCustomersPerDistrict;
        InitialNewOrders = (short) (MaxCustomersPerDistrict * 0.30);
        InitialHistories = MaxCustomersPerDistrict;
    }

    public synchronized TimerData getTimerDataPtr(short warehouseId) {
        TimerData temp;
        // System.out.println("warehouseID: " + warehouseId);
        // new Exception().printStackTrace();
        if (warehouseId == 0)
            temp = companyTimerData;
        else
            temp = (TimerData) timerdataTable[warehouseId];
        return temp;
    }

    public synchronized void trimOrdersForSteadyState() {
        short warehouseId;
        Warehouse warehousePtr;
        int initialOrders = this.getInitialOrders();
        int initialNewOrders = this.getInitialNewOrders();
        // print results
        for (warehouseId = 1; warehouseId <= MaxWarehouses; warehouseId++) {
            warehousePtr = getWarehousePtr(warehouseId, false); // protected by
            warehousePtr.trimOrdersForSteadyState(initialOrders,
                    initialNewOrders);
        }
    }

    public void displayResultTotals(boolean showWarehouseDetail) {
        short warehouseId;
        TimerData warehouseTimerData;
        // Wait for all threads to start.
        synchronized (initThreadsStateChange) {
            while (initThreadsCount != MaxWarehouses) {
                try {
                    initThreadsStateChange.wait();
                }
                catch (InterruptedException e) {
                }
            }
        }
        // Multi-JVM changes
        if (JBBmain.multiJVMMode) {
            System.out.println(JBBmain.instanceId + ":READY");
            JBBmain.socOut.println(JBBmain.instanceId + ":READY");
            JBBmain.socOut.flush();
            String mesg = "NULL";
            String needMsg = JBBmain.instanceId + ":START";
            try {
                while (!mesg.matches(needMsg))
                    mesg = JBBmain.socIn.readLine();
            }
            catch (java.io.IOException excep) {
                JBButil.getLog().log(Level.WARNING,
                        excep + ": Unable to read from socket", excep);
                System.exit(1);
            }
            String messageReceived = ">>>>>>>> Got Message: " + mesg;
            JBButil.getLog().info(messageReceived);
            System.out.println(messageReceived);
        }
        // Tell everybody it's time for warmups.
        setrunMode(runModes.RAMP_UP);
        synchronized (initThreadsCountMonitor) {
            initThreadsCountMonitor.notifyAll();
        }
        String msg;
        long start_time = 0;
        long end_time = 0;
        long start_rampup_time = 0;
        long end_rampdown_time = 0;
        DecimalFormat df = new DecimalFormat("#####.##");
        long tmpTime = 0;
        if (JBBmain.multiJVMMode) {
            setrunMode(runModes.MULTI_RAMP);
            // multi-jvm ramp-up padding
            start_rampup_time = System.currentTimeMillis();
            tmpTime = (long)per_jvm_warehouse_rampup;
            msg = "User Thread Multi-JVM Rampup padding began "
                    + new Date().toString() + " for " + tmpTime + " seconds";
            JBButil.getLog().info(msg);
            System.out.println(msg); // display rampup start time
            JBButil.SecondsToSleep((int) tmpTime);
        }
        if (rampup_time > 0) {
            msg = "User Thread Rampup began " + new Date().toString() + " for "
                    + df.format(rampup_time / 60.) + " minutes";
            JBButil.getLog().info(msg);
            System.out.println(msg); // display rampup start time
            JBButil.SecondsToSleep((int) rampup_time);
        }
        if (measurement_time > 0) {
            msg = "Timing Measurement began " + new Date().toString() + " for "
                    + df.format(measurement_time / 60.) + " minutes";
            setrunMode(runModes.RECORDING);
            start_time = System.currentTimeMillis();
            JBButil.getLog().info(msg);
            System.out.println(msg); // display start time
            // Wait while user threads do the recorded run
            JBButil.SecondsToSleep((int) measurement_time);
        }
        end_time = System.currentTimeMillis();
        msg = "Timing Measurement ended " + new Date().toString();
        JBButil.getLog().info(msg);
        System.out.println(msg); // display stop time
        if (JBBmain.multiJVMMode) {
            setrunMode(runModes.MULTI_RAMP);
            // multi-jvm ramp-down padding
            tmpTime = (long) per_jvm_warehouse_rampdown;
            msg = "User Thread Multi-JVM Rampdown padding began "
                    + new Date().toString() + " for " + tmpTime + " seconds";
            JBButil.getLog().info(msg);
            System.out.println(msg); // display rampup start tim
            JBButil.SecondsToSleep((int) tmpTime);
            end_rampdown_time = System.currentTimeMillis();
        }
        setrunMode(runModes.RAMP_DOWN);
        elapsed_time = end_time - start_time;
        System.out.println("");
        synchronized (threadsDoneCountMonitor) {
            while (threadsDoneCount != MaxWarehouses) {
                try {
                    threadsDoneCountMonitor.wait();
                }
                catch (InterruptedException e) {
                }
            }
        }
        setrunMode(runModes.STOP);
        synchronized (stopThreadsCountMonitor) {
            while (stopThreadsCount != MaxWarehouses) {
                try {
                    stopThreadsCountMonitor.wait();
                }
                catch (InterruptedException e) {
                }
            }
        }
        // print results
        outPropFile.println(propPrefix + "warehouses=" + MaxWarehouses);
        if (JBBmain.multiJVMMode) {
            outPropFile.println(propPrefix + "start_rampup_time_milliseconds="
                    + start_rampup_time);
            outPropFile.println(propPrefix + "end_rampdown_time_milliseconds="
                    + end_rampdown_time);
        }
        outPropFile.println(propPrefix + "start_time_milliseconds="
                + start_time);
        outPropFile.println(propPrefix + "end_time_milliseconds=" + end_time);
        outPropFile
                .println(propPrefix + "elapsed_milliseconds=" + elapsed_time);
        System.out.println("");
        int total_warehouse_trans = 0;
        long min_transaction_count = Long.MAX_VALUE;
        long max_transaction_count = Long.MIN_VALUE;
        for (warehouseId = 1; warehouseId <= MaxWarehouses; warehouseId++) {
            // System.out.print("\nTOTALS FOR WAREHOUSE " + warehouseId + ":");
            warehouseTimerData = getTimerDataPtr(warehouseId);
            warehouseTimerData.calculateResponseTimeStats();
            // CJB 2001/11/01: warehouse data is not used when calculating
            // results,
            // and all this data is causing the results file to get too large,
            // so we are making it optional.
            if (showWarehouseDetail)
                warehouseTimerData.propResults(propPrefix + "warehouse_"
                        + warehouseId + ".", outPropFile);
            total_warehouse_trans = 0;
            for (int txntype = 0; txntype < Transaction.maxTxnTypes; txntype++) {
                total_warehouse_trans += warehouseTimerData
                        .getTransactionCount(txntype);
            }
            if (total_warehouse_trans < min_transaction_count) {
                min_transaction_count = total_warehouse_trans;
            }
            if (total_warehouse_trans > max_transaction_count) {
                max_transaction_count = total_warehouse_trans;
            }
        }
        // System.out.print("\n\n\nTOTALS FOR COMPANY:");
        System.out.println("Calculating results");
        companyTimerData.calculateResponseTimeStats();
        companyTimerData.displayThreadResults();
        long diff = max_transaction_count - min_transaction_count;
        float diff_pct = 100 * (float) diff / (float) max_transaction_count;
        System.out.println("");
        System.out.println("Minimum transactions by a warehouse = "
                + min_transaction_count);
        System.out.println("Maximum transactions by a warehouse = "
                + max_transaction_count);
        System.out.println("Difference (thread spread) = " + diff + " ("
                + df.format(diff_pct) + "%)");
        System.out.println("");
        companyTimerData.displayResults(
                ("COMPANY with " + MaxWarehouses + " warehouses "), JBButil
                        .currentTotalMem(), JBButil.currentFreeMem());
        companyTimerData.propResults(propPrefix + "company.", outPropFile,
                JBButil.currentTotalMem(), JBButil.currentFreeMem());
        companyTimerData
                .propThreadResults(propPrefix + "company.", outPropFile);
        outPropFile.println(propPrefix + "company.min_warehouse_transactions="
                + min_transaction_count);
        outPropFile.println(propPrefix + "company.max_warehouse_transactions="
                + max_transaction_count);
        if (Transaction.steadyStateMem) {
            trimOrdersForSteadyState();
        }
        setrunMode(runModes.DEFAULT_MODE);
    }

    public synchronized void loadWarehouseTable() {
        for (short i = (short) (PreviousMaxWarehouses + 1); i <= MaxWarehouses; ++i) {
            // WRR: Pass warehouseId here so Random stream can be registered.
            Warehouse newWarehouse = new Warehouse();
            newWarehouse.initWarehouse(this, itemTable, i);
            newWarehouse.setUsingRandom(i);
            warehouseTable[i] = newWarehouse;
            TimerData newTimerData = new TimerData();
            timerdataTable[i] = newTimerData;
        }
    }

    public synchronized void loadCustomerTable() {
        // System.out.println("MaxCustomers = " + MaxDistrictsPerWarehouse *
        // MaxCustomersPerDistrict);
        short customerId;
        long customers_loaded = 0;
        // go through all of the warehouses
        for (short warehouseId = (short) (PreviousMaxWarehouses + 1); warehouseId <= MaxWarehouses; ++warehouseId) {
            // go through all districts
            for (byte districtId = 1; districtId <= MaxDistrictsPerWarehouse; ++districtId) {
                // go through and create customers for each district
                for (customerId = 1; customerId <= MaxCustomersPerDistrict; ++customerId) {
                    // Customer newCustomer = Customer.createCustomer(this,
                    // null);
                    Customer newCustomer = new Customer();
                    newCustomer.setUsingRandom(customerId, warehouseId,
                            districtId);
                    long uniqueCustomerNumber = buildUniqueCustomerKey(
                            warehouseId, districtId, customerId);
                    customerTable.put(uniqueCustomerNumber, newCustomer);
                    String custNameKey = Integer.toString((int) warehouseId)
                            + "_" + Integer.toString((int) districtId) + "_"
                            + newCustomer.getLastName() + "_"
                            + Integer.toString((int) customerId);
                    lastNameCustomerTable
                            .put(custNameKey, uniqueCustomerNumber);
                    if (JBButil.getLog().isLoggable(Level.FINEST)) {
                        Logger log = JBButil.getLog();
                        log.finest("Company::loadCustomerTable");
                        log.finest("  newCustomer=" + newCustomer);
                        log.finest("  customerId=" + customerId);
                        log.finest("  districtId=" + districtId);
                        log.finest("  warehouseId=" + warehouseId);
                        log.finest("  uniqueCustomerNumber="
                                + uniqueCustomerNumber);
                        log.finest("  custNameKey=" + custNameKey);
                    }
                }
                customers_loaded += customerId - 1;
            }
        }
    }

    public void setrunMode(runModes inmode) {
        // first set per-warehouse run-mode
        TransactionManager tm;
        for (int i = 0; i < warehouseThreads.size(); i++) {
            tm = (TransactionManager) warehouseThreads.get(i);
            tm.setrunMode(inmode);
        }
        mode = inmode;
    }

    public synchronized void loadItemTable() {
        for (int i = 1; i <= MaxItems; ++i) {
            Item anItem = new Item();
            anItem.setUsingRandom(i);
            itemTable.put(i, anItem);
        }
    }

    public synchronized void dumpWarehouseTable() {
    }

    public synchronized void dumpCustomerTable() {
    }

    public synchronized void dumpItemTable() {
    }

    public synchronized void loadWarehouseHistoryTable() {
        for (short i = (short) (PreviousMaxWarehouses + 1); i <= MaxWarehouses; ++i) {
            ((Warehouse) warehouseTable[i]).loadHistoryTable();
        }
    }

    public synchronized void loadInitialOrders() {
        // go through all of the warehouses
        for (short warehouseId = (short) (PreviousMaxWarehouses + 1); warehouseId <= MaxWarehouses; ++warehouseId) {
            NewOrderTransaction newOrderTransaction = new NewOrderTransaction(
                    this, warehouseId);
            // go through all of the districts for each warehouse
            for (byte districtId = 1; districtId <= MaxDistrictsPerWarehouse; ++districtId) {
                // go through all of the customers for each district
                for (short customerId = 1; customerId <= MaxCustomersPerDistrict; ++customerId) {
                    newOrderTransaction.init();
                    newOrderTransaction.setDistrictandCustomer(districtId,
                            customerId);
                    newOrderTransaction.processPreloadedOrders();
                }
            }
        }
    }

    void setPropOutputPrefix(String s) {
        propPrefix = s;
    }
}
