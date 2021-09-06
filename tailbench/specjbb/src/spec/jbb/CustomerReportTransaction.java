/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import spec.jbb.infra.Util.TransactionLogBuffer;
import spec.jbb.infra.Util.XMLTransactionLog;
import java.math.BigDecimal;
import java.util.*;

class CustomerReportTransaction extends Transaction {
    // This goes right after each class/interface statement
    static final String          COPYRIGHT            = "SPECjbb2005,"
                                                              + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                                              + "All rights reserved,"
                                                              + "(C) Copyright IBM Corp., 1996 - 2005"
                                                              + "All rights reserved,"
                                                              + "Licensed Materials - Property of SPEC";

    private Company              company;

    private short                warehouseId;

    private byte                 districtId;

    private boolean              home;

    private char                 use_customerId;

    private String               cust_last_name;

    private short                customerId;

    private short                customerWarehouseId;

    private byte                 customerDistrictId;

    private Date                 reportTime;

    private Warehouse            warehousePtr;

    private District             districtPtr;

    private Customer             customerPtr;

    private TransactionLogBuffer initLog;

    private TransactionLogBuffer customerLog;

    private XMLTransactionLog    xmlCustomerLog;

    int                          lastPaymentsQuantity = 5;

    int                          lastOrdersQuantity   = 5;

    Date                         paymentDates[]       = new Date[lastPaymentsQuantity];

    BigDecimal                   paymentAmount[]      = new BigDecimal[lastPaymentsQuantity];

    Date                         orderDates[]         = new Date[lastOrdersQuantity];

    BigDecimal                   orderAmount[]        = new BigDecimal[lastOrdersQuantity];

    String[]                     validationScreen     = {
            "                                 CUSTOMER REPORT",
            "Date: 31-12-2004 04:30:07",
            "",
            "Warehouse:    1                          District:  1",
            "mypIw7RUtW7hoQdmASd                      Q5pck00LNkV         ",
            "vIfg0HYwMq                               U2KXH6WkfD          ",
            "PWBAjD5CvVrZNaS      Vv 76447-9863       8FUvKJooC6SiWztmS    Wu 76407-6439",
            "",
            "Customer:   19  Cust-Warehouse:    1  Cust-District:  1",
            "Name:   5C8n4mfBkU32Y8   OE BARABLEPRES          Since:  31-12-2004",
            "        Dy6cn4FXooQQHqDkl                        Credit: GC",
            "        syXdFH3PQeJjwyR6Kr                       %Disc:   2.65",
            "Last payments         Date:                   Amount:",
            "                      31-12-2004 04:30:07    $10.00",
            "                      31-12-2004 04:30:07    $10.00",
            "                      31-12-2004 04:30:07    $10.00",
            "                      31-12-2004 04:30:07    $10.00",
            "                      31-12-2004 04:30:07    $10.00",
            "Last orders           Date:                   Amount:",
            "                      31-12-2004 04:30:07    $3594.00", "", "",
            "", ""
                                                      };

    boolean[]                    checkLine            = {
            true, false, true, true, true, true, true, true, true, false, true,
            true, true, false, false, false, false, false, true, false, true,
            true, true, true
                                                      };

    public CustomerReportTransaction(Company inCompany, short inWarehouseId) {
        company = inCompany;
        warehouseId = inWarehouseId;
        warehousePtr = company.getWarehousePtr(warehouseId, false);
        customerLog = new TransactionLogBuffer();
        setupCustomerLog();
        xmlCustomerLog = new XMLTransactionLog();
        initLog = new TransactionLogBuffer(customerLog);
        setupInitLog();
    }

    private void setupCustomerLog() {
        customerLog.putText("CUSTOMER REPORT", 33, 0, 15);
        customerLog.putText("Date:", 0, 1, 5);
        customerLog.putText("Warehouse:", 0, 3, 10);
        customerLog.putText("District:", 41, 3, 9);
        customerLog.putText("Customer:", 0, 8, 9);
        customerLog.putText("Cust-Warehouse:", 16, 8, 15);
        customerLog.putText("Cust-District:", 38, 8, 14);
        customerLog.putText("Name:", 0, 9, 5);
        customerLog.putText("Since:", 49, 9, 6);
        customerLog.putText("Credit:", 49, 10, 7);
        customerLog.putText("%Disc:", 49, 11, 6);
        customerLog.putText("Last payments", 0, 12, 13);
        customerLog.putText("Date:", 22, 12, 5);
        customerLog.putText("Amount:", 46, 12, 7);
        customerLog.putText("Last orders", 0, 18, 11);
        customerLog.putText("Date:", 22, 18, 5);
        customerLog.putText("Amount:", 46, 18, 7);
    }

    private void setupInitLog() {
        int i;
        initLog.putText("DD-MM-YYYY", 6, 1, 10);
        initLog.putText("hh:mm:ss", 17, 1, 8);
        initLog.putCharFill('9', 11, 3, 4);
        initLog.putCharFill('9', 51, 3, 2);
        initLog.putCharFill('X', 0, 4, 20);
        initLog.putCharFill('X', 41, 4, 20);
        initLog.putCharFill('X', 0, 5, 20);
        initLog.putCharFill('X', 41, 5, 20);
        initLog.putCharFill('X', 0, 6, 20);
        initLog.putCharFill('X', 21, 6, 2);
        initLog.putText("XXXXX-XXXX", 24, 6, 10);
        initLog.putCharFill('X', 41, 6, 20);
        initLog.putCharFill('X', 62, 6, 2);
        initLog.putText("XXXXX-XXXX", 65, 6, 10);
        initLog.putCharFill('9', 10, 8, 4);
        initLog.putCharFill('9', 32, 8, 4);
        initLog.putCharFill('9', 53, 8, 2);
        initLog.putCharFill('X', 8, 9, 16);
        initLog.putCharFill('X', 25, 9, 2);
        initLog.putCharFill('X', 28, 9, 16);
        initLog.putText("DD-MM-YYYY", 57, 9, 10);
        initLog.putCharFill('X', 8, 10, 20);
        initLog.putCharFill('X', 57, 10, 2);
        initLog.putCharFill('X', 8, 11, 20);
        initLog.putText("99.99", 57, 11, 5);
        for (i = 0; i < 5; i++) {
            initLog.putText("DD-MM-YYYY", 22, 13 + i, 10);
            initLog.putText("hh:mm:ss", 33, 13 + i, 8);
            initLog.putText("$9999.99", 45, 13 + i, 8);
        }
        for (i = 0; i < 5; i++) {
            initLog.putText("DD-MM-YYYY", 22, 19 + i, 10);
            initLog.putText("hh:mm:ss", 33, 19 + i, 8);
            initLog.putText("$9999.99", 45, 19 + i, 8);
        }
    }

    public String getMenuName() {
        return "CustomerReport";
    }

    public synchronized void delete() {
    }

    public synchronized void init() {
        JBButil.getLog().entering("spec.jbb.CustomerReportTransaction", "init");
        districtId = (byte) JBButil.random(1, company
                .getMaxDistrictsPerWarehouse(), warehouseId);
        districtPtr = warehousePtr.getDistrictPtr(districtId, false);
        int x = (int) JBButil.random(1, 100, warehouseId);
        int y = (int) JBButil.random(1, 100, warehouseId);
        if (y <= 60) {
            cust_last_name = JBButil.choose_random_last_name(company
                    .getMaxCustomersPerDistrict(), warehouseId);
            use_customerId = 'F';
        }
        else {
            customerId = JBButil.create_random_customer_id(company
                    .getMaxCustomersPerDistrict(), warehouseId);
            use_customerId = 'T';
        }
        if (x <= 85) {
            customerDistrictId = districtId;
            customerWarehouseId = warehouseId;
        }
        else {
            customerDistrictId = (byte) JBButil.random(1, company
                    .getMaxDistrictsPerWarehouse(), warehouseId);
            int maxWarehouses = company.getMaxWarehouses();
            for (customerWarehouseId = (short) JBButil.random(1, maxWarehouses,
                    warehouseId); (customerWarehouseId == warehouseId)
                    && (maxWarehouses > 1); customerWarehouseId = (short) JBButil
                    .random(1, maxWarehouses, warehouseId))
                ;
        }
        home = (customerWarehouseId == warehouseId);
        JBButil.getLog().exiting("spec.jbb.CustomerReportTransaction", "init");
    }

    public boolean process() {
        JBButil.getLog().entering("spec.jbb.CustomerReportTransaction",
                "process");
        String warehousename = warehousePtr.getName();
        switch (use_customerId) {
            case 'T': {
                long uniqueCustomerId = company.buildUniqueCustomerKey(
                        customerWarehouseId, customerDistrictId, customerId);
                // get customer ptr with write lock
                customerPtr = company.getCustomer(uniqueCustomerId, true);
            }
                break;
            case 'F':
                // get customer ptr with write lock
                customerPtr = company
                        .getCustomerByLastName(customerWarehouseId,
                                customerDistrictId, cust_last_name);
        }
        if (customerPtr == null) {
        }
        else {
            JBBDataStorage historyTable = warehousePtr.getHistoryTable();
            Iterator historyIter = historyTable.elements();
            History history;
            History payments[] = new History[historyTable.size()];
            int histCount = 0;
            int i = 0;
            while (historyIter.hasNext()) {
                history = (History) historyIter.next();
                if (history.getCustomerId() == customerPtr.getId()) {
                    histCount++;
                    payments[histCount] = history;
                }
            }
            for (i = histCount; i > 0; i--) {
                if (histCount - i < lastPaymentsQuantity) {
                    paymentDates[histCount - i] = payments[i].getDate();
                    paymentAmount[histCount - i] = payments[i].getAmount();
                }
                else {
                    break;
                }
            }
            JBBDataStorage orderTable = districtPtr.getOrderTable();
            Iterator orderIter = orderTable.elements();
            Order order;
            Order lastOrders[] = new Order[orderTable.size()];
            int orderCount = 0;
            int lastOrdersQuantity = 5;
            while (orderIter.hasNext()) {
                order = (Order) orderIter.next();
                if (order.getCustomerPtr() == customerPtr) {
                    orderCount++;
                    lastOrders[orderCount] = order;
                }
            }
            for (i = orderCount; i > 0; i--) {
                if (orderCount - i < lastOrdersQuantity) {
                    orderDates[orderCount - i] = lastOrders[i].getEntryTime();
                    orderAmount[orderCount - i] = lastOrders[i]
                            .getTotalAmount();
                }
                else {
                    break;
                }
            }
            reportTime = new Date();
        }
        ;
        JBButil.getLog().exiting("spec.jbb.CustomerReportTransaction",
                "process");
        return true;
    }

    public synchronized void display() {
        System.out.println("CustomerTransaction Display *********************");
        System.out.println("Warehouse ID is " + warehouseId
                + " ******************");
        System.out.println("District  ID is " + districtId
                + " ******************");
        System.out.println("Customer  ID is " + customerId
                + " ******************");
        System.out
                .println("**************************************************");
    }

    public synchronized void initializeTransactionLog() {
        // "00000000001111111111222222222233333333334444444444555555555566666666667777777777"
        // "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        // " CUSTOMER REPORT"
        // "Date: DD-MM-YYYY hh:mm:ss"
        // "Warehouse: 9999 District: 99"
        // + "XXXXXXXXXXXXXXXXXXXX XXXXXXXXXXXXXXXXXXXX"
        // + "XXXXXXXXXXXXXXXXXXXX XXXXXXXXXXXXXXXXXXXX"
        // + "XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX XXXXXXXXXXXXXXXXXXXX XX
        // XXXXX-XXXX"
        // + "\n"
        // + "Customer: 9999 Cust-Warehouse: 9999 Cust-District: 99"
        // + "Name: XXXXXXXXXXXXXXXX XX XXXXXXXXXXXXXXXX Since: DD-MM-YYYY"
        // + " XXXXXXXXXXXXXXXXXXXX Credit: XX"
        // + " XXXXXXXXXXXXXXXXXXXX %Disc: 99.99"
        // + "Last payments Date: Amount:"
        // + " DD-MM-YYYY hh:mm:ss $9999.99"
        // + " DD-MM-YYYY hh:mm:ss $9999.99"
        // + " DD-MM-YYYY hh:mm:ss $9999.99"
        // + " DD-MM-YYYY hh:mm:ss $9999.99"
        // + " DD-MM-YYYY hh:mm:ss $9999.99"
        // + "Last orders Date: Amount:"
        // + " DD-MM-YYYY hh:mm:ss $9999.99"
        // + " DD-MM-YYYY hh:mm:ss $9999.99"
        // + " DD-MM-YYYY hh:mm:ss $9999.99"
        // + " DD-MM-YYYY hh:mm:ss $9999.99"
        // + " DD-MM-YYYY hh:mm:ss $9999.99"
        // + "\n\n";
        JBButil.getLog().entering("spec.jbb.CustomerReportTransaction",
                "initializeTransactionLog");
        if (Transaction.enableLogWrite)
            initLog.display();
        JBButil.getLog().exiting("spec.jbb.CustomerReportTransaction",
                "initializeTransactionLog");
    }

    public synchronized void processTransactionLog() {
        JBButil.getLog().entering("spec.jbb.CustomerReportTransaction",
                "processTransactionLog");
        customerLog.clearBuffer();
        xmlCustomerLog.clear();
        setupCustomerLog();
        Address ware_addr = warehousePtr.getAddress();
        Address dist_addr = districtPtr.getAddress();
        String phone;
        String zip;
        // "00000000001111111111222222222233333333334444444444555555555566666666667777777777"
        // "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        // " CUSTOMER REPORT"
        // "Date: DD-MM-YYYY hh:mm:ss"
        customerLog.putDate(reportTime, 6, 1, 10);
        customerLog.putTime(reportTime, 17, 1, 8);
        // "Warehouse: 9999 District: 99"
        customerLog.putInt(warehouseId, 11, 3, 4);
        customerLog.putInt(districtId, 51, 3, 2);
        // + "XXXXXXXXXXXXXXXXXXXX XXXXXXXXXXXXXXXXXXXX"
        customerLog.putText(ware_addr.getStreet1(), 0, 4, 20);
        customerLog.putText(dist_addr.getStreet1(), 41, 4, 20);
        // + "XXXXXXXXXXXXXXXXXXXX XXXXXXXXXXXXXXXXXXXX"
        customerLog.putText(ware_addr.getStreet2(), 0, 5, 20);
        customerLog.putText(dist_addr.getStreet2(), 41, 5, 20);
        // + "XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX XXXXXXXXXXXXXXXXXXXX XX
        // XXXXX-XXXX"
        customerLog.putText(ware_addr.getCity(), 0, 6, 20);
        customerLog.putText(ware_addr.getState(), 21, 6, 2);
        zip = ware_addr.getZip();
        customerLog.putSubstring(zip, 24, 6, 0, 5);
        customerLog.putChar('-', 29, 6);
        customerLog.putSubstring(zip, 30, 6, 5, 4);
        customerLog.putText(dist_addr.getCity(), 41, 6, 20);
        customerLog.putText(dist_addr.getState(), 62, 6, 2);
        zip = dist_addr.getZip();
        customerLog.putSubstring(zip, 65, 6, 0, 5);
        customerLog.putChar('-', 70, 6);
        customerLog.putSubstring(zip, 71, 6, 5, 4);
        // + "\n"
        // + "Customer: 9999 Cust-Warehouse: 9999 Cust-District: 99"
        if (customerPtr != null) {
            Address cust_addr = customerPtr.getAddress();
            Date custDate = customerPtr.getSince();
            customerLog.putInt(customerPtr.getId(), 10, 8, 4);
            customerLog.putInt(customerPtr.getWarehouseId(), 32, 8, 4);
            customerLog.putInt(customerPtr.getDistrictId(), 53, 8, 2);
            // + "Name: XXXXXXXXXXXXXXXX XX XXXXXXXXXXXXXXXX Since: DD-MM-YYYY"
            customerLog.putText(customerPtr.getFirstName(), 8, 9, 16);
            customerLog.putText(customerPtr.getMiddleName(), 25, 9, 2);
            customerLog.putText(customerPtr.getLastName(), 28, 9, 16);
            customerLog.putDate(custDate, 57, 9, 10);
            // + " XXXXXXXXXXXXXXXXXXXX Credit: XX"
            customerLog.putText(cust_addr.getStreet1(), 8, 10, 20);
            customerLog.putText(customerPtr.getCreditStatus(), 57, 10, 2);
            // + " XXXXXXXXXXXXXXXXXXXX %Disc: 99.99"
            customerLog.putText(cust_addr.getStreet2(), 8, 11, 20);
            customerLog.putDouble(customerPtr.getDiscountRate().movePointRight(
                    2).toString(), 57, 11, 5);
            // + "Last payments Date: Amount:"
            // + " DD-MM-YYYY hh:mm:ss $9999.99"
            // + " DD-MM-YYYY hh:mm:ss $9999.99"
            // + " DD-MM-YYYY hh:mm:ss $9999.99"
            // + " DD-MM-YYYY hh:mm:ss $9999.99"
            // + " DD-MM-YYYY hh:mm:ss $9999.99"
            for (int i = 0; i < lastPaymentsQuantity; i++) {
                if (paymentDates[i] != null) {
                    customerLog.putDate(paymentDates[i], 22, 13 + i, 10);
                    customerLog.putTime(paymentDates[i], 33, 13 + i, 8);
                    customerLog.putDollars(paymentAmount[i], 45, 13 + i, 8);
                }
                else {
                    break;
                }
            }
            // + "Last orders Date: Amount:"
            // + " DD-MM-YYYY hh:mm:ss $9999.99"
            // + " DD-MM-YYYY hh:mm:ss $9999.99"
            // + " DD-MM-YYYY hh:mm:ss $9999.99"
            // + " DD-MM-YYYY hh:mm:ss $9999.99"
            // + " DD-MM-YYYY hh:mm:ss $9999.99"
            for (int i = 0; i < lastOrdersQuantity; i++) {
                if (orderDates[i] != null) {
                    customerLog.putDate(orderDates[i], 22, 19 + i, 10);
                    customerLog.putTime(orderDates[i], 33, 19 + i, 8);
                    customerLog.putDollars(orderAmount[i], 45, 19 + i, 8);
                }
                else {
                    break;
                }
            }
        }
        // create XML representation
        xmlCustomerLog.populateXML(customerLog);
        if (Transaction.enableLogWrite)
            customerLog.display();
        if (Transaction.validationFlag) {
            String[] s = customerLog.validate();
            if (s.length != validationScreen.length) {
                System.out
                        .println("VALIDATION ERROR:  mismatch in screen lengths for CustomerReportTransaction");
                System.out.println("    Screen length should be:  "
                        + validationScreen.length);
                System.out.println("    Screen length is:  " + s.length);
                Transaction.invalidateRun();
            }
            for (int i = 0; i < validationScreen.length; i++) {
                if (checkLine[i]) {
                    if (!s[i].equals(validationScreen[i])) {
                        System.out
                                .println("VALIDATION ERROR:  incorrect output for CustomerReportTransaction");
                        System.out.println("    Line " + (i + 1)
                                + " should be:  |" + validationScreen[i] + "|");
                        System.out.println("    Line " + (i + 1) + " is:  |"
                                + s[i] + "|");
                        Transaction.invalidateRun();
                    }
                }
            }
        }
        JBButil.getLog().exiting("spec.jbb.CustomerReportTransaction",
                "processTransactionLog");
    }
}
