/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 * 
 */
package spec.jbb;

import java.util.*;
import java.io.PrintStream;
import spec.jbb.infra.Util.TransactionLogBuffer;
import spec.jbb.infra.Util.XMLTransactionLog;
import java.math.BigDecimal;

class DeliveryTransaction extends Transaction {
    // This goes right after each class/interface statement
    static final String          COPYRIGHT     = "SPECjbb2005,"
                                                       + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                                       + "All rights reserved,"
                                                       + "(C) Copyright IBM Corp., 1996 - 2005"
                                                       + "All rights reserved,"
                                                       + "Licensed Materials - Property of SPEC";

    private Company              company;

    private short                warehouseId;

    private short                carrierId;

    private DeliveryHandler      deliveryHandler;

    private Warehouse            warehousePtr;

    private long                 deliveryTime;

    private TransactionLogBuffer deliveryLog;

    private TransactionLogBuffer initLog;

    private TransactionLogBuffer queuedLog;

    private XMLTransactionLog    xmlDeliveryLog;

    String[]                     validationLog = {
            "                                     Delivery", "Warehouse:    1",
            "", "Carrier Number:  3", "",
            "Execution Status: Delivery has been queued ", "", "", "", "", "",
            "", "", "", "", "", "", "", "", "", "", "", "", ""
                                               };

    boolean[]                    checkLine     = {
            true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, true, true, true, true, true,
            true, true
                                               };

    public DeliveryTransaction(Company aCompany, short aWarehouse) {
        company = aCompany;
        warehouseId = aWarehouse;
        warehousePtr = company.getWarehousePtr(warehouseId, false);
        deliveryHandler = new DeliveryHandler(aCompany.getOutDeliveriesFile());
        deliveryLog = new TransactionLogBuffer();
        setupDeliveryLog();
        xmlDeliveryLog = new XMLTransactionLog();
        initLog = new TransactionLogBuffer(deliveryLog);
        setupInitLog();
        queuedLog = new TransactionLogBuffer(16, 64);
    }

    private void setupDeliveryLog() {
        deliveryLog.putText("Delivery", 37, 0, 8);
        deliveryLog.putText("Warehouse:", 0, 1, 10);
        deliveryLog.putInt(warehouseId, 11, 1, 4);
        deliveryLog.putText("Carrier Number:", 0, 3, 15);
        deliveryLog.putText("Execution Status:", 0, 5, 17);
    }

    private void setupInitLog() {
        initLog.putCharFill('9', 16, 3, 2);
        initLog.putCharFill('X', 18, 5, 25);
    }

    public String getMenuName() {
        return "Delivery";
    }

    public void delete() {
    }

    public void init() {
        JBButil.getLog().entering("spec.jbb.DeliveryTransaction", "init");
        carrierId = (short) JBButil.random(1, 10, warehouseId); // 2.7.1.2
        JBButil.getLog().exiting("spec.jbb.DeliveryTransaction", "init");
    }

    public boolean preprocess() {
        // place warehouse ID in output stream
        queuedLog.putText("Warehouse:", 0, 1, 10);
        queuedLog.putInt(warehouseId, 11, 1, 4);
        // place carrier ID in output stream
        queuedLog.putText("Carrier ID:", 16, 1, 12);
        queuedLog.putInt(carrierId, 28, 1, 2);
        // place header for order info in output stream
        queuedLog.putText("Items Delivered", 0, 2, 15);
        queuedLog.putText("District            Order", 0, 3, 25);
        int distCount = warehousePtr.getDistrictCount();
        int distId;
        // iterate over the district list of the given warehouse
        District currentDistrict;
        Date delDate = new Date();
        for (distId = 1; distId <= distCount; distId++) {
            currentDistrict = warehousePtr.getDistrict(distId);
            ;
            Iterator possibleOrderIter = currentDistrict.newOrderIter();
            NewOrder possibleNewOrder = null;
            Order possibleOrder = null;
            Customer possibleCustomerPtr = null;
            Orderline[] requiredOrderLine;
            Orderline orderline;
            while (possibleOrderIter.hasNext()) {
                possibleNewOrder = (NewOrder) possibleOrderIter.next();
                possibleOrder = (Order) possibleNewOrder.getOrderPtr();
                possibleCustomerPtr = (Customer) possibleOrder.getCustomerPtr();
                // check if customer balance is greater than order amount
                BigDecimal hisBalance = possibleCustomerPtr.getBalance();
                BigDecimal orderAmount = possibleOrder.getTotalAmount();
                if ((hisBalance.compareTo(orderAmount)) == -1) {
                    continue;
                }
                // check if the ordered quantity is available in stock at that
                // moment
                requiredOrderLine = possibleOrder.getOrderlineList();
                for (int i = 0; i < requiredOrderLine.length; i++) {
                    orderline = (Orderline) requiredOrderLine[i];
                    if (orderline == null) {
                        continue;
                    }
                    int requiredQuantity = orderline.getQuantity();
                    int itemId = orderline.getItemId();
                    Stock stock = warehousePtr.retrieveStock(itemId);
                    int availableQuantity = stock.getQuantity();
                    if (availableQuantity >= requiredQuantity) {
                        stock.changeQuantity(-requiredQuantity);
                        break;
                    }
                }
            }
            if (possibleNewOrder != null) {
                NewOrder currentNewOrder = currentDistrict
                        .removeNewOrder(possibleNewOrder.getId());
                if (Transaction.steadyStateMem) {
                    currentDistrict
                            .removeOldNewOrders((company.getInitialOrders() - company
                                    .getInitialNewOrders()));
                    currentDistrict.removeOldOrders(company.getInitialOrders());
                }
                Order matchingOrder = currentNewOrder.getOrderPtr();
                matchingOrder.setCarrierId(carrierId);
                matchingOrder.dateOrderlines(delDate);
                queuedLog.putInt(currentDistrict.getId(), 6, distId + 3, 2);
                queuedLog.putInt(currentDistrict.getId(), 20, distId + 3, 5);
                // get customer ptr with a write lock
                Customer customerPtr = matchingOrder.getCustomerPtr();
                customerPtr.adjustBalance(matchingOrder.getTotalAmount());
                customerPtr.incrementDeliveryCount();
                // commit
            } // if
        } // if
        // place finish time in output stream
        Date dayTime = new Date();
        queuedLog.putText("Processing finished at:", 0, 14, 23);
        queuedLog.putDate(dayTime, 24, 14, 10);
        queuedLog.putTime(dayTime, 36, 14, 10);
        return true;
    }

    public synchronized void display(PrintStream out) {
        // " 1 2 3 4 5 6 7 "
        // "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        // "********************************************************************************"
        // 00 "Queueing Time:Wed Jan 15 13:07:05 CST 1997"
        // 01 "Warehouse: 9999 Carrier ID: 99"
        // 02 "Items Delivered "
        // 03 "District Order"
        // 04 " 99 99999"
        // 05 " 99 99999"
        // 06 " 99 99999"
        // 07 " 99 99999"
        // 08 " 99 99999"
        // 09 " 99 99999"
        // 10 " 99 99999"
        // 11 " 99 99999"
        // 12 " 99 99999"
        // 13 " 99 99999"
        // 14 "Processing finished at: Wed Jan 15 13:07:05 CST 1997"
        // 15 ""
        // "********************************************************************************"
        synchronized (out) {
            if (Transaction.enableLogWrite)
                queuedLog.display(out);
        }
        ;
    }

    public void initializeTransactionLog() {
        JBButil.getLog().entering("spec.jbb.DeliveryTransaction",
                "initializeTransactionLog");
        // " 1 2 3 4 5 6 7 "
        // "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        // "********************************************************************************"
        // " DELIVERY"
        // "Warehouse: 9999"
        // ""
        // "Carrier Number: 99"
        // ""
        // "Execution Status: XXXXXXXXXXXXXXXXXXXXXXXXX"
        // "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
        // "********************************************************************************"
        if (Transaction.enableLogWrite)
            initLog.display();
        JBButil.getLog().exiting("spec.jbb.DeliveryTransaction",
                "initializeTransactionLog");
    }

    public synchronized void processTransactionLog() {
        JBButil.getLog().entering("spec.jbb.DeliveryTransaction",
                "processTransactionLog");
        deliveryLog.clearBuffer();
        xmlDeliveryLog.clear();
        setupDeliveryLog();
        // " 1 2 3 4 5 6 7 "
        // "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        // "********************************************************************************"
        // " DELIVERY"
        // "Warehouse: 9999"
        // deliveryScreen.putInt (warehouseId, 11, 1, 4); // already done by
        // ctor SJM
        // ""
        // "Carrier Number: 99"
        deliveryLog.putInt(carrierId, 16, 3, 2);
        // ""
        // "Execution Status: XXXXXXXXXXXXXXXXXXXXXXXXX"
        deliveryLog.putText("Delivery has been queued", 18, 5, 25);
        // "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
        // "********************************************************************************"
        // create XML representation
        xmlDeliveryLog.populateXML(deliveryLog);
        if (Transaction.enableLogWrite)
            deliveryLog.display();
        if (Transaction.validationFlag) {
            String[] s = deliveryLog.validate();
            if (s.length != validationLog.length) {
                StringBuffer sb = new StringBuffer(200);
                sb
                        .append("VALIDATION ERROR:  mismatch in screen lengths for DeliveryTransaction");
                sb.append(System.getProperty("line.separator"));
                sb.append("    Screen length should be:  "
                        + validationLog.length);
                sb.append(System.getProperty("line.separator"));
                sb.append("    Screen length is:  " + s.length);
                JBButil.getLog().warning(sb.toString());
                Transaction.invalidateRun();
            }
            for (int i = 0; i < validationLog.length; i++) {
                if (checkLine[i]) {
                    if (!s[i].equals(validationLog[i])) {
                        StringBuffer sb = new StringBuffer(200);
                        sb
                                .append("VALIDATION ERROR:  incorrect output for DeliveryTransaction");
                        sb.append(System.getProperty("line.separator"));
                        sb.append("    Line " + (i + 1) + " should be:  |"
                                + validationLog[i] + "|");
                        sb.append(System.getProperty("line.separator"));
                        sb.append("    Line " + (i + 1) + " is:  |" + s[i]
                                + "|");
                        JBButil.getLog().warning(sb.toString());
                        Transaction.invalidateRun();
                    }
                }
            }
        }
        JBButil.getLog().exiting("spec.jbb.DeliveryTransaction",
                "processTransactionLog");
    }

    public synchronized boolean process() {
        JBButil.getLog().entering("spec.jbb.DeliveryTransaction", "process");
        Date dayTime = new Date();
        // place queuing time in output stream
        queuedLog.clearBuffer();
        queuedLog.putText("Queueing Time:", 0, 0, 12);
        queuedLog.putDate(dayTime, 12, 0, 10);
        queuedLog.putTime(dayTime, 24, 0, 10);
        // place in deliveryHandler's queue
        deliveryHandler.handleDelivery(this);
        JBButil.getLog().exiting("spec.jbb.DeliveryTransaction", "process");
        return true;
    }
}
