/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.util.*;
import java.util.logging.Level;
import spec.jbb.infra.Util.TransactionLogBuffer;
import spec.jbb.infra.Util.XMLTransactionLog;

class StockLevelTransaction extends Transaction {
    // This goes right after each class/interface statement
    static final String          COPYRIGHT     = "SPECjbb2005,"
                                                       + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                                       + "All rights reserved,"
                                                       + "(C) Copyright IBM Corp., 1996 - 2005"
                                                       + "All rights reserved,"
                                                       + "Licensed Materials - Property of SPEC";

    private Company              company;

    private short                warehouseId;

    private byte                 districtId;

    private District             districtPtr;

    private JBBDataStorage       orderTable;

    private int                  threshold;

    private int                  lowStock;

    private TransactionLogBuffer stockLog;

    private TransactionLogBuffer initLog;

    private XMLTransactionLog    xmlStockLog;

    String[]                     validationLog = {
            "                                    Stock-Level",
            "Warehouse:    1  District:   9", "", "Stock Level Threshold: 15",
            "", "low stock:  14", "", "", "", "", "", "", "", "", "", "", "",
            "", "", "", "", "", "", ""
                                               };

    boolean[]                    checkLine     = {
            true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, true, true, true, true, true,
            true, true
                                               };

    public StockLevelTransaction(Company inCompany, short inWarehouseId) {
        company = inCompany;
        warehouseId = inWarehouseId; // 2.8.1.1
        districtId = (byte) JBButil.random(1, company
                .getMaxDistrictsPerWarehouse(), warehouseId); // 2.5.1.2
        districtPtr = company.getWarehousePtr(warehouseId, false)
                .getDistrictPtr(districtId, false); // 2.8.2.2 bullet 3
        orderTable = districtPtr.getOrderTable();
        // 2.8.3.1
        stockLog = new TransactionLogBuffer();
        setupStockLog();
        xmlStockLog = new XMLTransactionLog();
        initLog = new TransactionLogBuffer(stockLog);
        setupInitLog();
    }

    private void setupStockLog() {
        stockLog.putText("Stock-Level", 36, 0, 11);
        stockLog.putText("Warehouse:", 0, 1, 10);
        stockLog.putInt(warehouseId, 11, 1, 4);
        stockLog.putText("District:", 17, 1, 10);
        stockLog.putInt(districtId, 28, 1, 2);
        stockLog.putText("Stock Level Threshold:", 0, 3, 22);
        stockLog.putText("low stock:", 0, 5, 10);
    }

    private void setupInitLog() {
        // initScreen.putInt(warehouseId, 11, 1, 4);
        // initScreen.putInt(districtId, 28, 1, 2);
        initLog.putCharFill('9', 23, 3, 2);
        initLog.putCharFill('9', 11, 5, 3);
    }

    public void init() {
        JBButil.getLog().entering("spec.jbb.StockLevelTransaction", "init");
        threshold = (int) JBButil.random(10, 20, warehouseId); // 2.8.1.2
        lowStock = 0;
        JBButil.getLog().exiting("spec.jbb.StockLevelTransaction", "init");
    }

    public String getMenuName() {
        return "Stock-Level";
    }

    public synchronized void delete() {
    }

    public boolean process() {
        JBButil.getLog().entering("spec.jbb.StockLevelTransaction", "process");
        int lastMinus20;
        ArrayList<Stock> stockList = new ArrayList<Stock>(200);
        int lastOrderId = districtPtr.lastOrderId();
        if (lastOrderId <= 20)
            lastMinus20 = 1;
        else
            lastMinus20 = lastOrderId - 20;
        for (int id = lastMinus20; id <= lastOrderId; id++) {
            Order currentOrder = (Order) orderTable.get(id);
            if (currentOrder != null) {
                Orderline[] orderlineList = currentOrder.getOrderlineList();
                int orderLineCount = currentOrder.getOrderlineCount();
                if (JBButil.getLog().isLoggable(Level.FINEST)) {
                    JBButil.getLog().finest(
                            "StockLevelTransaction::process"
                                    + "  orderlineList=" + orderlineList
                                    + "  orderLineCount=" + orderLineCount);
                }
                Orderline currentOrderLine;
                int i;
                for (i = 0; i < orderLineCount; i++) {
                    int stockId;
                    currentOrderLine = (Orderline) orderlineList[i];
                    Stock stockPtr = currentOrderLine.getStockPtr();
                    stockList.add(stockPtr);
                    if (JBButil.getLog().isLoggable(Level.FINEST)) {
                        JBButil.getLog().finest(
                                "  orderline#=" + i + "  orderline="
                                        + currentOrderLine);
                    }
                }
            }
        }
        for (Stock stockPtr : stockList) {
            // find stock entries in the given warehouse's stock table that
            // match the item IDs
            // of the items in the orderline list that have less than threshold
            // quantity
            if (JBButil.getLog().isLoggable(Level.FINEST)) {
                JBButil.getLog().finest("  stock#=" + stockPtr.getId());
            }
            if (stockPtr.getQuantity() < threshold)
                ++lowStock;
        }
        JBButil.getLog().exiting("spec.jbb.StockLevelTransaction", "process");
        return true;
    }

    public void initializeTransactionLog() {
        JBButil.getLog().entering("spec.jbb.StockLevelTransaction",
                "initializeTransactionLog");
        // " 1 2 3 4 5 6 7 "
        // "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        // "********************************************************************************"
        // " STOCK-LEVEL"
        // "Warehouse: 9999 District: 99"
        // ""
        // "Stock Level Threshold: 99"
        // ""
        // "low stock: 999"
        // "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
        // "********************************************************************************"
        if (Transaction.enableLogWrite)
            initLog.display();
        JBButil.getLog().exiting("spec.jbb.StockLevelTransaction",
                "initializeTransactionLog");
    }

    public synchronized void processTransactionLog() {
        JBButil.getLog().entering("spec.jbb.StockLevelTransaction",
                "processTransactionLog");
        stockLog.clearBuffer();
        xmlStockLog.clear();
        setupStockLog();
        // " 1 2 3 4 5 6 7 "
        // "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        // "********************************************************************************"
        // " STOCK-LEVEL"
        // "Warehouse: 9999 District: 99"
        // stockScreen.putInt(warehouseId, 11, 1, 4);
        // stockScreen.putInt(districtId, 28, 1, 2);
        // ""
        // "Stock Level Threshold: 99"
        stockLog.putInt(threshold, 23, 3, 2);
        // ""
        // "low stock: 999"
        stockLog.putInt(lowStock, 11, 5, 3);
        // "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
        // "********************************************************************************"
        // create XML representation
        xmlStockLog.populateXML(stockLog);
        if (Transaction.enableLogWrite)
            stockLog.display();
        if (Transaction.validationFlag) {
            String[] s = stockLog.validate();
            if (s.length != validationLog.length) {
                StringBuffer sb = new StringBuffer(200);
                sb
                        .append("VALIDATION ERROR:  mismatch in screen lengths for StockLevelTransaction");
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
                                .append("VALIDATION ERROR:  incorrect output for StockLevelTransaction");
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
        JBButil.getLog().exiting("spec.jbb.StockLevelTransaction",
                "processTransactionLog");
    }
}
