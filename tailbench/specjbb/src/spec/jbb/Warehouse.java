/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.util.Iterator;
import java.util.*;
import java.math.BigDecimal;

public class Warehouse {
    // This goes right after each class/interface statement
    static final String    COPYRIGHT = "SPECjbb2005,"
                                             + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                             + "All rights reserved,"
                                             + "(C) Copyright IBM Corp., 1996 - 2005"
                                             + "All rights reserved,"
                                             + "Licensed Materials - Property of SPEC";

    // required fields
    private String         name;

    private Address        address;

    private BigDecimal     taxRate;

    private BigDecimal     ytd;

    private short          warehouseId;

    private Company        company;

    private JBBDataStorage stockTable;

    private JBBDataStorage itemTable;

    private Object[]       districts;

    private int            distCount;

    private Random         per_wh_r;

    private JBBDataStorage historyTable;

    private long           historyCount;

    private long           oldestHistory;

    public Warehouse(Company inCompany, JBBDataStorage inItemTable) {
        company = inCompany;
        districts = new Object[20];
        stockTable = Infrastructure.createStorage();
        itemTable = inItemTable;
        address = new Address();
        historyTable = Infrastructure.createSortedStorage();
        historyCount = 0;
        oldestHistory = 0;
        ytd = BigDecimal.valueOf(0, 2);
        loadStockTable();
    }

    public Warehouse() {
    }

    public void initWarehouse(Company inCompany, JBBDataStorage inItemTable,
            short warehouseId) {
        per_wh_r = JBButil.derived_random_init(warehouseId);
        JBButil.register_warehouse_Random_stream(warehouseId, per_wh_r);
        this.warehouseId = warehouseId;
        ytd = BigDecimal.valueOf(0, 2);
        company = inCompany;
        districts = new Object[20];
        stockTable = Infrastructure.createStorage();
        itemTable = inItemTable;
        address = new Address();
        historyTable = Infrastructure.createSortedStorage();
        historyCount = 0;
        oldestHistory = 0;
        ytd = BigDecimal.valueOf(0, 2);
        loadStockTable();
    }

    public boolean validDistrict(byte inDistrictId) {
        District currentDistrict;
        int i;
        for (i = 0; i < distCount; i++) {
            currentDistrict = (District) districts[i];
            if (currentDistrict.getId() == inDistrictId)
                return true;
        }
        return false; // not found
    }

    public short getId() {
        return warehouseId;
    }

    public BigDecimal getTaxRate() {
        return taxRate;
    }

    public Address getAddress() {
        return address;
    }

    public District getDistrictPtr(byte inDistrictId, boolean lockFlag) {
        District result = null;
        if (inDistrictId > 0) {
            inDistrictId--;
            if (inDistrictId < distCount)
                result = (District) districts[inDistrictId];
        }
        ;
        return result;
    }

    public Stock retrieveStock(int inItemId) {
        return (Stock) stockTable.get(inItemId);
    }

    public Item retrieveItem(int inItemId) {
        return (Item) itemTable.get(inItemId);
    }

    public synchronized void receivePayment(BigDecimal paymentAmount) {
        ytd = ytd.add(paymentAmount);
    }

    public District getDistrict(int distId) {
        District result = null;
        if (distId > 0) {
            distId--;
            if (distId < distCount)
                result = (District) districts[distId];
        }
        ;
        return result;
    }

    public int getDistrictCount() {
        return distCount;
    }

    public synchronized void trimOrdersForSteadyState() {
        int initialOrders = company.getInitialOrders();
        int initialNewOrders = company.getInitialNewOrders();
        trimOrdersForSteadyState(initialOrders, initialNewOrders);
    }

    public synchronized void trimOrdersForSteadyState(int initialOrders,
            int initialNewOrders) {
        int distCount = this.getDistrictCount();
        int distId;
        // iterate over the district list of the given warehouse
        District currentDistrict;
        for (distId = 1; distId <= distCount; distId++) {
            currentDistrict = this.getDistrict(distId);
            if (Transaction.steadyStateMem) {
                currentDistrict
                        .removeOldNewOrders((initialOrders - initialNewOrders));
                currentDistrict.removeOldOrders(initialOrders);
            }
        }
    }

    public synchronized void setUsingRandom(short inWarehouseId) {
        int i;
        warehouseId = inWarehouseId; // set what we get as input into the
        // object
        name = new String(JBButil.create_random_a_string(6, 10, per_wh_r));
        address.setUsingRandom(per_wh_r); // address
        float temp = JBButil.create_random_float_val_return(0.0f, 0.2000f,
                0.0001f, per_wh_r);
        taxRate = new BigDecimal(temp).setScale(4, BigDecimal.ROUND_HALF_UP);
        ytd = BigDecimal.valueOf(30000000, 2);
        distCount = company.getMaxDistrictsPerWarehouse();
        for (i = 0; i < distCount; ++i) {
            byte newDistrictId = (byte) (i + 1);
            District newDistrict = new District(warehouseId, newDistrictId);
            newDistrict.setUsingRandom();
            districts[i] = newDistrict;
        }
    }

    public String getName() {
        return new String(name);
    }

    public void loadStockTable() {
        Iterator itemIter = itemTable.elements();
        Item currItem;
        Stock newStock;
        int i = 0;
        while (itemIter.hasNext()) {
            currItem = (Item) itemIter.next();
            i++;
            newStock = new Stock(company, currItem.getId(), warehouseId);
            stockTable.put(newStock.getId(), newStock);
        }
    }

    private Object updateHistorySynch = new Object();

    public void updateHistory(History inHistory) {
        synchronized (updateHistorySynch) {
            historyCount++;
            historyTable.put(historyCount, inHistory);
        }
    }

    private Object removeOldestHistorySynch = new Object();

    public void removeOldestHistory() {
        synchronized (removeOldestHistorySynch) {
            oldestHistory++;
            historyTable.remove(oldestHistory);
        }
    }

    public synchronized void loadHistoryTable() {
        String historyData;
        // go through all of the districts for each warehouse
        for (byte districtId = 1; districtId <= company
                .getMaxDistrictsPerWarehouse(); ++districtId) {
            // go through all of the customers for each district
            for (short customerId = 1; customerId <= company
                    .getMaxCustomersPerDistrict(); ++customerId) {
                long uniqueCustomerNumber = company.buildUniqueCustomerKey(
                        warehouseId, districtId, customerId);
                // get customer
                Customer customerPtr = company.getCustomer(
                        uniqueCustomerNumber, false);
                BigDecimal amount = BigDecimal.valueOf(1000, 2);
                historyData = new String(JBButil.create_random_a_string(12, 25,
                        warehouseId));
                Date creation_time = new Date();
                History newHistory = new History(customerId, customerPtr
                        .getDistrictId(), customerPtr.getWarehouseId(),
                        districtId, warehouseId, creation_time, amount,
                        historyData);
                updateHistory(newHistory);
            }
        }
    }

    public synchronized JBBDataStorage getHistoryTable() {
        return historyTable;
    }
}
