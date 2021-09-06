/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.util.*;
import java.math.BigDecimal;

public class Orderline {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "(C) Copyright IBM Corp., 1996 - 2005"
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    // required fields
    private Date        deliveryDateTime;

    private String      districtInfo;

    private BigDecimal  amount;

    private int         orderId;

    private int         itemId;

    private short       orderLineNumber;

    private short       quantity;

    private byte        districtId;

    private short       warehouseId;

    private short       supplyWarehouseId;

    private Company     company;

    private Warehouse   warehousePtr;

    private Stock       stockPtr;

    private String      itemName;

    private BigDecimal  itemPrice;

    private int         stockQuantity;

    private char        BrandGeneric;

    public Orderline(Company inCompany, int inOrderId, byte inDistrictId,
            short inWarehouseId, short inLineNumber,
            short number_of_orderlines, boolean rollback) {
        company = inCompany;
        orderId = inOrderId;
        districtId = inDistrictId;
        warehouseId = inWarehouseId;
        orderLineNumber = inLineNumber;
        itemId = JBButil.create_random_item_id(company.getMaxItems(),
                warehouseId);
        if ((inLineNumber == number_of_orderlines) && rollback) {
            itemId = 0;
        }
        deliveryDateTime = null;
        quantity = 5;
        amount = BigDecimal.valueOf(0, 2);
        districtInfo = null;
    }

    public synchronized boolean validateAndProcess(Warehouse inWarehousePtr) {
        boolean temp;
        Item itemRef = null;
        Stock stockRef = null;
        if (inWarehousePtr != null) {
            itemRef = inWarehousePtr.retrieveItem(itemId);
            stockRef = inWarehousePtr.retrieveStock(itemId);
        }
        if ((inWarehousePtr != null) && (itemRef != null) && (stockRef != null)) {
            stockPtr = stockRef;
            process(itemRef, stockRef);
            temp = true;
        }
        else
            temp = false;
        return temp;
    }

    private void process(Item itemRef, Stock stockRef) {
        itemName = itemRef.getName();
        itemPrice = itemRef.getPrice();
        String itemData = itemRef.getBrandInfo();
        stockQuantity = stockRef.getQuantity();
        String stock_districtInfo = stockRef.getDistrictInfo(districtId);
        String stockData = stockRef.getData();
        if (stockQuantity >= (quantity + 10))
            stockRef.changeQuantity(-quantity);
        else
            stockRef.changeQuantity(91 - quantity);
        stockRef.incrementYTD(quantity);
        stockRef.incrementOrderCount();
        if (warehouseId == supplyWarehouseId)
            stockRef.incrementRemoteCount();
        amount = BigDecimal.valueOf(quantity).multiply(itemPrice).setScale(2,
                BigDecimal.ROUND_HALF_UP);
        if ((itemData.indexOf("ORIGINAL") > 0)
                && (stockData.indexOf("ORIGINAL") > 0))
            BrandGeneric = 'B';
        else
            BrandGeneric = 'G';
        districtInfo = new String(stock_districtInfo);
    }

    public Stock getStockPtr() {
        return stockPtr;
    }

    public synchronized BigDecimal getAmount() {
        BigDecimal temp = amount;
        return temp;
    }

    public synchronized char getBrandGeneric() {
        char temp = BrandGeneric;
        return temp;
    }

    public synchronized int getItemId() {
        int temp = itemId;
        return temp;
    }

    public synchronized String getItemName() {
        String temp = itemName;
        return temp;
    }

    public synchronized BigDecimal getItemPrice() {
        BigDecimal temp = itemPrice;
        return temp;
    }

    public synchronized short getSupplyWarehouse() {
        short temp = supplyWarehouseId;
        return temp;
    }

    public synchronized int getStockQuantity() {
        int temp = stockQuantity;
        return temp;
    }

    public synchronized void setSupplyWarehouse(short inSupplyWarehouseId) {
        supplyWarehouseId = inSupplyWarehouseId;
    }

    public synchronized Date getDeliveryDateTime() {
        Date temp = deliveryDateTime;
        return temp;
    }

    public synchronized void setDeliveryTime(Date deliveryTime) {
        deliveryDateTime = deliveryTime;
    }

    public synchronized short getQuantity() {
        short temp = quantity;
        return temp;
    }

    public synchronized void setQuantity(short inQuantity) {
        quantity = inQuantity;
    }

    public synchronized void setAmount(BigDecimal inAmount) {
        amount = inAmount;
    }

    public synchronized void setDistrictInfo(String inDistrictInfo) {
        districtInfo = inDistrictInfo;
    }
}
