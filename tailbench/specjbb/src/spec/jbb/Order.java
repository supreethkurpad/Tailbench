/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC)
 * 
 * All rights reserved.
 * 
 * Copyright (c) 1996-2005 IBM Corporation, Inc. All rights reserved.
 * 
 */
package spec.jbb;

import java.util.*;
import java.util.logging.Level;
import java.math.BigDecimal;

public class Order {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "(C) Copyright IBM Corp., 1996 - 2005"
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    private Company     company;

    private Customer    customerPtr;

    private Orderline[] orderlineList;

    private BigDecimal  totalAmount;

    private BigDecimal  customerDiscountRate;

    private BigDecimal  districtTax;

    private BigDecimal  warehouseTax;

    private Date        entryDateTime;

    private int         orderId;

    private short       customerId;

    private short       carrierId;

    private short       orderLineCount;

    private byte        districtId;

    private short       warehouseId;

    private boolean     allLocal;

    public Order(Company inCompany, int inOrderId, byte inDistrictId,
            short inWarehouseId, short inCustomerId, Customer inCustomerPtr,
            BigDecimal inDistrictTaxRate, BigDecimal inWarehouseTaxRate,
            BigDecimal inCustomerDiscountRate) {
        this.initOrder(inCompany, inOrderId, inDistrictId, inWarehouseId,
                inCustomerId, inCustomerPtr, inDistrictTaxRate,
                inWarehouseTaxRate, inCustomerDiscountRate);
    };

    public void initOrder(Company inCompany, int inOrderId, byte inDistrictId,
            short inWarehouseId, short inCustomerId, Customer inCustomerPtr,
            BigDecimal inDistrictTaxRate, BigDecimal inWarehouseTaxRate,
            BigDecimal inCustomerDiscountRate) {
        company = inCompany;
        orderId = inOrderId;
        districtId = inDistrictId;
        warehouseId = inWarehouseId;
        customerId = inCustomerId;
        customerPtr = inCustomerPtr;
        orderLineCount = 0;
        orderlineList = new Orderline[(Transaction.aveOrderlines + 10)];
        if (JBButil.getLog().isLoggable(Level.FINEST)) {
            JBButil.getLog().finest(
                    "Order::initOrder  orderlineList=" + orderlineList);
        }
        entryDateTime = new Date();
        carrierId = 0;
        allLocal = true;
        districtTax = inDistrictTaxRate;
        warehouseTax = inWarehouseTaxRate;
        customerDiscountRate = inCustomerDiscountRate;
    }

    public synchronized boolean processLines(Warehouse inWarehousePtr,
            short number_of_orderlines, boolean rollback) {
        short supplyWarehouseId;
        boolean processLinesResult = true;
        BigDecimal amount = BigDecimal.valueOf(0, 2);
        for (short LineNumber = 1; LineNumber <= number_of_orderlines; ++LineNumber) {
            Orderline newOrderline = new Orderline(company, orderId,
                    districtId, warehouseId, LineNumber, number_of_orderlines,
                    rollback);
            supplyWarehouseId = warehouseId;
            if ((JBButil.random(1, 100, warehouseId) == 1)
                    && (company.getMaxWarehouses() > 1)) // Comment #2
            {
                while (supplyWarehouseId == warehouseId) {
                    supplyWarehouseId = (short) JBButil.random(1, company
                            .getMaxWarehouses(), warehouseId);
                }
                allLocal = false;
            }
            newOrderline.setSupplyWarehouse(supplyWarehouseId);
            if (newOrderline.validateAndProcess(inWarehousePtr)) {
                amount = amount.add(newOrderline.getAmount());
                orderlineList[orderLineCount] = newOrderline;
                orderLineCount++;
            }
            else {
                processLinesResult = false;
            }
        }
        BigDecimal subtotal = amount.multiply(
                (BigDecimal.valueOf(1, 0)).subtract(customerDiscountRate))
                .setScale(2, BigDecimal.ROUND_HALF_UP);
        totalAmount = subtotal.multiply(
                (BigDecimal.valueOf(1, 0)).add(warehouseTax).add(districtTax))
                .setScale(2, BigDecimal.ROUND_HALF_UP);
        return processLinesResult;
    }

    public synchronized void dateOrderlines(Date orderdate) {
        int i;
        Orderline orderline;
        if (JBButil.getLog().isLoggable(Level.FINEST)) {
            JBButil.getLog().finest(
                    "Order::dateOrderlines  orderlineList=" + orderlineList
                            + " orderlineCount=" + orderLineCount);
        }
        for (i = 0; i < orderLineCount; i++) {
            orderline = orderlineList[i];
            if (JBButil.getLog().isLoggable(Level.FINEST)) {
                JBButil.getLog().finest(
                        " orderline=" + orderline + " orderline#=" + i);
            }
            ;
            orderline.setDeliveryTime(orderdate);
        }
    }

    public synchronized int getId() {
        int temp = orderId;
        return temp;
    }

    public synchronized Customer getCustomerPtr() {
        Customer temp = customerPtr;
        return temp;
    }

    public synchronized BigDecimal getTotalAmount() {
        BigDecimal temp = totalAmount;
        return temp;
    }

    public synchronized Date getEntryTime() {
        Date temp = entryDateTime;
        return temp;
    }

    public synchronized void setCarrierId(short inCarrierId) {
        carrierId = inCarrierId;
    }

    public synchronized short getCarrierId() {
        short temp = carrierId;
        return temp;
    }

    public synchronized Orderline[] getOrderlineList() {
        if (JBButil.getLog().isLoggable(Level.FINEST)) {
            JBButil.getLog().finest(
                    "Order::getOrderlineList  orderlineList=" + orderlineList);
        }
        return orderlineList;
    }

    public synchronized short getOrderlineCount() {
        short temp = orderLineCount;
        return temp;
    }

    public synchronized void setAllLocal(boolean inAllLocal) {
        allLocal = inAllLocal;
    }

    public synchronized void setEntryDateTime(Date inEntryDateTime) {
        entryDateTime = inEntryDateTime;
    }
}
