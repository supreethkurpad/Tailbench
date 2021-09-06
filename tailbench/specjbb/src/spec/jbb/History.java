/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 * 
 */
package spec.jbb;

import java.util.*;
import java.math.BigDecimal;

public class History {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "(C) Copyright IBM Corp., 1996 - 2005"
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    private Date        date;

    private String      data;

    private BigDecimal  amount;

    private short       customerId;

    private byte        customerDistrictId;

    private short       customerWarehouseId;

    private byte        districtId;

    private short       warehouseId;

    public History(short inCustomerId, byte inCustomerDistrictId,
            short inCustomerWarehouseId, byte inDistrictId,
            short inWarehouseId, Date inDate, BigDecimal inAmount, String inData) {
        customerId = inCustomerId;
        customerDistrictId = inCustomerDistrictId;
        customerWarehouseId = inCustomerWarehouseId;
        districtId = inDistrictId;
        warehouseId = inWarehouseId;
        date = inDate;
        amount = inAmount;
        data = inData;
    }

    public History() {
    }

    public void initHistory(short inCustomerId, byte inCustomerDistrictId,
            short inCustomerWarehouseId, byte inDistrictId,
            short inWarehouseId, Date inDate, BigDecimal inAmount, String inData) {
        customerId = inCustomerId;
        customerDistrictId = inCustomerDistrictId;
        customerWarehouseId = inCustomerWarehouseId;
        districtId = inDistrictId;
        warehouseId = inWarehouseId;
        date = inDate;
        amount = inAmount;
        data = inData;
    }

    public synchronized String buildData(String warehouseName,
            String districtName) {
        data = new String((warehouseName + "   " + districtName));
        String temp = data;
        return temp;
    }

    public short getCustomerId() {
        short temp = customerId;
        return temp;
    }

    public Date getDate() {
        Date temp = date;
        return temp;
    }

    public BigDecimal getAmount() {
        BigDecimal temp = amount;
        return temp;
    }
}
