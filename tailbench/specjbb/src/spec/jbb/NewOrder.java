/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.util.logging.Level;

public class NewOrder {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "(C) Copyright IBM Corp., 1996 - 2005"
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    private Order       orderPtr;

    private int         orderId;                                                    // NO_O_ID

    private byte        districtId;                                                 // NO_D_ID

    private short       warehouseId;                                                // NO_W_ID

    public void destroy() {
    };

    public NewOrder(Order inOrderPtr, int inOrderId, byte inDistrictId,
            short inWarehouseId) {
        orderPtr = inOrderPtr;
        orderId = inOrderId;
        districtId = inDistrictId;
        warehouseId = inWarehouseId;
    }

    public void initNewOrder(Order inOrderPtr, int inOrderId,
            byte inDistrictId, short inWarehouseId) {
        orderPtr = inOrderPtr;
        orderId = inOrderId;
        districtId = inDistrictId;
        warehouseId = inWarehouseId;
        if (JBButil.getLog().isLoggable(Level.FINEST)) {
            JBButil.getLog().finest(
                    "NewOrder::initNewOrder, orderId=" + orderId
                            + ", districtId=" + districtId + ", warehouseId="
                            + warehouseId);
        }
    }

    public synchronized int getId() {
        int temp = orderId;
        return temp;
    }

    public synchronized Order getOrderPtr() {
        Order temp = orderPtr;
        return temp;
    }
}
