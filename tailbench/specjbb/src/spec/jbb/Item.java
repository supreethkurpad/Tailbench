/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 * 
 */
package spec.jbb;

import java.math.BigDecimal;

public class Item {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "(C) Copyright IBM Corp., 1996 - 2005"
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    private String      name;

    private String      brandInformation;

    private BigDecimal  price;

    private int         id;

    public Item() {
    };

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getBrandInfo() {
        return brandInformation;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public synchronized void setUsingRandom(int inId) {
        id = inId;
        name = new String(JBButil.create_random_a_string(14, 24));
        float temp = JBButil.create_random_float_val_return(1.00f, 100.00f,
                .01f);
        price = new BigDecimal(temp).setScale(2, BigDecimal.ROUND_HALF_UP);
        brandInformation = new String(JBButil.create_a_string_with_original(26,
                50, 10, /* hit */null));
    }
}
