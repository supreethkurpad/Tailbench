/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.util.Random;

public class Address {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "(C) Copyright IBM Corp., 1996 - 2005"
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    private String      street1;

    private String      street2;

    private String      city;

    private String      state;

    private String      zip;

    public Address() {
    };

    public synchronized void setUsingRandom(Random r) {
        street1 = new String(JBButil.create_random_a_string(10, 20, r));
        street1 = new String(JBButil.create_random_a_string(10, 20, r));
        street2 = new String(JBButil.create_random_a_string(10, 20, r));
        city = new String(JBButil.create_random_a_string(10, 20, r));
        state = new String(JBButil.create_random_a_string(2, 2, r));
        zip = new String(JBButil.create_random_n_string(9, 9, r));
    }

    public synchronized void setUsingRandom(short warehouseId) {
        street1 = new String(JBButil
                .create_random_a_string(10, 20, warehouseId));
        street1 = new String(JBButil
                .create_random_a_string(10, 20, warehouseId));
        street2 = new String(JBButil
                .create_random_a_string(10, 20, warehouseId));
        city = new String(JBButil.create_random_a_string(10, 20, warehouseId));
        state = new String(JBButil.create_random_a_string(2, 2, warehouseId));
        zip = new String(JBButil.create_random_n_string(9, 9, warehouseId));
    }

    public synchronized void display() {
        System.out.println("****** ADDRESS display *********************");
        System.out.println("     Street 1 is  " + street1);
        System.out.println("     Street 2 is  " + street2);
        System.out.println("    City name is  " + city);
        System.out.println("   State name is  " + state);
        System.out.println("     Zip code is  " + zip);
        System.out.println("********************************************");
    }

    public synchronized String getStreet1() {
        String temp = street1;
        return temp;
    }

    public synchronized String getStreet2() {
        String temp = street2;
        return temp;
    }

    public synchronized String getCity() {
        String temp = city;
        return temp;
    }

    public synchronized String getState() {
        String temp = state;
        return temp;
    }

    public synchronized String getZip() {
        String temp = zip;
        return temp;
    }
}
