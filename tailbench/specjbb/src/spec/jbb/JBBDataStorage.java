/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.util.Iterator;

public interface JBBDataStorage {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "(C) Copyright IBM Corp., 1996 - 2005"
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    /**
     * Returns true if JBBDataStorage contains a value for specified key
     */
    boolean containsKey(Object key);

    /**
     * Returns the value for specified key
     */
    Object get(Object key);

    /**
     * Associates the specified value with specified key
     */
    Object put(Object key, Object value);

    /**
     * Removes the value for the specified key
     */
    Object remove(Object key);

    /**
     * Returns the number of key-value pairs in the JBBDataStorage
     */
    int size();

    /**
     * Returns an iterator over the elements in this JBBDataStorage
     */
    Iterator elements();
}
