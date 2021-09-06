/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

public interface JBBSortedStorage extends JBBDataStorage {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "(C) Copyright IBM Corp., 1996 - 2005"
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    /**
     * Removes the first element from JBBDataStorage
     */
    Object removeFirstElem();

    /**
     * Removes the first "quant" elements from JBBDataStorage and deletes an
     * Entities associated with their values
     */
    boolean deleteFirstEntities(int quant);

    /**
     * Removes the first element from JBBDataStorage and deletes an Entity
     * associated with its value
     */
    boolean deleteFirstEntities();

    /**
     * Returns the median element of the sub-set of JBBDataStorage
     */
    public Object getMedianValue(Object firstKey, Object lastKey);
}
