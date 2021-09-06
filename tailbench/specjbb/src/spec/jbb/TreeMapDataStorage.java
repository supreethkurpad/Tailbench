/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Iterator;

public class TreeMapDataStorage extends MapDataStorage implements
        JBBSortedStorage {
    /**
     * serialversionUID of 1 for first release
     */
    private static final long serialVersionUID = 1L;

    // This goes right after each class/interface statement
    static final String       COPYRIGHT        = "SPECjbb2005,"
                                                       + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                                       + "All rights reserved,"
                                                       + "(C) Copyright IBM Corp., 1996 - 2005"
                                                       + "All rights reserved,"
                                                       + "Licensed Materials - Property of SPEC";

    TreeMapDataStorage() {
        super(new TreeMap());
    };

    public boolean deleteFirstEntities(int quant) {
        Iterator iter = data.values().iterator();
        for (int i = 1; i <= quant; i++) {
            if (iter.hasNext()) {
                iter.next();
                iter.remove();
            }
            else {
                return false;
            }
        }
        return true;
    }

    public boolean deleteFirstEntities() {
        return deleteFirstEntities(1);
    }

    public Object removeFirstElem() {
        int size = this.size();
        if (size > 0) {
            return remove(((SortedMap) data).firstKey());
        }
        else
            return null;
    }

    public Object getMedianValue(Object firstKey, Object lastKey) {
        Object avgValue = null;
        SortedMap subMap = ((SortedMap) data).subMap(firstKey, lastKey);
        int size = subMap.size();
        Iterator iter = subMap.values().iterator();
        for (int i = 1; i <= (size + 1) / 2; i++) {
            avgValue = iter.next();
        }
        return avgValue;
    }
}
