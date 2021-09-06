/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MapDataStorage implements JBBDataStorage {
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

    protected Map             data;

    protected MapDataStorage() {
        this(new HashMap());
    };

    protected MapDataStorage(Map data) {
        this.data = data;
    }

    public Iterator elements() {
        return data.values().iterator();
    }

    public boolean containsKey(Object key) {
        return data.containsKey(key);
    }

    public Object get(Object key) {
        return data.get(key);
    }

    public Object put(Object key, Object value) {
        return data.put(key, value);
    }

    public Object remove(Object key) {
        return data.remove(key);
    }

    public int size() {
        return data.size();
    }
}
