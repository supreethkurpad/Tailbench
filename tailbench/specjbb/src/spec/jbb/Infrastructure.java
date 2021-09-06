/* Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. 
 */
package spec.jbb;

import java.util.Iterator;

class Infrastructure {
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "(C) Copyright IBM Corp., 1996 - 2005"
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    private static class SynchronizedJBBDataStorage implements JBBDataStorage {
        JBBDataStorage s;

        SynchronizedJBBDataStorage(JBBDataStorage s) {
            this.s = s;
        }

        public boolean containsKey(Object o) {
            synchronized (s) {
                return s.containsKey(o);
            }
        }

        public Object get(Object o) {
            synchronized (s) {
                return s.get(o);
            }
        }

        public Object put(Object key, Object value) {
            synchronized (s) {
                return s.put(key, value);
            }
        }

        public Object remove(Object o) {
            synchronized (s) {
                return s.remove(o);
            }
        }

        public int size() {
            synchronized (s) {
                return s.size();
            }
        }

        public Iterator elements() {
            synchronized (s) {
                return s.elements();
            }
        }
    }

    private static class SynchronizedJBBSortedStorage extends
            SynchronizedJBBDataStorage implements JBBSortedStorage {
        SynchronizedJBBSortedStorage(JBBSortedStorage s) {
            super(s);
        }

        public Object removeFirstElem() {
            synchronized (s) {
                return ((JBBSortedStorage) s).removeFirstElem();
            }
        }

        public boolean deleteFirstEntities(int quant) {
            synchronized (s) {
                return ((JBBSortedStorage) s).deleteFirstEntities(quant);
            }
        }

        public boolean deleteFirstEntities() {
            synchronized (s) {
                return ((JBBSortedStorage) s).deleteFirstEntities();
            }
        }

        public Object getMedianValue(Object firstKey, Object lastKey) {
            synchronized (s) {
                return ((JBBSortedStorage) s).getMedianValue(firstKey, lastKey);
            }
        }
    }

    private static JBBDataStorage synchStorage(JBBDataStorage s) {
        return new SynchronizedJBBDataStorage(s);
    }

    public static JBBDataStorage createStorage() {
        return new MapDataStorage();
    }

    public static JBBDataStorage createStorage(boolean synch) {
        JBBDataStorage s = createStorage();
        if (synch) {
            s = synchStorage(s);
        }
        return s;
    }

    public static JBBSortedStorage createSortedStorage() {
        return new TreeMapDataStorage();
    }
}
