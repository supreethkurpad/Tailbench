/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 2000-2005 Hewlett-Packard All rights reserved.
 * Copyright (c) 1997-2005 Sun Microsystems, Inc. All rights reserved.
 * 
 * This source code is provided as is, without any express or implied warranty.
 *  
 */
package spec.reporter;

import java.io.*;
import java.util.Properties;

public class ReportProps extends Properties {
    /**
     * serialversionUID = 1 for first release
     */
    private static final long serialVersionUID = 1L;

    // This goes right after each class/interface statement
    static final String       COPYRIGHT        = "SPECjbb2005,"
                                                       + "Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC),"
                                                       + "All rights reserved,"
                                                       + "Copyright (c) 2000-2005 Hewlett-Packard,"
                                                       + "All rights reserved,"
                                                       + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                                       + "All rights reserved,"
                                                       + "Licensed Materials - Property of SPEC";

    public String get(String key) {
        return super.getProperty(key);
    }

    public String get(String key, String def) {
        String s = super.getProperty(key);
        if (s == null)
            return def;
        return s;
    }

    public double getDouble(Object key) {
        return getDouble(key, 0);
    }

    public double getDouble(Object key, double def) {
        try {
            String s = (String) super.get(key);
            if (s != null)
                return Double.valueOf(s).doubleValue();
        }
        catch (Exception e) {
        }
        return def;
    }

    public int getInt(Object key) {
        return getInt(key, 0);
    }

    public int getInt(Object key, int def) {
        try {
            String s = (String) super.get(key);
            if (s != null)
                return Integer.parseInt(s.trim());
        }
        catch (Exception e) {
        }
        return def;
    }

    public long getLong(Object key) {
        return getLong(key, 0);
    }

    public long getLong(Object key, int def) {
        try {
            String s = (String) super.get(key);
            if (s != null)
                return Long.parseLong(s);
        }
        catch (Exception e) {
        }
        return def;
    }

    public String getString(Object key) {
        return (String) super.get(key);
    }

    public void load(String name) throws IOException {
        load(new FileReader(name));
    }

    public void load(Reader readstream) throws IOException {
        BufferedReader in = new BufferedReader(readstream);
        String line;
        while ((line = in.readLine()) != null) {
            if (line.startsWith("#"))
                continue;
            int i = line.indexOf('=');
            if (i < 0)
                continue;
            String key = line.substring(0, i);
            String value = line.substring(i + 1);
            put(key, value);
        }
    }
}
