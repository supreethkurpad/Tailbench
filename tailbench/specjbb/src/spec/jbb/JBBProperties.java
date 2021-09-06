/*
 * 
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1996-2005 IBM Corporation, Inc. All rights
 * reserved.
 */
package spec.jbb;

import java.util.*;
import java.util.logging.Level;
import java.io.*;

public class JBBProperties {
    // This goes right after each class/interface statement
    static final String          COPYRIGHT                                 = "SPECjbb2005,"
                                                                                   + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                                                                   + "All rights reserved,"
                                                                                   + "(C) Copyright IBM Corp., 1996 - 2005"
                                                                                   + "All rights reserved,"
                                                                                   + "Licensed Materials - Property of SPEC";

    private Properties           PropertiesForBatch;

    String                       val;

    public int                   warehousePopulationBase;

    public int                   orderlinesPerOrder;

    public int                   rampupSeconds;

    public int                   measurementSeconds;

    public int                   expectedPeakWarehouse                     = 4;                                              // defaults

    // to 4
    public boolean               deterministicRandomSeed                   = false;

    public int                   jvm_instances                             = 1;

    public float                 per_jvm_warehouse_rampup                  = 0f;

    public float                 per_jvm_warehouse_rampdown                = 0f;

    public int                   waitTimePercent;

    public boolean               showWarehouseDetail                       = false;

    public int                   startingNumberWarehouses;

    public int                   incrementNumberWarehouses;

    public int                   endingNumberWarehouses;

    public int[]                 sequenceOfWarehouses;

    public boolean               steadyStateFlag;

    public boolean               screenWriteFlag;

    public boolean               checkThroughput;

    public float                 minBTPSRatio;

    public static int            overrideItemTableSize                     = 20000;

    public static boolean        uniformRandomItems                        = true;

    public static boolean        printPropertiesAndArgs                    = true;

    private Level                applicationLoggingLevel                   = Level.ALL;

    private static final float   COMPLIANT_RATE_per_jvm_warehouse_rampup   = 3.0f;

    private static final float   COMPLIANT_RATE_per_jvm_warehouse_rampdown = 20.0f;

    private static final int     COMPLIANT_RATE_warehousePopulationBase    = 60;

    private static final int     COMPLIANT_RATE_orderlinesPerOrder         = 10;

    private static final int     COMPLIANT_RATE_rampupSeconds              = 30;

    private static final int     COMPLIANT_RATE_measurementSeconds         = 240;

    private static final int     COMPLIANT_RATE_waitTimePercent            = 0;

    private static final boolean COMPLIANT_RATE_steadyStateFlag            = true;

    private static final boolean COMPLIANT_RATE_screenWriteFlag            = false;

    private static final boolean COMPLIANT_RATE_uniformRandomItems         = true;

    private static final int     COMPLIANT_RATE_overrideItemTableSize      = 20000;

    private static final boolean COMPLIANT_RATE_deterministicRandomSeed    = false;

    private static final Level   COMPLIANT_RATE_applicationLoggingLevel    = Level.INFO;

    public JBBProperties(String propertiesFileName) {
        System.out.println("");
        System.out.println("Loading properties from " + propertiesFileName);
        try {
            FileInputStream PropertiesFile = new FileInputStream(
                    propertiesFileName);
            PropertiesForBatch = new Properties();
            PropertiesForBatch.load(PropertiesFile);
        }
        catch (IOException e) {
            JBButil.getLog().log(Level.WARNING,
                    "ERROR:  Properties File error; please start again", e);
            return;
        }
        val = getOptionalProperty("input.include_file");
        if (val != null) {
            try {
                FileInputStream IncludedPropertiesFile = new FileInputStream(
                        val);
                PropertiesForBatch.load(IncludedPropertiesFile);
            }
            catch (IOException e) {
                JBButil
                        .getLog()
                        .log(
                                Level.WARNING,
                                "ERROR:  Properties File error with included properties file; please start again",
                                e);
                return;
            }
        }
    }

    public String getRequiredProperty(String s) {
        try {
            return (PropertiesForBatch.getProperty(s).trim());
        }
        catch (NullPointerException e) {
            JBButil.getLog().log(
                    Level.WARNING,
                    "ERROR:  Property " + s
                            + " not specified; please add to Properties File",
                    e);
            return (null);
        }
    }

    public void setRequiredProperty(String property, String value) {
        try {
            // return (PropertiesForBatch.getProperty(s).trim());
            PropertiesForBatch.put(property, value);
        }
        catch (NullPointerException e) {
            JBButil.getLog()
                    .log(Level.WARNING, "ERROR: setting " + property, e);
        }
    }

    public String getOptionalProperty(String s) {
        try {
            return (PropertiesForBatch.getProperty(s).trim());
        }
        catch (NullPointerException e) {
            return (null);
        }
    }

    public boolean getProps() {
        String prefix = "input.";
        String val;
        int tmpValue = 0;
        boolean retval = true;
        boolean valid_warehouse_sequence = false;
        sequenceOfWarehouses = null;
        val = getRequiredProperty(prefix + "suite");
        if (val != null) {
            if (val.equals("SPECjbb")) {
                checkThroughput = false;
            }
            else {
                System.out.println("ERROR:  Property file error");
                System.out.println("   suite must be SPECjbb");
                retval = false;
            }
        }
        else {
            retval = false;
        }
        val = getRequiredProperty(prefix + "log_level");
        if (val != null) {
            try {
                applicationLoggingLevel = Level.parse(val);
            }
            catch (IllegalArgumentException iae) {
                // The logging level specified is not something that can
                // be properly mapped by the Level class. Force the user
                // to correct this mistake.
                retval = false;
            }
        }
        else {
            retval = false;
        }
        if (checkThroughput) { // Capacity needs criterion for minimum BTPS
            val = getRequiredProperty(prefix + "min_capacity_ratio");
            if (val != null) {
                minBTPSRatio = Float.valueOf(val).floatValue();
            }
            else {
                retval = false;
            }
        }
        val = getOptionalProperty(prefix + "warehouse_population");
        if (val != null) {
            warehousePopulationBase = Integer.parseInt(val);
        }
        else {
            warehousePopulationBase = 60;
            PropertiesForBatch.put(prefix + "warehouse_population", "60");
        }
        val = getOptionalProperty(prefix + "orderlines_per_order");
        if (val != null) {
            orderlinesPerOrder = Integer.parseInt(val);
        }
        else {
            orderlinesPerOrder = 10;
            PropertiesForBatch.put(prefix + "orderlines_per_order", "10");
        }
        val = getRequiredProperty(prefix + "ramp_up_seconds");
        if (val != null) {
            rampupSeconds = Integer.parseInt(val);
        }
        else {
            retval = false;
        }
        val = getRequiredProperty(prefix + "jvm_instances");
        if (val != null) {
            jvm_instances = Integer.parseInt(val);
        }
        else {
            retval = false;
        }
        val = getRequiredProperty(prefix + "per_jvm_warehouse_rampup");
        if (val != null) {
            per_jvm_warehouse_rampup = Float.parseFloat(val);
        }
        else {
            retval = false;
        }
        val = getRequiredProperty(prefix + "per_jvm_warehouse_rampdown");
        if (val != null) {
            per_jvm_warehouse_rampdown = Float.parseFloat(val);
        }
        else {
            retval = false;
        }
        val = getRequiredProperty(prefix + "measurement_seconds");
        if (val != null) {
            measurementSeconds = Integer.parseInt(val);
        }
        else {
            retval = false;
        }
        val = getOptionalProperty(prefix + "expected_peak_warehouse");
        if (val != null) {
            tmpValue = Integer.parseInt(val);
            if (tmpValue == 0) {
                tmpValue = Runtime.getRuntime().availableProcessors();
                if (jvm_instances > 1 && tmpValue > 1)
                    expectedPeakWarehouse = Runtime.getRuntime()
                            .availableProcessors()
                            / jvm_instances;
                else
                    expectedPeakWarehouse = Runtime.getRuntime()
                            .availableProcessors();
            }
            else{
                if(tmpValue == Runtime.getRuntime().availableProcessors()){
                    System.out.println("Warning: Explicitly setting " + prefix + "expected_peak_warehouse");
                    System.out.println("requires submission and review by SPEC in order to publish the result.");
                }
                expectedPeakWarehouse = tmpValue;
            }
        }
        else {
            tmpValue = Runtime.getRuntime().availableProcessors();
            if (jvm_instances > 1 && tmpValue > 1)
                expectedPeakWarehouse = Runtime.getRuntime()
                        .availableProcessors()
                        / jvm_instances;
            else
                expectedPeakWarehouse = Runtime.getRuntime()
                        .availableProcessors();
        }
        PropertiesForBatch.put(prefix + "expected_peak_warehouse", String
                .valueOf(expectedPeakWarehouse));
        val = getRequiredProperty(prefix + "deterministic_random_seed");
        if (val != null) {
            deterministicRandomSeed = Boolean.parseBoolean(val);
        }
        else {
            retval = false;
        }
        val = getOptionalProperty(prefix + "wait_time_percent");
        if (val != null) {
            waitTimePercent = Integer.parseInt(val);
        }
        else {
            waitTimePercent = 0;
            PropertiesForBatch.put(prefix + "wait_time_percent", "0");
        }
        val = getOptionalProperty(prefix + "screen_write");
        if (val != null) {
            if (val.equals("true")) {
                screenWriteFlag = true;
            }
            else if (val.equals("false")) {
                screenWriteFlag = false;
            }
            else {
                System.out.println("ERROR:  Property file error");
                System.out.println("   screen_write must be 'true' or 'false'");
                retval = false;
            }
        }
        else {
            screenWriteFlag = false;
            PropertiesForBatch.put(prefix + "screen_write", "false");
        }
        val = getOptionalProperty(prefix + "steady_state");
        if (val != null) {
            if (val.equals("true")) {
                steadyStateFlag = true;
            }
            else if (val.equals("false")) {
                steadyStateFlag = false;
            }
            else {
                System.out.println("ERROR:  Property file error");
                System.out.println("   steady_state must be 'true' or 'false'");
                retval = false;
            }
        }
        else {
            steadyStateFlag = true;
            PropertiesForBatch.put(prefix + "steady_state", "true");
        }
        val = getOptionalProperty(prefix + "override_itemtable_size");
        if (val != null) {
            overrideItemTableSize = Integer.parseInt(val);
        }
        val = getOptionalProperty(prefix + "uniform_random_items");
        if (val != null) {
            if (val.equals("true")) {
                uniformRandomItems = true;
            }
            else if (val.equals("false")) {
                uniformRandomItems = false;
            }
            else {
                System.out.println("ERROR:  Property file error");
                System.out
                        .println("   uniform_random_items must be true or false");
                retval = false;
            }
        }
        val = getOptionalProperty(prefix + "starting_number_warehouses");
        if (val != null) {
            startingNumberWarehouses = Integer.parseInt(val);
        }
        val = getOptionalProperty(prefix + "increment_number_warehouses");
        if (val != null) {
            incrementNumberWarehouses = Integer.parseInt(val);
        }
        val = getOptionalProperty(prefix + "ending_number_warehouses");
        if (val != null) {
            endingNumberWarehouses = Integer.parseInt(val);
        }
        else {
            endingNumberWarehouses = expectedPeakWarehouse * 2;
            if (endingNumberWarehouses < 8)
                endingNumberWarehouses = 8;
            PropertiesForBatch.put(prefix + "ending_number_warehouses", String
                    .valueOf(endingNumberWarehouses));
        }
        if ((startingNumberWarehouses > 0)
                && (endingNumberWarehouses >= startingNumberWarehouses)
                && (incrementNumberWarehouses > 0)) {
            valid_warehouse_sequence = true;
        }
        val = getOptionalProperty(prefix + "sequence_of_number_of_warehouses");
        if (val != null) {
            if (valid_warehouse_sequence) {
                System.out.println("ERROR:  Property file error");
                System.out
                        .println("   Cannot specify both sequence_of_number_of_warehouses and {starting,ending,increment}_number_warehouses");
                retval = false;
            }
            else {
                int startIndex = 0;
                int nextSpace;
                int numEntries = 0;
                while ((nextSpace = val.indexOf(' ', startIndex)) > 0) {
                    numEntries++;
                    startIndex = nextSpace + 1;
                }
                numEntries++; // The one at the end, with no space after it.
                sequenceOfWarehouses = new int[numEntries];
                startIndex = 0;
                numEntries = 0;
                while ((nextSpace = val.indexOf(' ', startIndex)) > 0) {
                    sequenceOfWarehouses[numEntries] = Integer.parseInt(val
                            .substring(startIndex, nextSpace));
                    numEntries++;
                    startIndex = nextSpace + 1;
                }
                sequenceOfWarehouses[numEntries] = Integer.parseInt(val
                        .substring(startIndex, val.length()));
                numEntries++;
                valid_warehouse_sequence = true;
                for (int i = 1; i < numEntries; i++) {
                    if (sequenceOfWarehouses[i] < sequenceOfWarehouses[i - 1]) {
                        valid_warehouse_sequence = false;
                        break;
                    }
                }
            }
        }
        val = getOptionalProperty(prefix + "print_properties_and_args");
        if (val != null) {
            if (val.equals("true")) {
                printPropertiesAndArgs = true;
            }
            else if (val.equals("false")) {
                printPropertiesAndArgs = false;
            }
            else {
                System.out.println("ERROR:  Property file error");
                System.out
                        .println("   print_properties_and_args must be true or false");
                retval = false;
            }
        }
        System.out.println("\nInput Properties:");
        System.out.println("  per_jvm_warehouse_rampup = "
                + per_jvm_warehouse_rampup);
        System.out.println("  per_jvm_warehouse_rampdown = "
                + per_jvm_warehouse_rampdown);
        System.out.println("  jvm_instances = " + jvm_instances);
        System.out.println("  deterministic_random_seed = "
                + deterministicRandomSeed);
        System.out.println("  ramp_up_seconds = " + rampupSeconds);
        System.out.println("  measurement_seconds = " + measurementSeconds);
        // System.out.println("wait_time_percent = " + waitTimePercent);
        if (checkThroughput) {
            // System.out.println("min_capacity_ratio = " + minBTPSRatio);
        }
        // System.out.println(" forcegc = " + forceGC);
        // System.out.println("steady_state = " + steadyStateFlag);
        // System.out.println("screen_write = " + screenWriteFlag);
        if (sequenceOfWarehouses == null) {
            System.out.println("  starting_number_warehouses = "
                    + startingNumberWarehouses);
            System.out.println("  increment_number_warehouses = "
                    + incrementNumberWarehouses);
            System.out.println("  ending_number_warehouses = "
                    + endingNumberWarehouses);
            System.out.println("  expected_peak_warehouse = "
                    + expectedPeakWarehouse);
        }
        else {
            System.out.print("  sequence_of_number_of_warehouses = ");
            for (int i = 0; i < sequenceOfWarehouses.length; i++) {
                System.out.print(" " + sequenceOfWarehouses[i]);
            }
            System.out.println("  expected_peak_warehouse = "
                    + expectedPeakWarehouse);
            System.out.println("");
        }
        if (!valid_warehouse_sequence) {
            System.out.println("ERROR:  Property file error");
            System.out.println("   No valid warehouse sequence specified.");
        }
        if (overrideItemTableSize > 0) {
            // System.out.println("Item table size overridden to: " +
            // overrideItemTableSize);
        }
        // System.out.println("uniform_random_items = " + uniformRandomItems);
        // System.out.println("print_properties_and_args = " +
        // printPropertiesAndArgs);
        return retval && valid_warehouse_sequence;
    }

    public boolean copyPropsToOutput(PrintStream outRawFile) {
        boolean retval = true;
        String[] input_props = {
                "suite", "log_level", "warehouse_population",
                "orderlines_per_order", "ramp_up_seconds",
                "measurement_seconds", "wait_time_percent", "screen_write",
                "steady_state", "starting_number_warehouses",
                "increment_number_warehouses", "ending_number_warehouses",
                "sequence_of_number_of_warehouses", "include_file",
                "override_itemtable_size", "uniform_random_items",
                "print_properties_and_args", "output_directory",
                "expected_peak_warehouse", "deterministic_random_seed",
                "jvm_instances", "per_jvm_warehouse_rampup",
                "per_jvm_warehouse_rampdown"
        };
        int num_props = 0;
        for (Enumeration e = PropertiesForBatch.propertyNames(); e
                .hasMoreElements();) {
            String name = (String) e.nextElement();
            if (name.startsWith("config.")) {
                num_props++;
            }
            else if (name.startsWith("input.")) {
                boolean found = false;
                for (int i = 0; i < input_props.length; i++) {
                    if (name.equals("input." + input_props[i])) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    num_props++;
                }
                else if (name.equals("input.min_capacity_ratio")) {
                    if (checkThroughput) {
                        num_props++;
                    }
                    else {
                        System.out.println("ERROR:  Property file error");
                        System.out
                                .println("   input.min_capacity_ratio invalid for input.suite=SPECjbb");
                        retval = false;
                    }
                }
                else if (name.equals("input.show_warehouse_detail")) {
                    if (PropertiesForBatch.getProperty(
                            "input.show_warehouse_detail").equalsIgnoreCase(
                            "true"))
                        showWarehouseDetail = true;
                    num_props++;
                }
                else {
                    System.out.println("ERROR:  Property file error");
                    System.out.println("   Unrecognized property:  " + name);
                    retval = false;
                }
            }
            else {
                // Set return value to false, but keep processing so user can
                // correct all spelling errors in a single pass.
                System.out.println("ERROR:  Property file error");
                System.out.println("   Unrecognized property:  " + name);
                retval = false;
            }
        }
        if (retval) {
            Vector keyvec = new Vector(PropertiesForBatch.keySet());
            Collections.sort(keyvec);
            for (int i = 0; i < keyvec.size(); i++) {
                String propsKey = (String) keyvec.elementAt(i);
                String svalue = PropertiesForBatch.getProperty(propsKey);
                outRawFile.println(propsKey + "=" + svalue);
            }
            Integer procsAvail = new Integer(Runtime.getRuntime()
                    .availableProcessors());
            outRawFile.println("config.sw.procsAvailtoJava" + "="
                    + procsAvail.toString());
        }
        return retval;
    }

    public String setProp(String prop, String value) {
        PropertiesForBatch.put(prop, value);
        return PropertiesForBatch.getProperty(prop);
    }

    private boolean checkCompliance(int actualValue, int compliantValue,
            String name) {
        // System.out.println(name + " " + actualValue + " " + compliantValue);
        if (actualValue != compliantValue) {
            System.out.println("INVALID:  " + name + " = " + actualValue
                    + ", must be " + compliantValue);
            return false;
        }
        return true;
    }

    private boolean checkCompliance(boolean actualValue,
            boolean compliantValue, String name) {
        // System.out.println(name + " " + actualValue + " " + compliantValue);
        if (actualValue != compliantValue) {
            System.out.println("INVALID:  " + name + " = " + actualValue
                    + ", must be " + compliantValue);
            return false;
        }
        return true;
    }

    private boolean checkCompliance(float actualValue, float compliantValue,
            String name) {
        // System.out.println(name + " " + actualValue + " " + compliantValue);
        if (actualValue != compliantValue) {
            System.out.println("INVALID:  " + name + " = " + actualValue
                    + ", must be " + compliantValue);
            return false;
        }
        return true;
    }

    private boolean checkCompliance(Level actualValue, Level compliantValue,
            String name) {
        // System.out.println(name + " " + actualValue + " " + compliantValue);
        if (actualValue.equals(compliantValue) == false) {
            System.out.println("INVALID:  " + name + " = " + actualValue
                    + ", must be " + compliantValue);
            return false;
        }
        return true;
    }

    public void checkCompliance() {
        boolean compliant = true;
        System.out.println("\n\nChecking whether run will be valid");
        if (checkThroughput) { // Capacity
        }
        else { // Rate
            compliant = checkCompliance(warehousePopulationBase,
                    COMPLIANT_RATE_warehousePopulationBase,
                    "warehouse_population_base")
                    && compliant;
            compliant = checkCompliance(orderlinesPerOrder,
                    COMPLIANT_RATE_orderlinesPerOrder, "orderlines_per_order")
                    && compliant;
            compliant = checkCompliance(deterministicRandomSeed,
                    COMPLIANT_RATE_deterministicRandomSeed,
                    "deterministic_random_seed")
                    && compliant;
            compliant = checkCompliance(per_jvm_warehouse_rampup,
                    COMPLIANT_RATE_per_jvm_warehouse_rampup,
                    "per_jvm_warehouse_rampup")
                    && compliant;
            compliant = checkCompliance(per_jvm_warehouse_rampdown,
                    COMPLIANT_RATE_per_jvm_warehouse_rampdown,
                    "per_jvm_warehouse_rampdown")
                    && compliant;
            compliant = checkCompliance(rampupSeconds,
                    COMPLIANT_RATE_rampupSeconds, "ramp_up_seconds")
                    && compliant;
            compliant = checkCompliance(measurementSeconds,
                    COMPLIANT_RATE_measurementSeconds, "measurement_seconds")
                    && compliant;
            compliant = checkCompliance(waitTimePercent,
                    COMPLIANT_RATE_waitTimePercent, "wait_time_percent")
                    && compliant;
            compliant = checkCompliance(steadyStateFlag,
                    COMPLIANT_RATE_steadyStateFlag, "steady_state")
                    && compliant;
            compliant = checkCompliance(screenWriteFlag,
                    COMPLIANT_RATE_screenWriteFlag, "screen_write")
                    && compliant;
            compliant = checkCompliance(uniformRandomItems,
                    COMPLIANT_RATE_uniformRandomItems, "uniform_random_items")
                    && compliant;
            compliant = checkCompliance(applicationLoggingLevel,
                    COMPLIANT_RATE_applicationLoggingLevel, "log_level")
                    && compliant;
            if (overrideItemTableSize > 0) {
                compliant = checkCompliance(overrideItemTableSize,
                        COMPLIANT_RATE_overrideItemTableSize,
                        "override_itemtable_size")
                        && compliant;
            }
        }
        if (compliant) {
            System.out.println();
            System.out.println("Run will be COMPLIANT");
            System.out.println();
        }
        else {
            System.out.println();
            System.out.println("INVALID:  Run will NOT be compliant");
            System.out.println();
        }
    }

    public Level getApplicationLoggingLevel() {
        return applicationLoggingLevel;
    }
}
