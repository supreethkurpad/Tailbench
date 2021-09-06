/*
 *
 * Copyright (c) 20002005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 1997-2005 Sun Microsystems, Inc. All rights
 * reserved.
 */
/*
 * Complain about Java errors that ought to be caught as exceptions by the JVM.
 * *NOT* an exhaustive conformance test. This is just intended to catch some
 * common errors and omissions for the convenience of the benchmarker in
 * avoiding some run rule mistakes. This needs to be expanded to test more.
 *
 * The timing of this "benchmark" is ignored in metric calculation. It is here
 * only in order to pass or fail output verification.
 */
package spec.jbb.validity;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.*;

public class Check {
    // This goes right after each class/interface statement
    static final String COPYRIGHT = "SPECjbb2005,"
                                          + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                          + "All rights reserved,"
                                          + "Copyright (c) 1997-2005 Sun Microsystems, Inc."
                                          + "All rights reserved,"
                                          + "Licensed Materials - Property of SPEC";

    public static boolean doCheck() {
        System.out.println("\nChecking JVM\n");
        boolean caughtIndex = false;
        boolean gotToFinally = false;
        boolean error = false;
        try {
            int[] a = new int[10];
            for (int i = 0; i <= 10; i++)
                a[i] = i;
            System.out.println("Error: array bounds not checked");
            error = true;
        }
        catch (ArrayIndexOutOfBoundsException e) {
            caughtIndex = true;
        }
        finally {
            gotToFinally = true;
        }
        if (!caughtIndex) {
            System.out
                    .println("1st bounds test error:\tindex exception not received");
            error = true;
        }
        if (!gotToFinally) {
            System.out
                    .println("1st bounds test error:\tfinally clause not executed");
            error = true;
        }
        if (caughtIndex && gotToFinally)
            System.out.println("1st bounds test:\tOK");
        if (checkSubclassing())
            error = true;
        if (checkXMLErrorChecking())
            error = true;
        LoopBounds mule = new LoopBounds();
        LoopBounds.run();
        if (LoopBounds.gotError) {
            System.out.println("2nd bounds test:\tfailed");
            error = true;
        }
        else {
            System.out.println("2nd bounds test:\tOK");
        }
        PepTest horse = new PepTest();
        horse.instanceMain();
        if (horse.gotError)
            error = true;
        if (error)
            System.out.println("\nINVALID: JVM Check detected error(s)");
        else
            System.out.println("\nJVM Check OK");
        return error;
    }

    private static boolean checkSubclassing() {
        boolean error = false;
        Super sup = new Super(3);
        Sub sub = new Sub(3);
        System.out.println(sup.getName() + ": " + sup.toString());
        System.out.println(sub.getName() + ": " + sub.toString());
        if (!sup.toString().equals(
                "Class Super, public=34, protected=33, private=32"))
            error = true;
        if (!sub.toString().equals(
                "Class Super, public=804, protected=803, private=802"))
            error = true;
        System.out.println("Super: prot=" + sup.getProtected() + ", priv="
                + sup.getPrivate());
        System.out.println("Sub:  prot=" + sub.getProtected() + ", priv="
                + sub.getPrivate());
        if (sup.getProtected() != 33 || sup.getPrivate() != 32)
            error = true;
        if (sub.getProtected() != 111 || sub.getPrivate() != 105)
            error = true;
        System.out.println("Subclass test " + (error ? "error" : "OK"));
        return error;
    }

    private static boolean checkXMLErrorChecking() {
        // initialize document
        boolean error = false;
        Document document;
        DocumentBuilder builder;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            builder = factory.newDocumentBuilder();
            document = builder.newDocument();
            boolean result = false;
            result = document.getStrictErrorChecking(); // Comment this line out
            // if compiling with a
            // 1.4 JDK.
            // strictErrorChecking must be true.
            if (result)
                error = false;
            else
                error = true;
            System.out.println("XML StrictErrorChecking test: "
                    + (error ? "error" : "OK"));
        }
        catch (ParserConfigurationException pce) {
            // Parser with specified options can't be built
            pce.printStackTrace();
        }
        return error;
    }
}// end class
