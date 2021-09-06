/*
 *
 * Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC) All
 * rights reserved. Copyright (c) 2005 Sun Microsystems, Inc. All rights
 * reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 */
package spec.jbb.infra.Util;

import java.io.FileOutputStream;
import java.io.FileDescriptor;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import java.io.IOException;
import org.w3c.dom.*;

class XMLLineDocumentException extends RuntimeException {
    /**
     * serialversionUID = 1 for first release
     */
    private static final long serialVersionUID = 1L;

    // This goes right after each class/interface statement
    static final String       COPYRIGHT        = "SPECjbb2005,"
                                                       + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                                       + "All rights reserved,"
                                                       + "(C) Copyright Sun Microsystems, Inc. 2005"
                                                       + "All rights reserved,"
                                                       + "Licensed Materials - Property of SPEC";

    XMLLineDocumentException() {
        super();
    };

    XMLLineDocumentException(String s) {
        super(s);
    };
};

public class XMLTransactionLog {
    // This goes right after each class/interface statement
    static final String     COPYRIGHT = "SPECjbb2005,"
                                              + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                              + "All rights reserved,"
                                              + "(C) Copyright Sun Microsystems, Inc."
                                              + "All rights reserved,"
                                              + "Licensed Materials - Property of SPEC";

    static FileOutputStream fout      = new FileOutputStream(FileDescriptor.out);

    static Document         templateDocument;

    static DocumentBuilder  builder;
    static {
        // initialize document
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            builder = factory.newDocumentBuilder();
            templateDocument = builder.parse("xml/template-document.xml");
        }
        catch (SAXException sxe) {
            // Error generated during parsing)
            System.err
                    .println("SAX Error in template-document initialization.");
            Exception ex = sxe;
            if (sxe.getException() != null)
                ex = sxe.getException();
            ex.printStackTrace();
            System.exit(1);
        }
        catch (ParserConfigurationException pce) {
            // Parser with specified options can't be built
            System.err
                    .println("Parser configuration error in template-document initialization.");
            pce.printStackTrace();
            System.exit(1);
        }
        catch (IOException ioe) {
            // I/O error
            System.err
                    .println("I/O error in template-document initialization.");
            ioe.printStackTrace();
            System.exit(1);
        }
    }

    private ArrayList<Node> lineCache = null;

    Document                document;

    public XMLTransactionLog() {
        // create new line cache
        lineCache = new ArrayList<Node>(0);
        // copy from template XML document
        copy(templateDocument);
    }

    public void populateXML(TransactionLogBuffer log_buffer) {
        for (int i = 0; i < log_buffer.getLineCount(); i++) {
            putLine(log_buffer.getLine(i), i);
        }
    }

    public void clear() {
        Element baseElement = document.getDocumentElement();
        Node current_node = baseElement.getLastChild();
        Node next_node = null;
        while ((next_node = current_node.getPreviousSibling()) != null) {
            Node lineNode = baseElement.removeChild(current_node);
            if (lineNode.getNodeName().equals("Line")) {
                // set the removed line's LineData Text Value to ""
                lineNode.getLastChild().getLastChild().setNodeValue("");
                // add the removed line to the lineCache
                lineCache.add(lineNode);
            }
            current_node = next_node;
        }
    };

    public void copy(Document master) {
        // copy the document
        document = (Document) master.cloneNode(true);
    }

    private final void putLine(String s, int n) {
        int line_number = n;
        Node jbbDocument = document.getLastChild();
        // Check and see if a line element is available
        // in the line cache
        int cacheLength = lineCache.size();
        if (cacheLength > 0) {
            // fetch a line from the line cache
            Node lineNode = lineCache.remove(cacheLength - 1);
            jbbDocument.appendChild(lineNode);
            // set the TextNode of the LineData child from lineNode
            lineNode.getLastChild().getLastChild().setNodeValue(s);
        }
        else {
            // Create a new line element and append it to the document
            Element lineNode = (Element) document.createElement("Line");
            jbbDocument.appendChild(lineNode);
            Element newData = (Element) document.createElement("LineData");
            lineNode.appendChild(newData);
            Node new_node = document.createTextNode(s);
            newData.appendChild(new_node);
        }
    }
}
