/*
 *
 * Copyright (c) 2000-2005 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 * Copyright (c) 1996-2005 IBM Corporation, Inc. All rights reserved.
 *
 */
package spec.jbb;

import spec.jbb.infra.Util.TransactionLogBuffer;
import spec.jbb.infra.Util.XMLTransactionLog;
import java.math.BigDecimal;
import java.util.Date;

class PaymentTransaction extends Transaction {
    // This goes right after each class/interface statement
    static final String          COPYRIGHT     = "SPECjbb2005,"
                                                       + "Copyright (c) 2005 Standard Performance Evaluation Corporation (SPEC),"
                                                       + "All rights reserved,"
                                                       + "(C) Copyright IBM Corp., 1996 - 2005"
                                                       + "All rights reserved,"
                                                       + "Licensed Materials - Property of SPEC";

    private Company              company;

    private short                warehouseId;

    private byte                 districtId;

    private boolean              home;

    private char                 use_customerId;

    private String               cust_last_name;

    private short                customerId;

    private short                customerWarehouseId;

    private byte                 customerDistrictId;

    private BigDecimal           paymentAmount;

    private Date                 paymentTime;

    private Warehouse            warehousePtr;

    private District             districtPtr;

    private Customer             customerPtr;

    private TransactionLogBuffer initLog;

    private TransactionLogBuffer paymentLog;

    private XMLTransactionLog    xmlPaymentLog;

    String[]                     validationLog = {
            "                                     Payment",
            "Date: 13-04-2000 11:03:14",
            "",
            "Warehouse:    1                          District:  2",
            "mypIw7RUtW7hoQdmASd                      tNLmWyOsX24ecS      ",
            "vIfg0HYwMq                               L1Z5ItczBCNAU33y    ",
            "PWBAjD5CvVrZNaS      Vv 76447-9863       j4h9MghrbPuGahsw     MO 65513-2224",
            "",
            "Customer:    4  Cust-Warehouse:    1  Cust-District:  2",
            "Name:   hkEGDlBmY        OE BARBARPRI            Since:  13-04-2000",
            "        uocINc31tj7haFutp                        Credit: GC",
            "        5vCd1B7isyiCPeg7afQU                     %Disc:  33.99",
            "        6bMzHApngp           Ox 43760-8175       Phone:  308878-613-628-8473",
            "",
            "Amount Paid      $200.00           New Cust-Balance:    -./0.00",
            "Credit Limit    $50000.00", "", "Cust-Data:", "", "", "", "", "",
            ""
                                               };

    boolean[]                    checkLine     = {
            true, false, true, true, true, true, true, true, true, false, true,
            true, true, true, false, true, true, true, true, true, true, true,
            true, true
                                               };

    public PaymentTransaction(Company inCompany, short inWarehouseId) // 2.5.1.1
    {
        company = inCompany;
        warehouseId = inWarehouseId;
        warehousePtr = company.getWarehousePtr(warehouseId, false); // 2.5.2.2
        // bullet 3
        paymentLog = new TransactionLogBuffer();
        setupPaymentLog();
        xmlPaymentLog = new XMLTransactionLog();
        initLog = new TransactionLogBuffer(paymentLog);
        setupInitLog();
    }

    private void setupPaymentLog() {
        paymentLog.putText("Payment", 37, 0, 7);
        paymentLog.putText("Date:", 0, 1, 5);
        paymentLog.putText("Warehouse:", 0, 3, 10);
        paymentLog.putText("District:", 41, 3, 9);
        paymentLog.putText("Customer:", 0, 8, 9);
        paymentLog.putText("Cust-Warehouse:", 16, 8, 15);
        paymentLog.putText("Cust-District:", 38, 8, 14);
        paymentLog.putText("Name:", 0, 9, 5);
        paymentLog.putText("Since:", 49, 9, 6);
        paymentLog.putText("Credit:", 49, 10, 7);
        paymentLog.putText("%Disc:", 49, 11, 6);
        paymentLog.putText("Phone:", 49, 12, 6);
        paymentLog.putText("Amount Paid:", 0, 14, 11);
        paymentLog.putText("New Cust-Balance:", 36, 14, 17);
        paymentLog.putText("Credit Limit:", 0, 15, 12);
        paymentLog.putText("Cust-Data:", 0, 17, 10);
    }

    private void setupInitLog() {
        initLog.putText("DD-MM-YYYY", 6, 1, 10);
        initLog.putText("hh:mm:ss", 17, 1, 8);
        initLog.putCharFill('9', 11, 3, 4);
        initLog.putCharFill('9', 51, 3, 2);
        initLog.putCharFill('X', 0, 4, 20);
        initLog.putCharFill('X', 41, 4, 20);
        initLog.putCharFill('X', 0, 5, 20);
        initLog.putCharFill('X', 41, 5, 20);
        initLog.putCharFill('X', 0, 6, 20);
        initLog.putCharFill('X', 21, 6, 2);
        initLog.putText("XXXXX-XXXX", 24, 6, 10);
        initLog.putCharFill('X', 41, 6, 20);
        initLog.putCharFill('X', 62, 6, 2);
        initLog.putText("XXXXX-XXXX", 65, 6, 10);
        initLog.putCharFill('9', 10, 8, 4);
        initLog.putCharFill('9', 32, 8, 4);
        initLog.putCharFill('9', 53, 8, 2);
        initLog.putCharFill('X', 8, 9, 16);
        initLog.putCharFill('X', 25, 9, 2);
        initLog.putCharFill('X', 28, 9, 16);
        initLog.putText("DD-MM-YYYY", 57, 9, 10);
        initLog.putCharFill('X', 8, 10, 20);
        initLog.putCharFill('X', 57, 10, 2);
        initLog.putCharFill('X', 8, 11, 20);
        initLog.putText("99.99", 57, 11, 5);
        initLog.putCharFill('X', 8, 12, 20);
        initLog.putCharFill('X', 29, 12, 2);
        initLog.putText("XXXXX-XXXX", 32, 12, 10);
        initLog.putText("XXXXXX-XXX-XXX-XXXX", 57, 12, 19);
        initLog.putText("$9999.99", 17, 14, 8);
        initLog.putText("$-99999.99", 54, 14, 10);
        initLog.putText("$99999.99", 16, 15, 9);
        initLog.putCharFill('X', 11, 17, 50);
        initLog.putCharFill('X', 11, 18, 50);
        initLog.putCharFill('X', 11, 19, 50);
        initLog.putCharFill('X', 11, 20, 50);
    }

    public String getMenuName() {
        return "Payment";
    }

    public synchronized void delete() {
    }

    // WRR: Changed to use per-warehouse Random stream.
    public synchronized void init() {
        JBButil.getLog().entering("spec.jbb.PaymentTransaction", "init");
        districtId = (byte) JBButil.random(1, company
                .getMaxDistrictsPerWarehouse(), warehouseId); // 2.5.1.2
        districtPtr = warehousePtr.getDistrictPtr(districtId, false); // 2.5.2.2
        // bullet
        // 4
        int x = (int) JBButil.random(1, 100, warehouseId);
        int y = (int) JBButil.random(1, 100, warehouseId);
        if (y <= 60) {
            cust_last_name = JBButil.choose_random_last_name(company
                    .getMaxCustomersPerDistrict(), warehouseId);
            use_customerId = 'F';
        }
        else {
            customerId = JBButil.create_random_customer_id(company
                    .getMaxCustomersPerDistrict(), warehouseId);
            use_customerId = 'T';
        }
        if (x <= 85) {
            customerDistrictId = districtId;
            customerWarehouseId = warehouseId;
        }
        else {
            customerDistrictId = (byte) JBButil.random(1, company
                    .getMaxDistrictsPerWarehouse(), warehouseId);
            int maxWarehouses = company.getMaxWarehouses();
            for (customerWarehouseId = (short) JBButil.random(1, maxWarehouses,
                    warehouseId); (customerWarehouseId == warehouseId)
                    && (maxWarehouses > 1); customerWarehouseId = (short) JBButil
                    .random(1, maxWarehouses, warehouseId))
                ;
        }
        paymentAmount = BigDecimal.valueOf(20000, 2);
        home = (customerWarehouseId == warehouseId);
        JBButil.getLog().exiting("spec.jbb.PaymentTransaction", "init");
    }

    public boolean process() {
        JBButil.getLog().entering("spec.jbb.PaymentTransaction", "process");
        warehousePtr.receivePayment(paymentAmount);
        String warehousename = warehousePtr.getName();
        districtPtr.receivePayment(paymentAmount);
        switch (use_customerId) {
            case 'T': {
                long uniqueCustomerId = company.buildUniqueCustomerKey(
                        customerWarehouseId, customerDistrictId, customerId);
                customerPtr = company.getCustomer(uniqueCustomerId, true);
            }
                break;
            case 'F':
                customerPtr = company
                        .getCustomerByLastName(customerWarehouseId,
                                customerDistrictId, cust_last_name);
        }
        if (customerPtr == null) {
        }
        else {
            customerPtr.adjustBalance(paymentAmount.negate());
            customerPtr.increaseYTD(paymentAmount);
            customerPtr.incrementPaymentCount();
            if (customerPtr.getCreditStatus() == "BC") {
                String newData = " " + customerPtr.getId() + " "
                        + customerPtr.getDistrictId() + " "
                        + customerPtr.getWarehouseId() + " " + districtId + " "
                        + warehouseId + " " + paymentAmount + " ";
                customerPtr.updateCustomerData(newData);
            }
            paymentTime = new Date();
            History hist = new History(customerPtr.getId(), customerPtr
                    .getDistrictId(), customerPtr.getWarehouseId(), districtId,
                    warehouseId, paymentTime, BigDecimal.valueOf(0, 2), null);
            hist.buildData(warehousename, districtPtr.getName());
            warehousePtr.updateHistory(hist);
            if (Transaction.steadyStateMem) {
                warehousePtr.removeOldestHistory();
            }
        }
        ;
        JBButil.getLog().exiting("spec.jbb.PaymentTransaction", "process");
        return true;
    }

    public synchronized void display() {
        System.out.println("PaymentTransaction Display **********************");
        System.out.println("Warehouse ID is " + warehouseId
                + " ******************");
        System.out.println("District  ID is " + districtId
                + " ******************");
        System.out.println("Customer  ID is " + customerId
                + " ******************");
        System.out
                .println("**************************************************");
    }

    public synchronized void initializeTransactionLog() {
        JBButil.getLog().entering("spec.jbb.PaymentTransaction",
                "initializeTransactionLog");
        // "00000000001111111111222222222233333333334444444444555555555566666666667777777777"
        // "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        // " PAYMENT"
        // "Date: DD-MM-YYYY hh:mm:ss"
        // "Warehouse: 9999 District: 99"
        // + "XXXXXXXXXXXXXXXXXXXX XXXXXXXXXXXXXXXXXXXX"
        // + "XXXXXXXXXXXXXXXXXXXX XXXXXXXXXXXXXXXXXXXX"
        // + "XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX XXXXXXXXXXXXXXXXXXXX XX
        // XXXXX-XXXX"
        // + "\n"
        // + "Customer: 9999 Cust-Warehouse: 9999 Cust-District: 99"
        // + "Name: XXXXXXXXXXXXXXXX XX XXXXXXXXXXXXXXXX Since: DD-MM-YYYY"
        // + " XXXXXXXXXXXXXXXXXXXX Credit: XX"
        // + " XXXXXXXXXXXXXXXXXXXX %Disc: 99.99"
        // + " XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX Phone: XXXXXX-XXX-XXX-XXXX"
        // + "Amount Paid: $9999.99 New Cust-Balance: $-99999.99"
        // + "Credit Limit: $99999.99"
        // + "\n"
        // + "Cust-Data: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        // + " XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        // + " XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        // + " XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        // + "\n\n";
        if (Transaction.enableLogWrite)
            initLog.display();
        JBButil.getLog().exiting("spec.jbb.PaymentTransaction",
                "initializeTransactionLog");
    }

    public synchronized void processTransactionLog() {
        JBButil.getLog().entering("spec.jbb.PaymentTransaction",
                "processTransactionLog");
        paymentLog.clearBuffer();
        xmlPaymentLog.clear();
        setupPaymentLog();
        Address ware_addr = warehousePtr.getAddress();
        Address dist_addr = districtPtr.getAddress();
        String phone;
        String zip;
        // "00000000001111111111222222222233333333334444444444555555555566666666667777777777"
        // "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
        // " PAYMENT"
        // "Date: DD-MM-YYYY hh:mm:ss"
        paymentLog.putDate(paymentTime, 6, 1, 10);
        paymentLog.putTime(paymentTime, 17, 1, 8);
        // "Warehouse: 9999 District: 99"
        paymentLog.putInt(warehouseId, 11, 3, 4);
        paymentLog.putInt(districtId, 51, 3, 2);
        // + "XXXXXXXXXXXXXXXXXXXX XXXXXXXXXXXXXXXXXXXX"
        paymentLog.putText(ware_addr.getStreet1(), 0, 4, 20);
        paymentLog.putText(dist_addr.getStreet1(), 41, 4, 20);
        // + "XXXXXXXXXXXXXXXXXXXX XXXXXXXXXXXXXXXXXXXX"
        paymentLog.putText(ware_addr.getStreet2(), 0, 5, 20);
        paymentLog.putText(dist_addr.getStreet2(), 41, 5, 20);
        // + "XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX XXXXXXXXXXXXXXXXXXXX XX
        // XXXXX-XXXX"
        paymentLog.putText(ware_addr.getCity(), 0, 6, 20);
        paymentLog.putText(ware_addr.getState(), 21, 6, 2);
        zip = ware_addr.getZip();
        // --paymentScreen.putText((zip.substring(0,5) + "-" +
        // zip.substring(5,9)), 24, 6, 10);
        paymentLog.putSubstring(zip, 24, 6, 0, 5);
        paymentLog.putChar('-', 29, 6);
        paymentLog.putSubstring(zip, 30, 6, 5, 4);
        paymentLog.putText(dist_addr.getCity(), 41, 6, 20);
        paymentLog.putText(dist_addr.getState(), 62, 6, 2);
        zip = dist_addr.getZip();
        // --paymentScreen.putText((zip.substring(0,5) + "-" +
        // zip.substring(5,9)), 65, 6, 10);
        paymentLog.putSubstring(zip, 65, 6, 0, 5);
        paymentLog.putChar('-', 70, 6);
        paymentLog.putSubstring(zip, 71, 6, 5, 4);
        // + "\n"
        // + "Customer: 9999 Cust-Warehouse: 9999 Cust-District: 99"
        if (customerPtr != null) {
            Address cust_addr = customerPtr.getAddress();
            Date custDate = customerPtr.getSince();
            paymentLog.putInt(customerPtr.getId(), 10, 8, 4);
            paymentLog.putInt(customerPtr.getWarehouseId(), 32, 8, 4);
            paymentLog.putInt(customerPtr.getDistrictId(), 53, 8, 2);
            // + "Name: XXXXXXXXXXXXXXXX XX XXXXXXXXXXXXXXXX Since: DD-MM-YYYY"
            paymentLog.putText(customerPtr.getFirstName(), 8, 9, 16);
            paymentLog.putText(customerPtr.getMiddleName(), 25, 9, 2);
            paymentLog.putText(customerPtr.getLastName(), 28, 9, 16);
            paymentLog.putDate(custDate, 57, 9, 10);
            // + " XXXXXXXXXXXXXXXXXXXX Credit: XX"
            paymentLog.putText(cust_addr.getStreet1(), 8, 10, 20);
            paymentLog.putText(customerPtr.getCreditStatus(), 57, 10, 2);
            // + " XXXXXXXXXXXXXXXXXXXX %Disc: 99.99"
            paymentLog.putText(cust_addr.getStreet2(), 8, 11, 20);
            // paymentScreen.putDouble((customerPtr.getDiscountRate().doubleValue()*100.0),
            // 57, 11, 5, 2);
            paymentLog.putDouble(customerPtr.getDiscountRate()
                    .movePointRight(2).toString(), 57, 11, 5);
            // + " XXXXXXXXXXXXXXXXXXXX XX XXXXX-XXXX Phone:
            // XXXXXX-XXX-XXX-XXXX"
            paymentLog.putText(cust_addr.getCity(), 8, 12, 20);
            paymentLog.putText(cust_addr.getState(), 29, 12, 2);
            zip = cust_addr.getZip();
            phone = customerPtr.getPhone();
            // --paymentScreen.putText((zip.substring(0,5) + "-" +
            // zip.substring(5,9)), 32, 12, 10);
            paymentLog.putSubstring(zip, 32, 12, 0, 5);
            paymentLog.putChar('-', 37, 12);
            paymentLog.putSubstring(zip, 38, 12, 5, 4);
            // --paymentScreen.putText((phone.substring(0,6) + "-" +
            // phone.substring(6,9) + "-" + phone.substring(9,12) + "-" +
            // phone.substring(12,16)), 57, 12, 19);
            paymentLog.putSubstring(phone, 57, 12, 0, 6);
            paymentLog.putChar('-', 63, 12);
            paymentLog.putSubstring(phone, 64, 12, 6, 3);
            paymentLog.putChar('-', 67, 12);
            paymentLog.putSubstring(phone, 68, 12, 9, 3);
            paymentLog.putChar('-', 71, 12);
            paymentLog.putSubstring(phone, 72, 12, 12, 4);
            // + "Amount Paid: $9999.99 New Cust-Balance: $-99999.99"
            /*
             * paymentScreen.putDollars(paymentAmount.doubleValue(), 17, 14, 8);
             * paymentScreen.putDollars(customerPtr.getBalance().doubleValue(),
             * 54, 14, 10); // + "Credit Limit: $99999.99"
             * paymentScreen.putDollars(customerPtr.getCreditLimit().doubleValue(),
             * 16, 15, 9);
             */
            paymentLog.putDollars(paymentAmount, 17, 14, 8);
            paymentLog.putDollars(customerPtr.getBalance(), 54, 14, 10);
            // + "Credit Limit: $99999.99"
            paymentLog.putDollars(customerPtr.getCreditLimit(), 16, 15, 9);
            // + "\n"
            // + "Cust-Data: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
            // + " XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
            // + " XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
            // + " XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
            // + "\n\n";
            if (customerPtr.getCreditStatus().equals("BC")) {
                String custData = customerPtr.getCustomerDataParts();
                paymentLog.putSubstring(custData, 11, 17, 0, 50);
                paymentLog.putSubstring(custData, 11, 18, 50, 50);
                paymentLog.putSubstring(custData, 11, 19, 100, 50);
                paymentLog.putSubstring(custData, 11, 20, 150, 50);
            }
            ;
        }
        else {
            paymentLog.putText("No customer found for last name =", 11, 17, 35);
            paymentLog.putText(cust_last_name, 46, 17, 16);
            paymentLog.putText("District :", 11, 18, 10);
            paymentLog.putInt(districtId, 22, 18, 2);
            paymentLog.putText("Warehouse:", 11, 19, 10);
            paymentLog.putInt(warehouseId, 22, 19, 4);
            paymentLog.putText("No payment processed.", 11, 20, 25);
        }
        // create XML representation
        xmlPaymentLog.populateXML(paymentLog);
        if (Transaction.enableLogWrite)
            paymentLog.display();
        if (Transaction.validationFlag) {
            String[] s = paymentLog.validate();
            if (s.length != validationLog.length) {
                StringBuffer sb = new StringBuffer(200);
                sb
                        .append("VALIDATION ERROR:  mismatch in screen lengths for PaymentTransaction");
                sb.append(System.getProperty("line.separator"));
                sb.append("    Screen length should be:  "
                        + validationLog.length);
                sb.append(System.getProperty("line.separator"));
                sb.append("    Screen length is:  " + s.length);
                sb.append(System.getProperty("line.separator"));
                JBButil.getLog().warning(sb.toString());
                Transaction.invalidateRun();
            }
            for (int i = 0; i < validationLog.length; i++) {
                if (checkLine[i]) {
                    if (!s[i].equals(validationLog[i])) {
                        StringBuffer sb = new StringBuffer(200);
                        sb
                                .append("VALIDATION ERROR:  incorrect output for PaymentTransaction");
                        sb.append(System.getProperty("line.separator"));
                        sb.append("    Line " + (i + 1) + " should be:  |"
                                + validationLog[i] + "|");
                        sb.append(System.getProperty("line.separator"));
                        sb.append("    Line " + (i + 1) + " is:  |" + s[i]
                                + "|");
                        sb.append(System.getProperty("line.separator"));
                        JBButil.getLog().warning(sb.toString());
                        Transaction.invalidateRun();
                    }
                }
            }
        }
        JBButil.getLog().exiting("spec.jbb.PaymentTransaction",
                "processTransactionLog");
    }
}
