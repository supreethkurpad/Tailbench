#ifndef __REQUEST_H
#define __REQUEST_H

enum ReqType { NEW_ORDER = 0, PAYMENT = 1, DELIVERY = 2, ORDER_STATUS = 3, 
    STOCK_LEVEL = 4};

struct Request {
    ReqType type;
};

struct Response {
    bool success;
};

#endif
