#ifndef __REQUEST_H
#define __REQUEST_H

enum Status { Success, Failure };

struct Request {
    int xctType;
};

struct Response {
    Status status;
};

#endif
