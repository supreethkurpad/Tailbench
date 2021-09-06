/*<std-header orig-src='shore'>

 $Id: opaque.cpp,v 1.17.2.5 2009/10/30 23:50:53 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <basics.h>
#include <cassert>
#include <tid_t.h>

#include <iostream>

#ifdef __GNUG__
template ostream &operator<<(ostream &, const opaque_quantity<max_server_handle_len> &);
#endif


int main()
{
    // test unaligned vectors if possible;

    char dummy[500];

    char *d = &dummy[3];
    server_handle_t *s = 0;

    int j(0);
    int &k = *((int *)&dummy[3]);
    char * junk = (char *)&k;
    // memcpy((char *)&k, &j, sizeof(j)); // dumps core
    memcpy(junk, &j, sizeof(j)); // ok
    memcpy(&dummy[3], &j, sizeof(j)); // ok


    {
        s = (server_handle_t *)dummy; // aligned, if possible
        *s = "COPY";
        server_handle_t &th = *s;
        server_handle_t ch = th;
        cout << "value of ch = " << ch << endl;
        cout << "length of ch = " << ch.length() << endl;
    }
    {
        s = (server_handle_t *)dummy; // aligned, if possible
        *s = "ALIGNED";
        // If we print the address to cout, we can't diff with a .out file
        cerr << "(not an error: )address of s = " << W_ADDR(s) << endl;
        cout << "value of s = " << *s << endl;
        cout << "length of s = " << s->length() << endl;
    }

    {
        s = (server_handle_t *)d; // unaligned, if possible
        *s = "NOTALIGNED";
        cerr << "(not an error: )address of s = " << W_ADDR(s) << endl;
        cout << "value of s = " << *s << endl;
        cout << "length of s = " << s->length() << endl;
    }

    {
        s = (server_handle_t *)dummy; // aligned, if possible
        *s = "BYTEORDER";
        cout << "value of s = " << *s << endl;
        cout << "length of s = " << s->length() << endl;
        s->hton();
        // net order value to cerror since it can change depending on platform
        cerr << "hton length of s = " << hex << s->length() << endl;
        // but the bytes should always be the same!
        cout << "hton bytes of s = ";
        // XXX magic types/numbers, but "well known"
        union {
            uint4_t    l;
            uint1_t    c[4];
        } un;
        un.l = s->length();
        for (unsigned i = 0; i < sizeof(un.c); i++)
            cout << ' ' << hex << (unsigned) un.c[i];
        cout << endl;

        s->ntoh();
        cout << "ntoh length of s = " << hex << s->length() << endl;
    }

    {
        tid_t uninit;
        tid_t t1 = uninit;
        tid_t t2 = tid_t();
        tid_t t3 = t2;
        t3 = t1;
    }

    return 0;
}

