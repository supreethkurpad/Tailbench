/*<std-header orig-src='shore'>

 $Id: vectors.cpp,v 1.25.2.6 2010/03/19 22:19:21 nhall Exp $

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

#include <w.h>
#include <basics.h>
#include <vec_t.h>
#include <w_debug.h>

#include <iostream>
#include <w_strstream.h>

const char *d = "dddddddddd";
const char *djunk = "Djunk";
const char *b = "bbbbbbbbbb";
const char *bjunk = "Bjunk";
const char *c = "cccccccccc";
const char *cjunk = "Cjunk";
const char *a = "aaaaaaaaaa";
const char *ajunk = "Ajunk";

void V(const vec_t &a, int b, int c, vec_t &d)
{

    DBG(<<"*******BEGIN TEST("  << b << "," << c << ")");

    for(int l = 0; l<100; l++) {
        if(c > (int) a.size()) break;
        a.mkchunk(b, c, d);

        c+=b;
    }
    DBG(<<"*******END TEST");
}

void
P(const char *s) 
{
    w_istrstream anon(s);

    vec_t    t;
    cout << "P:" << s << endl;
    anon >> t;

    cout << "input operator:" << endl;

    cout << "output operator:" << endl;
    cout << t;
    cout << endl;

    t.vecdelparts();

}

int main()
{
    vec_t test;
    vec_t tout;

#define TD(i,j) test.put(&d[i], j); 
#define TB(i,j) test.put(&b[i], j);
#define TA(i,j) test.put(&a[i], j); 
#define TC(i,j) test.put(&c[i], j);

    TA(0,10);
    TB(0,10);
    TC(0,10);
    TD(0,10);


    V(test, 5, 7, tout);
    V(test, 5, 10, tout);
    V(test, 5, 22, tout);

    V(test, 11, 0, tout);
    V(test, 11, 7, tout);
    V(test, 11, 9, tout);

    V(test, 30, 9, tout);
    V(test, 30, 29, tout);
    V(test, 30, 40, tout);
    V(test, 40, 30, tout);

    V(test, 100, 9, tout);

    P("{ {1   \"}\" }");
    /*{{{*/
    P("{ {3 \"}}}\"      }}");
    P("{ {30 \"xxxxxxxxxxyyyyyyyyyyzzzzzzzzzz\"} }");
    P("{ {30 \"xxxxxxxxxxyyyyyyyyyyzzzzzzzzzz\"}{10    \"abcdefghij\"} }");

    {
        vec_t t;
        t.reset();
        t.put("abc",3);
        cout << "FINAL: "<< t << endl;
    }

    {
        cout << "ZVECS: " << endl;

        {
            zvec_t z(0);
            cout << "z(0).count() = " << z.count() << endl;
            cout << "z(0) is zero vec: " << z.is_zvec() << endl;
            vec_t  zv;
            zv.set(z);
            cout << "zv.set(z).count() = " << zv.count() << endl;
            cout << "zv is zero vec: " << zv.is_zvec() << endl;
        }
        {
            zvec_t z;
            cout << "z is zero vec: " << z.is_zvec() << endl;
            cout << "z.count() = " << z.count() << endl;
            vec_t  zv;
            zv.set(z);
            cout << "zv.set(z).count() = " << zv.count() << endl;
            cout << "zv(0) is zero vec: " << zv.is_zvec() << endl;
        }
    }

// This is endian-dependent, so let's adjust the test program
// to match the "vector.out" file, since this is easier than
// dealing with different .out files.
        {
#ifdef WORDS_BIGENDIAN
                int n = 0x03000080;
                int m = 0xfcffffef;
#else
                int n = 0x80000003;
                int m = 0xeffffffc;
#endif
                vec_t   num( (void*)&n, sizeof(n));
                vec_t   num2( (void*)&m, sizeof(m));

                cout << "vec containing 0x80000003 (little-endian)"
                << " prints as: " << num  << endl;

                cout << "vec containing 0xeffffffc (little-endian)"
                << " prints as: " << num2  << endl;

        }


    {
        char c = 'h';
        char last = (char)'\377';
        char last1 = '\377';
        char last2 = (char)0xff;

        vec_t   cv( (void*)&c, sizeof(c));
        vec_t   lastv( (void*)&last, sizeof(last));
        vec_t   last1v( (void*)&last1, sizeof(last1));
        vec_t   last2v( (void*)&last2, sizeof(last2));

        bool result = (bool)(cv < lastv);
        cout << "cv <= lastv: " << result << endl;
        cout << "cv prints as: " << cv <<endl;
        cout << "lastv prints as: " << lastv <<endl;
        cout << "last1 prints as: " << last1v <<endl;
        cout << "last2 prints as: " << last2v <<endl;
    }
    return 0;
}

