/*<std-header orig-src='shore'>

 $Id: lockid_test.cpp,v 1.2 2010/06/15 17:30:11 nhall Exp $

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

#include <w_stream.h>
#include <sys/types.h>
#include <cassert>
#include "sm_vas.h"

// To get the extid_t:
// #include "sm_s.h"

vid_t    vol(1000);
stid_t   stor(vol,900);
lpid_t   page(stor, 50);
rid_t    rec(page, 6);
extid_t  extent;
kvl_t    kvl;
typedef lockid_t::user1_t user1_t;
typedef lockid_t::user2_t user2_t;
typedef lockid_t::user3_t user3_t;
typedef lockid_t::user4_t user4_t;
user1_t  u1(1);
user2_t  u2(u1.u1, 2);
user3_t  u3(u1.u1, u2.u2, 3);
user4_t  u4(u1.u1, u2.u2, u3.u3, 4);


void dump(lockid_t &l)
{
    cout << "Lock: lspace " << l.lspace() << endl;
    cout << "\t vid()=" << l.vid() << endl;

    stid_t stor;
    l.extract_stid(stor);
    cout << "\t store()=" << l.store() << " stid_t=(" << stor << ")" << endl;


    lpid_t page;
    l.extract_lpid(page);
    cout << "\t page()=" << l.page() << " (lpid_t=" << page << ")" << endl;

    rid_t rec;
    l.extract_rid(rec);
    cout << "\t slot()=" << l.slot() << " (rid_t=" << rec << ")" << endl;

    extid_t extent;
    l.extract_extent(extent);
    cout << "\t extent()=" << l.extent() << " (extid_t=" << extent << ")" << endl;

#if 0
    // Removed b/c it's hash-based and makes the -out files differ
    // and I do want to compare...
    kvl_t kvl;
    l.extract_kvl(kvl);
    cout << "\t kvl" << "(" << kvl << ")" << endl;
#endif

    user1_t u1;
    l.extract_user1(u1);
    cout << "\t u1()=" << l.u1() << " user1_t=(" << u1 << ")" << endl;

    user2_t u2;
    l.extract_user2(u2);
    cout << "\t u2()=" << l.u2() << " (user2_t=" << u2 << ")" << endl;

    user3_t u3;
    l.extract_user3(u3);
    cout << "\t u3()=" << l.u3() << " ( user3_t=" << u3 << ")" << endl;

    user4_t u4;
    l.extract_user4(u4);
    cout << "\t u4()=" << l.u4() << " ( user4_t=" << u4 << ")" << endl;

#if 0
    // debugging
    {
    cout << "hex/dec dump - b" << endl;
    u_char *p = (u_char *)&l;
    for(int i=0; i < int(sizeof(l)); i++) {
        cout << i << ":" << ::hex << unsigned(*p)
            << "/" << ::dec << unsigned(*p) << " " ;
        p++;
    }
    cout << endl;
    }
    {
    cout << "hex/dec dump - s" << endl;
    uint2_t *p = (uint2_t *)&l;
    for(int i=0; i < int(sizeof(l)/sizeof(uint2_t)); i++) {
        cout << i << ":" << ::hex << unsigned(*p)
            << "/" << ::dec << unsigned(*p) << " ";
        p++;
    }
    cout << endl;
    }
    {
    cout << "hex/dec dump - w" << endl;
    u_int *p = (u_int *)&l;
    for(int i=0; i < int(sizeof(l)/sizeof(uint_t)); i++) {
        cout << i << ":" << ::hex << unsigned(*p)
            << "/" << ::dec << unsigned(*p) << " ";
        p++;
    }
    cout << endl;
    }
#endif
}

int
main(int /*argc*/, char* /*argv*/[])
{
    extent.vol = vol;
    extent.ext = 33;

    const char     *keybuf = "Admiral Richard E. Byrd";
    const char     *elembuf= "Most of the confusion in the world comes from not knowing how little we need.";
    vec_t    key(keybuf, strlen(keybuf));
    vec_t    elem(elembuf, strlen(elembuf));
    kvl.set(stor, key, elem);

    cout << "Sources: " << endl
        <<  "\t vol " << vol << endl
        <<  "\t store " << stor << endl
        <<  "\t page " << page << endl
        <<  "\t rec " << rec << endl
        <<  "\t kvl " << kvl << endl
        <<  "\t extent " << extent << endl
        <<  "\t u1 " << u1 << endl
        <<  "\t u2 " << u2 << endl
        <<  "\t u3 " << u3 << endl
        <<  "\t u4 " << u4 << endl
        ;

    {
        lockid_t l(vol);
        cout << "{ Volume lock " << l << endl;
        dump(l);
        cout << "}" << endl;
    }
    {
        lockid_t l(stor);
        cout << "Store lock " << l << endl;
        dump(l);
        cout << "}" << endl;
    }
    {
        lockid_t l(page);
        cout << "Page lock " << l << endl;
        dump(l);
        cout << "}" << endl;
    }
    {
        lockid_t l(rec);
        cout << "Record lock " << l << endl;
        dump(l);
        cout << "}" << endl;
    }
    {
        lockid_t l(kvl);
        cout << "Kvl lock " << l << endl;
        cout << "}" << endl;
    }
    {
        lockid_t l(extent);
        cout << "Extent lock " << l << endl;
        dump(l);
        cout << "}" << endl;
    }
    {
        lockid_t l(u1);
        cout << "User 1 lock " << l << endl;
        dump(l);
        cout << "}" << endl;
    }
    {
        lockid_t l(u2);
        cout << "User 2 lock " << l << endl;
        dump(l);
        cout << "}" << endl;
    }
    {
        lockid_t l(u3);
        cout << "User 3 lock " << l << endl;
        dump(l);
        cout << "}" << endl;
    }
    {
        lockid_t l(u4);
        cout << "User 4 lock " << l << endl;
        dump(l);
        cout << "}" << endl;
    }
}

