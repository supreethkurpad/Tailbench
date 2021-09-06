/*<std-header orig-src='shore'>

 $Id: kvl.cpp,v 1.15.2.4 2010/03/19 22:19:21 nhall Exp $

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
#include <kvl_t.h>

#include <iostream>
#include <w_strstream.h>

int main(int argc, const char *argv[])
{
    if(argc != 3 && argc != 4) {
    cerr << "Usage: " << argv[0]
        << " <store-id: x.y> <key: string> [<elem: string >]"
        << endl;
    return 1;
    }
    else {
    int v;
    int st;
    {
        w_istrstream anon(argv[1]);
        char dot;
        anon >> v;
        anon >> dot;
        anon >> st;
    }
    stid_t s(v,st);
    cvec_t key(argv[2], strlen(argv[2]));

    cvec_t elem;
    if(argc > 3) {
        elem.put(argv[3], strlen(argv[3]));
    }
    kvl_t  kvl(s, key, elem);

    cout 
        << "kvl(store= " << s 
        << " key= " << key 
        << " elem= " << elem 
        << ")= " << kvl << endl;

    // calclulate kvl for null vector
    vec_t nul;
    kvl_t kvl1(s, nul, elem);
    cout 
        << "kvl(store= " << s 
        << " key= " << nul 
        << " elem= " << elem 
        << ")= " << kvl1 << endl;

    kvl_t kvl3(s, key, nul);
    cout 
        << "kvl(store= " << s 
        << " key= " << key 
        << " elem= " << nul 
        << ")= " << kvl3 << endl;

    kvl_t kvl2(s, nul, nul);
    cout 
        << "kvl(store= " << s 
        << " key= " << nul 
        << " elem= " << nul 
        << ")= " << kvl2 << endl;
    }
    return 0;
}

