/*<std-header orig-src='shore'>

 $Id: lock_cache_test.cpp,v 1.1 2010/06/15 22:22:10 nhall Exp $

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
#define SM_SOURCE
class lock_request_t {};
#include "lock_cache.h"

// To get the extid_t:
// #include "sm_s.h"

vid_t    vol(1000);
stid_t   stor(vol,900);
lpid_t   page(stor, 50);
rid_t    rec(page, 6);
extid_t  extent;
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
    cout << "\t vid() " << l.vid() << endl;

    stid_t stor;
    l.extract_stid(stor);
    cout << "\t store() " << l.store() << "(" << stor << ")" << endl;


    lpid_t page;
    l.extract_lpid(page);
    cout << "\t page() " << l.page() << "(" << page << ")" << endl;

    rid_t rec;
    l.extract_rid(rec);
    cout << "\t slot() " << l.slot() << "(" << rec << ")" << endl;

    extid_t extent;
    l.extract_extent(extent);
    cout << "\t extent() " << l.extent() << "(" << extent << ")" << endl;

    user1_t u1;
    l.extract_user1(u1);
    cout << "\t u1() " << l.u1() << "(" << u1 << ")" << endl;

    user2_t u2;
    l.extract_user2(u2);
    cout << "\t u2() " << l.u2() << "(" << u2 << ")" << endl;

    user3_t u3;
    l.extract_user3(u3);
    cout << "\t u3() " << l.u3() << "(" << u3 << ")" << endl;

    user4_t u4;
    l.extract_user4(u4);
    cout << "\t u4() " << l.u4() << "(" << u4 << ")" << endl;
}

lock_cache_t<5,10> C;

void
testit(lockid_t &l, int line) 
{
    lock_cache_elem_t victim;
    victim.clear();
    bool evicted = C.put (l, SH, NULL, victim);
    cout << "{" << endl;
    cout << "\tInserted " << l << " at line " << line << endl; 
    cout << "\tEvicted " << evicted << endl;
    if(evicted) {
        cout << "\t\t Victim " ; victim.dump(); cout << endl;
    }
    cout << "\tDump at line " << line << endl; C.dump();

    lock_cache_elem_t *probed = C.probe(l, l.lspace());
    w_assert0(probed != NULL);
    lock_cache_elem_t *searched = C.search(l);
    if(searched != probed) {
        cout << "Not found, collision with "; probed->dump(); cout << endl;
    }


    int found = (searched != NULL);
    // Possibilities:
    // 1) Didn't insert b/c the entry returned by probe  has
    // higher granularity than what we're trying to insert.
    // But we don't know that and the result of put() doesn't
    // tell us. Search() does.  
    // Victim had better be the one we tried to insert.  
    //
    // 2) Inserted and evicted noone.
    //
    // 3) Inserted and evicted someone else.
    // Victim had better have equal or higher granularity that what we
    // tried to insert.  It might be a parent of the one we tried to
    // insert.
    //
    // 4) not inserted and not evicted : not an option
    w_assert0( (!found && !evicted) == false);

    if(!found) {
        // case 1 above
        w_assert0(victim.lock_id == l);
    }
    if(found && evicted) {
        // case 3 above
        cout << "Now compact..." << endl;
        bool evicted2;
        C.compact(victim.lock_id);
        cout << "\tDump after compact " << endl; C.dump();
        lock_cache_elem_t victim2;
        if(victim.lock_id == l) {
            cout << "Try to reinsert victim after eviction. " << endl;
            evicted2 = C.put(victim.lock_id, SH, NULL, victim2);
        } else {
            cout << "Try again after eviction. " << endl;
            evicted2 = C.put(l, SH, NULL, victim2);
        }
        cout << "\tThis time evicted " << evicted2 << endl;
        if(evicted2) {
            cout << "\t\t This victim " ; victim2.dump(); cout << endl;
        }
        cout << "\tDump after reinsert " << endl; C.dump();
    }

    // Ok, now let's ensure that no subsumed entries are in the
    // table.
    if(l.lspace() <= lockid_t::t_page) {
        lockid_t ancestor(l); // make a non-const copy.
        for (int k = l.lspace(); 
                k < lockid_t::t_page; )
        {
            k++;
            ancestor.truncate(lockid_t::name_space_t(k));
            lock_cache_elem_t *p = C.search(ancestor);
            w_assert0(p == NULL);
        }
    }


    cout << "}" << endl;
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

    cout << "Sources: " << endl
        <<  "\t vol " << vol << endl
        <<  "\t store " << stor << endl
        <<  "\t page " << page << endl
        <<  "\t rec " << rec << endl
        <<  "\t extent " << extent << endl
        <<  "\t u1 " << u1 << endl
        <<  "\t u2 " << u2 << endl
        <<  "\t u3 " << u3 << endl
        <<  "\t u4 " << u4 << endl
        ;

    lockid_t lvol(vol);
    lockid_t lstor(stor);
    lockid_t lpage(page);
    lockid_t lrec(rec);
    lockid_t lx(extent);
    lockid_t l1(u1);
    lockid_t l2(u2);
    lockid_t l3(u3);
    lockid_t l4(u4);

    testit(lvol, __LINE__);
    testit(lstor, __LINE__);
    testit(lpage, __LINE__);
    testit(lrec, __LINE__);
    testit(lx, __LINE__);
    testit(l1, __LINE__);
    testit(l2, __LINE__);
    testit(l3, __LINE__);
    testit(l4, __LINE__);

    C.compact();
    cout << "Dump after compacting all at line " << __LINE__ << endl; 
    C.dump();
}
