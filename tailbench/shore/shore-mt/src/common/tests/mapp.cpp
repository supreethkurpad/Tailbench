/*<std-header orig-src='shore'>

 $Id: mapp.cpp,v 1.2 2010/05/26 01:20:15 nhall Exp $

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
#include <w_base.h>
#include <w_getopt.h>
#include <basics.h>
#include <iostream>

#include <stid_t.h>
#include <map>
#include <set>

typedef std::multimap<snum_t, int, compare_snum_t> MM;
typedef std::multiset<snum_t> MS;

// typedef std::map<snum_t, int, compare_snum_t> M;
typedef std::map<snum_t, int> M;
typedef std::set<snum_t>  S;

void
dump(MM &m)
{
    cout  << "---------------------- MULTIMAP:" << endl;
    cout 
        // << " max size " << m.max_size() << endl
        << " size " << m.size() << endl
        << endl;

    MM::iterator iter;
    for( iter = m.begin(); iter != m.end(); ++iter ) {
        cout << "idx: " << iter->first
            << " count: " << m.count(iter->first)
            << endl;

        cout << "idx: " << iter->first << ", val: " << iter->second << endl;
    }
}

void
dump(const MS &m)
{
    cout  << "---------------------- MULTISET:" << endl;
    cout 
        // << " max size " << m.max_size() << endl
        << " size " << m.size() << endl
        << endl;

    S::iterator iter;
    for( iter = m.begin(); iter != m.end(); ++iter ) {
        const snum_t snum = *iter;
        cout << "idx: " << snum
            << " count: " << m.count(snum)
            << endl;
    }
}

void
dump(M &m)
{
    cout  << "------------------- MAP:" << endl;

    M::iterator iter;
    for( iter = m.begin(); iter != m.end(); ++iter ) {
        cout << "idx: " << iter->first
            << " count: " << m.count(iter->first)
            << endl;

        cout << "idx: " << iter->first << ", val: " << iter->second << endl;
    }
    cout 
        // << " max size " << m.max_size() << endl
        << " size " << m.size() << endl
        << endl;
}
void
dump(S &m)
{
    cout  << "---------------------- SET:" << endl;
    cout 
        // << " max size " << m.max_size() << endl
        << " size " << m.size() << endl
        << endl;

    S::iterator iter;
    for( iter = m.begin(); iter != m.end(); ++iter ) {
        const snum_t snum = *iter;
        cout << "idx: " << snum
            << " count: " << m.count(snum)
            << endl;
    }
}

int main(int , char **)
{

    M m;
    MM mm;
    MS ms;
    S s;

    m[0] = 100;
    m[0] = 100;
    m[1] = 101;
    m[1] = 101;
    m[2] = 102;
    m[2] = 102;
	int test=m[4];
	cout << " test " << test << endl;
    dump(m);

    s.insert(0);
    s.insert(0);
    s.insert(1);
    s.insert(1);
    s.insert(1);
    s.insert(2);
    s.insert(2);
    dump(s);

    mm.insert(std::pair<snum_t, int>(0, 100));
    mm.insert(std::pair<snum_t, int>(1, 101));
    mm.insert(std::pair<snum_t, int>(1, 101));
    mm.insert(std::pair<snum_t, int>(1, 101));
    mm.insert(std::pair<snum_t, int>(2, 102));
    dump(mm);

    ms.insert(0);
    ms.insert(0);
    ms.insert(1);
    ms.insert(1);
    ms.insert(1);
    ms.insert(2);
    ms.insert(2);
    dump(ms);

    return 0;
}


