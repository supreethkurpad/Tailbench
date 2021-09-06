/*<std-header orig-src='shore'>

 $Id: list2.cpp,v 1.24.2.5 2010/03/19 22:17:53 nhall Exp $

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
#include <cstddef>

#include <w.h>

struct elem2_t {
    int         i;
    w_link_t        link;
};

int main()
{
    w_list_t<elem2_t, unsafe_list_dummy_lock_t> l(W_LIST_ARG(elem2_t, link),
			unsafe_nolock);

    elem2_t array[10];

    int i;
    for (i = 0; i < 10; i++)  {
        array[i].i = i;
        l.push(&array[i]);
    }

    {
        w_list_i<elem2_t, unsafe_list_dummy_lock_t> iter(l);
        for (i = 0; i < 10; i++)  {
#if W_DEBUG_LEVEL>0
            elem2_t* p = iter.next();
            w_assert1(p);
            w_assert1(p->i == 9 - i);
#else
            (void) iter.next();
#endif
        }
        w_assert1(iter.next() == 0);

        w_list_const_i<elem2_t, unsafe_list_dummy_lock_t> const_iter(l);
        for (i = 0; i < 10; i++)  {
#if W_DEBUG_LEVEL>0
            const elem2_t* p = const_iter.next();
            w_assert1(p);
            w_assert1(p->i == 9 - i);
#else
            (void) const_iter.next();
#endif
        }
        w_assert1(const_iter.next() == 0);
    }

    for (i = 0; i < 10; i++)  {
#if W_DEBUG_LEVEL>0
        elem2_t* p = l.pop();
        w_assert1(p);
        w_assert1(p->i == 9 - i);
#else
        (void) l.pop();
#endif
    }

    w_assert1(l.pop() == 0);

    for (i = 0; i < 10; i++)  {
        l.append(&array[i]);
    }

    {
        w_list_i<elem2_t, unsafe_list_dummy_lock_t> iter(l);
        for (i = 0; i < 10; i++)  {
#if W_DEBUG_LEVEL>0
            elem2_t* p = iter.next();
            w_assert1(p);
            w_assert1(p->i == i);
#else
            (void) iter.next();
#endif
        }
    }

    for (i = 0; i < 10; i++)  {
#if W_DEBUG_LEVEL>0
        elem2_t* p = l.chop();
        w_assert1(p);
        w_assert1(p->i == 9 - i);
#else
        (void) l.chop();
#endif
    }
    w_assert1(l.chop() == 0);

    return 0;
}

#ifdef __BORLANDC__
#pragma option -Jgd
#include <w_list.cpp>
typedef w_list_t<elem2_t, unsafe_list_dummy_lock_t> w_list_t_elem2_t_dummy;
#endif /*__BORLANDC__*/

#ifdef EXPLICIT_TEMPLATE
template class w_list_t<elem2_t, unsafe_list_dummy_lock_t>;
template class w_list_i<elem2_t, unsafe_list_dummy_lock_t>;
template class w_list_const_i<elem2_t, unsafe_list_dummy_lock_t>;
#endif

