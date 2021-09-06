/*<std-header orig-src='shore'>

 $Id: list3.cpp,v 1.24.2.4 2010/03/19 22:17:53 nhall Exp $

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

#define W_INCL_LIST_EX
#include <w.h>

struct elem3_t {
    int         i;
    w_link_t        link;
};

typedef w_ascend_list_t<elem3_t, unsafe_list_dummy_lock_t, int>   elem_ascend_list_t;
typedef w_descend_list_t<elem3_t, unsafe_list_dummy_lock_t, int>  elem_descend_list_t;

int main()
{
    elem3_t array[10];
    elem3_t* p;

    int i;
    for (i = 0; i < 10; i++)
        array[i].i = i;

    {
        elem_ascend_list_t u(W_KEYED_ARG(elem3_t, i, link), unsafe_nolock);

        for (i = 0; i < 10; i += 2)   {
            u.put_in_order(&array[9 - i]);        // insert 9, 7, 5, 3, 1
        }

        for (i = 0; i < 10; i += 2)  {
            u.put_in_order(&array[i]);        // insert 0, 2, 4, 6, 8
        }

        for (i = 0; i < 10; i++)  {
            p = u.search(i);
            w_assert9(p && p->i == i);
        }

        {
            w_list_i<elem3_t, unsafe_list_dummy_lock_t> iter(u);
            for (i = 0; i < 10; i++)  {
                p = iter.next();
                w_assert9(p && p->i == i);
            }
            w_assert9(iter.next() == 0);
        }

        p = u.first();
        w_assert9(p && p->i == 0);

        p = u.last();
        w_assert9(p && p->i == 9);

        for (i = 0; i < 10; i++)  {
            p = u.first();
            w_assert9(p && p->i == i);
            p = u.pop();
            w_assert9(p && p->i == i);
        }

        p = u.pop();
        w_assert9(!p);
    }

    {
        elem_descend_list_t d(W_KEYED_ARG(elem3_t, i, link), unsafe_nolock);

        for (i = 0; i < 10; i += 2)  {
            d.put_in_order(&array[9 - i]);        // insert 9, 7, 5, 3, 1
        }
    
        for (i = 0; i < 10; i += 2)  {
            d.put_in_order(&array[i]);        // insert 0, 2, 4, 6, 8
        }

        for (i = 0; i < 10; i++)  {
            p = d.search(i);
            w_assert9(p == &array[i]);
        }

        {
            w_list_i<elem3_t, unsafe_list_dummy_lock_t> iter(d);
            for (i = 0; i < 10; i++)  {
                p = iter.next();
                w_assert9(p && p->i == 9 - i);
            }
            w_assert9(iter.next() == 0);
        }

        p = d.first();
        w_assert9(p && p->i == 9);

        p = d.last();
        w_assert9(p && p->i == 0);

        for (i = 0; i < 10; i++)  {
            p = d.first();
            w_assert9(p && p->i == 9 - i);
            p = d.pop();
            w_assert9(p && p->i == 9 - i);
        }

        p = d.pop();
        w_assert9(!p);
    }

    return 0;
}

#ifdef EXPLICIT_TEMPLATE
template class w_ascend_list_t<elem3_t, unsafe_list_dummy_lock_t, int>;
template class w_descend_list_t<elem3_t, unsafe_list_dummy_lock_t, int>;
template class w_keyed_list_t<elem3_t, unsafe_list_dummy_lock_t, int>;
template class w_list_t<elem3_t, unsafe_list_dummy_lock_t>;
template class w_list_i<elem3_t, unsafe_list_dummy_lock_t>;
#endif

