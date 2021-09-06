/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore'>

 $Id: keyed.cpp,v 1.30 2010/05/26 01:20:39 nhall Exp $

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

#define SM_SOURCE
#define KEYED_C

#ifdef __GNUG__
#   pragma implementation
#endif

#include <sm_int_1.h>
#include <keyed.h>

MAKEPAGECODE(keyed_p, page_p)

/*--------------------------------------------------------------*
 *  keyed_p::shift()                                                *
 *--------------------------------------------------------------*/
rc_t
keyed_p::shift(int idx, keyed_p* rsib)
{
    w_assert1(idx >= 0 && idx < nrecs());

    int n = nrecs() - idx;
    vec_t* tp = new vec_t[n];
    w_assert1(tp);
    
    for (int i = 0; i < n; i++) {
        tp[i].put(page_p::tuple_addr(1 + idx + i),
                  page_p::tuple_size(1 + idx + i));
    }
    
    /*
     *  insert all of tp into slot 1 of rsib 
     *  (slot 0 reserved for header)
     */
    rc_t rc = rsib->insert_expand(1, n, tp); // logged
    if (! rc.is_error())  {
        rc = remove_compress(1 + idx, n);
    }

    delete[] tp;

    return rc.reset();
}

rc_t
keyed_p::set_hdr(const cvec_t& data)
{
    W_DO( page_p::overwrite(0, 0, data) );
    return RCOK;
}

/*--------------------------------------------------------------*
 *  keyed_p::insert()                                                *
 *        Insert a <key, el> pair into a particular slot of a        *
 *        keyed page.                                                 *
 *--------------------------------------------------------------*/
rc_t
keyed_p::insert(
    const cvec_t& key,                // the key to be inserted
    const cvec_t& el,                // the element associated with key
    int slot,                        // the interesting slot
    shpid_t child)
{
    keyrec_t::hdr_s hdr;
    hdr.klen = key.size();
    hdr.elen = el.size();
    hdr.child = child;

    vec_t vec;
    vec.put(&hdr, sizeof(hdr)).put(key).put(el);
    
    W_DO( page_p::insert_expand(slot + 1, 1, &vec) ); // logged

    return RCOK;
}

/*--------------------------------------------------------------*
 *  keyed_p::remove()                                                *
 *--------------------------------------------------------------*/
rc_t
keyed_p::remove(int slot)
{
    W_DO( page_p::remove_compress(slot + 1, 1) );
    return RCOK;
}

/*--------------------------------------------------------------*
 *  keyed_p::format()                                                *
 *--------------------------------------------------------------*/
rc_t
keyed_p::format(const lpid_t& /*pid*/, tag_t /*tag*/, 
        uint4_t /*flags*/, store_flag_t /* store_flags */)
{
    /* 
     *  keyed_p is never instantiated individually. it is meant to
     *  be inherited.
     */
    w_assert1(eINTERNAL);
    return RCOK;
}

rc_t
keyed_p::format(const lpid_t& pid, tag_t tag,
                uint4_t flags, 
                store_flag_t store_flags,
                const cvec_t& hdr)
{
    w_assert3(tag == page_p::t_rtree_p
            || tag == page_p::t_keyed_p
    );

    /* don't log : last arg -> false means don't log  */
    W_DO( page_p::_format(pid, tag, flags, store_flags) );
    W_COERCE( page_p::insert_expand(0, 1, &hdr, false/*logit*/) );

    /* Now, log as one (combined) record: */
    rc_t rc = log_page_format(*this, 0, 1, &hdr); // keyed_p, rtree_p
    return rc;
}

