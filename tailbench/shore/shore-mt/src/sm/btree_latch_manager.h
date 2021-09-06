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

/*<std-header orig-src='shore' incl-file-exclusion='BTREE_LATCH_MANAGER_H'>

 $Id: btree_latch_manager.h,v 1.303.2.15 2010/03/25 18:05:15 nhall Exp $

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

#ifndef BTREE_LATCH_MGR_H
#define BTREE_LATCH_MGR_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef BASICS_H
#include "basics.h"
#endif

#ifndef SM_S_H
#include "sm_s.h"
#endif

#ifndef LATCH_H
#include "latch.h"
#endif

#include <map>

/**\brief  Manages a set of latches, one per btree root.
 *
 * \details
 *  To be used for the MRBT design to allow multiple SMOs
 *  for an index. (One SMO at a time for each sub-tree)
 *
 *  Used by the btree implementation.
 *
 *  When a sub-tree is merged with another one, when the
 *  store of the index is destroyed or its volume dismounted,
 *  the latch should be removed from the map and destroyed.
 */
class btree_latch_manager 
{
private:
    struct lpid_cmp {
        bool operator() (const lpid_t &lhs, const lpid_t &rhs) const
        { return (lhs._stid.vol < rhs._stid.vol)  
		|| ( (lhs._stid.vol == rhs._stid.vol) && (lhs._stid.store < rhs._stid.store) )
		|| ( (lhs._stid.vol == rhs._stid.vol) && (lhs._stid.store == rhs._stid.store) 
		     && (lhs.page < rhs.page) ); }
    };
    typedef std::map<lpid_t, latch_t*, lpid_cmp> btree_latch_map;

    long _socket_count;

    btree_latch_map* _socket_latches;
    btree_latch_map _latches ;
    
#define USE_OCC_LOCK_HERE 1
#ifdef USE_OCC_LOCK_HERE
    typedef occ_rwlock Lock;
#else
    typedef queue_based_lock_t Lock;
#endif
    Lock* _socket_latch_locks;
    Lock _latch_lock;

    void _destroy_latches(lpid_t const &root) ; // worker, assumes have lock

public:
    // Return a ref to the per-btree latch. If none exists, creates one.
    latch_t &find_latch(lpid_t const &root, bool const bIgnoreLatches = false) ;
    // Destroy latch for the btree with root.
    void destroy_latches(lpid_t const &root) ; // on btree_merge
    // Destroy all latches for btrees on this store.
    void destroy_latches(stid_t const &store) ; 
    // Clean up for shutting down storage manager.
    void shutdown() ;
    btree_latch_manager();
    ~btree_latch_manager() ;
}; 

extern btree_latch_manager btree_latches;

#endif
