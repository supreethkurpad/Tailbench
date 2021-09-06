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

/*<std-header orig-src='shore' incl-file-exclusion='STID_T_H'>

 $Id: store_latch_manager.h,v 1.1.2.7 2010/03/19 22:19:19 nhall Exp $

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

#ifndef STORE_LATCH_MGR_H
#define STORE_LATCH_MGR_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef BASICS_H
#include "basics.h"
#endif

#ifndef STID_T_H
#include "stid_t.h"
#endif

#ifndef LATCH_H
#include "latch.h"
#endif

#include <map>

/**\brief  Manages a set of latches, one per store id.
 *
 * \details
 *  This allows us to grab per-store latches without latching a
 *  buffer-pool page, which requires a read, and consumes buffer-pool
 *  space.
 *
 *  Used by the btree implementation and by the page allocation.
 *
 *  When a store is destroyed or its volume dismounted, the
 *  latch should be removed from the map and destroyed.
 */
class store_latch_manager 
{
private:
    struct stid_cmp {
        bool operator() (const stid_t &lhs, const stid_t &rhs) const
        { return (lhs.vol < rhs.vol)  
            || ( (lhs.vol == rhs.vol) && (lhs.store < rhs.store)); }

    };
    typedef std::map<stid_t, latch_t*, stid_cmp> latch_map;
    latch_map _latches ;
#define USE_OCC_LOCK_HERE 1
#ifdef USE_OCC_LOCK_HERE
    occ_rwlock _latch_lock;
#else
    queue_based_lock_t _latch_lock;
#endif
    void _destroy_latches(stid_t const &store) ; // worker, assumes have lock

public:
    // Return a ref to the per-store latch. If none exists, creates one.
    latch_t &find_latch(stid_t const &store) ;
    // Destroy latche for this store.
    void destroy_latches(stid_t const &store) ; // on destroy_store
    // Destroy all latches for stores on this volume.
    void destroy_latches(vid_t const &volume) ; // on dismount
    // Clean up for shutting down storage manager.
    void shutdown() ;
    ~store_latch_manager() ;
}; 

extern store_latch_manager store_latches;

#endif
