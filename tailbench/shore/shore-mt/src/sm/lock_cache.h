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

// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='LOCK_X_H'>

 $Id: lock_cache.h,v 1.1 2010/06/15 17:30:07 nhall Exp $

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

#ifndef LOCK_CACHE_H
#define LOCK_CACHE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

struct lock_cache_elem_t : public w_base_t {
    lockid_t                lock_id;
    lock_base_t::lmode_t    mode;
    lock_request_t*         req;

    lock_cache_elem_t()
    : mode(NL),
      req(0)
    {
    }


    const lock_cache_elem_t &operator=(const lock_cache_elem_t &r)
    {
        lock_id = r.lock_id;
        mode = r.mode;
        req = r.req;
        return *this;
    }

    void clear() {
        mode = NL; // NL == invalid
    }

    void dump() const {
        if(mode != NL) {
            cout << "\tlock_id " << lock_id << " mode " << mode << endl;
        }
    }

private:
    // disabled
    lock_cache_elem_t(const lock_cache_elem_t &);
};
    

template <int S, int L>
class lock_cache_t : public w_base_t {
    lock_cache_elem_t        buf[S][L];
public:
    void dump() const {
        for(int j=0; j < L; j++) 
        for(int i=0; i < S; i++) buf[i][j].dump();
    }
    void reset() {
        // for(int i=0; i < S; i++) buf[i].lock_id.zero();
        // zeroing out the lock id doesn't change the mode and
        // thereby make the slot available!
        for(int j=0; j < L; j++) 
        for(int i=0; i < S; i++) buf[i][j].clear();
    }
    lock_cache_elem_t* probe(const lockid_t& id, int l) {
        // probe a single bucket. Caller should verify its contents
        uint4_t idx = w_hash(id);
        return  &buf[idx % S][l];
    }
    lock_cache_elem_t* search(const lockid_t& id) {
        // probe a single bucket. If it fails, oh well.
        lock_cache_elem_t* p = probe(id, id.lspace());
        return (p->lock_id == id && p->mode != NL)? p : NULL;
        //If probe finds one with a different (presumably higher-in
        //hierarcy) lspace, we return NULL.
    }

    // Remove from the table all locks subsumed by this one.
    // To make this a little more easily, we've made the table into an array of
    // hash tables.
    void compact(const lockid_t &_l) 
    {
        for (int k = _l.lspace() + 1; k <= lockid_t::cached_granularity; k++)
        {
            for(int i=0; i < S; i++) {
                lock_cache_elem_t *p = &buf[i][k];
                if(p->mode == NL) continue;
                lockid_t l(p->lock_id);
                l.truncate(_l.lspace());
                if(l == _l) {
                    p->clear(); // make it available
                }
            }
        }
    }
    void compact() {
        // do nothing...
        for(int j=0; j < L; j++) 
        for(int i=0; i < S; i++) compact(buf[i][j].lock_id);
    }
    bool put(const lockid_t& id, lock_base_t::lmode_t m, 
                lock_request_t* req, lock_cache_elem_t &victim) 
    {
        lock_cache_elem_t* p = probe(id, id.lspace());
        bool evicted = true;
        // mode is NL means it's an empty slot.
        if(p->mode != NL) {
            // don't replace entries that are higher in the hierarchy!
            // Hash *might* take us to an that which happens to
            // subsume our lock.
            if(p->lock_id.lspace() >= id.lspace())
            {
                // Element in table has equal or higher granularity.
                // Replace it.
                victim = *p;
            } else {
                // item we're trying to insert has
                // higher granularity.  Don't insert.
                victim.lock_id = id;
                victim.req = req;
                victim.mode = m;
                return true; // never mind...
                // make it look like the one we requested was
                // evicted b/c it's subsumed by the entry found
                // in the table, although the modes might differ.
                // OR it's a hash collision and the
                // lower-granularity lock wins.
            }
        } else {
            // empty slot. Use it. Didn't evict anyone.
            evicted = false;
        }
    
        p->lock_id = id;
        p->req = req;
        p->mode = m;
        return evicted;
    }
};


#endif
