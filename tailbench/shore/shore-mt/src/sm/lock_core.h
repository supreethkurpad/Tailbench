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
/*<std-header orig-src='shore' incl-file-exclusion='LOCK_CORE_H'>

 $Id: lock_core.h,v 1.43 2010/06/15 17:30:07 nhall Exp $

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

#ifndef LOCK_CORE_H
#define LOCK_CORE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif


class LockCoreFunc {
 public:
    virtual ~LockCoreFunc();

    virtual void operator()(const xct_t* xct) = 0;
};


class bucket_t; // defined in lock_core.cpp

class lock_core_m : public lock_base_t{
    enum { BPB=CHAR_BIT };

public:
    typedef lock_base_t::lmode_t lmode_t;
    typedef lock_base_t::duration_t duration_t;

    NORET        lock_core_m(uint sz);
    NORET        ~lock_core_m();

    int          collect(vtable_t&, bool names_too);

    void        assert_empty() const;
    void        dump();
    void        dump(ostream &o);
    void        _dump(ostream &o);


    lock_head_t*    find_lock_head(
                const lockid_t&            n,
                bool                create);
public:
    typedef     w_list_t<lock_head_t,queue_based_lock_t> chain_list_t;
    typedef     w_list_i<lock_head_t,queue_based_lock_t> chain_list_i;

private:
    lock_head_t*    _find_lock_head_in_chain(
                chain_list_t               &l,
                const lockid_t&            n);

public:
    w_rc_t::errcode_t  acquire_lock(
                xct_t*            xd,
                const lockid_t&   name,
                lock_head_t*      lock,
                lock_request_t**  request,
                lmode_t           mode,
                lmode_t&          prev_mode,
                duration_t        duration,
                timeout_in_ms     timeout,
                lmode_t&          ret);

    rc_t        release_lock(
                xct_lock_info_t*  theLockInfo,
                const lockid_t&   name,
                lock_head_t*      lock,
                lock_request_t*   request,
                bool              force);

    bool        wakeup_waiters(lock_head_t*& lock, lock_request_t* self=NULL);

    bool        upgrade_ext_req_to_EX_if_should_free(
                lock_request_t*        req);

    rc_t        release_duration(
                xct_lock_info_t*    theLockInfo,
                duration_t        duration,
                bool            all_less_than,
                extid_t*        ext_to_free);

    rc_t        open_quark(xct_t*        xd);
    rc_t        close_quark(
                xct_t*            xd,
                bool            release_locks);

    lock_head_t*    GetNewLockHeadFromPool(
                const lockid_t&        name,
                lmode_t            mode);
    
    void        FreeLockHeadToPool(lock_head_t* theLockHead);

    enum sli_parent_cmd { RECLAIM_NO_PARENT, RECLAIM_CHECK_PARENT, RECLAIM_RECLAIM_PARENT };
    lock_request_t* sli_reclaim_request(lock_request_t* &req, sli_parent_cmd pcmd, lock_head_t::my_lock* lock_mutex);
    bool sli_invalidate_request(lock_request_t* &req);
    void sli_abandon_request(lock_request_t* &req, lock_head_t::my_lock* lock_mutex);
    void sli_purge_inactive_locks(xct_lock_info_t* theLockInfo, bool force=false);


    /* search the lock cache. if reclaim=true, attempt to reclaim the
       node (possibly failing to do so); otherwise ignore inactive
       requests.
     */
    lock_cache_elem_t*    search_cache(
                xct_lock_info_t* theLockInfo,
                lockid_t const &name,
                bool reclaim=false);

    void        put_in_cache(xct_lock_info_t* theLockInfo,
                     lockid_t const &name,
                     lock_mode_t mode,
                     lock_request_t* req);
    void        compact_cache(xct_lock_info_t* theLockInfo, 
			                        lockid_t const &name );
    
private:
    uint4_t        _table_hash(uint4_t) const; // mod it to fit table size
    w_rc_t::errcode_t _check_deadlock(xct_t* xd, bool first_time,
				      lock_request_t *myreq);
    void    _update_cache(xct_lock_info_t *theLockInfo, const lockid_t& name, lmode_t m);
    bool	_maybe_inherit(lock_request_t* request, bool is_ancestor=false);
    
    // internal version that does the actual release
    rc_t    _release_lock(lock_request_t* request, bool force);

#define DEBUG_LOCK_HASH 0
#if DEBUG_LOCK_HASH
    void               compute_lock_hash_numbers() const;
    void               dump_lock_hash_numbers() const;
#endif
    bucket_t*          _htab;
    uint4_t            _htabsz;
    int                _requests_allocated; // currently-allocated requests.
    // For further study.
};


#define ACQUIRE_BUCKET_MUTEX(i) MUTEX_ACQUIRE(_htab[i].mutex);
#define RELEASE_BUCKET_MUTEX(i) MUTEX_RELEASE(_htab[i].mutex);
#define BUCKET_MUTEX_IS_MINE(i) w_assert3(MUTEX_IS_MINE(_htab[i].mutex));


/*<std-footer incl-file-exclusion='LOCK_CORE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
