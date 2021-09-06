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
/*<std-header orig-src='shore'>

 $Id: lock.cpp,v 1.151 2010/06/15 17:30:07 nhall Exp $

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
#define LOCK_C

#ifdef __GNUG__
#pragma implementation "lock.h"
#endif

#include <sm_int_1.h>
#include <lock_x.h>
#include <lock_core.h>
#include <new>

#define W_VOID(x) x

#ifdef EXPLICIT_TEMPLATE
template class w_list_i<lock_request_t>;
template class w_list_t<lock_request_t>;
template class w_list_i<lock_head_t>;
template class w_list_t<lock_head_t>;
template class w_list_t<XctWaitsForLockElem>;
template class w_list_i<XctWaitsForLockElem>;
#endif


lock_m::lock_m(int sz)
{
    _core = new lock_core_m(sz);
    w_assert1(_core);
}


void
lock_m::assert_empty() const
{
    _core->assert_empty();
}

lock_m::~lock_m()
{
    assert_empty();
    delete _core;
}


extern "C" void lock_dump_locks();
void lock_dump_locks() { 
    smlevel_0::lm->dump(cerr);
    cerr << flushl;
}
void lock_m::dump(ostream &o)
{
    _core->dump(o);
}

bool 
lock_m::get_parent(const lockid_t& c, lockid_t& p)
{
    switch (c.lspace()) {
        case lockid_t::t_vol:
            // no parent
        case lockid_t::t_extent:
            // no parent
            p.zero(); // sets type to t_bad
            break;
        case lockid_t::t_store:
            // parent of store is volume
            new (&p) lockid_t(c.vid());
            break;
        case lockid_t::t_page: {
            // parent of page is store
            stid_t s;
            c.extract_stid(s);
            new (&p) lockid_t(s);
            }
            break;
        case lockid_t::t_kvl: {
            // parent of kvl is store?
            stid_t s;
            c.extract_stid(s);
            new (&p) lockid_t(s);
            }
            break;
        case lockid_t::t_record: {
            // parent of record is page
            lpid_t pg;
            c.extract_lpid(pg);
            new (&p) lockid_t(pg);
            }
            break;
        case lockid_t::t_user1:
            // no parent
            p.zero();
            break;
        case lockid_t::t_user2: {
            lockid_t::user1_t u;
            c.extract_user1(u);
            new (&p) lockid_t(u);
            }
            break;
        case lockid_t::t_user3:  {
            lockid_t::user2_t u;
            c.extract_user2(u);
            new (&p) lockid_t(u);
            }
            break;
        case lockid_t::t_user4:  {
            lockid_t::user3_t u;
            c.extract_user3(u);
            new (&p) lockid_t(u);
            }
            break;
        default:
            W_FATAL(eINTERNAL);
    }
    DBGTHRD(<< "child:" << c << "  parent: " << p );
#ifdef W_TRACE
    bool b =  p.lspace() != lockid_t::t_bad;
    DBGTHRD(<< "get_parent returns " << b
        << " for parent type " << int(p.lspace()) );
#endif
    return p.lspace() != lockid_t::t_bad;
}

bool lock_m::sli_query(lockid_t const &n)
{
    xct_t *        xd = xct();
    bool rval = false;
    if (n.lspace() <= lockid_t::t_page && xd 
	&& xd->lock_cache_enabled())  {
	xct_lock_info_t* const theLockInfo = xd->lock_info();
	W_COERCE(theLockInfo->lock_info_mutex.acquire());

	lock_cache_elem_t* e = _core->search_cache(theLockInfo, n, true);
	rval = (e && e->req->_sli_status == sli_active);
	W_VOID(theLockInfo->lock_info_mutex.release());
    }
    return rval;
}

rc_t lock_m::query(
    const lockid_t&     n,
    lmode_t&            m,
    const tid_t&        tid,
    bool                implicit,
    bool                cache_only)
{
    DBGTHRD(<<"lock_m::query for lock " << n);
    xct_t *        xd = xct();
    w_assert9(!implicit || tid != tid_t::null);

    INC_TSTAT(lock_query_cnt);
    if (!implicit) {
        m = NL;

        // search the cache first, if you can

        if (n.lspace() <= lockid_t::t_page && xd 
                && tid == xd->tid() && xd->lock_cache_enabled())  {
            xct_lock_info_t* const theLockInfo = xd->lock_info();
            W_COERCE(theLockInfo->lock_info_mutex.acquire());

            lock_cache_elem_t* e = _core->search_cache(theLockInfo, n);
            if (e) {
                m = e->mode;
                W_VOID(theLockInfo->lock_info_mutex.release());
                return RCOK;
            }
            W_VOID(theLockInfo->lock_info_mutex.release());
        }

        // cache was not searched, or search failed
        if(cache_only)
          return RCOK;

        if (tid == tid_t::null) {
            lock_head_t* lock = _core->find_lock_head(n, false);//do not create
            if (lock) {
                // lock head mutex was acquired by find_lock_head
                m = lock->granted_mode;
                MUTEX_RELEASE(lock->head_mutex);
            }
            return RCOK;
        }
        w_assert2(xd);

        lock_request_t* req = 0;
        lock_head_t* lock = _core->find_lock_head(n, false); // do not create
        if (lock) {
            // lock head mutex was acquired by find_lock_head
            req = lock->find_lock_request(xd->lock_info());
        }
        if (req) {
            m = req->mode();
            MUTEX_RELEASE(lock->head_mutex);
            return RCOK;
        }

        if (lock)
            MUTEX_RELEASE(lock->head_mutex);
        return RCOK;

    } else {
      rc_t result =  _query_implicit(n, m, tid, cache_only);
      return result;
    }
    return RCOK;
}


rc_t lock_m::_query_implicit(
    const lockid_t&     n,
    lmode_t&            m,
    const tid_t&        tid,
    bool                cache_only)
{
    w_assert1(tid != tid_t::null);
    m         = NL;
    xct_t  *xd = xct();
    lockid_t  path[lockid_t::NUMLEVELS];
    bool   SIX_found = false;
    int    pathlen = 1;

    DBGTHRD(<<"lock_m::_query_implicit for lock " << n);

    path[0] = n;

    while (pathlen < lockid_t::NUMLEVELS && 
        get_parent(path[pathlen-1], path[pathlen])) {
        pathlen++;
    }

    // search the cache first, if you can
    xct_lock_info_t* const        theLockInfo        = xd->lock_info();

    if (xd && tid == xd->tid() && xd->lock_cache_enabled())  {
        W_COERCE(theLockInfo->lock_info_mutex.acquire());

        for (int curr = pathlen-1; curr >= 0; curr--) {
            if (path[curr].lspace() <= lockid_t::t_page) {
                if (lock_cache_elem_t* e = _core->search_cache(
                            theLockInfo, path[curr]))  {
                    lmode_t cm = e->mode;
                    if (curr == 0) {
                        w_assert9(path[curr] == n);
                        if (SIX_found)
                            m = supr[SH][cm];
                        else
                            m = cm;
                        W_VOID(theLockInfo->lock_info_mutex.release());
                        return RCOK;
                    } else {
                        if (cm == SH || cm == UD || cm == EX) {
                            m = cm;
                        } else if (cm == SIX) {
                            SIX_found = true;
                            continue;
                        } else {
                            continue;
                        }
                        W_VOID(theLockInfo->lock_info_mutex.release());
                        return RCOK;
                    }
                } else {
                    break;
                }
            }
        }
        W_VOID(theLockInfo->lock_info_mutex.release());
    }

    // cache was not searched, or search failed
    if(cache_only)
      return RCOK;
    
    SIX_found = false;

    for (int curr = pathlen-1; curr >= 0; curr--) {
        if (lock_head_t* lock = 
            _core->find_lock_head(path[curr], false)//don't create one if not found
            )  
        {
            // lock head mutex was acquired by find_lock_head

            DBGTHRD(<<" lock " << lock << "=" << *lock);
            lock_request_t* req = lock->find_lock_request(theLockInfo);            
            if (req)  {
                w_assert9(req);
                DBGTHRD(<<" req " << req << "=" << *req);
                if (curr == 0) {
                    w_assert9(path[curr] == n);
                    if (SIX_found)
                        m = supr[SH][req->mode()];
                    else
                        m = req->mode();
                    MUTEX_RELEASE(lock->head_mutex);
                    return RCOK;
                } else {
                    if (req->mode() == SH || req->mode() == UD || req->mode() == EX) {
                        m = req->mode();
                        MUTEX_RELEASE(lock->head_mutex);
                        return RCOK;
                    } else if (req->mode() == SIX) {
                        SIX_found = true;
                        m = SH;        // the mode we are looking for is at least SH;
                                    // we must look deeper in case there is higher
                                    // mode
                    }
                }
            }
            MUTEX_RELEASE(lock->head_mutex);
        }
    }

    return RCOK;
}

rc_t
lock_m::open_quark()
{
    W_DO(lm->_core->open_quark(xct()));
    return RCOK;
}


rc_t
lock_m::close_quark(bool release_locks)
{
    W_DO(lm->_core->close_quark(xct(), release_locks));
    return RCOK;
}

void lock_m::disable_sli(xct_lock_info_t* theLockInfo) {
    /* release any inherited locks. note that the list has newest
       locks at the front so a forward iterator preserves the
       hierarchical locking protocol.
    */
    W_COERCE(theLockInfo->lock_info_mutex.acquire());
    request_list_i it(theLockInfo->sli_list);
    while(lock_request_t* req=it.next()) 
	_core->sli_abandon_request(req, NULL);

    W_COERCE(theLockInfo->lock_info_mutex.release());
    theLockInfo->_sli_enabled = false;
}


rc_t
lock_m::_lock(
    const lockid_t&         _n,
    lmode_t                 m,
    lmode_t&                prev_mode,
    lmode_t&                prev_pgmode,
    duration_t              duration,
    timeout_in_ms           timeout, 
    bool                    force,
    lockid_t**              nameInLockHead
#ifdef SM_DORA
    , const bool bIgnoreParents
#endif
    )
{
    FUNC(lock_m::_lock);
    w_rc_t                 rc; // == RCOK
    xct_t*                 xd = xct();
    bool                   acquired = false;
    xct_lock_info_t*       theLockInfo = 0;
    lock_request_t*        theLastLockReq = 0;
    lockid_t               n = _n;

    INC_TSTAT(lock_request_cnt);

#ifdef W_TRACE
    DBGTHRD(<< "lock " << n 
        << " mode=" << int(m) << " duration=" << int(duration) 
        << " timeout=" << timeout << " force=" << force );
    if (xd)  {
        DBGTHRD( << " xct=" << *xd );
    }  else  {
        DBGTHRD( << " NO XCT" );
    }
#endif

    prev_mode = NL;
    prev_pgmode = NL;

    w_assert9(n.lspace() != lockid_t::t_extent || 
                duration == t_long && (m == EX || m == IX));

    switch (timeout) {
        case WAIT_SPECIFIED_BY_XCT:
            // If there's no xct, use thread (default is WAIT_FOREVER)
            if (xd) {
                timeout = xd->timeout_c();
                break;
            }
            // DROP THROUGH to WAIT_SPECIFIED_BY_THREAD ...
            // (whose default is WAIT_FOREVER)

        case WAIT_SPECIFIED_BY_THREAD:
            timeout = me()->lock_timeout();
            break;
    
        default:
            break;
    }

    w_assert9(timeout >= 0 || timeout == WAIT_FOREVER);

    if (xd) {
        // The lock info is created with the xct constructor.
        theLockInfo = xd->lock_info();

        // This ensures that no two threads can be locking
        // on behalf of the same xct at the same time.
        W_COERCE(theLockInfo->lock_info_mutex.acquire());
        
        // Where in the hierarchy
        // is the lock requested relative to the xct's
        // escalation level ?    If it's finer than the
        // xct's locking level, then make it a coarser lock.
        if(n.lspace() >= theLockInfo->lock_level() && 
                n.lspace() != lockid_t::t_extent && 
                n.lspace() != lockid_t::t_kvl &&
                !n.is_user_lock()) {
            n.truncate(theLockInfo->lock_level());
        }

        acquired=true;

        // check to see if the exact lock we need is 
        // in the cache, if so we are done.
        if (n.lspace() <= lockid_t::t_page && xd->lock_cache_enabled()) 
        {
            if (lock_cache_elem_t* e = _core->search_cache(
                        theLockInfo, n, true))  {
                lmode_t cm = e->mode;
                if (cm == supr[m][cm]) {
                    prev_mode = cm;
                    if (n.lspace() == lockid_t::t_page)  {
                        prev_pgmode = cm;
                    }
                    theLastLockReq = e->req;
                    INC_TSTAT(lock_cache_hit_cnt);
                    goto done; // release mutex
                }
            }
        }

        lockid_t* h = xd->lock_info_hierarchy();
        w_assert9(h);
        h[0] = n;

        lmode_t pmode = parent_mode[m];
        lmode_t* hmode[lockid_t::NUMLEVELS];
        hmode[0] = 0;

        // check to see if the correct lock is held on the parent(n) 
        // to volume(n) in the cache.
        // h[i] will be the name of the nearest ancestor held in the 
        // correct mode on exit.
        // as a side effect h[] will be initialized with the names 
        // of the ancestors.
        int i = 1;

#ifdef SM_DORA
        if (!bIgnoreParents) {
#endif

        if (xd->lock_cache_enabled() && !n.is_user_lock()) {
            for (i = 1; i < lockid_t::NUMLEVELS; i++)  {

                if (!get_parent(h[i-1], h[i]))
                    break;
                DBGTHRD(<< "  get_parent true" << h[i-1] << " " << h[i]);

                hmode[i] = 0;
                {
                lock_cache_elem_t* e = _core->search_cache(
                        theLockInfo, h[i], true);
                hmode[i] = e ? &e->mode : 0;

                if (hmode[i] && supr[*hmode[i]][pmode] == *hmode[i])  {
                    // cache hit
                    DBGTHRD(<< "  " 
                            << h[i] 
                            << "(mode=" << int(*hmode[i]) << ") cache hit");
                    theLastLockReq = e->req;
                    INC_TSTAT(lock_cache_hit_cnt);

                    if (h[i].lspace() == lockid_t::t_page)
                        prev_pgmode = *hmode[i];

                    if (! force)  {
                        lmode_t held = *hmode[i];
                        if (held == SH || held == EX || 
                                held == UD || (held == SIX && m == SH))  {
                            if (held == SIX && n.lspace() > h[i].lspace()) {
                                // need to release the mutex, since 
                                // _query_implicit acquires it
                                // and it also makes it safe to 
                                // use W_DO macros
                                W_VOID(theLockInfo->lock_info_mutex.release());
                                acquired = false;

                                W_DO(_query_implicit(n, prev_mode, xd->tid()));
                                if (n.lspace() == lockid_t::t_record && 
                                        h[i].lspace() < lockid_t::t_page)  {
                                    lpid_t        tmp_lpid;
                                    n.extract_lpid(tmp_lpid);
                                    lockid_t tmp_lockid(tmp_lpid);
                                    W_DO( _query_implicit(tmp_lockid, 
                                                prev_pgmode, xd->tid()) );
                                }  else  {
                                    prev_pgmode = prev_mode;
                                }
                            } else {
                                prev_mode = held;
                                prev_pgmode = held;
                            }
                            goto done;
                        }
                    }
                    break;
                }
#if W_DEBUG_LEVEL > 2
                else if (hmode[i])  {
                    DBGTHRD(<< "  " 
                    << h[i] 
                    << "(mode=" << int(*hmode[i]) 
                            << ") cache hit/wrong mode");
                }  else  {
                  DBGTHRD(<< "  " << h[i] << " cache miss");
                }
#endif 
                }
            }
        }  else  {
            DBGTHRD(<< "  Lock cache disabled or is user lock. Initialize h[]");
            // just initialize h[] when the cache is disabled
            for (i = 1; i < lockid_t::NUMLEVELS; i++)  {
                if (!get_parent(h[i-1], h[i]))
                    break;
                DBGTHRD(<< "  get_parent true" << h[i-1] << " " << h[i]);
            }
        }

#ifdef SM_DORA
        }
#endif
        DBGTHRD(<< "  i" << i << " is closest correctly-held ancestor :" << h[i]);
        DBGTHRD(<< "Entire array:");
        for(int ii=0;  ii < lockid_t::NUMLEVELS; ii++ ) {
            // note: h[0] is n, the requested lock
            // note: h[i+1] is the parent of h[i]
            DBGTHRD(<< "  h[" << ii << "] = " << h[ii]);
        }
        if(theLastLockReq) {
            DBGTHRD(<< "theLastLockReq :" << *theLastLockReq);
        }

        w_assert3(i < lockid_t::NUMLEVELS || (i == lockid_t::NUMLEVELS && 
                 (h[i-1].lspace() == lockid_t::t_vol || 
                  h[i-1].lspace() == lockid_t::t_user1)));
        
        // If the transaction is running during a quark, the
        // locks should be short duration so that the quark can
        // unlock them.  Extents locks need to be held long term
        // always for the space management to work.
        if (theLockInfo->in_quark_scope() && n.lspace() != lockid_t::t_extent) {
            if (duration > t_short) {
                duration = t_short;
            }
        }

        // do the locking protocol.  h[i] is the name of the 
        // closest ancestor which is
        // held in the correct mode from the cache.  
        // so obtain locks on names h[i-1] to
        // h[0] (which is n).
        for (int j = i - 1; j >= 0; j--)  {
            lmode_t                ret;
            lmode_t                need = (j == 0 ? m : pmode);
            lock_request_t*        req = 0;

            DBG(<< "level j=" << j << " need mode " << need);

            w_rc_t::errcode_t rce(eOK);
            do {
                rce = _core->acquire_lock(xd, 
                        h[j], /* lockid_t in the hierarchy */
                        NULL /* lock */, 
                        &req, /* out: request */
                        need,  /* needed mode */
                        prev_mode,  /* out: previously-held mode */
                        duration, 
                        timeout, 
                        ret /* out: new mode */
                        );
            } while (rce && (rce == eRETRY));

#if W_DEBUG_LEVEL >= 8
            if (rce && rce == eDEADLOCK) {
                w_ostrstream s;
                s  << _n << " mode " << m;
                fprintf(stderr, 
                    "Deadlock detected acquiring %s\n", s.c_str());
            }
#endif
            if (rce) {
                rc = RC(rce);
                goto done;
            }

            // save the previous mode for the page lock when we get to the page
            // level in the hierarchy
            if (h[j].lspace() == lockid_t::t_page)
                prev_pgmode = prev_mode;

            bool brake = (ret == SH || ret == EX || ret == UD); 

            if (duration == t_instant) {
#if W_DEBUG_LEVEL > 0
                w_assert1(MUTEX_IS_MINE(theLockInfo->lock_info_mutex));
                // this check is for trying to track down gnats 115,
                // wherein Ryan says that we get unwanted escalation.
                // I'm trying to be sure that t_instant locks aren't
                // affecting the situation.
                // It should be impossible for two threads of an xct
                // to acquire t_instant locks at the same time, therefore
                // driving up the ref count and making the instant locks
                // hang around.  This assert is part of checking that.
                if(prev_mode == NL) {
                    w_assert1(req->get_count() == 1);
                }
#endif
                W_COERCE( _core->release_lock(theLockInfo, h[j], 
                            req->get_lock_head(), req, false) );

            } else if (duration >= t_long && 
                    xd->lock_cache_enabled() && !n.is_user_lock())  
            {
                if (hmode[j])
                    *hmode[j] = ret;
                else if (h[j].lspace() <= lockid_t::t_page)  {
                    _core->put_in_cache(theLockInfo, h[j], ret, req);
                    lock_cache_elem_t* e = _core->search_cache(
                            theLockInfo, h[j]);
                    hmode[j] = e? &e->mode : 0;
                }

                // check if lock escalation needs to be performed
                if (prev_mode != ret && theLastLockReq)  {
                    if (theLastLockReq->num_children() >= dontEscalate)  {
                        if (theLastLockReq->num_children() == dontEscalate)  {
                            req->set_num_children ( dontEscalate );
                        }
                    }  else  {
                        // prev_mode == NL means we just acquired this lock
                        // for the first time, "this lock" meaning h[j] for
                        // whatever j we are now addressing.
                        if (prev_mode == NL)
                            theLastLockReq->inc_num_children();

                        int4_t threshold = 
                            xd->GetEscalationThresholdsArray()[h[j + 1].lspace()];
                        if (threshold < dontEscalate && 
                                theLastLockReq->num_children() >= threshold)  {
                            // time to escalate, get the parent in the proper 
                            // explicit mode
                            // do it by adjusting the pmode and j, and continuing.
                            lmode_t new_pmode = (m == SH || m == IS) ? SH : EX;
                            lmode_t new_prev_mode = NL;

                            if (!force || 
                                new_pmode != supr[new_pmode][theLastLockReq->mode()])  {
                                // don't do  for the case where we already have 
                                // the parent in the correct mode, this can only 
                                // happen when force=true

                                DBG( 
                                        << "escalating from " << n 
                                        << "(mode=" << int(m) 
                                        << ") to " << h[j+1] 
                                        << "(mode="
                                         << int(*hmode[j+1]) 
                                         << "->" 
                                         << int(new_pmode) << ")");

                                w_rc_t::errcode_t       escalate_rc(eOK);
                                lock_request_t*         escalate_req = 0;

                                do {
                                    escalate_rc = _core->acquire_lock(xd, 
                                            h[j+1], 0, 
                                            &escalate_req, new_pmode,
                                            new_prev_mode, duration, 0, ret);
                                }  while (escalate_rc && 
                                        escalate_rc == eRETRY);

                                if (!escalate_rc)  {
                                    brake = (ret == SH || ret == EX || ret == UD);

                                    if (duration >= t_long && xd->lock_cache_enabled())  {
                                        if (hmode[j+1])
                                            *hmode[j+1] = ret;
                                        else if (h[j+1].lspace() <= lockid_t::t_page)  {
                                            _core->put_in_cache(theLockInfo, h[j+1], 
                                                    ret, escalate_req);
                                        }
                                    }

                                    switch (h[j + 1].lspace())  {
                                        case lockid_t::t_page:
                                            INC_TSTAT(lock_esc_to_page);
                                            break;
                                        case lockid_t::t_store:
                                            INC_TSTAT(lock_esc_to_store);
                                            break;
                                        case lockid_t::t_vol:
                                            INC_TSTAT(lock_esc_to_volume);
                                            break;
                                        default:
                                            // shouldn't get here
                                            w_assert1(false);
                                            break;
                                    }
                                }
                            }
                        }
                    }
                }
                theLastLockReq = req;

                if (h[j].lspace() <= lockid_t::t_page && (brake || ret == SIX)){
                    // remove from the cache the locks subsumed by
                    // this lock. This was originally done in order
                    // not to waste space in the limited-sized cache.
                    // It still makes sense to do this if 
                    // escalation is being used.
                    _core->compact_cache(theLockInfo, h[j]);

                    for (int k = j-1; k >= 0; k--)
                        hmode[k] = NULL;
                }
            } 
            if (! force && (brake || (ret == SIX && m == SH))) {
                if (h[j].lspace() < lockid_t::t_page)
                    prev_pgmode = prev_mode;
                break;
            }
        }
    } else {
        // Tan thinks this might cause a problem during recovery (when
        // (de)allocating pages.  So far it hasn't. 
        // Tan: as predicted, this has shown up in btree undo with KVL.
        // W_FATAL(eNOTRANS); 

        /* To expound on the above comment...
         * There is an implicit layering in the sm, below which
         * locks are not (in most cases) acquired.  Take file.cpp for
         * example: when entering from the ss_m api, the procedure
         * prologue at the ss_m layer checks for a running transaction
         * where necessary.   All locks are acquired in the top half of
         * the file.cpp code or above that.  Once you get down to file_p
         * code, it is assumed that all needed locks are already had,
         * and only latches, page updates, and logging are performed.
         * On rollback and redo, only the file_p half of the file code is 
         * called.  Thus, for a while it was thought that locks were never
         * acquired outside a transaction,  or during redo,
         * since redo only called the lower layers of code.  That's
         * why the W_FATAL(eNOTRANS); was stuck in in the first place.
         * ...however...
         * Where logical logging is done (btrees, i/o layer), this 
         * is not the case -- when pages are allocated & deallocated,
         * which might happen in btree undo(), locks are acquired. 
         * That's why the "Tan" comments were added, and the W_FATAL(eNOTRANS)
         * was commented-out.
         * Of course, in redo we don't have to acquire the lock even though
         * we call this method.
         *
         * Also note that we could acquire a lock for
         * page alloc/dealloc in undo of a file page allocation, as happens
         * in gnats 77 scenario.
         */
        w_assert2(smlevel_0::in_recovery_redo());
    }
done:
    if (acquired)  {
        w_assert9(xd != 0);
        w_assert9(theLockInfo != 0);
        W_VOID(theLockInfo->lock_info_mutex.release());

        if (!rc.is_error() && nameInLockHead)  {
            /* XXX This is a problem, it happens! */
            w_assert3(theLastLockReq);
            *nameInLockHead = &theLastLockReq->get_lock_head()->name;
        }
    }
    return rc;
}


rc_t
lock_m::unlock(const lockid_t& n)
{
    FUNC(lock_m::unlock);
    DBGTHRD(<< "unlock " << n );

    xct_t*         xd = xct();
    w_rc_t         rc; // == RCOK
    if (xd)  {
        W_COERCE(xd->lock_info()->lock_info_mutex.acquire());
        lockid_t h[2];
        int c = 0;
        h[c] = n;
        do {
            DBGTHRD(<< "  while get_parent true" << h[1-c] << " " << h[c]);
            c = 1 - c;
            rc = _core->release_lock(xd->lock_info(), h[1-c], 0, 0, false);
            if (rc.is_error())
                break;
        } while (get_parent(h[1-c], h[c]));
        W_VOID(xd->lock_info()->lock_info_mutex.release());
    }

    INC_TSTAT(unlock_request_cnt);
    return rc;
}


/* 
 * Free all locks of a given duration
 *  all_less_than : if true, release not just those whose
 *     duration matches, but all those which shorter duration also
 *  dont_clean_exts
 */
rc_t lock_m::unlock_duration(
    duration_t          duration,
    bool                all_less_than, // always true, as we use it
    bool                dont_clean_exts)
{
    FUNC(lock_m::unlock_duration);
    DBGTHRD(<< "lock_m::unlock_duration" 
        << " duration=" << int(duration)
        << " all_less_than=" << all_less_than 
    );
    xct_t*         xd = xct();
    xct_lock_info_t* theLockInfo = xd->lock_info();
    w_rc_t        rc;        // == RCOK
    if (xd)  {
        do  {
            extid_t        extid;
            W_COERCE(theLockInfo->lock_info_mutex.acquire());

            rc =  _core->release_duration(theLockInfo, duration, 
                    all_less_than, dont_clean_exts ? 0 : &extid);

            W_VOID(theLockInfo->lock_info_mutex.release());

            if (rc.err_num() == eFOUNDEXTTOFREE)  {
                W_DO( smlevel_0::io->free_ext_after_xct(extid) );
                lockid_t        name(extid);
                W_COERCE(theLockInfo->lock_info_mutex.acquire());
                W_DO( _core->release_lock(theLockInfo, name, 0, 0, true) );
                W_VOID(theLockInfo->lock_info_mutex.release());
            }
        }  while (rc.err_num() == eFOUNDEXTTOFREE);
    }
    return rc;
}

// this function will prevent lock escalation from occuring on the
// object specified and its decendants.
rc_t lock_m::dont_escalate(const lockid_t& n, bool passOnToDescendants)
{
    FUNC(lock_m::dont_escalate);
    DBGTHRD( << "lock_m::dont_escalate(" << n << ", " << passOnToDescendants << ")" );

    xct_t*                xd = xct();
    lmode_t                prev_mode;
    lmode_t                ret;
    lock_request_t*        req;

    if (xd)  {
        W_COERCE(xd->lock_info()->lock_info_mutex.acquire());
        W_EDO(_core->acquire_lock(xd, n, 0, &req, NL, 
                prev_mode, t_long, xd->timeout_c(), ret));

        w_assert1(req);
        req->set_num_children(passOnToDescendants ? 
                dontEscalate : dontEscalateDontPassOn);
                W_VOID(xd->lock_info()->lock_info_mutex.release());
        return RCOK;
    }  else  {
        return RC(eNOTRANS);
    }
}

rc_t
lock_m::lock(
    const lockid_t&      n, 
    lmode_t              m,
    duration_t           duration,
    timeout_in_ms        timeout,
    lmode_t*             prev_mode,
    lmode_t*             prev_pgmode,
    lockid_t**           nameInLockHead
#ifdef SM_DORA
    , const bool bIgnoreParents
#endif
    )
{
#if W_DEBUG_LEVEL > 2
    w_assert3 (n.lspace() != lockid_t::t_bad);
#endif 

    lmode_t _prev_mode;
    lmode_t _prev_pgmode;

    rc_t rc = _lock(n, m, _prev_mode, _prev_pgmode, duration, timeout, false, nameInLockHead
#ifdef SM_DORA
                    , bIgnoreParents
#endif
                    );

    if (prev_mode != 0)
        *prev_mode = _prev_mode;
    if (prev_pgmode != 0)
        *prev_pgmode = _prev_pgmode;
    return rc;
}

rc_t
lock_m::lock_force(
    const lockid_t&      n, 
    lmode_t              m,
    duration_t           duration,
    timeout_in_ms        timeout,
    lmode_t*             prev_mode,
    lmode_t*             prev_pgmode,
    lockid_t**           nameInLockHead
#ifdef SM_DORA
    , const bool bIgnoreParents
#endif
    )
{
    lmode_t _prev_mode;
    lmode_t _prev_pgmode;

    rc_t rc = _lock(n, m, _prev_mode, _prev_pgmode, duration, timeout, true, nameInLockHead
#ifdef SM_DORA
                    , bIgnoreParents
#endif
                    );

    if (prev_mode != 0)
        *prev_mode = _prev_mode;
    if (prev_pgmode != 0)
        *prev_pgmode = _prev_pgmode;
    return rc;
}
