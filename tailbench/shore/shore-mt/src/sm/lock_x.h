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

 $Id: lock_x.h,v 1.65 2010/06/15 17:30:07 nhall Exp $

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

#ifndef LOCK_X_H
#define LOCK_X_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/**\file lock_x.h
 *\ingroup Macros
 *
 *
This file contains declarations for classes used in implementing
the association between locks and transactions.  The important
classes are:
    lock_request_t: a transaction's request for a lock. 
    lock_cache_t: cache of transactions recent requests
    xct_lock_info_t: lock information associated with a transaction 

This file also has hooks for the implmentation of "quarks".  Quarks are
a sub-transaction locking scope.  Opening a quark begins the scope.
While a quark is open all locks acquired are recorded.  When a quark is
closed, all SH/IS/UD locks acquired since the opening of the quark can
be released.  Requests for long-duration locks during a quark are
converted to short-duration requests.  If a lock was held before
a quark was opened, closing the quark will not release the lock.

Only one quark may be open at a time for a transaction.

Because short-duration locks are not cached, yet, obtaining
locks during a quark is slower.

The user interface for quarks is sm_quark_t defined in sm.h and
implemented in sm.cpp.  The sm_quark_t::open/close methods simply
call 
lock_core_m::open/close_quark where the real work is done.

Only minor data structure additions where needed.  lock_request_t
now has a constructor which takes a bool indicating that
the request is just a special marker for the opening of
a quark.  A marking added to the lock list in xct_lock_info_t
when a quark is opened.  xct_lock_info_t also has a pointer,
quark_marker, pointing to the marker if a quark is open,
and null otherwise.

 *
 *
 */

#ifdef __GNUG__
#pragma interface
#endif

class lock_head_t;
class xct_impl; // forward
class xct_lock_info_t; // forward

/* SLI status types

   NOTE: it *is* possible to invalidate and abandon a request
   simultaneously; the final state will be sli_invalid or
   sli_abandoned depending on who finishes first.
 */
enum sli_status_t {
    sli_not_inherited,
    sli_active,
    // those above must stay above
    sli_inactive,
    // those below must stay below
    sli_invalidating,
    sli_invalid,
    sli_abandoned,
};

// typedef     w_list_t<lock_request_t,queue_based_lock_t> request_list_t;
#define request_list_t w_list_t<lock_request_t,queue_based_lock_t> 
// typedef     w_list_i<lock_request_t,queue_based_lock_t> request_list_i;
#define request_list_i w_list_i<lock_request_t,queue_based_lock_t> 

/**\brief Structure representing a lock request
 * \details
 *
 * Lock requests are strung together into two lists.
 * When a lock request is in the lock table (whether granted or not),
 * it hangs off a lock_head_t list called _queue.
 *
 * Lock requests also sit in a list hanging off the transaction that
 * made the request; this is the xct_lock_info_t's my_req_list.
 *
 * These lists are protected by locks in the objects that hold the
 * lists.  The lock manager grabs a lock in the lock_head_t to
 * traverse the requests for that lock.
 *
 */
class lock_request_t : public w_base_t {
public:
    friend class lock_core_m;
    typedef lock_base_t::lmode_t lmode_t; // common/basics.h: lock_mode_t
    typedef lock_base_t::duration_t duration_t; // in common/basics.h
    typedef lock_base_t::status_t status_t; // lock_s.h
private:
    lock_base_t::status_t  _state;        // lock state
    lmode_t           _mode;        // mode requested (and granted)
    lmode_t           _convert_mode; // if in convert wait, mode desired 
    xct_lock_info_t*  _lock_info;    // owning xct/agent. SLI makes
    smthread_t*       _thread;        // thread to wakeup when serviced 
    int               _ref_count;
    duration_t        _duration;    // lock duration
    int4_t            _num_children; // number of child objects obtained
                      // under same criteria as xct cache

public:
    /// The  thread to wake up when a request can be "serviced"
    smthread_t*       thread() const { return _thread;} 

    /// set_thread used only by acquire (attempt to acquire the lock)
    void              set_thread(smthread_t *t) { _thread=t;} 

    /// long/immediate, etc
    duration_t        get_duration() const { return _duration; } 

    // set_duration used only by acquire in event of upgrade
    void              set_duration(duration_t d) { _duration=d; } 

    // get_count used in release to make sure we don't free the
    // structure while still needed
    int               get_count() const { return _ref_count; }
    // inc_count used in acquire; we hold the lock head mutex
    int               inc_count() { return ++_ref_count; }
    // dec_count used in release; we hold the lock head mutex
    int               dec_count() { return --_ref_count; }
    w_link_t          rlink;        // link of requests in lock _queue
                      // hanging off the lock_head.
                      // protected by the lock_head mutex.

                      // the get_lock_head() trick
                      // w/ xlink.member_of() unsafe
    w_link_t          xlink;        // link for xd->_lock.list 

    int4_t            num_children() const { return _num_children;}    
    void              set_num_children(int4_t n) { _num_children=n;}    
    int4_t            inc_num_children() { return ++_num_children; }    

    /* Speculative lock inheritance rules:

       Every request must be either in a transaction list or else in
       an agent's SLI list.

       If an agent needs to release a lock (perhaps it is dying) it
       should mark the requests "abandoned" and let other transactions
       clean up when they find it..

       When a transaction ends, any locks it decides to pass on will
       be marked "inactive" and the next transaction must (atomically)
       change the state back to "active" before it can inherit the request.

       An inherited request is really only useful if it's in the lock
       cache (and we don't have to grab hte lock mutex to figure out
       we have it)

       When a transaction inherits a lock, it must check whether the
       lock's parent is properly held, potentially inheriting it as
       well. It does this by checking the lock cache; if not there SLI
       isn't helping anyway and the request can be dropped on the
       floor. For the same reason, a request which gets evicted from
       the cache makes no effort to notify its inactive descendants
       that they are no longer valid.
       
     */
    /* SLI state protocol:

       A --> B means a normal transition. A ==> B means the transition is racy and requires CAS

       sli_not_inherited --> sli_inactive	A transaction initiates SLI during lock release
       
       sli_inactive ==> sli_active		A transaction successfully reclaims the request
       sli_inactive ==> sli_invalidating	The lock manager decides to stop SLI
       sli_inactive ==> sli_abandoning		The agent thread decides to stop SLI
       
       sli_invalidating ==> sli_invalid		The lock manager finished stopping SLI first
       sli_invalidating ==> sli_abandoned	The agent thread raced ahead and finished first

       sli_abandoning ==> sli_abandoned		The agent thread finished stopping SLI first
       sli_abandoned ==> sli_invalid		The lock manager raced ahead and finished first

       Once the request is successfully abandoned or invalidated it
       must not be accessed again; the other thread could free it
       at any time and leave a dangling pointer.
     */
    sli_status_t		_sli_status;
    bool keep_me;

    lock_request_t volatile* vthis() { return this; }

    /* perform a CAS on the sli status variable. WARNING: this
       function doesn't behave like a normal CAS because, on success,
       it returns what the *new* value is rather than the old
       one. This is useful for the sli protocols and is safe because
       no two threads ever try to set the status to the same value.
     */
    sli_status_t cas_sli_status(sli_status_t expect, sli_status_t assign) {
	return (sli_status_t) atomic_cas_32((unsigned*)&_sli_status, expect, assign);
    }

    // convert_mode used when we hold lock head mutex
    lmode_t           convert_mode() const { return _convert_mode; } 

    // set_convert_mode used only by acquire in event of upgrade
    // and we hold lock head mutex
    void              set_convert_mode(lmode_t m) { _convert_mode=m; } 

    /// returns an enum lock_mode_t: NL,IS, IX, SH, SIX, UD, EX 
    lmode_t           mode() const { return _mode; } // mode requested &granted)
    // set_mode used:
    // release-duration when upgrading to EX for the purpose of freeing an
    //    extent in the case of extent locks; we grab the lock head mutex
    // acquire compatible upgrade lock, no waiting; we have lock head mutex
    // wakeup_waiters : acquire failed, or release
    //   when we have the lock head mutex and are safely traversing the
    //   queue
    void              set_mode(lmode_t m) { _mode=m; } 

    /// granted, converting, waiting,
    status_t          status() const       { return (status_t) _state; }

    // set_status used:
    // acquire compatible lock, no waiting; we have lock head mutex
    // wakeup_waiters : acquire failed, or release
    //   when we have the lock head mutex and are safely traversing the
    //   queue
    void              set_status(status_t s) { _state = s; }

    NORET             lock_request_t();
    
    lock_request_t*   init(
                          xct_t*        x,
                          lmode_t        m,
                          duration_t    d);

    lock_request_t*   init(
                          xct_t*        x,
                          bool        is_quark_marker);

    void             reset();

    NORET            ~lock_request_t(); // inlined below because of fwd ref

    void             vtable_collect(vtable_row_t &t);
    static void      vtable_collect_names(vtable_row_t &t);

    lock_head_t*     get_lock_head() const;
    xct_lock_info_t* get_lock_info() const { return _lock_info; }
    
    static bool	     is_reclaimed(sli_status_t s) {
        return s < sli_inactive;
    }
    bool	     is_reclaimed() {
        return is_reclaimed(vthis()->_sli_status);
    }

    bool             is_quark_marker() const;

    friend ostream&  operator<<(ostream&, const lock_request_t& l);

private:
    /* disabled */
    lock_request_t(const lock_request_t &);
    lock_request_t &operator=(const lock_request_t &);
};

#include "lock_cache.h"

/* This is the same class over and over, but we need it to be unique
   so that each place it's defined gets a different thread-lock /me/
   variable.
 */

/**
 * \brief A queue-based lock (mutex) to protect the containing class.
 * \todo DEF_LOCK_X_TYPE macro and lock_x type (lock_x.h)
 * \addtogroup TLS
 * \details
 * Each class containing a struct lock_x type defines a
 * static TLS qnode (queue node for a queue-based synchronization primitive). 
 *
 */
#define DEF_LOCK_X_TYPE(N) \
struct lock_x {                      \
    typedef queue_based_lock_t::ext_qnode qnode;   \
    queue_based_lock_t mutex;                      \
    qnode* get_me() {                \
        return &(me()->get_me##N()); \
    }                                \
    rc_t acquire() {                 \
        mutex.acquire(get_me());     \
        return RCOK;                 \
    }                                \
    rc_t release() {                 \
        mutex.release(get_me());     \
        return RCOK;                 \
    }                                \
    bool is_mine() {                 \
        return mutex.is_mine(get_me()); \
    }                                \
}

/**\brief Shared between transaction (xct_t) and lock manager
 * \details
 */
class xct_lock_info_t : private lock_base_t {

public:
    NORET            xct_lock_info_t();
    NORET            ~xct_lock_info_t();

    /// Prepare this structure for use by a new transaction.
    /// Used by the TLS agent when recycling a structure after the
    /// xct that used it goes away.
    void	     init(tid_t const &t, lockid_t::name_space_t l); 
    void 	     reset();

    /// Non-null indicates a thread is trying to satisfy this
    /// request for this xct, and is either blocked or is in the middle
    /// of deadlock detection.
    lock_request_t *  waiting_request() const { return _wait_request; }

    /// See above.
    void             set_waiting_request(lock_request_t*r) { _wait_request=r; }

    /// True if the waiting_request() q.v. is blocking.
    bool             waiting_request_is_blocking() const { return _blocking; }

    /// See above.
    void             set_waiting_request_is_blocking(bool b) { _blocking=b; }

    /// unsafe output operator, for debugging
    friend ostream & operator<<(ostream &o, const xct_lock_info_t &x);

    /// unsafe output operator, for debugging
    ostream &        dump_locks(ostream &out) const;

    /// Unsafe, for debugging
    rc_t             get_locks(
                        lock_mode_t       mode,
                        int               numslots,
                        lockid_t *        space_l,    
                        lock_mode_t *     space_m = 0,
                        bool              extents=false
                     ) const;

    /// Unsafe, for debugging
    rc_t             get_lock_totals( int & total_EX, 
                        int    & total_IX,
                        int    & total_SIX, 
                        int & total_extent ) const;

    enum { lock_cache_size = 25};

    /// ID of the transaction that owns this structure.
    tid_t            tid() const { return _tid; }
    /// See above.
    void             set_tid(const tid_t &t) { _tid=t; }
    
    /// Each thread has a wait_map
    //\todo explain per transaction wait map?
    atomic_thread_map_t const &get_wait_map() const { return _wait_map; }
    void              clear_wait_map() { 
                            _wait_map.lock_for_write();
                            _wait_map.clear(); 
                            _wait_map.unlock_writer();
                        }
    bool              update_wait_map(atomic_thread_map_t const &pmap);

    void              done_waiting();

    /// Level of locks acquired by default.  Transaction can request
    /// coarser locks to be the default.
    /// \todo Nancy have lock levels been tested with shore-mt?
    lockid_t::name_space_t lock_level() const { return _lock_level;}
    void              set_lock_level(lockid_t::name_space_t l) { 
                                                _lock_level =l;}

    /// true if this transaction is in a quark.
    bool             in_quark_scope() const {return _quark_marker != 0;}
    /// The start of the quark, if there this transaction is in one.
    lock_request_t*  quark_marker() const { return _quark_marker; }
    /// See in_quark_scope().
    void             set_quark_marker(lock_request_t*  m) { _quark_marker=m; }

    /// Find a lock in cache if it's there.
    lock_cache_elem_t* search_cache(const lockid_t& name) { 
                                return _lock_cache.search(name); }
    /// Put lock in cache. 
    bool            put_cache(const lockid_t &name, 
                        lock_mode_t mode, 
                        lock_request_t *req,
                        lock_cache_elem_t & victim) {
                        return _lock_cache.put(name, mode, req, victim);
                    }
    void            compact_cache(const lockid_t &name) {
                        _lock_cache.compact(name);
                    }


    void            set_nonblocking();
    bool            is_nonblocking() const { return _noblock; }

    void print_sli_list();
private:
    lock_cache_t<lock_cache_size,(lockid_t::cached_granularity+1)>    _lock_cache;
    DEF_LOCK_X_TYPE(2);                // declare & define lock_x type
public:
    // serialize access to lock_info_t: public for lock_m
    lock_x          lock_info_mutex; 

    /*
     * List of locks acquired by this xct. Protected by the
     * lock_x mutex in the xct_lock_info_t (this structure).
     * Chained through: xlink.
     * Named my_req_list to distinguish it from the list of
     * lock requests hanging off the lock_head_t (_queue).
     * Lists are of the same type.
     * Public for lock_core_m
     */
    request_list_t  my_req_list[t_num_durations];
    request_list_t  sli_list;

private:

    // tid of the most recent transaction using this lock_info; monotonically 
    // increasing.
    tid_t           _tid;     
    lock_request_t* _wait_request;  // lock waited for a thread of this xct 
    bool            _blocking;      // the thread trying to satisfy
                                     // _wait_request is blocking, rather than
                                     // in the deadlock detector but running.
    atomic_thread_map_t  _wait_map; // for dreadlocks DLD

public:
    bool			_sli_enabled; // does the user want to use sli?
    bool			_sli_purged;
    sdesc_cache_t*		_sli_sdesc_cache;
    
private:
    // now this is in the thread :
    // lockid_t     hierarchy[lockid_t::NUMLEVELS];
    lockid_t::name_space_t _lock_level;

    // for implementing quarks
    lock_request_t*  _quark_marker;

    // checkpoint-induced poisoning active?
    bool             _noblock;

private:
     /* disabled */
     xct_lock_info_t(xct_lock_info_t&);
     xct_lock_info_t &operator=(const xct_lock_info_t &);
};


class lock_head_t {
    friend lock_head_t* lock_request_t::get_lock_head() const;
public:
    typedef lock_base_t::lmode_t lmode_t;
    typedef lock_base_t::duration_t duration_t;

    // Note: The purged, adaptive, and critical flags are set on page locks
    // only and the pending and repeat_cb flags on record locks only.

    enum { t_purged = 1,    // set if a page is purged while still in use.
       t_adaptive = 2,    // set if adaptive EX locks are held on the page
       t_pending = 8,    // set when a record level callback gets blocked
       t_repeat_cb = 16    // set when a callback operation needs to be
                // repeated (see comment in callback.cpp).
   };

    w_link_t         chain;        // link in hash chain off the bucket.
                                      // protected by bucket mutex.
    lock_head_t*     _next;
    lockid_t         name;        // the name of this lock
                     // requests for this lock 
    lmode_t          granted_mode;    // the mode of the granted group
    bool             waiting;    // flag indicates
                     // nonempty wait group
    /* # threads trying to acquire this lock's head_mutex.
       In order to avoid acquiring the (expensive, blocking)
       lock->head_mutex during the (contended, otherwise short) bucket lock
       critical section, threads increment the pin_cnt, release the
       bucket, acquire the lock->mutex, then decrement pin_cnt. Changes
       to pin_cnt must be atomic because it's not protected fully by
       any critical section.

       This lock can only be deallocated safely if request _queue is
       empty (as before) *AND* the pin_cnt is zero while a thread
       holds the bucket mutex -- any thread caught trying to add
       itself to the lock's _queue will have the lock pinned; any
       thread that has already added itself will have unpinned the
       lock.

       Note that the lock's chain is protected by the bucket the lock
       lives in, not the lock->head_mutex (because it's logically part of
       the bucket rather than the lock head)
    */
public:
    int volatile     pin_cnt;
    struct my_lock {
        queue_based_lock_t mutex;
        
        // track whether this mutex is contended
        int              total_acquires;
        int              contended_acquires;
        bool             _contended;
#define MY_LOCK_DEBUG 0
#if MY_LOCK_DEBUG
        sthread_t*         _holder;
#endif

        /* Detect contention for the mutex.
           We use a fixed-point representation of the running average
           new_contention = (old_contention*7+contended)/8
           There is also hysteresis here: if the contention value
           rises above 6 it must fall below 1 before the lock reverts
           to uncontended again.
         */
        bool is_contended() {
            if(!_contended && contended_acquires > 1024*6)
                _contended = true;
            else if(_contended && contended_acquires < 1024*1)
                _contended = false;
            return _contended;
        }
        queue_based_lock_t::ext_qnode* get_me() {
            // This is in the tcb_t of smthread:
            return &me()->get_me1();
        }
        rc_t acquire() {
            bool contended = mutex.acquire(get_me());
            if(contended)
                contended_acquires = ((contended_acquires*7) >> 3) + 1024;
#if MY_LOCK_DEBUG
            _holder = me();
#endif
            return RCOK;
        }
        rc_t release() {
#if MY_LOCK_DEBUG
            _holder = 0;
#endif
            mutex.release(get_me());
            return RCOK;
        }
#if MY_LOCK_DEBUG
        bool is_mine() { return _holder == me(); }
#endif
        my_lock(char const* = NULL) 
            : total_acquires(5)
            , contended_acquires(0)
#if MY_LOCK_DEBUG
            , _holder(0)
#endif
        { }
    };
    my_lock             head_mutex;        // serialize access to lock_head_t

    NORET            lock_head_t(
        const lockid_t&         name, 
        lmode_t                 mode);

    NORET            ~lock_head_t()   { chain.detach(); }

    lmode_t          granted_mode_other(const lock_request_t* exclude);
    
    // Invalidate all inactive SLI requests, returning true if any were found
    bool             invalidate_sli();
    
    lock_request_t*  find_lock_request(const xct_lock_info_t*  xdli);
    int              queue_length() const { 
#if MY_LOCK_DEBUG
                            w_assert2(MUTEX_IS_MINE(
                                const_cast<lock_head_t *>(this)->head_mutex));
#endif
                            return _queue.num_members(); 
                     }
    int              unsafe_queue_length() const { return _queue.num_members(); }
    void             queue_append(lock_request_t * r) { 
#if MY_LOCK_DEBUG
                            w_assert1(MUTEX_IS_MINE(head_mutex));
#endif
                            w_assert1(r->status()); // != lock_m::t_no_status
                            _queue.append(r); 
                        }
    lock_request_t*  queue_prev(lock_request_t *req) { 
#if MY_LOCK_DEBUG
                          w_assert2(MUTEX_IS_MINE(head_mutex));
#endif
                          return _queue.prev(&req->rlink); 
                     }

    // Iterates over queue w/o asserting that we hold the lock_head mutex
    struct unsafe_queue_iterator_t : public request_list_i
    {
        NORET unsafe_queue_iterator_t(lock_head_t &l) 
        {
            reset(l._queue, false);
        }
        NORET ~unsafe_queue_iterator_t() {}
    };

    // Iterates over queue after asserting that we hold the lock_head mutex.
    // This class doesn't really DO anything safely, it just asserts.
    // Its purpose is for self-documentation, really.
    struct safe_queue_iterator_t : public request_list_i
                       // This is protected by lock_head_t::mylock.mutex
    {
        NORET safe_queue_iterator_t(lock_head_t &l) 
        {
#if MY_LOCK_DEBUG
            w_assert1(MUTEX_IS_MINE(l.head_mutex));
#endif
            reset(l._queue, false);
        }
        NORET ~safe_queue_iterator_t() {}
    };

    friend ostream&  operator<<(ostream&, const lock_head_t& l);

private:
    // disabled
    NORET            lock_head_t(lock_head_t&);
    lock_head_t&     operator=(const lock_head_t&);

    // DATA
    request_list_t                       _queue;
};

inline NORET
lock_head_t::lock_head_t( const lockid_t& n, lmode_t m)
: 
  name(n),
  granted_mode(m),
  waiting(false),
  pin_cnt(0),
  head_mutex(W_IFDEBUG("m:lkhdt")),  // unnamed if not debug
  _queue(W_LIST_ARG(lock_request_t, rlink), &head_mutex.mutex)
{
    INC_TSTAT(lock_head_t_cnt);
}


inline lock_head_t* 
lock_request_t::get_lock_head() const
{
    // XXX perhaps this should be an "associated list"??
    union {
	w_list_base_t* list;
	long n;
	lock_head_t* head;
    } u = {rlink.member_of()};
    if (u.list) 
	u.n-= w_offsetof(lock_head_t, _queue);
    return u.head;
}

inline NORET            
lock_request_t::~lock_request_t() 
{
    w_assert1(rlink.member_of() == 0 
        /* no is_mine() in the lock head mutex */);
    w_assert1(xlink.member_of() == 0 || 
        MUTEX_IS_MINE(get_lock_info()->lock_info_mutex));
    set_thread(NULL);
}

/*<std-footer incl-file-exclusion='LOCK_X_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
