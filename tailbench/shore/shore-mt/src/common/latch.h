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
/*<std-header orig-src='shore' incl-file-exclusion='LATCH_H'>

 $Id: latch.h,v 1.33 2010/06/15 17:28:29 nhall Exp $

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

#ifndef LATCH_H
#define LATCH_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef STHREAD_H
#include <sthread.h>
#endif

#ifdef __GNUG__
#pragma interface
#endif

#include <list>

/**\enum latch_mode_t
 */
enum latch_mode_t { LATCH_NL = 0, LATCH_SH = 1, LATCH_EX = 2 };



class latch_t;
extern ostream &operator<<(ostream &, const latch_t &);

/** \brief Indicates a latch is held by this thread.
 *
 *\details Every time we want to grab a latch,
 * we have to create a latch_holder_t. 
 * We do that with the holder_search class, 
 * which searches a TLS list to make sure  we(this thread) 
 * doesn't already hold the latch, and, if not,
 * it creates a new latch_holder_t for the new latch acquisition.
 * It then stuffs the latch_holder_t in the TLS list.  
 * If we do already have hold the latch in some capacity, 
 * the holder_search returns that existing latch_holder_t.
 * \sa holder_search
 */
class latch_holder_t 
{
public:

// #define LATCH_CAN_BLOCK_LONG 0 in sthread.h 
/*
 FRJ says that it's not worth blocking the thread because
 latches aren't held long enough.  Possible exceptions: tree-
 crabbing, OS preemptions when not enough threads.  
 I also notice that we hold latches for possibly-long times, while
 acquiring page_write_mutexes.

 I thought about keeping this code in place, due to OS preemptions and
 b/c I didn't want to make any assumptions about #threads vs #cpu-equivalents,
 but the code doesn't even compile (what they sent us doesn't compile).  
 So I'm leaving it macro-d out for now; we need to resurrect and fix this.
*/
/**\bug GNATS 66 LATCH_CAN_BLOCK_LONG we haven't tested with this in place,
 and we need to decide which policy should be the default.
*/
    static __thread latch_holder_t* thread_local_holders;
    static __thread latch_holder_t* thread_local_freelist;

    latch_t*     _latch;
    latch_mode_t _mode;
    int          _count;
private:
    sthread_t*   _threadid; // REMOVE ME (for debuging)

    // disabled
    latch_holder_t &operator=(latch_holder_t const &other); 
public:
    // internal freelist use only!
    latch_holder_t* _prev;
    latch_holder_t* _next;
    
    latch_holder_t()
    : _latch(NULL), _mode(LATCH_NL), _count(0), _threadid(NULL)
    {
        _threadid = sthread_t::me();
    }

    bool operator==(latch_holder_t const &other) const {
        if(_threadid != other._threadid) return false;
        return _latch == other._latch && 
              _mode == other._mode && _count == other._count;    
    }

    void print(ostream &o) const;
};

#include <iosfwd>

/**\brief A short-term hold (exclusive or shared) on a page.
 *
 * \details
 * A latch may be acquire()d multiple times by a single thread.  
 * The mode of subsequent acquire()s must be at or above the level 
 * of the currently held latch.  
 * Each of these individual locks must be released.
 * \sa latch_holder_t
 */
class latch_t : public sthread_named_base_t {

public:
    /**\cond skip */
    /// Used for debugging support:
    /// Link together all the thread-local storage for latch holders in a list,
    /// so that we can print this info later.
    static void             on_thread_init(sthread_t *);
    /// Used for debugging support:
    /// Remove thread's latch info from the list.
    static void             on_thread_destroy(sthread_t *);
    /**\endcond skip */
    
    /// Create a latch with the given name.
    NORET                   latch_t(const char* const desc = 0);
    NORET                   ~latch_t();

    // Dump latch info to the ostream. Not thread-safe. 
    ostream&                print(ostream &) const;

    // Return a unique id for the latch.For debugging.
    inline const void *     id() const { return &_lock; } 
    
    /// Change the name of the latch.
    inline void             setname(const char *const desc);

    /// Acquire the latch in given mode. \sa  timeout_t.
    w_rc_t                  latch_acquire(
                                latch_mode_t             m, 
                                sthread_t::timeout_in_ms timeout = 
                                      sthread_base_t::WAIT_FOREVER);
    /**\brief Upgrade from SH to EX if it can be done w/o blocking.
     * \details Returns bool indicating if it would have blocked, in which
     * case the upgrade did not occur. If it didn't have to block, the
     * upgrade did occur.
     * \note Does \b not increment the count. 
     */
    w_rc_t                  upgrade_if_not_block(bool& would_block);

    /**\brief Convert atomically an EX latch into an SH latch. 
     * \details
     * Does not decrement the latch count.
     */
    void                    downgrade();

    /**\brief release the latch.  
     * \details
     * Decrements the latch count.
     */
    void                    latch_release();
    /**\brief Unreliable, but helpful for some debugging.
     */
    bool                    is_latched() const;

    /*
     * GNATS 30 fix: changes lock_cnt name to latch_cnt,
     * and adds _total_cnt to the latch structure itself so it can
     * keep track of the total #holders
     * This is an additional cost, but it is a great debugging aid.
     * \todo TODO: get rid of BUG_LATCH_SEMANTICS_FIX: replace with gnats #
     */

    /// Number of acquires.  A thread may hold more than once.
    int                     latch_cnt() const { return _total_count; }

    /// How many threads hold the R/W lock.
    int                     num_holders() const;
    ///  True iff held in EX mode.
    bool                    is_mine() const; // only if ex 
    ///  True iff held in EX or SH mode.
    int                     held_by_me() const; // sh or ex
    ///  EX,  SH, or NL (if not held at all).
    latch_mode_t            mode() const;

    /// string names of modes. 
    static const char* const    latch_mode_str[3];

private:
    // found, iterator
    w_rc_t                _acquire(latch_mode_t m,
                                 sthread_t::timeout_in_ms,
                                 latch_holder_t* me);
    void                  _release(latch_holder_t* me);
    void                  _downgrade(latch_holder_t* me);
    
/* 
 * Note: the problem with #threads and #cpus and os preemption is real.
 * And it causes things to hang; and it's hard to debug, in the sense that
 * using pthread facilities gives thread-analysis tools and debuggers 
 * understood-things with which to work.
 * Consequently, we use w_pthread_rwlock for our lock.
 */
    mutable srwlock_t     _lock;
#if LATCH_CAN_BLOCK_LONG
    bool                        _blocking;   // true-> on _blocked_list
    pthread_mutex_t             _block_lock; // paired w/ _blocked_list
    sthread_priority_list_t     _blocked_list; // paired w/ _block_lock
#endif

    // disabled
    NORET                        latch_t(const latch_t&);
    latch_t&                     operator=(const latch_t&);

    w_base_t::uint4_t            _total_count;
};



inline void
latch_t::setname(const char* const desc)
{
    rename("l:", desc);
}

inline bool
latch_t::is_latched() const
{
    /* NOTE: Benign race -- this function is naturally unreliable, as
     * its return value may become invalid as soon as it is
     * generated. The only way to reliably know if the lock is held at
     * a particular moment is to hold it yourself, which defeats the
     * purpose of asking in the first place...
     * ... except for assertions / debugging... since there are bugs
     * in acquire/release of latches 
     */
    return _lock.is_locked();
}

inline int
latch_t::num_holders() const
{
    return _lock.num_holders();
}


inline latch_mode_t
latch_t::mode() const
{
    switch(_lock.mode()) {
    case mcs_rwlock::NONE: return LATCH_NL;
    case mcs_rwlock::WRITER: return LATCH_EX;
    case mcs_rwlock::READER: return LATCH_SH;
    default: w_assert1(0); // shouldn't ever happen
             return LATCH_SH; // keep compiler happy
    }
}

// unsafe: for use in debugger:
extern "C" void print_my_latches();
extern "C" void print_all_latches();
extern "C" void print_latch(const latch_t *l);

/*<std-footer incl-file-exclusion='LATCH_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
