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

/*<std-header orig-src='shore' incl-file-exclusion='XCT_H'>

 $Id: xct.h,v 1.144.2.15 2010/03/25 18:05:18 nhall Exp $

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

#ifndef XCT_H
#define XCT_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#if W_DEBUG_LEVEL > 4
// You can rebuild with this turned on 
// if you want comment log records inserted into the log
// to help with deciphering the log when recovery bugs
// are nasty.
#define  X_LOG_COMMENT_ON 1
#define  ADD_LOG_COMMENT_SIG ,const char *debugmsg
#define  ADD_LOaG_COMMENT_USE ,debugmsg
#define  LOG_COMMENT_USE(x)  ,x

#else

#define  X_LOG_COMMENT_ON 0
#define  ADD_LOG_COMMENT_SIG
#define  ADD_LOG_COMMENT_USE
#define  LOG_COMMENT_USE(x) 
#endif

#include "block_alloc.h"

class xct_dependent_t;

/**\cond skip */
/**\internal Tells whether the log is on or off for this xct at this moment.
 * \details
 * This is used internally for turning on & off the log during 
 * top-level actions.
 */
class xct_log_t : public smlevel_1 {
private:
    //per-thread-per-xct info
    bool         _xct_log_off;
public:
    NORET        xct_log_t(): _xct_log_off(false) {};
    bool         xct_log_is_off() { return _xct_log_off; }
    void         set_xct_log_off() { _xct_log_off = true; }
    void         set_xct_log_on() { _xct_log_off = false; }
};
/**\endcond skip */

class lockid_t; // forward
class sdesc_cache_t; // forward
class xct_i; // forward
class restart_m; // forward
class lock_m; // forward
class lock_core_m; // forward
class lock_request_t; // forward
class xct_log_switch_t; // forward
class xct_lock_info_t; // forward
class xct_prepare_alk_log; // forward
class xct_prepare_fi_log; // forward
class xct_prepare_lk_log; // forward
class sm_quark_t; // forward
class smthread_t; // forward
class xct_list;
class xct_link;

class logrec_t; // forward
class page_p; // forward

class stid_list_elem_t  {
    public:
    stid_t        stid;
    w_link_t    _link;

    stid_list_elem_t(const stid_t& theStid)
        : stid(theStid)
        {};
    ~stid_list_elem_t()
    {
        if (_link.member_of() != NULL)
            _link.detach();
    }
    static w_base_t::uint4_t    link_offset()
    {
        return W_LIST_ARG(stid_list_elem_t, _link);
    }
};




/**\brief A transaction. Internal to the storage manager.
 * \ingroup LOGSPACE
 *
 * This class may be used in a limited way for the handling of 
 * out-of-log-space conditions.  See \ref LOGSPACE.
 */
class xct_t : public smlevel_1 {
/**\cond skip */
#if USE_BLOCK_ALLOC_FOR_LOGREC 
    friend class block_alloc<xct_t>;
#endif
    friend class xct_i;
    friend struct xct_list;
    friend class smthread_t;
    friend class restart_m;
    friend class lock_m;
    friend class lock_core_m;
    friend class lock_request_t;
    friend class xct_log_switch_t;
    friend class xct_prepare_alk_log;
    friend class xct_prepare_fi_log; 
    friend class xct_prepare_lk_log; 
    friend class sm_quark_t; 
    friend class xct_lock_info_t;
    friend class object_cache_initializing_factory<xct_t>;

protected:
    enum commit_t { t_normal = 0, t_lazy = 1, t_chain = 2 };
/**\endcond skip */

/**\cond skip */
public:
    typedef xct_state_t           state_t;

    static
    xct_t*                        new_xct(
        sm_stats_info_t*             stats = 0,  // allocated by caller
        timeout_in_ms                timeout = WAIT_SPECIFIED_BY_THREAD);
    
    static
    xct_t*                       new_xct(
        const tid_t&                 tid, 
        state_t                      s, 
        const lsn_t&                 last_lsn,
        const lsn_t&                 undo_nxt,
        timeout_in_ms                timeout = WAIT_SPECIFIED_BY_THREAD);
    static
    void                        destroy_xct(xct_t* xd);

    static void			set_elr_enabled(bool enable);

    struct xct_core;            // forward  
private:
    NORET                       xct_t();
    void			init(
        xct_core*                     core,
        sm_stats_info_t*             stats,  // allocated by caller
        const lsn_t&                 last_lsn,
        const lsn_t&                 undo_nxt);
    NORET                       ~xct_t();
    void			reset();

public:

    friend ostream&             operator<<(ostream&, const xct_t&);

    static int                  collect(vtable_t&, bool names_too);
    void                        vtable_collect(vtable_row_t &);
    static void                 vtable_collect_names(vtable_row_t &);

    state_t                     state() const;
    void                        set_timeout(timeout_in_ms t) ;

    timeout_in_ms               timeout_c() const;

    /*  
     * for 2pc: internal, external
     */
public:
    void                         force_readonly();
    bool                         forced_readonly() const;

    vote_t                       vote() const;
    bool                         is_extern2pc() const;
    rc_t                         enter2pc(const gtid_t &g);
    const gtid_t*                gtid() const;
    const server_handle_t&       get_coordinator()const; 
    void                         set_coordinator(const server_handle_t &); 
    static rc_t                  recover2pc(const gtid_t &g,
                                 bool mayblock, xct_t *&);  
    static rc_t                  query_prepared(int &numtids);
    static rc_t                  query_prepared(int numtids, gtid_t l[]);

    rc_t                         prepare();
    rc_t                         log_prepared(bool in_chkpt=false);

    /*
     * basic tx commands:
     */
public:
    static void                 dump(ostream &o); 
    static int                  cleanup(bool dispose_prepared=false); 
                                 // returns # prepared txs not disposed-of


    bool                        is_instrumented() {
                                   return (__stats != 0);
                                }
    void                        give_stats(sm_stats_info_t* s) {
                                    w_assert1(__stats == 0);
                                    __stats = s;
                                }
    void                        clear_stats() {
                                    memset(__stats,0, sizeof(*__stats)); 
                                }
    sm_stats_info_t*            steal_stats() {
                                    sm_stats_info_t*s = __stats; 
                                    __stats = 0;
                                    return         s;
                                }
    const sm_stats_info_t&      const_stats_ref() { return *__stats; }
    rc_t                        commit(bool lazy = false, lsn_t* plastlsn=NULL);
    rc_t                        rollback(const lsn_t &save_pt);
    rc_t                        save_point(lsn_t& lsn);
    rc_t                        chain(bool lazy = false);
    rc_t                        abort(bool save_stats = false);

    // used by restart.cpp, some logrecs
protected:
    sm_stats_info_t&            stats_ref() { return *__stats; }
    rc_t                        dispose();
    void                        change_state(state_t new_state);
    void                        set_first_lsn(const lsn_t &) ;
    void                        set_last_lsn(const lsn_t &) ;
    void                        set_undo_nxt(const lsn_t &) ;
/**\endcond skip */

public:

    // used by checkpoint, restart:
    const lsn_t&                last_lsn() const;
    const lsn_t&                first_lsn() const;
    const lsn_t&                undo_nxt() const;
    bool                        is_read_only_sofar() const;
    const logrec_t*             last_log() const;
    fileoff_t                   get_log_space_used() const;
    rc_t                        wait_for_log_space(fileoff_t amt);
    
    // used by restart, chkpt among others
    static xct_t*               look_up(const tid_t& tid);
    static tid_t                oldest_tid();        // with min tid value
    static tid_t                youngest_tid();        // with max tid value

    // used by sm.cpp:
    static w_base_t::uint4_t    num_active_xcts();

/**\cond skip */
    // used for compensating (top-level actions)
    const lsn_t&                anchor(bool grabit = true);
    void                        release_anchor(bool compensate
                                   ADD_LOG_COMMENT_SIG
                                   );
    int                         compensated_op_depth() const ;

    // -------------------------------------------------------------
    // start_crit and stop_crit are used by the io_m to
    // ensure that at most one thread of the attached transaction
    // enters the io_m at a time. That was the original idea; now it's
    // making sure that at most one thread that's in an sm update operation
    // enters the io_m at any time (allowing concurrent read-only activity). 
    //
    // start_crit grabs the xct's 1thread_log mutex if it doesn't
    // already hold it.  False means we don't actually grab the
    // anchor, so it's not really starting a top-level action.

    void                        start_crit() { (void) anchor(false); }
    // stop_crit frees the xct's 1thread_log mutex if the depth of
    // anchor()/release() calls reaches 0.
    // False means we don't compensate, 
    // so it's not really finishing a top-level action.

    void                        stop_crit() {(void) release_anchor(false
                                            LOG_COMMENT_USE( "stopcrit"));}
    // -------------------------------------------------------------
    
    void                        compensate(const lsn_t&, 
                                          bool undoable
                                          ADD_LOG_COMMENT_SIG
                                          );
    // for recovery:
    void                        compensate_undo(const lsn_t&);
/**\endcond skip */

    // For handling log-space warnings
    // If you've warned wrt a tx once, and the server doesn't
    // choose to abort that victim, you don't want every
    // ssm prologue to warn thereafter. This allows the
    // callback function to turn off the warnings for the (non-)victim. 
    void                         log_warn_disable();
    void                         log_warn_resume();
    bool                         log_warn_is_on() const;

/**\cond skip */

public:
    // used in sm.cpp
    rc_t                        add_dependent(xct_dependent_t* dependent);
    rc_t                        remove_dependent(xct_dependent_t* dependent);
    bool                        find_dependent(xct_dependent_t* dependent);

    //
    //        logging functions -- used in logstub_gen.cpp only
    //
    bool                        is_log_on() const;
    rc_t                        get_logbuf(logrec_t*&, const page_p *p = 0);
    rc_t                        give_logbuf(logrec_t*, const page_p *p = 0);

    //
    //        Used by I/O layer
    //
    void                        AddStoreToFree(const stid_t& stid);
    void                        AddLoadStore(const stid_t& stid);
    //        Used by vol.cpp
    void                        set_alloced() { }

    void                        num_extents_marked_for_deletion(
                                        base_stat_t &num);
public:
    //        For SM interface:
    void                        GetEscalationThresholds(
                                        w_base_t::int4_t &toPage, 
                                        w_base_t::int4_t &toStore, 
                                        w_base_t::int4_t &toVolume);
    void                        SetEscalationThresholds(
                                        w_base_t::int4_t toPage,
                                        w_base_t::int4_t toStore, 
                                        w_base_t::int4_t toVolume);
    bool                        set_lock_cache_enable(bool enable);
    bool                        lock_cache_enabled();

protected:
    /////////////////////////////////////////////////////////////////
    // the following is put here because smthread 
    // doesn't know about the structures
    // and we have changed these to be a per-thread structures.
    static lockid_t*            new_lock_hierarchy();
    static void			delete_lock_hierarchy(lockid_t* &l);
    static sdesc_cache_t*       new_sdesc_cache_t();
    static void			delete_sdesc_cache_t(sdesc_cache_t* &sdc);
    static xct_log_t*           new_xct_log_t();
    static void			delete_xct_log_t(xct_log_t* &l);
    void                        steal(lockid_t*&, sdesc_cache_t*&, xct_log_t*&);
    void                        stash(lockid_t*&, sdesc_cache_t*&, xct_log_t*&);

    void                        attach_thread(); 
    void                        detach_thread(); 


    // stored per-thread, used by lock.cpp
    lockid_t*                   lock_info_hierarchy() const {
                                    return me()->lock_hierarchy();
                                }
public:
    // stored per-thread, used by dir.cpp
    sdesc_cache_t*              sdesc_cache() const;

protected:
    // for xct_log_switch_t:
    /// Set {thread,xct} pair's log-state to on/off (s) and return the old value.
    switch_t                    set_log_state(switch_t s);
    /// Restore {thread,xct} pair's log-state to on/off (s) 
    void                        restore_log_state(switch_t s);


public:
    concurrency_t                get_lock_level(); // non-const: acquires mutex 
    void                         lock_level(concurrency_t l);

    int                          num_threads();          
    rc_t                         check_one_thread_attached() const;   
    void                         attach_update_thread();
    void                         detach_update_thread();
    int                          update_threads() const;

protected:
    // For use by lock manager:
    w_rc_t                       lockblock(timeout_in_ms timeout);// await other thread
    void                         lockunblock(); // inform other waiters
    const w_base_t::int4_t*      GetEscalationThresholdsArray();

    rc_t                         check_lock_totals(int nex, 
                                        int nix, int nsix, int ) const;
    rc_t                         obtain_locks(lock_mode_t mode, 
                                        int nlks, const lockid_t *l); 
    rc_t                         obtain_one_lock(lock_mode_t mode, 
                                        const lockid_t &l); 

    xct_lock_info_t*             lock_info() const;

public:
    /* "poisons" the transaction so cannot block on locks (or remain
       blocked if already so), instead aborting the offending lock
       request with eDEADLOCK. We use eDEADLOCK instead of
       eLOCKTIMEOUT because all transactions must expect the former
       and must abort in response; transactions which specified
       WAIT_FOREVER won't be expecting timeouts, and the SM uses
       timeouts (WAIT_IMMEDIATE) as internal signals which do not
       usually trigger a transaction abort.

       chkpt::take uses this to ensure timely and deadlock-free
       completion/termination of transactions which would prevent a
       checkpoint from freeing up needed log space.
     */
    void                         force_nonblocking();


/////////////////////////////////////////////////////////////////
// DATA
/////////////////////////////////////////////////////////////////
protected:
    xct_link*			 _xlink;
    void 			 join_xlist();
    enum xct_end_type { xct_end_nolog, xct_end_commit, xct_end_abort };
    rc_t			 _xct_ended(xct_end_type type);
    
private:
    sm_stats_info_t*             __stats; // allocated by user
    sdesc_cache_t*                __saved_sdesc_cache_t;
    bool 			 __saved_sdesc_owner;

public:
    void                         acquire_1thread_xct_mutex() const; // serialize
    void                         release_1thread_xct_mutex() const; // concurrency ok
    bool                         is_1thread_log_mutex_mine() const; 
/**\endcond skip */

private:
    void                         acquire_1thread_log_mutex();
    void                         release_1thread_log_mutex();
    void                         assert_1thread_log_mutex_free()const;
private:
    bool                         is_1thread_xct_mutex_mine() const;
    void                         assert_1thread_xct_mutex_free()const;

    rc_t                         _abort();
    rc_t                         _commit(w_base_t::uint4_t flags,
			                                     lsn_t* plastlsn=NULL);

protected:
    // for xct_log_switch_t:
    switch_t                    set_log_state(switch_t s, bool &nested);
    void                        restore_log_state(switch_t s, bool nested);

private:
    bool                        one_thread_attached() const;   // assertion
    // helper function for compensate() and compensate_undo()
    void                        _compensate(const lsn_t&, bool undoable = false);

    w_base_t::int4_t            escalationThresholds[lockid_t::NUMLEVELS-1];
public:
    void                        SetDefaultEscalationThresholds();

    void                        ClearAllStoresToFree();
    void                        FreeAllStoresToFree();
    rc_t                        PrepareLogAllStoresToFree();
    void                        DumpStoresToFree();
    rc_t                        ConvertAllLoadStoresToRegularStores();
    void                        ClearAllLoadStores();

    ostream &                   dump_locks(ostream &) const;

    /////////////////////////////////////////////////////////////////
private:
    /////////////////////////////////////////////////////////////////
    // non-const because it acquires mutex:
    // removed, now that the lock mgrs use the const,INLINE-d form
    // timeout_in_ms        timeout(); 

    static void                 xct_stats(
									u_long&             begins,
									u_long&             commits,
									u_long&             aborts,
									bool                 reset);

    w_rc_t                     _flush_logbuf();
    w_rc_t                     _sync_logbuf(bool block=true);
    void                       _teardown(bool is_chaining);

public:
    /* A nearly-POD struct whose only job is to enable a N:1
       relationship between the log streams of a transaction (xct_t)
       and its core functionality such as locking and 2PC (xct_core).

       Any transaction state which should not eventually be replicated
       per-thread goes here. Usually such state is protected by the
       1-thread-xct-mutex.

       Static data members can stay in xct_t, since they're not even
       duplicated per-xct, let alone per-thread.
     */
    struct xct_core
    {
        void init(tid_t const &t, state_t s, timeout_in_ms timeout);
	void reset();
	xct_core();
        ~xct_core();

        //-- from xct.h ----------------------------------------------------
        timeout_in_ms          _timeout; // default timeout value for lock reqs
        bool                   _warn_on;
        xct_lock_info_t*       _lock_info;

        /* 
         * _lock_cache_enable is protected by its own mutex, because
         * it is used from the lock manager, and the lock mgr is used
         * by the volume mgr, which necessarily holds the xct's 1thread_log
         * mutex.  Thus, in order to avoid mutex-mutex deadlocks,
         * we have a mutex to cover _lock_cache_enable that is used
         * for NOTHING but reading and writing this datum.
         */
        bool                   _lock_cache_enable;
        
        // the 1thread_xct mutex is used to ensure that only one thread
        // is using the xct structure on behalf of a transaction 
        // TBD whether this should be a spin- or block- lock:
        queue_based_lock_t     _1thread_xct;
        
        // Count of number of threads are doing update operations.
        // Used by start_crit and stop_crit.
        volatile int           _updating_operations; 

        // to be manipulated only by smthread funcs
        volatile int           _threads_attached; 

        // used in lockblock, lockunblock, by lock_core 
        pthread_cond_t            _waiters_cond;  // paired with _waiters_mutex
        mutable pthread_mutex_t   _waiters_mutex;  // paired with _waiters_cond

        state_t                   _state;
        bool                      _forced_readonly;
        vote_t                    _vote;
        gtid_t *                  _global_tid; // null if not participating
        server_handle_t*          _coord_handle; // ignored for now
        bool                      _read_only;

        /*
         * List of stores which this xct will free after completion
         * Protected by _1thread_xct.
         */
        w_list_t<stid_list_elem_t,queue_based_lock_t>    _storesToFree;

        /*
         * List of load stores:  converted to regular on xct commit,
         *                act as a temp files during xct
         */
        w_list_t<stid_list_elem_t,queue_based_lock_t>    _loadStores;

        volatile int      _xct_ended; // used for self-checking (assertions) only
    };
    
private: // all data members private
    // the 1thread_xct mutex is used to ensure that only one thread
    // is using the xct structure on behalf of a transaction 
    // It protects a number of things, including the xct_dependents list

    // the 1thread_log mutex is used to ensure that only one thread
    // is logging on behalf of this xct 
    mutable queue_based_lock_t   _1thread_log;

    lsn_t                        _first_lsn;
    lsn_t                        _last_lsn;
    lsn_t                        _undo_nxt;

    // list of dependents: protected by _1thread_xct
    // FRJ: this will become per-stream and not need the mutex any more
    w_list_t<xct_dependent_t,queue_based_lock_t>    _dependent_list;

    /*
     *  lock request stuff
     */
    static lockid_t::name_space_t    convert(concurrency_t cc);
    static concurrency_t             convert(lockid_t::name_space_t n);

    /*
     *  log_m related
     */
    logrec_t*                    _last_log;    // last log generated by xct
    logrec_t*                    _log_buf;

    /* track log space needed to avoid wedging the transaction in the
       event of an abort due to full log
     */ 
    fileoff_t                    _log_bytes_rsvd; // reserved for rollback
    fileoff_t                    _log_bytes_ready; // avail for insert/reserv
    fileoff_t                    _log_bytes_used; // total used by the xct
    bool                         _rolling_back;


#if CHECK_NESTING_VARIABLES
	// for assertions only
    volatile int                 _acquire_1thread_log_depth;  
public:
    void inc_acquire_1thread_log_depth() { _acquire_1thread_log_depth ++; }
    void dec_acquire_1thread_log_depth() { -- _acquire_1thread_log_depth; }
    int  acquire_1thread_log_depth() const { return 
                                               _acquire_1thread_log_depth; }
#else
public:
    void inc_acquire_1thread_log_depth() { }
    void dec_acquire_1thread_log_depth() { }
    int  acquire_1thread_log_depth() const { return  0; }
#endif
private:

     volatile int                _in_compensated_op; 
        // in the midst of a compensated operation
        // use an int because they can be nested.
     lsn_t                       _anchor;
        // the anchor for the outermost compensated op

     xct_core*                   _core;

public:
    tid_t                       tid() const;
};

/**\cond skip */
class auto_release_anchor_t {
private:
    xct_t* _xd;
    lsn_t _anchor;
    bool _and_compensate;
    int    _line; // for debugging

    operator lsn_t const&() const { return _anchor; }
public:

    auto_release_anchor_t(bool and_compensate, int line)
        : _xd(xct()), _and_compensate(and_compensate), _line(line)
    {
    if(_xd)
        _anchor = _xd->anchor(_and_compensate);
    }
    void compensate() {
    if(_xd)
        _xd->compensate(_anchor, false
                LOG_COMMENT_USE("auto_release_anchor_t")
                );
    _xd = 0; // cancel pending release in destructor
    }
    ~auto_release_anchor_t(); // in xct.cpp
};
/**\endcond skip */

/*
 * Use X_DO inside compensated operations
 */
#if X_LOG_COMMENT_ON
#define X_DO1(x,anchor,line)             \
{                           \
    w_rc_t __e = (x);       \
    if (__e.is_error()) {        \
        w_assert3(xct());        \
        W_COERCE(xct()->rollback(anchor));        \
        xct()->release_anchor(true, line );    \
        return RC_AUGMENT(__e); \
    } \
}
#define to_string(x) # x
#define X_DO(x,anchor) X_DO1(x,anchor, to_string(x))

#else

#define X_DO(x,anchor)             \
{                           \
    w_rc_t __e = (x);       \
    if (__e.is_error()) {        \
        w_assert3(xct());        \
        W_COERCE(xct()->rollback(anchor));        \
        xct()->release_anchor(true);        \
        return RC_AUGMENT(__e); \
    } \
}
#endif

/**\cond skip */
class xct_log_switch_t : public smlevel_0 {
    /*
     * NB: use sparingly!!!! EVERYTHING DONE UNDER
     * CONTROL OF ONE OF THESE IS A CRITICAL SECTION
     * This is necessary to support multi-threaded xcts,
     * to prevent one thread from turning off the log
     * while another needs it on, or vice versa.
     */
    switch_t old_state;
public:
    /// Initialize old state
    NORET xct_log_switch_t(switch_t s)  : old_state(OFF)
    {
        if(smlevel_1::log) {
            INC_TSTAT(log_switches);
            if (xct()) {
                old_state = xct()->set_log_state(s);
            }
        }
    }

    NORET
    ~xct_log_switch_t()  {
        if(smlevel_1::log) {
            if (xct()) {
                xct()->restore_log_state(old_state);
            }
        }
    }
};
/**\endcond skip */

inline
bool xct_t::is_log_on() const {
    return (me()->xct_log()->xct_log_is_off() == false);
}

/**\cond skip */
// For use in sm functions that don't allow
// active xct when entered.  These are functions that
// apply to local volumes only.
class xct_auto_abort_t : public smlevel_1 {
public:
    xct_auto_abort_t() : _xct(xct_t::new_xct()) {}
    ~xct_auto_abort_t() {
        switch(_xct->state()) {
        case smlevel_1::xct_ended:
            // do nothing
            break;
        case smlevel_1::xct_active:
            W_COERCE(_xct->abort());
            break;
        default:
            W_FATAL(eINTERNAL);
        }
    xct_t::destroy_xct(_xct);
    }
    rc_t commit() {
        // These are only for local txs
        // W_DO(_xct->prepare());
        W_DO(_xct->commit());
        return RCOK;
    }
    rc_t abort() {W_DO(_xct->abort()); return RCOK;}

private:
    xct_t*        _xct;
};
/**\endcond skip */


/* Iterators provide a thread-safe but non-static view of the
   transaction list. Transactions are free to begin and end while an
   iterator is active, and garbage collection will be coordinated to
   avoid invalid pointer accesses.
 */
struct xct_i {
    struct maybe_lock {
	bool _already_locked;
	maybe_lock(bool already_locked);
	~maybe_lock();
    };
    maybe_lock _lock;
    xct_link* _end_xd;
    xct_link* _cur_xd;
    xct_i(bool already_locked=false);
    ~xct_i();
    xct_t* next(bool can_delete=false);
    xct_t* erase_and_next();
private:
    // disabled
    xct_i(const xct_i&);
    xct_i& operator=(const xct_i&);

};


inline
xct_t::state_t
xct_t::state() const
{
    return _core->_state;
}


inline
bool
operator>(const xct_t& x1, const xct_t& x2)
{
    return (x1.tid() > x2.tid());
}

inline void
xct_t::SetEscalationThresholds(w_base_t::int4_t toPage, 
                w_base_t::int4_t toStore, 
                w_base_t::int4_t toVolume)
{
    if (toPage != dontModifyThreshold)
                escalationThresholds[2] = toPage;
    
    if (toStore != dontModifyThreshold)
                escalationThresholds[1] = toStore;
    
    if (toVolume != dontModifyThreshold)
                escalationThresholds[0] = toVolume;
}

inline void
xct_t::SetDefaultEscalationThresholds()
{
    SetEscalationThresholds(smlevel_0::defaultLockEscalateToPageThreshold,
            smlevel_0::defaultLockEscalateToStoreThreshold,
            smlevel_0::defaultLockEscalateToVolumeThreshold);
}

inline void
xct_t::GetEscalationThresholds(w_base_t::int4_t &toPage, 
                w_base_t::int4_t &toStore, 
                w_base_t::int4_t &toVolume)
{
    toPage = escalationThresholds[2];
    toStore = escalationThresholds[1];
    toVolume = escalationThresholds[0];
}

inline const w_base_t::int4_t *
xct_t::GetEscalationThresholdsArray()
{
    return escalationThresholds;
}

inline
xct_t::vote_t
xct_t::vote() const
{
    return _core->_vote;
}

inline
const lsn_t&
xct_t::last_lsn() const
{
    return _last_lsn;
}

inline
void
xct_t::set_last_lsn( const lsn_t&l)
{
    _last_lsn = l;
}

inline
const lsn_t&
xct_t::first_lsn() const
{
    return _first_lsn;
}

inline
void
xct_t::set_first_lsn(const lsn_t &l) 
{
    _first_lsn = l;
}

inline
const lsn_t&
xct_t::undo_nxt() const
{
    return _undo_nxt;
}

inline
void
xct_t::set_undo_nxt(const lsn_t &l) 
{
    _undo_nxt = l;
}

inline
bool xct_t::is_read_only_sofar() const
{
    return (_first_lsn == lsn_t::null);
}

inline
const logrec_t*
xct_t::last_log() const
{
    return _last_log;
}

inline
bool
xct_t::forced_readonly() const
{
    return _core->_forced_readonly;
}

/*********************************************************************
 *
 *  bool xct_t::is_extern2pc()
 *
 *  return true iff this tx is participating
 *  in an external 2-phase commit protocol, 
 *  which is effected by calling enter2pc() on this
 *
 *********************************************************************/
inline bool            
xct_t::is_extern2pc() 
const
{
    // true if is a thread of global tx
    return _core->_global_tid != 0;
}


inline
const gtid_t*           
xct_t::gtid() const 
{
    return _core->_global_tid;
}


/*<std-footer incl-file-exclusion='XCT_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
