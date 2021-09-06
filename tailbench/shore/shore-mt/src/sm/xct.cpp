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

 $Id: xct.cpp,v 1.212 2010/06/21 20:39:39 nhall Exp $

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
#define XCT_C

#include <new>
#define SM_LEVEL 0
#include "sm_int_1.h"

#include "sdesc.h"
#include "block_alloc.h"
#include "tls.h"

#include "lock.h"
#include <sm_int_4.h>
#include "xct_dependent.h"
#include "xct.h"
#include "logrec.h"
#include "sdesc.h"
#include "lock_x.h"
#include "sm_escalation.h"
#include "chkpt_serial.h"
#include <w_strstream.h>

#include <sm.h>
#include "tls.h"
#include <sstream>
#include "crash.h"
#include "chkpt.h"

#ifdef EXPLICIT_TEMPLATE
template class w_list_t<xct_t, queue_based_lock_t>;
template class w_list_i<xct_t, queue_based_lock_t>;
template class w_list_t<xct_dependent_t,queue_based_lock_t>;
template class w_list_i<xct_dependent_t,queue_based_lock_t>;
template class w_keyed_list_t<xct_t, queue_based_lock_t, tid_t>;
template class w_descend_list_t<xct_t, queue_based_lock_t, tid_t>;
template class w_list_t<stid_list_elem_t, queue_based_lock_t>;
template class w_list_i<stid_list_elem_t, queue_based_lock_t>;
template class w_auto_delete_array_t<lock_mode_t>;
template class w_auto_delete_array_t<lockid_t>;
template class w_auto_delete_array_t<stid_t>;

#endif /* __GNUG__*/

// definition of LOGTRACE is in crash.h
#define DBGX(arg) DBG(<<" th."<<me()->id << " " << "gtid. " << gtid()  arg)

#define UNDO_FUDGE_FACTOR(nbytes) (3*(nbytes))

#ifdef W_TRACE
extern "C" void debugflags(const char *);
void
debugflags(const char *a) 
{
   _w_debug.setflags(a);
}
#endif /* W_TRACE */

SPECIALIZE_CS(xct_t, int _dummy, (_dummy=0),
            _mutex->acquire_1thread_xct_mutex(), 
            _mutex->release_1thread_xct_mutex());


/*********************************************************************
 *
 *  The xct list is sorted for easy access to the oldest and
 *  youngest transaction. All instantiated xct_t objects are
 *  in the list.
 *
 *  Here are the transaction list and the mutex that protects it.
 *
 *********************************************************************/

/* There are several ways we need to protect the list of active
   transactions:

   1. We must always know the oldest and newest transaction (ie the
      list is sorted). This is accomplished by virtue of the list
      being a variant of the MCS spinlock.

   2. No transaction is allowed to change state during a checkpoint
      because of a race between making the change and logging it. The
      chkpt_serial_m enforces this for us.

      NOTE: currently there are at least three distinct uses of
      chkpt_serial: mount/dismount volume, transaction prepare, and
      transaction state change (e.g. active -> freeing space ->
      ended). AFAICT, this is unnecessarily coarse -- (un)mount
      operations and transaction state changes only need to serialze
      with chkpt_dev_tab_log and chkpt_xct_tab_log generation,
      respectively. Prepared transactions are part of the lost 2PC
      code, but even if that feature hadn't been lost it would still
      only need to serialize for chkpt_xct_tab_log and the checkpoint
      thread's calls to xct_t::log_prepared. Some day these should be
      split apart properly so checkpointing doesn't interfere with
      normal operation so much -- FRJ

   3. No transaction's list node may be freed while iteration is in
      progress (this is orthogonal to #2 -- checkpointing happens to
      do #3 as part of #2, but #3 can happen by itself). We accomplish
      this by reference counting all iterators; garbage collection
      does not occur if the count is nonzero.

   4. Mount/dismount cannot begin while any transaction is active, and
      no transaction may begin while a (dis)mount is in progress. We
      enforce these conditions by attempting to add a fake transaction
      to the list; if the list is not empty the attempt fails and
      dis(mount) cannot begin. Otherwise we purposefully delay
      propagating the tid, preventing any new transaction(s) from
      acquiring a tid until the operation completes.
 */

enum { NODE_DELETED=-2, NODE_LEFT=-1, NODE_ACTIVE, NODE_HEAD };

struct xct_link {
    xct_link*	_next;
    tid_t	_tid;
    xct_t*	_owner;
    int		_node_state;
    bool 	_fake;
    xct_link volatile* 		vthis() { return this; }
    xct_link(xct_t* owner, bool fake=false)
	: _next(0), _owner(owner), _node_state(NODE_ACTIVE), _fake(fake)
    {
    }
    int cas_state(int expected, int to_assign) {
	return atomic_cas_32((unsigned int*) &_node_state, expected, to_assign);
    }
    xct_link* _reclaim_and_next(xct_link* sentinel);
};

struct xct_list {
    xct_list();
    ~xct_list();
    void insert(xct_link* xd);
    void insert_existing_unsafe(xct_t* xd);
    void remove(xct_link* xd);
    tid_t oldest_tid() { return anchor()->_tid.next(); }

    xct_link _anchor;
    xct_link* volatile _tail;
    
    xct_link* swap_tail(xct_link* xd) {
	return (xct_link*) atomic_swap_ptr(&_tail, xd);
    }
    xct_link* cas_tail(xct_link* old_tail, xct_link* new_tail) {
	return (xct_link*) atomic_cas_ptr(&_tail, old_tail, new_tail);
    }

    xct_link* anchor() { return &_anchor; }
} _xlist;

xct_i::maybe_lock::maybe_lock(bool already_locked)
    : _already_locked(already_locked)
{
    if(!_already_locked)
	chkpt_serial_m::chkpt_acquire();
}

xct_i::maybe_lock::~maybe_lock() {
    if(!_already_locked)
	chkpt_serial_m::chkpt_release();
}

/* Iterators provide a thread-safe but dynamic view of the transaction
   list. Transactions are free to begin and end while an iterator is
   active, and garbage collection will be coordinated to avoid invalid
   pointer accesses.

   WARNING: threads risk deadlock if they create multiple iterators at once
 */
xct_i::xct_i(bool already_locked)
    : _lock(already_locked)
    , _end_xd(*&_xlist._tail)
    , _cur_xd(_end_xd? _xlist.anchor() : 0)
{
}
xct_i::~xct_i() {
}

xct_t* xct_i::next(bool can_delete) {
    /* no thread can leave while the iterator is active so we can walk
       the list at our leisure, skipping or deleting nodes which
       already left.
       
       NOTE: new transactions can still join the list, so we have to
       end when we reach the tail we originally saw rather than
       checking tail repeatedly.

       NOTE: the list is (and will stay) in a consistent state
       because transactions can only remove nodes if we don't hold the
       lock (and we do). This means we can skip NODE_LEFT nodes instead
       of trying to reclaim them, though we'll delete them if allowed to.

       NOTE: if we hit a null next pointer, we know that the
       transaction cannot have started because it does not have a tid
       yet. However, its successors may have read its just-set tid and
       continued on without it. This means we cannot stop iterating
       until we reach _end_xd

     */
    while(_cur_xd != _end_xd) {
	xct_link* n;
	while( !(n=_cur_xd->vthis()->_next) ) ;
	if(NODE_LEFT == n->vthis()->_node_state) {
	    if(can_delete) {
                if (n == *&_xlist._tail) {
                    w_assert1(n == _end_xd);
                    if (_xlist.anchor()->_next == n)
                        _xlist.anchor()->_next = 0;
                    _cur_xd = _end_xd = _xlist._tail = 0;
                }
                else {
                    // unlink+delete the next and retry
                    // NOTE: single-threaded => no atomic ops
                    _cur_xd->_next = n->_next;
                    delete n;
                }
	    }
	    else {
		// next!
		_cur_xd = n;
	    }
	}
	else {
	    // success!
	    _cur_xd = n;
	    return n->_owner;
	}
    }
    return 0;
}

xct_t* xct_i::erase_and_next() {
    // keep this in sync with list remove (esp. NODE_HEAD)
    if(_cur_xd == _end_xd)
	return 0;

    xct_link* old_xd = _cur_xd;
    if (old_xd->_owner) {
        old_xd->_owner->_xlink = 0;
        old_xd->_owner = 0;
    }
    xct_t* n = next(true);
    if (old_xd->_node_state == NODE_HEAD) {
        xct_link* anchor = _xlist.anchor();
        if (_cur_xd) {
            anchor->_tid = _cur_xd->_tid;
            _cur_xd->_node_state = NODE_HEAD;
        }
        anchor->_next = _cur_xd;
        delete old_xd;
    }
    else {
        old_xd->_node_state = NODE_LEFT;
    }
    return n;
}



static void pretty_print(ostream &out, xct_list const* /* rec */) {
    xct_link* cur = &_xlist._anchor;
    out << "[anchor: " << cur->_tid << "]  ";
    while( (cur=cur->_next) ) {
	out << cur->_tid;
	if(cur->_node_state == NODE_LEFT)
	    out << "*";
	else if(cur->_node_state == NODE_HEAD)
	    out << "(H)";
	
	out << "  ";
    }
}
#include <sstream>
char const*
db_pretty_print(xct_list const* rec, int /* i=0 */, char const* /* s=0 */) {
    static stringstream out;
    static string str;
    out.str("");
    pretty_print(out, rec);
    str = out.str();
    return str.c_str();
}

xct_list::xct_list()
  : _anchor(0, true)
  , _tail(0)
{
    _anchor._tid = tid_t::null.next();
}

xct_list::~xct_list() {
    w_assert1(0 == _anchor._next);
}


/* Insert a link at the end of the list
 */
void xct_list::insert(xct_link* xd) {
    xd->_tid = tid_t::null;
    xd->_next = 0;
    xd->_node_state = NODE_ACTIVE;
    membar_producer();
    xct_link* pred = swap_tail(xd);
    if(pred) {
	// wait for their tid to become valid
	while( pred->vthis()->_tid.invalid() ) { }
	xd->_tid = pred->_tid.next();
	membar_producer();
	pred->_next = xd;
    }
    else {
	// joined an empty list... use anchor's next_tid
	xd->_tid = _anchor._tid;
	xd->_node_state = NODE_HEAD;
	membar_producer();
	_anchor._next = xd;
    }
}

void xct_list::insert_existing_unsafe(xct_t* owner) {
    /* FRJ: this code is absolutely NOT thread-safe. It should only
       used during recovery when single-thread access is
       guaranteed. Note that by induction all current members of the
       list were inserted by this same code segment.
    */
    xct_link* _xlink = owner->_xlink = new xct_link(owner);
    tid_t t = _xlink->_tid = owner->tid();
    w_assert1(not t.invalid());
    w_assert1(smlevel_0::operating_mode == smlevel_0::t_in_analysis);

    if(_tail) {
	if(t < _tail->_tid) {
	    // not eol
	    if(t < _anchor._tid) {
		// bol
		_anchor._tid = t;
		_xlink->_next = _anchor._next;
		_anchor._next = _xlink;
		_xlink->_node_state = NODE_HEAD;
		_xlink->_next->_node_state = NODE_ACTIVE;
	    }
	    else {
		// somewhere in the middle
		xct_link* xd = &_anchor;
		while(1) {
		    xct_link* next = xd->_next;
		    if(t < next->_tid) 
			break;
		    xd=next;
		}
		w_assert1(t != xd->_tid);
		_xlink->_next = xd->_next;
		xd->_next = _xlink;
		_xlink->_node_state = NODE_ACTIVE;
	    }
	}
	else {
	    // eol
	    _xlink->_next = NULL;
	    _tail = _tail->_next = _xlink;
	    _xlink->_node_state = NODE_ACTIVE;
	}
    }
    else {
	// empty list (eol+bol)
	_anchor._tid = t;
	_xlink->_next = NULL;
	_tail = _anchor._next = _xlink;
	_xlink->_node_state = NODE_HEAD;
    }
}

void xct_list::remove(xct_link* xd) {
    if (not xd)
        return;

    xct_link* next;
    if(xd->_owner) {
	xd->_owner->_xlink = 0;
	xd->_owner = 0;
    }

    /* There are three possible outcomes of an attempt to hand off:

       1. We finish before becoming NODE_HEAD. In this case we do nothing more.

       2. Otherwise (as NODE_HEAD), we update the anchor with info from our potential successor, then...

       2a. Succeed in marking our successor as NODE_HEAD. In this case we do nothing more
       
       3. Otherwise (as NODE_HEAD and failing to mark our successor)
       check whether our successor is the current NODE_HEAD. This
       indicates we already tried to reclaim it but could not because
       it is at EOQ. In this case do nothing more (joining threads
       know to check whether their predecessor is the
       head).

       3a. Otherwise, start over, but still aware of being NODE_HEAD
     */
    next = xd->vthis()->_next;
    if(next) {
    reclaim:
	// have successor
	if(NODE_LEFT == next->vthis()->_node_state) {
	    // successor left
	    xct_link* after_next = next->vthis()->_next;
	    if(after_next) {
		// we can reclaim this successor
		xd->_next = after_next;
		membar_producer();
		delete next;
		next = after_next;
		goto reclaim;
	    }
	    // else successor may be eoq... don't bother
	}
	// else successor still active... don't bother
    }
    // else we may be eoq... don't bother
    
    // try to escape...
    int s = xd->cas_state(NODE_ACTIVE, NODE_LEFT);
    if(NODE_ACTIVE != s) {
	// caught at queue head... look for a successor
	if(next) {
	    // potential successor found
	    w_assert1(NODE_HEAD == s);
	handoff:
	    _anchor._tid = next->_tid;
	    _anchor._next = next;
	    membar_enter();
	    delete xd; // done with this no matter what...
	    xd = next; 
	    int s2 = xd->cas_state(NODE_ACTIVE, NODE_HEAD);
	    if(NODE_ACTIVE != s2) {
		// too slow... node already left
		w_assert1(NODE_LEFT == s2);
		next = xd->vthis()->_next;
		if(next) {
		    // it can be reclaimed
		    goto handoff;
		}
		else {
		    // successor may be eoq... try to reset queue
		    goto clear_queue;
		}
	    }
	    // else done -- handoff succeeded!
	}
	else {
	    // no apparent successor... try to reset the queue
	clear_queue:
	    /* clear out the anchor... no race w/ anchor node because
	       only one node can be queue head and tail simultaneously
	     */
	    _anchor._tid = xd->_tid.next();
	    _anchor._next = 0;
	    membar_producer();
	    if(xd == *&_tail && xd == cas_tail(xd, 0)) {
		// done -- succeeded at clearing queue
		delete xd;
	    }
	    else {
		// wait for new arrival to introduce itself
		while( !(next=xd->vthis()->_next) ) ;

		// link the newly formed list into the anchor
		_anchor._next = next; 
		goto handoff;
	    }
	}
    }
    // else done -- we escaped!
}


/*********************************************************************
 *
 *  Constructors and destructor
 *
 *********************************************************************/
#define USE_OBJECT_CACHE_FOR_XCT_IMPL 1

#if USE_BLOCK_ALLOC_FOR_XCT_IMPL 
DECLARE_TLS(block_alloc<xct_t>, xct_pool);
DECLARE_TLS(block_alloc<xct_t::xct_core>, core_pool);
#define NEW_XCT new (*xct_pool) xct_t
#define DELETE_XCT(xd) xct_pool->destroy_object(xd)
#define NEW_CORE new (*core_pool) xct_core
#define DELETE_CORE(c) core_pool->destroy_object(c)

#elif USE_OBJECT_CACHE_FOR_XCT_IMPL
#define COMMA2 COMMA
DECLARE_TLS(PROTECT(object_cache<xct_t, PROTECT(object_cache_initializing_factory<xct_t>)>), xct_pool);
DECLARE_TLS(PROTECT(object_cache<xct_t::xct_core, PROTECT(object_cache_initializing_factory<xct_t::xct_core>)>), core_pool);
#define NEW_XCT xct_pool->acquire
#define DELETE_XCT(xd) xct_pool->release(xd)
#define NEW_CORE core_pool->acquire
#define DELETE_CORE(c) core_pool->release(c)

#else
#define NEW_XCT new xct_t
#define DELETE_XCT(xd) delete xd
#define NEW_CORE new xct_core
#define DELETE_CORE(c) delete c
#endif

xct_t*
xct_t::new_xct(
        sm_stats_info_t* stats, 
        timeout_in_ms timeout)
{
    xct_core* core = NEW_CORE(tid_t::null, xct_active, timeout);
    xct_t* xd = NEW_XCT(core, stats, lsn_t(), lsn_t());
    me()->attach_xct(xd);
    return xd;
}

xct_t*
xct_t::new_xct(const tid_t& t, state_t s, const lsn_t& last_lsn,
             const lsn_t& undo_nxt, timeout_in_ms timeout) 
{

    // Uses user(recovery)-provided tid
    /* FRJ: VERY important to only call this constructor during
       recovery because we have to wander the _xlist in completely
       thread-unsafe ways if we discover any tids out of order.
     */
    w_assert1(operating_mode == t_in_analysis);
    w_assert1(not t.invalid());
    xct_core* core = NEW_CORE(t, s, timeout);
    xct_t* xd = NEW_XCT(core, (sm_stats_info_t*)0, last_lsn, undo_nxt);
    
    /// Don't attach
    w_assert1(me()->xct() == 0);
    return xd;
}

void
xct_t::destroy_xct(xct_t* xd) 
{
    xct_core* core = xd->_core;
    DELETE_XCT(xd);
    DELETE_CORE(core);
}

static bool elr_enabled = false;

void
xct_t::set_elr_enabled(bool enable) {
    elr_enabled = enable;
}

#if W_DEBUG_LEVEL > 2
/* debugger-callable */
extern "C" void dumpXct(const xct_t *x) { if(x) { cout << *x <<endl;} }

/* help for debugger-callable dumpThreadById() below */
class PrintSmthreadById : public SmthreadFunc
{
    public:
        PrintSmthreadById(ostream& out, int i ) : o(out), _i(0) {
                _i = sthread_base_t::id_t(i);
        };
        void operator()(const smthread_t& smthread);
    private:
        ostream&        o;
        sthread_base_t::id_t                 _i;
};
void PrintSmthreadById::operator()(const smthread_t& smthread)
{
    if (smthread.id == _i)  {
        o << "--------------------" << endl << smthread;
    }
}

/* debugger-callable */
extern "C" void 
dumpThreadById(int i) { 
    PrintSmthreadById f(cout, i);
    smthread_t::for_each_smthread(f);
}
#endif 

/*
 * clean up existing transactions -- called from ~ss_m, so
 * this should never be subject to multiple
 * threads using the xct list.
 */
int
xct_t::cleanup(bool dispose_prepared)
{
    int         nprepared = 0;
    xct_t*      next;
    {
        /*
         *  We cannot delete an xct while iterating. Use a loop
         *  to iterate and delete one xct for each iteration.
         */
        xct_i i;
	for(xct_t* xd=i.next(); xd; xd=next) {
            switch(xd->state()) {
            case xct_active: {
                    me()->attach_xct(xd);
                    /*
                     *  We usually want to shutdown cleanly. For debugging
                     *  purposes, it is sometimes desirable to simply quit.
                     *
                     *  NB:  if a vas has multiple threads running on behalf
                     *  of a tx at this point, it's going to run into trouble.
                     */
		    next = i.erase_and_next(); // do it first so we don't deadlock!
                    if (shutdown_clean) {
                        W_COERCE( xd->abort() );
                    } else {
                        W_COERCE( xd->dispose() );
                    }
                    DELETE_XCT(xd);
                } 
                break;

            case xct_freeing_space:
            case xct_ended: {
                next = i.erase_and_next();
                    DBG(<< xd->tid() <<"deleting " 
                            << " w/ state=" << xd->state() );
                    DELETE_XCT(xd);
                }
                break;

            case xct_prepared: {
                    if(dispose_prepared) {
                        me()->attach_xct(xd);
			next = i.erase_and_next();
                        W_COERCE( xd->dispose() );
                        DELETE_XCT(xd);
                    } else {
                        DBG(<< xd->tid() <<"keep -- prepared ");
                        nprepared++;
			next = i.next(true);
                    }
                } 
                break;

            default: {
                    DBG(<< xd->tid() <<"skipping " 
                            << " w/ state=" << xd->state() );
		    next = i.next(true);
                }
                break;
            
            } // switch on xct state
        } // xd not null
    }
    return nprepared;
}




/*********************************************************************
 *
 *  xct_t::num_active_xcts()
 *
 *  Return the number of active transactions (equivalent to the
 *  size of _xlist.
 *
 *********************************************************************/
w_base_t::uint4_t
xct_t::num_active_xcts()
{
    w_base_t::uint4_t num = 0;
    for(xct_i it; it.next(); num++) { }
    return  num;
}



/*********************************************************************
 *
 *  xct_t::look_up(tid)
 *
 *  Find the record for tid and return it. If not found, return 0.
 *
 *********************************************************************/
xct_t* 
xct_t::look_up(const tid_t& tid)
{
    xct_t* xd;
    xct_i iter(true);

    while ((xd = iter.next()) && xd->tid() < tid);
    return (xd && xd->tid() == tid)? xd : 0;
}

xct_lock_info_t*
xct_t::lock_info() const {
    return _core->_lock_info;
}

timeout_in_ms
xct_t::timeout_c() const {
    return _core->_timeout;
}

tid_t
xct_t::tid() const
{
    return lock_info()->tid();
}

/*********************************************************************
 *
 *  xct_t::oldest_tid()
 *
 *  Return the tid of the oldest active xct.
 *
 *********************************************************************/
tid_t
xct_t::oldest_tid()
{
    return _xlist.oldest_tid();
}


rc_t
xct_t::abort(bool save_stats_structure /* = false */)
{
    if(is_instrumented() && !save_stats_structure) {
        delete __stats;
        __stats = 0;
    }
    return _abort();
}

/*********************************************************************
 *
 *  xct_t::recover2pc(...)
 *
 *  Locate a prepared tx with this global tid
 *
 *********************************************************************/

rc_t 
xct_t::recover2pc(const gtid_t &g,
        bool        /*mayblock*/,
        xct_t        *&xd)
{
    xct_i i;
    while ((xd = i.next()))  {
        if( xd->state() == xct_prepared ) {
            if(xd->gtid() &&
                *(xd->gtid()) == g) {
                // found
                // TODO  try to reach the coordinator
                return RCOK;
            }
        }
    }
    return RC(eNOSUCHPTRANS);
}

/*********************************************************************
 *
 *  xct_t::query_prepared(...)
 *
 *  given a buffer into which to write global transaction ids, fill
 *  in those for all prepared tx's
 *
 *********************************************************************/
rc_t 
xct_t::query_prepared(int list_len, gtid_t list[])
{
    xct_i iter;
    int i=0;
    xct_t *xd;
    while ((xd = iter.next()))  {
        if( xd->state() == xct_prepared ) {
            if(xd->gtid()) {
                if(i < list_len) {
                    list[i++]=*(xd->gtid());
                } else {
                    return RC(fcFULL);
                }
            // } else {
                // was not external 2pc
            }
        }
    }
    return RCOK;
}

/*********************************************************************
 *
 *  xct_t::query_prepared(...)
 *
 *  Tell how many prepared tx's there are.
 *
 *********************************************************************/
rc_t 
xct_t::query_prepared(int &numtids)
{
    xct_i iter;
    numtids=0;
    xct_t *xd;
    while ((xd = iter.next()))  {
        if( xd->state() == xct_prepared ) {
            numtids++;
        }
    }
    return RCOK;
}

int
xct_t::num_threads()
{
    return _core->_threads_attached;
}

#if CHECK_NESTING_VARIABLES

int
check_compensated_op_nesting::acquire_1thread_log_depth(xct_t* xd, int dflt)
{
    // all bets are off if there's another thread attached to this xct.
    // return the default, which will allow the asserts to pass
    if(xd->num_threads() > 1) return dflt;
    return xd->acquire_1thread_log_depth();
}

int
xct_t::compensated_op_depth() const
{
    return _in_compensated_op;
}

int
check_compensated_op_nesting::compensated_op_depth(xct_t* xd, int dflt)
{
    // all bets are off if there's another thread attached to this xct.
    // return the default, which will allow the asserts to pass
    if(xd->num_threads() > 1) return dflt;
    return xd->compensated_op_depth();
}
#endif



auto_release_anchor_t::~auto_release_anchor_t() 
{
    if(_xd) _xd->release_anchor(_and_compensate LOG_COMMENT_USE("autorel"));
}

void
xct_t::force_nonblocking()
{
    lock_info()->set_nonblocking();
}

rc_t
xct_t::commit(bool lazy,lsn_t* plastlsn)
{
    // w_assert9(one_thread_attached());
    // removed because a checkpoint could
    // be going on right now.... see comments
    // in log_prepared and chkpt.cpp

    return _commit(t_normal | (lazy ? t_lazy : t_normal), plastlsn);
}


rc_t
xct_t::chain(bool lazy)
{
    w_assert9(one_thread_attached());
    return _commit(t_chain | (lazy ? t_lazy : t_chain));
}


bool
xct_t::lock_cache_enabled() 
{
    bool volatile* result = &_core->_lock_cache_enable;
    return *result;
}

bool
xct_t::set_lock_cache_enable(bool enable)
{
    bool result;
    membar_exit();
    result = atomic_swap_8((unsigned char*) &_core->_lock_cache_enable, enable);
    membar_enter();
    return result;
}

sdesc_cache_t*          
xct_t::new_sdesc_cache_t()
{
    /* NB: this gets stored in the thread tcb(), so it's
    * a per-thread cache
    */
    sdesc_cache_t*          _sdesc_cache = new sdesc_cache_t;
    if (!_sdesc_cache) W_FATAL(eOUTOFMEMORY);
    return _sdesc_cache;
}

void
xct_t::delete_sdesc_cache_t(sdesc_cache_t* &sdc)
{
    delete sdc;
    sdc = NULL;
}

xct_log_t*          
xct_t::new_xct_log_t()
{
    xct_log_t*  l = new xct_log_t; 
    if (!l) W_FATAL(eOUTOFMEMORY);
    return l;
}

void
xct_t::delete_xct_log_t(xct_log_t* &l)
{
    delete l;
    l = NULL;
}

lockid_t*          
xct_t::new_lock_hierarchy()
{
    lockid_t*          l = new lockid_t [lockid_t::NUMLEVELS];
    if (!l) W_FATAL(eOUTOFMEMORY);
    return l;
}

void
xct_t::delete_lock_hierarchy(lockid_t* &l)
{
    delete [] l;
    l = NULL;
}

sdesc_cache_t*                    
xct_t::sdesc_cache() const
{
    return me()->sdesc_cache();
}

/**\brief Used by smthread upon attach_xct() to avoid excess heap activity.
 *
 * \details
 * If the xct has a stashed copy of the caches, hand them over to the
 * calling smthread. If not, allocate some off the stack.
 */
void                        
xct_t::steal(lockid_t*&l, sdesc_cache_t*&s, xct_log_t*&x)
{
    /* See comments in smthread_t::new_xct() */
    w_assert1(is_1thread_xct_mutex_mine());
    // Don't dup acquire. acquire_1thread_xct_mutex();
    if( !l ) {
        l = new_lock_hierarchy(); // deleted when thread goes away
    }

    // the sdesc_cache is the only one of these which has any state
    // worth migrating if a thread attaches or detaches.
    if( !s ) {
	std::swap(s, __saved_sdesc_cache_t);
    }
    if( !s ) {
        s = new_sdesc_cache_t(); // deleted when thread goes away
    }

    if( !x ) {
        x = new_xct_log_t(); // deleted when thread finishes
    }
    // Don't dup release
    // release_1thread_xct_mutex();
}

/**\brief Used by smthread upon detach_xct() to avoid excess heap activity.
 *
 * \details
 * If the xct has a stashed copy of the caches, free the caches
 * passed in, otherwise, hang onto them to hand over to the next
 * thread that attaches to this xct.
 */
void                        
xct_t::stash(lockid_t*& /*l*/, sdesc_cache_t*& s, xct_log_t*& /*x*/)
{
    /* See comments in smthread_t::new_xct() */
    w_assert1(is_1thread_xct_mutex_mine());
    // don't dup acquire acquire_1thread_xct_mutex();

    if(__saved_sdesc_owner) {
	if(__saved_sdesc_cache_t) {
	    DBGX(<<"stash: delete " << s);
	    delete s; 
	}
	else { __saved_sdesc_cache_t = s;}
	s = 0;
    }

    // dup acquire/release removed release_1thread_xct_mutex();
}
    


rc_t                        
xct_t::check_lock_totals(int nex, int nix, int nsix, int nextents) const
{
    int        num_EX, num_IX, num_SIX, num_extents;
    W_DO(lock_info()->get_lock_totals( num_EX, num_IX, num_SIX, num_extents));
    if( nex != num_EX || nix != num_IX || nsix != num_SIX) {
        // IX and SIX are the same for this purpose,
        // but whereas what was SH+IX==SIX when the
        // prepare record was written, will be only
        // IX when acquired implicitly as a side effect
        // of acquiring the EX locks.
        // NB: taking this out because it seems that even
        // in the absence of escalation, and if it's
        // not doing a lock_force, the numbers could be off.

        // w_assert1(nix + nsix <= num_IX + num_SIX );
        w_assert1(nex <= num_EX);

        if(nextents != num_extents) {
            smlevel_0::errlog->clog  << fatal_prio
            << "FATAL: " <<endl
            << "nextents logged in xct_prepare_fi_log:" << nextents <<endl
            << "num_extents locked via "
            << "xct_prepare_lk_log and xct_prepare_alk_logs : " << num_extents
            << endl;
            lm->dump(smlevel_0::errlog->clog);
            W_FATAL(eINTERNAL);
        }
        w_assert1(nextents == num_extents);
    }
    return RCOK;
}

rc_t                        
xct_t::obtain_locks(lock_mode_t mode, int num, const lockid_t *locks)
{
    // Turn off escalation for recovering prepared xcts --
    // so the assertions will work.
    sm_escalation_t SAVE;

#if W_DEBUG_LEVEL > 2
    int        b_EX, b_IX, b_SIX, b_extents;
    W_DO(lock_info()->get_lock_totals(b_EX, b_IX, b_SIX, b_extents));
    DBG(<< b_EX << "+" << b_IX << "+" << b_SIX << "+" << b_extents);
#endif 

    int  i;
    rc_t rc;

    for (i=0; i<num; i++) {
        DBG(<<"Obtaining lock : " << locks[i] << " in mode " << int(mode));
#if W_DEBUG_LEVEL > 2
        int        bb_EX, bb_IX, bb_SIX, bb_extents;
        W_DO(lock_info()->get_lock_totals(bb_EX, bb_IX, bb_SIX, bb_extents));
        DBG(<< bb_EX << "+" << bb_IX << "+" << bb_SIX << "+" << bb_extents);
#endif 

        rc =lm->lock(locks[i], mode, t_long, WAIT_IMMEDIATE);
        if(rc.is_error()) {
            lm->dump(smlevel_0::errlog->clog);
            smlevel_0::errlog->clog << fatal_prio
                << "can't obtain lock " <<rc <<endl;
            W_FATAL(eINTERNAL);
        }
        {
            int        a_EX, a_IX, a_SIX, a_extents;
            W_DO(lock_info()->get_lock_totals(a_EX, a_IX, a_SIX, a_extents));
            DBG(<< a_EX << "+" << a_IX << "+" << a_SIX << "+" << a_extents);
            switch(mode) {
                case EX:
                    w_assert9((bb_EX + 1) == (a_EX)); 
                    break;
                case IX:
                case SIX:
                    w_assert9((bb_IX + 1) == (a_IX));
                    break;
                default:
                    break;
                    
            }
        }
    }

    return RCOK;
}

rc_t                        
xct_t::obtain_one_lock(lock_mode_t mode, const lockid_t &lock)
{
    // Turn off escalation for recovering prepared xcts --
    // so the assertions will work.
    DBG(<<"Obtaining 1 lock : " << lock << " in mode " << int(mode));

    sm_escalation_t SAVE;
    rc_t rc;
#if W_DEBUG_LEVEL > 2
    int        b_EX, b_IX, b_SIX, b_extents;
    W_DO(lock_info()->get_lock_totals(b_EX, b_IX, b_SIX, b_extents));
    DBG(<< b_EX << "+" << b_IX << "+" << b_SIX << "+" << b_extents);
#endif 
    rc = lm->lock(lock, mode, t_long, WAIT_IMMEDIATE);
    if(rc.is_error()) {
        lm->dump(smlevel_0::errlog->clog);
        smlevel_0::errlog->clog << fatal_prio
            << "can't obtain lock " <<rc <<endl;
        W_FATAL(eINTERNAL);
    }
#if W_DEBUG_LEVEL > 2
    {
        int        a_EX, a_IX, a_SIX, a_extents;
        W_DO(lock_info()->get_lock_totals(a_EX, a_IX, a_SIX, a_extents));
        DBG(<< a_EX << "+" << a_IX << "+" << a_SIX << "+" << a_extents);

        // It could be a repeat, so let's do this:
        if(b_EX + b_IX + b_SIX == a_EX + a_IX + a_SIX) {
            DBG(<<"DIDN'T GET LOCK " << lock << " in mode " << int(mode));
        } else  {
            switch(mode) {
                case EX:
                    w_assert9((b_EX +  1) == (a_EX));
                    break;
                case IX:
                    w_assert9((b_IX + 1) == (a_IX));
                    break;
                case SIX:
                    w_assert9((b_SIX + 1) == (a_SIX));
                    break;
                default:
                    break;
            }
        }
    }
#endif 
    return RCOK;
}

NORET
sm_escalation_t::sm_escalation_t( int4_t p, int4_t s, int4_t v) 
{
    w_assert9(me()->xct());
    me()->xct()->GetEscalationThresholds(_p, _s, _v);
    me()->xct()->SetEscalationThresholds(p, s, v);
}
NORET
sm_escalation_t::~sm_escalation_t() 
{
    w_assert9(me()->xct());
    me()->xct()->SetEscalationThresholds(_p, _s, _v);
}


/**\brief Set the log state for this xct/thread pair to the value \e s.
 */
smlevel_0::switch_t 
xct_t::set_log_state(switch_t s) 
{
    xct_log_t *mine = me()->xct_log();

    switch_t old = (mine->xct_log_is_off()? OFF: ON);

    if(s==OFF) mine->set_xct_log_off();

    else mine->set_xct_log_on();

    return old;
}

void
xct_t::restore_log_state(switch_t s) 
{
    (void) set_log_state(s);
}


#warning Assumes caller holds chkpt_serial_m. Seg fault otherwise
tid_t
xct_t::youngest_tid()
{
    xct_i it(true);
    xct_link* who;
    if(it._end_xd) {
	tid_t tid;
	while( it._end_xd->vthis()->_tid.invalid() ) { }
	who = it._end_xd;
    }
    else {
	who = _xlist.anchor();
    }
    return who->_tid;
}


void
xct_t::force_readonly() 
{
    acquire_1thread_xct_mutex();
    _core->_forced_readonly = true;
    release_1thread_xct_mutex();
}

smlevel_0::fileoff_t
xct_t::get_log_space_used() const
{
    return _log_bytes_used
    + _log_bytes_ready
    + _log_bytes_rsvd;
}

rc_t
xct_t::wait_for_log_space(fileoff_t amt) {
    rc_t rc = RCOK;
    if(log) {
    fileoff_t still_needed = amt;
    // check whether we even need to wait...
    if(log->reserve_space(still_needed)) {
        still_needed = 0;
    }
    else {
        timeout_in_ms timeout = first_lsn().valid()? 100 : WAIT_FOREVER;
        fprintf(stderr, "%s:%d: first_lsn().valid()? %d    timeout=%d\n",
            __FILE__, __LINE__, first_lsn().valid(), timeout);
        rc = log->wait_for_space(still_needed, timeout);
        if(rc.is_error()) {
        //rc = RC(eOUTOFLOGSPACE);
        }
    }
    
    // update our reservation with whatever we got
    _log_bytes_ready += amt - still_needed;
    }
    return rc;
}

void
xct_t::dump(ostream &out) 
{
    out << "xct_t: "
            << num_active_xcts() << " transactions"
        << endl;
    xct_i i;
    xct_t* xd;
    while ((xd = i.next()))  {
        out << "********************" << endl;
        out << *xd << endl;
    }
}

void                        
xct_t::set_timeout(timeout_in_ms t) 
{ 
    _core->_timeout = t; 
}

w_rc_t 
xct_log_warn_check_t::check(xct_t *& _victim) 
{
    /* FRJ: TODO: use this with the new log reservation code. One idea
       would be to return eLOGSPACEWARN if this transaction (or some
       other?) has been forced nonblocking. Another would be to hook
       in with the LOG_RESERVATIONS stuff and warn if transactions are
       having to wait to acquire log space. Yet another way would be
       to hook in with the checkpoint thread and see if it feels
       stressed...
     */
    /* 
     * NEH In the meantime, we do this crude check in prologues
     * to sm routines, if for no other reason than to test
     * the callbacks.  User can turn off log_warn_check at will with option:
     * -sm_log_warn
     */

    DBG(<<"generate_log_warnings " <<  me()->generate_log_warnings());

    // default is true 
    // User can turn it off, too
    if (me()->generate_log_warnings() && 
            smlevel_0::log &&
            smlevel_0::log_warn_trigger > 0)  
    {
        _victim = NULL;
        w_assert1(smlevel_1::log != NULL);

        // Heuristic, pretty crude:
        smlevel_0::fileoff_t left = smlevel_1::log->space_left() ;
        DBG(<<"left " << left << " trigger " << smlevel_0::log_warn_trigger);

        if( left < smlevel_0::log_warn_trigger ) 
        {
            // Try to force the log first
            log->flush(log->curr_lsn());
        }
        if( left < smlevel_0::log_warn_trigger ) 
        {
            if(log_warn_callback) {
                xct_t *v = xct();
                // Check whether we have log warning on - to avoid
                // cascading errors.
                if(v && v->log_warn_is_on()) {
                    xct_i i(true);
                    lsn_t l = smlevel_1::log->global_min_lsn();
                    char  buf[max_devname];
                    log_m::make_log_name(l.file(), buf, max_devname);
                    w_rc_t rc = (*log_warn_callback)(
                        &i,   // iterator
                        v,    // victim
                        left, // space left  
                        smlevel_0::log_warn_trigger, // threshold
                        buf
                    );
                    if(rc.is_error() && (rc.err_num() == eUSERABORT)) {
                        _victim = v;
                    }
                    return rc;
                }
            } else {
                return  RC(eLOGSPACEWARN);
            }
        }
    }
    return RCOK;
}

struct lock_info_ptr {
    xct_lock_info_t* _ptr;
    
    lock_info_ptr() : _ptr(0) { }
    
    void swap(xct_lock_info_t* &ptr) {
	if(!_ptr)
	    _ptr = new xct_lock_info_t;
	std::swap(_ptr, ptr);
    }
    
    ~lock_info_ptr() { delete _ptr; }
};

DECLARE_TLS(lock_info_ptr, agent_lock_info);



/*********************************************************************
 *
 *  Print out tid and status
 *
 *********************************************************************/
ostream&
operator<<(ostream& o, const xct_t& x)
{
    o << "tid="<< x.tid();

    o << " global_tid=";
    if (x._core->_global_tid)  {
        o << *x._core->_global_tid;
    }  else  {
        o << "<NONE>";
    }

    o << endl << " state=" << x.state() << " num_threads=" << x._core->_threads_attached << endl << "   ";

    o << " defaultTimeout=";
    print_timeout(o, x.timeout_c());
    o << " first_lsn=" << x._first_lsn << " last_lsn=" << x._last_lsn << endl << "   ";

    o << " num_storesToFree=" << x._core->_storesToFree.num_members()
      << " num_loadStores=" << x._core->_loadStores.num_members() << endl << "   ";

    o << " in_compensated_op=" << x._in_compensated_op << " anchor=" << x._anchor;

    if(x.lock_info()) {
         o << *x.lock_info();
    }

    return o;
}

#if USE_BLOCK_ALLOC_FOR_LOGREC 
DECLARE_TLS(block_alloc<logrec_t>, logrec_pool);
#endif

void xct_t::join_xlist() {
    w_assert1(!_xlink);
    _xlink = new xct_link(this);
    _xlist.insert(_xlink);
    _core->_lock_info->set_tid(_xlink->_tid);
}

rc_t
xct_t::_xct_ended(xct_end_type type) {
    rc_t rc;
    chkpt_serial_m::trx_acquire();
    change_state(xct_ended);
    if(type == xct_end_commit)
	rc = log_xct_end();
    else if(type == xct_end_abort)
	rc = log_xct_abort();
    chkpt_serial_m::trx_release();
    return rc;
}

/* the constructor only does physical initialization -- allocating
   memory, wiring up pointers, etc.
 */
xct_t::xct_core::xct_core()
    :
    _lock_info(0),
    _storesToFree(stid_list_elem_t::link_offset(), &_1thread_xct),
    _loadStores(stid_list_elem_t::link_offset(), &_1thread_xct)
{
    DO_PTHREAD(pthread_mutex_init(&_waiters_mutex, NULL));
    DO_PTHREAD(pthread_cond_init(&_waiters_cond, NULL));
}

void xct_t::xct_core::init(tid_t const &t, state_t s, timeout_in_ms timeout)
{
    _timeout = timeout;
    _warn_on = true;
    agent_lock_info->swap(_lock_info);
    _lock_cache_enable = true;
    _updating_operations = 0;
    _threads_attached = 0;
    _state = s;
    _forced_readonly = false;
    _vote = vote_bad;
    _global_tid = 0;
    _coord_handle = 0;
    _read_only = false;
    _xct_ended = 0;

    _lock_info->init(t, convert(cc_alg) );
    w_assert1(t == _lock_info->tid());
    
    INC_TSTAT(begin_xct_cnt);
}

/*********************************************************************
 *
 *  xct_t::xct_t(that, type)
 *
 *  Begin a transaction. The transaction id is assigned automatically,
 *  and the xct record is inserted into _xlist.
 *
 *********************************************************************/
xct_t::xct_t()
    :
    _dependent_list(W_LIST_ARG(xct_dependent_t, _link), 0/*&core->_1thread_xct*/)
{
#if USE_BLOCK_ALLOC_FOR_LOGREC 
    _log_buf = new (*logrec_pool) logrec_t; // deleted when xct goes away
#else
    _log_buf = new logrec_t; // deleted when xct goes away
#endif
    
#if ZERO_INIT
    memset(_log_buf, '\0', sizeof(logrec_t));
#endif

    if (!_log_buf)  {
        W_FATAL(eOUTOFMEMORY);
    }
}

void xct_t::init(xct_core* core, sm_stats_info_t* stats,
		 const lsn_t& last_lsn, const lsn_t& undo_nxt)
{
    _xlink = 0;
    __stats = stats;
    __saved_sdesc_cache_t = 0;
    __saved_sdesc_owner = !me()->sdesc_cache();
    _last_lsn = last_lsn;
    _undo_nxt = undo_nxt;
    _log_bytes_rsvd = 0;
    _log_bytes_ready = 0;
    _log_bytes_used = 0;
    _rolling_back = false;
    _in_compensated_op = 0;
    _last_log = 0;
    _core = core;
    _first_lsn = _last_lsn = _undo_nxt = lsn_t::null;
    _rolling_back = false;
	
    w_assert1(tid() == core->_lock_info->tid());
    SetDefaultEscalationThresholds();

    if (timeout_c() == WAIT_SPECIFIED_BY_THREAD) {
        // override in this case
        set_timeout(me()->lock_timeout());
    }
    w_assert9(timeout_c() >= 0 || timeout_c() == WAIT_FOREVER);

    xct_lock_info_t* li = lock_info();
    if(li->_sli_enabled)
	std::swap(__saved_sdesc_cache_t, li->_sli_sdesc_cache);
    
    if(tid().invalid()) 
	join_xlist();
    else 
	_xlist.insert_existing_unsafe(this);
}

xct_t::xct_core::~xct_core()
{
    delete _global_tid;
    delete _coord_handle;
}

void xct_t::xct_core::reset() {
    w_assert3(_state == xct_ended);
    _lock_info->reset();
    agent_lock_info->swap(_lock_info);
}

/*********************************************************************
 *
 *  xct_t::~xct_t()
 *
 *  Clean up and free up memory used by the transaction. The 
 *  transaction has normally ended (committed or aborted) 
 *  when this routine is called.
 *
 *********************************************************************/
xct_t::~xct_t()
{
    FUNC(xct_t::~xct_t);
#if USE_BLOCK_ALLOC_FOR_LOGREC 
    logrec_pool->destroy_object(_log_buf);
#else
    delete _log_buf;
#endif

    if(__saved_sdesc_cache_t) {
	delete __saved_sdesc_cache_t;
    }
    
    // caller deletes core...
}

void
xct_t::reset() {
    DBGX( << " ended: _log_bytes_rsvd " << _log_bytes_rsvd  
            << " _log_bytes_ready " << _log_bytes_ready
            << " _log_bytes_used " << _log_bytes_used
            );

    w_assert9(__stats == 0);

    _teardown(false);
    w_assert3(_in_compensated_op==0);

    if (shutdown_clean)  {
        w_assert1(me()->xct() == 0);
    }

    w_assert1(one_thread_attached());
    {
        CRITICAL_SECTION(xctstructure, *this);
        // w_assert1(is_1thread_xct_mutex_mine());

        while (_dependent_list.pop()) ;

        // clean up what's stored in the thread
        me()->no_xct(this);
    }

    xct_lock_info_t* li = lock_info();
    if(__saved_sdesc_cache_t && li->_sli_enabled) {
	__saved_sdesc_cache_t->inherit_all();
	std::swap(__saved_sdesc_cache_t, li->_sli_sdesc_cache);
    }
    if(__saved_sdesc_cache_t) {
	delete __saved_sdesc_cache_t;
	__saved_sdesc_cache_t=0;
    }
}

// common code needed by _commit(t_chain) and ~xct_t()
void
xct_t::_teardown(bool is_chaining) {
    if(is_chaining) {
	join_xlist();
    }

    DBGX( << " commit: _log_bytes_rsvd " << _log_bytes_rsvd  
     << " _log_bytes_ready " << _log_bytes_ready
     << " _log_bytes_used " << _log_bytes_used
     );
    if(long leftovers = _log_bytes_rsvd + _log_bytes_ready) {
    w_assert2(smlevel_0::log);
    smlevel_0::log->release_space(leftovers);
    };
    _log_bytes_rsvd = _log_bytes_ready = _log_bytes_used = 0;
    
}

/*********************************************************************
 *
 *  xct_t::set_coordinator(...)
 *  xct_t::get_coordinator(...)
 *
 *  get and set the coordinator handle
 *  The handle is an opaque value that's
 *  logged in the prepare record.
 *
 *********************************************************************/
void
xct_t::set_coordinator(const server_handle_t &h) 
{
    DBGX(<<"set_coord for tid " << tid()
        << " handle is " << h);
    /*
     * Make a copy 
     */
    if(!_core->_coord_handle) {
        _core->_coord_handle = new server_handle_t; // deleted when xct goes way
        if(!_core->_coord_handle) {
            W_FATAL(eOUTOFMEMORY);
        }
    }

    *_core->_coord_handle = h;
}

const server_handle_t &
xct_t::get_coordinator() const
{
    // caller can copy
    return *_core->_coord_handle;
}

/*********************************************************************
 *
 *  xct_t::change_state(new_state)
 *
 *  Change the status of the transaction to new_state. All 
 *  dependents are informed of the change.
 *
 *********************************************************************/
void
xct_t::change_state(state_t new_state)
{
    FUNC(xct_t::change_state);
    w_assert1(one_thread_attached());

    CRITICAL_SECTION(xctstructure, *this);
    w_assert1(is_1thread_xct_mutex_mine());

    w_assert2(_core->_state != new_state);
    w_assert2((new_state > _core->_state) || 
            (_core->_state == xct_chaining && new_state == xct_active));

    state_t old_state = _core->_state;
    _core->_state = new_state;

    w_list_i<xct_dependent_t,queue_based_lock_t> i(_dependent_list);
    xct_dependent_t* d;
    while ((d = i.next()))  {
        d->xct_state_changed(old_state, new_state);
    }
    if(new_state == xct_ended)
	_xlist.remove(_xlink);
}


/**\todo Figure out how log space warnings will interact with mtxct */
void
xct_t::log_warn_disable()
{
    _core->_warn_on = true;
}

void
xct_t::log_warn_resume()
{
    _core->_warn_on = false;
}

bool
xct_t::log_warn_is_on() const
{
    return _core->_warn_on;
}

/**\ Figure out how _updating_operations will interact with mtxct */
void
xct_t::attach_update_thread()
{
    w_assert2(_core->_updating_operations >= 0);
    atomic_inc(_core->_updating_operations);
}
void
xct_t::detach_update_thread()
{
    atomic_dec(_core->_updating_operations);
    w_assert2(_core->_updating_operations >= 0);
}

int
xct_t::update_threads() const
{ 
    return _core->_updating_operations;
} 

/*********************************************************************
 *
 *  xct_t::add_dependent(d)
 *  xct_t::remove_dependent(d)
 *
 *  Add a dependent to the dependent list of the transaction.
 *
 *********************************************************************/
rc_t
xct_t::add_dependent(xct_dependent_t* dependent)
{
    FUNC(xct_t::add_dependent);
    CRITICAL_SECTION(xctstructure, *this);
    w_assert9(dependent->_link.member_of() == 0);
    
    w_assert1(is_1thread_xct_mutex_mine());
    _dependent_list.push(dependent);
    dependent->xct_state_changed(_core->_state, _core->_state);
    return RCOK;
}
rc_t
xct_t::remove_dependent(xct_dependent_t* dependent)
{
    FUNC(xct_t::remove_dependent);
    CRITICAL_SECTION(xctstructure, *this);
    w_assert9(dependent->_link.member_of() != 0);
    
    w_assert1(is_1thread_xct_mutex_mine());
    dependent->_link.detach(); // is protected
    return RCOK;
}

/*********************************************************************
 *
 *  xct_t::find_dependent(d)
 *
 *  Return true iff a given dependent(ptr) is in the transaction's
 *  list.   This must cleanly return false (rather than crashing) 
 *  if d is a garbage pointer, so it cannot dereference d
 *
 *  **** Used by value-added servers. ****
 *
 *********************************************************************/
bool
xct_t::find_dependent(xct_dependent_t* ptr)
{
    FUNC(xct_t::find_dependent);
    xct_dependent_t        *d;
    CRITICAL_SECTION(xctstructure, *this);
    w_assert1(is_1thread_xct_mutex_mine());
    w_list_i<xct_dependent_t,queue_based_lock_t>    iter(_dependent_list);
    while((d=iter.next())) {
        if(d == ptr) {
            return true;
        }
    }
    return false;
}


/*********************************************************************
 *
 *  xct_t::prepare()
 *
 *  Enter prepare state. For 2 phase commit protocol.
 *  Set vote_abort or vote_commit if any
 *  updates were done, else return vote_readonly
 *
 *  We are called here if we are participating in external 2pc,
 *  OR we're just committing 
 *
 *  This does NOT do the commit
 *
 *********************************************************************/
rc_t 
xct_t::prepare()
{
    // This is to be applied ONLY to the local thread.

    // W_DO(check_one_thread_attached()); // now checked in prologue
    w_assert1(one_thread_attached());

    if(lock_info() && lock_info()->in_quark_scope()) {
        return RC(eINQUARK);
    }

    // must convert all these stores before entering the prepared state
    // just as if we were committing.
    W_DO( ConvertAllLoadStoresToRegularStores() );

    w_assert1(_core->_state == xct_active);

    // default unless all works ok
    _core->_vote = vote_abort;

    _core->_read_only = (_first_lsn == lsn_t::null);

    if(_core->_read_only || forced_readonly()) {
        _core->_vote = vote_readonly;
        // No need to log prepare record
#if W_DEBUG_LEVEL > 5
        // This is really a bogus assumption,
        // since a tx could have explicitly
        // forced an EX lock and then never
        // updated anything.  We'll leave it
        // in until we can run all the scripts.
        // The question is: should the lock 
        // be held until the tx is resolved,
        if(!forced_readonly()) {
            int        total_EX, total_IX, total_SIX, num_extents;
            W_DO(lock_info()->get_lock_totals(total_EX, total_IX, total_SIX, num_extents));
            if(total_EX != 0) {
                cout 
                   << "WARNING: " << total_EX 
                   << " write locks held by a read-only transaction thread. "
                   << " ****** voting read-only ***** "
                   << endl;
             }
            // w_assert9(total_EX == 0);
        }
#endif 
        // If commit is done in the readonly case,
        // it's done by ss_m::_prepare_xct(), NOT HERE

        change_state(xct_prepared);
        // Let the stat indicate how many prepare records were
        // logged
        INC_TSTAT(s_prepared);
        return RCOK;
    }
#if X_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "prepare ";
        W_DO(log_comment(s.c_str()));
    }
#endif

    ///////////////////////////////////////////////////////////
    // NOT read only
    ///////////////////////////////////////////////////////////

    if(is_extern2pc() ) {
        DBGX(<<"logging prepare because e2pc=" << is_extern2pc());
        W_DO(log_prepared());

    } else {
        // Not distributed -- no need to log prepare
    }

    /******************************************
    // Don't set the state until after the
    // log records are written, so that
    // if a checkpoint is going on right now,
    // it'll not log this tx in the checkpoint
    // while we are logging it.
    ******************************************/

    change_state(xct_prepared);
    INC_TSTAT(prepare_xct_cnt);

    _core->_vote = vote_commit;
    return RCOK;
}

/*********************************************************************
 * xct_t::log_prepared(bool in_chkpt)
 *  
 * log a prepared tx 
 * (fuzzy, so to speak -- can be intermixed with other records)
 * 
 * 
 * called from xct_t::prepare() when tx requests prepare,
 * also called during checkpoint, to checkpoint already prepared
 * transactions
 * When called from checkpoint, the argument should be true, false o.w.
 *
 *********************************************************************/

rc_t
xct_t::log_prepared(bool in_chkpt)
{
    FUNC(xct_t::log_prepared);
    w_assert1(_core->_state == (in_chkpt?xct_prepared:xct_active));

    w_rc_t rc;

    if( !in_chkpt)  {
        // grab the mutex that serializes prepares & chkpts
        chkpt_serial_m::trx_acquire();
    }


    SSMTEST("prepare.unfinished.0");

    int total_EX, total_IX, total_SIX, num_extents;
    if( ! _core->_coord_handle ) {
                return RC(eNOHANDLE);
    }
    rc = log_xct_prepare_st(_core->_global_tid, *_core->_coord_handle);
    if (rc.is_error()) { RC_AUGMENT(rc); goto done; }

    SSMTEST("prepare.unfinished.1");
    {

    rc = lock_info()->
            get_lock_totals(total_EX, total_IX, total_SIX, num_extents);
    if (rc.is_error()) { RC_AUGMENT(rc); goto done; }

    /*
     * We will not get here if this is a read-only
     * transaction -- according to _read_only, above
    */

    /*
     *  Figure out how to package the locks
     *  If they all fit in one record, do that.
     *  If there are lots of some kind of lock (most
     *  likely EX in that case), split those off and
     *  write them in a record of uniform lock mode.
     */  
    int i;

    /*
     * NB: for now, we assume that ONLY the EX locks
     * have to be acquired, and all the rest are 
     * acquired by virtue of the hierarchy.
     * ***************** except for extent locks -- they aren't
     * ***************** in the hierarchy.
     *
     * If this changes (e.g. any code uses dir_m::access(..,.., true)
     * for some non-EX lock mode, we have to figure out how
     * to locate those locks *not* acquired by virtue of an
     * EX lock acquisition, and log those too.
     *
     * We have the mechanism here to do the logging,
     * but we don't have the mechanism to separate the
     * extraneous IX locks, say, from those that DO have
     * to be logged.
     */

    i = total_EX;

    if (i < prepare_lock_t::max_locks_logged)  {
            /*
            // EX ONLY
            // we can fit them *all* in one record
            */
            lockid_t* space_l = new lockid_t[i]; // auto-del
            w_auto_delete_array_t<lockid_t> auto_del_l(space_l);

            rc = lock_info()-> get_locks(EX, i, space_l);
            if (rc.is_error()) { RC_AUGMENT(rc); goto done; }

            SSMTEST("prepare.unfinished.2");

            rc = log_xct_prepare_lk( i, EX, space_l);
            if (rc.is_error()) { RC_AUGMENT(rc); goto done; }
    }  else  {
            // can't fit them *all* in one record
            // so first we log the EX locks only,
            // the rest in one or more records

            /* EX only */
            lockid_t* space_l = new lockid_t[i]; // auto-del
            w_auto_delete_array_t<lockid_t> auto_del_l(space_l);

            rc = lock_info()-> get_locks(EX, i, space_l);
            if (rc.is_error()) { RC_AUGMENT(rc); goto done; }

            // Use as many records as needed for EX locks:
            //
            // i = number to be recorded in next log record
            // j = number left to be recorded altogether
            // k = offset into space_l array
            //
            i = prepare_lock_t::max_locks_logged;
            int j=total_EX, k=0;
            while(i < total_EX) {
                    rc = log_xct_prepare_lk(prepare_lock_t::max_locks_logged, EX, &space_l[k]);
                    if (rc.is_error()) { RC_AUGMENT(rc); goto done; }
                    i += prepare_lock_t::max_locks_logged;
                    k += prepare_lock_t::max_locks_logged;
                    j -= prepare_lock_t::max_locks_logged;
            }
            SSMTEST("prepare.unfinished.3");
            // log what's left of the EX locks (that's in j)

            rc = log_xct_prepare_lk(j, EX, &space_l[k]);
            if (rc.is_error()) { RC_AUGMENT(rc); goto done; }
    }

    {
            /* Now log the extent locks */
            i = num_extents;
            lockid_t* space_l = new lockid_t[i]; // auto-del
            lock_mode_t* space_m = new lock_mode_t[i]; // auto-del

            w_auto_delete_array_t<lockid_t> auto_del_l(space_l);
            w_auto_delete_array_t<lock_mode_t> auto_del_m(space_m);

            rc = lock_info()-> get_locks(NL, i, space_l, space_m, true);
            if (rc.is_error()) { RC_AUGMENT(rc); goto done; }

            SSMTEST("prepare.unfinished.4");
            int limit = prepare_all_lock_t::max_locks_logged;
            int k=0;
            while (i >= limit)  {
            rc = log_xct_prepare_alk(limit, &space_l[k], &space_m[k]);
            if (rc.is_error())  {
                    RC_AUGMENT(rc);
                    goto done;
            }
            i -= limit;
            k += limit;
            }
            if (i > 0)  {
            rc = log_xct_prepare_alk(i, &space_l[k], &space_m[k]);
            if (rc.is_error())  {
                    RC_AUGMENT(rc);
                    goto done;
            }
            }
    }
    }

    W_DO( PrepareLogAllStoresToFree() );

    SSMTEST("prepare.unfinished.5");

    rc = log_xct_prepare_fi(total_EX, total_IX, total_SIX, num_extents,
        this->first_lsn());
    if (rc.is_error()) { RC_AUGMENT(rc); goto done; }

done:
    // We have to force the log record to the log
    // If we're not in a chkpt, we also have to make
    // it durable
    if( !in_chkpt)  {
    _sync_logbuf();

        // free the mutex that serializes prepares & chkpts
        chkpt_serial_m::trx_release();
    }
    return rc;
}

/*********************************************************************
 *
 *  xct_t::commit(flags)
 *
 *  Commit the transaction. If flag t_lazy, log is not synced.
 *  If flag t_chain, a new transaction is instantiated inside
 *  this one, and inherits all its locks.
 *
 *  In *plastlsn it returns the lsn of the last log record for this
 *  xct.
 *
 *********************************************************************/
rc_t
xct_t::_commit(uint4_t flags,lsn_t* plastlsn)
{
    DBGX( << " commit: _log_bytes_rsvd " << _log_bytes_rsvd  
            << " _log_bytes_ready " << _log_bytes_ready
            << " _log_bytes_used " << _log_bytes_used
            );
    // W_DO(check_one_thread_attached()); // now checked in prologue
    w_assert1(one_thread_attached());

    if(is_extern2pc()) {
        w_assert1(_core->_state == xct_prepared);
    } else {
        w_assert1(_core->_state == xct_active || _core->_state == xct_prepared);
    };

    if(lock_info() && lock_info()->in_quark_scope()) {
        return RC(eINQUARK);
    }

    w_assert1(1 == atomic_inc_nv(_core->_xct_ended));

    W_DO( ConvertAllLoadStoresToRegularStores() );

    SSMTEST("commit.1");
    
    change_state(flags & xct_t::t_chain ? xct_chaining : xct_committing);

    SSMTEST("commit.2");

    if (_last_lsn.valid() || !smlevel_1::log)  {
        /*
         *  If xct generated some log, write a synchronous
         *  Xct End Record.
         *  Do this if logging is turned off. If it's turned off,
         *  we won't have a _last_lsn, but we still have to do 
         *  some work here to complete the tx; in particular, we
         *  have to destroy files...
         * 
         *  Logging a commit must be serialized with logging
         *  prepares (done by chkpt).
         */
        
        // wait for the checkpoint to finish
    chkpt_serial_m::trx_acquire();
        // Have to re-check since in the meantime another thread might
        // have attached. Of course, that's always the case...
        W_DO(check_one_thread_attached());

        // don't allow a chkpt to occur between changing the 
        // state and writing the log record, 
        // since otherwise it might try to change the state
        // to the current state (which causes an assertion failure).

        change_state(xct_freeing_space);
        rc_t rc = log_xct_freeing_space();
        SSMTEST("commit.3");
        chkpt_serial_m::trx_release();

        W_DO(rc);

        /*
         *  If ELR, free all locks. Do not free locks if chaining.
         */
        if (elr_enabled && ! (flags & xct_t::t_chain))  {
#if X_LOG_COMMENT_ON
            {
                w_ostrstream s;
                s << "unlock_duration, frees extents ";
                W_DO(log_comment(s.c_str()));
            }
#endif
            W_COERCE( lm->unlock_duration(t_long, true, false) );
        }

        if (!(flags & xct_t::t_lazy) /* && !_read_only */)  {
            _sync_logbuf();
        }
        else { // IP: If lazy, wake up the flusher but do not block
            _sync_logbuf(false);
        }

        // IP: Before destroying anything copy last_lsn
        if (plastlsn != NULL) *plastlsn = _last_lsn;

#if X_LOG_COMMENT_ON
        {
            w_ostrstream s;
            s << "FreeAllStores... ";
            W_DO(log_comment(s.c_str()));
        }
#endif

        /*
         *  Do the actual destruction of all stores which were requested
         *  to be destroyed by this xct.
         */
        FreeAllStoresToFree();


        /*
         *  If !ELR, free all locks. Do not free locks if chaining.
         */
        if (!elr_enabled && ! (flags & xct_t::t_chain))  {
#if X_LOG_COMMENT_ON
            {
                w_ostrstream s;
                s << "unlock_duration, frees extents ";
                W_DO(log_comment(s.c_str()));
            }
#endif
            W_COERCE( lm->unlock_duration(t_long, true, false) );
        }

        // don't allow a chkpt to occur between changing the state and writing
        // the log record, since otherwise it might try to change the state
        // to the current state (which causes an assertion failure).
	W_DO(_xct_ended(xct_end_commit));
    }  else  {
	// must hold chkpt_serial_m to make _xlist.remove safe
	W_COERCE(_xct_ended(xct_end_nolog));

        /*
         *  Free all locks. Do not free locks if chaining.
         *  Don't free exts as there shouldn't be any to free.
         */
        if (! (flags & xct_t::t_chain))  {
            W_COERCE( lm->unlock_duration(t_long, true, true) );
        }
    }

    me()->detach_xct(this);        // no transaction for this thread
    INC_TSTAT(commit_xct_cnt);


    /*
     *  Xct is now committed
     */

    if (flags & xct_t::t_chain)  {
#if X_LOG_COMMENT_ON
        {
            w_ostrstream s;
            s << "chaining... ";
            W_DO(log_comment(s.c_str()));
        }
#endif
        /*
         *  Start a new xct in place
         */
        _teardown(true);
        _first_lsn = _last_lsn = _undo_nxt = lsn_t::null;
        _core->_xct_ended = 0;
        _last_log = 0;
        _core->_lock_cache_enable = true;

        // should already be out of compensated operation
        w_assert3( _in_compensated_op==0 );

        me()->attach_xct(this);
        INC_TSTAT(begin_xct_cnt);
        _core->_state = xct_chaining; // to allow us to change state back
        // to active: there's an assert about this where we don't
        // have context to know that it's where we're chaining.
        change_state(xct_active);
    }

    return RCOK;
}


/*********************************************************************
 *
 *  xct_t::abort()
 *
 *  Abort the transaction by calling rollback().
 *
 *********************************************************************/
rc_t
xct_t::_abort()
{
    // If there are too many threads attached, tell the VAS and let it
    // ensure that only one does this.
    // W_DO(check_one_thread_attached()); // now done in the prologues.
    
    w_assert1(one_thread_attached());
    w_assert1(_core->_state == xct_active || _core->_state == xct_prepared);
    w_assert1(1 == atomic_inc_nv(_core->_xct_ended));
    w_assert1(_core->_state == xct_active || _core->_state == xct_prepared);

    change_state(xct_aborting);
#if X_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "aborting... ";
        W_DO(log_comment(s.c_str()));
    }
#endif

    /*
     * clear the list of load stores as they are going to be destroyed
     */
    ClearAllLoadStores();

    W_DO( rollback(lsn_t::null) );

    if (_last_lsn.valid()) {
        /*
         *  If xct generated some log, write a Xct End Record. 
         *  We flush because if this was a prepared
         *  transaction, it really must be synchronous 
         */

        // don't allow a chkpt to occur between changing the state and writing
        // the log record, since otherwise it might try to change the state
        // to the current state (which causes an assertion failure).
#if X_LOG_COMMENT_ON
        {
            w_ostrstream s;
            s << "rolled back ";
            W_DO(log_comment(s.c_str()));
        }
#endif

        chkpt_serial_m::trx_acquire();
        change_state(xct_freeing_space);
        rc_t rc = log_xct_freeing_space();
        chkpt_serial_m::trx_release();

        W_DO(rc);

        _sync_logbuf();
#if X_LOG_COMMENT_ON
        {
            w_ostrstream s;
            s << "FreeAllStoresToFree ";
            W_DO(log_comment(s.c_str()));
        }
#endif

        /*
         *  Do the actual destruction of all stores which were requested
         *  to be destroyed by this xct.
         */
        FreeAllStoresToFree();

#if X_LOG_COMMENT_ON
        {
            w_ostrstream s;
            s << "unlock_duration, frees extents ";
            W_DO(log_comment(s.c_str()));
        }
#endif
        /*
         *  Free all locks 
         */
        W_COERCE( lm->unlock_duration(t_long, true, false) );

        // don't allow a chkpt to occur between changing the state and writing
        // the log record, since otherwise it might try to change the state
        // to the current state (which causes an assertion failure).

	W_DO(_xct_ended(xct_end_abort));
    }  else  {
	// must hold chkpt_serial_m to make _xlist.remove safe
	W_COERCE(_xct_ended(xct_end_nolog));

        /*
         *  Free all locks. Don't free exts as there shouldn't be any to free.
         */
        W_COERCE( lm->unlock_duration(t_long, true, true) );
    }

    me()->detach_xct(this);        // no transaction for this thread
    INC_TSTAT(abort_xct_cnt);
    return RCOK;
}


/*********************************************************************
 *
 *  xct_t::enter2pc(...)
 *
 *  Mark this tx as a thread of a global tx (participating in EXTERNAL
 *  2PC)
 *
 *********************************************************************/
rc_t 
xct_t::enter2pc(const gtid_t &g)
{
    W_DO(check_one_thread_attached());// ***NOT*** checked in prologue
    w_assert1(_core->_state == xct_active);

    if(is_extern2pc()) {
        return RC(eEXTERN2PCTHREAD);
    }
    _core->_global_tid = new gtid_t; //deleted when xct goes away
    if(!_core->_global_tid) {
        W_FATAL(eOUTOFMEMORY);
    }
    DBGX(<<"ente2pc for tid " << tid() 
        << " global tid is " << g);
    *_core->_global_tid = g;

    return RCOK;
}


/*********************************************************************
 *
 *  xct_t::save_point(lsn)
 *
 *  Generate and return a save point in "lsn".
 *
 *********************************************************************/
rc_t
xct_t::save_point(lsn_t& lsn)
{
    // cannot do this with >1 thread attached
    // W_DO(check_one_thread_attached()); // now checked in prologue
    w_assert1(one_thread_attached());

    lsn = _last_lsn;
    return RCOK;
}


/*********************************************************************
 *
 *  xct_t::dispose()
 *
 *  Make the transaction disappear.
 *  This is only for simulating crashes.  It violates
 *  all tx semantics.
 *
 *********************************************************************/
rc_t
xct_t::dispose()
{
    delete __stats;
    __stats = 0;
    
    W_DO(check_one_thread_attached());
    W_COERCE( lm->unlock_duration(t_long, true, true) );
    ClearAllStoresToFree();
    ClearAllLoadStores();
    _core->_state = xct_ended; // unclean!
    me()->detach_xct(this);
    _xlist.remove(this->_xlink);
    return RCOK;
}


/*********************************************************************
 *
 *  xct_t::_flush_logbuf()
 *
 *  Write the log record buffered and update lsn pointers.
 *
 *********************************************************************/
w_rc_t
xct_t::_flush_logbuf()
{
    DBGX( << " _flush_logbuf: _log_bytes_rsvd " << _log_bytes_rsvd  
            << " _log_bytes_ready " << _log_bytes_ready
            << " _log_bytes_used " << _log_bytes_used);
    // ASSUMES ALREADY PROTECTED BY MUTEX

#ifdef CFG_SHORE_DORA
    acquire_1thread_log_mutex(); // IP: if already owner, it will return immediately
#endif

    w_assert2(is_1thread_log_mutex_mine() || one_thread_attached());

    if (_last_log)  {

        DBGX ( << " xct_t::_flush_logbuf " << _last_lsn);
        // Fill in the _prev field of the log rec if this record hasn't
        // already been compensated.
        _last_log->fill_xct_attr(tid(), _last_lsn);

        //
        // debugging prints a * if this record was written
        // during rollback
        //
        DBGX( << " " 
                << ((char *)(state()==xct_aborting)?"RB":"FW")
                << " approx lsn:" << log->curr_lsn() 
                << " rec:" << *_last_log 
                << " size:" << _last_log->length()  
                << " prevlsn:" << _last_log->prev() 
                );

        LOGTRACE( << setiosflags(ios::right) << _last_lsn
                      << resetiosflags(ios::right) << " I: " << *_last_log 
                      << " ... " );
        if(log) {
        logrec_t* l = _last_log;
        _last_log = 0;
            W_DO( log->insert(*l, &_last_lsn) );
        
        /* LOG_RESERVATIONS

           Now that we know the size of the log record which was
           generated, charge the bytes to the appropriate location.

           Normal log inserts consume /length/ available bytes and
           reserve an additional /length/ bytes against future
           rollbacks; undo records consume /length/ previously
           reserved bytes and leave available bytes unchanged..
           
           NOTE: we only track reservations during forward
           processing. The SM can no longer run out of log space,
           so during recovery we can assume that the log was not
           wedged at the time of the crash and will not become so
           during recovery (because redo generates only log
           compensations and undo was already accounted for)
        */
        if(smlevel_0::operating_mode == t_forward_processing) {
        long bytes_used = l->length();
        if(_rolling_back || state() != xct_active) {
#if USE_LOG_RESERVATIONS
            w_assert0(_log_bytes_rsvd >= bytes_used);
            _log_bytes_rsvd -= bytes_used;
#else
            if(_log_bytes_rsvd >= bytes_used) {
                _log_bytes_rsvd -= bytes_used;
            }
            else if(_log_bytes_ready >= bytes_used) {
                _log_bytes_ready -= bytes_used;
            }
            else {
                w_ostrstream trouble;
                trouble << "Log reservations disabled." << endl;
                trouble << " tid " << tid() 
                    << " state " << state() 
                    << "_log_bytes_ready " << _log_bytes_ready
                    << "_log_bytes_rsvd " << _log_bytes_rsvd 
                    << " bytes_used " << bytes_used
                    << endl;
                fprintf(stderr, "%s\n", trouble.c_str());
                // return RC(eOUTOFLOGSPACE);
            }
#endif
        }
            else {
                long to_reserve = UNDO_FUDGE_FACTOR(bytes_used);
                w_assert0(_log_bytes_ready >= bytes_used + to_reserve);
                _log_bytes_ready -= bytes_used + to_reserve;
                _log_bytes_rsvd += to_reserve;
            }
            _log_bytes_used += bytes_used;
        }

            // log insert effectively set_lsn to the lsn of the *next* byte of
            // the log.
            if ( ! _first_lsn.valid())  
                    _first_lsn = _last_lsn;
    
            _undo_nxt = (
                    l->is_undoable_clr() ? _last_lsn :
                    l->is_cpsn() ? l->undo_nxt() : _last_lsn);
        } // log non-null
    }

    return RCOK;
}

/*********************************************************************
 *
 *  xct_t::_sync_logbuf()
 *
 *  Force log entries up to the most recently written to disk.
 *
 *  block: If not set it does not block, but kicks the flusher. The
 *         default is to block, the no block option is used by AsynchCommit
 *
 *********************************************************************/
w_rc_t
xct_t::_sync_logbuf(bool block)
{
    return (log? log->flush(_last_lsn,block) : RCOK);
}

/*********************************************************************
 *
 *  xct_t::get_logbuf(ret)
 *  xct_t::give_logbuf(ret, page)
 *
 *  Flush the logbuf and return it in "ret" for use. Caller must call
 *  give_logbuf(ret) to free it after use.
 *  Leaves the xct's log mutex acquired
 *
 *  These are used in the log_stub.i functions
 *  and ONLY there.  THE ERROR RETURN (running out of log space)
 *  IS PREDICATED ON THAT -- in that it's expected that in the case of
 *  a normal  return (no error), give_logbuf will be called, but in
 *  the error case (out of log space), it will not, and so we must
 *  release the mutex in get_logbuf error cases.
 *
 *********************************************************************/
rc_t 
xct_t::get_logbuf(logrec_t*& ret, page_p const* p)
{
    // protect the log buf that we'll return
    acquire_1thread_log_mutex(); // this is get_logbuf
    ret = 0;

    INC_TSTAT(get_logbuf);

    // Instead of flushing here, we'll flush at the end of give_logbuf()
    // and assert here that we've got nothing buffered:
    w_assert1(!_last_log);

    /* LOG_RESERVATIONS
    // logrec_t is 3 pages, even though the real record is shorter.

       Make sure we have enough space, both to continue now and to
       guarantee the ability to roll back should something go wrong
       later. This means we need to reserve double space for each log
       record inserted (one for now, one for the potential undo).

       Unfortunately, we don't actually know the size of the log
       record to be inserted, so we have to be conservative and assume
       maximum size. Similarly, we don't know whether we'll eventually
       abort. We'll deal with the former by adjusting our reservation
       in _flush_logbuf, where we do know the log record's size; we deal
       with the undo reservation at commit time, releasing it all en
       masse.

       NOTE: during rollback we don't check reservations because undo
       was already paid-for when the original record was inserted.

       NOTE: we require three logrec_t worth of reservation: one each
       for forward and undo of the log record we're about to insert,
       and the third to handle asymmetric log records required to end
       the transaction, such as the commit/abort record and any
       top-level actions generated unexpectedly during rollback.
     */
    static u_int const MIN_BYTES_READY = 2*sizeof(logrec_t) + 
                                         UNDO_FUDGE_FACTOR(sizeof(logrec_t));
    static u_int const MIN_BYTES_RSVD =  sizeof(logrec_t);
    if(!_rolling_back && _core->_state == xct_active
       && _log_bytes_ready < MIN_BYTES_READY) {
        fileoff_t needed = MIN_BYTES_READY;
        if(!log->reserve_space(needed)) {
            /*
               Yikes! Log full!

               In order to reclaim space the oldest log partition must
               have no dirty pages or active transactions associated
               with it. If we're one of those overly old transactions
               we have no choice but to abort.
             */
            INC_TSTAT(log_full);
            bool badnews=false;
            if(_first_lsn.valid() && _first_lsn.hi() == 
                    log->global_min_lsn().hi()) {
                INC_TSTAT(log_full_old_xct);
                badnews = true;
            }

            lpid_t const* pid = p? &p->pid() : 0;
            if(!badnews && !bf->force_my_dirty_old_pages(pid)) {
                /* Dirty pages which I hold EX latches on pose a
                   significantly thornier question. On the face of it,
                   the WAL protocol suggests that we can write out any
                   such page without fear of data corruption in the
                   event of a crash.

                   However, the page_lsn is set by the WAL operation,
                   so pages not the subject of this log record are out
                   right off the bat (we have no way to know if
                   they've been changed yet).

                   If it's the current page that's the problem, the
                   only time we'd need to worry is if this and some
                   previous WAL are somehow intertwined, in the sense
                   that it's not actually safe to unlatch the page
                   before the second WAL sticks. This seems like a
                   supremely Bad Idea that (hopefully) none of our
                   code relies on...
                   
                   So, in the end, we assume we can flush page we're
                   about to WAL, if it happens to be old-dirty, but
                   any other old-dirty-EX page we hold will force us
                   to abort.

                   If we decide to play it safe, we just have to send
                   0 insetead of p to the buffer pool query.
                 */
                INC_TSTAT(log_full_old_page);
                badnews = true;
            }

            if(!badnews) {
                /* Now it's safe to wait for more log space to open up
                   But before we do anything, let's try to grab the
                   chkpt serial mutex so ongoing checkpoint has a
                   chance to complete before we go crazy.
                */
                chkpt_serial_m::trx_acquire(); // wait for chkpt to finish
                chkpt_serial_m::trx_release();

                static queue_based_block_lock_t emergency_log_flush_mutex;
                CRITICAL_SECTION(cs, emergency_log_flush_mutex);                
                for(int tries_left=3; tries_left > 0; tries_left--) {
                    if(tries_left == 1) {
                    // the checkpoint should also do this, but just in case...
                    lsn_t target = log_m::first_lsn(log->global_min_lsn().hi()+1);
                    w_rc_t rc = bf->force_until_lsn(target, false);
                    // did the force succeed?
                    if(rc.is_error()) {
                        INC_TSTAT(log_full_giveup);
                        fprintf(stderr, "Log recovery failed\n");
#if W_DEBUG_LEVEL > 0
                        extern void dump_all_sm_stats();
                        dump_all_sm_stats();
#endif
                        release_1thread_log_mutex();  // this is get_logbuf
                        if(rc.err_num() == eBPFORCEFAILED) 
                        return RC(eOUTOFLOGSPACE);
                        return rc;
                    }
                    }

                    // most likely it's well aware, but just in case...
                    chkpt->wakeup_and_take();
                    W_IGNORE(log->wait_for_space(needed, 100));
                    if(!needed) {
                    if(tries_left > 1) {
                        INC_TSTAT(log_full_wait);
                    }
                    else {
                        INC_TSTAT(log_full_force);
                    }
                    goto success;
                    }
                    
                    // try again...
                }

                if(!log->reserve_space(needed)) {
                    // won't do any good now...
                    log->release_space(MIN_BYTES_READY - needed); 
                    
                    // nothing's working... give up and abort
                    stringstream tmp;
                    tmp << "Log too full. me=" << me()->id
                    << " pthread=" << pthread_self()
                    << ": min_chkpt_rec_lsn=" << log->min_chkpt_rec_lsn()
                    << ", curr_lsn=" << log->curr_lsn() 
                    // also print info that shows why we didn't croak
                    // on the first check
                    << "; space left " << log->space_left()
                    << endl;
                    fprintf(stderr, "%s\n", tmp.str().c_str());
                    INC_TSTAT(log_full_giveup);
                    badnews = true;
                }
            }

            if(badnews) {
                release_1thread_log_mutex();  // this is get_logbuf
                return RC(eOUTOFLOGSPACE);
            }
        }
    success:
        _log_bytes_ready += MIN_BYTES_READY;
        DBGX( << " get_logbuf: _log_bytes_rsvd " << _log_bytes_rsvd  
            << " _log_bytes_ready " << _log_bytes_ready
            << " _log_bytes_used " << _log_bytes_used
            );
    }

    /* Transactions must make some log entries as they complete
       (commit or abort), so we have to have always some bytes
       reserved. This is the third of those three logrec_t in
       MIN_BYTES_READY, so we don't have to reserve more ready bytes
       just because of this.
     */
    if(!_rolling_back && _core->_state == xct_active
                && _log_bytes_rsvd < MIN_BYTES_RSVD) {
    _log_bytes_ready -= MIN_BYTES_RSVD;
    _log_bytes_rsvd += MIN_BYTES_RSVD;
    }
    
    DBGX( << " get_logbuf: _log_bytes_rsvd " << _log_bytes_rsvd  
            << " _log_bytes_ready " << _log_bytes_ready
            << " _log_bytes_used " << _log_bytes_used
            );
    ret = _last_log = _log_buf;

    // We hold the mutex to protect the log buf we are returning.
    w_assert3(is_1thread_log_mutex_mine());

    return RCOK;
}

// See comments above get_logbuf, above
rc_t
xct_t::give_logbuf(logrec_t* l, const page_p *page)
{
    FUNC(xct_t::give_logbuf);
    DBGX(<<"_last_log contains: "   << *l );
        
    // ALREADY PROTECTED from get_logbuf() call
    w_assert2(is_1thread_log_mutex_mine());

    w_assert1(l == _last_log);

    // WAL: hang onto last mod page so we can
    // keep it from being flushed before the log
    // is written.  The log isn't synced here, but the buffer
    // manager does a log flush to the lsn of the page before
    // it writes the page, which makes the log record durable
    // before the page gets written.   Our job here is to
    // get the lsn into the page before we unfix the page.
    page_p last_mod_page;

    if(page != (page_p *)0) {
        // Should already be EX-latched since it's the last modified page!
        w_assert1(page->latch_mode() == LATCH_EX);

        last_mod_page = *page; // refixes
    } 
    else if (l->shpid())  
    {
        // Those records with page ids stuffed in with fill() 
        // should match those whose 2nd argument (page) is non-null,
        // so those should hit the above case.
        // Exception: alloc_file_page_log does not need the
        // page latched, so it is called "logical",
        // but does stuff the pid into the log record.
        // So for that log record, having a pid doesn't mean that
        // the page is latched or is an updated page.  We don't
        // have to worry about WAL for this page, because the
        // log records that write the page are for the format, which
        // is yet to come.
        if(l->type() != logrec_t::t_alloc_file_page) {
            W_FATAL(eINTERNAL); // we shouldn't get here.
        }
    } else {
        // This log record doesn't use a page.
        w_assert1(l->tag() == 0);
    }

    w_assert2(is_1thread_log_mutex_mine());

    if(last_mod_page.is_fixed()) { // i.e., is  fixed
        w_assert2(l->tag()!=0);
        w_assert2(last_mod_page.latch_mode() == LATCH_EX);
    } else {
        w_assert2(l->tag()==0);
        w_assert3(last_mod_page.latch_mode() == LATCH_NL);
    }

    rc_t rc = _flush_logbuf(); 
                      // stuffs tid, _last_lsn into our record,
                      // then inserts it into the log, getting _last_lsn
    if(rc.is_error())
    goto done;
    
    if(last_mod_page.is_fixed() ) {
        w_assert2(last_mod_page.latch_mode() == LATCH_EX);
        // WAL: stuff the lsn into the page so the buffer manager
        // can force the log to that lsn before writing the page.
        last_mod_page.set_lsns(_last_lsn);
        last_mod_page.unfix_dirty();
        w_assert1(last_mod_page.check_lsn_invariant());
    }

 done:
    release_1thread_log_mutex(); // this is give_logbuf
    // We no longer hold the mutex to protect the log buf.
    return rc;
}


/*********************************************************************
 *
 *  xct_t::release_anchor(and_compensate)
 *
 *  stop critical sections vis-a-vis compensated operations
 *  If and_compensate==true, it makes the _last_log a clr
 *
 *********************************************************************/
void
xct_t::release_anchor( bool and_compensate ADD_LOG_COMMENT_SIG )
{
    FUNC(xct_t::release_anchor);

#if X_LOG_COMMENT_ON
    if(and_compensate) {
        w_ostrstream s;
        s << "release_anchor at " 
            << debugmsg;
        W_COERCE(log_comment(s.c_str()));
    }
#endif
    w_assert3(is_1thread_log_mutex_mine());
    DBGX(    
            << " RELEASE ANCHOR " 
            << " in compensated op==" << _in_compensated_op
            << " holds xct_mutex_1==" 
            /*<< (const char *)(_1thread_xct.is_mine()? "true" : "false"*)*/
    );

    w_assert3(_in_compensated_op>0);

    if(_in_compensated_op == 1) { // will soon be 0

        // NB: this whole section could be made a bit
        // more efficient in the -UDEBUG case, but for
        // now, let's keep in all the checks

        // don't flush unless we have popped back
        // to the last compensate() of the bunch

        // Now see if this last item was supposed to be
        // compensated:
        if(and_compensate && (_anchor != lsn_t::null)) {
           VOIDSSMTEST("compensate");
           if(_last_log) {
               if ( _last_log->is_cpsn()) {
                    DBGX(<<"already compensated");
                    w_assert3(_anchor == _last_log->undo_nxt());
               } else {
                   DBGX(<<"SETTING anchor:" << _anchor);
                   w_assert3(_anchor <= _last_lsn);
                   _last_log->set_clr(_anchor);
               }
           } else {
               DBGX(<<"no _last_log:" << _anchor);
               /* Can we update the log record in the log buffer ? */
               if( log && 
                   !log->compensate(_last_lsn, _anchor).is_error()) {
                   // Yup.
                    INC_TSTAT(compensate_in_log);
               } else {
                   // Nope, write a compensation log record.
                   // Really, we should return an rc from this
                   // method so we can W_DO here, and we should
                   // check for eBADCOMPENSATION here and
                   // return all other errors  from the
                   // above log->compensate(...)
                   
                   // compensations use _log_bytes_rsvd, 
                   // not _log_bytes_ready
                   bool was_rolling_back = _rolling_back;
                   _rolling_back = true;
                   W_COERCE(log_compensate(_anchor));
                   _rolling_back = was_rolling_back;
                   INC_TSTAT(compensate_records);
               }
            }
        }

        _anchor = lsn_t::null;

    }
    // UN-PROTECT 
    _in_compensated_op -- ;

    DBGX(    
        << " out compensated op=" << _in_compensated_op
        << " holds xct_mutex_1==" 
        /*        << (const char *)(_1thread_xct.is_mine()? "true" : "false")*/
    );
    release_1thread_log_mutex(); // this is release_anchor
}

/*********************************************************************
 *
 *  xct_t::anchor( bool grabit )
 *
 *  Return a log anchor (begin a top level action).
 *
 *  If argument==true (most of the time), it stores
 *  the anchor for use with compensations.  
 *
 *  When the  argument==false, this is used (by I/O monitor) not
 *  for compensations, but only for concurrency control.
 *
 *********************************************************************/
/**\todo Figure whether mtxct will need/support grabit=false */
const lsn_t& 
xct_t::anchor(bool grabit)
{
    // PROTECT
    acquire_1thread_log_mutex(); // this is anchor
    _in_compensated_op ++;

    INC_TSTAT(anchors);
    DBGX(    
            << " GRAB ANCHOR " 
            << " in compensated op==" << _in_compensated_op
    );


    if(_in_compensated_op == 1 && grabit) {
        // _anchor is set to null when _in_compensated_op goes to 0
        w_assert3(_anchor == lsn_t::null);
        _anchor = _last_lsn;
        DBGX(    << " anchor =" << _anchor);
    }
    DBGX(    << " anchor returns " << _last_lsn );

    return _last_lsn;
}


/*********************************************************************
 *
 *  xct_t::compensate_undo(lsn)
 *
 *  compensation during undo is handled slightly differently--
 *  the gist of it is the same, but the assertions differ, and
 *  we have to acquire the mutex first
 *********************************************************************/
void 
xct_t::compensate_undo(const lsn_t& lsn)
{
    DBGX(    << " compensate_undo (" << lsn << ") -- state=" << state());

    acquire_1thread_log_mutex();  // this is compensate_undo
    w_assert3(_in_compensated_op);
    // w_assert9(state() == xct_aborting); it's active if in sm::rollback_work

    _compensate(lsn, _last_log?_last_log->is_undoable_clr() : false);

    release_1thread_log_mutex(); // this is compensate_undo
}

/*********************************************************************
 *
 *  xct_t::compensate(lsn, bool undoable)
 *
 *  Generate a compensation log record to compensate actions 
 *  started at "lsn" (commit a top level action).
 *  Generates a new log record only if it has to do so.
 *
 *********************************************************************/
void 
xct_t::compensate(const lsn_t& lsn, bool undoable ADD_LOG_COMMENT_SIG)
{
    DBGX(    << " compensate(" << lsn << ") -- state=" << state());

    // acquire_1thread_mutex(); should already be mine
    w_assert3(is_1thread_log_mutex_mine());

    _compensate(lsn, undoable);

    w_assert3(is_1thread_log_mutex_mine());
    release_anchor(true ADD_LOG_COMMENT_USE);
}

/*********************************************************************
 *
 *  xct_t::_compensate(lsn, bool undoable)
 *
 *  
 *  Generate a compensation log record to compensate actions 
 *  started at "lsn" (commit a top level action).
 *  Generates a new log record only if it has to do so.
 *
 *  Special case of undoable compensation records is handled by the
 *  boolean argument. (NOT USED FOR NOW -- undoable_clrs were removed
 *  in 1997 b/c they weren't needed anymore;they were originally
 *  in place for an old implementation of extent-allocation. That's
 *  since been replaced by the dealaying of store deletion until end
 *  of xct).  The calls to the methods and infrastructure regarding
 *  undoable clrs was left in place in case it must be resurrected again.
 *  The reason it was removed is that there was some complexity involved
 *  in hanging onto the last log record *in the xct* in order to be
 *  sure that the compensation happens *in the correct log record*
 *  (because an undoable compensation means the log record holding the
 *  compensation isn't being compensated around, whereas turning any
 *  other record into a clr or inserting a stand-alone clr means the
 *  last log record inserted is skipped on undo).
 *  That complexity remains, since log records are flushed to the log
 *  immediately now (which was precluded for undoable_clrs ).
 *
 *********************************************************************/
void 
xct_t::_compensate(const lsn_t& lsn, bool undoable)
{
    DBGX(    << "_compensate(" << lsn << ") -- state=" << state());

    bool done = false;
    if ( _last_log ) {
        // We still have the log record here, and
        // we can compensate it.
        // NOTE: we used to use this a lot but now the only
        // time this is possible (due to the fact that we flush
        // right at insert) is when the logging code is hand-written,
        // rather than Perl-generated.

        /*
         * lsn is got from anchor(), and anchor() returns _last_lsn.
         * _last_lsn is the lsn of the last log record
         * inserted into the log, and, since
         * this log record hasn't been inserted yet, this
         * function can't make a log record compensate to itself.
         */
        w_assert3(lsn <= _last_lsn);
        _last_log->set_clr(lsn);
        INC_TSTAT(compensate_in_xct);
        done = true;
    } else {
        /* 
        // Log record has already been inserted into the buffer.
        // Perhaps we can update the log record in the log buffer.
        // However,  it's conceivable that nothing's been written
        // since _last_lsn, and we could be trying to compensate
        // around nothing.  This indicates an error in the calling
        // code.
        */
        if( lsn >= _last_lsn) {
            INC_TSTAT(compensate_skipped);
        }
        if( log && (! undoable) && (lsn < _last_lsn)) {
            if(!log->compensate(_last_lsn, lsn).is_error()) {
                INC_TSTAT(compensate_in_log);
                done = true;
            }
        }
    }
    w_assert3(is_1thread_log_mutex_mine());

    if( !done && (lsn < _last_lsn) ) {
        /*
        // If we've actually written some log records since
        // this anchor (lsn) was grabbed, 
        // force it to write a compensation-only record
        // either because there's no record on which to 
        // piggy-back the compensation, or because the record
        // that's there is an undoable/compensation and will be
        // undone (and we *really* want to compensate around it)
        */

        // compensations use _log_bytes_rsvd, not _log_bytes_ready
        bool was_rolling_back = _rolling_back;
        _rolling_back = true;
        W_COERCE(log_compensate(lsn));
        _rolling_back = was_rolling_back;
        INC_TSTAT(compensate_records);
    }
}



/*********************************************************************
 *
 *  xct_t::rollback(savept)
 *
 *  Rollback transaction up to "savept".
 *
 *********************************************************************/
rc_t
xct_t::rollback(const lsn_t &save_pt)
{
    FUNC(xct_t::rollback);
    // W_DO(check_one_thread_attached()); // now checked in prologue
    w_assert1(one_thread_attached());

    if(!log) { 
        ss_m::errlog->clog  << emerg_prio
        << "Cannot roll back with logging turned off. " 
        << flushl; 
        return RC(eNOABORT);
    }

    w_rc_t            rc;
    logrec_t*         buf =0;

    // MUST PROTECT anyway, since this generates compensations
    acquire_1thread_log_mutex(); // this is rollback

    if(_in_compensated_op > 0) {
        w_assert3(save_pt >= _anchor);
    } else {
        w_assert3(_anchor == lsn_t::null);
    }

    DBGX( << " in compensated op depth " <<  _in_compensated_op
            << " save_pt " << save_pt << " anchor " << _anchor);
    _in_compensated_op++;

    // rollback is only one type of compensated op, and it doesn't nest
    w_assert0(!_rolling_back); 
    _rolling_back = true; 

    lsn_t nxt = _undo_nxt;

    LOGTRACE( << "abort begins at " << nxt);

    { // Contain the scope of the following __copy__buf:

    logrec_t* __copy__buf = new logrec_t; // auto-del
    if(! __copy__buf) { W_FATAL(eOUTOFMEMORY); }
    w_auto_delete_t<logrec_t> auto_del(__copy__buf);
    logrec_t&         r = *__copy__buf;

    while (save_pt < nxt)  {
        rc =  log->fetch(nxt, buf, 0);
        if(rc.is_error() && rc.err_num()==eEOF) {
            DBGX(<< " fetch returns EOF" );
            log->release(); 
            goto done;
        } else
        {
             logrec_t& temp = *buf;
             w_assert3(!temp.is_skip());
          
             /* Only copy the valid portion of 
              * the log record, then release it 
              */
             memcpy(__copy__buf, &temp, temp.length());
             log->release();
        }

        if (r.is_undo()) {
            /*
             *  Undo action of r.
             */
            LOGTRACE( << setiosflags(ios::right) << nxt
                      << resetiosflags(ios::right) << " U: " << r 
                      << " ... " );

#if W_DEBUG_LEVEL > 2
        u_int    was_rsvd = _log_bytes_rsvd;
#endif 
            lpid_t pid = r.construct_pid();
            page_p page;

            if (! r.is_logical()) {
                store_flag_t store_flags = st_bad;
#define TMP_NOFLAG 0
                rc = page.fix(pid, page_p::t_any_p, LATCH_EX, 
                    TMP_NOFLAG, store_flags);
                if(rc.is_error()) {
                    goto done;
                }
                w_assert1(page.pid() == pid);
            }


            r.undo(page.is_fixed() ? &page : 0);

#if W_DEBUG_LEVEL > 2
            if(was_rsvd - _log_bytes_rsvd  > r.length()) {
                  LOGTRACE(<< " len=" << r.length() << " B= " <<
                          (was_rsvd - _log_bytes_rsvd));
            }
#endif 
            if(r.is_cpsn()) {
                LOGTRACE( << " compensating to " << r.undo_nxt() );
                nxt = r.undo_nxt();
            } else {
                LOGTRACE( << " undoing to " << r.prev() );
                nxt = r.prev();
            }

        } else  if (r.is_cpsn())  {
            LOGTRACE( << setiosflags(ios::right) << nxt
                      << resetiosflags(ios::right) << " U: " << r 
                      << " compensating to" << r.undo_nxt() );
            nxt = r.undo_nxt();
            // r.prev() could just as well be null

        } else {
            LOGTRACE( << setiosflags(ios::right) << nxt
              << resetiosflags(ios::right) << " U: " << r 
                      << " skipping to " << r.prev());
            nxt = r.prev();
            // w_assert9(r.undo_nxt() == lsn_t::null);
        }
    }

    // close scope so the
    // auto-release will free the log rec copy buffer, __copy__buf
    }

    _undo_nxt = nxt;

done:

    DBGX( << "leaving rollback: compensated op " << _in_compensated_op);
    _in_compensated_op --;
    _rolling_back = false;
    w_assert3(_anchor == lsn_t::null ||
                _anchor == save_pt);
    release_1thread_log_mutex(); // this is rollback

    if(save_pt != lsn_t::null) {
        INC_TSTAT(rollback_savept_cnt);
    }

    return rc;
}

void xct_t::AddStoreToFree(const stid_t& stid)
{
    FUNC(x);
    CRITICAL_SECTION(xctstructure, *this);
    _core->_storesToFree.push(new stid_list_elem_t(stid));
}

void xct_t::AddLoadStore(const stid_t& stid)
{
    FUNC(x);
    CRITICAL_SECTION(xctstructure, *this);
    _core->_loadStores.push(new stid_list_elem_t(stid));
}

/*
 * clear the list of stores to be freed upon xct completion
 * this is used by abort since rollback will recreate the
 * proper list of stores to be freed.
 *
 * don't *really* need mutex since only called when aborting => 1 thread
 * but we have the acquire here for internal documentation 
 */
void
xct_t::ClearAllStoresToFree()
{
    w_assert2(one_thread_attached());
    stid_list_elem_t*        s = 0;
    while ((s = _core->_storesToFree.pop()))  {
        delete s;
    }

    w_assert3(_core->_storesToFree.is_empty());
}


/*
 * this function will free all the stores which need to freed
 * by this completing xct.
 *
 * don't REALLY need mutex since only called when committing/aborting
 * => 1 thread attached
 */
void
xct_t::FreeAllStoresToFree()
{
    w_assert2(one_thread_attached());
    stid_list_elem_t*        s = 0;
    while ((s = _core->_storesToFree.pop()))  {
        W_COERCE( io->free_store_after_xct(s->stid) );
        delete s;
    }
}

/*
 * this method returns the # extents allocated to stores
 * marked for deletion by this xct
 */
void
xct_t::num_extents_marked_for_deletion(base_stat_t &num)
{
    FUNC(xct_t::num_extents_marked_for_deletion);
    CRITICAL_SECTION(xctstructure, *this);
    stid_list_elem_t*        s = 0;
    num = 0;
    SmStoreMetaStats _stats;
    base_stat_t j;

    w_list_i<stid_list_elem_t,queue_based_lock_t>        i(_core->_storesToFree);
    while ((s = i.next()))  {
        _stats.Clear();
        W_COERCE( io->get_store_meta_stats(s->stid, _stats) );
        j = _stats.numReservedPages; 
        w_assert3((j % ss_m::ext_sz) == 0);
        j /= ss_m::ext_sz;
        num += j;
    }
}

rc_t
xct_t::PrepareLogAllStoresToFree()
{
    // This check is more for internal documentation than for anything
    w_assert1(one_thread_attached());

    stid_t* stids = new stid_t[prepare_stores_to_free_t::max]; // auto-del
    w_auto_delete_array_t<stid_t> auto_del_stids(stids);

    stid_list_elem_t*              e;
    uint4_t                        num = 0;
    {
        w_list_i<stid_list_elem_t,queue_based_lock_t>        i(_core->_storesToFree);
        while ((e = i.next()))  {
            stids[num++] = e->stid;
            if (num >= prepare_stores_to_free_t::max)  {
                W_DO( log_xct_prepare_stores(num, stids) );
                num = 0;
            }
        }
    }
    if (num > 0)  {
        W_DO( log_xct_prepare_stores(num, stids) );
    }

    return RCOK;
}


void
xct_t::DumpStoresToFree()
{
    stid_list_elem_t*                e;
    w_list_i<stid_list_elem_t,queue_based_lock_t>        i(_core->_storesToFree);

    FUNC(xct_t::DumpStoresToFree);
    CRITICAL_SECTION(xctstructure, *this);
    cout << "list of stores to free";
    while ((e = i.next()))  {
        cout << " <- " << e->stid;
    }
    cout << endl;
}

/*
 * Moved here to work around a gcc/egcs bug that
 * caused compiler to choke.
 */
class VolidCnt {
    private:
        int unique_vols;
        int vol_map[xct_t::max_vols];
        snum_t vol_cnts[xct_t::max_vols];
    public:
        VolidCnt() : unique_vols(0) {};
        int Lookup(int vol)
            {
                for (int i = 0; i < unique_vols; i++)
                    if (vol_map[i] == vol)
                        return i;
                
                w_assert9(unique_vols < xct_t::max_vols);
                vol_map[unique_vols] = vol;
                vol_cnts[unique_vols] = 0;
                return unique_vols++;
            };
        int Increment(int vol)
            {
                return ++vol_cnts[Lookup(vol)];
            };
        int Decrement(int vol)
            {
                w_assert9(vol_cnts[Lookup(vol)]);
                return --vol_cnts[Lookup(vol)];
            };
#if W_DEBUG_LEVEL > 2
        ~VolidCnt()
            {
                for (int i = 0; i < unique_vols; i ++)
                    w_assert9(vol_cnts[i] == 0);
            };
#endif 
};

rc_t
xct_t::ConvertAllLoadStoresToRegularStores()
{

#if X_LOG_COMMENT_ON
    {
        static int volatile uniq=0;
        static int volatile last_uniq=0;
        int    nv =  atomic_inc_nv(uniq);
        //Even if someone else slips in here, the
        //values should never match. 
        w_assert1(last_uniq != nv);
        *&last_uniq = nv;

        // this is to help us figure out if we
        // are issuing duplicate commits/log entries or
        // if the problem is in the log code or the
        // grabbing of the 1thread log mutex 
        w_ostrstream s;
        s << "ConvertAllLoadStores uniq=" << uniq 
            << " xct state " << _state
            << " xct ended " << _xct_ended
            << " tid " << tid()
            << " thread " << me()->id;
        W_DO(log_comment(s.c_str()));
    }
#endif

    w_assert2(one_thread_attached());
    stid_list_elem_t*        s = 0;
    VolidCnt cnt;

    {
        w_list_i<stid_list_elem_t,queue_based_lock_t> i(_core->_loadStores);

        while ((s = i.next()))  {
            cnt.Increment(s->stid.vol);
        }
    }

    while ((s = _core->_loadStores.pop()))  {
        bool sync_volume = (cnt.Decrement(s->stid.vol) == 0);
        store_flag_t f;
        W_DO( io->get_store_flags(s->stid, f));
        // if any combo of  st_tmp, st_insert_file, st_load_file, convert
        // but only insert and load are put into this list.
        if(f != st_regular) {
            W_DO( io->set_store_flags(s->stid, st_regular, sync_volume) );
        }
        delete s;
    }

    w_assert3(_core->_loadStores.is_empty());

    return RCOK;
}


void
xct_t::ClearAllLoadStores()
{
    w_assert2(one_thread_attached());
    stid_list_elem_t*        s = 0;
    while ((s = _core->_loadStores.pop()))  {
        delete s;
    }

    w_assert3(_core->_loadStores.is_empty());
}

smlevel_0::concurrency_t                
xct_t::get_lock_level()  
{ 
    FUNC(xct_t::get_lock_level);
    CRITICAL_SECTION(xctstructure, *this);
    smlevel_0::concurrency_t l = t_cc_bad;
    l =  convert(lock_info()->lock_level());
    return l;
}

void                           
xct_t::lock_level(concurrency_t l) 
{
    FUNC(xct_t::lock_level);
    CRITICAL_SECTION(xctstructure, *this);
    lockid_t::name_space_t n = convert(l);
    if(n != lockid_t::t_bad) {
        lock_info()->set_lock_level(n);
    }
}

void 
xct_t::attach_thread() 
{
    FUNC(xct_t::attach_thread);
    CRITICAL_SECTION(xctstructure, *this);

    w_assert2(is_1thread_xct_mutex_mine());
    int nt=atomic_inc_nv(_core->_threads_attached);
    if(nt > 1) {
        INC_TSTAT(mpl_attach_cnt);
    }
    w_assert2(_core->_threads_attached >=0);
    w_assert2(is_1thread_xct_mutex_mine());
    me()->new_xct(this);
    w_assert2(is_1thread_xct_mutex_mine());
}


void
xct_t::detach_thread() 
{
    FUNC(xct_t::detach_thread);
    CRITICAL_SECTION(xctstructure, *this);
    w_assert3(is_1thread_xct_mutex_mine());

    atomic_dec(_core->_threads_attached);
    w_assert2(_core->_threads_attached >=0);
    me()->no_xct(this);
}

w_rc_t
xct_t::lockblock(timeout_in_ms timeout)
{
// Used by lock manager. (lock_core_m)
// Another thread in our xct is blocking. We're going to have to
// wait on another resource, until our partner thread unblocks,
// and then try again.
    CRITICAL_SECTION(bcs, _core->_waiters_mutex);
    w_rc_t rc;
    if(num_threads() > 1) {
        DBGX(<<"blocking on condn variable");
        // Don't block if other thread has gone away
        // GNATS 119 This is still racy!  multi.1 shows us that.
        //
        xct_lock_info_t*       the_xlinfo = lock_info();
        if(! the_xlinfo->waiting_request()) {
            // No longer has waiting request - return w/o blocking.
            return RCOK;
        }

        // this code taken from scond_t implementation, from when
        // _waiters_cond was an scond_t:
        if(timeout == WAIT_IMMEDIATE)  return RC(sthread_t::stTIMEOUT);
        if(timeout == WAIT_FOREVER)  {
            DO_PTHREAD(pthread_cond_wait(&_core->_waiters_cond, &_core->_waiters_mutex));
        } else {
            struct timespec when;
            sthread_t::timeout_to_timespec(timeout, when);
            DO_PTHREAD_TIMED(pthread_cond_timedwait(&_core->_waiters_cond, &_core->_waiters_mutex, &when));
        }

        DBGX(<<"not blocked on cond'n variable");
    }
    if(rc.is_error()) {
        return RC_AUGMENT(rc);
    } 
    return rc;
}

void
xct_t::lockunblock()
{
// Used by lock manager. (lock_core_m)
// This thread in our xct is no longer blocking. Wake up anyone
// who was waiting on this other resource because I was 
// blocked in the lock manager.
    CRITICAL_SECTION(bcs, _core->_waiters_mutex);
    if(num_threads() > 1) {
        DBGX(<<"signalling waiters on cond'n variable");
        DO_PTHREAD(pthread_cond_broadcast(&_core->_waiters_cond));
        DBGX(<<"signalling cond'n variable done");
    }
}

//
// one_thread_attached() does not acquire the 1thread mutex; it
// just checks that the vas isn't calling certain methods
// when other threads are still working on behalf of the same xct.
// It doesn't protect the vas from trying calling, say, commit and
// later attaching another thread while the commit is going on.
// --- Can't protect a vas from itself in all cases.
rc_t
xct_t::check_one_thread_attached() const
{
    if(one_thread_attached()) return RCOK;
    return RC(eTWOTHREAD);
}

bool
xct_t::one_thread_attached() const
{
    // wait for the checkpoint to finish
    if( _core->_threads_attached > 1) {
        chkpt_serial_m::trx_acquire();
        if( _core->_threads_attached > 1) {
            chkpt_serial_m::trx_release();
#if W_DEBUG_LEVEL > 2
            fprintf(stderr, 
            "Fatal VAS or SSM error: %s %d %s %d.%d \n",
            "Only one thread allowed in this operation at any time.",
            _core->_threads_attached, 
            "threads are attached to xct",
            tid().get_hi(), tid().get_lo()
            );
#endif
            return false;
        }
	chkpt_serial_m::trx_release();
    }
    return true;
}

bool
xct_t::is_1thread_log_mutex_mine() const
{
  return _1thread_log.is_mine(&me()->get_1thread_log_me());
}
bool
xct_t::is_1thread_xct_mutex_mine() const
{
  return _core->_1thread_xct.is_mine(&me()->get_1thread_xct_me());
}

extern "C" void xctstophere(int c);
void xctstophere(int ) {
}

void
xct_t::acquire_1thread_log_mutex() 
{
    // Ordered acquire: we must not be able to acquire
    // these two mutexen in opposite orders.
    // Normally we acquire the 1thread log mutex, then, if
    // necessary, the 1thread xct mutex, but never the
    // other way around. The 1thread xct mutex just protects
    // the xct structure and is held for a short time only; the
    // 1thread log mutex can be held for the duration of a
    // top-level action.
    w_assert1(is_1thread_xct_mutex_mine() == false);

    if(is_1thread_log_mutex_mine()) {
        DBGX( << " duplicate acquire log mutex: " << _in_compensated_op);
        w_assert0(_in_compensated_op > 0);
        inc_acquire_1thread_log_depth(); // debug build only
        return;
    }
    // the queue_based_lock_t implementation can tell if it was
    // free or held; the w_pthread_lock_t cannot,
    // and always returns false.
    bool was_contended = _1thread_log.acquire(&me()->get_1thread_log_me());
    if(was_contended)
        INC_TSTAT(await_1thread_log);
    INC_TSTAT(acquire_1thread_log);

    DBGX(    << " acquired log mutex: " << _in_compensated_op);
    w_assert0(_in_compensated_op == 0);
    // increment after we have the mutex
    inc_acquire_1thread_log_depth(); // debug build only
}

// Should be used with CRITICAL_SECTION
void
xct_t::acquire_1thread_xct_mutex() const // default: true
{
    // We can already own the 1thread log mutx, if we're
    // in a top-level action or in the io_m.
    DBGX( << " acquire xct mutex");
    if(is_1thread_xct_mutex_mine()) {
        DBGX(<< "already mine");
        return;
    }
    // the queue_based_lock_t implementation can tell if it was
    // free or held; the w_pthread_lock_t cannot,
    // and always returns false.
    bool was_contended = _core->_1thread_xct.acquire(&me()->get_1thread_xct_me());
    if(was_contended) 
        INC_TSTAT(await_1thread_xct);
    DBGX(    << " acquireD xct mutex");
    w_assert2(is_1thread_xct_mutex_mine());
}

void
xct_t::release_1thread_xct_mutex() const
{
    DBGX( << " release xct mutex");
    w_assert3(is_1thread_xct_mutex_mine());
    _core->_1thread_xct.release(me()->get_1thread_xct_me());
    DBGX(    << " releaseD xct mutex");
    w_assert2(!is_1thread_xct_mutex_mine());
}

void
xct_t::release_1thread_log_mutex()
{
    w_assert2(is_1thread_log_mutex_mine());

    dec_acquire_1thread_log_depth();

    DBGX( << " maybe release log mutex: in_compensated_op = " 
            << _in_compensated_op << " holds xct_mutex_1==" 
    /*<< (const char *)(_1thread_xct.is_mine()? "true" : "false")*/
    );

    if(_in_compensated_op==0 ) {
        _1thread_log.release(me()->get_1thread_log_me());
    } else {
        DBGX( << " in compensated operation: can't release log mutex");
    }
}


ostream &
xct_t::dump_locks(ostream &out) const
{
    return lock_info()->dump_locks(out);
}


smlevel_0::switch_t 
xct_t::set_log_state(switch_t s, bool &) 
{
    xct_log_t *mine = me()->xct_log();
    switch_t old = (mine->xct_log_is_off()? OFF: ON);
    if(s==OFF) mine->set_xct_log_off();
    else mine->set_xct_log_on();
    return old;
}

void
xct_t::restore_log_state(switch_t s, bool n ) 
{
    (void) set_log_state(s, n);
}


lockid_t::name_space_t        
xct_t::convert(concurrency_t cc)
{
    switch(cc) {
        case t_cc_record:
                return lockid_t::t_record;

        case t_cc_page:
                return lockid_t::t_page;

        case t_cc_file:
                return lockid_t::t_store;

        case t_cc_vol:
                return lockid_t::t_vol;

        case t_cc_kvl:
        case t_cc_im:
        case t_cc_modkvl:
        case t_cc_bad: 
        case t_cc_none: 
        case t_cc_append:
                return lockid_t::t_bad;
    }
    return lockid_t::t_bad;
}

smlevel_0::concurrency_t                
xct_t::convert(lockid_t::name_space_t n)
{
    switch(n) {
        default:
        case lockid_t::t_bad:
        case lockid_t::t_kvl:
        case lockid_t::t_extent:
            break;

        case lockid_t::t_vol:
            return t_cc_vol;

        case lockid_t::t_store:
            return t_cc_file;

        case lockid_t::t_page:
            return t_cc_page;

        case lockid_t::t_record:
            return t_cc_record;
    }
    W_FATAL(eINTERNAL); 
    return t_cc_bad;
}

NORET
xct_dependent_t::xct_dependent_t(xct_t* xd) : _xd(xd), _registered(false)
{
}

void
xct_dependent_t::register_me() {
    // it's possible that there is no active xct when this
    // function is called, so be prepared for null
    xct_t* xd = _xd;
    if (xd) {
        W_COERCE( xd->add_dependent(this) );
    }
    _registered = true;
}

NORET
xct_dependent_t::~xct_dependent_t()
{
    w_assert2(_registered);
    // it's possible that there is no active xct the constructor
    // was called, so be prepared for null
    if (_link.member_of() != NULL) {
        w_assert1(_xd);
        // Have to remove it under protection of the 1thread_xct_mutex
        W_COERCE(_xd->remove_dependent(this));
    }
}
