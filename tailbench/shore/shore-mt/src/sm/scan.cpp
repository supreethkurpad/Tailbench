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

 $Id: scan.cpp,v 1.158 2010/06/15 17:30:07 nhall Exp $

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
#define SCAN_C
#ifdef __GNUG__
#   pragma implementation
#endif
#include <sm_int_4.h>
#include <sm.h>
#include <lgrec.h>
#include <pin.h>
#include <scan.h>
#include <bf_prefetch.h>
#include <btcursor.h>
#include <rtree_p.h>

#if W_DEBUG_LEVEL > 1
inline void         pin_i::_set_lsn_for_scan() {
    _hdr_lsn = _hdr_page().lsn();
}
#endif

/* NOTE (frj): this whole _error_occurred/PROLOGUE thing is kind of
   broken... I think the idea is that if one function call errors out
   the scan_i will remember and just return the error instead of doing
   more damage. However, with the new strict single owner semantics of
   w_rc_t, you only get to return the error once. Nobody should be
   retrying after an error anyway, but I found at least one spot where
   _error_occurred was not set and the next PROLOGUE blew up because
   of it.
   Update(neh): Yes, that's the idea. If it's not used properly,
   it's not the prologue that's broken; it's a sign of a bug
   elsewhere, so I'll keep this in. To get around the strict
   owner semantics issue, we'll generate a new error code from the
   error number.
   
 */
#define SCAN_METHOD_PROLOGUE1                           \
    do {                                                \
        if(_error_occurred.is_error())                  \
            return RC(_error_occurred.err_num());       \
    } while(0)


// Can no longer inline this in scan.h without requiring
// client (vas) to include def's for file_p and lgdata_p.
file_p&   append_file_i::_page() 
{ 
    return *aligned_cast<file_p>(_page_alias);
}


/*********************************************************************
 *
 *  scan_index_i::scan_index_i(stid, c1, bound1, c2, bound2, cc, prefetch)
 *
 *  Create a scan on index "stid" between "bound1" and "bound2".
 *  c1 could be >, >= or ==. c2 could be <, <= or ==.
 *  cc is the concurrency control method to use on the index.
 *
 *********************************************************************/
scan_index_i::scan_index_i(
    const stid_t&         stid_, 
    cmp_t                 c1, 
    const cvec_t&         bound1_, 
    cmp_t                 c2, 
    const cvec_t&         bound2_, 
    bool                  include_nulls,
    concurrency_t         cc,
    lock_mode_t           mode,
    const bool            bIgnoreLatches
    ) 
: xct_dependent_t(xct()),
  _stid(stid_),
  ntype(ss_m::t_bad_ndx_t),
  _eof(false),
  _error_occurred(),
  _btcursor(0),
  _skip_nulls( ! include_nulls ),
  _cc(cc),
  _bIgnoreLatches(bIgnoreLatches)
{
    INIT_SCAN_PROLOGUE_RC(scan_index_i::scan_index_i, prologue_rc_t::read_only, 1);

    _init(c1, bound1_, c2, bound2_, mode, bIgnoreLatches);
    register_me();
}


scan_index_i::~scan_index_i()
{
    finish();
}


/*********************************************************************
 *
 *  scan_index_i::_init(cond, b1, c2, b2)
 *
 *  Initialize a scan. Called by all constructors.
 *
 *  Of which there is only 1, and it uses mode=SH
 *
 *********************************************************************/
void 
scan_index_i::_init(
    cmp_t                 cond, 
    const cvec_t&         bound,
    cmp_t                 c2, 
    const cvec_t&         b2,
    lock_mode_t           mode,
    const bool            bIgnoreLatches)
{
    _finished = false;

    /*
     *  Determine index and kvl lock modes.
     */
    lock_mode_t index_lock_mode;
    concurrency_t key_lock_level;

    //  _cc was passed in on constructor
    //  Disallow certain kinds of scans on certain
    //  kinds of indexes:
    //

    switch(_cc) {
    case t_cc_none:
        index_lock_mode = lock_m::parent_mode[mode]; // IS if mode == SH
        key_lock_level = t_cc_none;
        break;

    case t_cc_im:
    case t_cc_kvl:
        index_lock_mode = lock_m::parent_mode[mode]; // IS if mode==SH
        key_lock_level = _cc;
        break;

    case t_cc_modkvl:
        index_lock_mode = lock_m::parent_mode[mode]; // IS if mode==SH
        // GROT: force the checks below to
        // check scan conditions
        key_lock_level = t_cc_none;
        break;

    case t_cc_file:
        index_lock_mode = mode;
        key_lock_level = t_cc_none;
        break;
    case t_cc_append:
    default:
        _error_occurred = RC(eBADLOCKMODE);
        return;
        break;
    }

    /*
     *  Save tid
     */
    tid = xct()->tid();

    /*
     *  Access directory entry
     */
    sdesc_t* sd = 0;
    _error_occurred = dir->access(_stid, sd, index_lock_mode);
    if (_error_occurred.is_error())  {
        return;
    }

    if (sd->sinfo().stype != t_index)  {
        _error_occurred = RC(eBADSTORETYPE);
        return;
    }

    if((concurrency_t)sd->sinfo().cc != key_lock_level) {
        switch((concurrency_t)sd->sinfo().cc) {
            case t_cc_none:
                //  allow anything
                break;

            case t_cc_modkvl:
                // certain checks are made in fetch_init
                if(_cc == t_cc_none || _cc == t_cc_file) {
                        key_lock_level = t_cc_none;
                } else {
                    key_lock_level = (concurrency_t)sd->sinfo().cc;
                }
                break;

            case t_cc_im:
            case t_cc_kvl:

                //  allow file 
                if(_cc == t_cc_file) {
                    key_lock_level = t_cc_file;
                    break;
                }
                key_lock_level = (concurrency_t)sd->sinfo().cc;
                break;

            default:
                _error_occurred = RC(eBADCCLEVEL);
                return;
        }
    }

    /*
     *  Initialize the fetch
     */
    switch (ntype = (ndx_t) sd->sinfo().ntype)  {
    case t_bad_ndx_t:  
        _error_occurred = RC(eBADNDXTYPE);
        return;

    case t_btree:
    case t_uni_btree:
        {
            _btcursor = new bt_cursor_t(!_skip_nulls);
            if (! _btcursor) {
                _error_occurred = RC(eOUTOFMEMORY);
                return;
            }
            bool inclusive = (cond == eq || cond == ge || cond == le);
	    
            cvec_t* elem = 0;

	    if(_btcursor->is_backward()) {
		elem = &(inclusive ? cvec_t::pos_inf : cvec_t::neg_inf);
	    } else {
		elem = &(inclusive ? cvec_t::neg_inf : cvec_t::pos_inf);
	    }

            _error_occurred = bt->fetch_init(*_btcursor, sd->root(), 
                                            sd->sinfo().nkc, sd->sinfo().kc,
                                            ntype == t_uni_btree,
                                            key_lock_level,
                                            bound, *elem, 
                                             cond, c2, b2, mode);
            if (_error_occurred.is_error())  {
                return;
            }
	    /*
            if(_btcursor->is_backward()) {
                // Not fully supported
                _error_occurred = RC(eNOTIMPLEMENTED);
                return;
            }
	    */
        }
        break;
    case t_mrbtree:
    case t_uni_mrbtree:
    case t_mrbtree_l:
    case t_uni_mrbtree_l:
    case t_mrbtree_p:
    case t_uni_mrbtree_p:
        {
            _btcursor = new bt_cursor_t(!_skip_nulls);
            if (! _btcursor) {
                _error_occurred = RC(eOUTOFMEMORY);
                return;
            }
            bool inclusive = (cond == eq || cond == ge || cond == le);

            cvec_t* elem = 0;

	    if(_btcursor->is_backward()) {
		elem = &(inclusive ? cvec_t::pos_inf : cvec_t::neg_inf);
	    } else {
		elem = &(inclusive ? cvec_t::neg_inf : cvec_t::pos_inf);
	    }

	    // traverse all the subtrees that covers the region [bound,b2]
	    vector<lpid_t> roots;
	    cvec_t* bound_key;
	    cvec_t* b2_key;
	    _error_occurred = bt->_scramble_key(bound_key, bound, sd->sinfo().nkc, sd->sinfo().kc);
	    char* bound_sc = (char*) malloc((*bound_key).size());
	    (*bound_key).copy_to(bound_sc, (*bound_key).size());
	    cvec_t b1(bound_sc, (*bound_key).size());
	    _error_occurred = bt->_scramble_key(b2_key, b2, sd->sinfo().nkc, sd->sinfo().kc);

	    if(&bound == &vec_t::neg_inf && &b2 == &vec_t::pos_inf) {
		_error_occurred = sd->partitions().getAllPartitions(roots);
	    } else {
		_error_occurred = sd->partitions().getPartitions(b1, inclusive, *b2_key,
								 c2 == eq || c2 == ge || c2 == le, roots);
	    }
	    _error_occurred = bt->mr_fetch_init(*_btcursor, roots, 
						sd->sinfo().nkc, sd->sinfo().kc,
						ntype == t_uni_btree,
						key_lock_level,
						b1, *elem, 
						cond, c2, *b2_key, mode,
                                                bIgnoreLatches);
	    free(bound_sc);
	    
	    if (_error_occurred.is_error())  {
                return;
            }
	    /*
            if(_btcursor->is_backward()) {
                // Not fully supported
                _error_occurred = RC(eNOTIMPLEMENTED);
                return;
            }
	    */
        }
        break;

    default:
        W_FATAL(eINTERNAL);
   }

}



/*********************************************************************
 * 
 *  scan_index_i::xct_state_changed(old_state, new_state)
 *
 *  Called by xct_t when transaction changes state. Terminate the
 *  the scan if transaction is aborting or committing.
 *  Note: this only makes sense in forward processing, since in
 *  recovery there is no such thing as an instantiated scan_index_i. 
 *
 *********************************************************************/
void 
scan_index_i::xct_state_changed(
    xct_state_t                /*old_state*/,
    xct_state_t                new_state)
{
    if (new_state == xct_aborting || new_state == xct_committing)  {
        finish();
    }
}


/*********************************************************************
 *
 *  scan_index_i::finish()
 *
 *  Terminate the scan.
 *
 *********************************************************************/
void 
scan_index_i::finish()
{
    _eof = true;
    switch (ntype)  {
    case t_btree:
    case t_uni_btree:
    case t_mrbtree:
    case t_uni_mrbtree:
    case t_mrbtree_l:
    case t_uni_mrbtree_l:
    case t_mrbtree_p:
    case t_uni_mrbtree_p:
        if (_btcursor)  {
            delete _btcursor;
            _btcursor = 0;
        }
        break;
    case t_bad_ndx_t:
        // error must have occured during init
        break;
    default:
        W_FATAL(eINTERNAL);
    }
}


/*********************************************************************
 *
 *  scan_index_i::_fetch(key, klen, el, elen, skip)
 *
 *  Fetch current entry into "key" and "el". If "skip" is true,
 *  advance the scan to the next qualifying entry.
 *
 *********************************************************************/
rc_t
scan_index_i::_fetch(
    vec_t*         key, 
    smsize_t*         klen, 
    vec_t*         el, 
    smsize_t*         elen,
    bool         skip)
{
    // Check if error condition occured.
    if (_error_occurred.is_error()) {
        if(_error_occurred.err_num() == eBADCMPOP) {
            _eof = true;
            return RCOK;
        }
        return w_rc_t(_error_occurred);
    }

    SM_PROLOGUE_RC(scan_index_i::_fetch, in_xct, read_only, 0);

    /*
     *  Check if scan is terminated.
     */
    if (_finished)  {
        return RC(eBADSCAN);
    }
    if (_eof)  {
        return RC(eEOF);
    }
    w_assert1(xct()->tid() == tid);
    switch (ntype)  {
    case t_btree:
    case t_uni_btree:
    case t_mrbtree:
    case t_uni_mrbtree:
    case t_mrbtree_l:
    case t_uni_mrbtree_l:
    case t_mrbtree_p:
    case t_uni_mrbtree_p:
        if (skip) {
            /*
             *  Advance cursor.
             */
            do {
                DBG(<<"");
                W_DO( bt->fetch(*_btcursor, _bIgnoreLatches) );
                if(_btcursor->eof()) break;
            } while (_skip_nulls && (_btcursor->klen() == 0));
        }
        break;

    case t_bad_ndx_t:
    default:
        W_FATAL(eINTERNAL);
    }

    /*
     *  Copy result to user buffer.
     */
    if (_btcursor->eof())  {
        DBG(<<"eof");
        _eof = true;
    } else {
        w_assert3(_btcursor->key());
        DBG(<<"not eof");
        if (klen)  *klen = _btcursor->klen();
        if (elen)  *elen = _btcursor->elen();

        bool k_ok = ((key == 0) || key->size() >= (size_t)_btcursor->klen());
        
        bool e_ok = ((el == 0) || (el->size() >= (size_t)_btcursor->elen()) );

        if (! (e_ok && k_ok))  {
            return RC(eRECWONTFIT);
        }

        if (key)  {
            key->copy_from(_btcursor->key(), _btcursor->klen());
        }
        if (el)  {
            el->copy_from(_btcursor->elem(), _btcursor->elen());
        }
    }

    return RCOK;
}
    

scan_rt_i::scan_rt_i(const stid_t& stid_, 
        nbox_t::sob_cmp_t c, 
        const nbox_t& qbox, 
        bool include_nulls,
        concurrency_t cc) 
: xct_dependent_t(xct()), stid(stid_), ntype(t_bad_ndx_t),
    _eof(false), _error_occurred(),
  _cursor(0), _skip_nulls( !include_nulls ), _cc(cc)
{
    INIT_SCAN_PROLOGUE_RC(scan_rt_i::scan_rt_i, 
            cc == t_cc_append? prologue_rc_t::read_write : prologue_rc_t::read_only, 
            1);
    _init(c, qbox);
    register_me();
}


scan_rt_i::~scan_rt_i()
{
        finish();
}


void scan_rt_i::finish()
{
    _eof = true;
    if (_cursor )  {
        if(_cursor->thread_can_delete()) {
            delete _cursor;
            _cursor = 0;
        }
    }
}

void scan_rt_i::_init(nbox_t::sob_cmp_t c, const nbox_t& qbox)
{
    _finished = false;

    tid = xct()->tid();

    // determine index lock mode
    lock_mode_t index_lock_mode;
    switch(_cc) {
    case t_cc_none:
        index_lock_mode = IS;
        break;
    case t_cc_page:
        index_lock_mode = IS;
        break;
    case t_cc_file:
        index_lock_mode = SH;
        break;
    case t_cc_append:
    default:
        _error_occurred = RC(eBADLOCKMODE);
        return;
        break;
    }

    sdesc_t* sd = 0;
    _error_occurred = dir->access(stid, sd, index_lock_mode);
    if (_error_occurred.is_error())  {
        return;
    }

    if (sd->sinfo().stype != t_index)  {
        _error_occurred = RC(eBADSTORETYPE);
        return;
    }

    _cursor = new rt_cursor_t(!_skip_nulls);
    w_assert1(_cursor);
    _cursor->qbox = qbox;
    _cursor->cond = c;

    switch (ntype = (ndx_t) sd->sinfo().ntype)  {
    case t_bad_ndx_t:
        _error_occurred = RC(eBADNDXTYPE);
        return;
    case t_rtree:
        _error_occurred = rt->fetch_init(sd->root(), *_cursor);
        if (_error_occurred.is_error())  {
            return;
        }
        break;
    default:
        W_FATAL(eINTERNAL);
    }
}

rc_t
scan_rt_i::_fetch(nbox_t& key, void* el, smsize_t& elen, bool& eof, bool skip)
{
    if (_error_occurred.is_error())   {
        return _error_occurred;
    }

    SM_PROLOGUE_RC(scan_rt_i::_fetch, in_xct, read_only, 0);

    if (_finished)  {
        return RC(eBADSCAN);
    }

    w_assert1(xct()->tid() == tid);

    switch (ntype)  {
    case t_rtree:
        _error_occurred = rt->fetch(*_cursor, key, el, elen, _eof, skip);
        if (_error_occurred.is_error())  {
            return w_rc_t(_error_occurred);
        }
        break;
    case t_bad_ndx_t:
    default:
        W_FATAL(eINTERNAL);
    }
    eof = _eof;

    return RCOK;
}

/*********************************************************************
 * 
 *  scan_rt_i::xct_state_changed(old_state, new_state)
 *
 *  Called by xct_t when transaction changes state. Terminate the
 *  the scan if transaction is aborting or committing.
 *  Note: this only makes sense in forward processing, since in
 *  recovery there is no such thing as an instantiated scan_rt_i. 
 *
 *********************************************************************/
void 
scan_rt_i::xct_state_changed(
    xct_state_t                /*old_state*/,
    xct_state_t                new_state)
{
    if (new_state == xct_aborting || new_state == xct_committing)  {
        finish();
    }
}


scan_file_i::scan_file_i(
        const stid_t& stid_, const rid_t& start,
        concurrency_t cc, bool pre, 
        lock_mode_t, /*mode TODO: remove.  is documented as ignored*/
        const bool bIgnoreLatches) 
: xct_dependent_t(xct()),
  stid(stid_),
  curr_rid(start),
  _eof(false),
  _cc(cc), 
  _bIgnoreLatches(bIgnoreLatches),
  _do_prefetch(pre),
  _prefetch(0)
{
    INIT_SCAN_PROLOGUE_RC(scan_file_i::scan_file_i,
            cc == t_cc_append ? prologue_rc_t::read_write : prologue_rc_t::read_only,
            0);

    /* _init sets error state */
    W_IGNORE(_init(cc == t_cc_append));
#if W_DEBUG_LEVEL > 1
    (void) _cursor.is_mine(); // Not an assert - just a 
    // consistency check
#endif
    register_me();
}

scan_file_i::scan_file_i(const stid_t& stid_, concurrency_t cc, 
                         bool pre, 
                         lock_mode_t, /*mode TODO: remove. this documented as ignored*/ 
                         const bool bIgnoreLatches)
: xct_dependent_t(xct()),
  stid(stid_),
  _eof(false),
  _cc(cc),
  _bIgnoreLatches(bIgnoreLatches),
  _do_prefetch(pre),
  _prefetch(0)
{
    INIT_SCAN_PROLOGUE_RC(scan_file_i::scan_file_i,
        cc == t_cc_append?prologue_rc_t::read_write:prologue_rc_t::read_only,  0);

    /* _init sets error state */
    W_IGNORE(_init(cc == t_cc_append));
#if W_DEBUG_LEVEL > 1
    (void) _cursor.is_mine(); // Not an assert - just a 
    // consistency check
#endif
    register_me();
}

scan_file_i::~scan_file_i()
{
#if W_DEBUG_LEVEL > 1
    (void) _cursor.is_mine(); // Not an assert - just a 
    // consistency check
#endif
    finish();
}




rc_t scan_file_i::_init(bool for_append) 
{
    // Can't nest these prologues
    // SCAN_METHOD_PROLOGUE(scan_file_i::_init, read_only, 1);
    this->_prefetch = 0;

    bool  eof = false;

    tid = xct()->tid();

    sdesc_t* sd = 0;

    // determine file and record lock modes
    // mode is the lock mode used on the entire store
    // in the call to dir->access.
    lock_mode_t mode = NL;

    switch(_cc) {
    case t_cc_none:
        mode = IS;
        _page_lock_mode = NL;
        _rec_lock_mode = NL;
        break;

    case t_cc_record:
        /* 
         * This might seem weird, but it's necessary
         * in order to prevent phantoms when another
         * tx does delete-in-order/abort or 
         * add-in-reverse-order/commit (using location hints)
         * while a scan is going on. 
         *
         * See the note about SERIALIZABILITY, below.
         * It turns out that this trick isn't enough.
         * Setting page lock mode to SH keeps that code
         * from being necessary, and it should be deleted.
         */
        mode = IS;
        _page_lock_mode = SH;  // record lock with IS the page
        _rec_lock_mode = NL;
        break;

    case t_cc_page:
        mode = IS;
        _page_lock_mode = SH;
        _rec_lock_mode = NL;
        break;

    case t_cc_append:
        mode = IX;
        _page_lock_mode = EX;
        _rec_lock_mode = EX;
        break;

    case t_cc_file:
        mode = SH;
        _page_lock_mode = NL;
        _rec_lock_mode = NL;
        break;

    default:
        _error_occurred = RC(eBADLOCKMODE);
        return w_rc_t(_error_occurred);
        break;
    }

    DBGTHRD(<<"scan_file_i calling access for stid " << stid);
    _error_occurred = dir->access(stid, sd, mode);
    if (_error_occurred.is_error())  {
        return w_rc_t(_error_occurred);
    }

    if (_error_occurred.is_error())  {
        return w_rc_t(_error_occurred);
    }

    // see if we need to figure out the first rid in the file
    // (ie. it was not specified in the constructor)
    if (curr_rid == rid_t::null) {
        if(for_append) {
            _error_occurred = fi->last_page(stid, 
                    curr_rid.pid, NULL/*alloc only*/); 
        } else {
            _error_occurred = fi->first_page(stid, 
                    curr_rid.pid, NULL/*alloc only*/);
        }

        if (_error_occurred.is_error())  {
            return w_rc_t(_error_occurred);
        }
        curr_rid.slot = 0;  // get the header object

    } else {
        // subtract 1 slot from curr_rid so that next will advance
        // properly.  Also pin the previous slot it.
        if (curr_rid.slot > 0 && !for_append) curr_rid.slot--;
    }

    if (_page_lock_mode != NL) {
        w_assert3(curr_rid.pid.page != 0);
        _error_occurred = lm->lock(curr_rid.pid, _page_lock_mode, 
                                  t_long, WAIT_SPECIFIED_BY_XCT);
        if (_error_occurred.is_error())  {
            return w_rc_t(_error_occurred);
        }
    }

    // remember the next pid
    _next_pid = curr_rid.pid;
    _error_occurred = fi->next_page(_next_pid, eof, NULL/*alloc only*/);
    if(for_append) {
        w_assert3(eof);
    } else if (_error_occurred.is_error())  {
        return w_rc_t(_error_occurred);
    }
    if (eof) {
        _next_pid = lpid_t::null;
    } 

    if(smlevel_0::do_prefetch && this->_do_prefetch && !for_append) {
        // prefetch first page
        this->_prefetch = new bf_prefetch_thread_t;
        if (this->_prefetch) {
            W_COERCE( this->_prefetch->fork());

            DBGTHRD(<<" requesting first page: " << curr_rid.pid);

            W_COERCE(this->_prefetch->request(curr_rid.pid, 
                        pin_i::lock_to_latch(_page_lock_mode)));
        }
    }
#if W_DEBUG_LEVEL > 1
    (void) _cursor.is_mine(); // Not an assert - just a 
    // consistency check
#endif
    return RCOK;
}

rc_t
scan_file_i::next(pin_i*& pin_ptr, smsize_t start, bool& eof)
{
     
#if W_DEBUG_LEVEL > 1
    (void) _cursor.is_mine(); // Not an assert - just a 
    // consistency check
#endif
    return _next(pin_ptr, start, eof);
}

rc_t
scan_file_i::_next(pin_i*& pin_ptr, smsize_t start, bool& eof)
{
    SCAN_METHOD_PROLOGUE1;
    file_p*        curr;

    w_assert1(xct()->tid() == tid); // (ip) ???

    // scan through pages and slots looking for the next valid slot
    while (!_eof) {
#if W_DEBUG_LEVEL > 1
        (void) _cursor.is_mine(); // Not an assert - just a 
        // consistency check
#endif
        if (!_cursor.pinned()) {
            // We're getting a new page
            rid_t temp_rid = curr_rid;
            temp_rid.slot = 0; 
            if(this->_prefetch) {
                // It should have been prefetched
                DBGTHRD(<<" fetching page: " << temp_rid.pid);
                _error_occurred = this->_prefetch->fetch(temp_rid.pid, 
                                _cursor._hdr_page());
                if(!_error_occurred.is_error()) {
                    if(_next_pid != lpid_t::null) {
                        // Must lock before latch...
                        if (_page_lock_mode != NL) {
                            DBGTHRD(<<" locking " << _next_pid);
                            w_assert3(_next_pid.page != 0);
                            _error_occurred = lm->lock(_next_pid,
                                              _page_lock_mode,
                                              t_long,
                                              WAIT_SPECIFIED_BY_XCT);
                            if (_error_occurred.is_error())  {
                                return w_rc_t(_error_occurred);
                            }
                        }
                        DBGTHRD(<<" requesting next page: " << _next_pid);
                        W_COERCE(this->_prefetch->request(_next_pid, 
                            pin_i::lock_to_latch(_page_lock_mode)));
                    }
                }
            } 
            _error_occurred = _cursor._pin(temp_rid, start,
                              _rec_lock_mode);
            if (_error_occurred.is_error())  {
                return w_rc_t(_error_occurred);
            }
            _cursor._set_lsn_for_scan();
        }
#if W_DEBUG_LEVEL > 1
        (void) _cursor.is_mine(); // Not an assert - just a 
        // consistency check
#endif
        curr = _cursor._get_hdr_page_no_lsn_check();
#if W_DEBUG_LEVEL > 1
        (void) _cursor.is_mine(); // Not an assert - just a 
        // consistency check
#endif
        {
            slotid_t        slot;
            // next_slot returns the slot we are wanting to lock,
            // but that's not what we're locking here:
            slot = curr->next_slot(curr_rid.slot);
            curr_rid.slot = slot;
            if(_rec_lock_mode != NL) {
                w_assert3(curr_rid.pid.page != 0);
                _error_occurred = lm->lock(curr_rid, 
                        _rec_lock_mode, t_long, WAIT_IMMEDIATE);
                if (_error_occurred.is_error())  
                {
                    if (_error_occurred.err_num() != eLOCKTIMEOUT) {
                        return w_rc_t(_error_occurred);
                    }
                    //
                    // re-fetch the slot if we had to wait for a lock.
                    // needed for SERIALIZABILITY, since the next_slot
                    // information is not protected by locks
                    //
                    _cursor.unpin();
                    _error_occurred = RCOK;
                    _error_occurred.reset();
                    continue; // while loop
                }
            }
            curr_rid.slot = slot;
        }
#if W_DEBUG_LEVEL > 1
        (void) _cursor.is_mine(); // Not an assert - just a 
        // consistency check
#endif
        if (curr_rid.slot == 0) 
        {
            // last slot, so go to next page
#if W_DEBUG_LEVEL > 1
            (void) _cursor.is_mine(); // Not an assert - just a 
            // consistency check
#endif
            _cursor.unpin();
            curr_rid.pid = _next_pid;
            if (_next_pid == lpid_t::null) {
                _eof = true;
            } else {
                if(this->_prefetch == 0) {
                    if (_page_lock_mode != NL) {
                        DBGTHRD(<<" locking " << curr_rid.pid);
                        _error_occurred = lm->lock(curr_rid.pid, 
                                                  _page_lock_mode,
                                                  t_long,
                                                  WAIT_SPECIFIED_BY_XCT);
                        if (_error_occurred.is_error())  {
                            return w_rc_t(_error_occurred);
                        }
                    }
                } else {
                    // prefetch case: we already locked & requested the next pid
                    // we'll fetch it at the top of this loop
                    // 
                    // All we have to do in this case is locate the
                    // page after that.
                }

                DBGTHRD(<<" locating page after " << _next_pid);
                bool tmp_eof;
                _error_occurred = fi->next_page(_next_pid, tmp_eof, NULL/*alloc only*/);
                if (_error_occurred.is_error())  {
                    return w_rc_t(_error_occurred);
                }
                if (tmp_eof) {
                    _next_pid = lpid_t::null;
                } 
                DBGTHRD(<<" next page is " << _next_pid);
            }
#if W_DEBUG_LEVEL > 1
        (void) _cursor.is_mine(); // Not an assert - just a 
        // consistency check
#endif
        } else 
        {
            _error_occurred = _cursor._pin(curr_rid, start, _rec_lock_mode);
            if(_error_occurred.is_error()) {
                return w_rc_t(_error_occurred);
            }
            _cursor._set_lsn_for_scan();
            break;
        }
    }
#if W_DEBUG_LEVEL > 1
        (void) _cursor.is_mine(); // Not an assert - just a 
        // consistency check
#endif

    eof = _eof;

#if W_DEBUG_LEVEL > 1
        (void) _cursor.is_mine(); // Not an assert - just a 
        // consistency check
#endif

    pin_ptr = &_cursor;
    return RCOK;
}

rc_t
scan_file_i::next_page(pin_i*& pin_ptr, smsize_t start, bool& eof)
{
    SCAN_METHOD_PROLOGUE1;
    SCAN_METHOD_PROLOGUE(scan_file_i::next_page, read_only, 1);

#if W_DEBUG_LEVEL > 1
        (void) _cursor.is_mine(); // Not an assert - just a 
        // consistency check
#endif

    /*
     * The trick here is to make the scan think we are on the
     * last slot on the page and then just call _next()
     * If the _cursor is not pinned, then next will start at the
     * first slot.  This is sufficient for our needs.
     */
    if (_cursor.pinned()) {
        curr_rid.slot = _cursor._hdr_page().num_slots()-1;
    }
    return _next(pin_ptr, start, eof);
}

void scan_file_i::finish()
{
    // must finish regardless of error
    // SCAN_METHOD_PROLOGUE(scan_file_i::finish);

#if W_DEBUG_LEVEL > 1
        (void) _cursor.is_mine(); // Not an assert - just a 
        // consistency check
#endif
    _eof = true;

    if(_cursor.is_mine()) {
        // Don't unpin unless I'm the owner. 
        // (This function can get called from xct_state_changed(),
        // running in a non-owner thread.)
        _cursor.unpin();
    }

    if (this->_prefetch) {
        this->_prefetch->retire();
        delete this->_prefetch;
        this->_prefetch = 0;
    }
}

/*********************************************************************
 * 
 *  scan_file_i::xct_state_changed(old_state, new_state)
 *
 *  Called by xct_t when transaction changes state. Terminate the
 *  the scan if transaction is aborting or committing.
 *  Note: this only makes sense in forward processing, since in
 *  recovery there is no such thing as an instantiated scan_file_i. 
 *
 *********************************************************************/
void 
scan_file_i::xct_state_changed(
    xct_state_t                /*old_state*/,
    xct_state_t                new_state)
{
    if (new_state == xct_aborting || new_state == xct_committing)  {
        finish();
    }
}

append_file_i::append_file_i(const stid_t& stid_) 
 : scan_file_i(stid_, t_cc_append)
{
    INIT_SCAN_PROLOGUE_RC(append_file_i::append_file_i, prologue_rc_t::read_write, 0);
    _init_constructor();
    W_IGNORE(_init(true));
    if(_error_occurred.is_error()) return;
        _error_occurred = lm->lock(stid, EX, t_long, WAIT_SPECIFIED_BY_XCT);
    if(_error_occurred.is_error()) return;
    sdesc_t *sd;
    _error_occurred = dir->access(stid, sd, IX); 
    // makes a copy - only because the create_rec
    // functions that we want to call require that we
    // have one to reference.
    _cached_sdesc = *sd;
    w_assert3( !_page().is_fixed() );
}

void
append_file_i::_init_constructor()
{
    FUNC(append_file_i::_init_constructor);
    //  just make sure _page_alias is big enough
    w_assert3(sizeof(_page_alias) >= sizeof(file_p) + __alignof__(file_p));

    new (&_page()) file_p();
    w_assert3( !_page().is_fixed() );

}

append_file_i::~append_file_i() 
{ 
    FUNC(append_file_i::~append_file_i);
    if(_page().is_fixed()) {
        _page().unfix();
    }
    DBG( <<  (_page().is_fixed()? "IS FIXED-- ERROR" : "OK") );
    _page().destructor();

    // Must invalidate so that ref counts are adjusted
    DBGTHRD(<<"invalidating cached descr");
    _cached_sdesc.invalidate_sdesc();
    finish(); 
}

rc_t
append_file_i::next(pin_i*&, smsize_t, bool& )
{
    return RC(eAPPENDFILEINOSCAN);
}

rc_t                        
append_file_i::create_rec(
        const vec_t&                     hdr,
        smsize_t                     len_hint, 
        const vec_t&                      data,
        rid_t&                             rid
        )
{
    SCAN_METHOD_PROLOGUE(append_file_i::create_rec, read_write, 1);

#if W_DEBUG_LEVEL > 2
    if(_page().is_fixed()) {
        DBG(<<"IS FIXED! ");
    }
#endif 

    W_DO( fi->create_rec_at_end(_page(), 
        len_hint, hdr, data, _cached_sdesc,  curr_rid) );

    rid = curr_rid;
    return RCOK;
}

