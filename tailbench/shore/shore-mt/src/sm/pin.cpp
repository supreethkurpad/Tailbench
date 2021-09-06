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

 $Id: pin.cpp,v 1.142 2010/06/08 22:28:55 nhall Exp $

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
#define PIN_C

#ifdef __GNUG__
#pragma implementation "pin.h"
#endif

#include <sm_int_4.h>
#include <pin.h>
#include <lgrec.h>
#include <sm.h>

#include <new>

/* delegate is used in debug case */
#define RETURN_RC( _err) \
        do { w_rc_t rc = rc_t(_err); return  rc; } while(0)

#if W_DEBUG_LEVEL > 1
inline void         pin_i::_set_lsn() {
        // NB: in the case of multi-threaded xcts, this
        // lsn might not be *the* lsn that changed the page -- it
        // could be later
        _hdr_lsn = _hdr_page().lsn();
}
#endif

// Can no longer inline this in pin.h without requiring
// client (vas) to include def's for file_p and lgdata_p.
file_p&        pin_i::_hdr_page() const 
{
  return *aligned_cast<file_p>(_hdr_page_alias);
}

lgdata_p& pin_i::_data_page() const 
{
  return *aligned_cast<lgdata_p>(_data_page_alias);
}

pin_i::~pin_i()
{ 
    unpin();

    // unpin() actuall calls unfix on these pages and unfix does
    // everything the destructor does, but to be safe we still call
    // the destructor
    _hdr_page().destructor();
    _data_page().destructor();
}

rc_t pin_i::pin(const rid_t& rid, smsize_t start, lock_mode_t lmode,
                const bool bIgnoreLatches)
{
    latch_mode_t latch_mode = ( bIgnoreLatches ? LATCH_NL : lock_to_latch(lmode) );
    return pin(rid, start, lmode, latch_mode);
}

rc_t pin_i::pin(const rid_t& rid, smsize_t start, 
                lock_mode_t lmode, latch_mode_t latch_mode)
{
    SM_PROLOGUE_RC(pin_i::pin, in_xct, read_only,  2);
    if (lmode != SH && lmode != UD && lmode != EX && lmode != NL)
        return RC(eBADLOCKMODE);
    W_DO(_pin(rid, start, lmode, latch_mode));
    _set_lsn();
    return RCOK;
}


void pin_i::unpin()
{
    if (pinned()) {
        if (_flags & pin_lg_data_pinned) {
            _data_page().unfix();  
        }
        _hdr_page().unfix();  
        _flags = pin_empty;
        _rec = NULL;

        INC_TSTAT(rec_unpin_cnt);
    }
    w_assert2(!pinned()); // force execution of asserts in pinned()
}

// The pinning thread might not be the thread checking is_mine();
bool 
pin_i::is_mine() const
{
    bool        mine = false;
    bool        d_others = false;
    bool        h_others = false;

    if(_data_page().is_fixed()) {
        if(_data_page().pinned_by_me()) mine = true; 
        else                            d_others = true;
    } 

    if( _hdr_page().is_fixed())  {
        if( _hdr_page().pinned_by_me()) {
            if(d_others) {
                // Had better not have data page owned by other thread
                // while header page owned by me
                W_FATAL(eINTERNAL);
            }
            mine = true;
        } else {
            h_others = true;
        }
    }
    if(h_others && mine) {
        // Had better not have data page owned by me
        // and header page owned by others
        W_FATAL(eINTERNAL);
    }

    // returns false if not fixed at all
    return mine;
}

void pin_i::set_ref_bit(int value)
{
    if (pinned()) {
        if (is_large()) {
            if (_flags & pin_lg_data_pinned) {
                _data_page().set_ref_bit(value);  
            }
        } else {
            _hdr_page().set_ref_bit(value);  
        }
    }
}

rc_t pin_i::repin(lock_mode_t lmode)
{
    SM_PROLOGUE_RC(pin_i::repin, in_xct, read_only,  2);
    if (lmode != SH && lmode != UD && lmode != EX) return RC(eBADLOCKMODE);
    DBG(<<" repin " << this->_rid);
    W_DO(_repin(lmode));
    return RCOK;
}

// returns eEOF if no more bytes available
rc_t pin_i::next_bytes(bool& eof)
{
    SM_PROLOGUE_RC(pin_i::next_bytes, in_xct, read_only,  0);
    smsize_t        newstart;
    _check_lsn();

    eof = false;
    if (_rec->is_large()) {
        w_assert3(_start % lgdata_p::data_sz == 0);
        newstart = _start + lgdata_p::data_sz;
        if (newstart < _rec->body_size()) {
            _len = MIN(((smsize_t)lgdata_p::data_sz),
                       _rec->body_size()-newstart);
            _flags &= ~pin_lg_data_pinned;  // data page is not pinned
            _start = newstart;
            return RCOK;
        }
    }
    eof = true;        // reached end of object        
                // leaves previous chunk still pinned
    return RCOK;
}

rc_t pin_i::update_rec(smsize_t start, const vec_t& data,
                       int* old_value /* temporary: for degugging only */
#ifdef SM_DORA
                       , const bool bIgnoreLocks
#endif
                       )
{
    bool        was_pinned = pinned(); // must be first due to hp CC bug
    w_rc_t      rc;

    SM_PROLOGUE_RC(pin_i::update_rec, in_xct, read_write,  0);
    DBG(<<"update_rec " << this->_rid << " #bytes=" << data.size());

    if (was_pinned && _rec->is_small()) {

        if (was_pinned) {
            DBG(<<"pinned");
            _check_lsn();
        }
        W_DO_GOTO(rc, _repin(EX, old_value
#ifdef SM_DORA
                             , bIgnoreLocks
#endif
                             ));
        w_assert3(_hdr_page().latch_mode() == LATCH_EX);
        w_assert3((_lmode == EX)
#ifdef SM_DORA
                  || bIgnoreLocks // IP: In DORA we disable the second assertion
#endif
                  );

        //
        // Avoid calling ss_m::_update_rec by just
        // splicing in the new data, but first make sure
        // the starting location and size to no go off
        // the "end" of the record.
        //
        if (start >= rec()->body_size()) {
            return RC(eBADSTART);
        }
        if (data.size() > (rec()->body_size()-start)) {
            return RC(eRECUPDATESIZE);
        }
        W_DO_GOTO(rc, _hdr_page().splice_data(_rid.slot, u4i(start), data.size(), data));

    } else {

#ifdef SM_DORA
        if (bIgnoreLocks) {
            W_DO_GOTO(rc, SSM->_update_rec(_rid, start, data, bIgnoreLocks));      
        } else {
#endif  
  
        // if !locked, then unpin in case lock req (in update) blocks
        if (was_pinned && _lmode != EX) unpin();

        W_DO_GOTO(rc, SSM->_update_rec(_rid, start, data));
        _lmode = EX;  // record is now EX locked
        if (was_pinned) W_DO_GOTO(rc, _repin(EX));

#ifdef SM_DORA
        }
#endif  

    }

// success
    if (was_pinned) {
        w_assert2(pinned());
        _set_lsn();
    } else {
        unpin();
    }
    w_assert2(was_pinned == pinned());
    return RCOK;

failure:
    if (was_pinned && !pinned()) {
        // this should not fail.
        // Unfortunately, it can deadlock here.
        // We (or another cooperating thread) might
        // have converted a latch-lock deadlock to a
        // lock-lock deadlock, and we might be the victim.
        // In that case, the other (which probably holds the latch)
        // cannot continue until we free our locks, and
        // if we try to repin here we are right back into the
        // latch-lock deadlock scenario.
        // So we check for eDEADLOCK here, and if that wasn't
        // the cause, we'll try to repin, but if it was,
        // we'll not try to repin.
        // I'm not sure this won't result in all sorts of
        // other problems because our pin state changed.
        if(rc.err_num() == eDEADLOCK) return rc; // gnats 90
        
        W_COERCE(_repin(SH)); 
    }
    w_assert2(was_pinned == pinned());

    return rc;
}

rc_t pin_i::update_mrbt_rec(smsize_t start, const vec_t& data,
			    int* old_value, /* temporary: for degugging only */
			    const bool bIgnoreLocks,
			    const bool bIgnoreLatches)
{
    bool        was_pinned = pinned(); // must be first due to hp CC bug
    w_rc_t      rc;

    SM_PROLOGUE_RC(pin_i::update_mrbt_rec, in_xct, read_write,  0);
    DBG(<<"update_rec " << this->_rid << " #bytes=" << data.size());

    if (was_pinned && _rec->is_small()) {

        if (was_pinned) {
            DBG(<<"pinned");
            _check_lsn();
        }
        W_DO_GOTO(rc, _repin(EX, old_value
#ifdef SM_DORA
                             , bIgnoreLocks
#endif
                             ));
        w_assert3(bIgnoreLatches || _hdr_page().latch_mode() == LATCH_EX);
        w_assert3((_lmode == EX) || bIgnoreLocks); // IP: In DORA we disable the second assertion

        //
        // Avoid calling ss_m::_update_rec by just
        // splicing in the new data, but first make sure
        // the starting location and size to no go off
        // the "end" of the record.
        //
        if (start >= rec()->body_size()) {
            return RC(eBADSTART);
        }
        if (data.size() > (rec()->body_size()-start)) {
            return RC(eRECUPDATESIZE);
        }
        W_DO_GOTO(rc, _hdr_page().splice_data(_rid.slot, u4i(start), data.size(), data));

    } else {

        if (bIgnoreLocks) {
            W_DO_GOTO(rc, SSM->_update_mrbt_rec(_rid, start, data, bIgnoreLocks, bIgnoreLatches));      
        } else {
  
	    // if !locked, then unpin in case lock req (in update) blocks
	    if (was_pinned && _lmode != EX) unpin();
	    
	    W_DO_GOTO(rc, SSM->_update_mrbt_rec(_rid, start, data, bIgnoreLocks, bIgnoreLatches));
	    _lmode = EX;  // record is now EX locked
	    if (was_pinned) W_DO_GOTO(rc, _repin(EX));
	    
        }
	
    }

// success
    if (was_pinned) {
        w_assert2(pinned());
        _set_lsn();
    } else {
        unpin();
    }
    w_assert2(was_pinned == pinned());
    return RCOK;

failure:
    if (was_pinned && !pinned()) {
        // this should not fail.
        // Unfortunately, it can deadlock here.
        // We (or another cooperating thread) might
        // have converted a latch-lock deadlock to a
        // lock-lock deadlock, and we might be the victim.
        // In that case, the other (which probably holds the latch)
        // cannot continue until we free our locks, and
        // if we try to repin here we are right back into the
        // latch-lock deadlock scenario.
        // So we check for eDEADLOCK here, and if that wasn't
        // the cause, we'll try to repin, but if it was,
        // we'll not try to repin.
        // I'm not sure this won't result in all sorts of
        // other problems because our pin state changed.
        if(rc.err_num() == eDEADLOCK) return rc; // gnats 90
        
        W_COERCE(_repin(SH)); 
    }
    w_assert2(was_pinned == pinned());

    return rc;
}

rc_t pin_i::update_rec_hdr(smsize_t start, const vec_t& hdr
#ifdef SM_DORA
                           , const bool bIgnoreLocks
#endif
                           )
{
    bool was_pinned = pinned(); // must be first due to hp CC bug
    rc_t rc;
    if (was_pinned) {
        DBG(<<"pinned");
        _check_lsn();
    }

    SM_PROLOGUE_RC(pin_i::update_rec_hdr, in_xct, read_write, 0);

    lock_mode_t repin_lock_mode = EX;
#ifdef SM_DORA
    if (bIgnoreLocks) repin_lock_mode = SH;
#endif
    W_DO_GOTO(rc, _repin(repin_lock_mode));

    W_DO_GOTO(rc, _hdr_page().splice_hdr(_rid.slot, u4i(start), hdr.size(), hdr));

// success
    if (was_pinned) {
        w_assert3(pinned());
        _set_lsn();
    } else {
        unpin();
    }
    w_assert3(was_pinned == pinned());
    w_assert1(rc.is_error()==false);
    return rc;

failure:
    if (was_pinned && !pinned()) {
        // this should not fail
        if(rc.err_num() == eDEADLOCK) return rc; // gnats 90
        W_COERCE(_repin(SH)); 
    }
    w_assert3(was_pinned == pinned());
    return rc;
}

rc_t pin_i::append_rec(const vec_t& data)
{
    bool was_pinned = pinned(); // must be first due to hp CC bug

    SM_PROLOGUE_RC(pin_i::append_rec, in_xct,  read_write,0);
    DBG(<< this->_rid << " #bytes=" << data.size());
    rid_t  rid;  // local variable for phys rec id

    // must unpin for 2 reasons:
    // 1. in case lock request (in append) blocks
    // 2. since record may move on page
    if (was_pinned) unpin();

    rc_t rc = SSM->_append_rec(_rid, data); 
    DBG(<<"rc=" << rc);
    if (rc.is_error()) {
        goto failure;
    }

    // record is now EX locked
    _lmode = EX;

    if (was_pinned) W_DO_GOTO(rc, _repin(EX));

// success
    if (was_pinned) {
        w_assert3(pinned());
        _set_lsn();
    } else {
        unpin();
    }
    w_assert3(was_pinned == pinned());
    w_assert2(rc.is_error() == false);
    return rc;

failure:
    if (was_pinned && !pinned()) {
        // this should not fail
        if(rc.err_num() == eDEADLOCK) return rc; // gnats 90
        W_COERCE(_repin(SH)); 
    }
    w_assert3(was_pinned == pinned());
    return rc;
}

rc_t pin_i::append_mrbt_rec(const vec_t& data,
			    const bool bIgnoreLocks,
			    const bool bIgnoreLatches)
{
    bool was_pinned = pinned(); // must be first due to hp CC bug

    SM_PROLOGUE_RC(pin_i::append_mrbt_rec, in_xct,  read_write,0);
    DBG(<< this->_rid << " #bytes=" << data.size());
    rid_t  rid;  // local variable for phys rec id

    // must unpin for 2 reasons:
    // 1. in case lock request (in append) blocks
    // 2. since record may move on page
    if (was_pinned) unpin();

    rc_t rc = SSM->_append_mrbt_rec(_rid, data, bIgnoreLocks, bIgnoreLatches); 
    DBG(<<"rc=" << rc);
    if (rc.is_error()) {
        goto failure;
    }

    // record is now EX locked
    _lmode = EX;

    if (was_pinned) W_DO_GOTO(rc, _repin(EX));

// success
    if (was_pinned) {
        w_assert3(pinned());
        _set_lsn();
    } else {
        unpin();
    }
    w_assert3(was_pinned == pinned());
    w_assert2(rc.is_error() == false);
    return rc;

failure:
    if (was_pinned && !pinned()) {
        // this should not fail
        if(rc.err_num() == eDEADLOCK) return rc; // gnats 90
        W_COERCE(_repin(SH)); 
    }
    w_assert3(was_pinned == pinned());
    return rc;
}

rc_t pin_i::truncate_rec(smsize_t amount)
{
    bool was_pinned = pinned(); // must be first due to hp CC bug
    rc_t rc;
    SM_PROLOGUE_RC(pin_i::truncate_rec, in_xct, read_write, 0);
    DBG(<< this->_rid << " #bytes= " << amount);

    rid_t  rid;  // remember phys rec id in here
    bool should_forward;    // set by _truncate_rec

    // must unpin for 2 reasons:
    // 1. in case lock request (in append) blocks
    // 2. since record may move on page
    if (was_pinned) unpin();

    W_DO_GOTO(rc, SSM->_truncate_rec(_rid, amount, should_forward));

    // record is now EX locked
    _lmode = EX;

    if (was_pinned) W_DO_GOTO(rc, _repin(EX));

// success
    if (was_pinned) {
        w_assert3(pinned());
        _set_lsn();
    } else {
        unpin();
    }
    w_assert3(was_pinned == pinned());
    w_assert2(rc.is_error()==false);
    return rc;

failure:
    if (was_pinned && !pinned()) {
        // this should not fail
        if(rc.err_num() == eDEADLOCK) return rc; // gnats 90
        W_COERCE(_repin(SH)); 
    }
    w_assert3(was_pinned == pinned());
    return rc;
}

const char* pin_i::hdr_page_data()
{
    _check_lsn();
    if (!pinned()) return NULL;
    return (const char*) &(_hdr_page().persistent_part());
}

lpid_t 
pin_i::page_containing(smsize_t offset, smsize_t& start_byte) const 
{
    _check_lsn();
    if(is_small()) {
        w_assert3(!is_large());
        start_byte = 0;
        return _get_hdr_page()->pid();
    } else {
        w_assert3(!is_small());
        return rec()->pid_containing(offset, start_byte, _hdr_page());
    }
}

const lsn_t& pin_i::_get_hdr_lsn() const { return _hdr_page().lsn(); }

/*
 * This function is called to pin the current large record data page
 */
rc_t pin_i::_pin_data()
{
    smsize_t start_verify = 0;
    w_assert3(!(_flags & pin_lg_data_pinned));
    lpid_t data_pid = _rec->pid_containing(_start, start_verify, _hdr_page());
    if(data_pid == lpid_t::null) {
        w_assert3(_rec->body_size() == 0);
        return RC(eEOF);
    }
    w_assert3(start_verify == _start);
    W_DO( _data_page().fix(data_pid, LATCH_SH) );
    w_assert1(_data_page().is_fixed());

    _flags |= pin_lg_data_pinned;
    return RCOK;
}

void pin_i::_init_constructor()
{
    //  just make sure _page_alias is big enough
    w_assert2(sizeof(_hdr_page_alias) >= sizeof(file_p) + __alignof__(file_p));
    w_assert2(sizeof(_data_page_alias) >= 
            sizeof(lgdata_p) + __alignof(lgdata_p));

    _flags = pin_empty;
    _rec = NULL;
    _lmode = NL; 
    new (&_hdr_page()) file_p();
    new (&_data_page()) lgdata_p();
}

const char* pin_i::_body_large()
{
    _check_lsn();
    if (!(_flags & pin_lg_data_pinned)) {
                w_rc_t rc = _pin_data();
        if (rc.is_error()) return NULL;
    }
    return (char*) _data_page().tuple_addr(0);
}

rc_t pin_i::_pin(const rid_t& rid, 
                 smsize_t start, 
                 lock_mode_t lock_mode,
                 const bool bIgnoreLatches)
{
    latch_mode_t latch_mode = ( bIgnoreLatches ? LATCH_NL : lock_to_latch(lock_mode) );
    return _pin(rid, start, lock_mode, latch_mode);
}

rc_t pin_i::_pin(const rid_t& rid, smsize_t start, 
                 lock_mode_t lmode, 
                 latch_mode_t latch_mode)
{
    rc_t rc;
    bool pin_page = false;        // true indicates page needs pinning

    w_assert3(lmode == SH || lmode == EX || lmode == UD ||
            lmode == NL /*for scan.cpp*/ );

    DBGTHRD(<<"enter _pin");
    if (pinned()) {
        DBG(<<"pinned");
        if (_flags & pin_lg_data_pinned) {
            w_assert3(_flags & pin_separate_data && _data_page().is_fixed());  
            _data_page().unfix();  
        }
   
        /*
         * If the page for the new record is not the same as the
         * old (or if we need to get a lock),
         * then unpin the old and get the new page.
         * If we need to get the lock, then we must unfix since
         * we may block on the lock.  We may want to do something
         * like repin where we try to lock with 0-timeout and
         * only if we fail do we unlatch.
         */
        if (rid.pid != _rid.pid || lmode != NL) {
            _hdr_page().unfix();  
            pin_page = true;
            INC_TSTAT(rec_unpin_cnt);

        }
    } else {
        DBG(<<"not pinned");
        pin_page = true;
    }

    // aquire lock only if lock is requested
    if (lmode != NL) {
        DBG(<<"acquiring lock");
        W_DO_GOTO(rc, lm->lock(rid, lmode, t_long, WAIT_SPECIFIED_BY_XCT));
        DBG(<<"lock is acquired");
        _lmode = lmode;
    } else {
        // we trust the caller and therefore can do this
        if (_lmode == NL) _lmode = SH;
    }
    w_assert3(_lmode > NL); 

    if (pin_page) {
        DBGTHRD(<<"pin");
        W_DO_GOTO(rc, fi->locate_page(rid, _hdr_page(), latch_mode));
        INC_TSTAT(rec_pin_cnt);

    }

    W_DO_GOTO(rc, _hdr_page().get_rec(rid.slot, _rec));
    if (_rec == NULL) goto failure;
    if (start > 0 && start >= _rec->body_size()) {
        rc = RC(eBADSTART);
        goto failure;
    }

    _flags = pin_rec_pinned;
    _rid = rid;

    /*
     * See if the record is small or large.  Record number zero
     * is a special header record and is considered small
     */
    if (rid.slot == 0 || _rec->is_small()) {  
        _start = 0;
        _len = _rec->body_size()-_start;
    } else {
        _start = (start/lgdata_p::data_sz)*lgdata_p::data_sz;
        _len = MIN(((smsize_t)lgdata_p::data_sz),
                   _rec->body_size()-_start);
        _flags |= pin_separate_data;
    }

/* success: */
    _set_lsn();
    w_assert2(rc.is_error()==false);
    return rc;
  
failure:
    if (pin_page) {
        _hdr_page().unfix();  
        INC_TSTAT(rec_unpin_cnt);

    }
    _flags = pin_empty;
    return rc;
}

rc_t pin_i::_repin(lock_mode_t lmode, int* /*old_value*/
#ifdef SM_DORA
                   , const bool bIgnoreLocks
#endif
                   )
{
    rc_t         rc;

    w_assert3(lmode == SH || lmode == UD || lmode == EX);
    // acquire lock if current one is not strong enough
    // TODO: this should probably use the lock supremum table

    if ((_lmode < lmode) 
#ifdef SM_DORA
        && !bIgnoreLocks
#endif  
        )
    {
        DBG(<<"acquiring lock");
        // see if we can get the lock without blocking
        rc = lm->lock(_rid, lmode, t_long, WAIT_IMMEDIATE);
        if (rc.is_error()) {
            if (rc.err_num() == eLOCKTIMEOUT) {
                // we would block, so unpin is necessary, then get lock
                if (pinned()) unpin();
                W_DO_GOTO(rc, lm->lock(_rid, lmode, t_long, WAIT_SPECIFIED_BY_XCT));
            } else {
                W_DO_GOTO(rc, rc.reset());
            }
        }
        DBG(<<"lock is acquired");
        _lmode = lmode;
    }        

    if (pinned()) {
        w_assert3(_hdr_page().is_fixed());  
        if (_flags & pin_lg_data_pinned) {
            w_assert3(_flags & pin_separate_data && _data_page().is_fixed());  
        }

        // upgrade to an EX latch if all we had before was an SH latch

        if (_hdr_page().latch_mode() != lock_to_latch(_lmode)) {
            w_assert3(_hdr_page().latch_mode() == LATCH_SH);
            w_assert3(_lmode == EX || _lmode == UD
#ifdef SM_DORA
                      || bIgnoreLocks
#endif
                      );

            bool would_block = false;  // was page unpinned during upgrade
            W_DO_GOTO(rc, _hdr_page().upgrade_latch_if_not_block(would_block));
            if (would_block) {
                unpin();
                // NB: CONVERT latch-lock to lock-lock
                // Acquire the page lock so we convert this possible 
                // latch-lock deadlock to lock-lock deadlock.
                // Only wait on the page lock if we can't get it. We
                // Accomplish this by first trying a conditional instant lock;
                // if that fails,  we do a long-term, unconditional lock.
                //
                // BUGBUG: (filed as GNATS 128) we are now trying to 
				// lock the PAGE in EX mode
                // rather than in IX mode (or the record in EX mode),
                // so we get unnecessary blocking here; If we're locking the
                // record in EX mode we only need the page in IX mode.
                // This shows up with the script pin_deadlock.1, where, having
                // converted to a lock-lock deadlock and discovered that,
                // we can't get the threads to sync because (even after the abort
                // due to this deadlock being caught), one of the pinners is
                // waiting on an EX page lock rather than having
                // success with the expected IX page lock.
                rc = lm->lock(_rid.pid, lmode, t_instant, WAIT_IMMEDIATE);
                if (rc.err_num() == eLOCKTIMEOUT) {
                    // get it long-term
                    INC_TSTAT(rec_repin_cvt);
                    W_DO_GOTO(rc, 
                        lm->lock(_rid.pid, lmode, t_long, WAIT_SPECIFIED_BY_XCT));
                }
                // we are willing to wait on the page latch
                /* NB:  race condition here for preemptive threads */

            } else {
                w_assert3(_hdr_page().latch_mode() == lock_to_latch(_lmode));
            }
        }
    }

    if (pinned()) {
#if W_DEBUG_LEVEL > 2
        // make sure record is where it's supposed to be
        record_t* tmp;
        W_DO_GOTO(rc, _hdr_page().get_rec(_rid.slot, tmp) );
        w_assert3(tmp == _rec);
#endif 
    } else {
        // find the page we need and latch it
        DBGTHRD(<<"repin");
        W_DO_GOTO(rc, fi->locate_page(_rid, _hdr_page(), lock_to_latch(_lmode)));
        W_DO_GOTO(rc, _hdr_page().get_rec(_rid.slot, _rec) );
        w_assert3(_rec);
        if (_start > 0 && _start >= _rec->body_size()) {
            rc = RC(eBADSTART);
            goto failure;
        }
        _flags = pin_rec_pinned;

        /*
         * See if the record is small or large.  Record number zero
         * is a special header record and is considered small
         */
        if (_rid.slot == 0 || _rec->is_small()) {  
            _start = 0;
            _len = _rec->body_size()-_start;
        } else {
            // keep _start as it is
            _len = MIN(((smsize_t)lgdata_p::data_sz),
                       _rec->body_size()-_start);
            _flags |= pin_separate_data;
        }
        INC_TSTAT(rec_pin_cnt);
    }

/* success: */
    _set_lsn();
    _check_lsn();
    w_assert1(rc.is_error() == false);
    return RCOK;
  
failure:
    _flags = pin_empty;
    return rc;
}

const char* pin_i::body()
{
    _check_lsn();
    if (!pinned() || (_flags & pin_hdr_only)) {
        return NULL;
    } else if (_flags & pin_separate_data) {
        return _body_large();
    }

    // must be a small record
    _check_lsn();
    w_assert3(is_aligned(_rec->body()));
    return _rec->body();
}
