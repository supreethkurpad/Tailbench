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

 $Id: btcursor.cpp,v 1.16 2010/06/08 22:28:55 nhall Exp $

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
#define BTREE_C

#ifdef __GNUG__
#   pragma implementation "btcursor.h"
#endif

#include "sm_int_2.h"
#include "lexify.h"
#include "btree_p.h"
#include "btcursor.h"

rc_t
bt_cursor_t::set_up( const lpid_t& root, int nkc, const key_type_s* kc,
                     bool unique, concurrency_t cc, 
                     cmp_t cond2, const cvec_t& bound2, lock_mode_t mode)
{
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_im) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl)
        ) return RC(smlevel_0::eBADCCLEVEL);
    _root = root;
    _nkc = nkc;
    _kc = kc;
    _unique = unique;
    _cc = cc;
    _cond2 = cond2;
    _mode = mode;

    if (_bound2_buf) {
        // get rid of old _bound2_buf
        delete [] _bound2_buf;
        _bound2_buf = 0;
    }

    /*
     * Cache bound 2
     */
    if (bound2.is_pos_inf())  {
        _bound2 = &(cvec_t::pos_inf);
    } else if (bound2.is_neg_inf())  {
        _bound2 = &(cvec_t::neg_inf);
    } else {
        size_t len = bound2.size();
        _bound2_buf = new char[len];
        if (! _bound2_buf)  {
            return RC(eOUTOFMEMORY);
        }
        bound2.copy_to(_bound2_buf, len);
        _bound2 = &_bound2_tmp;
        _bound2->put(_bound2_buf, len);
    }

    return RCOK;
}
rc_t
bt_cursor_t::set_up_part_2(cmp_t cond1, const cvec_t& bound1)
{
    _cond1 = cond1;

    if (_bound1_buf) {
        // get rid of old _bound1_buf
        delete [] _bound1_buf;
        _bound1_buf = 0;
    }
    /*
     * Cache bound 1
     */
    if (bound1.is_pos_inf())  {
        _bound1 = &(cvec_t::pos_inf);
    } else if (bound1.is_neg_inf())  {
        _bound1 = &(cvec_t::neg_inf);
    } else {
        size_t len = bound1.size();
        _bound1_buf = new char[len];
        if (! _bound1_buf)  {
            return RC(eOUTOFMEMORY);
        }
        bound1.copy_to(_bound1_buf, len);
        _bound1 = &_bound1_tmp;
        _bound1->put(_bound1_buf, len);
    }

    return check_bounds();
}

void
bt_cursor_t::set_roots(vector<lpid_t>& roots) {
    is_mrbt = true;
    _roots = roots;
    if(_backward) {
	_next_root = 1;
    } else {
	_next_root = roots.size() - 2;
    }
}

bool
bt_cursor_t::get_next_root() {
    if(_backward && (size_t) _next_root != _roots.size()) {
	_root = _roots[_next_root];
	_next_root++;
	return true;
    } else if(_next_root >= 0) {
	_root = _roots[_next_root];
	_next_root--;
	return true;
    } else {
	return false;
    }
}

/*********************************************************************
 *
 *  bt_cursor_t::inbounds(r, check_both_bounds, keep_going)
 *  bt_cursor_t::inbounds(v, check_both_bounds, keep_going)
 *
 *  Given a btrec_t r or a vector v,
 *  see if it meets the lower bound (!check_both_bounds)
 *  or the lower and upper bound (check_both_bounds).
 *  Set keep_going to true if, based
 *  on the upper bound, there's a chance that we could meet it later.
 *  This can only happen if 
 *    bounds: >10 && == 20, and r points at, say, 12, or
 *    bounds: >10 && < 20, and r points at, say, 9 (shouldn't ever
 *                            get there, though -- other aspects of the
 *                      algorithm should prevent us from ever getting
 *                         this case)
 *
 *********************************************************************/

bool
bt_cursor_t::inbounds(
        const cvec_t &v, 
        bool check_both_bounds,
        bool& _keep_going
) const
{
    /*
     * See if key is out of bounds
     * Handle weird cases like 
     *      >= 22 == 23
     *      >= 22 <= 20
     *      == 22 <= 30
     *      >= 30 <= 20
     */

    bool ok1 = false;  // true if meets criterion 1
    bool ok2 = false;  // true if meets criterion 2
    bool _in_bounds = false;  // set to true if this
                        // kv pair meets BOTH criteria
    bool more = false;  // might be more if we were to keep going

    // because we start by traversing for the lower(upper) bound,
    // this should be true:
	DBG(<<"v <= bound2() == " << (bool)(v <= bound2()) );
	DBG(<<"v >= bound1() == " << (bool)(v <= bound1()) );
	W_IFDEBUG3(
    if(is_backward()) {
        if(!v.is_null()) { w_assert3(v <= bound2()); }
    } else {
        if(!v.is_null()) { w_assert3(v >= bound1()); }
	}
    )

    ok1 = inbound(v, cond1(), bound1(), more);

    DBG(<< "bound1: " << bound1() << " cond1()=" <<  int(cond1()));
    DBG(<< "ok wrt lower (bound1): " << ok1);

    // don't bother testing upper until we've
    // reached lower

    if(!check_both_bounds) return ok1;

    bool more2 = false;  // might be more if we were to keep going
    if(ok1) ok2 = inbound(v, cond2(), bound2(), more2);

    DBG(<< "ok wrt upper (bound2): " << ok2);
    DBG(<< "bound2: " << bound2() << " cond2()=" <<  int(cond2()));

    if(ok1 && ok2) {
        _in_bounds = true;
        _keep_going = false;
    } else {
        _in_bounds = false;
        _keep_going = more2; // more --> ok1
            // this happens if upper bound is == x,
            // and we haven't yet reached x

    }
    DBG(<< "in_bounds: " << _in_bounds
        << " keep_going: " << _keep_going);
    return _in_bounds;
}

bool
bt_cursor_t::inbound(
    const cvec_t &        v, 
    cmp_t                cond,
    const cvec_t &        bound,
    bool&                more
) const
{
    bool ok=false;
    switch (cond) {
    case eq:
        ok = (v == bound);
        more = _backward? (v > bound) : (v < bound);
        break;

    case ge:
        ok = (v >= bound);
        break;

    case gt:
        if(_include_nulls && v.is_null() && bound.is_neg_inf()) {
            ok = true;
        } else {
            ok = (v > bound);
        }
        break;

    case le:
        ok= (v <= bound);
        break;

    case lt:
        if(_include_nulls && v.is_null() && bound.is_pos_inf()) {
            ok = true;
        } else {
            ok = (v < bound);
        }
        break;

    default:
        W_FATAL(eINTERNAL);
    }
    DBG(<< "ok=" << ok
        << " v=" << v
        << " bound=" << bound 
        << " cond=" << int(cond)
        << " more=" << more

        );
    return ok;
}

bool
bt_cursor_t::inbounds(
        const btrec_t &r, 
        bool check_both_bounds,
        bool& _keep_going) 
        const
{
    return inbounds(r.key(), check_both_bounds, _keep_going);
}

void                         
bt_cursor_t::update_lsn(const btree_p& page)
{
    if(_pid == page.pid()) _lsn = page.lsn();
}

/*********************************************************************
 *
 *  bt_cursor_t::make_rec(page, slot)
 *
 *  Make the cursor point to record at "slot" on "page".
 *
 *********************************************************************/
rc_t
bt_cursor_t::make_rec(const btree_p& page, int slot)
{
    FUNC(bt_cursor_t::make_rec);

    DBG(<<" make_rec slot " << slot);

    /*
     *  Fill up pid, lsn, and slot
     */
    _pid = page.pid();
    _lsn = page.lsn();
    _slot = slot;

    /*
     *  Copy the record to buffer
     */
    btrec_t r(page, slot);
    _klen = r.klen();
    _elen = r.elen();
    if (_klen + _elen > _splen)  {
        if (_space) { delete[] _space; _space=0; }
        if (! (_space = new char[_splen = _klen + _elen]))  {
            _klen = 0;
            return RC(eOUTOFMEMORY);
        }
    }
    w_assert3(_klen + _elen <= _splen);

    bool in_bounds =  inbounds(r, true, keep_going);

    DBG(<< "in_bounds: " << in_bounds << " keep_going: " << keep_going);

    if (in_bounds) {
        // key is in bounds, so put it into proper order
        cvec_t* user_key;
        if(_klen > 0) {
            W_DO(bt->_unscramble_key(user_key, r.key(), _nkc, _kc));
            user_key->copy_to(_space);
        }
        r.elem().copy_to(_space + _klen);
    } else if(!keep_going) {
        free_rec();
    } // else do nothing -- don't want to blow away pid

    return RCOK;
}

rc_t
bt_cursor_t::check_bounds()
{
    /*
     *  Check for valid conditions.
     *  Insist that first is > >= or ==
     *  Insist that 2nd is < <= or ==
     *  These checks are only slightly simplifying
     *  because they do not prevent such things as:
     *   > 30 <= 30 
     *   == 30 <= 20
     */
    bool b1=false, b2=false;
    w_assert3(!_backward);

    if (! (cond1() == eq || cond1() == gt || cond1() == ge) )  {
        b1 = true;
    }

    if (! (cond2() == eq || cond2() == lt || cond2() == le) )  {
        b2 = true;
    }

    if (cond1() == eq || cond2()== eq) {
        if((cond1() == eq)  && (cond2() == eq) ) {
            if (bound1() != bound2()) {
                return RC(eBADCMPOP);
            }
        }
    }
    if(b1 != b2) return RC(eBADCMPOP);

    if(b1 && b2) {
        if (bound1() >= bound2()) {
            _backward = true;
        } else {
            return RC(eBADCMPOP);
        }
    } else { // b1 == b2 == false
        if (bound1() > bound2()) {
            return RC(eBADCMPOP);
        }
    }
    if(is_backward()) {
        DBG(<<"BACKWARD SCAN " );
        cvec_t  *b= _bound2;
        _bound2 = _bound1;
        _bound1 = b;

        cmp_t        c = cond2();
        _cond2 = cond1();
        _cond1 = c;
    }

    return RCOK;
}

