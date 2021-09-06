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

/*<std-header orig-src='shore' incl-file-exclusion='BTCURSOR_H'>

 $Id: btcursor.h,v 1.9.2.4 2010/01/28 04:53:57 nhall Exp $

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

#ifndef BTCURSOR_H
#define BTCURSOR_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

class btree_p;
class btrec_t;

class bt_cursor_t : smlevel_2 
{
public:
    NORET            bt_cursor_t(bool include_nulls);
    NORET            ~bt_cursor_t() ;

    rc_t            check_bounds();

    rc_t            set_up(
    const lpid_t&             root, 
    int                nkc,
    const key_type_s*        kc,
    bool                unique,
    concurrency_t            cc,
    cmp_t                cond2,
    const cvec_t&            bound2,
    lock_mode_t            mode = SH);

    rc_t            set_up_part_2(
    cmp_t                cond1,
    const cvec_t&            bound1
    );

    // for mrbt index scan
    void            set_roots(vector<lpid_t>& roots);
    bool            get_next_root();
    bool is_mrbt;
    void            set_slot(int slot) { _slot = slot; }
    void            set_pid(const lpid_t& pid) { _pid = pid; }
    
    lpid_t            root()     const { return _root; }
    const lpid_t&        pid()     const { return _pid; }
    const lsn_t&        lsn()     const { return _lsn; }
    int                slot()   const { return _slot; }
    bool            first_time;
    bool            keep_going;
    bool            unique() const { return _unique; }
    concurrency_t        cc()     const { return _cc; }
    int                nkc()     const { return _nkc; }
    const key_type_s*        kc()     const { return _kc; }
    cmp_t            cond1()     const { return _cond1;}
    const cvec_t&         bound1() const { return *_bound1;}
    cmp_t            cond2()     const { return _cond2;}
    const cvec_t&         bound2() const { return *_bound2;}
    lock_mode_t            mode()   const { return _mode; }

    bool                        inbounds(const cvec_t&, bool check_both, 
                                      bool& keep_going) const;
    bool                        inbounds(const btrec_t &r, bool check_both, 
                                      bool& keep_going) const;
    bool             inbound( const cvec_t &    v, 
                      cmp_t        cond,
                      const cvec_t &    bound,
                      bool&        more) const;

    bool            is_valid() const { return _slot >= 0; } 
    bool            is_backward() const { return _backward; }
    rc_t             make_rec(const btree_p& page, int slot);
    void             free_rec();
    void             update_lsn(const btree_p&page);
    int             klen() const   { return _klen; } 
    char*            key()     { return _eof ? 0 : _space; }
    bool            eof()     { return _eof;  }
    int                elen() const     { return _elen; }
    char*            elem()     { return _eof ? 0 :  _space + _klen; }

    void            delegate(void*& ptr, int& kl, int& el);

private:
    lpid_t            _root;

    // for mrbt index scan
    vector<lpid_t> _roots;
    int _next_root;
    
    bool            _unique;
    smlevel_0::concurrency_t    _cc;
    int                _nkc;
    const key_type_s*        _kc;

    int                _slot;
    char*            _space;
    int                _splen;
    int                _klen;
    int                _elen;
    lsn_t            _lsn;
    lpid_t            _pid;
    cmp_t            _cond1;
    char*            _bound1_buf;
    cvec_t*            _bound1;
    cvec_t            _bound1_tmp; // used if cond1 is not
                             // pos or neg_infinity

    cmp_t            _cond2;
    char*            _bound2_buf;
    cvec_t*            _bound2;
    cvec_t            _bound2_tmp; // used if cond2 is not
                             // pos or neg_infinity
    lock_mode_t            _mode;
    bool            _backward; // for backward scans
    bool            _eof; // no element left
    bool            _include_nulls; 
};

inline NORET
bt_cursor_t::bt_cursor_t(bool include_nulls)
    : is_mrbt(false), first_time(false), keep_going(true), _slot(-1), 
      _space(0), _splen(0), _klen(0), _elen(0), 
      _bound1_buf(0), _bound2_buf(0), _backward(false), _eof(false),
      _include_nulls(include_nulls)
{
}

inline NORET
bt_cursor_t::~bt_cursor_t()
{
    if (_space)  {
    delete[] _space;
    _space = 0;
    }
    if (_bound1_buf) {
    delete[] _bound1_buf;
    _bound1_buf = 0;
    }
    if (_bound2_buf) {
    delete[] _bound2_buf;
    _bound2_buf = 0;
    }
    _slot = -1;
    _pid = lpid_t::null;
}


inline void 
bt_cursor_t::free_rec()
{
    _eof = true;
    _klen = _elen = 0;
    _slot = -1;
    _pid = lpid_t::null;
    _lsn = lsn_t::null;
}

inline void
bt_cursor_t::delegate(void*& ptr, int& kl, int& el)
{
    kl = _klen, el = _elen;
    ptr = (void*) _space;
    _space = 0; _splen = 0;
}

/*<std-footer incl-file-exclusion='BTCURSOR_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
