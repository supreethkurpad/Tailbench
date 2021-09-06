/*<std-header orig-src='shore' incl-file-exclusion='RTREE_P_H'>

 $Id: rtree_p.h,v 1.20 2010/05/26 01:20:41 nhall Exp $

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

#ifndef RTREE_P_H
#define RTREE_P_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#ifndef KEYED_P_H
#include <keyed.h>
#endif

//
// control header for rtree page
//
struct rtctrl_t {
    lpid_t    root;
    int2_t    level;    // leaf if 1, non-leaf if > 1
    int2_t    dim;    // for rtree, this is dimension.  
};

#ifdef __GNUC__
class _rtrec_t_dummy_ {
    friend class _rtrec_t;
public: 
};
#endif

class rtrec_t : public keyrec_t  {
public:
#ifdef __GNUC__
    friend class _rtrec_t_dummy_;
#endif
private:
    rtrec_t()    {};        // disabled
};

class rtree_base_p : public keyed_p {
protected:
    rtctrl_t&             _hdr();
    const rtctrl_t&         _hdr() const;
    
public:
    MAKEPAGE(rtree_base_p, keyed_p, 1);

    enum {
    // max # of boxes in a page
    max_scnt = (data_sz - sizeof(rtctrl_t)) / 
        (sizeof(rtrec_t) + 5*sizeof(int) + sizeof(slot_t)) + 1,
    smap_sz = max_scnt / sizeof(u_char) + 1
    };

    rc_t            set_hdr(const rtctrl_t& new_hdr);
    rc_t            set_level(int2_t l);
    rc_t            set_dim(int2_t d=2);

    bool             is_leaf() const;
    bool             is_node() const;

    int             level() const;
    int             dim() const;
    
    void             root(lpid_t& r) const;
    shpid_t             root() const;
    
    rc_t            insert(
    const cvec_t&            key,
    const cvec_t&             el, 
    int                 slot, 
    shpid_t             child = 0);
    rc_t             remove(slotid_t slot);
    rc_t             remove(int slot) { return remove(slotid_t(slot)); }

    rc_t            shift(int snum, rtree_base_p* rsib);
    const rtrec_t&         rec(int idx) const;
    int             nrecs() const;

    friend class page_link_log;   // just to keep g++ happy
};

class rtree_p : public rtree_base_p {

    bool _key_match(const nbox_t& key, int slot, bool include_nulls,
            bool& bigger) const;
    bool exact_match(
        const nbox_t& key, u_char smap[], const cvec_t& el,
            const shpid_t child,
        bool include_nulls
        );
    bool spatial_srch(const nbox_t& key, nbox_t::sob_cmp_t ctype, u_char smap[],
            int2_t& numSlot,
            bool include_nulls);

public:

    MAKEPAGE(rtree_p, rtree_base_p, 1);

    NORET            rtree_p(const rtree_base_p &rbp);
    rc_t            calc_bound(nbox_t& nbound);
    void             pick_optimum(
    const nbox_t&             key,
    int2_t&                 ret_slot);

    rc_t            insert(const rtrec_t& tuple);
    rc_t            insert(
    const nbox_t&             key,
    const cvec_t&             el,
    shpid_t             child = 0);

    rc_t             ov_remove(
    rtree_p*             tmp,
    const nbox_t&             key, 
    const nbox_t&             bound);
    bool             search(
    const nbox_t&             key,
    nbox_t::sob_cmp_t         ctype,
    u_char                 smap[],
    int2_t&             num_slot, 
    bool                 include_nulls,
    const cvec_t*             el = 0, 
    const shpid_t             child = 0
    );
    bool             query(
    const nbox_t&             key, 
    nbox_t::sob_cmp_t         ctype,
    u_char                 smap[], 
    int2_t&                num_slot,
    bool                include_nulls);

    void             print();
    void             draw(ostream &, nbox_t &);
    uint2_t          ovp();        

};

//
// path stack for rtree_base
//

struct rtstk_entry {
    rtree_base_p page;
    int2_t idx;
};

class rtstk_t {
  public:
    enum { max_rtstk_sz = 10 };

  private:
    rtstk_entry         _stk[max_rtstk_sz];
    int2_t             _top;

  public:
    NORET            rtstk_t();
    NORET            ~rtstk_t();

    void             push(const rtree_base_p& page, int2_t index);

    rtstk_entry&         pop();
    rtstk_entry&         top();
    rtstk_entry&         second();
    rtstk_entry&         bottom();

    void            update_top(int2_t index);

    void             drop_all_but_last(); 

    int2_t             size() { return _top; }

    bool             is_full()    { return _top >= max_rtstk_sz; }
    bool             is_empty()   { return _top == 0; }
};

//
// dfs search stack for rtree_base
//
const int ftstk_chunk = 50;

class ftstk_t {
  private:
    int2_t _top;
    shpid_t _stk[ftstk_chunk];
    shpid_t* _indirect[ftstk_chunk];

  public:
    ftstk_t();
    ~ftstk_t();

    void push(const shpid_t pid) {
    w_assert1(! is_full());
    if (_top < ftstk_chunk) _stk[_top++] = pid;
    else {
        int2_t pos = _top / ftstk_chunk - 1;
        int2_t off = _top % ftstk_chunk;
        if (!_indirect[pos])
        _indirect[pos] = new shpid_t[ftstk_chunk];
        _indirect[pos][off] = pid;
        _top++;
        }
    }

    shpid_t pop() {
    w_assert1(! is_empty());
    if (_top <= ftstk_chunk) return _stk[--_top];
    else {
        _top--;
        shpid_t pid = _indirect[_top/ftstk_chunk-1][_top%ftstk_chunk];
        if (_top>=ftstk_chunk && _top%ftstk_chunk==0) {
        delete _indirect[_top/ftstk_chunk - 1];
        _indirect[_top/ftstk_chunk - 1] = 0;
        }
        return pid;
        }
    }

    void empty_all();
        
    bool is_full()    { return _top >= ftstk_chunk+ftstk_chunk*ftstk_chunk; }
    bool is_empty()   { return _top == 0; }
};

class rt_cursor_t {
public:
    bool     _include_nulls;
    nbox_t::sob_cmp_t   cond;
    nbox_t    qbox;
    ftstk_t     fl;

    int2_t    num_slot;
    int2_t    idx;
    u_char    smap[rtree_p::smap_sz];
    rtree_p     page;

    rt_cursor_t(bool include_nulls=false) : _include_nulls(include_nulls) { 
    num_slot = idx = 0; bm_zero(smap, sizeof(u_char)*8); 
    }
    bool thread_can_delete() const {
    if(page.is_fixed()) return page.pinned_by_me();
    return true;
    }
};

struct wrk_branch_t {
    nbox_t rect;
    int2_t idx;
    double area;
};

inline rtctrl_t&
rtree_base_p::_hdr()
{
    return * (rtctrl_t*) keyed_p::get_hdr(); 
}

inline const rtctrl_t&
rtree_base_p::_hdr() const
{
    return * (rtctrl_t*) keyed_p::get_hdr(); 
}

/*--------------------------------------------------------------*
 *    rtree_base_p::root()                    *
 *--------------------------------------------------------------*/
inline void rtree_base_p::root(lpid_t& r) const
{
    r = _hdr().root;
}

inline shpid_t rtree_base_p::root() const
{
    return _hdr().root.page;
}

inline int rtree_base_p::level() const
{
    return _hdr().level;
}

inline int rtree_base_p::dim() const
{
    return _hdr().dim;
}

inline bool rtree_base_p::is_leaf() const
{
    return level() == 1;
}

inline bool rtree_base_p::is_node() const
{
    return ! is_leaf();
}

inline rc_t
rtree_base_p::insert(
    const cvec_t& key, 
    const cvec_t& el, 
    int slot, 
    shpid_t child)
{
    return keyed_p::insert(key, el, slot, child);
}


inline rc_t
rtree_base_p::shift(int snum, rtree_base_p* rsib)
{
    w_assert3(level() == rsib->level());
    return keyed_p::shift(snum, rsib);
}

inline const rtrec_t&
rtree_base_p::rec(int idx) const
{
    return (rtrec_t&) keyed_p::rec(idx);
}

inline int
rtree_base_p::nrecs() const
{
    return keyed_p::nrecs();
}

inline NORET
rtree_p::rtree_p(const rtree_base_p& rbp)
    : rtree_base_p(rbp)
{
}

inline NORET
rtstk_t::rtstk_t()
    : _top(0)
{
}

inline NORET
rtstk_t::~rtstk_t()
{
}

inline void
rtstk_t::push(const rtree_base_p& page, int2_t index) 
{
    w_assert1(! is_full());
    _stk[_top].page = page;
    _stk[_top++].idx = index;
}

inline rtstk_entry&
rtstk_t::pop()
{
    w_assert1(! is_empty());
    return _stk[--_top];
}

inline rtstk_entry&
rtstk_t::top()
{
    w_assert1(! is_empty());
    return _stk[_top-1];
}

inline rtstk_entry&
rtstk_t::second()
{
    w_assert1( _top>1 );
    return _stk[_top-2];
}

inline rtstk_entry&
rtstk_t::bottom()
{
    w_assert1(! is_empty());
    return _stk[0];
}

inline void
rtstk_t::update_top(int2_t index)
{
    w_assert1(! is_empty());
    _stk[_top-1].idx = index;
}

/*<std-footer incl-file-exclusion='RTREE_P_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
