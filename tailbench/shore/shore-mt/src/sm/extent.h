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

/*<std-header orig-src='shore' incl-file-exclusion='EXTENT_H'>

 $Id: extent.h,v 1.14 2010/06/08 22:28:55 nhall Exp $

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

#ifndef EXTENT_H
#define EXTENT_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/


/********************************************************************
 * class extlink_t
 ********************************************************************/

class extlink_t {
    // Grot: this became 4-byte aligned when extnum_t grew to 4 bytes
    Pmap_Align4        pmap;        // LOGGED. this must be first
public:
    extnum_t           next;        // 4 bytes
    extnum_t           prev;        // 4 bytes
    snum_t             owner;       // 4 bytes
    uint4_t            pbucketmap; // 4 bytes, unlogged !!! used by
                                   // histograms

    static int       logged_size();
    NORET            extlink_t();
    NORET            extlink_t(const extlink_t& e);
    extlink_t&       operator=(const extlink_t&);

    void             clrall();
    void             setall();
    void             setmap(const Pmap &m);
    void             getmap(Pmap &m) const;
    void             set(int i);
    void             clr(int i);
    bool             is_set(int i) const;
    bool             is_clr(int i) const;
    int              first_set(int start) const;
    int              first_clr(int start) const;
    int              last_set(int start) const;
    int              last_clr(int start) const;
    int              num_set() const;
    int              num_clr() const;

    space_bucket_t         get_page_bucket(int i)const;

    friend ostream& operator<<(ostream &, const extlink_t &e);
};

inline NORET
extlink_t::extlink_t(const extlink_t& e) 
: pmap(e.pmap),
  next(e.next),
  prev(e.prev),
  owner(e.owner),
  pbucketmap(e.pbucketmap)
{
    // this is needed elsewhere -- see extlink_p::set_byte
    w_assert9(w_offsetof(extlink_t, pmap) == 0);
}

inline extlink_t& 
extlink_t::operator=(const extlink_t& e)
{
    pmap = e.pmap;
    prev = e.prev;
    next = e.next; 
    owner = e.owner;
    pbucketmap = e.pbucketmap;
    return *this;
}
inline void 
extlink_t::setmap(const Pmap &m)
{
    pmap = m;
}
inline void 
extlink_t::getmap(Pmap &m) const
{
    m = pmap;
    DBGTHRD(<<"getmap " << m);
}

inline void 
extlink_t::clrall()
{
    pmap.clear_all();
}

inline void 
extlink_t::setall()
{
    pmap.set_all();
}

inline void 
extlink_t::set(int i)
{
    pmap.set(i);
}

inline void 
extlink_t::clr(int i)
{
    pmap.clear(i);
}

inline bool 
extlink_t::is_set(int i) const
{
    w_assert9(i < smlevel_0::ext_sz);
    return pmap.is_set(i);
}

inline bool 
extlink_t::is_clr(int i) const
{
    return (! is_set(i));
}

inline int extlink_t::first_set(int start) const
{
    return pmap.first_set(start);
}

inline int 
extlink_t::first_clr(int start) const
{
    return pmap.first_clear(start);
}

inline int 
extlink_t::last_set(int start) const
{
    return pmap.last_set(start);
}

inline int 
extlink_t::last_clr(int start) const
{
    return pmap.last_clear(start);
}

inline int 
extlink_t::num_set() const
{
    return pmap.num_set();
}

inline int 
extlink_t::num_clr() const
{
    return pmap.num_clear();
}


/********************************************************************
* class extlink_p
********************************************************************/

class extlink_p : public page_p {
public:
    MAKEPAGE(extlink_p, page_p, 2); // make extent links a little hotter than
    // others

    // max # extent links on a page
    enum { max = data_sz / sizeof(extlink_t) };

    const extlink_t&         get(slotid_t idx);
    void             put(slotid_t idx, const extlink_t& e);
    void             set_byte(slotid_t idx, u_char bits, 
                          enum page_p::logical_operation);
    void             set_bytes(slotid_t idx,
                          smsize_t    offset,
                          smsize_t     count,
                          const uint1_t* bits, 
                          enum page_p::logical_operation);
    void             clr_pmap_bit(slotid_t idx, int bit); 
    static bool      on_same_page(extnum_t e1, extnum_t e2);

private:
    extlink_t&             item(int i);

    struct layout_t {
    extlink_t             item[max];
    };

    // disable
    friend class page_link_log;        // just to keep g++ happy
    friend class extlink_i;        // needs access to item
};

inline bool
extlink_p::on_same_page(extnum_t e1, extnum_t e2)
{
    shpid_t p1 = e1 / (extlink_p::max);
    shpid_t p2 = e2 / (extlink_p::max);
    return (p1 == p2);
}

inline extlink_t&
extlink_p::item(int i)
{
    w_assert9(i < max);
    return ((layout_t*)tuple_addr(0))->item[i];
}


inline const extlink_t&
extlink_p::get(slotid_t idx)
{
    return item(idx);
}

inline int
extlink_t::logged_size() {
    //
    // NOTE: watch the order of attributes so that we don't log the
    // pbucketmap!!!
    //
    // return __offsetof(extlink_t, pbucketmap);
    return w_offsetof(extlink_t, pbucketmap);
}

inline void
extlink_p::put(slotid_t idx, const extlink_t& e)
{
    DBG(<<"extlink_p::put(" <<  idx << " owner=" <<
    e.owner << ", " << e.next << ")");
    const vec_t    extent_vec_tmp(&e, extlink_t::logged_size());
    W_COERCE(overwrite(0, idx * sizeof(extlink_t), extent_vec_tmp));
}



struct stnode_t {
    extnum_t            head;
    w_base_t::uint2_t        eff;
    w_base_t::uint2_t        flags;
    w_base_t::uint2_t        deleting;
    fill2            filler; // align to 4 bytes
};


class stnode_p : public page_p {
    public:
    MAKEPAGE(stnode_p, page_p, 1);

    // max # store nodes on a page
    enum { max = data_sz / sizeof(stnode_t) };

    const stnode_t&       get(slotid_t idx);
    rc_t                  put(slotid_t idx, const stnode_t& e);

    private:
    stnode_t&             item(snum_t i);
    struct layout_t {
        stnode_t          item[max];
    };

    friend class page_link_log;        // just to keep g++ happy
    friend class stnode_i;        // needs access to item
};    

inline stnode_t&
stnode_p::item(snum_t i)
{
    w_assert9(i < max);
    return ((layout_t*)tuple_addr(0))->item[i];
}

inline const stnode_t&
stnode_p::get(slotid_t idx)
{
    return item(idx);
}

inline w_rc_t 
stnode_p::put(slotid_t idx, const stnode_t& e)
{
    const vec_t stnode_vec_tmp(&e, sizeof(e));
    W_DO(overwrite(0, idx * sizeof(stnode_t), stnode_vec_tmp));
    return RCOK;
}

/*********************************************************************
 *
 *  class extlink_i
 *
 *  Iterator on extent link area of the volume.
 *
 *  The general scheme for managing extents is described in the comment
 *  above (COMMENT)
 *
 *********************************************************************/
class extlink_i {
public:
    NORET            extlink_i(const lpid_t& root)
                    : 
                    _root(root) {
                        // The extent maps start on page 1. 
                        w_assert1(root.page >= 1);
                    }
            
    bool             on_same_root(extnum_t idx);
    
    lpid_t           get_pid(extnum_t idx) const;
    rc_t             get(extnum_t idx, const extlink_t* &);
    rc_t             get(extnum_t idx, extlink_t* &);
    rc_t             get_copy(extnum_t idx, extlink_t &);
    rc_t             put(extnum_t idx, const extlink_t&);
    bool             on_same_page(extnum_t ext1, extnum_t ext2) const ;

    rc_t             update_histo(extnum_t ext,     
                        int    offset,
                        space_bucket_t bucket);
    rc_t             fix_EX(extnum_t idx);
    void             unfix();
    rc_t             set_pmap_bits(snum_t snum, extnum_t idx, const Pmap &bits);
    rc_t             clr_pmap_bit(snum_t snum, extnum_t idx, int bit);

    rc_t             clr_pmap_bits(snum_t snum, extnum_t idx, const Pmap &bits);
    rc_t             set_next(extnum_t ext, extnum_t new_next, bool log_it = true);
    const extlink_p& page() const { return _page; } // for logging purposes

private:
    extid_t             _id;
    lpid_t              _root;
    extlink_p           _page;

    inline void        update_pmap(extnum_t idx,
                        const Pmap &pmap,
                        page_p::logical_operation how);
    inline void        update_pbucketmap(extnum_t idx,
                        uint4_t map,
                        page_p::logical_operation how);
};


/*********************************************************************
 *
 *  class stnode_i
 *
 *  Iterator over store node area of volume.
 *
 *********************************************************************/
class stnode_i: private smlevel_0 {
public:
    NORET               stnode_i(const lpid_t& root) : _root(root) {
                             // store nodes are after extent links and
                             // those start on page 1.
                             w_assert1(root.page >= 1);
                        };
    w_rc_t              get(snum_t idx, stnode_t &stnode);
    w_rc_t              get(snum_t idx, const stnode_t *&stnode);
    w_rc_t              put(snum_t idx, const stnode_t& stnode);      
    w_rc_t              store_operation(const store_operation_param & op);
private:
    lpid_t              _root;
    stnode_p            _page;
};


/*<std-footer incl-file-exclusion='EXTENT_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
