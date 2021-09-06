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

/*<std-header orig-src='shore' incl-file-exclusion='BTREE_P_H'>

 $Id: btree_p.h,v 1.33 2010/05/26 01:20:37 nhall Exp $

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

#ifndef BTREE_P_H
#define BTREE_P_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#ifndef ZKEYED_H
#include <zkeyed.h>
#endif

struct btree_lf_stats_t;
struct btree_int_stats_t;


class btrec_t {
public:
    NORET            btrec_t()        {};
    NORET            btrec_t(const btree_p& page, slotid_t slot);
    NORET            ~btrec_t()        {};

    btrec_t&            set(const btree_p& page, slotid_t slot);
    
    smsize_t            klen() const    { return _key.size(); }
    smsize_t            elen() const    { return _elem.size(); }
    smsize_t            plen() const    { return (smsize_t)_prefix_bytes; }

    const cvec_t&        key() const    { return _key; }
    const cvec_t&        elem() const     { return _elem; }
    shpid_t            child() const    { return _child; }

private:
    shpid_t            _child;
    cvec_t            _key;
    cvec_t            _elem;
    int                _prefix_bytes;
    friend class btree_p;

    // disabled
    NORET            btrec_t(const btrec_t&);
    btrec_t&            operator=(const btrec_t&);
};

inline NORET
btrec_t::btrec_t(const btree_p& page, slotid_t slot)  
{
    set(page, slot);
}

class btree_p : public zkeyed_p {
public:
    friend class btrec_t;

    enum flag_t{
        t_none         = 0x0,
        t_smo         = 0x01,
        t_delete    = 0x02,
        t_compressed    = 0x10
    };
    struct btctrl_t {
        shpid_t    root;         // root page
        shpid_t    pid0;        // first ptr in non-leaf nodes
        int2_t    level;        // leaf if 1, non-leaf if > 1
        uint2_t    flags;        // a mask of flags
    };

    MAKEPAGE(btree_p, zkeyed_p, 1);

    
    int              level() const;
    shpid_t          pid0() const;
    lpid_t           root() const;
    shpid_t          root_shpid() const;
    bool             is_leaf() const;
    bool             is_leaf_parent() const;
    bool             is_node() const;

    bool             is_compressed() const;
    bool             is_smo() const;
    bool             is_delete() const;
    
    rc_t             set_hdr(
    shpid_t                root, 
    int                    level,
    shpid_t                pid0,
    uint2_t                flags);
    
    rc_t             set_root(shpid_t root);

    rc_t             set_pid0(shpid_t pid);

    rc_t             set_delete();
    rc_t             set_smo(bool compensate=false) {
                        return _set_flag(t_smo, compensate); }

    rc_t             clr_smo(bool compensate=false) { 
                        return _clr_flag(t_smo, compensate); }
    rc_t             clr_delete();

    rc_t             unlink_and_propagate(
                        const cvec_t&     key,
                        const cvec_t&     elem,
                        btree_p&        rsib,
                        lpid_t&        parent_pid,
                        const lpid_t&    root_pid,
			const bool bIgnoreLatches = false);
    rc_t             cut_page(lpid_t &child, slotid_t slot);
    
    rc_t            distribute(
    btree_p&              rsib,
    bool&                 left_heavy,
    slotid_t&             snum,
    smsize_t              addition, 
    int                   factor,
    const bool            bIgnoreLatches);

    void             print(sortorder::keytype kt = sortorder::kt_b,
                            bool print_elem=false);
    
    rc_t            shift(
    slotid_t             snum,
    btree_p&             rsib);

    rc_t            shift(
    slotid_t             snum,
    slotid_t             snum_dest,
    btree_p&             rsib);

    shpid_t         child(slotid_t idx) const;
    int             rec_size(slotid_t idx) const;
    int             nrecs() const;

    rc_t            search(
                        const cvec_t&             k,
                        const cvec_t&             e,
                        bool&                     found_key,
                        bool&                     found_key_elem,
                        slotid_t&             ret_slot
                        ) const;
    rc_t            insert(
                        const cvec_t&             key,
                        const cvec_t&             el,
                        slotid_t            slot, 
                        shpid_t             child = 0,
                        bool                do_it = true
                        );

    // stats for leaf nodes
    rc_t             leaf_stats(btree_lf_stats_t& btree_lf);
    // stats for interior nodes
    rc_t             int_stats(btree_int_stats_t& btree_int);


    static smsize_t         max_entry_size;
    static smsize_t         overhead_requirement_per_entry;

private:
    rc_t            _unlink(btree_p &, const bool bIgnoreLatches = false);
    rc_t            _clr_flag(flag_t, bool compensate=false);
    rc_t            _set_flag(flag_t, bool compensate=false);
    rc_t            _set_hdr(const btctrl_t& new_hdr);
    const btctrl_t& _hdr() const ;

};

inline const btree_p::btctrl_t&
btree_p::_hdr() const
{
    return * (btctrl_t*) zkeyed_p::get_hdr(); 
}

/*--------------------------------------------------------------*
 *    btree_p::root()                        * 
 *    Needed for logging/recovery                               *
 *--------------------------------------------------------------*/
inline lpid_t btree_p::root() const
{
    lpid_t p = pid();
    p.page = _hdr().root;
    return p;
}

inline shpid_t btree_p::root_shpid() const
{
    return _hdr().root;
}

/*--------------------------------------------------------------*
 *    btree_p::level()                        *
 *--------------------------------------------------------------*/
inline int btree_p::level() const
{
    return _hdr().level;
}

/*--------------------------------------------------------------*
 *    btree_p::pid0()                        *
 *--------------------------------------------------------------*/
inline shpid_t btree_p::pid0() const
{
    return _hdr().pid0;
}

/*--------------------------------------------------------------*
 *    btree_p::is_delete()                    *
 *--------------------------------------------------------------*/
inline bool btree_p::is_delete() const
{
    return (_hdr().flags & t_delete)!=0;
}

/*--------------------------------------------------------------*
 *    btree_p::is_compressed()                        *
 *--------------------------------------------------------------*/
inline bool btree_p::is_compressed() const
{
    // return _hdr().flags & t_compressed;
    return (_hdr().flags & t_compressed) != 0;
}

/*--------------------------------------------------------------*
 *    btree_p::is_smo()                        *
 *--------------------------------------------------------------*/
inline bool btree_p::is_smo() const
{
    return (_hdr().flags & t_smo)!=0;
}

/*--------------------------------------------------------------*
 *    btree_p::is_leaf()                    *
 *--------------------------------------------------------------*/
inline bool btree_p::is_leaf() const
{
    return level() == 1;
}

/*--------------------------------------------------------------*
 *    btree_p::is_leaf_parent()                    *
 *    return true if this node is the lowest interior node,     *
 *    i.e., the parent of a leaf.  Used to tell how we should   *
 *    latch a child page : EX or SH                             *
 *--------------------------------------------------------------*/
inline bool btree_p::is_leaf_parent() const
{
    return level() == 2;
}

/*--------------------------------------------------------------*
 *    btree_p::is_node()                    *
 *--------------------------------------------------------------*/
inline bool btree_p::is_node() const
{
    return ! is_leaf();
}

inline rc_t
btree_p::shift(
    slotid_t         snum,
    btree_p&         rsib)  
{
    w_assert9(level() == rsib.level());
    return zkeyed_p::shift(snum, &rsib, is_compressed());
}


inline rc_t
btree_p::shift(
    slotid_t         snum,
    slotid_t         snum_dest,
    btree_p&         rsib)  
{
    w_assert9(level() == rsib.level());
    return zkeyed_p::shift(snum, snum_dest, &rsib, is_compressed());
}


inline int
btree_p::rec_size(slotid_t idx) const
{
    return zkeyed_p::rec_size(idx);
}

inline int
btree_p::nrecs() const
{
    return zkeyed_p::nrecs();
}

/*<std-footer incl-file-exclusion='BTREE_P_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
