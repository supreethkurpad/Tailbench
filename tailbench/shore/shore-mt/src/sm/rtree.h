/*<std-header orig-src='shore' incl-file-exclusion='RTREE_H'>

 $Id: rtree.h,v 1.75 2010/05/26 01:20:41 nhall Exp $

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

#ifndef RTREE_H
#define RTREE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifndef NBOX_H
#include <nbox.h>
#endif

#ifdef __GNUG__
#pragma interface
#endif

struct rtree_stats_t;

const float  MIN_RATIO = (float)0.4;
const float  REMOVE_RATIO = (float)0.3;

enum oper_t { t_read = 0, t_insert, t_remove };

class sort_stream_i;
class rt_cursor_t;
class rtree_base_p;
class rtree_p;
class rtstk_t;
class ftstk_t;
class rtrec_t;

//
// rtree bulk load descriptor: specify heuristics for better clusering
//
struct rtld_desc_t {
    w_base_t::uint1_t   h;        // flag to indicate to apply heuristic or not
            // (DON'T USE BOOLEANS FOR ANYTHING PERSISTENT
            // because size of bool is implementation-dependent,
            // and we'll get purify errors an misaligned data
            // structures if we use bool if we use bool)
    fill1    dummy1;     // for alignment & for purify
    fill2    dummy2;     // for alignment & for purify

    int2_t     h_fill;    // heuristic fill factor
    int2_t     h_expn;    // heuristic expansion factor
    nbox_t*  universe;  // universal bounding box of spatial objects indexed

    rtld_desc_t() {
        h_fill = 65;
        h_expn = 120;
        h = true;
        universe = NULL;
    }

    rtld_desc_t(nbox_t* u, int2_t hff, int2_t hef) {
        h_fill = hff;
        h_expn = hef;
        h = (hff<100 && hef>100)?1:0;
        universe = u;
    }
};

class rtree_m : public smlevel_2 {
public:
    rtree_m()    {};
    ~rtree_m()  {};

    static rc_t            create(
    stid_t                 stid,
    lpid_t&             root,
    int2_t                 dim);

    static rc_t            lookup(
    const lpid_t&             root,
    const nbox_t&             key,
    void*                 el,
    smsize_t&            elen,
    bool&                found );
    
    static rc_t            insert(
    const lpid_t&             root,
    const nbox_t&             key, 
    const cvec_t&             elem);

    static rc_t            remove(
    const lpid_t&             root,
    const nbox_t&             key,
    const cvec_t&             elem);

    static rc_t            fetch_init(
    const lpid_t&             root,
    rt_cursor_t&             cursor
    );

    static rc_t            fetch(
    rt_cursor_t&             cursor,
    nbox_t&             key,
    void*                 el,
    smsize_t&            elen, 
    bool&                     eof,
    bool                 skip);

    static rc_t            print(const lpid_t& root);

    static rc_t            draw(
    const lpid_t&             root,
    ostream                &DrawFile,
        bool                 skip = false);

    static rc_t            stats(
    const lpid_t&             root,
    rtree_stats_t&            stat,
    uint2_t             size = 0,
    uint2_t*             ovp = NULL,
    bool                audit = true);

    static rc_t            bulk_load(
    const lpid_t&             root,
    int                nsrcs,
    const stid_t*            src,
    const rtld_desc_t&         desc, 
    rtree_stats_t&            stats);
    static rc_t            bulk_load(
    const lpid_t&             root,
    sort_stream_i&            sorted_stream,
    const rtld_desc_t&         desc, 
    rtree_stats_t&            stats);

    static bool         is_empty(const lpid_t& root);

private:
    
    friend class rtld_stk_t;
    
    static rc_t            _alloc_page(
    const lpid_t&             root,
    int2_t                level,
    const rtree_p&             near,
    int2_t                 dim,
    lpid_t&             pid);

    static rc_t            _search(
    const lpid_t&             root,
    const nbox_t&             key, 
    const cvec_t&             el, 
    bool&                found,
    rtstk_t&             pl,
    oper_t                 oper,
    bool                include_nulls);

    static rc_t            _traverse(
    const nbox_t&             key, 
    const cvec_t&             el,
    bool&                found, 
    rtstk_t&             pl,
    oper_t                 oper,
    bool                include_nulls);

    static rc_t            _dfs_search(
    const lpid_t&             root,
    const nbox_t&             key, 
    bool&                    found,
    nbox_t::sob_cmp_t         ctype, 
    ftstk_t&             fl,
    bool                include_nulls);

    static rc_t            _pick_branch(
    const lpid_t&             root,
    const nbox_t&             key,
    rtstk_t&             pl,
    int2_t                 lvl,
    oper_t                 oper); 
    
    static rc_t            _ov_treat(
    const lpid_t&             root, 
    rtstk_t&             pl,
    const nbox_t&             key, 
    rtree_p&             ret_page,
    bool*                lvl_split);

    static rc_t            _split_page(
    const lpid_t&             root,
    rtstk_t&             pl,
    const nbox_t&             key,
    rtree_p&             ret_page,
    bool*                lvl_split);

    static rc_t            _new_node(
    const lpid_t&             root,
    rtstk_t&             pl, 
    const nbox_t&             key,
    rtree_p&             subtree,
    bool*                lvl_split);

    static rc_t            _reinsert(
    const lpid_t&             root,
    const rtrec_t&             tuple,
    rtstk_t&             pl,
    int2_t                 level,
    bool*                lvl_split);

    // helper for _propagate_insert
    static rc_t            __propagate_insert(
    xct_t *                xd,
    rtstk_t&             pl);

    static rc_t            _propagate_insert(
    rtstk_t&             pl,
    bool                 compensate = true);
    
    // helper for _propagate_remove
    static rc_t            __propagate_remove(
    xct_t *                xd,
    rtstk_t&             pl);

    static rc_t            _propagate_remove(
    rtstk_t&             pl, 
    bool                 compensate = true);

    static rc_t            _draw(
    const lpid_t&             pid,
    ostream                &DrawFile,
    nbox_t                &CoverAll,
    bool                skip);
    
    static rc_t            _stats(
    const lpid_t&             root,
    rtree_stats_t&            stat,
    base_stat_t&            fill_sum,
    uint2_t                 size,
    uint2_t*                 ovp);
};

/*<std-footer incl-file-exclusion='RTREE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
