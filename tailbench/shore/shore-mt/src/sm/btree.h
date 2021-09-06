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

/*<std-header orig-src='shore' incl-file-exclusion='BTREE_H'>

 $Id: btree.h,v 1.131.2.7 2010/03/19 22:20:23 nhall Exp $

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

#ifndef BTREE_H
#define BTREE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 *  Interface to btree manager.  
 *  NB: put NO INLINE FUNCTIONS here.
 *  Implementation is class btree_impl, in btree_impl.[ch].
 */
#ifdef __GNUG__
#pragma interface
#endif

#ifndef LEXIFY_H
#include <lexify.h>
#endif

class sort_stream_i;
class btree_p;
struct btree_stats_t;
class bt_cursor_t;

/*--------------------------------------------------------------*
 *  class btree_m                                                *
 *--------------------------------------------------------------*/
class btree_m : public smlevel_2 {
    friend class btree_p;
    friend class btree_impl;
    friend class bt_cursor_t;
    friend class btree_remove_log;
    friend class btree_insert_log;
    friend class btree_purge_log;

public:
    NORET                        btree_m()   {};
    NORET                        ~btree_m()  {};

    static smsize_t                max_entry_size(); 

    typedef bt_cursor_t         cursor_t;

    static rc_t                        create(
        const stid_t &              stid,
        lpid_t&                     root,
        bool                        compressed,
	const bool                  bIgnoreLatches = false
        );
    static rc_t                        bulk_load(
        const lpid_t&                     root,
        int                               nsrcs,
        const stid_t*                     src,
        int                               nkc,
        const key_type_s*                 kc,
        bool                              unique,
        concurrency_t                     cc,
        btree_stats_t&                    stats,
        bool                              sort_duplicates = true,
        bool                              lexify_keys   = true
        );
    static rc_t                        bulk_load(
        const lpid_t&                     root,
        sort_stream_i&                    sorted_stream,
        int                               nkc,
        const key_type_s*                 kc,
        bool                              unique,
        concurrency_t                      cc,
        btree_stats_t&                    stats);
    
    static rc_t                        mr_bulk_load(
        key_ranges_map&                   partitions,
        int                               nsrcs,
        const stid_t*                     src,
        int                               nkc,
        const key_type_s*                 kc,
        bool                              unique,
        concurrency_t                     cc,
        btree_stats_t&                    stats,
        bool                              sort_duplicates = true,
        bool                              lexify_keys   = true,
	const bool                        bIgnoreLatches = false
        );
    static rc_t                        mr_bulk_load(
        key_ranges_map&                   partitions,
        sort_stream_i&                    sorted_stream,
        int                               nkc,
        const key_type_s*                 kc,
        bool                              unique,
        concurrency_t                      cc,
        btree_stats_t&                    stats);
    static rc_t                        mr_bulk_load_l(
        key_ranges_map&                   partitions,
        int                               nsrcs,
        const stid_t*                     src,
        int                               nkc,
        const key_type_s*                 kc,
        bool                              unique,
        concurrency_t                     cc,
        btree_stats_t&                    stats,
        bool                              sort_duplicates = true,
        bool                              lexify_keys   = true,
	const bool                        bIgnoreLatches = false
        );
    static rc_t                        mr_bulk_load_l(
        key_ranges_map&                   partitions,
        sort_stream_i&                    sorted_stream,
        int                               nkc,
        const key_type_s*                 kc,
        bool                              unique,
        concurrency_t                      cc,
        btree_stats_t&                    stats);
    static rc_t                        split_tree(
        const lpid_t&                     root_old,
	const lpid_t&                     root_new,
        const cvec_t&                     key,
	lpid_t&                           leaf_old,
	lpid_t&                           leaf_new,
	const bool                        bIgnoreLatches);
    static rc_t                        relocate_recs_l(
        lpid_t&                   leaf_old,
        const lpid_t&                   leaf_new,
	const bool bIgnoreLatches = false,
	RELOCATE_RECORD_CALLBACK_FUNC relocate_callback = NULL);
    static rc_t                        relocate_recs_p(
        const lpid_t&                   root_old,
        const lpid_t&                   root_new,
	const bool bIgnoreLatches = false,
	RELOCATE_RECORD_CALLBACK_FUNC relocate_callback = NULL);
    static rc_t                        merge_trees(
        lpid_t&                           root,
        const lpid_t&                     root1,
	const lpid_t&                     root2,
        cvec_t&                           startKey2,
	const bool                        update_owner = false,
	const bool                        bIgnoreLatches = false);
    static rc_t                    mr_insert(
        const lpid_t&                     root,
	bool                              unique,
        concurrency_t                     cc,
        const cvec_t&                     key,
        const cvec_t&                     elem,
        int                               split_factor = 50,
	const bool                        bIgnoreLatches = false);
   static rc_t                    mr_insert_l(
        const lpid_t&                     root,
	bool                              unique,
        concurrency_t                     cc,
        const cvec_t&                     key,
        //rc_t (*fill_el)(vec_t&, const lpid_t&), 
	el_filler*                       ef,
	size_t el_size,
        int                               split_factor = 50,
	const bool                        bIgnoreLatches = false,
	RELOCATE_RECORD_CALLBACK_FUNC relocate_callback = NULL);
    static rc_t                    mr_insert_p(
        const lpid_t&                     root,
	bool                              unique,
        concurrency_t                     cc,
        const cvec_t&                     key,
	//rc_t (*fill_el)(vec_t&, const lpid_t&),
	el_filler*                       ef,
	size_t el_size,
        int                               split_factor = 50,
	const bool                        bIgnoreLatches = false);
    static rc_t                        mr_remove(
        const lpid_t&                    root,
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                    key,
        const cvec_t&                    elem,
	const bool                        bIgnoreLatches);
    static rc_t                        mr_remove_key(
        const lpid_t&                    root,
        int                              nkc,
        const key_type_s*                kc,
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                    key,
        int&                             num_removed,
	const bool                       bIgnoreLatches);
    static rc_t                        mr_lookup(
        const lpid_t&                    root, 
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                    key_to_find, 
        void*                            el, 
        smsize_t&                        elen,
        bool&                            found,
	const bool                       bIgnoreLatches);
    static rc_t                        mr_update(
        const lpid_t&                    root, 
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                    key_to_find, 
        const cvec_t&                    old_el, 
	const cvec_t&                    new_el,
        bool&                            found,
	const bool                       bIgnoreLatches);

    static rc_t                        insert(
        const lpid_t&                     root,
        int                               nkc,
        const key_type_s*                 kc,
        bool                              unique,
        concurrency_t                     cc,
        const cvec_t&                     key,
        const cvec_t&                     elem,
        int                               split_factor = 50);
    static rc_t                        remove(
        const lpid_t&                    root,
        int                              nkc,
        const key_type_s*                kc,
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                    key,
        const cvec_t&                    elem);
    static rc_t                        remove_key(
        const lpid_t&                    root,
        int                              nkc,
        const key_type_s*                kc,
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                    key,
        int&                             num_removed);

    static void                 print(const lpid_t& root, 
                                    sortorder::keytype kt = sortorder::kt_b,
                                    bool print_elem = true);

    static rc_t                        lookup(
        const lpid_t&                     root, 
        int                            nkc,
        const key_type_s*            kc,
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                     key_to_find, 
        void*                            el, 
        smsize_t&                     elen,
        bool&                     found,
	bool                         use_dirbuf = false);

    /* for lid service only */
    static rc_t                        lookup_prev(
        const lpid_t&                     root, 
        int                            nkc,
        const key_type_s*            kc,
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                     key, 
        bool&                             found,
        void*                            key_prev,
        smsize_t&                    key_prev_len);
    static rc_t                        fetch_init(
        cursor_t&                     cursor, 
        const lpid_t&                     root,
        int                            nkc,
        const key_type_s*            kc,
        bool                             unique, 
        concurrency_t                    cc,
        const cvec_t&                     key, 
        const cvec_t&                     elem,
        cmp_t                            cond1,
        cmp_t                           cond2,
        const cvec_t&                    bound2,
        lock_mode_t                    mode = SH,
	const bool bIgnoreLatches = false);

        static rc_t                        mr_fetch_init(
        cursor_t&                     cursor, 
        vector<lpid_t>&                     roots,
        int                            nkc,
        const key_type_s*            kc,
        bool                             unique, 
        concurrency_t                    cc,
        const cvec_t&                     key, 
        const cvec_t&                     elem,
        cmp_t                            cond1,
        cmp_t                           cond2,
        const cvec_t&                    bound2,
        lock_mode_t                    mode = SH,
	const bool bIgnoreLatches = false);

    static rc_t                        fetch_reinit(cursor_t& cursor, const bool bIgnoreLatches = false); 
    static rc_t                        fetch(cursor_t& cursor,
					     const bool bIgnoreLatches = false);
    static rc_t                        is_empty(const lpid_t& root, bool& ret, const bool bIgnoreLatches = false);
    static rc_t                        purge(const lpid_t& root, bool check_empty, 
					     bool forward_processing, const bool bIgnoreLatches = false);
    static rc_t                 get_du_statistics(
        const lpid_t&                    root, 
        btree_stats_t&                btree_stats,
        bool                            audit);

    /* shared with btree_p: */
    static rc_t                        _scramble_key(
        cvec_t*&                    ret,
        const cvec_t&                    key, 
        int                             nkc,
        const key_type_s*            kc,
	bool                        use_dirbuf = false);

    static rc_t                        _unscramble_key(
        cvec_t*&                    ret,
        const cvec_t&                    key, 
        int                             nkc,
        const key_type_s*             kc,
	bool                        use_dirbuf = false);

    // pin: to debug (protected shoudl be moved up later
protected:
    /* 
     * for use by logrecs for undo, redo
     */
    static rc_t                        _insert(
        const lpid_t&                     root,
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                     key,
        const cvec_t&                     elem,
        int                             split_factor = 50);
    static rc_t                        _remove(
        const lpid_t&                    root,
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                     key,
        const cvec_t&                     elem);

    static const smlevel_0::store_flag_t        bulk_loaded_store_type;
};

/*<std-footer incl-file-exclusion='BTREE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
