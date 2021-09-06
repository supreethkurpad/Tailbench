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

/*<std-header orig-src='shore' incl-file-exclusion='BTREE_IMPL_H'>

 $Id: btree_impl.h,v 1.17 2010/06/08 22:28:55 nhall Exp $

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

#ifndef BTREE_IMPL_H
#define BTREE_IMPL_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#include <set>

#ifndef BTREE_H
#include "btree.h"
#endif
#ifndef KVL_T_H
#include "kvl_t.h"
#endif

typedef enum {  m_not_found_end_of_file, 
        m_satisfying_key_found_same_page,
        m_not_found_end_of_non_empty_page, 
        m_not_found_page_is_empty } m_page_search_cases;

class btsink_t;

class btree_impl : protected btree_m  {
    friend class btree_m;
    friend class btree_p;
    friend class btsink_t;

protected:

    static rc_t                        _split_tree(
        const lpid_t&                   root_old,
	const lpid_t&                   root_new,
        const cvec_t&                   key,
	lpid_t&                         leaf_old,
	lpid_t&                         leaf_new,
	const bool                      bIgnoreLatches);
    
    static rc_t                        _relocate_recs_l(
        lpid_t&                   leaf_old,
	const lpid_t&                   leaf_new,
	const bool                      was_root,
	const bool bIgnoreLatches = false,
	RELOCATE_RECORD_CALLBACK_FUNC relocate_callback = NULL);

    static rc_t                        _relocate_recs_p(
        const lpid_t&                   root_old,
        const lpid_t&                   root_new,
	const bool bIgnoreLatches = false,
	RELOCATE_RECORD_CALLBACK_FUNC relocate_callback = NULL);

    static rc_t                        _merge_trees(
        lpid_t&                         root,			      	    
        const lpid_t&                   root1,
	const lpid_t&                   root2,
	cvec_t&                         startKey2,	
	const bool                      update_owner,
	const bool                      bIgnoreLatches);
    
    static rc_t                        _mr_insert(
        const lpid_t&                     root,
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                     key,
	el_filler*                       ef,
	size_t el_size,
        int                             split_factor = 50,
	const bool bIgnoreLatches = false,
	RELOCATE_RECORD_CALLBACK_FUNC relocate_callback = NULL);

    static rc_t                 _split_leaf_and_relocate_recs(
        const lpid_t&                    root_pid,         // I - root of tree
        btree_p&                    leaf,         // I - page to be split
        const cvec_t&                    key,        // I-  which key causes split
        const cvec_t&                    el,                // I-  which element causes split
        int                             split_factor,
	const bool bIgnoreLatches,
	RELOCATE_RECORD_CALLBACK_FUNC relocate_callback = NULL);

    static rc_t                       _link_after_merge(
	const lpid_t& root,
	shpid_t p1,
	shpid_t p2,
	bool set_root1,
	const bool bIgnoreLatches);

    static rc_t                       _update_owner(
	const lpid_t& new_owner,
	const lpid_t& old_owner,
	const bool bIgnoreLatches);

    static rc_t _move_recs_l(
	const stid_t& fid,
	bool& first,
	const lpid_t& leaf,
	btree_p& leaf_page,
	file_mrbt_p& new_page,
	file_mrbt_p& old_page,
	vector<rid_t>& recs,
	map<rid_t, slotid_t>& slot_map,
	vector<rid_t>& old_rids,
	vector<rid_t>& new_rids,
	const bool bIgnoreLatches); 

    static rc_t _move_recs_p(
        const stid_t& fid,
	bool& first,
	const lpid_t& root,
	file_mrbt_p& new_page,
	file_mrbt_p& old_page,
	vector<rid_t>& recs,
	map<rid_t, slotid_t>& slot_map,
	map<rid_t, lpid_t>& leaf_map,
	vector<rid_t>& old_rids,
	vector<rid_t>& new_rids,
	const bool bIgnoreLatches); 


    static rc_t                        _alloc_page(
        const lpid_t&                    root,
        int2_t                           level,
        const lpid_t&                    near,
        btree_p&                         ret_page,
        shpid_t                          pid0 = 0,
        bool                             set_its_smo=false,
        bool                             compress=false,
        store_flag_t                     stf = st_regular,
	const bool                       bIgnoreLatches = false);

    static rc_t                        _insert(
        const lpid_t&                     root,
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                     key,
        const cvec_t&                     elem,
        int                             split_factor = 50,
	const bool bIgnoreLatches = false);
    static rc_t                        _remove(
        const lpid_t&                    root,
        bool                             unique,
        concurrency_t                    cc,
        const cvec_t&                     key,
        const cvec_t&                     elem,
	const bool bIgnoreLatches = false);

    static rc_t                 _lookup(
        const lpid_t&                     root,  // I-  root of btree
        bool                            unique,// I-  true if btree is unique
        concurrency_t                    cc,           // I-  concurrency control
        const cvec_t&                     key,   // I-  key we want to find
        const cvec_t&                     elem,  // I-  elem we want to find (may be null)
        bool&                             found, // O-  true if key is found
        bt_cursor_t*                    cursor,// I/o - put result here OR
        void*                             el,           // I/o-  buffer to put el if !cursor
        smsize_t&                     elen,   // IO- size of el if !cursor
	const bool bIgnoreLatches = false);

    static rc_t          _update(
        const lpid_t&       root,        // I-  root of btree
	bool                unique, // I-  true if btree is unique
	concurrency_t       cc,        // I-  concurrency control
	const cvec_t&       key,        // I-  key we want to find
	const cvec_t&       old_el,    // I-  element we want to update
	const cvec_t&       new_el,    // I-  new value of the element
	bool&               found,  // O-  true if key is found
	const bool bIgnoreLatches = false);                

    static rc_t                 _skip_one_slot(
        btree_p&                    p1, 
        btree_p&                    p2, 
        btree_p*&                    child, 
        slotid_t&                     slot, // I/O
        bool&                            eof,
        bool&                            found,
        bool                             backward=false,
	const bool bIgnoreLatches = false);

    static rc_t                 _propagate(
        const lpid_t&                     root_pid,         // I-  root page  -- fixed
        const cvec_t&                    key,        // I- key being inserted or deleted
        const cvec_t&                    elem,        // I- elem being inserted or deleted
        const lpid_t&                    _child_pid, // I-  pid of leaf page removed
                                        //  or of the page that was split
        int                            child_level, // I - level of child_pid
        bool                              isdelete,     // I-  true if delete being propagated
	const bool bIgnoreLatches = false);
    static void                         _skip_one_slot(
        btree_p&                     p1,
        btree_p&                     p2,
        btree_p*&                     child,
        slotid_t&                     slot,
        bool&                             eof
        );


    static void                 mk_kvl(
                                    concurrency_t cc, 
                                    lockid_t& kvl, 
                                    stid_t stid, 
                                    bool unique, 
                                    const cvec_t& key, 
                                    const cvec_t& el = cvec_t::neg_inf);

    static void                 mk_kvl(
                                    concurrency_t cc, 
                                    lockid_t& kvl, 
                                    stid_t stid, 
                                    bool unique, 
                                    const btrec_t& rec) {
                                        mk_kvl(cc, kvl, stid, 
                                                unique, rec.key(), rec.elem());
                                    }

    static void                 mk_kvl_eof(
                                    concurrency_t cc, 
                                    lockid_t& kvl, 
                                    stid_t stid) {
                                        // use both key and 
                                        // element to make it less likely
                                        // that it clashes with a user
                                    mk_kvl(cc, kvl, stid, 
                                    false, kvl_t::eof, kvl_t::eof); 
                                }
private:
    /* NB: these are candidates for a subordinate class that
     * exists only in the .c files btree_bl.cpp btree_impl.cpp:
     */

    static rc_t                        _search(
        const btree_p&                    page,
        const cvec_t&                    key,
        const cvec_t&                    elem,
        bool&                            found_key,
        bool&                            total_match,
        slotid_t&                    ret_slot);
    static rc_t                 _satisfy(
        const btree_p&                    page,
        const cvec_t&                    key,
        const cvec_t&                    elem,
        bool&                            found_key,
        bool&                            total_match,
        slotid_t&                    slot,
        uint&                             wcase);
    static rc_t                 _traverse(
        const lpid_t&                    __root,        // I-  root of tree 
        const lpid_t&                    _start,        // I-  root of search 
        const lsn_t&                     _start_lsn,// I-  old lsn of start 
        const cvec_t&                    key,        // I-  target key
        const cvec_t&                    elem,        // I-  target elem
        bool&                             found,        // O-  true if sep is found
        latch_mode_t                     mode,        // I-  EX for insert/remove, SH for lookup
        btree_p&                     leaf,        // O-  leaf satisfying search
        btree_p&                     parent,        // O-  parent of leaf satisfying search
        lsn_t&                             leaf_lsn,        // O-  lsn of leaf 
        lsn_t&                             parent_lsn,        // O-  lsn of parent 
	const bool bIgnoreLatches); 
    static rc_t                 _propagate_split(
        btree_p&                     parent,     // I - page to get the insertion
        const lpid_t&                    _pid,       // I - pid of child that was split
        slotid_t                      slot,        // I - slot where the split page sits
                                    //  which is < slot where the new entry goes
        bool&                        was_split,   // O - true if parent was split by this
	const bool bIgnoreLatches);
    static rc_t                 _split_leaf(
        const lpid_t&                    root_pid,         // I - root of tree
        btree_p&                    leaf,         // I - page to be split
        const cvec_t&                    key,        // I-  which key causes split
        const cvec_t&                    el,                // I-  which element causes split
        int                             split_factor,
	const bool bIgnoreLatches);

    static rc_t                 __split_page(
        btree_p&                    page,        // IO- page that needs to split
        lpid_t&                                sibling_page,// O-  new sibling
        bool&                               left_heavy,        // O-  true if insert should go to left
        slotid_t&                   slot,        // IO- slot of insertion after split
        int                            addition,        // I-  # bytes intended to insert
        int                            split_factor, // I-  % of left page that should remain
	const bool bIgnoreLatches);

    static rc_t                        _grow_tree(btree_p& root,
						  const bool bIgnoreLatches);
    static rc_t                        _shrink_tree(btree_p& root,
						    const bool bIgnoreLatches);
    

    static rc_t                 _handle_dup_keys(
        btsink_t*                    sink,
        slotid_t&                    slot,
        file_p*                            prevp,
        file_p*                            curp,
        int&                             count,
        record_t*&                     r,
        lpid_t&                            pid,
        int                            nkc,
            const key_type_s*            kc,
        bool                            lexify_keys,
        const bool bIgnoreLatches = false);

};

/************************************************************************** 
 *
 * Class tree_latch: helper class for btree_m.
 * It mimics a btree_p, but when it is fixed and unfixed,
 * it logs any necessaries for recovery purposes.
 *
 **************************************************************************/
class tree_latch {
    
private:
    lpid_t         _pid;
    latch_mode_t   _mode;
    latch_t&       _latch;
    bool           _fixed;


public:
    NORET tree_latch(const lpid_t pid, const bool bIgnoreLatches = false) ;

    NORET ~tree_latch(); 

    w_error_t::err_num_t   
          get_for_smo(bool           conditional,
                      latch_mode_t   mode,
                      btree_p&       p1,
                      latch_mode_t   p1mode,
                      bool           relatch_p2, //relatch, if it was latched
                      btree_p*       p2,
                      latch_mode_t   p2mode,
		      const bool     bIgnoreLatches = false);
    
    //btree_p&                page()        { return _page; }
    void                  unfix();
    // is_fixed() returns true if the latch is held BY ME. It does
    // not know about other thread's holding of these per-store latches
    bool                  is_fixed() const { check(); return _fixed; }
    bool                  pinned_by_me() const {check();  
                               return _latch.held_by_me(); }
    latch_mode_t          latch_mode() const { check(); return _latch.mode(); }
    const lpid_t&         pid() const { check(); return _pid; }
    void                  check() const  {
                            // NB: _fixed might not reflect the state of 
                            // the latch at first because another tree-latch
                            // could have it latched. It just reflects
                            // the state of this tree_latch structure.
                            // Once get_for_smo is called, check() should
                            // work because _fixed will be true.
                            // So we can make check() always work if we
                            // don't check the latch state in the !_fixed case.
                            W_IFDEBUG2(
								if(_fixed) w_assert2(_latch.held_by_me() != 0);)
                            }
};

#if W_DEBUG_LEVEL > 4

#define BTREE_CHECK_LATCHES
extern "C" {
    void bstop();
	// only in single-thread tests, senseless in mt-environment:
    void _check_latches(int line, uint _nsh, uint _nex, uint _max,
            const char *file);
}

/* 
 * call get_latches() at the beginning of the function, with
 * 2 variable names (undeclared).
 */
#        define get_latches(_nsh, _nex) \
        uint _nsh=0, _nex=0; { uint nd = 0;\
        smlevel_0::bf->snapshot_me(_nsh, _nex, nd); }

/* 
 * call check_latches() throughout the function, using the above
 *  two variable names +/- some constants, and the 3rd constant
 *  that indicates a maximum for both sh, ex latches
 * NB: ndiff is the # different pages latched (some might have 2 or 
 * more latches on them)
 */
#        define check_latches(_nsh, _nex, _max) \
        _check_latches(__LINE__, _nsh, _nex, _max, __FILE__);

#else
#        define check_latches(_nsh, _nex, _max)
#        define get_latches(_nsh, _nex) 
#endif 

/*<std-footer incl-file-exclusion='BTREE_IMPL_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
