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

 $Id: btree_impl.cpp,v 1.40 2010/06/15 17:30:07 nhall Exp $

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

#if W_DEBUG_LEVEL > 0
#define BTREE_LOG_COMMENT_ON 1
#endif

/*
 * Btree implementation is from Mohan, et. al.
 * References: 
 * IBM Research Report # RJ 7008 
 * 9/6/89
 * C. Mohan,
 * Aries/KVL: A Key-Value
 * Locking Method for Concurrency Control of Multiaction Transactions
 * Operating on B-Tree Indexes
 *
 * IBM Research Report # RJ 6846
 * 8/29/89
 * C. Mohan, Frank Levine
 * Aries/IM: An Efficient and High Concureency Index Management
 * Method using Write-Ahead Logging
 */

#include "sm_int_2.h"
#ifdef __GNUG__
#   pragma implementation "btree_impl.h"
#endif
#include "btree_p.h"
#include "btree_impl.h"
#include "btcursor.h"
#include <crash.h>
#include <store_latch_manager.h>
#include "btree_latch_manager.h"
// common/store_latch_manager.h defines or undefs the following:
extern store_latch_manager store_latches; // sm_io.cpp
extern btree_latch_manager btree_latches; // smindex.cpp

#if W_DEBUG_LEVEL > 3
/* change these at will */
#        define print_sanity false
#        define print_split false
#        define print_traverse false
#        define print_traverse false
#        define print_ptraverse false
#        define print_wholetree false
#        define print_remove false
#        define print_propagate false
#else
/* don't change these */
#        define print_sanity false
#        define print_wholetreee false
#        define print_traverse false
#        define print_ptraverse false
#        define print_remove false
#        define print_propagate false

#endif 


#ifdef BTREE_CHECK_LATCHES

void 
_check_latches(int line, uint _nsh, uint _nex, uint _max,
        const char * file)
{
    uint nsh, nex, ndiff; 
    bool dostop=false;
    smlevel_0::bf->snapshot_me(nsh, nex, ndiff);
// fprintf(stderr, "line %d: shared %d ex %d max %d  %s\n", 
        // line, _nsh, _nex, _max, file);
    if(ndiff != _max)
    {
        if((nsh >_nsh) || (nex > _nex) ||
            (nsh+nex>_max)) {
            cerr << line << " " << file << ": nsh have " << nsh 
            << "( expect " << _nsh << ")" 
            << " nex have " << nex 
            << "( expect " << _nex << ")" 
            << " # diff pages =" << ndiff 
            << endl; 
            dostop=true;
        } 
    }
    if(dostop) bstop(); 
}
void bstop() 
{
    // bf_m::dump();
}
void dumptree(const lpid_t &root)
{
    // no key interp
    btree_m::print(root, sortorder::kt_b, true);
}

#endif 


/*********************************************************************
 *
 *  btree_impl::mk_kvl(cc, lockid_t kvl, ...)
 *
 *  Make key-value or RID lock for the given <key, el> (or record), 
 *  and return it in kvl.
 *
 *  NOTE: if btree is non-unique, KVL lock is <key, el>
 *          if btree is unique, KVL lock is <key>.
 *
 *  NOTE: for t_cc_im, the el is interpreted as a RID and the RID is 
 *            locked, and unique/non-unique cases are treated the same.
 *
 *********************************************************************/

void 
btree_impl::mk_kvl(concurrency_t cc, lockid_t& kvl, 
    stid_t stid, bool unique, 
    const cvec_t& key, const cvec_t& el)
{
    if(cc > t_cc_none) {
    // shouldn't get here if cc == t_cc_none;
        if(cc == t_cc_im) {
            rid_t r;
            el.copy_to(&r, sizeof(r));
            kvl = r;
#if W_DEBUG_LEVEL > 4
            if(el.size() != sizeof(r)) {
                // This happens when we lock kvl_t::eof
                ;
            }
#endif 
        } else {
            w_assert9(cc == t_cc_kvl || cc == t_cc_modkvl);
            kvl_t k;
            if (unique) {
                k.set(stid, key);
            } else {
                w_assert9(&el != &cvec_t::neg_inf);
                k.set(stid, key, el);
            }
            kvl = k;
        }
    }

}



tree_latch::tree_latch(const lpid_t pid, const bool bIgnoreLatches) 
    : _pid(pid), _mode(LATCH_NL), 
    // Store latch manager keeps track of a single latch per store.
    // If there isn't already one for this store, we create a latch_t
    // for it.  This allows us to grab read/write/upgradable/downgradable
    // locks on a store.
      _latch(btree_latches.find_latch(pid, bIgnoreLatches)), _fixed(false)
{
    check(); 
    // w_assert2(_latch.held_by_me() == 0); might be wrong until the first 
    // get_for_smo is done
}

tree_latch::~tree_latch()
{
    unfix();
}


#if SM_PLP_TRACING
static __thread timeval my_time;
#endif

void                  
tree_latch::unfix() 
{
    check();
    // NOTE: in the original implementation, when we held a
    // btree_p (handle) here instead of a _latch, we just called
    // _page.unfix(), but that didn't try to unfix the page unless it
    // was thought to be fixed.  Now we need to check before unlatching.
    if(_fixed) {

#if SM_PLP_TRACING
        if (smlevel_0::_ptrace_level>=smlevel_0::PLP_TRACE_PAGE) {
            gettimeofday(&my_time, NULL);
            CRITICAL_SECTION(plpcs,smlevel_0::_ptrace_lock);
            smlevel_0::_ptrace_out << _pid << " " << pthread_self() << " " << _latch.mode() << " "
                                   << my_time.tv_sec << "." << my_time.tv_usec << endl;
            plpcs.exit();
        }
#endif

        _latch.latch_release();
        _fixed = false;
        /// RACY w_assert2(_latch.mode() == LATCH_NL );
        w_assert2(_latch.held_by_me() == 0);
    }
}

/* 
 * tree_latch::get_for_smo(bool condl, mode, p1, p1mode, bool, p2, p2mode)
 * 
 * Conditionally or unconditionally latch the _latch
 * (private attribute) in the given mode.
 *
 * Assumes that p1 is already latched, and p2 might be latched.
 *
 * If the conditional latch on "latch" fails, unlatch p1 (and p2)
 * and unconditionally latch "latch" in the given mode, re-latch
 * p1 in p1mode, (and p2 in p2mode IF the boolean argument so 
 * indicates).  P2 doesn't have to be valid -- this can get called
 * with (...,true, &p2, mode) where it's not known if p2 is a valid
 * (fixed) page_p.
 *
 * In the end the same # pages are fixed (unless it returns in 
 * error, in which case it's the caller's responsibility to unlatch
 * the pages) and are fixed in the given modes.
 * 
 * This is used to grab the tree latch for SMO-related activity.
 * When the caller is waiting for an ongoing SMO to finish (i.e., waiting for
 * a POSC), it calls this as follows
    insert:
        rc = tree_root.get_for_smo(true, LATCH_SH, 
                        leaf, LATCH_EX, false, &parent, LATCH_NL);
    remove:
        rc = tree_root.get_for_smo(true, LATCH_SH,
                        leaf, LATCH_EX, false, &parent, LATCH_NL);

        rc = tree_root.get_for_smo(true, LATCH_SH,
                        leaf, LATCH_EX, false, &sib, LATCH_NL);
     lookup:
        rc = tree_root.get_for_smo(false, LATCH_SH,
                       leaf, LATCH_SH, false, &parent, LATCH_NL);
     traverse:
        rc = tree_root.get_for_smo(false, LATCH_SH,
                       p[c], LATCH_SH, true, &p[1-c], LATCH_SH);
                        
 * When the caller wants to split a page (insert), it calls this thus:
        rc = tree_root.get_for_smo(true, LATCH_EX,
                        leaf, LATCH_EX, false, 0, LATCH_NL);

 * When the caller wants to delete a page (remove), it calls this thus:
        rc = tree_root.get_for_smo(true, LATCH_EX,
                       leaf, LATCH_EX, true, &sib, LATCH_EX);

 * When remove wants to deal with a boundary condition, it calls this thus:
        rc = tree_root.get_for_smo(true, tree_latch_mode,
                       leaf, LATCH_EX, sib.is_fixed(), &sib, sib.latch_mode());

 * When lookup doesn't find the key it's looking for, it calls this thus:
        rc = tree_root.get_for_smo(false, LATCH_SH,
                       leaf, LATCH_SH, false, &p2, LATCH_NL);
 *
 * NB: if caller already holds the latch _latch, this will try to upgrade
 * if it can, but if it can't will release the latch and re-acquire.
 * It notes the page lsns (but not the root page's lsn, btw, since it's
 * not really looking at the root page).
 *
 * TODO (performance): see if there's a way to cause the I/O layer to 
 * prefetch the page w/o latching *!* so that we can avoid doing I/O while
 * the tree is latched.
 */
w_error_t::err_num_t
tree_latch::get_for_smo(
    bool                conditional,
    latch_mode_t        mode,       // for our _latch
    btree_p&            p1,         // is latched
    latch_mode_t        p1mode,
    bool                relatch_p2, // do relatch it, if it was latched
    btree_p*            p2,         // might be latched
    latch_mode_t        p2mode,
    const bool bIgnoreLatches)        
{
    check();
    if(!bIgnoreLatches) {
	get_latches(___s,___e); // for checking that we
                                // exit with the same number of latches, +1
    }
    
    w_assert2(p1.is_fixed());

    lpid_t         p1_pid = p1.pid();
    lsn_t          p1_lsn = p1.lsn(); 

    lpid_t         p2_pid = lpid_t::null;
    lsn_t          p2_lsn = lsn_t::null;

    if(relatch_p2) {
        w_assert2(p2);
        if(p2->is_fixed()) { // could be invalid (unfixed) on entry
            p2_pid = p2->pid(); 
            p2_lsn = p2->lsn();
        }
    }


    bool do_refix=false;


    /*
     * Protect against multiple latches by same thread.
     * If we're going to do this, we CANNOT ENTER ANY
     * of btree functions (lookup, fetch, insert, delete) HOLDING 
     * A TREE LATCH.  No other modules should be doing that
     * anyway, but... !???
     */
    if(is_fixed()) {

        // Already latched.  Is it the mode we want?
        if(latch_mode() != mode) {
            // try upgrade - the first function assumes
            // upgrade to EX.  The other 2nd function
            // could skip the mode also.

            if(conditional) {
		if(!bIgnoreLatches) {
		    // Should only be upgrading to LATCH_EX
		    w_assert2(mode == LATCH_EX);
		}
                bool would_block = false;
                w_rc_t rc = _latch.upgrade_if_not_block(would_block);
                if(rc.is_error()) return rc.err_num();

                if(would_block) {
                    conditional = false; // try unconditional
                } else if(!bIgnoreLatches) {
                    w_assert2(latch_mode() == LATCH_EX );
                }
            }

            if(!conditional) {
                // try unconditional either because we were told not
                // to bother with the conditional latch, or because
                // it failed and now we have to wait.
                p1.unfix();
                if(p2) p2->unfix(); // unlatch p2 regardless of relatch_p2
                unfix();   // my private _latch.  
                // Freed all latches. Now do unconditional acquire in 
                // the desired mode.
                W_COERCE(_latch.latch_acquire(mode));
                _fixed=true;
                do_refix = true; // we unfixed p1 and p2
            }
        }
        // else ok, we're done
    } else {
        // Not already fixed.
        if(conditional) {
            // acquire our latch conditionally 
            // Note: since we're not in an upgrade case here,
            // this shouldn't return stINUSE, but will return stTIMEOUT
            // if it can't get the latch.
            w_rc_t rc = _latch.latch_acquire(mode, WAIT_IMMEDIATE);
            DBG(<<"rc=" << rc);
            if(rc.is_error() ) {
                if(rc.err_num() == smthread_t::stTIMEOUT) {
                    conditional = false; // try unconditional
                } else
                {
                    // what other error could be possibly get here?
                    w_assert1( !rc.is_error() );
                }
            } else {
                _fixed = true;
            }
        }

        if(!conditional) {
            // couldn't get the conditional latch
            // or didn't want to try
            p1.unfix();
            if(p2) p2->unfix(); // unlatch p2 regardless of relatch_p2

            w_rc_t rc = _latch.latch_acquire(mode);
            if(rc.is_error()) return rc.err_num();

            _fixed = true;
            do_refix = true; // we unfixed p1 and p2
        }
    } // not is_fixed() to begin with (latched)

    // Ok now we have our private latch in the desired mode.  
    w_assert2(is_fixed()); 
    w_assert2(latch_mode() == mode); 

    check();

    if(do_refix) {
        // we had to unfix p1,p2 above 
        w_rc_t rc = p1.fix(p1_pid, p1mode);
        if(rc.is_error()) return rc.err_num();

        bool page_has_changed= ( p1_lsn != p1.lsn() );

        if(!bIgnoreLatches && relatch_p2 && p2_pid.page) {
            w_assert2(p2mode > LATCH_NL);

            w_rc_t rc = p2->fix(p2_pid, p2mode);
            if(rc.is_error()) return rc.err_num();

            page_has_changed |= (p2_lsn != p2->lsn());
        }

        if(page_has_changed)  
        {
	    if(!bIgnoreLatches) {
		check_latches(___s+1,___e+1, ___s+___e+1);
	    }
            // Keep the tree latch and p1,p2 fixed, tell the
            // caller that the lsns have changed. Caller
            // will unfix
            return smlevel_0::eRETRY;
        }
    }

    w_assert2(is_fixed());
    w_assert9(p1.is_fixed());

    if(relatch_p2 && p2_pid.page) {
        w_assert2(p2->is_fixed());
    } else {
        if(p2) {
            p2->unfix();
            w_assert9(!p2->is_fixed());
        }
    }
    if(!bIgnoreLatches) {
	check_latches(___s+1,___e+1, ___s+___e+1);
    }
    // posc found if we were able to grab the latches
    INC_TSTAT(bt_posc);
    check();
    return smlevel_0::eOK;
}


/******************************************************************
 *
 *  btree_impl::_split_tree(root_old, root_new, key, leaf_old, leaf_new)
 *
 *  Mrbtree modification after adding a new partition
 *
 ******************************************************************/

rc_t
btree_impl::_split_tree(
    const lpid_t&       root_old,            // I-  root of the old btree
    const lpid_t&       root_new,           // I - root of the new btree
    const cvec_t&       key,                // I-  which key
    lpid_t&       leaf_old,               // O - the leaf whose contents shifted
    lpid_t&       leaf_new,               // O - the new leaf
    const bool bIgnoreLatches)
{

    FUNC(btree_impl::_split_tree);

    DBGTHRD(<<"_split_tree: " << " key=" << key );

    btree_p         root_page_new;
  
    latch_mode_t latch = LATCH_SH;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }

    cvec_t dummy_el; // not important since we're only interested in the key now
    bool found = false;
    bool found_elem = false;
    slotid_t ret_slot;
    vector<slotid_t> ret_slots;
    vector<lpid_t> pids;
    btree_p page;
    shpid_t prev_child = root_old.page;
    lpid_t pid;
    pid._stid = root_old._stid;
    bool is_leaf = false;
    shpid_t pid0;
	
    // find the key's first appearence on the tree
    while(!found && !is_leaf) {
	pid.page = prev_child;
	W_DO( page.fix(pid, latch) );
	W_DO( page.search(key, dummy_el, found, found_elem, ret_slot) );
	ret_slots.push_back(ret_slot);
	pids.push_back(pid);
	btrec_t rec(page, ret_slot-1);
	prev_child = rec.child();
	is_leaf = page.is_leaf();
	page.unfix();
    }

    latch = LATCH_EX;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }

    // to be used for updating prev/next pointers
    btree_p next_page;
    shpid_t next_page_id;
    lpid_t next_page_pid;
    next_page_pid._stid = root_old._stid;
	
    // special case for first one
    // pid0 is rec.child here while for the other's it's new_tree_page.pid
    W_DO( page.fix(pid, latch) );
    btrec_t rec(page, ret_slot);
    btree_p new_tree_page;
    pid0 = rec.child();
    if( pid != root_old ) {
	W_DO( _alloc_page(root_new, page.level(), root_new, new_tree_page,
			  rec.child(), false, page.is_compressed(),
                          st_regular, bIgnoreLatches) );
	if(ret_slot < page.nrecs()) {
	    W_DO( page.shift(ret_slot, new_tree_page) );
	}
	pid0 = new_tree_page.pid().page;
	// update prev/next pointers
	next_page_id = page.next();
	W_DO( page.link_up(page.prev(), 0) );
	W_DO( new_tree_page.link_up(0, next_page_id) );
	if(next_page_id != 0) {
	    next_page_pid.page = next_page_id;
	    W_DO( next_page.fix(next_page_pid, latch) );
	    W_DO( next_page.link_up(pid0, next_page.next()) );
	    next_page.unfix();
	}
	if(page.is_leaf()) {
	    // special case for leaf pages; set pid0 to 0
	    // if we don't do this it gets a dummy value
	    W_DO( new_tree_page.set_pid0(0) );
	    leaf_new = new_tree_page.pid();
	    leaf_old = pid;
	}
	// set the root of the new page to root_new
	W_DO( new_tree_page.set_root(root_new.page) );
	new_tree_page.unfix();
    }
    page.unfix();
	
    // move the records starting from the first location the key is found (a bottom-up process)
    for(uint i = ret_slots.size() - 2; i > 0; i--) {
	ret_slot = ret_slots[i];
	pid = pids[i];
	W_DO( page.fix(pid, latch) );
	btrec_t rec(page, ret_slot);
	btree_p new_tree_page;
	W_DO( _alloc_page(root_new, page.level(), root_new, new_tree_page,
			  pid0, false, page.is_compressed(),
                          st_regular, bIgnoreLatches));
	if(ret_slot < page.nrecs()) {
	    W_DO( page.shift(ret_slot, new_tree_page) );
	}
	pid0 = new_tree_page.pid().page;
	// update prev/next pointers
	next_page_id = page.next();
	W_DO( page.link_up(page.prev(), 0) );
	W_DO( new_tree_page.link_up(0, next_page_id) );
	if(next_page_id != 0) {
	    next_page_pid.page = next_page_id;
	    W_DO( next_page.fix(next_page_pid, latch) );
	    W_DO( next_page.link_up(pid0, next_page.next()) );
	    next_page.unfix();
	}
	page.unfix();
	// set the root of the new page to root_new
	W_DO( new_tree_page.set_root(root_new.page) );
	new_tree_page.unfix();
    }
    
    // special case : new tree root
    W_DO( root_page_new.fix(root_new, latch) );
    ret_slot = ret_slots[0];
    pid = pids[0];
    w_assert9( pid == root_old );
    W_DO( page.fix(pid, latch) );
    W_DO( root_page_new.set_hdr(root_new.page,
				page.level(), 
				pid0, 
				page.is_compressed()) );
    if(ret_slot < page.nrecs()) {
	W_DO( page.shift(ret_slot, root_page_new) );
    }
    if(page.is_leaf()) {
	// special case for leaf pages; set pid0 to 0
	// if we don't do this it gets a dummy value
	W_DO( root_page_new.set_pid0(0) );
	leaf_new = root_new;
	leaf_old = root_old;
    }
    page.unfix();
    root_page_new.unfix();	    
    
    return RCOK;
}

/******************************************************************
 *
 *  btree_impl::_relocate_recs_l(leaf_old, leaf_new, was_root)
 *
 *  Places that this function is called:  
 *  1) Called after _split_tree during add_partition for the design
 *  where a heap page is pointed by only one leaf page
 *  to maintain this property.
 *  2) Called from _split_leaf_and_reloce_recs
 *  When a leaf split happens the record's whose assocs are moved to
 *  the new heap page has to be moved to another heap page to
 *  satisfy the constraint of a heap page is pointed by just one leaf..
 *  If the _split_leaf happened for a root page then leaf_split
 *  is also a new page so the owner of the old heap pages should be
 *  updated. (was_root is to handle this)
 *
 ******************************************************************/

rc_t
btree_impl::_relocate_recs_l(
    lpid_t&       leaf_old,  // I - leaf page whose contents are shifted
    const lpid_t&       leaf_new,  // I - the new leaf page
    const bool was_root,  // I - indicates whether there was a root page split
    const bool bIgnoreLatches,
    RELOCATE_RECORD_CALLBACK_FUNC relocate_callback)
{
    FUNC(btree_impl::_relocate_recs_l);

    DBGTHRD(<<"_relocate_recs_l: " << " leaf_old = " << leaf_old
	    <<" leaf_new = " << leaf_new);

    vector<rid_t> old_rids;
    vector<rid_t> new_rids;

    latch_mode_t latch = LATCH_EX;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }
    btree_p leaf_page_old;
    btree_p leaf_page_new;
    W_DO( leaf_page_old.fix(leaf_old, latch) );
    W_DO( leaf_page_new.fix(leaf_new, latch) );

    if(was_root) {
	leaf_old.page = leaf_page_old.pid0();
	leaf_page_old.unfix();
	W_DO( leaf_page_old.fix(leaf_old, latch) );
    }

    // if some of the heap pages the new leaf page points to are not pointed by any other leaf page already
    // we don't have to split that heap page, we just have to set the owner for that heap page

    // if in an heap page, the number of records pointed by the new leaf page is more than the ones pointed
    // by the old leaf page, then the records that are pointed by the old leaf page are relocated

    // 1. collect info on the heap_pages
    // 1.1 from the new page
    map<lpid_t, vector<rid_t> > recs_map_new;
    map<rid_t, slotid_t> slot_map_new;
    for(int i=0; i < leaf_page_new.nrecs(); i++) {
	btrec_t rec(leaf_page_new, i);
	rid_t rid;
	rec.elem().copy_to(&rid, sizeof(rid_t));
	recs_map_new[rid.pid].push_back(rid);
	slot_map_new[rid] = i;
    }
    // 1.2 from the old page
    map<lpid_t, vector<rid_t> > recs_map_old;
    map<rid_t, slotid_t> slot_map_old;
    for(int i=0; i < leaf_page_old.nrecs(); i++) {
	btrec_t rec(leaf_page_old, i);
	rid_t rid;
	rec.elem().copy_to(&rid, sizeof(rid_t));
	recs_map_old[rid.pid].push_back(rid);
	slot_map_old[rid] = i;
    }

    // 2. determine the heap pages that needs to be split
    file_mrbt_p heap_page;
    file_mrbt_p new_page_old;
    file_mrbt_p new_page_new;
    bool first_old = true;
    bool first_new = true;
    for(map<lpid_t, vector<rid_t> >::iterator iter = recs_map_new.begin(); iter != recs_map_new.end(); iter++) {
	W_DO( heap_page.fix(iter->first, latch) );
	if( recs_map_old[iter->first].size() > (iter->second).size() ) {
	    // have to move some records that are pointed by the new leaf page
	    W_DO( _move_recs_l((iter->first)._stid,
			       first_new,
			       leaf_new,
			       leaf_page_new,
			       new_page_new,
			       heap_page,
			       iter->second,
			       slot_map_new,
			       old_rids,
			       new_rids,
			       bIgnoreLatches) ); 
	    if(was_root) {
		W_DO( heap_page.set_owner(leaf_old) );
	    }
	} else if(recs_map_old[iter->first].size() > 0) {
	    // have to move some records that are pointed by the old leaf page
	    W_DO( _move_recs_l((iter->first)._stid,
			       first_old,
			       leaf_old,
			       leaf_page_old,
			       new_page_old,
			       heap_page,
			       recs_map_old[iter->first],
			       slot_map_old,
			       old_rids,
			       new_rids,
			       bIgnoreLatches) ); 
	    W_DO( heap_page.set_owner(leaf_new) );
	} else { // we don't have to move recs from this heap page but update its hdr
	    W_DO( heap_page.set_owner(leaf_new) );
	}
	heap_page.unfix();
    }

    // 3. unfix pages
    if(new_page_old.is_fixed()) {
	new_page_old.unfix();
    }
    if(new_page_new.is_fixed()) {
	new_page_new.unfix();
    }
    leaf_page_old.unfix();
    leaf_page_new.unfix();

    // 4. callback to update the secondary indexes
    if(old_rids.size() > 0) {
	W_DO( (*relocate_callback)(old_rids, new_rids) );
    }
    
    return RCOK;
}

/******************************************************************
 *
 *  btree_impl::_move_recs_l(...)
 *
 *  Helper function for _relocate_recs_l. Should only be called from this function.
 *  Simply moves the records in the given "recs" list to another heap page and
 *  performs the necessary updates due to changing rids.
 *
 ******************************************************************/
rc_t
btree_impl::_move_recs_l(
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
    const bool bIgnoreLatches) 
{
    lpid_t new_page_id;
    bool space_found = true;
    if(first) { // create the new heap page for the first record move	   
	W_DO( file_m::_alloc_mrbt_page(fid,
				       lpid_t::eof,
				       new_page_id,
				       new_page,
				       true) );
	W_DO( new_page.set_owner(leaf) );
	first = false;
    }
    // move the recs pointed by the leaf page 
    for(uint i=0; i < recs.size(); i++) {	
	rid_t rid = recs[i];
	// get the rec from the heap_page
	record_t* rec;
	W_DO( old_page.get_rec(rid.slot, rec) );
	// move it to new page
	vec_t hdr_vec;
	vec_t data_vec;
	char* hdr = (char*) malloc((*rec).hdr_size());
	memcpy(hdr, (*rec).hdr(), (*rec).hdr_size());
	char* data = (char*) malloc((*rec).body_size());
	memcpy(data, (*rec).body(), (*rec).body_size());
	rid_t new_rid;
	hdr_vec.put(hdr, (*rec).hdr_size());
	data_vec.put(data, (*rec).body_size());
	W_DO( file_m::move_mrbt_rec_to_given_page(0,
						  hdr_vec,
						  data_vec,
						  new_rid,
						  new_page,
						  space_found,
						  bIgnoreLatches) );
	if(!space_found) { // we have to create a new heap_page
	    W_DO( file_m::_alloc_mrbt_page(fid,
					   lpid_t::eof,
					   new_page_id,
					   new_page,
					   true) );
	    W_DO( new_page.set_owner(leaf) );
	    // retry the insert
	    W_DO( file_m::move_mrbt_rec_to_given_page(0,
						      hdr_vec,
						      data_vec,
						      new_rid,
						      new_page,
						      space_found,
						      bIgnoreLatches) );	    
	}
	// add old&new rid to the list
	old_rids.push_back(rid);
	new_rids.push_back(new_rid);
	// delete the record from its prev page
	W_DO( file_m::destroy_rec_slot(rid, old_page, bIgnoreLatches) );
	// update rid in leaf_page
	btrec_t btree_rec(leaf_page, slot_map[rid]);
	cvec_t elem;
	elem.put((char*)(&new_rid), sizeof(rid_t));
	W_DO( leaf_page.overwrite(slot_map[rid]+1, btree_rec.klen()+sizeof(int4_t), elem) );
    }
    return RCOK;
}

/******************************************************************
 *
 *  btree_impl::_relocate_recs_p(root_old, root_new)
 *
 *  Called after _split_tree during add_partition for the design
 *  where a heap page is pointed by only one partition's sub-tree
 *  to maintain this property.
 *  
 ******************************************************************/

rc_t
btree_impl::_relocate_recs_p(
    const lpid_t&       root_old,  // I - root of the old sub-tree
    const lpid_t&       root_new,  // I - root of the new sub-tree
    const bool bIgnoreLatches,
    RELOCATE_RECORD_CALLBACK_FUNC relocate_callback)
{
    
    FUNC(btree_impl::_relocate_recs_p);

    DBGTHRD(<<"_relocate_recs_p: " << " root_old = " << root_old
	    << " root_new = " << root_new);

    vector<rid_t> old_rids;
    vector<rid_t> new_rids;

    latch_mode_t latch = LATCH_SH;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }

    // if some of the heap pages the new sub-tree points to are not pointed by any other sub-tree already
    // we don't have to split that heap page, we just have to set the owner for that heap page

    // if in an heap page, the number of records pointed by the new sub-tree is more than the ones pointed
    // by the old sub-tree, then the records that are pointed by the old sub-tree page are relocated

    // 0. find the left-most leaf page in old&new sub-tree
    lpid_t pid_old(root_old._stid, root_old.page);
    btree_p page_old;
    W_DO( page_old.fix(pid_old, latch) );
    while(page_old.level() > 1) {
	pid_old.page = page_old.pid0();
	page_old.unfix();
	W_DO( page_old.fix(pid_old, latch) );
    }
    lpid_t pid_new(root_new._stid, root_new.page);
    btree_p page_new;
    W_DO( page_new.fix(pid_new, latch) );
    while(page_new.level() > 1) {
	pid_new.page = page_new.pid0();
	page_new.unfix();
	W_DO( page_new.fix(pid_new, latch) );
    }

    // 1. collect info on the heap_pages
    // 1.1 for old sub-tree
    map<lpid_t, vector<rid_t> > recs_map_old;
    map<rid_t, slotid_t> slot_map_old;
    map<rid_t, lpid_t> leaf_page_map_old;
    int i = 0;
    while(true) {
	if(i >= page_old.nrecs()) {
	    pid_old.page = page_old.next();
	    page_old.unfix();
	    if(pid_old.page != 0) {
		i = 0;
		W_DO( page_old.fix(pid_old, latch) );
	    } else {
		break;
	    }
	}
	btrec_t rec(page_old, i);
	rid_t rid;
	rec.elem().copy_to(&rid, sizeof(rid_t));
	recs_map_old[rid.pid].push_back(rid);
	slot_map_old[rid] = i;
	leaf_page_map_old[rid] = pid_old;
	i++;
    }
    // 1.2 for new sub-tree
    map<lpid_t, vector<rid_t> > recs_map_new;
    map<rid_t, slotid_t> slot_map_new;
    map<rid_t, lpid_t> leaf_page_map_new;
    i = 0;
    while(true) {
	if(i >= page_new.nrecs()) {
	    pid_new.page = page_new.next();
	    page_new.unfix();
	    if(pid_new.page != 0) {
		i = 0;
		W_DO( page_new.fix(pid_new, latch) );
	    } else {
		break;
	    }
	}
	btrec_t rec(page_new, i);
	rid_t rid;
	rec.elem().copy_to(&rid, sizeof(rid_t));
	recs_map_new[rid.pid].push_back(rid);
	slot_map_new[rid] = i;
	leaf_page_map_new[rid] = pid_new;
	i++;
    }

    // 2. determine the heap pages that needs to be split
    file_mrbt_p heap_page;
    file_mrbt_p new_page_old;
    file_mrbt_p new_page_new;
    bool first_old = true;
    bool first_new = true;
    latch = LATCH_EX;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }
    for(map<lpid_t, vector<rid_t> >::iterator iter = recs_map_new.begin(); iter != recs_map_new.end(); iter++) {
	W_DO( heap_page.fix(iter->first, latch) );
	if( recs_map_old[iter->first].size() > (iter->second).size() ) {
	    // have to move some records from the heap_page that are pointed by the new sub-tree
	    W_DO( _move_recs_p((iter->first)._stid,
			       first_new,
			       root_new,
			       new_page_new,
			       heap_page,
			       iter->second,
			       slot_map_new,
			       leaf_page_map_new,
			       old_rids,
			       new_rids,
			       bIgnoreLatches) ); 
	} else if(recs_map_old[iter->first].size() > 0) {
	    // have to move some records that are pointed by the old sub-tree
	    W_DO( _move_recs_p((iter->first)._stid,
			       first_old,
			       root_old,
			       new_page_old,
			       heap_page,
			       recs_map_old[iter->first],
			       slot_map_old,
			       leaf_page_map_old,
			       old_rids,
			       new_rids,
			       bIgnoreLatches) ); 
	    W_DO( heap_page.set_owner(root_new) );
	} else { // we don't have to move recs from this heap page but update its hdr
	    W_DO( heap_page.set_owner(root_new) );
	}
	heap_page.unfix();
    }

    // 3. unfix pages
    if(new_page_old.is_fixed()) {
	new_page_old.unfix();
    }
    if(new_page_new.is_fixed()) {
	new_page_new.unfix();
    }

    // 4. callback to update the secondary indexes
    if(old_rids.size() > 0) {
	W_DO( (*relocate_callback)(old_rids, new_rids) );
    }
    
    return RCOK;
}

/******************************************************************
 *
 *  btree_impl::_move_recs_p(...)
 *
 *  Helper function for _relocate_recs_p. Should only be called from this function.
 *  Simply moves the records in the given "recs" list to another heap page and
 *  performs the necessary updates due to changing rids.
 *
 ******************************************************************/  
rc_t
btree_impl::_move_recs_p(
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
    const bool bIgnoreLatches) 
{
    lpid_t new_page_id;
    bool space_found = true;
    btree_p leaf_page;
    latch_mode_t latch = LATCH_EX;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }
    if(first) { // create the new heap page for the first record move
	W_DO( file_m::_alloc_mrbt_page(fid,
				       lpid_t::eof,
				       new_page_id,
				       new_page,
				       true) );
	W_DO( new_page.set_owner(root) );
	first = false;
    }
    // move the recs pointed by the sub-tree 
    for(uint i=0; i < recs.size(); i++) {
	rid_t rid = recs[i];
	// get the rec from the heap_page
	record_t* rec;
	W_DO( old_page.get_rec(rid.slot, rec) );
	// move it to new page
	vec_t hdr_vec;
	vec_t data_vec;
	char* hdr = (char*) malloc((*rec).hdr_size());
	memcpy(hdr, (*rec).hdr(), (*rec).hdr_size());
	char* data = (char*) malloc((*rec).body_size());
	memcpy(data, (*rec).body(), (*rec).body_size());
	rid_t new_rid;
	hdr_vec.put(hdr, (*rec).hdr_size());
	data_vec.put(data, (*rec).body_size());
	W_DO( file_m::move_mrbt_rec_to_given_page(0,
						  hdr_vec,
						  data_vec,
						  new_rid,
						  new_page,
						  space_found,
						  bIgnoreLatches) );
	if(!space_found) { // we have to create a new heap_page
	    W_DO( file_m::_alloc_mrbt_page(fid,
					   lpid_t::eof,
					   new_page_id,
					   new_page,
					   true) );
	    W_DO( new_page.set_owner(root) );
	    // retry the insert
	    W_DO( file_m::move_mrbt_rec_to_given_page(0,
						      hdr_vec,
						      data_vec,
						      new_rid,
						      new_page,
						      space_found,
						      bIgnoreLatches) );
	}
	// add old&new rid to the list
	old_rids.push_back(rid);
	new_rids.push_back(new_rid);
	// delete the record from its prev page
	W_DO( file_m::destroy_rec_slot(rid, old_page, bIgnoreLatches) );
	// update rid in leaf_page
	W_DO( leaf_page.fix(leaf_map[rid], latch) );
	btrec_t btree_rec(leaf_page, slot_map[rid]);
	cvec_t elem;
	elem.put((char*)(&new_rid), sizeof(rid_t));
	W_DO( leaf_page.overwrite(slot_map[rid]+1, btree_rec.klen()+sizeof(int4_t), elem) );
	leaf_page.unfix();
    }
    return RCOK;
}

/*********************************************************************
 * 
 *  btree_impl::_split_leaf_and_relocate_recs(root, page, key, el, split_factor)
 *
 *  Does the same things with _split_leaf but in the end has a call
 *  to _relocate_recs_l. For the MRBT design where an heap page is 
 *  only pointed by one leaf page.
 *
 *********************************************************************/
rc_t
btree_impl::_split_leaf_and_relocate_recs(
    lpid_t const &        root_pid,         // I - root of tree
    btree_p&              leaf,         // I - page to be split
    const cvec_t&         key,        // I-  which key causes split
    const cvec_t&         el,        // I-  which element causes split
    int                   split_factor,
    const bool bIgnoreLatches,
    RELOCATE_RECORD_CALLBACK_FUNC relocate_callback)        
{
    w_assert9(leaf.is_fixed());
    w_assert9(!bIgnoreLatches || leaf.latch_mode() == LATCH_EX);
	
    lsn_t         anchor;         // serves as savepoint too
    xct_t*         xd = xct();

    check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
    if (xd)  anchor = xd->anchor();

#if BTREE_LOG_COMMENT_ON
    W_DO(log_comment("start leaf split"));
#endif 

    int            addition = key.size() + el.size() + 2;
    lpid_t         rsib_pid;
    lpid_t         leaf_pid = leaf.pid();;
    int            level = leaf.level();
    {
        bool         left_heavy;
        slotid_t     slot=1; // use 1 to leave at least one
                             // record in the left page.
        w_assert9(leaf.nrecs()>0);

        X_DO( __split_page(leaf, rsib_pid,  left_heavy,
			   slot, addition, split_factor, bIgnoreLatches), anchor );
        leaf.unfix();
    }

    X_DO(_propagate(root_pid, key, el, leaf_pid, level, false /*not delete*/, bIgnoreLatches), anchor);

    
#if BTREE_LOG_COMMENT_ON
    W_DO(log_comment("end leaf split"));
#endif 
    if (xd)  {
        SSMTEST("btree.propagate.s.1");
        xd->compensate(anchor,false/*not undoable*/ LOG_COMMENT_USE("btree.prop.1"));
    }

    if(relocate_callback != NULL) {
	W_DO( _relocate_recs_l(leaf_pid, rsib_pid, root_pid == leaf_pid, bIgnoreLatches, relocate_callback) );
    }

    return RCOK;
}

/******************************************************************
 *
 *  btree_impl::_link_after_merge()
 *
 *  After merge of btrees we have to set the prev/next values of
 *  pages properly. We also have to update the root of the pages in
 *  the merged tree. This function is to be called from _merge_trees
 *  for the above purposes.
 *
 ******************************************************************/

rc_t btree_impl::_link_after_merge(const lpid_t& root,
				   shpid_t p1, shpid_t p2,
				   bool set_root1,
				   const bool bIgnoreLatches) 
{
    FUNC(btree_impl::_link_after_merge);

    DBGTHRD(<<"_link_after_merge: " << " root = " << root
	    <<" p1: " << p1 << " p2: " << p2);

    latch_mode_t latch = LATCH_EX;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }

    stid_t stid = root._stid;
    lpid_t current_p1(stid, p1);
    lpid_t current_p2(stid, p2);
    btree_p page1;
    btree_p page2;

    W_DO( page1.fix(current_p1, latch) );
    W_DO( page2.fix(current_p2, latch) );
    
    w_assert0( page1.level() == page2.level() );

    // update prev/next values
    W_DO( page1.link_up(page1.prev(), p2) );
    W_DO( page2.link_up(p1, page2.next()) );
    
    // update root value
    if(set_root1) {
	W_DO( page1.set_root(root.page) );
    } else {
	W_DO( page2.set_root(root.page) );
    }

    // go update next level
    if(!page1.is_leaf()) {
	shpid_t new_p1 = page1.child(page1.nrecs()-1);
	shpid_t new_p2 = page2.pid0();
	
	page1.unfix();
	page2.unfix();

	W_DO( _link_after_merge(root, new_p1, new_p2, set_root1, bIgnoreLatches) );
    } else {
	page1.unfix();
	page2.unfix();
    }

    return RCOK;
}

/******************************************************************
 *
 *  btree_impl::_update_owner()
 *
 *  After merge of btrees we have to set the new owner for the design
 *  where heap pages are pointed by only one sub-tree.
 *  Called from _merge_trees.
 *
 ******************************************************************/

rc_t btree_impl::_update_owner(const lpid_t& new_owner,
			       const lpid_t& old_owner,
			       const bool bIgnoreLatches) 
{
    FUNC(btree_impl::_update_owner);

    DBGTHRD(<<"_update_owner: " << " new_owner = " << new_owner <<" old_owner: " << old_owner);

    latch_mode_t leaf_latch = LATCH_SH;
    latch_mode_t heap_latch = LATCH_EX;
    if(bIgnoreLatches) {
	leaf_latch = LATCH_NL;
	heap_latch = LATCH_NL;
    }

    // 0. find the left-most leaf page in this sub-tree
    lpid_t pid(old_owner._stid, old_owner.page);
    btree_p page;
    W_DO( page.fix(pid, leaf_latch) );
    while(page.level() > 1) {
	pid.page = page.pid0();
	page.unfix();
	W_DO( page.fix(pid, leaf_latch) );
    }

    // 1. update the owners of the heap_pages
    int i = 0;
    file_mrbt_p heap_page;
    set<lpid_t> pages;
    while(true) {
	if(i >= page.nrecs()) {
	    pid.page = page.next();
	    page.unfix();
	    if(pid.page != 0) {
		i = 0;
		W_DO( page.fix(pid, leaf_latch) );
	    } else {
		break;
	    }
	}
	btrec_t rec(page, i);
	rid_t rid;
	rec.elem().copy_to(&rid, sizeof(rid_t));
	if(pages.find(rid.pid) == pages.end()) {
	    W_DO( heap_page.fix(rid.pid, heap_latch) );
	    W_DO( heap_page.set_owner(new_owner) );
	    heap_page.unfix();
	    pages.insert(rid.pid);
	}
	i++;
    }
    
    return RCOK;
}

/******************************************************************
 *
 *  btree_impl::_merge_trees(root, root1, root2, start_key1)
 *
 *  Mrbtree modification after deleting a new partition
 *
 ******************************************************************/

rc_t
btree_impl::_merge_trees(
    lpid_t&             root,         // O- the root after merge
    const lpid_t&       root1,        // I- roots of the btrees to be merged
    const lpid_t&       root2,           
    cvec_t&             start_key2,    // I- starting boundary key for the second sub-tree (root2) 
    const bool          update_owner,
    const bool bIgnoreLatches)    
{
    FUNC(btree_impl::_merge_trees);

    DBGTHRD(<<"_merge_trees: root1 = " << root1 << " root2 = " << root2);

    latch_mode_t latch = LATCH_EX;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }

    btree_p         root_page_1;
    btree_p         root_page_2;
    W_DO( root_page_1.fix(root1, latch) );
    W_DO( root_page_2.fix(root2, latch) );
    int level_1 = root_page_1.level();
    int level_2 = root_page_2.level();
    root_page_1.unfix();
    root_page_2.unfix();

    cvec_t elem_to_insert; // dummy
    if ( level_1 < level_2 ) { // root2 has a higher level than root1
	                       // put root1 into appropriate slot in btree with root2
	if(update_owner) {
	    W_DO( _update_owner(root2, root1, bIgnoreLatches) );
	}
	root = root2;
	cvec_t elem_to_insert; // dummy
	// find the page to insert the other tree
	btrec_t rec;
	lpid_t pid(root2._stid, root2.page);
	btree_p page_to_insert;
	W_DO( page_to_insert.fix(pid, latch) );
	while(page_to_insert.level() > level_1+1) {
	    page_to_insert.unfix();
	    rec.set(page_to_insert, 0);
	    pid.page = rec.child();
	    W_DO( page_to_insert.fix(pid, latch) );
	}
	W_DO( page_to_insert.insert( start_key2, elem_to_insert, 0, page_to_insert.pid0()) );
	W_DO( page_to_insert.set_pid0( root1.page ) );
	W_DO( _link_after_merge(root, root1.page, page_to_insert.child(0), true, bIgnoreLatches) );
	page_to_insert.unfix();

	btree_latches.destroy_latches(root1);
    }
    else if ( level_2 < level_1 ) { // root1 has a higher level than root2
	                            // put root2 into appropriate slot in btree with root1
	if(update_owner) {
	    W_DO( _update_owner(root1, root2, bIgnoreLatches) );
	}
	root = root1;
	// find the page to insert the other tree
	btrec_t rec;
	lpid_t pid(root1._stid, root1.page);
	btree_p page_to_insert;
	W_DO( page_to_insert.fix(pid, latch) );
	while(page_to_insert.level() > level_2+1) {
	    page_to_insert.unfix();
	    rec.set(page_to_insert, page_to_insert.nrecs() - 1);
	    pid.page = rec.child();
	    W_DO( page_to_insert.fix(pid, latch) );
	}
	W_DO( page_to_insert.insert( start_key2, elem_to_insert, page_to_insert.nrecs(), root2.page) );
	W_DO( _link_after_merge(root, page_to_insert.child(page_to_insert.nrecs()-2),
				root2.page, false, bIgnoreLatches) );
	page_to_insert.unfix();

	btree_latches.destroy_latches(root2);
    }
    else { // both btrees have the same height
	   // append root2 entries to root1
	   // TODO: if root1 doesn't have enough space, don't let merge
	if(update_owner) {
	    W_DO( _update_owner(root1, root2, bIgnoreLatches) );
	}
	btree_p         root_page_1;
	btree_p         root_page_2;
	W_DO( root_page_1.fix(root1, latch) );
	W_DO( root_page_2.fix(root2, latch) );
	int nrecs = root_page_1.nrecs();
	W_DO( root_page_1.insert( start_key2, elem_to_insert,
				  root_page_1.nrecs(), root_page_2.pid0()) );
 	W_DO( root_page_2.shift(0, root_page_1.nrecs(), root_page_1) );
	root = root1;
	W_DO( _link_after_merge(root, root_page_1.child(nrecs-1), root_page_1.child(nrecs),
				false, bIgnoreLatches) );
	root_page_1.unfix();
	root_page_2.unfix();

	// free root2
	W_DO( io->free_page(root2, false/*checkstore*/) );
	
	btree_latches.destroy_latches(root2);

	INC_TSTAT(page_btree_dealloc);
    }
    
    return RCOK;
}

/******************************************************************
 *
 * btree_impl::_mr_insert(root, unique, cc, key, fill_el, el_size, split_factor)
 *
 * For the 2nd and 3rd MRBT designs.
 *
 * Called before the actual record insert to see which leaf page the
 * record's association will be placed to put the actual record in
 * one of the heap pages already pointed by this leaf page.
 * The fill_el call back function serves the purpose of informing the 
 * higher levels about the records position in the btree.
 * 
 * The rest is same as the _insert function.
 *
 ******************************************************************/

rc_t
btree_impl::_mr_insert(
    const lpid_t&        root,                // I-  root of btree
    bool                unique,                // I-  true if tree is unique
    concurrency_t        cc,                // I-  concurrency control 
    const cvec_t&        key,                // I-  which key
    //rc_t (*fill_el)(vec_t&, const lpid_t&),  // I-  callback function to determine the element
    el_filler*                       ef,
    size_t el_size,                          // I - size of the element
    int                 split_factor,        // I-  tune split in %
    const bool bIgnoreLatches,
    RELOCATE_RECORD_CALLBACK_FUNC relocate_callback)
{
    FUNC(btree_impl::_mr_insert);
    lpid_t                  search_start_pid = root;
    lsn_t                  search_start_lsn = lsn_t::null;
    stid_t                 stid = root.stid();

    void* tmp_el = malloc(el_size);
    vec_t el(tmp_el, el_size);
    vec_t el2;
    
    INC_TSTAT(bt_insert_cnt);

    DBGTHRD(<<"_insert: unique = " << unique << " cc=" << int(cc)
        << " key=" << key );

    if(!bIgnoreLatches) {
	get_latches(___s,___e);
    }
    
    {
        tree_latch         tree_root(root, bIgnoreLatches);

	// latch modes
	latch_mode_t traverse_latch;
	latch_mode_t smo_mode;
	latch_mode_t smo_p1mode;
	latch_mode_t fix_latch;
again:
    {
        btree_p         leaf;
        lpid_t          leaf_pid;
        lsn_t           leaf_lsn;
        btree_p         parent;
        lsn_t           parent_lsn;
        lpid_t          parent_pid;
        bool            found = false;
        bool            total_match = false;
        rc_t            rc;


        DBGTHRD(<<"_insert.again:");

        w_assert9( !leaf.is_fixed());
        w_assert9( !parent.is_fixed());

	if(!bIgnoreLatches) {
	    if(tree_root.is_fixed()) {
		check_latches(___s+1,___e+1, ___s+___e+1);
	    } else {
		check_latches(___s,___e, ___s+___e);
	    }
	}

        /*
         *  Walk down the tree.  Traverse doesn't
         *  search the leaf page; it's our responsibility
         *  to check that we're at the correct leaf.
         */
	traverse_latch = LATCH_EX;
	if(bIgnoreLatches) {
	    traverse_latch = LATCH_NL;
	}
	
        W_DO( _traverse(root, search_start_pid,
            search_start_lsn,
            key, el, found, 
            traverse_latch, leaf, parent,
			leaf_lsn, parent_lsn, bIgnoreLatches) );

	if(!bIgnoreLatches) {
	    if(leaf.pid() == root) {
		check_latches(___s,___e+2, ___s+___e+2);
	    } else {
		check_latches(___s+1,___e+1, ___s+___e+2);
	    }
	}

        w_assert9( leaf.is_fixed());
        w_assert9( leaf.is_leaf());

        w_assert9( parent.is_fixed());
        w_assert9( parent.is_node() || (parent.is_leaf() &&
                    leaf.pid() == root  ));
        w_assert9( parent.is_leaf_parent() || parent.is_leaf());

        /*
         * Deal with SMOs :  traversal checked the nodes, but
         * not the leaf.  We check the leaf for smo, delete bits.
         * (Delete only checks for smo bits.)
         */

        leaf_pid = leaf.pid();
        parent_pid = parent.pid(); 

        if(leaf.is_smo() || leaf.is_delete()) {

            /* 
             * SH-latch the tree for manual duration -- paragraph 1
             * of 3.3 Insert of KVL paper.  It gets unlatched
             * below, not far.
             */
            w_assert2(parent.is_fixed());
	    w_assert2(leaf.is_fixed());
	    if(!bIgnoreLatches) {
		w_assert2(parent.latch_mode()>=LATCH_SH); // could be EX
		w_assert2(leaf.latch_mode() == LATCH_EX);
	    }
	    /* conditional=true, try to get tree latch in SH mode,
             */
	    smo_mode = LATCH_SH;
	    smo_p1mode = LATCH_EX;
	    if(bIgnoreLatches) {
		smo_mode = LATCH_NL;
		smo_p1mode = LATCH_NL;
	    }
	    w_error_t::err_num_t rce = tree_root.get_for_smo(true, smo_mode, 
							     leaf, smo_p1mode, false, &parent,
							     LATCH_NL, bIgnoreLatches);

            w_assert9(leaf.is_fixed());
            w_assert9(tree_root.is_fixed());
	    if(!bIgnoreLatches) {
		w_assert9(leaf.latch_mode() == LATCH_EX);
		w_assert9(tree_root.latch_mode()>= LATCH_SH);
            }
	    w_assert9(! parent.is_fixed());

            if(rce) {
                if(rce == eRETRY) {
                    leaf.unfix();
                    parent.unfix();
                    /*
                     * Re-search, holding the tree-root latch.
                     * That search will culminate in 
                     * the leaf no longer having the bit set,
                     * OR it'll still be set but because we
                     * already held the latch, we'll not
                     * end up with eRETRY
                     */
                    DBGTHRD(<<"-->again TREE LATCH MODE "
                                << int(tree_root.latch_mode())
                                );
                    goto again;
                }
                return RC(rce);
            }
            w_assert9(leaf.is_fixed());

            /* 
             * Paragraph 1, 3.3 Insert of KVL paper.
             * if the tree root latch is granted, the
             * insert algorithm releases the latches on the root and the
             * parent, clears the SMO bit on the leaf
             * and continues with the insert
             */
            tree_root.unfix();
            parent.unfix();

            /*
             * SMO is completed
             */
            if(leaf.is_smo())     {
#if BTREE_LOG_COMMENT_ON
                W_DO(log_comment("clr_smo/I"));
#endif
                W_DO( leaf.clr_smo(true) );
            }
            SSMTEST("btree.insert.2");
            if(leaf.is_delete())     {
#if BTREE_LOG_COMMENT_ON
                W_DO(log_comment("clr_delete"));
#endif
                W_DO( leaf.clr_delete() );
            }
        } else {
            // release parent right away
            parent.unfix();
        }
        /*
         * case we grabbed the tree latch and then
         * we had to retry, we could still be holding
         * it (because another thread could have
         * unset the smo bit in the meantime)
         * We can call unfix because it's safe in event that
         * the latch isn't really held.
         */
        tree_root.unfix();

        w_assert2(leaf.is_fixed());
        w_assert2(! leaf.is_smo());

        w_assert2(!parent.is_fixed());
        w_assert2(!tree_root.is_fixed() );

        w_assert2(leaf.nrecs() || leaf.pid() == root || smlevel_0::in_recovery());

        /*
          * We know we're at the correct leaf now.
         * Do we have to split the page?
         * NB: this might cause unnecessary page splits
         * when the user tries to insert something that's
         * already there. But since we don't yet know if 
         * the item is there, we can't do much at this point.
         */

        {
            /*
             * Don't actually DO the insert -- just see
             * if there's room for the insert.
             */
            slotid_t slot = 0; // for this purpose, it doesn't much
                      // matter *what* slot we use
            DBG(<<" page " << leaf.pid() << " nrecs()= " << leaf.nrecs() );

            /*false -> don't DO it, just compute max space need */
            DBG(<<"Don't insert in leaf at slot " << slot
                << " just check for space");
            rc = leaf.insert(key, el, slot, 0, false);
            if(rc.is_error()) {
                DBG(<<" page " << leaf.pid() << " nrecs()= " << leaf.nrecs() 
                        << " rc= " << rc
                );
                if (rc.err_num() == eRECWONTFIT) {
                    /* 
                     * Cannot split 1 record!
                     * We shouldn't have allowed an insertion
                     * of a record that's too big to fit.
                     */

                    bool split_with_smo = true;

                    /*********************************************************
                      FRJ: Ordinarily only one leaf split per tree is
                      allowed at a time; we hack around this
                      limitation (probably killing recoverability) by
                      checking if we can latch the full leaf's parent
                      *and* the parent's lsn did not change since we
                      last had the latch *and* that parent is
                      non-full. If we succeed, we insert directly into
                      the parent without starting an official SMO.

                      Steps:
                      - attempt to latch child's current right sibling 
                         (in case is shares child's parent)
                      - attempt to latch parent and verify lsn
                      - check parent for space
                      - split child, noting right sibling's pid
                      - insert right sibling into parent and update smo bits
                     */

                    /* First try to latch the current right sib 
                     * (soon to be cousin)
                     */
		    if(0) {
			w_assert0(!parent.is_fixed());
                        lpid_t cousin_pid = leaf.pid();
                        cousin_pid.page = leaf.next();
                        btree_p cousin;

                        if(cousin_pid.page) {
			    fix_latch = LATCH_EX;
			    if(bIgnoreLatches) {
				fix_latch = LATCH_NL;
			    }
                            rc = cousin.conditional_fix(cousin_pid, fix_latch);
                            W_IFDEBUG1(
                            if(!rc.is_error())
                                w_assert1(cousin.prev() == leaf.pid().page);)
                        } else {
                            rc = RCOK;
                        }
                        
                        if(!rc.is_error()) {
                            unsigned int required_space  =
                                2 *(key.size() + el_size +
                                btree_p::overhead_requirement_per_entry);

			    fix_latch = LATCH_EX;
			    if(bIgnoreLatches) {
				fix_latch = LATCH_NL;
			    }
                            rc = parent.conditional_fix(parent_pid, fix_latch);
                            if(!rc.is_error()) {
                                /* check if parent has space for the new
                                   key. To avoid running afoul of shore's
                                   key compression scheme, we assume that
                                   the current key-to-be-inserted is as
                                   large as any that will ever be
                                   asserted, and that the current key is
                                   very similar to an existing one
                                   (basically, fixed-size key+elem and no
                                   compression is possible). 

                                   Really it all boils down to, "is there
                                   room for two of the current key/el
                                   pair?"
                                */
                                if(parent.lsn() == parent_lsn && 
                                    parent.usable_space() >= required_space) {
                                    /*
                                      phew! all is in order to begin the actual split...
                                    */

                                    // stolen from _split_leaf
                                    lsn_t anchor;
                                    xct_t* xd = xct();
                                    lpid_t rsib_pid;
                                    bool left_heavy;
                                    slotid_t slot = 1;
                                    int addition = key.size() + el_size + 2;
                                    check_compensated_op_nesting ccond(xd, __LINE__, __FILE__);
                                    if(xd) anchor = xd->anchor();
                DBG(<<" splitting page " << leaf.pid() 
                        << " parent.usabel_space()= " << parent.usable_space()
                        << " required_space = " << required_space
                        << " addition = " << addition
                   );
		rc = __split_page(leaf, rsib_pid, left_heavy, slot, addition, split_factor, bIgnoreLatches);

                                    // stolen from xct.h
                                    if(rc.is_error()) {
                                        if(xd) {
                                            W_COERCE(xd->rollback(anchor));
                                            xd->release_anchor(true LOG_COMMENT_USE("btimpl1"));
                                        }
                                    }
                                    else {
                                        // stolen from _propagate
                                        slotid_t child_slot = 0;
                                        bool found_key = false;
                                        bool total_match = false;
                                        rc = parent.search(key, el, found_key, total_match, child_slot);
                                        if(!total_match) child_slot--;
                                        if(rc.is_error()) {
                                            if(xd) {
                                                W_COERCE(xd->rollback(anchor));
                                                xd->release_anchor(true 
                                                        LOG_COMMENT_USE("btimpl2"));
                                            }
                                        }
                                        else {
                                            // stolen from _propagate_split
                                            bool was_split = false;
                                            rc = _propagate_split(parent, leaf.pid(), child_slot,
								  was_split, bIgnoreLatches);
                                            if(rc.is_error()) {
                                                if(xd) {
                                                    W_COERCE(xd->rollback(anchor));
                                                    xd->release_anchor(true 
                                                        LOG_COMMENT_USE("btimpl3"));
                                                }
                                            }
                                            else {
                                                w_assert1(!was_split);
                                                split_with_smo = false;
                                                if(xd) xd->compensate(anchor,false/*not undoable*/
                                                        LOG_COMMENT_USE("btree1"));

						tree_root.unfix();
						goto again;
                                            }
                                        }
                                    }
                                }
                                parent.unfix();
                            } // end not error on conditional fix
                        }
                        w_assert1(leaf.is_fixed());
                        w_assert1(! parent.is_fixed());
                    }
                    /*******************************************************/

                    if(split_with_smo) 
                    {
                        /*
                         * Split the page  - get the tree latch and
                         * hang onto it until we're done with the split.
                         * If a page changed during the wait for the
                         * tree latch, we'll start all over, having
                         * free the tree latch.
                         */
			smo_mode = LATCH_EX;
			smo_p1mode = LATCH_EX;
			if(bIgnoreLatches) {
			    smo_mode = LATCH_NL;
			    smo_p1mode = LATCH_NL;
			}
                        w_error_t::err_num_t rce = 
                            tree_root.get_for_smo(true, smo_mode,
                                leaf, smo_p1mode, false, 0, LATCH_NL, bIgnoreLatches);

                        w_assert2(tree_root.is_fixed());
                        w_assert2(leaf.is_fixed());
                        w_assert9(! parent.is_fixed());

                        if(rce) {
                            if(rce== eRETRY) {
                                /*
                                 * One of the pages had changed since
                                 * we unlatched and awaited the tree latch,
                                 * so free the tree latch and 
                                 * re-start the search.
                                 */
                                tree_root.unfix();
                                leaf.unfix();
                                DBGTHRD(<<"-->again TREE LATCH MODE "
                                    << int(tree_root.latch_mode())
                                    );
                                goto again;
                            }
                            return RC(rce);
                        }
                        w_assert2(!parent.is_fixed());
                        w_assert2(leaf.is_fixed());
                        w_assert2(tree_root.is_fixed());
                        W_DO( _split_leaf_and_relocate_recs(tree_root.pid(), leaf, key, 
							    el, split_factor, bIgnoreLatches,
							    relocate_callback) );
                        SSMTEST("btree.insert.3");

                        tree_root.unfix();
                        DBG(<<"split -->again");
                        goto again;
                    }
                } else {
                    return RC_AUGMENT(rc);
                }
            }
            DBG(<<" page " << leaf.pid() << " nrecs()= " << leaf.nrecs() );
        } // check for need to split

        slotid_t                 slot = -1;
        {
        /*
          * We know we're at the correct leaf now, and we
         * don't have to split.
         * Get the slot in the leaf, and figure out if
         * we have to lock the next key value.
         */

        btree_p                p2; // possibly needed for 2nd leaf
        lpid_t                p2_pid = lpid_t::null;
        lsn_t                p2_lsn;
        uint                whatcase;
        bool                 eof=false;
        slotid_t                 next_slot = 0;

        W_DO( _satisfy(leaf, key, el, found, 
                    total_match, slot, whatcase));

        DBGTHRD(<<"found = " << found
                << " total_match=" << total_match
                << " leaf=" << leaf.pid()
                << " case=" << whatcase
                << " slot=" << slot);

        w_assert9(!parent.is_fixed());

        lock_duration_t this_duration = t_long;
        lock_duration_t next_duration = t_instant;
        lock_mode_t        this_mode = EX;
        lock_mode_t        next_mode = IX;
        bool                 lock_next = true;

        bool                look_on_next_page = false;
        switch(whatcase) {

            case m_not_found_end_of_file:
                // Will use EOF lock
                w_assert9( !total_match ); // but found (key) could be true
                eof = true;
                break;

            case m_satisfying_key_found_same_page: {
                /*
                 * found means we found the key we're seeking, which
                 *   will ultimately result in an eDUPLICATE, and we won't
                 *   have to lock the next key.
                 *
                 *  !found means the satisfying key is the next key
                 *   and it's on the this page
                 */
                p2 = leaf; // refixes
                if(!found) {
                    next_slot = slot;
                    break;
                } else {
                    // Next entry might be on next page
                    next_slot = slot+1;
                    if(next_slot < leaf.nrecs()) {
                        break;
                    } else if( !leaf.next()) {
                        eof = true;
                        break;
                    }
                }
                look_on_next_page = true;
            } break; 

            case m_not_found_end_of_non_empty_page: {
                look_on_next_page = true;
            } break;

            case m_not_found_page_is_empty:
                /*
                 * there should be no smos in progress: 
                 * can't be the case during forward processing.
                 * but during undo we can get here.
                 */
                w_assert9(smlevel_0::in_recovery());
                w_assert9(leaf.next());
                look_on_next_page = true;
                break;
        }
        if(look_on_next_page) {
            /*
             * the next key is on the NEXT page, (if the next 
             * page isn't empty- we don't know that because we haven't yet
             * traversed there).
             */

            /*
             * This much is taken from the Fetch/lookup cases
             * since Mohan just says "in a manner similar to fetch"
             */
            w_assert9(leaf.nrecs() || leaf.pid() == root || smlevel_0::in_recovery());
            /*
             * Mohan: unlatch the parent and latch the successor.
             * If the successor is empty or does not have a
             * satisfying key, unlatch both pages and request
             * the tree latch in S mode, then restart the search
             * from the parent.
             */
            parent.unfix();

            w_assert9(leaf.next());
            p2_pid = root; // get volume, store part
            p2_pid.page = leaf.next();

            INC_TSTAT(bt_links);
	    fix_latch = LATCH_SH;
	    if(bIgnoreLatches) {
		fix_latch = LATCH_NL;
	    }
            W_DO( p2.fix(p2_pid, fix_latch) );
            next_slot = 0; //first rec on next page

            w_assert9(p2.nrecs());  // else we'd have been at
                                    // case m_not_found_end_of_file
            w_assert9( !found || !total_match ||
                    (whatcase == m_satisfying_key_found_same_page));
        } 

        /*
         *  Create KVL lock for to-be-inserted key value
         *  We need to use it in the determination (below)
         *  of lock/don't lock the next key-value
         */
#if W_DEBUG_LEVEL > 2
        if(p2_pid.page) w_assert3(p2.is_fixed());
#endif

        lockid_t kvl;
        mk_kvl(cc, kvl, stid, unique, key, el);

        /*
         * Figure out if we need to lock the next key-value
         * Ref: Figure 1 in VLDB KVL paper
         */

        if(unique && found) {
            /*
             * Try a conditional, commit-duration (gives RR -- instant
             * would satisfy CS) lock on the found key value
             * to make sure the found key is committed, or else
             * was put there by this xct. 
             */
            this_duration = t_long;
            this_mode = SH;
            lock_next = false;  

        } else if ( (!unique) && total_match ) {
            /*  non-unique index, total match */
            this_mode = IX;
            this_duration = t_long;
            lock_next = false;
            /*
             * we will return eDUPLICATE.  See Mohan KVL 3.3 Insert,
             * paragraph 2 for explanation why the IX lock on THIS key
             * is sufficient for CS,RR, and we don't have to go through
             * the rigamarole of getting the share latch, as in the case
             * of a unique index.
             */
        } else {
            w_assert9((unique && !found)  ||
                        (!unique && !total_match));
            this_mode = IX; // See note way below before we
                                // grab the THIS lock
            this_duration = t_long;
            lock_next = true;
            next_mode = IX;
            next_duration = t_instant;
        }

        /* 
         * Grab the key-value lock for the next key, if needed.
         * First try conditionally.  If that fails, unlatch the
         * leaf page(s) and try unconditionally.
         */
        if( (cc == t_cc_none) || (cc == t_cc_modkvl) ) lock_next = false;

#if W_DEBUG_LEVEL > 2
        if(p2_pid.page) w_assert3(p2.is_fixed());
#endif

        if(lock_next) {
            lockid_t nxt_kvl;

            if(eof) {
                mk_kvl_eof(cc, nxt_kvl, stid);
            } else {
                w_assert9(p2.is_fixed());
                btrec_t r(p2, next_slot); 
                mk_kvl(cc, nxt_kvl, stid, unique, r);
            }
            rc = lm->lock(nxt_kvl, next_mode, next_duration, WAIT_IMMEDIATE);
            if (rc.is_error())  {
                DBG(<<"rc= " << rc);
                w_assert9((rc.err_num() == eLOCKTIMEOUT) || (rc.err_num() == eDEADLOCK));

                if(p2.is_fixed()) {
                    p2_pid = p2.pid(); 
                    p2_lsn = p2.lsn(); 
                    p2.unfix();
                } else {
                  w_assert9(!p2_pid.valid());
                }
                leaf.unfix();
                W_DO(lm->lock(nxt_kvl, next_mode, next_duration));

		fix_latch = LATCH_EX;
		if(bIgnoreLatches) {
		    fix_latch = LATCH_NL;
		}
                W_DO(leaf.fix(leaf_pid, fix_latch) );
                if(leaf.lsn() != leaf_lsn) {
                    /*
                     * if the page changed, the found key
                     * might not be in the tree anymore.
                     */
                    leaf.unfix();
                    DBG(<<"-->again");
                    goto again;
                }
                if(p2_pid.page) {
		    fix_latch = LATCH_SH;
		    if(bIgnoreLatches) {
			fix_latch = LATCH_NL;
		    }
                    W_DO(p2.fix(p2_pid, fix_latch));
                    if(p2.lsn() != p2_lsn) {
                        /*
                         * Have to re-compute next value.
                         * TODO (performance): we should avoid this re-traversal
                         * but instead just 
                         * go from leaf -> leaf.next() again.
                         */
                        leaf.unfix();
                        p2.unfix();
                        DBG(<<"-->again");
                        goto again;
                    }
                }
            }
        }
        p2.unfix();

        /* WARNING: If this gets fixed, so we keep the IX lock,
         * we must change the way locks are logged at prepare time!
         * (Lock on next value is of instant duration, so it's ok.)
         */
        
        this_mode = EX;

        if(cc > t_cc_none) {
            /* 
             * Grab the key-value lock for the found key.
             * First try conditionally.  If that fails, unlatch the
             * leaf page(s) and try unconditionally.
             */
                rc = lm->lock(kvl, this_mode, this_duration, WAIT_IMMEDIATE);
            if (rc.is_error())  {
                DBG(<<"rc=" <<rc);
                w_assert9((rc.err_num() == eLOCKTIMEOUT) || (rc.err_num() == eDEADLOCK));

                if(p2.is_fixed()) {
                    p2_pid = p2.pid(); // might be null
                    p2_lsn = p2.lsn(); // might be null
                    p2.unfix();
                }
                leaf.unfix();
                W_DO(lm->lock(kvl, this_mode, this_duration));

		fix_latch = LATCH_EX;
		if(bIgnoreLatches) {
		    fix_latch = LATCH_NL;
		}
                W_DO(leaf.fix(leaf_pid, fix_latch) );
                if(leaf.lsn() != leaf_lsn) {
                    /*
                     * if the page changed, the found key
                     * might not be in the tree anymore.
                     */
                    parent.unfix();
                    leaf.unfix();
                    DBG(<<"-->again");
                    goto again;
                }
            }
        }
        if((found && unique) || total_match) {
            /* 
             * Didn't have to wait for a lock, or waited and the
             * pages didn't change. It's found, is really there, 
             * and it's an error.
             */
            DBG(<<"DUPLICATE");
            return RC(eDUPLICATE);
        }

        } // getting locks

        w_assert9(leaf.is_fixed());

        /* 
         * Ok - all necessary latches and locks are held.
         * If we have a sibling and had to get the next key value,
         *  we already released that page.
         */

	// place to make a call back to kits and inform it about the leaf page
	    el.reset();
	    free(tmp_el);
	    lpid_t leaf_page_pid = leaf.pid();
	    leaf.unfix();

	    ef->fill_el(el2, leaf_page_pid);
	    fix_latch = LATCH_EX;
	    if(bIgnoreLatches) {
		fix_latch = LATCH_NL;
	    }
	    W_DO( leaf.fix(leaf_page_pid, fix_latch) );
	    //

        /*
         *  Do the insert.
         *  Turn off logging and perform physical insert.
         */
        {
            w_assert9(!smlevel_1::log || me()->xct()->is_log_on());
            xct_log_switch_t toggle(OFF);

            w_assert9(slot >= 0 && slot <= leaf.nrecs());

            DBG(<<"insert in leaf at slot " << slot);
            rc = leaf.insert(key, el2, slot); 
            if(rc.is_error()) {
                DBG(<<"rc= " << rc);
                leaf.discard(); // force the page out.
                return rc.reset();
            }
            // Keep pinned until logging is done.
        }
        SSMTEST("btree.insert.4");
        /*
         *  Log is on here. Log a logical insert.
         *  If we get a retry, then retry a few times.
         */
        rc = log_btree_insert(leaf, slot, key, el2, unique);
        int count=10;
        while (rc.is_error() && (rc.err_num() == eRETRY) && --count > 0) {
            rc = log_btree_insert(leaf, slot, key, el2, unique);
        }

        SSMTEST("btree.insert.5");
        if (rc.is_error())  {
            /*
             *  Failed writing log. Manually undo physical insert.
             */
            xct_log_switch_t toggle(OFF);
            DBG(<<"log_btree_insert failed" << rc );

            w_rc_t rc2 = 
                leaf.remove(slot, leaf.is_compressed()); // zkeyed_p::remove
            if(rc2.is_error()) {
                DBG(<<"subsequent remove failed" << rc2 );
                leaf.discard(); // force the page out.
                return rc2.reset();
            }
            // The problem here is that the leaf page changes
            // might *still* not have been logged.
            if(rc.err_num() == eRETRY) {
                leaf.discard(); // force the page out.
            }
            return rc.reset();
        }
        w_assert9(!smlevel_1::log || me()->xct()->is_log_on());
    }
    }
    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e);
    }
    
    return RCOK;
}


/*********************************************************************
 *
 *  btree_impl::_alloc_page(root, level, near, ret_page, pid0,
 *                           set_its_smo, compressed)
 *
 *  Allocate a page for btree rooted at root. Fix the page allocated
 *  and return it in ret_page.
 *
 *          ============ IMPORTANT ============
 *  This function should be called in a compensating
 *  action so that it does not get undone if the xct
 *  aborts. The page allocated would become part of
 *  the btree as soon as other pages started pointing
 *  to it. So, page allocation as well as other
 *  operations to linkup the page should be a single
 *  top-level action.
 *
 *********************************************************************/
rc_t
btree_impl::_alloc_page(
    const lpid_t& root, 
    int2_t level,
    const lpid_t& near_p,
    btree_p& ret_page,
    shpid_t pid0, // = 0
    bool set_its_smo, // = false
    bool compressed,  // = false
    store_flag_t stf,  // = st_regular,
    const bool bIgnoreLatches // = false
    )
{
    FUNC(btree_impl::_alloc_page);

    latch_mode_t mode = LATCH_EX;
    if(bIgnoreLatches) {
	mode = LATCH_NL;
    }
	
    lpid_t pid;
    w_assert9(near_p != lpid_t::null);

    // temporary btrees are not supported: use st_regular
    // only way to exploit st_tmp here is through bulk-loading
    DBG(<<"stid " << root.stid());
    W_DO(smlevel_0::io->alloc_a_page(
        root.stid(), 
        near_p,         // hint
        pid,         // output
        true,         //  may_realloc 
        NL,        //  ignored
        true        // search file
        )) ;
    W_DO( ret_page.fix(pid, mode, ret_page.t_virgin, stf) );

    btree_p::flag_t f = compressed? btree_p::t_compressed : btree_p::t_none;

    if(set_its_smo) {
        f = (btree_p::flag_t)
                (((unsigned int) f) | ((unsigned int) btree_p::t_smo));
    }
    W_DO( ret_page.set_hdr(root.page, level, pid0, (uint2_t)f) );


    DBGTHRD(<<"allocated btree page " << pid << " at level " << level);

    INC_TSTAT(page_btree_alloc);
    
    return RCOK;
}

/******************************************************************
 *
 * btree_impl::_insert(root, unique, cc, key, el, split_factor)
 *  The essence of Mohan's insert function.  cc should take
 *  t_cc_none, t_cc_im, t_cc_kvl, t_cc_modkvl
 *
 ******************************************************************/

rc_t
btree_impl::_insert(
    const lpid_t&        root,                // I-  root of btree
    bool                unique,                // I-  true if tree is unique
    concurrency_t        cc,                // I-  concurrency control 
    const cvec_t&        key,                // I-  which key
    const cvec_t&        el,                // I-  which element
    int                 split_factor,        // I-  tune split in %
    const bool bIgnoreLatches)
{
    FUNC(btree_impl::_insert);
    lpid_t                  search_start_pid = root;
    lsn_t                  search_start_lsn = lsn_t::null;
    stid_t                 stid = root.stid();

    INC_TSTAT(bt_insert_cnt);

    DBGTHRD(<<"_insert: unique = " << unique << " cc=" << int(cc)
        << " key=" << key );

    if(!bIgnoreLatches) {
	get_latches(___s,___e);
    }
    
    {
        tree_latch         tree_root(root, bIgnoreLatches);

	// latch modes
	latch_mode_t traverse_latch;
	latch_mode_t smo_mode;
	latch_mode_t smo_p1mode;
	latch_mode_t fix_latch;
again:
    {
        btree_p         leaf;
        lpid_t          leaf_pid;
        lsn_t           leaf_lsn;
        btree_p         parent;
        lsn_t           parent_lsn;
        lpid_t          parent_pid;
        bool            found = false;
        bool            total_match = false;
        rc_t            rc;


        DBGTHRD(<<"_insert.again:");

        w_assert9( !leaf.is_fixed());
        w_assert9( !parent.is_fixed());

	if(!bIgnoreLatches) {
	    if(tree_root.is_fixed()) {
		check_latches(___s+1,___e+1, ___s+___e+1);
	    } else {
		check_latches(___s,___e, ___s+___e);
	    }
	}

        /*
         *  Walk down the tree.  Traverse doesn't
         *  search the leaf page; it's our responsibility
         *  to check that we're at the correct leaf.
         */
	traverse_latch = LATCH_EX;
	if(bIgnoreLatches) {
	    traverse_latch = LATCH_NL;
	}
	
        W_DO( _traverse(root, search_start_pid,
            search_start_lsn,
            key, el, found, 
            traverse_latch, leaf, parent,
			leaf_lsn, parent_lsn, bIgnoreLatches) );

	if(!bIgnoreLatches) {
	    if(leaf.pid() == root) {
		check_latches(___s,___e+2, ___s+___e+2);
	    } else {
		check_latches(___s+1,___e+1, ___s+___e+2);
	    }
	}

        w_assert9( leaf.is_fixed());
        w_assert9( leaf.is_leaf());

        w_assert9( parent.is_fixed());
        w_assert9( parent.is_node() || (parent.is_leaf() &&
                    leaf.pid() == root  ));
        w_assert9( parent.is_leaf_parent() || parent.is_leaf());

        /*
         * Deal with SMOs :  traversal checked the nodes, but
         * not the leaf.  We check the leaf for smo, delete bits.
         * (Delete only checks for smo bits.)
         */

        leaf_pid = leaf.pid();
        parent_pid = parent.pid(); 

        if(leaf.is_smo() || leaf.is_delete()) {

            /* 
             * SH-latch the tree for manual duration -- paragraph 1
             * of 3.3 Insert of KVL paper.  It gets unlatched
             * below, not far.
             */
            w_assert2(parent.is_fixed());
	    w_assert2(leaf.is_fixed());
	    if(!bIgnoreLatches) {
		w_assert2(parent.latch_mode()>=LATCH_SH); // could be EX
		w_assert2(leaf.latch_mode() == LATCH_EX);
	    }
	    /* conditional=true, try to get tree latch in SH mode,
             */
	    smo_mode = LATCH_SH;
	    smo_p1mode = LATCH_EX;
	    if(bIgnoreLatches) {
		smo_mode = LATCH_NL;
		smo_p1mode = LATCH_NL;
	    }
	    w_error_t::err_num_t rce = tree_root.get_for_smo(true, smo_mode, 
							     leaf, smo_p1mode, false, &parent,
							     LATCH_NL, bIgnoreLatches);

            w_assert9(leaf.is_fixed());
            w_assert9(tree_root.is_fixed());
	    if(!bIgnoreLatches) {
		w_assert9(leaf.latch_mode() == LATCH_EX);
		w_assert9(tree_root.latch_mode()>= LATCH_SH);
            }
	    w_assert9(! parent.is_fixed());

            if(rce) {
                if(rce == eRETRY) {
                    leaf.unfix();
                    parent.unfix();
                    /*
                     * Re-search, holding the tree-root latch.
                     * That search will culminate in 
                     * the leaf no longer having the bit set,
                     * OR it'll still be set but because we
                     * already held the latch, we'll not
                     * end up with eRETRY
                     */
                    DBGTHRD(<<"-->again TREE LATCH MODE "
                                << int(tree_root.latch_mode())
                                );
                    goto again;
                }
                return RC(rce);
            }
            w_assert9(leaf.is_fixed());

            /* 
             * Paragraph 1, 3.3 Insert of KVL paper.
             * if the tree root latch is granted, the
             * insert algorithm releases the latches on the root and the
             * parent, clears the SMO bit on the leaf
             * and continues with the insert
             */
            tree_root.unfix();
            parent.unfix();

            /*
             * SMO is completed
             */
            if(leaf.is_smo())     {
#if BTREE_LOG_COMMENT_ON
                W_DO(log_comment("clr_smo/I"));
#endif
                W_DO( leaf.clr_smo(true) );
            }
            SSMTEST("btree.insert.2");
            if(leaf.is_delete())     {
#if BTREE_LOG_COMMENT_ON
                W_DO(log_comment("clr_delete"));
#endif
                W_DO( leaf.clr_delete() );
            }
        } else {
            // release parent right away
            parent.unfix();
        }
        /*
         * case we grabbed the tree latch and then
         * we had to retry, we could still be holding
         * it (because another thread could have
         * unset the smo bit in the meantime)
         * We can call unfix because it's safe in event that
         * the latch isn't really held.
         */
        tree_root.unfix();

        w_assert2(leaf.is_fixed());
        w_assert2(! leaf.is_smo());

        w_assert2(!parent.is_fixed());
        w_assert2(!tree_root.is_fixed() );

        w_assert2(leaf.nrecs() || leaf.pid() == root || smlevel_0::in_recovery());

        /*
          * We know we're at the correct leaf now.
         * Do we have to split the page?
         * NB: this might cause unnecessary page splits
         * when the user tries to insert something that's
         * already there. But since we don't yet know if 
         * the item is there, we can't do much at this point.
         */

        {
            /*
             * Don't actually DO the insert -- just see
             * if there's room for the insert.
             */
            slotid_t slot = 0; // for this purpose, it doesn't much
                      // matter *what* slot we use
            DBG(<<" page " << leaf.pid() << " nrecs()= " << leaf.nrecs() );

            /*false -> don't DO it, just compute max space need */
            DBG(<<"Don't insert in leaf at slot " << slot
                << " just check for space");
            rc = leaf.insert(key, el, slot, 0, false);
            if(rc.is_error()) {
                DBG(<<" page " << leaf.pid() << " nrecs()= " << leaf.nrecs() 
                        << " rc= " << rc
                );
                if (rc.err_num() == eRECWONTFIT) {
                    /* 
                     * Cannot split 1 record!
                     * We shouldn't have allowed an insertion
                     * of a record that's too big to fit.
                     */

                    bool split_with_smo = true;

                    /*********************************************************
                      FRJ: Ordinarily only one leaf split per tree is
                      allowed at a time; we hack around this
                      limitation (probably killing recoverability) by
                      checking if we can latch the full leaf's parent
                      *and* the parent's lsn did not change since we
                      last had the latch *and* that parent is
                      non-full. If we succeed, we insert directly into
                      the parent without starting an official SMO.

                      Steps:
                      - attempt to latch child's current right sibling 
                         (in case is shares child's parent)
                      - attempt to latch parent and verify lsn
                      - check parent for space
                      - split child, noting right sibling's pid
                      - insert right sibling into parent and update smo bits
                     */

                    /* First try to latch the current right sib 
                     * (soon to be cousin)
                     */
		    if(0) {
			w_assert0(!parent.is_fixed());
                        lpid_t cousin_pid = leaf.pid();
                        cousin_pid.page = leaf.next();
                        btree_p cousin;

                        if(cousin_pid.page) {
			    fix_latch = LATCH_EX;
			    if(bIgnoreLatches) {
				fix_latch = LATCH_NL;
			    }
                            rc = cousin.conditional_fix(cousin_pid, fix_latch);
                            W_IFDEBUG1(
                            if(!rc.is_error())
                                w_assert1(cousin.prev() == leaf.pid().page);)
                        } else {
                            rc = RCOK;
                        }
                        
                        if(!rc.is_error()) {
                            unsigned int required_space  =
                                2 *(key.size() + el.size() +
                                btree_p::overhead_requirement_per_entry);

			    fix_latch = LATCH_EX;
			    if(bIgnoreLatches) {
				fix_latch = LATCH_NL;
			    }
                            rc = parent.conditional_fix(parent_pid, fix_latch);
                            if(!rc.is_error()) {
                                /* check if parent has space for the new
                                   key. To avoid running afoul of shore's
                                   key compression scheme, we assume that
                                   the current key-to-be-inserted is as
                                   large as any that will ever be
                                   asserted, and that the current key is
                                   very similar to an existing one
                                   (basically, fixed-size key+elem and no
                                   compression is possible). 

                                   Really it all boils down to, "is there
                                   room for two of the current key/el
                                   pair?"
                                */
                                if(parent.lsn() == parent_lsn && 
                                    parent.usable_space() >= required_space) {
                                    /*
                                      phew! all is in order to begin the actual split...
                                    */

                                    // stolen from _split_leaf
                                    lsn_t anchor;
                                    xct_t* xd = xct();
                                    lpid_t rsib_pid;
                                    bool left_heavy;
                                    slotid_t slot = 1;
                                    int addition = key.size() + el.size() + 2;
                                    check_compensated_op_nesting ccond(xd, __LINE__, __FILE__);
                                    if(xd) anchor = xd->anchor();
                DBG(<<" splitting page " << leaf.pid() 
                        << " parent.usabel_space()= " << parent.usable_space()
                        << " required_space = " << required_space
                        << " addition = " << addition
                   );
		rc = __split_page(leaf, rsib_pid, left_heavy, slot, addition, split_factor, bIgnoreLatches);

                                    // stolen from xct.h
                                    if(rc.is_error()) {
                                        if(xd) {
                                            W_COERCE(xd->rollback(anchor));
                                            xd->release_anchor(true LOG_COMMENT_USE("btimpl1"));
                                        }
                                    }
                                    else {
                                        // stolen from _propagate
                                        slotid_t child_slot = 0;
                                        bool found_key = false;
                                        bool total_match = false;
                                        rc = parent.search(key, el, found_key, total_match, child_slot);
                                        if(!total_match) child_slot--;
                                        if(rc.is_error()) {
                                            if(xd) {
                                                W_COERCE(xd->rollback(anchor));
                                                xd->release_anchor(true 
                                                        LOG_COMMENT_USE("btimpl2"));
                                            }
                                        }
                                        else {
                                            // stolen from _propagate_split
                                            bool was_split = false;
                                            rc = _propagate_split(parent, leaf.pid(), child_slot,
								  was_split, bIgnoreLatches);
                                            if(rc.is_error()) {
                                                if(xd) {
                                                    W_COERCE(xd->rollback(anchor));
                                                    xd->release_anchor(true 
                                                        LOG_COMMENT_USE("btimpl3"));
                                                }
                                            }
                                            else {
                                                w_assert1(!was_split);
                                                split_with_smo = false;
                                                if(xd) xd->compensate(anchor,false/*not undoable*/
                                                        LOG_COMMENT_USE("btree1"));

						tree_root.unfix();
						goto again;
                                            }
                                        }
                                    }
                                }
                                parent.unfix();
                            } // end not error on conditional fix
                        }
                        w_assert1(leaf.is_fixed());
                        w_assert1(! parent.is_fixed());
                    }
                    /*******************************************************/

                    if(split_with_smo) 
                    {
                        /*
                         * Split the page  - get the tree latch and
                         * hang onto it until we're done with the split.
                         * If a page changed during the wait for the
                         * tree latch, we'll start all over, having
                         * free the tree latch.
                         */
			smo_mode = LATCH_EX;
			smo_p1mode = LATCH_EX;
			if(bIgnoreLatches) {
			    smo_mode = LATCH_NL;
			    smo_p1mode = LATCH_NL;
			}
                        w_error_t::err_num_t rce = 
                            tree_root.get_for_smo(true, smo_mode,
                                leaf, smo_p1mode, false, 0, LATCH_NL, bIgnoreLatches);

                        w_assert2(tree_root.is_fixed());
                        w_assert2(leaf.is_fixed());
                        w_assert9(! parent.is_fixed());

                        if(rce) {
                            if(rce== eRETRY) {
                                /*
                                 * One of the pages had changed since
                                 * we unlatched and awaited the tree latch,
                                 * so free the tree latch and 
                                 * re-start the search.
                                 */
                                tree_root.unfix();
                                leaf.unfix();
                                DBGTHRD(<<"-->again TREE LATCH MODE "
                                    << int(tree_root.latch_mode())
                                    );
                                goto again;
                            }
                            return RC(rce);
                        }
                        w_assert2(!parent.is_fixed());
                        w_assert2(leaf.is_fixed());
                        w_assert2(tree_root.is_fixed());
                        W_DO( _split_leaf(tree_root.pid(), leaf, key, 
					  el, split_factor, bIgnoreLatches) );
                        SSMTEST("btree.insert.3");

                        tree_root.unfix();
                        DBG(<<"split -->again");
                        goto again;
                    }
                } else {
                    return RC_AUGMENT(rc);
                }
            }
            DBG(<<" page " << leaf.pid() << " nrecs()= " << leaf.nrecs() );
        } // check for need to split

        slotid_t                 slot = -1;
        {
        /*
          * We know we're at the correct leaf now, and we
         * don't have to split.
         * Get the slot in the leaf, and figure out if
         * we have to lock the next key value.
         */
        btree_p                p2; // possibly needed for 2nd leaf
        lpid_t                p2_pid = lpid_t::null;
        lsn_t                p2_lsn;
        uint                whatcase;
        bool                 eof=false;
        slotid_t                 next_slot = 0;

        W_DO( _satisfy(leaf, key, el, found, 
                    total_match, slot, whatcase));

        DBGTHRD(<<"found = " << found
                << " total_match=" << total_match
                << " leaf=" << leaf.pid()
                << " case=" << whatcase
                << " slot=" << slot);

        w_assert9(!parent.is_fixed());

        lock_duration_t this_duration = t_long;
        lock_duration_t next_duration = t_instant;
        lock_mode_t        this_mode = EX;
        lock_mode_t        next_mode = IX;
        bool                 lock_next = true;

        bool                look_on_next_page = false;
        switch(whatcase) {

            case m_not_found_end_of_file:
                // Will use EOF lock
                w_assert9( !total_match ); // but found (key) could be true
                eof = true;
                break;

            case m_satisfying_key_found_same_page: {
                /*
                 * found means we found the key we're seeking, which
                 *   will ultimately result in an eDUPLICATE, and we won't
                 *   have to lock the next key.
                 *
                 *  !found means the satisfying key is the next key
                 *   and it's on the this page
                 */
                p2 = leaf; // refixes
                if(!found) {
                    next_slot = slot;
                    break;
                } else {
                    // Next entry might be on next page
                    next_slot = slot+1;
                    if(next_slot < leaf.nrecs()) {
                        break;
                    } else if( !leaf.next()) {
                        eof = true;
                        break;
                    }
                }
                look_on_next_page = true;
            } break; 

            case m_not_found_end_of_non_empty_page: {
                look_on_next_page = true;
            } break;

            case m_not_found_page_is_empty:
                /*
                 * there should be no smos in progress: 
                 * can't be the case during forward processing.
                 * but during undo we can get here.
                 */
                w_assert9(smlevel_0::in_recovery());
                w_assert9(leaf.next());
                look_on_next_page = true;
                break;
        }
        if(look_on_next_page) {
            /*
             * the next key is on the NEXT page, (if the next 
             * page isn't empty- we don't know that because we haven't yet
             * traversed there).
             */

            /*
             * This much is taken from the Fetch/lookup cases
             * since Mohan just says "in a manner similar to fetch"
             */
            w_assert9(leaf.nrecs() || leaf.pid() == root || smlevel_0::in_recovery());
            /*
             * Mohan: unlatch the parent and latch the successor.
             * If the successor is empty or does not have a
             * satisfying key, unlatch both pages and request
             * the tree latch in S mode, then restart the search
             * from the parent.
             */
            parent.unfix();

            w_assert9(leaf.next());
            p2_pid = root; // get volume, store part
            p2_pid.page = leaf.next();

            INC_TSTAT(bt_links);
	    fix_latch = LATCH_SH;
	    if(bIgnoreLatches) {
		fix_latch = LATCH_NL;
	    }
            W_DO( p2.fix(p2_pid, fix_latch) );
            next_slot = 0; //first rec on next page

            w_assert9(p2.nrecs());  // else we'd have been at
                                    // case m_not_found_end_of_file
            w_assert9( !found || !total_match ||
                    (whatcase == m_satisfying_key_found_same_page));
        } 

        /*
         *  Create KVL lock for to-be-inserted key value
         *  We need to use it in the determination (below)
         *  of lock/don't lock the next key-value
         */
#if W_DEBUG_LEVEL > 2
        if(p2_pid.page) w_assert3(p2.is_fixed());
#endif

        lockid_t kvl;
        mk_kvl(cc, kvl, stid, unique, key, el);

        /*
         * Figure out if we need to lock the next key-value
         * Ref: Figure 1 in VLDB KVL paper
         */

        if(unique && found) {
            /*
             * Try a conditional, commit-duration (gives RR -- instant
             * would satisfy CS) lock on the found key value
             * to make sure the found key is committed, or else
             * was put there by this xct. 
             */
            this_duration = t_long;
            this_mode = SH;
            lock_next = false;  

        } else if ( (!unique) && total_match ) {
            /*  non-unique index, total match */
            this_mode = IX;
            this_duration = t_long;
            lock_next = false;
            /*
             * we will return eDUPLICATE.  See Mohan KVL 3.3 Insert,
             * paragraph 2 for explanation why the IX lock on THIS key
             * is sufficient for CS,RR, and we don't have to go through
             * the rigamarole of getting the share latch, as in the case
             * of a unique index.
             */
        } else {
            w_assert9((unique && !found)  ||
                        (!unique && !total_match));
            this_mode = IX; // See note way below before we
                                // grab the THIS lock
            this_duration = t_long;
            lock_next = true;
            next_mode = IX;
            next_duration = t_instant;
        }

        /* 
         * Grab the key-value lock for the next key, if needed.
         * First try conditionally.  If that fails, unlatch the
         * leaf page(s) and try unconditionally.
         */
        if( (cc == t_cc_none) || (cc == t_cc_modkvl) ) lock_next = false;

#if W_DEBUG_LEVEL > 2
        if(p2_pid.page) w_assert3(p2.is_fixed());
#endif

        if(lock_next) {
            lockid_t nxt_kvl;

            if(eof) {
                mk_kvl_eof(cc, nxt_kvl, stid);
            } else {
                w_assert9(p2.is_fixed());
                btrec_t r(p2, next_slot); 
                mk_kvl(cc, nxt_kvl, stid, unique, r);
            }
            rc = lm->lock(nxt_kvl, next_mode, next_duration, WAIT_IMMEDIATE);
            if (rc.is_error())  {
                DBG(<<"rc= " << rc);
                w_assert9((rc.err_num() == eLOCKTIMEOUT) || (rc.err_num() == eDEADLOCK));

                if(p2.is_fixed()) {
                    p2_pid = p2.pid(); 
                    p2_lsn = p2.lsn(); 
                    p2.unfix();
                } else {
                  w_assert9(!p2_pid.valid());
                }
                leaf.unfix();
                W_DO(lm->lock(nxt_kvl, next_mode, next_duration));

		fix_latch = LATCH_EX;
		if(bIgnoreLatches) {
		    fix_latch = LATCH_NL;
		}
                W_DO(leaf.fix(leaf_pid, fix_latch) );
                if(leaf.lsn() != leaf_lsn) {
                    /*
                     * if the page changed, the found key
                     * might not be in the tree anymore.
                     */
                    leaf.unfix();
                    DBG(<<"-->again");
                    goto again;
                }
                if(p2_pid.page) {
		    fix_latch = LATCH_SH;
		    if(bIgnoreLatches) {
			fix_latch = LATCH_NL;
		    }
                    W_DO(p2.fix(p2_pid, fix_latch));
                    if(p2.lsn() != p2_lsn) {
                        /*
                         * Have to re-compute next value.
                         * TODO (performance): we should avoid this re-traversal
                         * but instead just 
                         * go from leaf -> leaf.next() again.
                         */
                        leaf.unfix();
                        p2.unfix();
                        DBG(<<"-->again");
                        goto again;
                    }
                }
            }
        }
        p2.unfix();

        /* WARNING: If this gets fixed, so we keep the IX lock,
         * we must change the way locks are logged at prepare time!
         * (Lock on next value is of instant duration, so it's ok.)
         */
        
        this_mode = EX;

        if(cc > t_cc_none) {
            /* 
             * Grab the key-value lock for the found key.
             * First try conditionally.  If that fails, unlatch the
             * leaf page(s) and try unconditionally.
             */
                rc = lm->lock(kvl, this_mode, this_duration, WAIT_IMMEDIATE);
            if (rc.is_error())  {
                DBG(<<"rc=" <<rc);
                w_assert9((rc.err_num() == eLOCKTIMEOUT) || (rc.err_num() == eDEADLOCK));

                if(p2.is_fixed()) {
                    p2_pid = p2.pid(); // might be null
                    p2_lsn = p2.lsn(); // might be null
                    p2.unfix();
                }
                leaf.unfix();
                W_DO(lm->lock(kvl, this_mode, this_duration));

		fix_latch = LATCH_EX;
		if(bIgnoreLatches) {
		    fix_latch = LATCH_NL;
		}
                W_DO(leaf.fix(leaf_pid, fix_latch) );
                if(leaf.lsn() != leaf_lsn) {
                    /*
                     * if the page changed, the found key
                     * might not be in the tree anymore.
                     */
                    parent.unfix();
                    leaf.unfix();
                    DBG(<<"-->again");
                    goto again;
                }
            }
        }
        if((found && unique) || total_match) {
            /* 
             * Didn't have to wait for a lock, or waited and the
             * pages didn't change. It's found, is really there, 
             * and it's an error.
             */
            DBG(<<"DUPLICATE");
            return RC(eDUPLICATE);
        }

        } // getting locks

        w_assert9(leaf.is_fixed());

        /* 
         * Ok - all necessary latches and locks are held.
         * If we have a sibling and had to get the next key value,
         *  we already released that page.
         */

        /*
         *  Do the insert.
         *  Turn off logging and perform physical insert.
         */
        {
            w_assert9(!smlevel_1::log || me()->xct()->is_log_on());
            xct_log_switch_t toggle(OFF);

            w_assert9(slot >= 0 && slot <= leaf.nrecs());

            DBG(<<"insert in leaf at slot " << slot);
            rc = leaf.insert(key, el, slot); 
            if(rc.is_error()) {
                DBG(<<"rc= " << rc);
                leaf.discard(); // force the page out.
                return rc.reset();
            }
            // Keep pinned until logging is done.
        }
        SSMTEST("btree.insert.4");
        /*
         *  Log is on here. Log a logical insert.
         *  If we get a retry, then retry a few times.
         */
        rc = log_btree_insert(leaf, slot, key, el, unique);
        int count=10;
        while (rc.is_error() && (rc.err_num() == eRETRY) && --count > 0) {
            rc = log_btree_insert(leaf, slot, key, el, unique);
        }

        SSMTEST("btree.insert.5");
        if (rc.is_error())  {
            /*
             *  Failed writing log. Manually undo physical insert.
             */
            xct_log_switch_t toggle(OFF);
            DBG(<<"log_btree_insert failed" << rc );

            w_rc_t rc2 = 
                leaf.remove(slot, leaf.is_compressed()); // zkeyed_p::remove
            if(rc2.is_error()) {
                DBG(<<"subsequent remove failed" << rc2 );
                leaf.discard(); // force the page out.
                return rc2.reset();
            }
            // The problem here is that the leaf page changes
            // might *still* not have been logged.
            if(rc.err_num() == eRETRY) {
                leaf.discard(); // force the page out.
            }
            return rc.reset();
        }
        w_assert9(!smlevel_1::log || me()->xct()->is_log_on());
    }
    }
    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e);
    }
    
    return RCOK;
}

/*********************************************************************
 *
 * btree_impl::_remove() removed the exact key,el pair if they are there.
 *  does nothing for remove_all cases
 *
 *********************************************************************/
rc_t
btree_impl::_remove(
    const lpid_t&        root,        // root of btree
    bool                unique, // true if btree is unique
    concurrency_t        cc,        // concurrency control
    const cvec_t&        key,        // which key
    const cvec_t&        el,         // which el
    const bool bIgnoreLatches)        
{
    FUNC(btree_impl::_remove);
    lpid_t                  search_start_pid = root;
    lsn_t                  search_start_lsn = lsn_t::null;
    slotid_t                 slot;
    stid_t                 stid = root.stid();
    rc_t                rc;

    INC_TSTAT(bt_remove_cnt);

    DBGTHRD(<<"_remove:");
#if W_DEBUG_LEVEL > 3
    if(print_remove) {
        cout << "BEFORE _remove" <<endl;
        btree_m::print(root);
    }
#endif

    if(!bIgnoreLatches) {
	get_latches(___s,___e);
    }
    
    {
    tree_latch         tree_root(root, bIgnoreLatches);

    // latch modes
    latch_mode_t traverse_latch;
    latch_mode_t smo_mode;
    latch_mode_t smo_p1mode;
    latch_mode_t smo_p2mode;
    latch_mode_t fix_latch;

again:
    {
    btree_p         leaf;
    lpid_t         leaf_pid;
    lsn_t           leaf_lsn;
    btree_p         parent;
    lsn_t           parent_lsn;
    lpid_t         parent_pid;
    bool         found;
    bool         total_match;
    btree_p         sib; // put this here to hold the latch as long
                            // as we need to.
    lsn_t           sib_lsn;
    lpid_t          sib_pid;

    w_assert9( ! leaf.is_fixed());
    w_assert9( ! parent.is_fixed());

    if(!bIgnoreLatches) {
	if(tree_root.is_fixed()) {
	    check_latches(___s+1,___e+1, ___s+___e+1);
	} else {
	    check_latches(___s,___e, ___s+___e);
	}
    }
    
    DBGTHRD(<<"_remove.do:");

    /*
     *  Walk down the tree.  Traverse doesn't
     *  search the leaf page; it's our responsibility
     *  to check that we're at the correct leaf.
     */

    traverse_latch = LATCH_EX;
    if(bIgnoreLatches) {
	traverse_latch = LATCH_NL;
    }
    W_DO( _traverse(root, search_start_pid,
        search_start_lsn,
        key, el, found, 
        traverse_latch, leaf, parent,
		    leaf_lsn, parent_lsn, bIgnoreLatches) );

    if(!bIgnoreLatches) {
	if(leaf.pid() == root) {
	    check_latches(___s,___e+2, ___s+___e+2);
	} else {
	    check_latches(___s+1,___e+1, ___s+___e+2);
	}
    }
    
    w_assert9(leaf.is_fixed());
    w_assert9(leaf.is_leaf());

    w_assert9(parent.is_fixed());
    w_assert9( parent.is_node() || (parent.is_leaf() &&
                leaf.pid() == root  ));
    w_assert9( parent.is_leaf_parent() || parent.is_leaf());

    /*
     * Deal with SMOs :  traversal checked the nodes, but
     * not the leaf.  We check the leaf for smo bits.
     * (Insert checks for smo and delete bits.)
     *
     * -- these cases are treated in Mohan 3.4 (KVL)
     * and 6.4(IM)
     * KVL:  If delete notices that the leaf page's SM_Bit is 0,
     * then it releases the latch on the parent,
     * otherwise, while holding latches on the parent and the leaf,
     * delete requests a cond'l instant S latch on the tree.
     * (It needs to establish a POSC).  At this point, it 
     * makes reference to the IM paper.
     * IM: If delete notices that the leaf page's SM_Bit is 1,
     * while holding latches on parent and leaf, delete requests a cond'l
     * instant S latch on the tree. If the conditional, instant latch
     * on the tree is successful, delete sets the SM_Bit to 0
     * and releases the latch on the parent.
     * 
     * If the conditional tree latch doesn't work, it request an
     * unconditional tree latch and, once granted, sets the leaf's
     * SM_bit to 0 and restarts with a new search. 
     * Mohan doesn't say anything about the duration of the
     * unconditional tree latch (if conditional fails), so we
     * assume it's still supposed to be instant.  Since we have
     * no such thing as an instant latch, we just unlatch it soon.
     */
    leaf_pid = leaf.pid();
    parent_pid = parent.pid(); // remember for page deletes

    if(leaf.is_smo()) {
        w_error_t::err_num_t rce;
	smo_mode = LATCH_SH;
	smo_p1mode = LATCH_EX;
	if(bIgnoreLatches) {
	    smo_mode = LATCH_NL;
	    smo_p1mode = LATCH_NL;
	}
        rce = tree_root.get_for_smo(true, smo_mode,
                    leaf, smo_p1mode, false, &parent, LATCH_NL, bIgnoreLatches);
        tree_root.unfix(); // Instant latch

        w_assert2(!tree_root.is_fixed() );
        w_assert2(leaf.is_fixed());
        w_assert2( !parent.is_fixed());

        if(rce) {
            if(rce == eRETRY) {
                // see above paragraph: says we do this here
                // even though the lsn changed; then we re-search
                if(leaf.is_smo())     {
#if BTREE_LOG_COMMENT_ON
                    W_DO(log_comment("clr_smo/R1"));
#endif 
                    W_DO( leaf.clr_smo(true) );
                }
                leaf.unfix();
                DBGTHRD(<<"POSC done; ->again TREE LATCH MODE "
                                << int(tree_root.latch_mode())
                                );
                goto again;
            }
            return RC(rce);
        }
        if(leaf.is_smo())     {
#if BTREE_LOG_COMMENT_ON
            W_DO(log_comment("clr_smo/R2"));
#endif 
            W_DO( leaf.clr_smo(true) );
        }
        SSMTEST("btree.remove.1");
    } else {
        // release parent right away
        parent.unfix();
    }

    w_assert2(leaf.is_fixed());
    w_assert2(! leaf.is_smo());

    w_assert2(!parent.is_fixed());
    w_assert2(!tree_root.is_fixed() );

    sib_pid = root; // get vol id, store id
    sib_pid.page = leaf.next();

    /*
     * Verify that we have the right leaf and
     * find the slot in the leaf page.
     */
    uint whatcase;
    W_DO( _satisfy(leaf, key, el, found, 
                total_match, slot, whatcase));

    DBGTHRD(<<"found = " << found
            << " total_match=" << total_match
            << " leaf=" << leaf.pid()
            << " case=" << whatcase
            << " cc=" << int(cc)
            << " slot=" << slot);

    if(cc != t_cc_none) {
        /*
         * Deal with locks. The only time we can avoid
         * locking the next key value is when it's a non-unique
         * index and we witness another entry with the same
         * key.  In order to figure out if we can avoid locking
         * the next key, we have to inspect the next entry.
         */
        lockid_t        nxt_kvl;
        bool                    lock_next=true;
        lock_duration_t this_duration = t_long;
        lock_mode_t     this_mode = EX; // all cases
        lock_duration_t next_duration = t_long; // all cases
        lock_mode_t     next_mode = EX; // all cases
        bool            is_last_key = false;
        bool                 next_on_next_page = false;
                

        if(cc == t_cc_none || cc == t_cc_modkvl ) {
            lock_next = false;
            this_duration = t_long;

        } else switch(whatcase) {
            case m_not_found_end_of_file: 
                mk_kvl_eof(cc, nxt_kvl, stid);
                lock_next = true;
                break;
            case m_not_found_page_is_empty: 
                // smo in progress -- should have been caught above
                w_assert9(0); 
                break;

            case m_not_found_end_of_non_empty_page: 
            case m_satisfying_key_found_same_page: {
                /*
                 * Do we need to lock the next key-value?
                 * Mohan says yes if: 1) unique index, or 2)
                 * this is the last entry for this key. We
                 * can only tell if this is the last entry
                 * for this key if the next entry is different.
                 * As with Mohan, we don't bother with the question
                 * of whether there is a prior entry with this key.
                 */

                if (!total_match && slot < leaf.nrecs())  {
                    /* leaf.slot is next key value */
                    DBG(<<"");
                    btrec_t r(leaf, slot);
                    mk_kvl(cc, nxt_kvl, stid, unique, r);
                    lock_next = true;

                } else if (slot < leaf.nrecs() - 1)  {
                    /* next key value exists on same page */
                    DBG(<<"");
                    w_assert9(total_match);
                    btrec_t s(leaf, slot);
                    btrec_t r(leaf, slot + 1);
                    mk_kvl(cc, nxt_kvl, stid, unique, r);
                    if(unique) {
                        lock_next = true;
                    } else if(r.key() == s.key()) {
                        lock_next = false;
                    } else {
                        lock_next = true;
                        if(slot > 0) {
                            // check prior key
                            btrec_t q(leaf, slot - 1);
                            if(q.key() != s.key()) {
                                is_last_key = true;
                            }
                        }
                    }
                } else {
                    /* next key might be on next page */
                    if (! leaf.next())  {
                        DBG(<<"");
                        mk_kvl_eof(cc, nxt_kvl, stid);
                        lock_next = true;
                    } else {
                        // "following" a link here means fixing the page
                        INC_TSTAT(bt_links);
                        DBG(<<"");

			fix_latch = LATCH_SH;
			if(bIgnoreLatches) {
			    fix_latch = LATCH_NL;
			}
                        W_DO( sib.fix(sib_pid, fix_latch) );
                        sib_lsn = sib.lsn();
                        if(sib.nrecs() > 0) {
                            next_on_next_page =true;
                            btrec_t r(sib, 0);
                            mk_kvl(cc, nxt_kvl, stid, unique, r);

                            if(unique || !total_match) {
                                lock_next = true;
                            } else {
                                btrec_t s(leaf, slot);
                                if(r.key() == s.key()) {
                                    lock_next = false;
                                } else {
                                    lock_next = true;
                                    if(slot > 0) {
                                        // check prior key
                                        btrec_t q(leaf, slot - 1);
                                        if(q.key() != s.key()) {
                                            is_last_key = true;
                                        }
                                    }
                                }
                            }
                        } else {
                            /* empty page -- ongoing deletion 
                             * wait for POSC
                             */
                            DBG(<<"");
			    smo_mode = LATCH_SH;
			    smo_p1mode = LATCH_EX;
			    if(bIgnoreLatches) {
				smo_mode = LATCH_NL;
				smo_p1mode = LATCH_NL;
			    }
                            w_error_t::err_num_t rce;
                            rce = tree_root.get_for_smo(true, smo_mode,
                                leaf, smo_p1mode, false, &sib, LATCH_NL, bIgnoreLatches);
                            tree_root.unfix(); // Instant latch

                            w_assert2(!tree_root.is_fixed() );
                            w_assert2(leaf.is_fixed());
                            if(rce) { return RC(rce); }
                            DBGTHRD(<<"POSC done; ->again TREE LATCH MODE "
                                << int(tree_root.latch_mode())
                                );
                            goto again;

                        } // await posc
                    } //looking at next page
                }

                /*
                 * Even if we didn't see a next key, we might not be
                 * removing the last instance because we didn't check prior
                 * keys on prior pages.  
                 */
                if(is_last_key || unique) {
                    this_duration = t_instant;
                }
            } break;

        } // switch

        /*
         * if we return eNOTFOUND, the mode for
         * the next  key-value should as well be SH.
         * NB: might want different semantics for t_cc_modkvl
         * here.
         */
        if(!total_match){
            next_duration = t_long;
            next_mode = SH;
        }

        w_assert9(leaf.is_fixed());

        if (lock_next) {
            DBG(<<"lock next");
            /* conditional commit duration */
            rc = lm->lock(nxt_kvl, next_mode, next_duration, WAIT_IMMEDIATE);
            if (rc.is_error())  {
                DBG(<<"rc=" << rc);
                w_assert9((rc.err_num() == eLOCKTIMEOUT) || (rc.err_num() == eDEADLOCK));

                leaf.unfix();
                sib.unfix();
                W_DO( lm->lock(nxt_kvl, next_mode, next_duration) );

		fix_latch = LATCH_EX;
		if(bIgnoreLatches) {
		    fix_latch = LATCH_NL;
		}
                W_DO( leaf.fix(leaf_pid, fix_latch) );
                if(leaf.lsn() != leaf_lsn) {
                    leaf.unfix();
                    DBG(<<"-->again");
                    goto again;
                }
                if(sib_pid.page)  {
		    fix_latch = LATCH_SH;
		    if(bIgnoreLatches) {
			fix_latch = LATCH_NL;
		    }
                    W_DO( sib.fix(sib_pid, fix_latch) );
                    if(sib.lsn() != sib_lsn) {
                        leaf.unfix();
                        sib.unfix();
                        DBG(<<"-->again");
                        goto again;
                    }
                }
            }

            /*
             * IM paper says: if the next key lock is granted, then if 
             * the next key were on a different page from the one
             * on which the deletion is to be performed,
             * then the latch on the former is released.  This is not
             * in the KVL paper.
             */
            if(next_on_next_page) {
                sib.unfix();
            }
        } // locking next key value 

        /*
         *  Create KVL lock for to-be-deleted key value
         */
        lockid_t kvl;
        mk_kvl(cc, kvl, stid, unique, key, el);


#if W_DEBUG_LEVEL > 2
        w_assert3(leaf.is_fixed());
        if(sib.is_fixed()) {
            w_assert3(sib_pid != leaf_pid);
        }
#endif 
        rc = lm->lock(kvl, this_mode, this_duration, WAIT_IMMEDIATE);
        if (rc.is_error())  {
            DBG(<<"rc=" << rc);
            w_assert9((rc.err_num() == eLOCKTIMEOUT) || (rc.err_num() == eDEADLOCK));

            leaf.unfix();
            sib.unfix();
            W_DO( lm->lock(kvl, this_mode, this_duration) );

	    fix_latch = LATCH_EX;
	    if(bIgnoreLatches) {
		fix_latch = LATCH_NL;
	    }
            W_DO( leaf.fix(leaf_pid, fix_latch) );
            if(leaf.lsn() != leaf_lsn) {
                leaf.unfix();
                DBG(<<"-->again");
                goto again;
            }
            /* We don't try to re-fix the sibling here --
             * we'll let the page deletion code do it if
             * it needs to do so
             */
        }
    } // all locking

    w_assert9(leaf.is_fixed());

    if(!total_match) {
        return RC(eNOTFOUND);
    }

    /* 
     * We've hung onto the sibling in case we're
     * going to have to do a page deletion
     */

    {   /* 
         * Deal with "boundary keys".
         * If the key to be deleted is the smallest
         * or the largest one on the page, latch the
         * whole tree.  According to the IM paper, this
         * latch has to be grabbed whether or not this is
         * the last instance of the key.
         * 
         * NB: there could be a difference between "key" and "key-elem"
         * in this discussion.  Just because there's another
         * entry on the page doesn't mean that it's got a different
         * key. 
         * From the discussion in IM (with the hypothetical crash
         * with deletion of a boundary key, tree latch not held),
         * along with the treatment of unique/non-unique trees in
         * KVL/delete, we choose the following interpretation:
         * If this is a non-unique index and we're not removing
         * the last entry for this key, the to-be-re-inserted-key-
         * in-event-of-crash *IS* bound to this page by the other
         * entries with the same key, therefore, we treat it the
         * same as the case in which there's another key on the same
         * page.
         */
        if(slot == 0 || slot == leaf.nrecs()-1) {
            // Boundary key
            rc_t rc;

            /* 
             * Conditionally latch the tree in
             *  -EX mode if the key to be deleted is the ONLY key
             *     on the page, 
             *  -SH mode otherwise
             * This is held for the duration of the delete.
             */
            latch_mode_t  tree_latch_mode = 
                (leaf.nrecs() == 1)? LATCH_EX: LATCH_SH;

            SSMTEST("btree.remove.2");

	    smo_p1mode = LATCH_EX;
	    if(bIgnoreLatches) {
		tree_latch_mode = LATCH_NL;
		smo_p1mode = LATCH_NL;
	    }
            w_error_t::err_num_t rce = 
                tree_root.get_for_smo(true, tree_latch_mode,
                    leaf, smo_p1mode, sib.is_fixed(), &sib, sib.latch_mode(), bIgnoreLatches);

            w_assert2(tree_root.is_fixed());
            w_assert2(leaf.is_fixed());
            w_assert2(! parent.is_fixed());

            if(rce) {
                tree_root.unfix();
                if(rce == eRETRY) {
                    leaf.unfix();
                    DBGTHRD(<<"-->again TREE LATCH MODE "
                                << int(tree_root.latch_mode())
                                );
                    goto again;
                }
                return RC(rce);
            }
            w_assert2(tree_root.is_fixed());
        }
    } // handle boundary keys

    w_assert9(leaf.is_fixed());

    {   /* 
         * Ok - all necessary latches and locks are held.
         * Do the delete.
         *  
         *  Turn off logging and perform physical remove.
         */
        w_assert9(!smlevel_1::log || me()->xct()->is_log_on());
        {
            xct_log_switch_t toggle(OFF);
            DBGTHRD(<<" leaf: " << leaf.pid() << " removing slot " << slot);

            W_DO( leaf.remove(slot, leaf.is_compressed() ) ); // zkeyed_p::remove
            DBGTHRD(<<" removed slot " << slot);
        }
        w_assert9(!smlevel_1::log || me()->xct()->is_log_on());

        SSMTEST("btree.remove.3");

        /*
         *  Log is on here. Log a logical remove.
         */
        rc_t rc = log_btree_remove(leaf, slot, key, el, unique);
        int count=10;
        while (rc.is_error() && (rc.err_num() == eRETRY) && (--count > 0)) {
            rc = log_btree_remove(leaf, slot, key, el, unique);
        }
        SSMTEST("btree.remove.4");
        if (rc.is_error())  {
            /*
             *  Failed writing log. Manually undo physical remove.
             */
            xct_log_switch_t toggle(OFF);
            DBG(<<"Re-insert in leaf at slot " << slot);
            w_rc_t rc2 = leaf.insert(key, el, slot);
            if(rc2.is_error()) {
                DBG(<<"rc= " << rc);
                leaf.discard(); // force the page out.
                return rc2.reset();
            }
            // The problem here is that the leaf page changes
            // might *still* not have been logged.
            if(rc.err_num() == eRETRY) {
                leaf.discard(); // force the page out.
            }
            return rc.reset();
        }
        w_assert9(!smlevel_1::log || me()->xct()->is_log_on());
        SSMTEST("btree.remove.5");

        DBGTHRD(<<" logged ");

        /* 
         * Mohan(IM): 
         * After the key is deleted, set the delete bit if the
         * tree latch is not held.  This should translate to 
         * the case in which a lesser and a greater key
         * exist on the same page, i.e., this is not a boundary 
         * key.
         */
        if( ! tree_root.is_fixed() ) {
            // delete bit is cleared by insert
            w_assert9(leaf.is_leaf());
            SSMTEST("btree.remove.6");
#if BTREE_LOG_COMMENT_ON
            W_DO(log_comment("set_delete"));
#endif 
            DBGTHRD(<<" set_delete ");

            // Tree latch not held: have to compensate
            W_DO( leaf.set_delete() );
        }

        SSMTEST("btree.remove.7");
        if (leaf.nrecs() == 0)  {
#if BTREE_LOG_COMMENT_ON
            W_DO(log_comment("begin remove leaf"));
#endif 
            /*
             *  Remove empty page.
             *  First, try to get a conditional latch on the
             *  tree, while holding the other latches.  If
             *  not successful, unlatch the pages, latch the 
             *  tree, and re-latch the pages.  We're going
             *  to hold this until we're done with the SMO
             *  (page deletion).
             */
	    smo_mode = LATCH_EX;
	    smo_p1mode = LATCH_EX;
	    smo_p2mode = LATCH_EX;
	    if(bIgnoreLatches) {
		smo_mode = LATCH_NL;
		smo_p1mode = LATCH_NL;
		smo_p2mode = LATCH_NL;
	    }
            w_error_t::err_num_t rce;
            rce = tree_root.get_for_smo(true, smo_mode,
                    leaf, smo_p1mode,
                    true, &sib, smo_p2mode, bIgnoreLatches);
            // if sib wasn't latched before calling for the latch,
            // it won't be latched now.
            DBGTHRD(<<" rc= " << rc);

            w_assert2(tree_root.is_fixed());
            w_assert2(! parent.is_fixed());
            w_assert2(leaf.is_fixed());

            if(rce) {
                if(rce == eRETRY) {
                    /* This shouldn't happen, I don't think...*/
                    leaf.unfix();
                    sib.unfix();
                    SSMTEST("btree.remove.8");
                    DBGTHRD(<<"-->again TREE LATCH MODE "
                                << int(tree_root.latch_mode())
                                );
                    goto again;
                }
                tree_root.unfix();
                return RC(rce);
            }
        
            SSMTEST("btree.remove.9");
            if(sib_pid.page) {
                /*
                 * I think we are safe just to wait for these
                 * latches because we already have the tree latch
                 * and we only travel right from the leaf -- never left.
                 */
		fix_latch = LATCH_EX;
		if(bIgnoreLatches) {
		    fix_latch = LATCH_NL;
		}
                if(!sib.is_fixed()) {
                    W_DO(sib.fix(sib_pid, fix_latch));
                } else if (!bIgnoreLatches && sib.latch_mode() != LATCH_EX) {
#if COMMENT
                    // Took out because the buffer cleaner might
                    // have this page latched, but the buffer 
                    // cleaner *will* free it soon - it never blocks
                    // on latches.
                    bool would_block = false;
                    W_DO(sib.upgrade_latch_if_not_block(would_block));
                    w_assert9(!would_block);
#else
                    sib.upgrade_latch(LATCH_EX);
#endif /* COMMENT */
                }
            }
            w_assert9(leaf.is_fixed());
            DBGTHRD(<<" calling unlink&propogate ");
            W_DO(leaf.unlink_and_propagate(key, el, sib, parent_pid, 
					   tree_root.pid(), bIgnoreLatches));
            SSMTEST("btree.remove.10");

#if BTREE_LOG_COMMENT_ON
            W_DO(log_comment("end remove leaf"));
#endif 
        } // removal of empty page
    } // deletion of the key
    }
    }
    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e);
    }
    
    return RCOK;
}

/*********************************************************************
 *
 *  btree_impl::_lookup(...)
 *
 *  This is Mohan's "fetch" operation.
 * TODO(correctness) : we end up with commit-duration IX locks, so we have
 * to update log_prepare so that it logs and restores those locks also.
 *
 *********************************************************************/

rc_t
btree_impl::_lookup(
    const lpid_t&       root,        // I-  root of btree
    bool                unique, // I-  true if btree is unique
    concurrency_t       cc,        // I-  concurrency control
    const cvec_t&       key,        // I-  key we want to find
    const cvec_t&       elem,        // I-  elem we want to find (may be null)
    bool&               found,  // O-  true if key is found
    bt_cursor_t*        cursor, // I/o - put result here OR
    void*               el,        // I/o-  buffer to put el if !cursor
    smsize_t&           elen,        // IO- size of el if !cursor
    const bool bIgnoreLatches)                
{
    FUNC(btree_impl::_lookup);
    
    if(!bIgnoreLatches) {
	get_latches(___s,___e); 
    }
    
    rc_t        rc;

    INC_TSTAT(bt_find_cnt);
    stid_t         stid = root.stid();

    {
        tree_latch   tree_root(root, bIgnoreLatches); // for latching the whole tree
        lpid_t       search_start_pid = root;
        lsn_t        search_start_lsn = lsn_t::null;
	bool         first_restart = true;

	// latch modes
	latch_mode_t traverse_latch;
	latch_mode_t smo_mode;
	latch_mode_t smo_p1mode;
	latch_mode_t fix_latch;

	if(!bIgnoreLatches) {
	    check_latches(___s,___e, ___s+___e); 
	}
	
    again:
        DBGTHRD(<<"_lookup.again");


        {   // open scope here so that every restart
            // causes pages to be unpinned.
        bool                  total_match = false;

        btree_p                leaf; // first-leaf
        btree_p                p2; // possibly needed for 2nd leaf
        btree_p*               child = &leaf;        // child points to leaf or p2
        btree_p                parent; // parent of leaves

        lsn_t                  leaf_lsn, parent_lsn;
        slotid_t               slot;
        lockid_t               kvl;

        found = false;
	
        /*
         *  Walk down the tree.  Traverse doesn't
         *  search the leaf page; it's our responsibility
         *  to check that we're at the correct leaf.
         */
	traverse_latch = LATCH_SH;
	if(bIgnoreLatches) {
	    traverse_latch = LATCH_NL;
	}
        W_DO( _traverse(root, 
                search_start_pid, 
                search_start_lsn,
                key, elem, found, 
                traverse_latch, leaf, parent,
			leaf_lsn, parent_lsn, bIgnoreLatches) );

	if(!bIgnoreLatches) {
	    check_latches(___s+2,___e, ___s+___e+2); 
	}
	
        w_assert9(leaf.is_fixed());
        w_assert9(leaf.is_leaf());
        
        w_assert9(parent.is_fixed());
        w_assert9(parent.is_node() || (parent.is_leaf() &&
                leaf.pid() == root  ));
        w_assert9( parent.is_leaf_parent() || parent.is_leaf());


        /* 
         * if we re-start a traversal,
	 * we'll start with the parent for the first retraversal
	 * then if that does not work we'll restart from the root
         */
	if(first_restart) {
	    search_start_pid = parent.pid();
	    search_start_lsn = parent.lsn();
	    first_restart = false;
	} else {
	    search_start_pid = root;
	    search_start_lsn = lsn_t::null;
	}
	
        /*
         * verify that we're at correct page: search for smallest
         * satisfying key, or if not found, the next key.
         * In this case, we don't have an elem; we use null.
         * NB: <key,null> *could be in the tree* 
         * TODO(correctness) cope with null 
         * * in the tree. (Write a test for it).
         */
        uint whatcase;
        W_DO(_satisfy(leaf, key, elem, found, 
                    total_match, slot, whatcase));

        if(cursor && cursor->is_backward() && !total_match) {
            // we're pointing at the slot after the greatest element, which
            // is where the key would be inserted (shoving this element up) 
            if(slot>0) {
                // ?? what to do if this is slot 0 and
                // there's a previous page?
                slot --;
                DBG(<<" moving slot back one: slot=" << slot);
            }
            if(slot < leaf.nrecs()) {
                whatcase = m_satisfying_key_found_same_page;
            } 
        }
        DBGTHRD(<<"found = " << found 
                << " total_match=" << total_match
                << " leaf=" << leaf.pid() 
                << " case=" << whatcase
                << " slot=" << slot);

        /* 
         * Deal with SMOs -- these cases are treated in Mohan 3.1
         */
        switch (whatcase) {
        case m_satisfying_key_found_same_page:{
            // case 1:
            // found means we found the key we're seeking
            // !found means the satisfying key is the next key
            // w_assert9(found);
            parent.unfix();
            }break;

        case m_not_found_end_of_non_empty_page: {

            // case 2: possible smo in progress
            // but leaf is not empty, and it has
            // a next page -- that much is assured by
            // _satisfy

            w_assert9(leaf.nrecs());
            w_assert9(leaf.next());

            /*
             * Mohan: unlatch the parent and latch the successor.
             * If the successor is empty or does not have a
             * satisfying key, unlatch both pages and request
             * the tree latch in S mode, then restart the search
             * from the parent.
             */
            parent.unfix();

            lpid_t pid = root; // get volume, store part
            pid.page = leaf.next();

            INC_TSTAT(bt_links);
	    fix_latch = LATCH_SH;
	    if(bIgnoreLatches) {
		fix_latch = LATCH_NL;
	    }
            W_DO( p2.fix(pid, fix_latch) );
            /* 
             * does successor have a satisfying key?
             */
            slot = -1;
            W_DO( _satisfy(p2, key, elem, found, total_match, 
                slot, whatcase));

            DBGTHRD(<<"found = " << found 
                << " total_match=" << total_match
                << " leaf=" << leaf.pid() 
                << " case=" << whatcase
                << " slot=" << slot);

            w_assert9(whatcase != m_not_found_end_of_file);
            if(whatcase == m_satisfying_key_found_same_page) {
                /* 
                 * Mohan: If a satisfying key is found on the 
                 * 2nd leaf, (SM bit may be 1), unlatch the first leaf
                 * immediately, provided the found key is not the very
                 * first key in the 2nd leaf.
                 */
                if(slot > 0) {
                    leaf.unfix();
                }
                child =  &p2;
            } else {
                /* no satisfying key; page could be empty */

		smo_mode = LATCH_SH;
		smo_p1mode = LATCH_SH;
		if(bIgnoreLatches) {
		    smo_mode = LATCH_NL;
		    smo_p1mode = LATCH_NL;
		}
                w_error_t::err_num_t MAYBE_UNUSED rce;
                // unconditional
                rce = tree_root.get_for_smo(false, smo_mode,
                        leaf, smo_p1mode, false, &p2, LATCH_NL, bIgnoreLatches);
                //eRETRY means we need to restart the search
                //but we're going to restart it ANYWAY.
                //TODO look this case up in the paper and document it here
                //filed in GNATS 137
#warning Edge case?

                tree_root.unfix(); // instant latch

                DBGTHRD(<<"-->again TREE LATCH MODE "
                            << int(tree_root.latch_mode())
                            );
                /* 
                 * restart the search from the parent
                 */
                DBG(<<"-->again NO TREE LATCH");
                goto again;
            }
            }break;

        case m_not_found_end_of_file:{
            // case 0: end of file
            // Lock the special EOF value
            slot = -1;
            if(cursor) {
                cursor->keep_going = false;
                cursor->free_rec();
            }

            w_assert9( !total_match ); // but found (key) could be true

           } break;

        case m_not_found_page_is_empty: {
            // case 3: empty page: smo going on
            // or it's an empty index.
            w_assert9(whatcase == m_not_found_page_is_empty);
            w_assert9(leaf.nrecs() == 0);

            // Must be empty index or a deleted leaf page
            w_assert1(leaf.is_smo() || root == leaf.pid());

            if(leaf.is_smo()) {
		smo_mode = LATCH_SH;
		smo_p1mode = LATCH_SH;
		if(bIgnoreLatches) {
		    smo_mode = LATCH_NL;
		    smo_p1mode = LATCH_NL;
		}
                // unconditional
                w_error_t::err_num_t rce;
                rce = tree_root.get_for_smo(false, smo_mode,
                        leaf, smo_p1mode, false, &parent, LATCH_NL, bIgnoreLatches);
                tree_root.unfix();
                if(rce && (rce != eRETRY)) {
                    return RC(rce);
                }
                
                DBGTHRD(<<"-->again TREE LATCH MODE "
                            << int(tree_root.latch_mode())
                            );
                goto again;

            } else {
                slot = -1;
                whatcase = m_not_found_end_of_file;
            }
            }break;

        default:
            W_FATAL_MSG(fcINTERNAL, << "bad switch value for case :" << whatcase );
            break;
        } // switch

        if(cursor) {
            // starting condition
            cursor->update_lsn(*child);
        }

        /*
         *  Get a handle on the record to 
         *  grab the key-value locks, as well as to
         *  return the element the caller seeks.
         *
         *  At this point, we don't know if the entry
         *  we're looking at is precisely the one
         *  we looked up, or if it's the next one.
         */

        btrec_t rec;

        if(slot < 0) {
            w_assert9(whatcase == m_not_found_end_of_file);
            if(cc > t_cc_none) mk_kvl_eof(cc, kvl, stid);
        } else {
            w_assert9(slot < child->nrecs());
            if(cursor) {
                // does the unscramble
                W_DO(cursor->make_rec(*child, slot));
            }
            // else does not unscramble 
            
            // Prepare rec for mk_kvl:
            rec.set(*child, slot);
            if(cc > t_cc_none) mk_kvl(cc, kvl, stid, unique, rec);
        }

        if( (!found && !total_match) &&
            (cc == t_cc_modkvl) ) {
            ; /* we don't want to lock next */
        } else if (cc != t_cc_none)  {
            /*
             *  Conditionally lock current/next entry.
             */
            lock_mode_t mode = cursor? cursor->mode() : SH;
            rc = lm->lock(kvl, mode, t_long, WAIT_IMMEDIATE);
            if (rc.is_error()) {
                DBG(<<"rc=" << rc);
                w_assert9((rc.err_num() == eLOCKTIMEOUT) || (rc.err_num() == eDEADLOCK));

                /*
                 *  Failed to get lock immediately. Unfix pages
                 *  wait for the lock.
                 */
                lsn_t lsn = child->lsn();
                lpid_t pid = child->pid();
                leaf.unfix();
                child->unfix();
                parent.unfix(); // NEH: added (shore-mt gnats #69). 
                // Note, unfix checks for fix before unfixing.

                W_DO( lm->lock(kvl, mode, t_long) );

                /*
                 *  Got the lock. Fix child. If child has
                 *  changed (lsn does not match) then start the 
                 *  search over.  This is a gross simplification
                 *  of Mohan's treatment.
                 *  Re-starting  from the root is safe only because
                 *  before we got into btree_m, we'll have grabbed
                 *  an IS lock on the whole index, at a minimum,
                 *  meaning there's no chance of the whole index being
                 *  destroyed in the meantime.
                 */
		fix_latch = LATCH_SH;
		if(bIgnoreLatches) {
		    fix_latch = LATCH_NL;
		}
                W_DO( child->fix(pid, fix_latch) );
                if (lsn == child->lsn() && child == &leaf)  {
                    /* do nothing */;
                } else {
                    /* filed as GNATS 134 */
                    /* BUGBUG: A concurrency performance bug, actually:
                     * Mohan says if the subsequent re-search
                     * finds a different key (next-key), we should
                     * unlock the old kvl and lock the new kvl.
                     * We're not doing that.
                     */
                    DBGTHRD(<<"->again");
                    goto again;        // retry
                }
            } // acquiring locks
        } // if any locking needed

        if (found) {
            // rec is the key that we're looking up
            if(!cursor) {
                // Copy the element 
                // assume caller provided space
                if (el) {
                    if (elen < rec.elen())  {
                        DBG(<<"RECWONTFIT");
                        return RC(eRECWONTFIT);
                    }
                    elen = rec.elen();
                    rec.elem().copy_to(el, elen);
                }
            }
        } else {
            // rec will be the next record if !found
            // w_assert9(!rec);
        }

        }
        // Destructors did or will unfix all pages.
    }
    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e); 
    }
    return RCOK;
}


/*********************************************************************
 *
 *  btree_impl::_update(...)
 *
 * Template taken from lookup since this should be the lookup function
 * changed at the end
 *
 *********************************************************************/

rc_t
btree_impl::_update(
    const lpid_t&       root,        // I-  root of btree
    bool                unique, // I-  true if btree is unique
    concurrency_t       cc,        // I-  concurrency control
    const cvec_t&       key,        // I-  key we want to find
    const cvec_t&       old_el,    // I-  element we want to update
    const cvec_t&       new_el,    // I-  new value of the element
    bool&               found,  // O-  true if key is found
    const bool bIgnoreLatches)                
{
    FUNC(btree_impl::_update);
    
    if(!bIgnoreLatches) {
	get_latches(___s,___e); 
    }
    
    rc_t        rc;

    // TODO: add this to stats
    INC_TSTAT(bt_update_cnt);
    stid_t         stid = root.stid();

    {
        tree_latch   tree_root(root, bIgnoreLatches); // for latching the whole tree
        lpid_t       search_start_pid = root;
        lsn_t        search_start_lsn = lsn_t::null;

	// latch modes
	latch_mode_t traverse_latch;
	latch_mode_t smo_mode;
	latch_mode_t smo_p1mode;
	latch_mode_t fix_latch;

	if(!bIgnoreLatches) {
	    check_latches(___s,___e, ___s+___e); 
	}
	
    again:
        DBGTHRD(<<"_lookup.again");


        {   // open scope here so that every restart
            // causes pages to be unpinned.
        bool                  total_match = false;

        btree_p                leaf; // first-leaf
        btree_p                p2; // possibly needed for 2nd leaf
        btree_p*               child = &leaf;        // child points to leaf or p2
        btree_p                parent; // parent of leaves

        lsn_t                  leaf_lsn, parent_lsn;
        slotid_t               slot;
        lockid_t               kvl;

        found = false;

        /*
         *  Walk down the tree.  Traverse doesn't
         *  search the leaf page; it's our responsibility
         *  to check that we're at the correct leaf.
         */
	traverse_latch = LATCH_SH;
	if(bIgnoreLatches) {
	    traverse_latch = LATCH_NL;
	}
        W_DO( _traverse(root, 
                search_start_pid, 
                search_start_lsn,
                key, old_el, found, 
                traverse_latch, leaf, parent,
			leaf_lsn, parent_lsn, bIgnoreLatches) );

	if(!bIgnoreLatches) {
	    check_latches(___s+2,___e, ___s+___e+2); 
	}
	
        w_assert9(leaf.is_fixed());
        w_assert9(leaf.is_leaf());
        
        w_assert9(parent.is_fixed());
        w_assert9(parent.is_node() || (parent.is_leaf() &&
                leaf.pid() == root  ));
        w_assert9( parent.is_leaf_parent() || parent.is_leaf());


        /* 
         * if we re-start a traversal, we'll start with the parent
         * in most or all cases:
         */
        search_start_pid = parent.pid();
        search_start_lsn = parent.lsn();

        /*
         * verify that we're at correct page: search for smallest
         * satisfying key, or if not found, the next key.
         * In this case, we don't have an old_elem; we use null.
         * NB: <key,null> *could be in the tree* 
         * TODO(correctness) cope with null 
         * * in the tree. (Write a test for it).
         */
        uint whatcase;
        W_DO(_satisfy(leaf, key, old_el, found, 
                    total_match, slot, whatcase));

        DBGTHRD(<<"found = " << found 
                << " total_match=" << total_match
                << " leaf=" << leaf.pid() 
                << " case=" << whatcase
                << " slot=" << slot);

        /* 
         * Deal with SMOs -- these cases are treated in Mohan 3.1
         */
        switch (whatcase) {
        case m_satisfying_key_found_same_page:{
            // case 1:
            // found means we found the key we're seeking
            // !found means the satisfying key is the next key
            // w_assert9(found);
            parent.unfix();
            }break;

        case m_not_found_end_of_non_empty_page: {

            // case 2: possible smo in progress
            // but leaf is not empty, and it has
            // a next page -- that much is assured by
            // _satisfy

            w_assert9(leaf.nrecs());
            w_assert9(leaf.next());

            /*
             * Mohan: unlatch the parent and latch the successor.
             * If the successor is empty or does not have a
             * satisfying key, unlatch both pages and request
             * the tree latch in S mode, then restart the search
             * from the parent.
             */
            parent.unfix();

            lpid_t pid = root; // get volume, store part
            pid.page = leaf.next();

            INC_TSTAT(bt_links);
	    fix_latch = LATCH_SH;
	    if(bIgnoreLatches) {
		fix_latch = LATCH_NL;
	    }
            W_DO( p2.fix(pid, fix_latch) );
            /* 
             * does successor have a satisfying key?
             */
            slot = -1;
            W_DO( _satisfy(p2, key, old_el, found, total_match, 
                slot, whatcase));

            DBGTHRD(<<"found = " << found 
                << " total_match=" << total_match
                << " leaf=" << leaf.pid() 
                << " case=" << whatcase
                << " slot=" << slot);

            w_assert9(whatcase != m_not_found_end_of_file);
            if(whatcase == m_satisfying_key_found_same_page) {
                /* 
                 * Mohan: If a satisfying key is found on the 
                 * 2nd leaf, (SM bit may be 1), unlatch the first leaf
                 * immediately, provided the found key is not the very
                 * first key in the 2nd leaf.
                 */
                if(slot > 0) {
                    leaf.unfix();
                }
                child =  &p2;
            } else {
                /* no satisfying key; page could be empty */

		smo_mode = LATCH_SH;
		smo_p1mode = LATCH_SH;
		if(bIgnoreLatches) {
		    smo_mode = LATCH_NL;
		    smo_p1mode = LATCH_NL;
		}
                w_error_t::err_num_t MAYBE_UNUSED rce;
                // unconditional
                rce = tree_root.get_for_smo(false, smo_mode,
                        leaf, smo_p1mode, false, &p2, LATCH_NL, bIgnoreLatches);
                //eRETRY means we need to restart the search
                //but we're going to restart it ANYWAY.
                //TODO look this case up in the paper and document it here
                //filed in GNATS 137
#warning Edge case?

                tree_root.unfix(); // instant latch

                DBGTHRD(<<"-->again TREE LATCH MODE "
                            << int(tree_root.latch_mode())
                            );
                /* 
                 * restart the search from the parent
                 */
                DBG(<<"-->again NO TREE LATCH");
                goto again;
            }
            }break;

        case m_not_found_end_of_file:{
            // case 0: end of file
            // Lock the special EOF value
            slot = -1;
	    w_assert9( !total_match ); // but found (key) could be true

           } break;

        case m_not_found_page_is_empty: {
            // case 3: empty page: smo going on
            // or it's an empty index.
            w_assert9(whatcase == m_not_found_page_is_empty);
            w_assert9(leaf.nrecs() == 0);

            // Must be empty index or a deleted leaf page
            w_assert1(leaf.is_smo() || root == leaf.pid());

            if(leaf.is_smo()) {
		smo_mode = LATCH_SH;
		smo_p1mode = LATCH_SH;
		if(bIgnoreLatches) {
		    smo_mode = LATCH_NL;
		    smo_p1mode = LATCH_NL;
		}
                // unconditional
                w_error_t::err_num_t rce;
                rce = tree_root.get_for_smo(false, smo_mode,
                        leaf, smo_p1mode, false, &parent, LATCH_NL, bIgnoreLatches);
                tree_root.unfix();
                if(rce && (rce != eRETRY)) {
                    return RC(rce);
                }
                
                DBGTHRD(<<"-->again TREE LATCH MODE "
                            << int(tree_root.latch_mode())
                            );
                goto again;

            } else {
                slot = -1;
                whatcase = m_not_found_end_of_file;
            }
            }break;

        default:
            W_FATAL_MSG(fcINTERNAL, << "bad switch value for case :" << whatcase );
            break;
        } // switch

        /*
         *  Get a handle on the record to 
         *  grab the key-value locks, as well as to
         *  return the element the caller seeks.
         *
         *  At this point, we don't know if the entry
         *  we're looking at is precisely the one
         *  we looked up, or if it's the next one.
         */

        btrec_t rec;

        if(slot < 0) {
            w_assert9(whatcase == m_not_found_end_of_file);
            if(cc > t_cc_none) mk_kvl_eof(cc, kvl, stid);
        } else {
            w_assert9(slot < child->nrecs());          
            // Prepare rec for mk_kvl:
            rec.set(*child, slot);
            if(cc > t_cc_none) mk_kvl(cc, kvl, stid, unique, rec);
        }

        if( (!found && !total_match) &&
            (cc == t_cc_modkvl) ) {
            ; /* we don't want to lock next */
        } else if (cc != t_cc_none)  {
            /*
             *  Conditionally lock current/next entry.
             */
            lock_mode_t mode = SH;
            rc = lm->lock(kvl, mode, t_long, WAIT_IMMEDIATE);
            if (rc.is_error()) {
                DBG(<<"rc=" << rc);
                w_assert9((rc.err_num() == eLOCKTIMEOUT) || (rc.err_num() == eDEADLOCK));

                /*
                 *  Failed to get lock immediately. Unfix pages
                 *  wait for the lock.
                 */
                lsn_t lsn = child->lsn();
                lpid_t pid = child->pid();
                leaf.unfix();
                child->unfix();
                parent.unfix(); // NEH: added (shore-mt gnats #69). 
                // Note, unfix checks for fix before unfixing.

                W_DO( lm->lock(kvl, mode, t_long) );

                /*
                 *  Got the lock. Fix child. If child has
                 *  changed (lsn does not match) then start the 
                 *  search over.  This is a gross simplification
                 *  of Mohan's treatment.
                 *  Re-starting  from the root is safe only because
                 *  before we got into btree_m, we'll have grabbed
                 *  an IS lock on the whole index, at a minimum,
                 *  meaning there's no chance of the whole index being
                 *  destroyed in the meantime.
                 */
		fix_latch = LATCH_SH;
		if(bIgnoreLatches) {
		    fix_latch = LATCH_NL;
		}
                W_DO( child->fix(pid, fix_latch) );
                if (lsn == child->lsn() && child == &leaf)  {
                    /* do nothing */;
                } else {
                    /* filed as GNATS 134 */
                    /* BUGBUG: A concurrency performance bug, actually:
                     * Mohan says if the subsequent re-search
                     * finds a different key (next-key), we should
                     * unlock the old kvl and lock the new kvl.
                     * We're not doing that.
                     */
                    DBGTHRD(<<"->again");
                    goto again;        // retry
                }
            } // acquiring locks
        } // if any locking needed

	// pin: this is the only different part actually, you can have a more clever implementation here
        if (found) {
	    W_DO( child->overwrite(slot+1, rec.klen()+sizeof(int4_t), new_el) );
        } else {
            // rec will be the next record if !found
            // w_assert9(!rec);
        }

        }
        // Destructors did or will unfix all pages.
    }
    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e); 
    }
    return RCOK;
}


/*********************************************************************
 *
 *  btree_impl::_skip_one_slot(p1, p2, child, slot, eof, found, backward)
 *
 *  Given 
 *   p1: fixed
 *   p2: an unused page_p
 *   child: points to p1
 *  compute the next slot, which might be on a successor to p1,
 *  in which case, fix it in p2, and when you return, make
 *  child point to the page of the next slot, and "slot" indicate
 *  where on that page it is.
 *
 *  Leave exactly one page fixed.
 *
 *  Found is set to true if the slot was found (on child or successor)
 *  and is set to false if not, (2nd page was empty).
 *
 *  Backward==true means to a backward scan. NB: can get into latch-latch
 *  deadlocks here!
 *
 *********************************************************************/
rc_t 
btree_impl::_skip_one_slot(
    btree_p&                p1, 
    btree_p&                p2, 
    btree_p*&                child, 
    slotid_t&                 slot, // I/O
    bool&                eof,
    bool&                found,
    bool                backward, 
    const bool bIgnoreLatches)        
{
    FUNC(btree_impl::_skip_one_slot);
    w_assert9( p1.is_fixed());
    w_assert9(! p2.is_fixed());
    w_assert9(child == &p1);

    // latch modes
    latch_mode_t fix_latch;
	
    w_assert9(slot <= p1.nrecs());
    if(backward) {
        --slot;
    } else {
        ++slot;
    }
    eof = false;
    found = true;
    bool time2move = backward? slot < 0 : (slot >= p1.nrecs());

    if (time2move) {
        /*
         *  Move to right(left) sibling
         */
        lpid_t pid = p1.pid();
        if (! (pid.page = backward? p1.prev() :p1.next())) {
            /*
             *  EOF reached.
             */
            slot = backward? -1: p1.nrecs();
            eof = true;
            return RCOK;
        }
        p1.unfix();
        DBGTHRD(<<"fixing " << pid);
	fix_latch = LATCH_SH;
	if(bIgnoreLatches) {
	    fix_latch = LATCH_NL;
	}
        W_DO(p2.fix(pid, fix_latch));
        if(p2.nrecs() == 0) {
            w_assert9(p2.is_smo()); 
            found = false;
        }
        child =  &p2;
        slot = backward? p2.nrecs()-1 : 0;
    }
    // p1 is fixed or p2 is fixed, but not both
    w_assert9((p1.is_fixed() && !p2.is_fixed())
        || (p2.is_fixed() && !p1.is_fixed()));
    return RCOK;
}


/*********************************************************************
 *
 *  btree_impl::_shrink_tree(rootpage)
 *
 *  Shrink the tree. Copy the child page over the root page so the
 *  tree is effectively one level shorter.
 *
 *********************************************************************/
rc_t
btree_impl::_shrink_tree(btree_p& rp, const bool bIgnoreLatches)        			 
{
    FUNC(btree_impl::_shrink_tree);
    INC_TSTAT(bt_shrinks);

    // latch modes
    latch_mode_t fix_latch;
    if(!bIgnoreLatches) {
	w_assert3( rp.latch_mode() == LATCH_EX);
    }
    w_assert1( rp.nrecs() == 0);
    w_assert1( !rp.prev() && !rp.next() );

    lpid_t pid = rp.pid();
    if ((pid.page = rp.pid0()))  {
        /*
         *  There is a child in pid0. Copy child page over parent,
         *  and free child page.
         */
        btree_p cp;
	fix_latch = LATCH_EX;
	if(bIgnoreLatches) {
	    fix_latch = LATCH_NL;
	}
        W_DO( cp.fix(pid, fix_latch) );

        DBG(<<"shrink " << rp.pid()  
                << " from level " << rp.level()
                << " to " << cp.level());
        
        w_assert3(rp.level() == cp.level() + 1);
        w_assert3(!cp.next() && !cp.prev());
        W_DO( rp.set_hdr( rp.root().page, 
                rp.level() - 1, cp.pid0(), 
                (uint2_t)(rp.is_compressed()? 
                        btree_p::t_compressed: btree_p::t_none)) );

        w_assert3(rp.level() == cp.level());

        SSMTEST("btree.shrink.1");
        
        if (cp.nrecs()) {
            W_DO( cp.shift(0, rp) );
        }
        SSMTEST("btree.shrink.2");

	if(!bIgnoreLatches) {
	    w_assert3( cp.latch_mode() == LATCH_EX);
	}
        W_DO( io->free_page(pid, false/*checkstore*/) );

	INC_TSTAT(page_btree_dealloc);
    } else {
        /*
         *  No child in pid0. Simply set the level of root to 1.
         *  Level 1 because this is now an empty tree.
         *  It can jump from level N -> 1 for N larger than 2
         */
        DBG(<<"shrink " << rp.pid()  
                << " from level " << rp.level()
                << " to 1");

        W_DO( rp.set_hdr(rp.root().page, 1, 0, 
            (uint2_t)(rp.is_compressed()? 
                btree_p::t_compressed: btree_p::t_none)) );
    }
    SSMTEST("btree.shrink.3");
    return RCOK;
}


/*********************************************************************
 *
 *  btree_impl::_grow_tree(rootpage)
 *
 *  Root page has split. Allocate a new child, shift all entries of
 *  root to new child, and have the only entry in root (pid0) point
 *  to child. Tree grows by 1 level.
 *
 *********************************************************************/
rc_t
btree_impl::_grow_tree(btree_p& rp, const bool bIgnoreLatches)        
{
    FUNC(btree_impl::_grow_tree);
    INC_TSTAT(bt_grows);

    // latch modes
    latch_mode_t fix_latch;
	
    /*
     *  Sanity check
     */
    fix_latch = LATCH_EX;
    if(!bIgnoreLatches) {
        fix_latch = LATCH_EX;
        rp.upgrade_latch(fix_latch);
        w_assert9(rp.latch_mode() == fix_latch);
        w_assert1(rp.next());
        w_assert9(rp.is_smo());        
    }

    /*
     *  First right sibling
     */
    lpid_t nxtpid = rp.pid();
    nxtpid.page = rp.next();
    btree_p np;

    // "following" a link here means fixing the page
    INC_TSTAT(bt_links);

    //if(!bIgnoreLatches) {
    fix_latch = LATCH_EX;
    W_DO( np.fix(nxtpid, fix_latch) );
    w_assert1(!np.next());
    //}


    /*
     *  Allocate a new child, link it up with right sibling,
     *  and shift everything over to it (i.e. it is a copy
     *  of root).   If the first page is compressed, they all are.
     */
    btree_p cp;
    W_DO( _alloc_page(rp.pid(), rp.level(),
                      np.pid(), cp, rp.pid0(), true, rp.is_compressed(),
                      st_regular, bIgnoreLatches) );
    
    W_DO( cp.link_up(rp.prev(), rp.next()) );
    W_DO( np.link_up(cp.pid().page, np.next()) );
    W_DO( rp.shift(0, cp) );
    
    w_assert9(rp.prev() == 0);

    /*
     *  Reset new root page with only 1 entry:
     *          pid0 points to new child.
     */
    SSMTEST("btree.grow.1");
    W_DO( rp.link_up(0, 0) );

    SSMTEST("btree.grow.2");

    DBGTHRD(<<"growing to level " << rp.level() + 1);
    W_DO( rp.set_hdr(rp.root().page,  
                     rp.level() + 1,
                     cp.pid().page, 
                    (uint2_t)(rp.is_compressed()? btree_p::t_compressed: btree_p::t_none)) );

    w_assert9(rp.nrecs() == 0); // pid0 doesn't count in nrecs

    SSMTEST("btree.grow.3");
    return RCOK;
}


/*********************************************************************
 *
 *  btree_impl::_propagate(root, key, el, child_pid, child_level, is_delete)
 *
 *  Propagate the split/delete child up to the root.
 *  This requires a traverse from the root, gathering a stack
 *  of pids.
 *
 *********************************************************************/
#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_array_t<btree_p>; 
// template class w_auto_delete_array_t<lpid_t>; 
template class w_auto_delete_array_t<int>; 
#endif

rc_t
btree_impl::_propagate(
    lpid_t const&        root_pid,         // I-  root page  -- fixed
    const cvec_t&        key,        // I- key of leaf page being removed
                                // OR key of leaf page that split
    const cvec_t&        elem,        // I- elem ditto
    const lpid_t&        _child_pid, // I-  pid of leaf page removed
                                    //  or of the page that was split
    int                  child_level, // I - level of child_pid
    bool                 isdelete, // I-  true if delete being propagated
    const bool bIgnoreLatches)        
{
    // latch modes
    latch_mode_t fix_latch;
	
    btree_p root;
    fix_latch = LATCH_SH;
    if(bIgnoreLatches) {
	fix_latch = LATCH_NL;
    }
    root.fix(root_pid, fix_latch);
    w_assert9(root.is_fixed());
    // FRJ: not yet... if we're a 1-level btree (below) then this will be true
    //    w_assert9(root.latch_mode()==LATCH_EX);

#if BTREE_LOG_COMMENT_ON
    W_DO(log_comment("start propagate"));
#endif 
    lpid_t                 child_pid = _child_pid;

    /*
     * Run from root to child_pid (minus one level - we don't fix child)
     * and save the page ids - this might be a naive
     * way to do it, but we avoid EX latching all the pages the 
     * first time through because in some cases, we
     * won't have to propagate all the way up.  We'd rather
     * EX-latch only those pages that will be updated, so that
     * any other threads that got into the tree before we grabbed
     * our EX tree latch might not collide with us.
     */

    int max_levels = root.level();

    if(max_levels == 1) {
        if (isdelete) {
            DBG(<<"trying to shrink away the root");
            /* We don't shrink away the root */
            w_assert9(root.is_fixed());
	    if(!bIgnoreLatches) {
		w_assert9(root.latch_mode()==LATCH_EX);
	    }
	    w_assert9(0);
            return RCOK;
        } else {
            // Root is the leaf, and we have to grow the tree first.
            w_assert9(child_pid == root.pid());


            DBG(<<"LEAF == ROOT");
            w_assert9(root.next());

            W_DO(_grow_tree(root, bIgnoreLatches));
            w_assert9(root.level() > 1);
            w_assert9(root.nrecs() == 0);
            w_assert9(!root.next());
            w_assert9(root.pid0() != 0 );

            child_pid.page = root.pid0();
            DBGTHRD(<<"Propagating split leaf page " << child_pid
                        << " into root page " << root.pid()
                        );
            bool was_split = false;
            // slot in which child_pid sits is slot -1

            btree_p parent = root; // refix to keep latched twice
            W_DO(_propagate_split(parent, child_pid, -1, was_split, bIgnoreLatches));
            w_assert9(!was_split);
            w_assert9(!parent.is_fixed());
            return RCOK;
        }
    }
    {
        btree_p* p = new btree_p[max_levels]; // page stack -- 20 bytes each
        lpid_t*  pid = new lpid_t[max_levels];                
        slotid_t*     slot = new slotid_t[max_levels];

        if(!p || !pid || !slot) {
            W_FATAL(eOUTOFMEMORY);
        }
        w_auto_delete_array_t<btree_p> auto_del_p(p);
        w_auto_delete_array_t<lpid_t> auto_del_pid(pid);
        w_auto_delete_array_t<slotid_t> auto_del_slot(slot);

        int top = 0;
        pid[top] = root.pid();
        p[top] = root; // refix
        root.unfix();


        /*
         *  Traverse towards the leaf. 
         *  We expect to find the child_pid in each page
         *  on the way down, since we're looking for pages that are
         *  already there, but the key is a NEW key for the split case.  
         *  When we reach the level above
         *  the child's level, we can stop, and expect to find
         *  the child pid there. In the delete case, the child page has 
         *  already been unlatched and unlinked.   In the split case,
         *  the child has already been split and linked to its new sibling.
         *  Once we reach the child_pid, we pop back down the stack: 
         *   deleting children and cutting entries out of their parents
         * OR
         *  or inserting children's next() pages  into the parents and 
         *  splitting parents
         */

        for ( ; top < max_levels; top++ )  {

            bool         total_match = false;
            bool         found_key = false;

            DBG(<<"INSPECTING " << top << " pid " << pid[top] );
	    fix_latch = LATCH_SH;
	    if(bIgnoreLatches) {
		fix_latch = LATCH_NL;
	    }
            W_DO( p[top].fix(pid[top], fix_latch) );

#if W_DEBUG_LEVEL > 3
            if(print_propagate) {
                DBG(<<"top=" << top << " page=" << pid[top]
                        << " is_leaf=" << p[top].is_leaf());
                p[top].print();
            }
#endif 

            w_assert9(p[top].is_fixed());
               w_assert9(p[top].is_node());
            w_assert9(! p[top].is_smo() );

            // pid0 is not counted in nrecs()
            w_assert9(p[top].nrecs() > 0 || p[top].pid0() != 0);

            // search expects at least 2 pages
            DBG(<<"SEARCHING " << top << " page " << p[top].pid() << " for " << key);
            W_DO(p[top].search(key, elem, found_key, total_match, slot[top]));
            // might not find the exact key in interior nodes

            w_assert9(slot[top] >= 0);
            if(!total_match) --slot[top];
            {
                slotid_t child_slot = slot[top];

                lpid_t found_pid = pid[top]; // get vol, store
                found_pid.page = (child_slot<0) ? p[top].pid0() : 
                                p[top].child(child_slot) ;

                DBG(<<"LOCATED PID (top=" << top << ") page " << found_pid 
                    << " for key " << key);

                /* 
                 * quit the loop when we've reached the
                 * level above the child.  
                 */
                if(p[top].level() <= child_level + 1) {
                    w_assert9(child_pid == found_pid);
                    break;
                }
                if(top>0) p[top].unfix();
                pid[top+1] = found_pid;
            }
        }

        /* Now we've reached the level of the child_pid. The first
         * round of deletion/insertion will happen; after the
         * first deletion/insertion, there may be no need to
         * keep going.
         */
        DBG(<<"reached child at level " << p[top].level());

        w_assert9(top < max_levels);

        while ( top >= 0 )  {
            DBG(<<"top=" << top << " pid=" << pid[top] << " slot " << slot[top]);
            /*
             * OK: slot[i] is the slot of page p[i] in which a <key,pid>
             * pair was found, or should go.  Perhaps we should re-compute
             * the search as we pop the stacks, and make sure the slots haven't
             * changed..
             */
            w_assert9(p[top].is_fixed());
            if(!bIgnoreLatches && p[top].latch_mode() != LATCH_EX) {
                // should not block because we've got the tree latched
#if W_DEBUG_LEVEL > 3
                bool would_block = false;
                W_DO(p[top].upgrade_latch_if_not_block(would_block));
                if(would_block) {
                    // It's the bf cleaner!  
                    DBG(<<"BF-CLEANER? clash" );
                }
#endif 
                p[top].upgrade_latch(LATCH_EX);
            }

            if(isdelete) {
                DBGTHRD(<<"Deleting child page " << child_pid
                    << " and cutting it from slot " << slot[top]
                    << " of page " << pid[top]
                    );

                W_DO(p[top].cut_page(child_pid, slot[top]));

                if(p[top].pid0()) {
                    DBG(<<"Not empty; quit at top= " << top);
                    p[top].unfix();
                    break;
                }
            } else {
                DBGTHRD(<<"Inserting child page " << child_pid
                    << " in slot " << slot[top]
                    << " of page " << pid[top]
                    );
#if W_DEBUG_LEVEL > 3
            {
                DBG(<<"RE-SEARCHING " << top << " page " << p[top].pid() << " for " << key);
                slotid_t child_slot=0;
                bool found_key=false;
                bool total_match=false;
                // cvec_t         null;
                W_DO(p[top].search(key, elem, found_key, 
                            total_match, child_slot));
                if(!total_match) child_slot--;
                w_assert3(child_slot == slot[top]);
            }
#endif 
                w_assert9(slot[top] >= -1 && slot[top] < p[top].nrecs());

                bool was_split = false;
                DBG(<<"PROPAGATING SPLIT by inserting <key,pid> =" 
                        << key << "," << child_pid
                        << " INTO page " << p[top].pid());

                W_DO(_propagate_split(p[top], child_pid, slot[top], was_split, bIgnoreLatches));
                // p[top] is unfixed

                if( !was_split) {
                    DBG(<<"No split; quit at top =" << top);
                    break;
                }
            }
            DBG(<<"top=" << top);
            if( top == 0 ) {
                // we've hit the root and have to grow/shrink the
                // tree
                w_assert9(root_pid == pid[top]);
                if(isdelete) {
                    W_DO(_shrink_tree(p[top], bIgnoreLatches));
                    // we're done?
                    break;
                } else {
		    fix_latch = LATCH_EX;
		    if(bIgnoreLatches) {
			fix_latch = LATCH_NL;
		    }
                    p[top].fix(root_pid, fix_latch);
                    btree_p &root = p[top];
                    W_DO(_grow_tree(root, bIgnoreLatches));
                    w_assert9(root.nrecs() == 0);
                    w_assert9(root.pid0());
                    w_assert9(root.next()==0);
                    child_pid.page = root.pid0();
                    // have to do one more 
                    // iteration because root's one child was split 
#if W_DEBUG_LEVEL > 3
                    {
                        btree_p tmp;
			fix_latch = LATCH_SH;
			if(bIgnoreLatches) {
			    fix_latch = LATCH_NL;
			}
                        W_DO(tmp.fix(child_pid, fix_latch));
                        w_assert2(tmp.next() != 0);
                    }
#endif 
                    // avoid decrementing top
                    slot[top] = -1; // pid0
                    // pid[top] unchanged - root never changes
                    continue;
                }
            }
            p[top].unfix();
            child_pid = pid[top];
            --top;
#if W_DEBUG_LEVEL > 2
            if(pid[top] != root_pid) w_assert3(!p[top].is_fixed());
#endif

	    fix_latch = LATCH_EX;
	    if(bIgnoreLatches) {
		fix_latch = LATCH_NL;
	    }
            W_DO(p[top].fix(pid[top], fix_latch));
        }

    }

#if BTREE_LOG_COMMENT_ON
    W_DO(log_comment("end propagate"));
#endif 

    return RCOK;
}

rc_t
btree_impl::_propagate_split(
    btree_p&                 parent, // I - page to get the insertion
    const lpid_t&         _pid,  // I - pid of the child that was split (NOT the rsib)
    slotid_t                  slot,        // I - slot where the split page sits
                                //  which is 1 less than slot where the new entry goes
                                // slot -1 means pid0
    bool&                    was_split, // O - true if parent was split by this
    const bool bIgnoreLatches)
{

#if BTREE_LOG_COMMENT_ON
    W_DO(log_comment("start propagate_split"));
#endif 

    // latch modes
    latch_mode_t fix_latch;
	
    lpid_t pid = _pid;
    /*
     *  Fix first child so that we can get the pid of the new page
     */
    btree_p c1;
    fix_latch = LATCH_SH;
    if(bIgnoreLatches) {
	fix_latch = LATCH_NL;
    }
    W_DO( c1.fix(pid, fix_latch) );
#if W_DEBUG_LEVEL > 3
    if(print_split) { 
        DBG(<<"LEAF being split :");
        c1.print(); 
    }
#endif 

    w_assert9( c1.is_smo() );
    w_assert9( c1.nrecs());
    pid.page = c1.next();
    btrec_t r1(c1, c1.nrecs() - 1);

    btree_p c2;
    INC_TSTAT(bt_links);

    // GAK: EX because we have to do an update below
    fix_latch = LATCH_EX;
    if(bIgnoreLatches) {
	fix_latch = LATCH_NL;
    }
    W_DO( c2.fix(pid, fix_latch) ); 
#if W_DEBUG_LEVEL > 3
    if(print_split) { 
        DBG(<<"NEW right sibling :");
        c2.print(); 
    }
#endif 
    w_assert9( c2.nrecs());
    w_assert9( c2.is_smo() );

#if W_DEBUG_LEVEL > 3
    // Apparently page.distribute doesn't put anything into pid0
    w_assert9(c2.pid0() == 0);
#endif 

    shpid_t childpid = pid.page;
    /* 
     * Construct the key,pid entry
     * Get the key from the first entry on the page.
     */
    btrec_t r2(c2, 0);
    vec_t pkey;
    vec_t pelem;

    if (c2.is_node())  {
        pkey.put(r2.key());
        pelem.put(r2.elem());
    } else {
        /*
         *   Compare key first
         *   If keys are different, compress away all element parts.
         *   Otherwise, extract common key,element parts.
         */
        size_t common_size = 0;
        int diff = cvec_t::cmp(r1.key(), r2.key(), &common_size);
        DBGTHRD(<<"diff = " << diff << " common_size = " << common_size);
        if (diff)  {
            if (common_size < r2.key().size())  {
                pkey.put(r2.key(), 0, common_size + 1);
            } else {
                w_assert9(common_size == r2.key().size());
                pkey.put(r2.key());
                pelem.put(r2.elem(), 0, 1);
            }
        } else {
            /*
             *  keys are the same, r2.elem() must be greater than r1.elem()
             */
            pkey.put(r2.key());
            cvec_t::cmp(r1.elem(), r2.elem(), &common_size);
            w_assert9(common_size < r2.elem().size());
            pelem.put(r2.elem(), 0, common_size + 1);
        }
    }
    c1.unfix();

    /* 
     * insert key,pid into parent
     */
#if W_DEBUG_LEVEL > 3
    if(print_split) { 
        DBG(<<"PARENT PAGE" );
        parent.print(); 
    }
#endif 
    rc_t rc = parent.insert(pkey, pelem, ++slot, c2.pid().page);

#if W_DEBUG_LEVEL > 3
    if(print_split) { parent.print(); }
#endif 

    if (rc.is_error()) {
        DBG(<<"rc= " << rc);
        if (rc.err_num() != eRECWONTFIT) {
            return RC_AUGMENT(rc);
        }
        /*
         *  Parent is full --- split parent node
         */
        DBGTHRD(<<"parent is full -- split parent node");

        lpid_t rsib_page;
        int addition = (pkey.size() + pelem.size() + 2 + sizeof(shpid_t));
        bool left_heavy;
        SSMTEST("btree.propagate.s.6");

// TODO (performance) use btree's own split_factor
        W_DO( __split_page(parent, rsib_page, left_heavy,
			   slot, addition, 50, bIgnoreLatches) );

        SSMTEST("btree.propagate.s.7");

        btree_p rsib;
        btree_p& target = rsib;
        if(left_heavy) {
            w_assert9(parent.is_fixed());
            target = parent;
        } else {
	    fix_latch = LATCH_EX;
	    if(bIgnoreLatches) {
		fix_latch = LATCH_NL;
	    }
            W_DO(target.fix(rsib_page, fix_latch));
            w_assert9(rsib.is_fixed());
        }
        SSMTEST("btree.propagate.s.8");
        W_DO(target.insert(pkey, pelem, slot, childpid));
        was_split = true;
    }

    SSMTEST("btree.propagate.s.3");

    /*
     * clear the smo in the 2nd leaf, now that the
     * parent has a pointer to it
     */
    W_DO(c2.clr_smo());

    /*
     *  For node, move first record to pid0
     */
    if (c2.is_node())  {
        shpid_t pid0 = c2.child(0);
        DBGTHRD(<<"remove first record, pid0=" << pid0);
        W_DO(c2.remove(0, c2.is_compressed())); // zkeyed_p::remove
        SSMTEST("btree.propagate.s.9");
        W_DO(c2.set_pid0(pid0));
    }
    SSMTEST("btree.propagate.s.10");
    c2.unfix();

    if(was_split) {
        w_assert9( parent.is_smo() );
    } else {
        w_assert9(! parent.is_smo() );
    }

    /*
     * clear the smo in the first leaf, since
     * parent now has its smo set.
     */
    fix_latch = LATCH_EX;
    if(bIgnoreLatches) {
	fix_latch = LATCH_NL;
    }
    W_DO( c1.fix(_pid, fix_latch) );
    W_DO(c1.clr_smo());
    parent.unfix();

#if BTREE_LOG_COMMENT_ON
    W_DO(log_comment("end propagate_split"));
#endif 

    return RCOK;
}

    
/*********************************************************************
 *
 *  btree_impl::_split_leaf(root, page, key, el, split_factor)
 *  Split the given leaf page, based on planning to put key,el into it
 *     Compensate the operation.
 *
 *  btree_impl::__split_page(page, sibling, left_heavy, slot, 
 *                       addition, split_factor)
 *
 *  Split the page. The newly allocated right sibling is returned in
 *  "sibling". Based on the "slot" into which an "additional" bytes 
 *  would be inserted after the split, and the "split_factor", 
 *  routine computes the a new "slot" for the insertion after the
 *  split and computes a boolean flag "left_heavy" to indicate if the
 *  new "slot" is in the left or right sibling.
 *
 *  We grab an anchor and compensate this whole operation. If an
 *  error occurs within, we have to roll back while we have the
 *  tree latch. 
 *
 *********************************************************************/
rc_t
btree_impl::_split_leaf(
    lpid_t const &        root_pid,         // I - root of tree
    btree_p&              leaf,         // I - page to be split
    const cvec_t&         key,        // I-  which key causes split
    const cvec_t&         el,        // I-  which element causes split
    int                   split_factor,
    const bool bIgnoreLatches)        
{
    w_assert9(leaf.is_fixed());
    if(!bIgnoreLatches) {
	w_assert9(leaf.latch_mode() == LATCH_EX);
    }
    lsn_t         anchor;         // serves as savepoint too
    xct_t*         xd = xct();

    check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
    if (xd)  anchor = xd->anchor();

#if BTREE_LOG_COMMENT_ON
    W_DO(log_comment("start leaf split"));
#endif 

    int            addition = key.size() + el.size() + 2;
    lpid_t         rsib_pid;
    lpid_t         leaf_pid = leaf.pid();;
    int            level = leaf.level();
    {
        bool         left_heavy;
        slotid_t     slot=1; // use 1 to leave at least one
                             // record in the left page.
        w_assert9(leaf.nrecs()>0);

        X_DO( __split_page(leaf, rsib_pid,  left_heavy,
			   slot, addition, split_factor, bIgnoreLatches), anchor );
        leaf.unfix();
    }

    X_DO(_propagate(root_pid, key, el, leaf_pid, level, false /*not delete*/, bIgnoreLatches), anchor);

#if BTREE_LOG_COMMENT_ON
    W_DO(log_comment("end leaf split"));
#endif 
    if (xd)  {
        SSMTEST("btree.propagate.s.1");
        xd->compensate(anchor,false/*not undoable*/ LOG_COMMENT_USE("btree.prop.1"));
    }

    INC_TSTAT(bt_leaf_splits);
    return RCOK;
}

/* 
 * btree_impl::__split_page(...)
 * this does the work, and assumes that it's
 * being compensated
 */
rc_t
btree_impl::__split_page(
    btree_p&        page,                // IO- page that needs to split
    lpid_t&        sibling_page,        // O-  new sibling
    bool&        left_heavy,        // O-  true if insert should go to left
    slotid_t&        slot,                // IO- slot of insertion after split
    int                addition,        // I-  # bytes intended to insert
    int                split_factor,        // I-  % of left page that should remain
    const bool bIgnoreLatches)        
{
    FUNC(btree_impl::__split_page);

    INC_TSTAT(bt_splits);

    DBGTHRD( << "split page " << page.pid() << " addition " << addition);

    // latch modes
    latch_mode_t fix_latch;
	
    /*
     *  Allocate new sibling
     */
    btree_p sibling;
    lpid_t root = page.root();
    W_DO( _alloc_page(root, page.level(), page.pid(), sibling, 0, true,
                      page.is_compressed(), st_regular, bIgnoreLatches) );

    w_assert9(sibling.is_fixed());
    if(!bIgnoreLatches) {
	w_assert9(sibling.latch_mode() == LATCH_EX);
    }
    SSMTEST("btree.propagate.s.2");
    /*
     *  Page has a modified tree structure.
     *  NB: On restart undo we won't have the latch,
     *  so to cover the case where we crash in the middle
     *  of a smo,  our recovery can only undo 1 tx at a
     *  time, and it starts with the tx with the highest
     *  undo_nxt.
     */

    /*
     *  Hook up all three siblings: cousin is the original right
     *  sibling; 'sibling' is the new right sibling.
     */
    lpid_t old_next = page.pid();// get volume & store
    old_next.page = page.next();

    W_DO( sibling.link_up(page.pid().page, old_next.page) );
    sibling_page = sibling.pid(); // set result argument
                                // and save for use below

    W_DO( page.set_smo() );         
    SSMTEST("btree.propagate.s.4");
    W_DO( page.link_up(page.prev(), sibling_page.page) );

    /*
     *  Distribute content to sibling
     */
    W_DO( page.distribute(sibling, left_heavy,
                          slot, addition, split_factor, bIgnoreLatches) );
    /* Sibling has no pid0 at this point */

    DBGTHRD(<< " after split  new sibling " << sibling.pid()
        << " left_heavy=" << left_heavy
        << " slot " << slot
        << " page.nrecs() = " << page.nrecs()
        << " sibling.nrecs() = " << sibling.nrecs()
    );
    sibling.unfix();

    if (old_next.page) {
        btree_p cousin;

        // "following" a link here means fixing the page
        INC_TSTAT(bt_links);
	fix_latch = LATCH_EX;
	if(bIgnoreLatches) {
	    fix_latch = LATCH_NL;
	}
        W_DO( cousin.fix(old_next, fix_latch) );
        W_DO( cousin.link_up(sibling_page.page, cousin.next()));
    }

    SSMTEST("btree.propagate.s.5");

    // page.unfix();
    // keep page unfixed because one of the callers
    // will use it .
    // Nothing is latched except the root and page
    w_assert9(page.is_fixed());
    w_assert9(!sibling.is_fixed());

    /*
     * NB: caller propagates the split
     */
    return RCOK;
}    

/*********************************************************************
 *
 *  btree_impl::_traverse(__root, 
 *      start, old_start_lsn,
 *         key, elem, 
 *      unique, found, 
 *      mode, 
 *      leaf, parent, leaf_lsn, parent_lsn)
 *
 *  Traverse the btree starting at "start" (which may be below __root;
 *           __root is the true root of the tree, start is the root of the search)
 *  to find <key, elem>. 
 *
 *
 *  Return the leaf and slot
 *     that <key, elem> resides or, if not found, the leaf and slot 
 *     where <key, elem> SHOULD reside.
 *
 *  Returns found==true if:
 *      key found, regardless of uniqueness of index
 *
 *  ASSUMPTIONS: 
 *         
 *  FIXES: crabs down the tree, fixing each interior page in given mode,
 *         and returns with leaf and parent fixed in given mode
 *
 *********************************************************************/
rc_t
btree_impl::_traverse(
    const lpid_t&        __root,        // I-  root of tree 
    const lpid_t&        _start,        // I-  root of search 
    const lsn_t&         _start_lsn,// I-  old lsn of start 
    const cvec_t&        key,        // I-  target key
    const cvec_t&        elem,        // I-  target elem
    bool&                found,        // O-  true if sep is found
    latch_mode_t         mode,        // I-  EX for insert/remove, SH for lookup
    btree_p&             leaf,        // O-  leaf satisfying search
    btree_p&             parent,        // O-  parent of leaf satisfying search
    lsn_t&               leaf_lsn,        // O-  lsn of leaf 
    lsn_t&               parent_lsn,        // O-  lsn of parent 
    const bool bIgnoreLatches)         
{
    FUNC(btree_impl::_traverse);
    lsn_t        start_lsn = _start_lsn;
    lpid_t       start     = _start;
    rc_t         rc;

    if(!bIgnoreLatches) {
	get_latches(___s,___e);
    }

    if(start == __root) {
        INC_TSTAT(bt_traverse_cnt);
    } else {
        INC_TSTAT(bt_partial_traverse_cnt);
    }

    tree_latch                 tree_root(__root, bIgnoreLatches); // latch the store
                               // NOTE: this used to be the root page
    slotid_t                   slot = -1;

    // latch modes
    latch_mode_t smo_mode;
    latch_mode_t smo_p1mode;
    latch_mode_t smo_p2mode;
    latch_mode_t fix_latch;
    
pagain:

    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e); 
    }
#if W_DEBUG_LEVEL > 3
    DBGTHRD(<<"");
    if(print_ptraverse) {
        cout << "TRAVERSE.pagain "<<endl;
        print(start);
    }
#else
    DBGTHRD(<<"__traverse.pagain: mode " << int(mode));
#endif 

    {
        btree_p p[2];                // for latch coupling
        lpid_t  pid[2];                // for latch coupling
        lsn_t   lsns[2];        // for detecting changes

        int c;                        // toggle for p[] and pid[]. 
                                // p[1-c] is child of p[c].

        w_assert2(! p[0].is_fixed());
        w_assert2(! p[1].is_fixed());
        c = 0;

        found = false;

        pid[0] = start;
        pid[1] = start; // volume and store
        pid[1].page = 0;

        /*
         *  Fix parent.  If this is also the leaf, we'll
         *  upgrade the latch below, when we copy this over
         *  to the input/output argument "leaf".
         */

	fix_latch = LATCH_SH;
	if(bIgnoreLatches) {
	    fix_latch = LATCH_NL;
	}
        W_DO( p[c].fix(pid[c], fix_latch) );
        w_assert1(p[c].is_fixed());
        lsns[c] = p[c].lsn(); 

        /*
         * Check the latch mode. If this is a 1-level
         * btree, let's grab the right mode before
         * we go on - in the hope that we avoid extra
         * traversals when we get to the bottom and try
         * to upgrade the latch.
         */
        if(p[c].is_leaf() && mode != LATCH_SH) {
            p[c].unfix();
            W_DO( p[c].fix(pid[c], mode) );
            lsns[c] = p[c].lsn(); 
        }

        if(start_lsn.valid() && start_lsn != p[c].lsn()) {
            // restart at root;
            DBGTHRD(<<"starting point changed; restart");
            start_lsn = lsn_t::null;
            start = __root;
            INC_TSTAT(bt_restart_traverse_cnt);
            DBGTHRD(<<"->pagain");
            goto pagain;
        }

#if W_DEBUG_LEVEL > 2
        int waited_for_posc = 0;
#endif 

no_change:
        w_assert2(p[c].is_fixed());
        w_assert2( ! p[1-c].is_fixed() );
        DBGTHRD(<<"_traverse: no change: page " << p[c].pid());

        /*
         *  Traverse towards the leaf with latch-coupling.
         *  Go down as long as we're working on interior nodes (is_node());
         *  p[c] is always fixed.
         */

        for ( ; p[c].is_node(); c = 1 - c)  {
            DBGTHRD(<<"p[c].pid() == " << p[c].pid());

            w_assert2(p[c].is_fixed());
            w_assert2(p[c].is_node());
            w_assert2(p[c].pid0());


            bool total_match = false;
            bool wait_for_posc = false;

            if(p[c].is_smo()) {
                /* 
                 * smo in progress; we'll have to wait for it to complete
                 */
                DBG(<<"wait for posc because smo bit is set");
                wait_for_posc = true;

            } else if(p[c].pid0() ) {
                w_assert9(p[c].nrecs() >= 0);
                W_DO( _search(p[c], key, elem, found, total_match, slot) );

                /* 
                 * See if we're potentially at the wrong page.
                 * That's possible if key is > highest key on the page
                 * and sm bit is 1. Delete bits only appear on leaf pages,
                 * so we don't have to check them here.
                if( (slot == p[c].nrecs()) && p[c].is_delete() )  {
                } else 
                 */

                {
                    /*  
                     * Since this is an interior node, the first
                     * key on the page we find might be the tail of
                     * a duplicate cluster, or its key might
                     * fall between the first key on the prior page
                     * and the first key on this page.
                     * So for interior pages, we have to decrement slot,
                     * and search at the prior page.  If we found the key
                     * but didn't find a total match, the search could have
                     * put us at the page where the <key,elem> should be
                     * installed, which might be beyond the end of the child list.  
                     * If we found the key AND had a total match,
                     * we still to go to the page at the given key.
                     */

                    if(!total_match) slot--;
                }
            } else {
                w_assert9(p[c].nrecs() == 0);
                /*  
                 * Empty page left because of unfinished delete.
                 * Will cause us to establish a POSC. 
                 * Starting anywhere but the root is not an option
                 * for this because we'd have to be able to tell
                 * if the page were still in the same file, and the
                 * lsn check isn't sufficient for that.
                 */
                DBG(<<"wait for posc because empty page: " << p[c].pid());
                if(p[c].pid() != __root) {
                    w_assert9(p[c].is_delete());
                    wait_for_posc = true;
                }
            }

            if(wait_for_posc) {
#if W_DEBUG_LEVEL > 2
                w_assert3(waited_for_posc < 3);
                // TODO: remove
                waited_for_posc ++;
#endif 
                /* 
                 * Establish a POSC per RJ7008, 2: Tree traversal
                 * Grab LATCH_SH on root and await SMO to be completed.
                 * We've modified this to clear the SMO bit if
                 * we've got the tree EX latched.
                 * This is necessary to get the SMO bits on interior
                 * pages cleared, and it's ok because propagation
                 * of splits and deletes doesn't use _traverse()
                 */
                {
                    /*
                     * Mohan says that we don't bother with the 
                     * conditional latch here. Just release the
                     * page latches and request an unconditional
                     * SH latch on the root.  
                     * Ref: RJ7008  2. Tree Traversal
                     */
                    bool was_latched = tree_root.is_fixed();
#if W_DEBUG_LEVEL > 2
                    latch_mode_t old_mode = tree_root.latch_mode();
                    DBG(<<"old mode" << int(old_mode));
#endif 

		    smo_mode = LATCH_SH;
		    smo_p1mode = LATCH_SH;
		    smo_p2mode = LATCH_SH;
		    if(bIgnoreLatches) {
			smo_mode = LATCH_NL;
			smo_p1mode = LATCH_NL;
			smo_p2mode = LATCH_NL;
		    }
                    w_error_t::err_num_t rce;
                    rce = tree_root.get_for_smo(false, smo_mode,
						p[c], smo_p1mode, true, &p[1-c], smo_p2mode, bIgnoreLatches);

                    w_assert2(tree_root.is_fixed());
		    if(!bIgnoreLatches) {
			w_assert2(tree_root.latch_mode() >= LATCH_SH);
		    }
#if W_DEBUG_LEVEL > 2
                    if(was_latched && !bIgnoreLatches) {
                        w_assert3(tree_root.latch_mode() >= old_mode);
                    }
#endif 

                    if(p[c].is_smo()) {
                        /*
                         * We're traversing on behalf of _insert or _delete, 
                         * and we already have the tree latched. Don't
                         * latch it again. Just clear the smo bit.  We might
                         * not have *this* page fixed in EX mode though.
                         */
			fix_latch = LATCH_EX;
			if(bIgnoreLatches) {
			    fix_latch = LATCH_NL;
			}
                        if(p[c].latch_mode() < fix_latch) {
                            // We should be able to do this because
                            // we have the tree latch.  We're not
                            // turning a read-ohly query into read-write
                            // because we are operating on behalf of insert
                            // or delete.
                            w_assert2(tree_root.is_fixed());
                            w_assert2(tree_root.pinned_by_me());
                            p[c].upgrade_latch(fix_latch);
                        }
                        INC_TSTAT(bt_clr_smo_traverse);
#if BTREE_LOG_COMMENT_ON
                        W_DO(log_comment("clr_smo/T"));
#endif 
                        W_DO( p[c].clr_smo(true));
                        // no way to downgrade the latch. 
                        // TODO NOW WE CAN DOWNGRADE: modify this code according
                        // to Mohan
                        //filed as GNATS 137
                    }

                    // we shouldn't be able to get an error from get_tree_latch
                    if(rce && (rce != eRETRY)) {
                        tree_root.unfix();
                        return RC(rce);
                    }

                    /* 
                     * If we don't accept the potential starvation,
                     * we hang onto the tree latch.
                     * If we think it's highly unlikely, we can
                     * unlatch it now.  BUT... we don't want to
                     * unlatch it if we had already latched it when
                     * we got in here.
                     */
                    if(!was_latched) {
                        tree_root.unfix();
                    }

                    /* 
                     * We try the optimization: see if we can restart the 
                     * traverse at the parent.  We do that only if the
                     * page's lsn hasn't changed.
                     */
                    if(p[1-c].is_fixed() && (p[1-c].lsn() == lsns[1-c])) {
                        start = pid[1-c];
                        p[c].unfix(); // gak: we're going to re-fix
                                        // it immediately
                        c = 1-c;
                        DBGTHRD(<<"-->no_change TREE LATCH MODE "
                                << int(tree_root.latch_mode())
                                );
                        goto no_change;
                    }
                    p[c].unfix();
                    p[1-c].unfix(); // might not be fixed
                    DBGTHRD(<<"->again");
                    goto pagain;
                }
            }
#if W_DEBUG_LEVEL > 2
                else waited_for_posc =0;
#endif 

            w_assert9(  p[c].is_fixed());
            w_assert9( !p[c].is_smo() );
            w_assert9( !p[c].is_delete() );

            p[1-c].unfix(); // if it's valid

            DBGTHRD(<<" found " << found 
                << " total_match " << total_match
                << " slot=" << slot);

            /*
             *  Get pid of the child, and fix it.
             *  If the child is a leaf, we'll want to
             *  fix in the given mode, else fix in LATCH_SH mode.
             *  Slot < 0 means we hit the very beginning of the file.
             */
            pid[1-c].page = ((slot < 0) ? p[c].pid0() : p[c].child(slot));

	    fix_latch = LATCH_SH;
	    if(bIgnoreLatches) {
		fix_latch = LATCH_NL;
	    }
            latch_mode_t node_mode = p[c].is_leaf_parent()? mode : fix_latch;

            /*
             * 1-c is child, c is parent; 
             *  that will reverse with for-loop iteration.
             */
            W_DO( p[1-c].fix(pid[1-c], node_mode) );
            lsns[1-c] = p[1-c].lsn(); 

        } // for loop

        /* 
         * c is now leaf, 1-c is parent 
         */
        w_assert9( p[c].is_leaf());
        // pid0 isn't used for leaves
        w_assert9( p[c].pid0() == 0 );
        w_assert9( p[c].is_fixed());

        /* 
         * if the leaf isn't the root, we have a parent fixed
         */ 
        w_assert9(p[1-c].is_fixed() || 
                (pid[1-c].page == 0 && pid[c] == start));

        leaf = p[c]; // copy does a refix
        leaf_lsn = p[c].lsn(); // caller wants lsn
        p[c].unfix();

        if(p[1-c].is_fixed()) {
            DBGTHRD(<<"2-level btree");
            parent = p[1-c];        // does a refix
            parent_lsn =  p[1-c].lsn(); // caller wants lsn
            p[1-c].unfix();
            w_assert9( parent.is_fixed());
            w_assert9( parent.is_node());
            w_assert9( parent.is_leaf_parent());
        } else {
            DBGTHRD(<<"shallow btree: latch mode= " << int(leaf.latch_mode()));
            // We hit a leaf right away
            w_assert9( leaf.pid() == start );

            // We shouldn't encounter this unless
            // the tree is only 1 level deep
            w_assert9( start == __root );
            w_assert9( leaf.pid() == __root );

            // Upgrade the latch before we return.
            if(leaf.latch_mode() != mode) { 
                bool would_block=false;
                W_DO(leaf.upgrade_latch_if_not_block(would_block));
                if(would_block) {
                    leaf.unfix();
                    parent.unfix();
                    tree_root.unfix();
                    INC_TSTAT(bt_upgrade_fail_retry);
                    /* TODO: figure out how to avoid this
                    * situation -- it's too frequent - we need
                    * to pay better attention to the first fix 
                    * when we first enter : if the tree is 1 level
                    * at that time, just unfix, refix with proper mode
                    */
                    DBG(<<"--> again; cannot upgrade ");
                    goto pagain;
                }
		if(!bIgnoreLatches) {
		    w_assert9(leaf.latch_mode() == mode );
		}
            }

            // Doubly-fix the page:
            parent = leaf;
            parent_lsn = leaf_lsn;

        }
    }

    /*
     * Destructors for p[] unfix those pages, so 
     * we leave the leaf page and the parent page fixed.
     */
    w_assert2(leaf.is_fixed());
    w_assert2(leaf.is_leaf());
    if(!bIgnoreLatches) {
	w_assert2(leaf.latch_mode() == mode );
    }

#if W_DEBUG_LEVEL > 1
    if(smlevel_1::log && smlevel_0::logging_enabled) {
        w_assert2( leaf_lsn != lsn_t::null);
    }
#endif

    w_assert2(parent.is_fixed());
    w_assert2(parent.is_node() || 
        (parent.is_leaf() && leaf.pid() == start  ));
    w_assert2(parent.is_leaf_parent() || parent.is_leaf());

#if W_DEBUG_LEVEL > 1
    if(smlevel_1::log && smlevel_0::logging_enabled) 
    {
        w_assert9(parent_lsn != lsn_t::null);
    }
#endif

    if(!bIgnoreLatches) {
	if(leaf.pid() == __root) {
	    // sh, ex: 2,0 or 0,2, or 1,1
	    check_latches(___s+2,___e+2, ___s+___e+2);
	} else {
	    // sh,ex: 2,0 or 1,1
	    check_latches(___s+2,___e+1, ___s+___e+2);
	}
    }

    /* 
     * NB: we have checked for SMOs in the nodes; 
     * we have NOT checked the leaf.
     */
    DBGTHRD(<<" found = " << found << " leaf.pid()=" << leaf.pid()
        << " key=" << key);
    return rc;
}

/*********************************************************************
 *
 *  btree_impl::_search(page, key, elem, found_key, total_match, slot)
 *
 *  Search page for <key, elem> and return status in found and slot.
 *  This handles special cases of +/- infinity, which are not always
 *  necessary to check. For example, when propagating splits & deletes,
 *  we don't need to check these cases, so we do a direct btree_p::search().
 *
 *  Context: use in traverse
 *
 *********************************************************************/
rc_t
btree_impl::_search(
    const btree_p&        page,
    const cvec_t&        key,
    const cvec_t&        elem,
    bool&                found_key,
    bool&                total_match,
    slotid_t&                ret_slot
)
{
    if (key.is_neg_inf())  {
        found_key = (total_match = false);
        ret_slot = 0;
    } else if (key.is_pos_inf()) {
        found_key = (total_match = false); 
        ret_slot = page.nrecs();
    } else {
        rc_t rc = page.search(key, elem, found_key, total_match, ret_slot);
        if (rc.is_error())  return rc.reset();
    }
    return RCOK;
}


/*
 * _satisfy(page, key, el, found_key, total_match, slot, whatcase)
 *
 * Look at page, tell if it contains a "satisfying key-el"
 * according to the definition for lookup (fetch).
 *
 *   which is to say, found an entry with the key
 *      exactly matching the given key .
 *   OR, if no such entry, if we found a next entry is on this page.
 * 
 * Return found_key = true if there's an entry with a matching key.
 * Return total_match if there's an entry with matching key, elem.
 * Return slot# of entry of interest -- it's the slot where
 *   such an element should *go* if it's not on the page.  In that
 *   case, this slot could be 1 past the last slot on the page.
 * Return whatcase for use in Mohan KVL-style search.
 *
 * Does NOT follow next page pointer
 */

rc_t
btree_impl::_satisfy(
    const btree_p&       page,
    const cvec_t&        key,
    const cvec_t&        elem,
    bool&                found_key,
    bool&                total_match,
    slotid_t&            slot,
    uint&                wcase
)
{
    m_page_search_cases whatcase;
    // BUGBUG: for the time being, this only supports
    // forward scans
    slot = -1;

    W_DO( _search(page, key, elem, found_key, total_match, slot));

    w_assert9(slot >= 0 && slot <= page.nrecs());
    if(total_match) {
        w_assert9(slot >= 0 && slot < page.nrecs());
    }

    DBG(
        << " on page " << page.pid()
        << " found_key = " << found_key 
        << " total_match=" << total_match
        << " slot=" << slot
        << " nrecs()=" << page.nrecs()
        );

    /*
     * Determine if we have a "satisfying key" according
     *  to Mohan's defintion: either key matches, or, if
     *  no such thing, is next key on the page?
     */
    if(total_match) {
        whatcase = m_satisfying_key_found_same_page;
    } else {
        // exact match not found, (but
        // key could have been found) 
        // slot points to the NEXT key,elem
        // see if that's on this page

        if(slot < page.nrecs()) {
            // positioned before end --> not empty page
            w_assert9(page.nrecs()>0);
            // next key is slot
            whatcase = m_satisfying_key_found_same_page;

         } else // positioned at end of page 
         if( !page.next() ) {
            // last page in file
            whatcase = m_not_found_end_of_file; 

         } else // positioned at end of page 
                // and not last page in file
         if(page.nrecs()) {
            // not empty page
            // next key is found on next leaf page
            w_assert9(page.next());
            whatcase = m_not_found_end_of_non_empty_page;

         } else // positioned at end of page 
                // and not last page in file
                // end empty page
         {
            w_assert9(page.next());
            whatcase = m_not_found_page_is_empty;
         }
    }
    wcase = (uint) whatcase;
    DBG(<<"returning case= " << int(whatcase));
    return RCOK;
}

