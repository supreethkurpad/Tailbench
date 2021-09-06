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

 $Id: btree.cpp,v 1.282 2010/06/15 17:30:07 nhall Exp $

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
#           pragma implementation "btree.h"
#           pragma implementation "btree_impl.old.h"
#endif

#include "sm_int_2.h"
#include "btree_p.h"
#include "btree_impl.h"
#include "btcursor.h"
#include "lexify.h"
#include "sm_du_stats.h"
#include <crash.h>

#if W_DEBUG_LEVEL > 0
#define  BTREE_LOG_COMMENT_ON 1
#else
#define  BTREE_LOG_COMMENT_ON 0
#endif

static 
rc_t badcc() {
    // for the purpose of breaking in gdb
    return RC(smlevel_0::eBADCCLEVEL);
}


/*********************************************************************
 *
 * Btree manager
 *
 ********************************************************************/

smsize_t                        
btree_m::max_entry_size() {
    return btree_p::max_entry_size;
}

/*********************************************************************
 *
 *  btree_m::create(stid_t, root)
 *
 *  Create a btree. Return the root page id in root.
 *
 *********************************************************************/
rc_t
btree_m::create(
    const stid_t&           stid,                
    lpid_t&                 root,                 // O-  root of new btree
    bool                    compressed,
    const bool              bIgnoreLatches)
{
    FUNC(btree_m::create);
    DBGTHRD(<<"stid " << stid);
#if BTREE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "btree create " << stid;
        W_DO(log_comment(s.c_str()));
    }
#endif
    latch_mode_t latch = LATCH_NL;
    if(!bIgnoreLatches) {
	get_latches(___s,___e); 
	check_latches(___s,___e, ___s+___e);
	latch = LATCH_EX;
    } 
    lsn_t anchor;
    xct_t* xd = xct();

    check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);

    if (xd)  anchor = xd->anchor();

    DBGTHRD(<<"allocating a page to store " << stid);

    X_DO( io->alloc_a_page(stid, 
            lpid_t::eof,  // hint
            root,        // resulting page
            true,        // may_realloc
            NL,          // acquires a lock on the page in this mode
            true        // search file
            ), anchor );
    SSMTEST("btree.create.1");

    {
	DBGTHRD(<<"formatting the page for store " << stid);
    
        btree_p page;
        /* Format/init the page: */
        X_DO( page.fix(root, latch, page.t_virgin), anchor );

        btree_p::flag_t f = compressed? btree_p::t_compressed: btree_p::t_none;
        X_DO( page.set_hdr(root.page, 1, 0, f), anchor );

#if W_DEBUG_LEVEL > 2
        if(compressed) {
            w_assert3(page.is_compressed());
        }
#endif 
    } // page is unfixed

    DBGTHRD(<<"compensatng the  page create for store " << stid);
    
    if (xd)  {
        SSMTEST("btree.create.2");
        xd->compensate(anchor, false/*not undoable*/ LOG_COMMENT_USE("btree.create.2"));
    }

    bool empty=false;
    W_DO(is_empty(root,empty));
    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e);
    }
    if(!empty) {
         DBGTHRD(<<"eNDXNOTEMPTY");
         return RC(eNDXNOTEMPTY);
    }
    DBGTHRD(<<"returning from btree_create, store " << stid);

    INC_TSTAT(page_btree_alloc);
	
    return RCOK;
}

/*********************************************************************
 *
 *  btree_m::is_empty(root, ret)
 *
 *  Return true in ret if btree at root is empty. false otherwise.
 *
 *********************************************************************/
rc_t
btree_m::is_empty(
    const lpid_t&        root,        // I-  root of btree
    bool&                 ret,        // O-  true if btree is empty
    const bool           bIgnoreLatches)
{
    if(!bIgnoreLatches) {
	get_latches(___s,___e); 
	check_latches(___s,___e, ___s+___e); 
    }
    key_type_s kc(key_type_s::b, 0, 4);
    cursor_t cursor(true);
    W_DO( fetch_init(cursor, root, 1, &kc, false, t_cc_none,
                     cvec_t::neg_inf, // bound1
                     cvec_t::neg_inf, // elem of bound1
                     ge,                // cond1
                     le,              // cond2
                     cvec_t::pos_inf, // bound2
                     SH,
		     bIgnoreLatches));
    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e); 
    }
    
    W_DO( fetch(cursor, bIgnoreLatches) );
    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e); 
    }
    ret = (cursor.key() == 0);
    return RCOK;
}


/*********************************************************************
 *
 *  btree_m::insert(root, unique, cc, key, el, split_factor)
 *
 *  Insert <key, el> into the btree. Split_factor specifies 
 *  percentage factor in spliting (if it occurs):
 *        60 means split 60/40 with 60% in left and 40% in right page
 *  A normal value for "split_factor" is 50. However, if I know
 *  in advance that I am going insert a lot of sorted entries,
 *  a high split factor would result in a more condensed btree.
 *
 *********************************************************************/
rc_t
btree_m::insert(
    const lpid_t&        root,                // I-  root of btree
    int                  nkc,
    const key_type_s*    kc,
    bool                 unique,                // I-  true if tree is unique
    concurrency_t        cc,                // I-  concurrency control 
    const cvec_t&        key,                // I-  which key
    const cvec_t&        el,                // I-  which element
    int                  split_factor)        // I-  tune split in %
{
#if BTREE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "btree insert " << root;
        W_DO(log_comment(s.c_str()));
    }
#endif
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();
    w_assert1(kc && nkc > 0);

    if(key.size() + el.size() > btree_p::max_entry_size) {
        DBGTHRD(<<"RECWONTFIT: key.size=" << key.size() 
                << " el.size=" << el.size());
        return RC(eRECWONTFIT);
    }
    rc_t rc;

    cvec_t* real_key;
    DBGTHRD(<<"");
    W_DO(_scramble_key(real_key, key, nkc, kc));
    DBGTHRD(<<"");
    
    // int retries = 0; // for debugging
 retry:
    rc = btree_impl::_insert(root, unique, cc, *real_key, el, split_factor);
    if(rc.is_error()) {
        if(rc.err_num() == eRETRY) {
            // retries++; // for debugging
            // fprintf(stderr, "-*-*-*- Retrying (%d) a btree insert!\n",
            //       retries);
            goto retry;
        }
        DBGTHRD(<<"rc=" << rc);
    }
    return  rc;
}


/*********************************************************************
 *
 *  btree_m::mr_insert(root, unique, cc, key, el, split_factor)
 *
 *  Same as normal btree insert except there is no scrambling of the 
 *  key value since it's already done.
 *
 *********************************************************************/
rc_t
btree_m::mr_insert(
    const lpid_t&        root,                // I-  root of btree
    bool                 unique,                // I-  true if tree is unique
    concurrency_t        cc,                // I-  concurrency control 
    const cvec_t&        key,                // I-  which key
    const cvec_t&        el,                // I-  which element
    int                  split_factor,        // I-  tune split in %
    const bool           bIgnoreLatches)
{
#if BTREE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "mrbtree insert " << root;
        W_DO(log_comment(s.c_str()));
    }
#endif
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();

    if(key.size() + el.size() > btree_p::max_entry_size) {
        DBGTHRD(<<"RECWONTFIT: key.size=" << key.size() 
                << " el.size=" << el.size());
        return RC(eRECWONTFIT);
    }
    rc_t rc;

    DBGTHRD(<<"");
    // int retries = 0; // for debugging
 retry:
    rc = btree_impl::_insert(root, unique, cc, key, el, split_factor, bIgnoreLatches);
    if(rc.is_error()) {
        if(rc.err_num() == eRETRY) {
            // retries++; // for debugging
            // fprintf(stderr, "-*-*-*- Retrying (%d) a btree insert!\n",
            //       retries);
            goto retry;
        }
        DBGTHRD(<<"rc=" << rc);
    }
    return  rc;
}

/*********************************************************************
 * 
 *  btree_m::mr_insert_l(root, unique, cc, key, el, split_factor)
 *
 *  Same as mr_insert except we might need to relocate records to enforce
 *  a heap page to be pointed by only one leaf page.
 *
 *********************************************************************/
rc_t
btree_m::mr_insert_l(
    const lpid_t&        root,                // I-  root of btree
    bool                 unique,                // I-  true if tree is unique
    concurrency_t        cc,                // I-  concurrency control 
    const cvec_t&        key,                // I-  which key
    //rc_t (*fill_el)(vec_t&, const lpid_t&), // I- callback function to determine the element
    el_filler*                       ef,
    size_t el_size,                         // I - size of the element
    int                  split_factor,        // I-  tune split in %
    const bool           bIgnoreLatches,
    RELOCATE_RECORD_CALLBACK_FUNC relocate_callback)
{
#if BTREE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "mrbtree insert " << root;
        W_DO(log_comment(s.c_str()));
    }
#endif
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();

    if(key.size() + el_size > btree_p::max_entry_size) {
        DBGTHRD(<<"RECWONTFIT: key.size=" << key.size() 
                << " el.size=" << el_size);
        return RC(eRECWONTFIT);
    }
    rc_t rc;

    DBGTHRD(<<"");
    // int retries = 0; // for debugging
 retry:
    rc = btree_impl::_mr_insert(root, unique, cc, key, ef, el_size, split_factor, 
				bIgnoreLatches, relocate_callback);
    if(rc.is_error()) {
        if(rc.err_num() == eRETRY) {
            // retries++; // for debugging
            // fprintf(stderr, "-*-*-*- Retrying (%d) a btree insert!\n",
            //       retries);
            goto retry;
        }
        DBGTHRD(<<"rc=" << rc);
    }
    return  rc;
}

/*********************************************************************
 *
 *  btree_m::mr_insert_p(root, unique, cc, key, el, split_factor)
 *
 *  Same as mr_insert except we might need to relocate records to enforce
 *  a heap page to be pointed by only one sub-tree.
 *
 *********************************************************************/
rc_t
btree_m::mr_insert_p(
    const lpid_t&        root,                // I-  root of btree
    bool                 unique,                // I-  true if tree is unique
    concurrency_t        cc,                // I-  concurrency control 
    const cvec_t&        key,                // I-  which key
    //rc_t (*fill_el)(vec_t&, const lpid_t&),  // I-  callback function to determine the element
    el_filler*                       ef,
    size_t el_size,                          // I - size of the element
    int                  split_factor,        // I-  tune split in %
    const bool           bIgnoreLatches)
{
#if BTREE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "mrbtree insert " << root;
        W_DO(log_comment(s.c_str()));
    }
#endif
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();

    if(key.size() + el_size > btree_p::max_entry_size) {
        DBGTHRD(<<"RECWONTFIT: key.size=" << key.size() 
                << " el.size=" << el_size);
        return RC(eRECWONTFIT);
    }
    rc_t rc;

    DBGTHRD(<<"");
    // int retries = 0; // for debugging
 retry:
    rc = btree_impl::_mr_insert(root, unique, cc, key, ef, el_size, split_factor, bIgnoreLatches);
    if(rc.is_error()) {
        if(rc.err_num() == eRETRY) {
            // retries++; // for debugging
            // fprintf(stderr, "-*-*-*- Retrying (%d) a btree insert!\n",
            //       retries);
            goto retry;
        }
        DBGTHRD(<<"rc=" << rc);
    }
    return  rc;
}

/*********************************************************************
 *
 *  btree_m::mr_remove(root, unique, cc, key, el)
 *
 *  Same as normal btree remove except there is no scrambling of the 
 *  key value since it's already done.
 *
 *********************************************************************/
rc_t
btree_m::mr_remove(
    const lpid_t&        root,        // root of btree
    bool                unique, // true if btree is unique
    concurrency_t        cc,        // concurrency control
    const cvec_t&        key,        // which key
    const cvec_t&        el,        // which el
    const bool           bIgnoreLatches)
{
#if BTREE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "btree remove " << root;
        W_DO(log_comment(s.c_str()));
    }
#endif

    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();

        DBGTHRD(<<"");
 retry:
	rc_t rc =  btree_impl::_remove(root, unique, cc, key, el, bIgnoreLatches);
    if(rc.is_error() && rc.err_num() == eRETRY) {
        //fprintf(stderr, "-*-*-*- Retrying a btree insert!\n");
        goto retry;
    }

    DBGTHRD(<<"rc=" << rc);
    return rc;
}

/*********************************************************************
 *
 *  btree_m::mr_remove_key(root, unique, cc, key, num_removed)
 *
 *  Same as normal btree remove_key except there is no scrambling of the 
 *  key value since it's already done.
 *
 *********************************************************************/
rc_t
btree_m::mr_remove_key(
    const lpid_t&        root,        // root of btree
    int                        nkc,
    const key_type_s*        kc,
    bool                unique, // true if btree is unique
    concurrency_t        cc,        // concurrency control
    const cvec_t&        key,        // which key
    int&                num_removed,
    const bool          bIgnoreLatches
)
{
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();

    num_removed = 0;

    /*
     *  We do this the dumb way ... optimization needed if this
     *  proves to be a bottleneck.
     */
    while (1)  {
        /*
         *  scan for key
         */
        cursor_t cursor(true);
        W_DO( fetch_init(cursor, root, nkc, kc, 
			 unique, cc, key, cvec_t::neg_inf,
			 ge, 
			 le, key, SH, bIgnoreLatches));
        W_DO( fetch(cursor, bIgnoreLatches) );
        if (!cursor.key()) {
            /*
             *  no more occurence of key ... done! 
             */
            break;
        }
        /*
         *  call btree_m::_remove() 
         */
        const cvec_t cursor_vec_tmp(cursor.elem(), cursor.elen());
        W_DO( mr_remove(root, unique, cc, key, cursor_vec_tmp, bIgnoreLatches));
        ++num_removed;

        if (unique) break;
    }
    if (num_removed == 0)  {
        fprintf(stderr, "could not find  key\n" );
        return RC(eNOTFOUND);
    }

    return RCOK;
}

/*********************************************************************
 *
 *  btree_m::mr_lookup(...)
 *
 *  Same as normal btree lookup except there is no scrambling of the 
 *  key value since it's already done.
 *
 *********************************************************************/
rc_t
btree_m::mr_lookup(
    const lpid_t&         root,        // I-  root of btree
    bool                  unique, // I-  true if btree is unique
    concurrency_t         cc,        // I-  concurrency control
    const cvec_t&         key,        // I-  key we want to find
    void*                 el,        // I-  buffer to put el found
    smsize_t&             elen,        // IO- size of el
    bool&                 found,        // O-  true if key is found
    const bool            bIgnoreLatches)
{
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();

    DBGTHRD(<<"");
    cvec_t null;
    W_DO( btree_impl::_lookup(root, unique, cc, key, null, found, 0, el, elen, bIgnoreLatches));
    return RCOK;
}

/*********************************************************************
 *
 *  btree_m::mr_update(...)
 *
 *  To update a value associated with a key.
 *  Right now used after record movement in mrbt_leaf and mrbt_part
 *  designs to update the rids (elements in the btrees) of the moved
 *  records in secondary indexes.
 *  The primary index is updated while record movement is taking place.
 *  Not used for regular btrees since they don't have such a concept as
 *  record movement now.
 *
 *********************************************************************/
rc_t
btree_m::mr_update(
    const lpid_t&         root,        // I-  root of btree
    bool                  unique, // I-  true if btree is unique
    concurrency_t         cc,        // I-  concurrency control
    const cvec_t&         key,        // I-  key we want to find
    const cvec_t&         old_el,        // I-  el to be updated
    const cvec_t&         new_el,        // I- new value of old_el
    bool&                 found,        // O-  true if key is found
    const bool            bIgnoreLatches)
{
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();

    DBGTHRD(<<"");
    W_DO( btree_impl::_update(root, unique, cc, key, old_el, new_el, found, bIgnoreLatches));
    return RCOK;
}

/*********************************************************************
 *
 *  btree_m::split_tree(root_old, root_new, key, leaf_old, leaf-new)
 *
 *  Split the tree starting from the given key.
 *
 *********************************************************************/
rc_t
btree_m::split_tree(
    const lpid_t&        root_old,          // I-  root of btree
    const lpid_t&        root_new,           // I- root of the new btree
    const cvec_t&        key,              // I-  which key
    lpid_t&              leaf_old,        // O - leaf whose contents are shifted to another leaf
    lpid_t&              leaf_new, // O - the new leaf
    const bool           bIgnoreLatches)                
{
#if BTREE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "btree split " << root_old;
        W_DO(log_comment(s.c_str()));
    }
#endif

    rc_t rc;

    DBGTHRD(<<"");    
    rc = btree_impl::_split_tree(root_old, root_new, key, leaf_old, leaf_new, bIgnoreLatches);
    
    return  rc;
}

/*********************************************************************
 *
 *  btree_m::relocate_recs_l(leaf_old, leaf_new)
 *
 *  For the second MRBT design to relocate records after a tree split.
 *
 *********************************************************************/
rc_t
btree_m::relocate_recs_l(
        lpid_t&                   leaf_old,
        const lpid_t&                   leaf_new,
	const bool bIgnoreLatches,
	RELOCATE_RECORD_CALLBACK_FUNC relocate_callback)
{
#if BTREE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "relocate records: leaf_old= " << leaf_old << " leaf_new=" << leaf_new;
        W_DO(log_comment(s.c_str()));
    }
#endif

    rc_t rc;

    DBGTHRD(<<"");    
    rc = btree_impl::_relocate_recs_l(leaf_old, leaf_new, false, bIgnoreLatches, relocate_callback);
    
    return  rc;
}

/*********************************************************************
 *
 *  btree_m::relocate_recs_p(root_old, root_new)
 *  For the third MRBT design to relocate records after a tree split.
 *
 *********************************************************************/
rc_t
btree_m::relocate_recs_p(
        const lpid_t&                   root_old,
        const lpid_t&                   root_new,
	const bool bIgnoreLatches,
	RELOCATE_RECORD_CALLBACK_FUNC relocate_callback)
{
#if BTREE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "relocate records: root_old=" << root_old << " root_new=" << root_new;
        W_DO(log_comment(s.c_str()));
    }
#endif

    rc_t rc;

    DBGTHRD(<<"");    
    rc = btree_impl::_relocate_recs_p(root_old, root_new, bIgnoreLatches, relocate_callback);
    
    return  rc;
}

/*********************************************************************
 *
 *  btree_m::merge_trees()
 *
 *  Merge two trees 
 *
 *********************************************************************/
rc_t
btree_m::merge_trees(
    lpid_t&             root,         // O- the root after merge
    const lpid_t&       root1,        // I- roots of the btrees to be merged
    const lpid_t&       root2,           
    cvec_t&             startKey2,    // I- starting boundary key for the second sub-tree (root2)
    const bool          update_owner,
    const bool          bIgnoreLatches)
{
#if BTREE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "btree merge roots " << root1 << " and " << root2;
        W_DO(log_comment(s.c_str()));
    }
#endif
    
    rc_t rc;
    rc = btree_impl::_merge_trees(root, root1, root2, startKey2, update_owner, bIgnoreLatches);
    
    return  rc;
}


/*********************************************************************
 *
 *  btree_m::remove_key(root, unique, cc, key, num_removed)
 *
 *  Remove all occurences of "key" in the btree, and return
 *  the number of entries removed in "num_removed".
 *
 *********************************************************************/
rc_t
btree_m::remove_key(
    const lpid_t&        root,        // root of btree
    int                        nkc,
    const key_type_s*        kc,
    bool                unique, // true if btree is unique
    concurrency_t        cc,        // concurrency control
    const cvec_t&        key,        // which key
    int&                num_removed
)
{
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();

    w_assert1(kc && nkc > 0);

    num_removed = 0;

    /*
     *  We do this the dumb way ... optimization needed if this
     *  proves to be a bottleneck.
     */
    while (1)  {
        /*
         *  scan for key
         */
        cursor_t cursor(true);
        W_DO( fetch_init(cursor, root, nkc, kc, 
                     unique, cc, key, cvec_t::neg_inf,
                     ge, 
                     le, key));
        W_DO( fetch(cursor) );
        if (!cursor.key()) {
            /*
             *  no more occurence of key ... done! 
             */
            break;
        }
        /*
         *  call btree_m::_remove() 
         */
        const cvec_t cursor_vec_tmp(cursor.elem(), cursor.elen());
        W_DO( remove(root, nkc, kc, unique, cc, key, cursor_vec_tmp));
        ++num_removed;

        if (unique) break;
    }
    if (num_removed == 0)  {
        fprintf(stderr, "could not find  key\n" );
        return RC(eNOTFOUND);
    }

    return RCOK;
}


/*********************************************************************
 *
 *  btree_m::remove(root, unique, cc, key, el)
 *
 *  Remove <key, el> from the btree.
 *
 *********************************************************************/
rc_t
btree_m::remove(
    const lpid_t&        root,        // root of btree
    int                        nkc,
    const key_type_s*        kc,
    bool                unique, // true if btree is unique
    concurrency_t        cc,        // concurrency control
    const cvec_t&        key,        // which key
    const cvec_t&        el)        // which el
{
#if BTREE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "btree remove " << root;
        W_DO(log_comment(s.c_str()));
    }
#endif
    w_assert1(kc && nkc > 0);

    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();

    cvec_t* real_key;
    DBGTHRD(<<"");
    W_DO(_scramble_key(real_key, key, nkc, kc));

    DBGTHRD(<<"");
 retry:
    rc_t rc =  btree_impl::_remove(root, unique, cc, *real_key, el);
    if(rc.is_error() && rc.err_num() == eRETRY) {
        //fprintf(stderr, "-*-*-*- Retrying a btree insert!\n");
        goto retry;
    }

    DBGTHRD(<<"rc=" << rc);
    return rc;
}

/*********************************************************************
 *
 *  btree_m::lookup(...)
 *
 *  Find key in btree. If found, copy up to elen bytes of the 
 *  entry element into el. 
 *
 *********************************************************************/
rc_t
btree_m::lookup(
    const lpid_t&         root,        // I-  root of btree
    int                   nkc,
    const key_type_s*     kc,
    bool                  unique, // I-  true if btree is unique
    concurrency_t         cc,        // I-  concurrency control
    const cvec_t&         key,        // I-  key we want to find
    void*                 el,        // I-  buffer to put el found
    smsize_t&             elen,        // IO- size of el
    bool&                 found,        // O-  true if key is found
    bool                  use_dirbuf)
{
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();

    w_assert1(kc && nkc > 0);
    cvec_t* real_key;
    DBGTHRD(<<"");
    W_DO(_scramble_key(real_key, key, nkc, kc, use_dirbuf));

    DBGTHRD(<<"");
    cvec_t null;
    W_DO( btree_impl::_lookup(root, unique, cc, *real_key, 
        null, found, 0, el, elen ));
    return RCOK;
}


/*
 * btree_m::lookup_prev() - find previous entry 
 * 
 * context: called by lid manager
 */ 
rc_t
btree_m::lookup_prev(
    const lpid_t&         root,        // I-  root of btree
    int                        nkc,
    const key_type_s*        kc,
    bool                unique, // I-  true if btree is unique
    concurrency_t        cc,        // I-  concurrency control
    const cvec_t&         keyp,        // I-  find previous key for key
    bool&                 found,        // O-  true if a previous key is found
    void*                 key_prev,        // I- is set to
                                        //    nearest one less than key
    smsize_t&                 key_prev_len // IO- size of key_prev
)        
{
    // bt->print(root, sortorder::kt_b);

    /* set up a backward scan from the keyp */
    bt_cursor_t * _btcursor = new bt_cursor_t(true);
    if (! _btcursor) {
        W_FATAL(eOUTOFMEMORY);
    }

    rc_t rc = bt->fetch_init(*_btcursor, root,
            nkc, kc,
            unique,
            cc,
            keyp, cvec_t::pos_inf, 
            le, ge, cvec_t::neg_inf);
    DBGTHRD(<<"rc=" << rc);
    if(rc.is_error()) return RC_AUGMENT(rc);
    
    W_DO( bt->fetch(*_btcursor) );
    DBGTHRD(<<"");
    found = (_btcursor->key() != 0);

    smsize_t        mn = (key_prev_len > (smsize_t)_btcursor->klen()) ? 
                            (smsize_t)_btcursor->klen() : key_prev_len;
    key_prev_len  = _btcursor->klen();
    DBGTHRD(<<"klen = " << key_prev_len);
    if(found) {
        memcpy( key_prev, _btcursor->key(), mn);
    }
    delete _btcursor;
    return RCOK;
}




/*********************************************************************
 *
 *  btree_m::fetch_init(cursor, root, numkeys, unique, 
 *        is-unique, cc, key, elem,
 *        cond1, cond2, bound2)
 *
 *  Initialize cursor for a scan for entries greater(less, if backward)
 *  than or equal to <key, elem>.
 *
 *********************************************************************/
rc_t
btree_m::fetch_init(
    cursor_t&                 cursor, // IO- cursor to be filled in
    const lpid_t&         root,        // I-  root of the btree
    int                        nkc,
    const key_type_s*        kc,
    bool                unique,        // I-  true if btree is unique
    concurrency_t        cc,        // I-  concurrency control
    const cvec_t&         ukey,        // I-  <key, elem> to start
    const cvec_t&         elem,        // I-
    cmp_t                cond1,        // I-  condition on lower bound
    cmp_t                cond2,        // I-  condition on upper bound
    const cvec_t&        bound2,        // I-  upper bound
    lock_mode_t                mode,        // I-  mode to lock index keys in
    const bool          bIgnoreLatches)
{
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();
    w_assert1(kc && nkc > 0);
    if(!bIgnoreLatches) {
	get_latches(___s,___e); 
	check_latches(___s,___e, ___s+___e); 
    }
    INC_TSTAT(bt_scan_cnt);

    /*
     *  Initialize constant parts of the cursor
     */
    cvec_t* key;
    cvec_t* bound2_key;

    DBGTHRD(<<"");
    W_DO(_scramble_key(bound2_key, bound2, nkc, kc));
    W_DO(cursor.set_up(root, nkc, kc, unique, cc, 
                       cond2, *bound2_key, mode));

    DBGTHRD(<<"");
    W_DO(_scramble_key(key, ukey, nkc, kc));
    W_DO(cursor.set_up_part_2( cond1, *key));

    /*
     * GROT: For scans: TODO
     * To handle backward scans from scan.cpp, we have to
     * reverse the elem in the backward case: replace it with
        elem = &(inclusive ? cvec_t::pos_inf : cvec_t::neg_inf);
     */

    cursor.first_time = true;

    if((cc == t_cc_modkvl) ) {
        /*
         * only allow scans of the form ==x ==x
         * and grab a SH lock on x, whether or not
         * this is a unique index.
         */
        if(cond1 != eq || cond2 != eq) {
            return RC(eBADSCAN);
        }
        lockid_t k;
        btree_impl::mk_kvl(cc, k, root.stid(), true, *key);
        // wait for commit-duration share lock on key
        W_DO (lm->lock(k, mode, t_long));
    }

    bool         found=false;
    smsize_t         elen = elem.size();

    DBGTHRD(<<"Scan is backward? " << cursor.is_backward());

    W_DO (btree_impl::_lookup( cursor.root(), cursor.unique(), cursor.cc(),
			       *key, elem, found, &cursor, cursor.elem(), elen, bIgnoreLatches));

    DBGTHRD(<<"found=" << found);

    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e); 
    }
    return RCOK;
}


/*********************************************************************
 *
 *  btree_m::mr_fetch_init(cursor, roots, numkeys, unique, 
 *        is-unique, cc, key, elem,
 *        cond1, cond2, bound2)
 *
 *  Initialize cursor for a scan for entries greater(less, if backward)
 *  than or equal to <key, elem>.
 *
 *********************************************************************/
rc_t
btree_m::mr_fetch_init(
    cursor_t&                 cursor, // IO- cursor to be filled in
    vector<lpid_t>&         roots,        // I-  roots of the subtrees
    int                        nkc,
    const key_type_s*        kc,
    bool                unique,        // I-  true if btree is unique
    concurrency_t        cc,        // I-  concurrency control
    const cvec_t&         ukey,        // I-  <key, elem> to start
    const cvec_t&         elem,        // I-
    cmp_t                cond1,        // I-  condition on lower bound
    cmp_t                cond2,        // I-  condition on upper bound
    const cvec_t&        bound2,        // I-  upper bound
    lock_mode_t                mode,        // I-  mode to lock index keys in
    const bool          bIgnoreLatches)
{
    if(
        (cc != t_cc_none) && (cc != t_cc_file) &&
        (cc != t_cc_kvl) && (cc != t_cc_modkvl) &&
        (cc != t_cc_im) 
        ) return badcc();
    w_assert1(kc && nkc > 0);
    if(!bIgnoreLatches) {
	get_latches(___s,___e); 
	check_latches(___s,___e, ___s+___e); 
    }
    INC_TSTAT(bt_scan_cnt);

    lpid_t root = roots.back();
    
    /*
     *  Initialize constant parts of the cursor
     */

    DBGTHRD(<<"");
    W_DO(cursor.set_up(root, nkc, kc, unique, cc, 
                       cond2, bound2, mode));

    DBGTHRD(<<"");
    W_DO(cursor.set_up_part_2( cond1, ukey));

    cursor.set_roots(roots);

    /*
     * GROT: For scans: TODO
     * To handle backward scans from scan.cpp, we have to
     * reverse the elem in the backward case: replace it with
        elem = &(inclusive ? cvec_t::pos_inf : cvec_t::neg_inf);
     */

    cursor.first_time = true;

    if((cc == t_cc_modkvl) ) {
        /*
         * only allow scans of the form ==x ==x
         * and grab a SH lock on x, whether or not
         * this is a unique index.
         */
        if(cond1 != eq || cond2 != eq) {
            return RC(eBADSCAN);
        }
        lockid_t k;
        btree_impl::mk_kvl(cc, k, root.stid(), true, ukey);
        // wait for commit-duration share lock on key
        W_DO (lm->lock(k, mode, t_long));
    }

    bool         found=false;
    smsize_t         elen = elem.size();

    DBGTHRD(<<"Scan is backward? " << cursor.is_backward());

    W_DO (btree_impl::_lookup( cursor.root(), cursor.unique(), cursor.cc(),
			       ukey, elem, found, &cursor, cursor.elem(), elen, bIgnoreLatches));

    DBGTHRD(<<"found=" << found);

    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e); 
    }
    return RCOK;
}


/*********************************************************************
 *
 *  btree_m::fetch_reinit(cursor)
 *
 *  Reinitialize cursor for a scan.
 *  If need be, it unconditionally grabs a share latch on the whole tree
 *
 *********************************************************************/
rc_t
btree_m::fetch_reinit(
    cursor_t&                 cursor, // IO- cursor to be filled in
    const bool                bIgnoreLatches
) 
{
    smsize_t    elen = cursor.elen();
    bool        found = false;

    if(!bIgnoreLatches) {
	get_latches(___s,___e); 
	check_latches(___s,___e, ___s+___e); 
    }
    
    // reinitialize the cursor
    // so that the _fetch_init
    // will do a make_rec() to evaluate the correctness of the
    // slot it finds; make_rec()  updates the cursor.slot(). If
    // don't do this, make_rec() doesn't get called in _fetch_init,
    // and then the cursor.slot() doesn't get updated, but the
    // reason our caller called us was to get cursor.slot() updated.
    cursor.first_time = true;
    cursor.keep_going = true;

    cvec_t* real_key;
    DBGTHRD(<<"");
    cvec_t key(cursor.key(), cursor.klen());
    W_DO(_scramble_key(real_key, key, cursor.nkc(), cursor.kc()));

    const cvec_t cursor_vec_tmp(cursor.elem(), cursor.elen());
    rc_t rc= btree_impl::_lookup(
        cursor.root(), cursor.unique(), cursor.cc(),
        *real_key,
        cursor_vec_tmp,
        found,
        &cursor, 
        cursor.elem(), elen,
        bIgnoreLatches);
    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e); 
    }
    return rc;
}


/*********************************************************************
 *
 *  btree_m::fetch(cursor)
 *
 *  Fetch the next key of cursor, and advance the cursor.
 *  This is Mohan's "fetch_next" operation.
 *
 *********************************************************************/
rc_t
btree_m::fetch(cursor_t& cursor, const bool bIgnoreLatches)
{
    FUNC(btree_m::fetch);
    bool __eof = false;
    bool __found = false;

    if(!bIgnoreLatches) {
	get_latches(___s,___e); 
	check_latches(___s,___e, ___s+___e);
    }
    DBGTHRD(<<"first_time=" << cursor.first_time
        << " keep_going=" << cursor.keep_going);

    latch_mode_t mode = LATCH_SH;
    
    if (cursor.first_time)  {
        /*
         *  Fetch_init() already placed cursor on
         *  first key satisfying the first condition.
         *  Check the 2nd condition.
         */

        cursor.first_time = false;
        if(cursor.key()) {
            //  either was in_bounds or keep_going is true
            if( !cursor.keep_going ) {
		// OK- satisfies both
		return RCOK;
            }
            // else  keep_going
        } else {
            // no key - wasn't in both bounds
            return RCOK;
        }

        w_assert3(cursor.keep_going);
    }
    if(!bIgnoreLatches) {
       check_latches(___s,___e, ___s+___e); 
    }
    /*
     *  We now need to move cursor one slot to the right
     */
    stid_t stid = cursor.root().stid();
    slotid_t  slot = -1;
    rc_t rc;

  again: 
    DBGTHRD(<<"fetch.again is_valid=" << cursor.is_valid());
    {
        btree_p p1, p2;
        w_assert3(!p2.is_fixed());
	if(!bIgnoreLatches) {
	    check_latches(___s,___e, ___s+___e); 
	}
	
        while (cursor.is_valid()) {
            /*
             *  Fix the cursor page. If page has changed (lsn
             *  mismatch) then call fetch_init to re-traverse.
             */
	    mode = LATCH_SH;
	    if(bIgnoreLatches) {
		mode = LATCH_NL;
	    }
            W_DO( p1.fix(cursor.pid(), mode) );
            if (cursor.lsn() == p1.lsn())  {
                break;
            }
            p1.unfix();
            W_DO(fetch_reinit(cursor, bIgnoreLatches)); // re-traverses the tree
            cursor.first_time = false;
            // there exists a possibility for starvation here.
            goto again;
        }

        slot = cursor.slot();
        if (cursor.is_valid())  {
            w_assert3(p1.pid() == cursor.pid());
            btree_p* child = &p1;        // child points to p1 or p2

            /*
             *  Move one slot to the right(left if backward scan)
             *  NB: this does not do the Mohan optimizations
             *  with all the checks, since we can't really
             *  tell if the page is still in the btree.
             */
            w_assert3(p1.is_fixed());
            w_assert3(!p2.is_fixed());
            W_DO(btree_impl::_skip_one_slot(p1, p2, child, 
					    slot, __eof, __found, cursor.is_backward(), bIgnoreLatches));

            w_assert3(child->is_fixed());
            w_assert3(child->is_leaf());


	    if(__eof && cursor.is_mrbt) {
		if(cursor.get_next_root()) {
		    p1.unfix();
		    lpid_t pid = cursor.root();
		    btree_p page;
		    W_DO( page.fix(pid, mode) );
		    while(page.level() > 1) {
			pid.page = page.pid0();
			page.unfix();
			W_DO( page.fix(pid, mode) );
		    }
		    cursor.set_slot(0);
		    cursor.set_pid(pid);
		    cursor.update_lsn(page);
		    goto again;
		}
	    }


            if(__eof) {
                w_assert3(slot >= child->nrecs() || (cursor.is_backward()));
                cursor.free_rec();

            } else if(!__found ) {
                // we encountered a deleted page
                // grab a share latch on the tree and try again

                // unconditional
                tree_latch tree_root(child->root());
		mode = LATCH_SH;
		if(bIgnoreLatches) {
		    mode = LATCH_NL;
		}
                w_error_t::err_num_t rce =
                   tree_root.get_for_smo(false, mode,
                            *child, mode, false, 
                                child==&p1? &p2 : &p1, LATCH_NL, bIgnoreLatches);
                if(rce) return RC(rce);

                p1.unfix();
                p2.unfix();
                tree_root.unfix();
                W_DO(fetch_reinit(cursor, bIgnoreLatches)); // re-traverses the tree
                cursor.first_time = false;
                DBGTHRD(<<"-->again TREE LATCH MODE "
                            << int(tree_root.latch_mode())
                            );
                goto again;

            } else {
                w_assert3(__found) ;
                w_assert3(slot >= 0);
                w_assert3(slot < child->nrecs());

                // Found next item, and it fulfills lower bound
                // requirement.  What about upper bound?
                /*
                 *  Point cursor to satisfying key
                 */
                W_DO( cursor.make_rec(*child, slot) );
                if(cursor.keep_going) {
                    // keep going until we reach the
                    // first satisfying key.
                    // This should only happen if we
                    // have something like:
                    // >= x && == y where y > x
                    p1.unfix();
                    p2.unfix();
                    DBGTHRD(<<"->again");
                    goto again; // leaving scope unfixes pages
                }
            }

            w_assert3(child->is_fixed());
            w_assert3(child->is_leaf());
            if(__eof) {
                w_assert3(slot >= child->nrecs() || (cursor.is_backward()));
            } else {
                w_assert3(slot < child->nrecs());
                w_assert3(slot >= 0);
            }

            /*
             * NB: scans really shouldn't be done with the
             * t_cc_mod*, but we're allowing them in restricted
             * circumstances.
             */
            if (cursor.cc() != t_cc_none) {
                /*
                 *  Get KVL locks
                 */
                lockid_t kvl;
                if (slot >= child->nrecs())  {
                    btree_impl::mk_kvl_eof(cursor.cc(), kvl, stid);
                } else {
                    w_assert3(slot < child->nrecs());
                    btrec_t r(*child, slot);
                    btree_impl::mk_kvl(cursor.cc(), kvl, stid, cursor.unique(), r);
                }
                rc = lm->lock(kvl, SH, t_long, WAIT_IMMEDIATE);
                if (rc.is_error())  {
                    DBGTHRD(<<"rc=" << rc);
                    w_assert3((rc.err_num() == eLOCKTIMEOUT) || (rc.err_num() == eDEADLOCK));

                    lpid_t pid = child->pid();
                    lsn_t lsn = child->lsn();
                    p1.unfix();
                    p2.unfix();
                    W_DO( lm->lock(kvl, SH, t_long) );
		    mode = LATCH_SH;
		    if(bIgnoreLatches) {
			mode = LATCH_NL;
		    }
                    W_DO( child->fix(pid, mode) );
                    if (lsn == child->lsn() && child == &p1)  {
                        ;
                    } else {
                        DBGTHRD(<<"->again");
                        goto again;
                    }
                } // else got lock 

            } 

        } // if cursor.is_valid()
    }
    DBGTHRD(<<"returning, is_valid=" << cursor.is_valid());
    if(!bIgnoreLatches) {
	check_latches(___s,___e, ___s+___e);
    }
    return RCOK;
}


/*********************************************************************
 *
 *  btree_m::get_du_statistics(root, stats, audit)
 *
 *********************************************************************/
rc_t
btree_m::get_du_statistics(
    const lpid_t&        root,
    btree_stats_t&        _stats,
    bool                 audit)
{
    lpid_t pid = root;
    lpid_t child = root;
    child.page = 0;

    base_stat_t        lf_cnt = 0;
    base_stat_t        int_cnt = 0;
    base_stat_t        level_cnt = 0;

    btree_p page[2];
    int c = 0;

    /*
       Traverse the btree gathering stats.  This traversal scans across
       each level of the btree starting at the root.  Unfortunately,
       this scan misses "unlinked" pages.  Unlinked pages are empty
       and will be free'd during the next top-down traversal that
       encounters them.  This traversal should really be DFS so it
       can find "unlinked" pages, but we leave it as is for now.
       We account for the unlinked pages after the traversal.
    */
    do {
        btree_lf_stats_t        lf_stats;
        btree_int_stats_t       int_stats;

        W_DO( page[c].fix(pid, LATCH_SH) );
        if (page[c].level() > 1)  {
            int_cnt++;;
            W_DO(page[c].int_stats(int_stats));
            if (audit) {
                W_DO(int_stats.audit());
            }
            _stats.int_pg.add(int_stats);
        } else {
            lf_cnt++;
            W_DO(page[c].leaf_stats(lf_stats));
            if (audit) {
                W_DO(lf_stats.audit());
            }
            _stats.leaf_pg.add(lf_stats);
        }
        if (page[c].prev() == 0)  {
            child.page = page[c].pid0();
            level_cnt++;
        }
        if (! (pid.page = page[c].next()))  {
            pid = child;
            child.page = 0;
        }
        c = 1 - c;

        // "following" a link here means fixing the page,
        // which we'll do on the next loop through, if pid.page
        // is non-zero
        INC_TSTAT(bt_links);
    } while (pid.page);

    // count unallocated pages
    rc_t rc;
    bool allocated;
    base_stat_t alloc_cnt = 0;
    base_stat_t unlink_cnt = 0;
    base_stat_t unalloc_cnt = 0;
    rc = io->first_page(root.stid(), pid, &allocated);
    while (!rc.is_error()) {
        // no error yet;
        if (allocated) {
            alloc_cnt++;
        } else {
            unalloc_cnt++;
        }
        rc = io->next_page(pid, &allocated);
    }
    unlink_cnt = alloc_cnt - (lf_cnt + int_cnt);
    if (rc.err_num() != eEOF) return rc;

    if (audit) {
        if (!((alloc_cnt+unalloc_cnt) % smlevel_0::ext_sz == 0)) {
#if W_DEBUG_LEVEL > 0
            fprintf(stderr, 
                "alloc_cnt %lld + unalloc_cnt %lld not a mpl of ext size\n",
                (long long) alloc_cnt, (long long) unalloc_cnt);
#endif
            return RC(fcINTERNAL);
        }
        if (!((lf_cnt + int_cnt + unlink_cnt + unalloc_cnt) % 
                        smlevel_0::ext_sz == 0)) {
#if W_DEBUG_LEVEL > 0
            fprintf(stderr, 
            "lf_cnt %lld + int_cnt %lld + unlink_cnt %lld + unalloc_cnt %lld not a mpl of ext size\n",
            (long long) lf_cnt, (long long) int_cnt, 
            (long long) unlink_cnt, (long long) unalloc_cnt);
#endif
            return RC(fcINTERNAL);
        }

        // I think if audit is true, and we have the right locks,
        // there should be no unlinked pages
        if ( unlink_cnt != 0) {
            fprintf(stderr, " found %lu unlinked pages\n", 
                    // make it work for LP32
                    (unsigned long) unlink_cnt);
            return RC(fcINTERNAL);
        }

    }

    _stats.unalloc_pg_cnt += unalloc_cnt;
    _stats.unlink_pg_cnt += unlink_cnt;
    _stats.leaf_pg_cnt += lf_cnt;
    _stats.int_pg_cnt += int_cnt;
    _stats.level_cnt = MAX(_stats.level_cnt, level_cnt);
    return RCOK;
}



/*********************************************************************
 *
 *  btree_m::_scramble_key(ret, key, nkc, kc)
 *  btree_m::_unscramble_key(ret, key, nkc, kc)
 *
 *  These functions put/pull a key into/from lexicographic order.
 *  _scramble checks for some legitimacy of the values, too, namely
 *  length.
 *
 *********************************************************************/
rc_t
btree_m::_scramble_key(
    cvec_t*&                 ret,
    const cvec_t&         key, 
    int                 nkc,
    const key_type_s*         kc,
    bool                use_dirbuf)
{
    FUNC(btree_m::_scramble_key);
    DBGTHRD(<<" SCrambling " << key );
    w_assert1(kc && nkc > 0);

    if (&key == &key.neg_inf || &key == &key.pos_inf)  {
        ret = (cvec_t*) &key;
        return RCOK;
    }
    if(key.size() == 0) {
        // do nothing
        ret = (cvec_t*) &key;
        return RCOK;
    }


    ret = me()->get_kc_vec(use_dirbuf);
    ret->reset();

    char* p = 0;
    for (int i = 0; i < nkc; i++)  {
        key_type_s::type_t t = (key_type_s::type_t) kc[i].type;
        if (
            t == key_type_s::i || 
            t == key_type_s::I || 
            t == key_type_s::u ||
            t == key_type_s::U ||
            t == key_type_s::f ||
            t == key_type_s::F 
            ) {
            p = me()->get_kc_buf(use_dirbuf);
            break;
        }
    }

    if (! p)  {
        // found only uninterpreted bytes (b, b*)
        ret->put(key);
    } else {
        // s,p are destination
        // key contains source -- unfortunately,
        // it's not necessarily contiguous. 
        char *src=0;
        char *malloced=0;
        if(key.count() > 1) {
            malloced = new char[key.size()];
            w_assert1(malloced);
            key.copy_to(malloced);
            src= malloced;
        } else {
            const vec_t *v = (const vec_t *)&key;
            src= (char *)(v->ptr(0));
        }
        char* s = p;

        int   key_remaining = key.size();
        for (int i = 0; i < nkc; i++)  {
            DBGTHRD(<<"len " << kc[i].length);
            if(!kc[i].variable) {
                key_remaining -= kc[i].length;
                if(key_remaining <0) {
                    if(malloced) delete[] malloced;
                    return RC(eBADKEY);
                }

                // Can't check and don't care for variable-length
                // stuff:
                if( (char *)(alignon(((ptrdiff_t)src), (kc[i].length))) != src) {
                    // 8-byte things (floats) only have to be 4-byte 
                    // aligned on some machines.  TODO (correctness): figure out
                    // which allow this and which, if any, don't.
                    /* XXX */
                    if(kc[i].length <= 4) {
                        if(malloced) delete[] malloced;
                        cout << "Unaligned, scramble, ptr=" << (void*)src
                                << ", align=" << kc[i].length << endl;
                        return RC(eALIGNPARM);
                    }
                }
            }
            if( !SortOrder.lexify(&kc[i], src, s) )  {
                w_assert3(kc[i].variable);
                // must be the last item in the list
                w_assert3(i == nkc-1);
                // copy the rest
                DBGTHRD(<<"lexify failed-- doing memcpy of " 
                        << key.size() - (s-p) 
                        << " bytes");
                memcpy(s, src, key.size() - (s-p));
            }
            src += kc[i].length;
            s += kc[i].length;
        }
        if(malloced) delete[] malloced;
        src = 0;
        if(nkc == 1 && kc[0].type == key_type_s::b) {
            // prints bogosities if type isn't a string
            DBGTHRD(<<" ret->put(" << p << "," << (int)(s-p) << ")");
        }
        ret->put(p, s - p);
    }
    DBGTHRD(<<" Scrambled " << key << " into " << *ret);
    return RCOK;
}


rc_t
btree_m::_unscramble_key(
    cvec_t*&                 ret,
    const cvec_t&         key, 
    int                 nkc,
    const key_type_s*         kc,
    bool                 use_dirbuf)
{
    FUNC(btree_m::_unscramble_key);
    DBGTHRD(<<" UNscrambling " << key );
    w_assert1(kc && nkc > 0);
    ret = me()->get_kc_vec(use_dirbuf);
    ret->reset();
    char* p = 0;
    int i;
#ifdef W_TRACE
    for (i = 0; i < nkc; i++)  {
        DBGTHRD(<<"key type is " << kc[i].type);
    }
#endif 
    for (i = 0; i < nkc; i++)  {
        key_type_s::type_t t = (key_type_s::type_t) kc[i].type;
        if (
                t == key_type_s::i || 
                t == key_type_s::I || 
                t == key_type_s::U ||
                t == key_type_s::u ||
                t == key_type_s::f ||
                t == key_type_s::F 
                )  {
            p = me()->get_kc_buf(use_dirbuf);
            break;
        }
    }
    if (! p)  {
        ret->put(key);
    } else {
        // s,p are destination
        // key contains source -- unfortunately,
        // it's not contiguous. 
        char *src=0;
        char *malloced=0;
        if(key.count() > 1) {
            malloced = new char[key.size()];
            key.copy_to(malloced);
            src= malloced;
        } else {
            const vec_t *v = (const vec_t *)&key;
            src= (char *)v->ptr(0);
        }
        char* s = p;
        if(src) for (i = 0; i < nkc; i++)  {
            if(! kc[i].variable) {
                // Can't check and don't care for variable-length
                // stuff:
                int len = kc[i].length;
                // only require 4-byte alignment for doubles
                /* XXXX */
                if(len == 8) { len = 4; }
                if(len == 4 && (char *)(alignon(((ptrdiff_t)src), len)) != src) {
                    cerr << "Invalid alignment, unscramble, ptr="
                        << (void*)src << ", align=" << len << endl;
                    return RC(eALIGNPARM);
                }
            }
            if( !SortOrder.unlexify(&kc[i], src, s) )  {
                w_assert3(kc[i].variable);
                // must be the last item in the list
                w_assert3(i == nkc-1);
                // copy the rest
                DBGTHRD(<<"unlexify failed-- doing memcpy of " 
                        << key.size() - (s-p) 
                        << " bytes");
                memcpy(s, src, key.size() - (s-p));
            }
            src += kc[i].length;
            s += kc[i].length;
        }
        if(malloced) delete[] malloced;
        src = 0;
        if(nkc == 1 && kc[0].type == key_type_s::b) {
            DBGTHRD(<<" ret->put(" << p << "," << (int)(s-p) << ")");
        }
        ret->put(p, s - p);
    }
    DBGTHRD(<<" UNscrambled " << key << " into " << *ret);
    return RCOK;
}

/*********************************************************************
 *
 *  btree_m::print(root, sortorder::keytype kt = sortorder::kt_b,
 *          bool print_elem=false);
 *
 *  Print the btree (for debugging only)
 *
 *********************************************************************/
void 
btree_m::print(const lpid_t& root, 
    sortorder::keytype kt,
    bool print_elem 
)
{
    lpid_t nxtpid, pid0;
    nxtpid = pid0 = root;
    {
        btree_p page;
        W_COERCE( page.fix(root, LATCH_SH) ); // coerce ok-- debugging

        for (int i = 0; i < 5 - page.level(); i++) {
            cout << '\t';
        }
        cout 
             << (page.is_smo() ? "*" : " ")
             << (page.is_delete() ? "D" : " ")
             << " "
             << "LEVEL " << page.level() 
             << ", page " << page.pid().page 
             << ", prev " << page.prev()
             << ", next " << page.next()
             << ", nrec " << page.nrecs()
             << endl;
        page.print(kt, print_elem);
        cout << flush;
        if (page.next())  {
            nxtpid.page = page.next();
        }

        if ( ! page.prev() && page.pid0())  {
            pid0.page = page.pid0();
        }
    }
    if (nxtpid != root)  {
        print(nxtpid, kt, print_elem);
    }
    if (pid0 != root) {
        print(pid0, kt, print_elem);
    }
}
/* 
 * for use by logrecs for undo, redo
 */
rc_t                        
btree_m::_insert(
    const lpid_t&                     root,
    bool                             unique,
    concurrency_t                    cc,
    const cvec_t&                     key,
    const cvec_t&                     elem,
    int                             split_factor) 
{
    return btree_impl::_insert(root,unique,cc, key, elem, split_factor);
}

rc_t                        
btree_m::_remove(
    const lpid_t&                    root,
    bool                             unique,
    concurrency_t                    cc,
    const cvec_t&                     key,
    const cvec_t&                     elem)
{
    return btree_impl::_remove(root,unique,cc, key, elem);
}

