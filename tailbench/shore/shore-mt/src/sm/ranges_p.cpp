/* -*- mode:C++; c-basic-offset:4 -*-
   Shore-kits -- Benchmark implementations for Shore-MT

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

/** @file:   ranges_p.cpp
 *
 *  @brief:  Implementation of the ranges_p functions.
 *
 *  @date:   August 2010
 *
 *  @author: Pinar Tozun (pinar)
 *  @author: Ippokratis Pandis (ipandis)
 *  @author: Ryan Johnson (ryanjohn)
 */

#define SM_SOURCE
#define RANGES_C

#ifdef __GNUG__
#       pragma implementation "ranges_p.h"
#endif

#include "sm_int_2.h"

#include "ranges_p.h"


rc_t ranges_m::create(const stid_t stid, lpid_t& pid, const lpid_t& subroot) 
{
    W_DO(io->alloc_a_page(stid, lpid_t::eof, pid, true, IX, true));
    ranges_p page;
    W_DO(page.fix(pid, LATCH_EX, page.t_virgin)); 
    // add one subtree to ranges 
    cvec_t startKey;
    int i = 0;
    startKey.put((char*)(&i),sizeof(i));
    W_DO( page.add_default_partition(startKey, subroot) );
    page.unfix();
    return RCOK;
}

rc_t ranges_m::create(const stid_t stid, lpid_t& pid, vector<cvec_t*>& keys, vector<lpid_t>& subroots) 
{
    W_DO(io->alloc_a_page(stid, lpid_t::eof, pid, true, IX, true));
    ranges_p page;
    W_DO(page.fix(pid, LATCH_EX, page.t_virgin)); 
    // add the initial ranges 
    W_DO( page.fill_page(keys, subroots) );
    page.unfix();
    return RCOK;
}
   
rc_t ranges_m::add_partition(const lpid_t& pid, cvec_t& key, const lpid_t& root) 
{
    ranges_p page;
    W_DO(page.fix(pid, LATCH_EX));
    W_DO(page.add_partition(key, root));
    page.unfix();
    return RCOK;
}

rc_t ranges_m::delete_partition(const lpid_t& pid, const lpid_t& root_to_delete,
				const lpid_t& root_to_update_old, const lpid_t& root_to_update_new)
{
    ranges_p page;
    W_DO(page.fix(pid, LATCH_EX));
    W_DO(page.delete_partition(root_to_delete, root_to_update_old, root_to_update_new));
    page.unfix();
    return RCOK;
}
 
rc_t ranges_m::fill_ranges_map(const lpid_t& pid, key_ranges_map& partitions)
{
    ranges_p page;
    W_DO(page.fix(pid, LATCH_SH));
    W_DO(page.fill_ranges_map(partitions));
    page.unfix();
    return RCOK;
}

rc_t ranges_m::fill_page(const lpid_t& pid, key_ranges_map& partitions) 
{
    ranges_p page;
    W_DO(page.fix(pid, LATCH_EX));
    W_DO(page.fill_page(partitions));
    page.unfix();
    return RCOK;
}

MAKEPAGECODE(ranges_p, page_p)

rc_t ranges_p::fill_ranges_map(key_ranges_map& partitions)
{
    // get the contents of the header
    char* hdr_ptr = (char*) page_p::tuple_addr(0);
    uint4_t num_pairs = *((uint4_t*)hdr_ptr);
    //
    int current_slot_size = 0;
    for(uint4_t i=1; i <= num_pairs; i++) {
	current_slot_size = page_p::tuple_size(i);
	if (current_slot_size != 0) {
	    // get the contents of the slot
	    char* pair = (char*) page_p::tuple_addr(i);
	    cvec_t pair_vec;
	    pair_vec.put(pair, current_slot_size);
	    // split it to its key-root parts
	    cvec_t root_vec;
	    cvec_t key;
	    pair_vec.split(sizeof(lpid_t), root_vec, key);
	    lpid_t root_id;
	    root_vec.copy_to(&root_id, sizeof(lpid_t));
	    // add this pair to the partitions map
	    partitions.addPartition(key, root_id);
	}
    }

    return RCOK;
}

rc_t ranges_p::fill_page(key_ranges_map& partitions)
{
    key_ranges_map::KRMap partitions_map = partitions.getMap();

    page_p::mark_free(1);
    uint4_t i = 1;
 
    // put rest of the partitions' info
    for(key_ranges_map::KRMapIt iter = partitions_map.begin();
	iter != partitions_map.end();
	iter++, i++) {
	cvec_t v;
	// put subroot
	v.put((char*)&iter->second, sizeof(lpid_t));
	// put key
        v.put((*iter).first._m,(*iter).first._len);
	// add this key-subroot pair to page's data
	W_DO(page_p::reclaim(i, v, true));
    }

    // header of the page keeps how many startKey-root pairs are stored
    cvec_t hdr;
    hdr.put((char*)(&i), sizeof(uint4_t));
    W_DO(page_p::overwrite(0, 0, hdr));

    return RCOK;
}

rc_t ranges_p::fill_page(vector<cvec_t*>& keys, vector<lpid_t>& subroots)
{
    // pin: should only be called for initial partitions
    //map<char*, lpid_t, cmp_greater> partitions_map = partitions.getMap();
    uint4_t i = 0;
    // put the partitions' info 
/*
    for(key_ranges_map::keysIter iter = partitions_map.begin(); iter != partitions_map.end(); iter++, i++) {
	cvec_t v;
	// put subroot
	v.put((char*)(&subroots[i-1]), sizeof(lpid_t));
	// put key
	v.put(iter->first, sizeof(iter->first));
	// add this key-subroot pair to page's data
	W_DO(page_p::reclaim(i, v, true));
    } */

    for(; i < keys.size(); i++) {
	cvec_t v;
	// put subroot
	v.put((char*)(&subroots[i]), sizeof(lpid_t));
	// put key
	v.put(*(keys[i]));
	// add this key-subroot pair to page's data
	W_DO(page_p::reclaim(i+1, v, true));
    }

    // header of the page keeps how many startKey-root pairs are stored
    cvec_t hdr;
    hdr.put((char*)(&i), sizeof(uint4_t));
    W_DO(page_p::overwrite(0, 0, hdr));

    return RCOK;
}

rc_t ranges_p::add_partition(cvec_t& key, const lpid_t& root) 
{    
    // get the contents of the header
    char* hdr_ptr = (char*) page_p::tuple_addr(0);
    uint4_t num_pairs = *((uint4_t*)hdr_ptr);

    // update header
    num_pairs++;

    // add the partition
    cvec_t v;
    // put subroot
    v.put((char*)(&root), sizeof(lpid_t));
    // put key
    v.put(key);
    // add this key-subroot pair to page's data
    W_DO(page_p::reclaim(num_pairs, v, true));

    cvec_t hdr;
    hdr.put((char*)(&num_pairs), sizeof(uint4_t));

    W_DO(page_p::overwrite(0, 0, hdr));

    return RCOK;
}

rc_t ranges_p::delete_partition(const lpid_t& root_to_delete,
				const lpid_t& root_to_update_old,
				const lpid_t& root_to_update_new)
{
    uint4_t nslots = page_p::nslots();
    //
    uint4_t i=1;
    uint4_t slot_to_delete = 0;
    bool root_updated = (root_to_update_old == root_to_update_new);
    bool root_deleted = false;
    for(; !(root_updated && root_deleted) && i < nslots; i++) {
	// get the contents of the slot
	char* pair = (char*) page_p::tuple_addr(i);
	cvec_t pair_vec;
	pair_vec.put(pair, page_p::tuple_size(i));
	// get the root
	lpid_t current_root_id;
	pair_vec.copy_to(&current_root_id, sizeof(lpid_t));
	//
	if(!root_deleted && current_root_id == root_to_delete) {
	    slot_to_delete = i; // found the slot for the key to be deleted
	    root_deleted = true;
	}
	if(!root_updated && current_root_id == root_to_update_old) {
	    cvec_t root_vec;
	    cvec_t key;
	    pair_vec.split(sizeof(lpid_t), root_vec, key);
	    cvec_t new_pair_vec;
	    new_pair_vec.put((char*)(&root_to_update_new), sizeof(lpid_t));
	    new_pair_vec.put(key);
	    W_DO(page_p::overwrite(i, 0, new_pair_vec));
	    root_updated = true;
	}
    }
    // here the key should be found because if it's not the deletePartition
    // call to the key_ranges_map should give the error (it's called before
    // this function to give the lpid_t of the root page 

    if(slot_to_delete == 0) {
	// TODO: return error
	cout << "ERROR" << endl;
    }

    // free the slot
    return page_p::mark_free(slot_to_delete);
}

rc_t ranges_p::add_default_partition(cvec_t& key, const lpid_t& root) 
{    
   uint4_t num_pairs = 1;

   // add the partition
   cvec_t v;
   // put subroot
   v.put((char*)(&root), sizeof(lpid_t));
   // put key
   v.put(key);
   // add this key-subroot pair to page's data
   W_DO(page_p::reclaim(num_pairs, v, true));

   cvec_t hdr;
   hdr.put((char*)(&num_pairs), sizeof(uint4_t));

   W_DO(page_p::overwrite(0, 0, hdr));
   
   return RCOK;
}

rc_t ranges_p::format(const lpid_t& pid, tag_t tag, uint4_t flags, 
		      store_flag_t store_flags)
{
    // pin: template taken from file_p

    w_assert3(tag == t_ranges_p);

    /* first, don't log it */
    W_DO( page_p::_format(pid, tag, flags, store_flags) );

    // always set the store_flag here -- see comments 
    // in bf::fix(), which sets the store flags to st_regular
    // for all pages, and lets the type-specific store manager
    // override (because only file pages can be insert_file)
    //
    // persistent_part().set_page_storeflags(store_flags);
    this->set_store_flags(store_flags); // through the page_p, through the bfcb_t

    // initialize header
    cvec_t hdr;
    int num_pairs = 0;
    hdr.put((char*)(&num_pairs), sizeof(uint4_t));
    W_COERCE(page_p::reclaim(0, hdr, true));

    return RCOK;
}

