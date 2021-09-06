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

/** @file:   ranges_p.h
 *
 *  @brief:  Definition of the page type which is used to store the key ranges partitions
 *           used by baseline MRBTrees.
 *
 *  @date:   August 2010
 *
 *  @author: Pinar Tozun (pinar)
 *  @author: Ippokratis Pandis (ipandis)
 *  @author: Ryan Johnson (ryanjohn)
 */

#ifndef RANGES_P_H
#define RANGES_P_H

#include "w_defines.h"

#ifdef __GNUG__
#pragma interface
#endif

#include "key_ranges_map.h"

class ranges_p : public page_p {

public:

    MAKEPAGE(ranges_p, page_p, 1); 

    // forms the key_ranges_map from the partitions' info kept in this page
    rc_t fill_ranges_map(key_ranges_map& partitions);
    
    // stores the partitions' info from key_ranges_map in this page
    rc_t fill_page(key_ranges_map& partitions);

    // stores the partitions' info from key_ranges_map in this page
    // this is for initial partitions where partitions have dummy subroot ids
    // the real subroot ids are in subroots
    rc_t fill_page(vector<cvec_t*>& keys, vector<lpid_t>& subroots);    

    // stores the newly added partition info
    rc_t add_partition(cvec_t& key, const lpid_t& root);

    // deletes the newly deleted partition
    rc_t delete_partition(const lpid_t& root_to_delete,
			  const lpid_t& root_to_update_old,
			  const lpid_t& root_to_update_new);

    // for the first partition, to initialize the header properly to 1
    rc_t add_default_partition(cvec_t& key, const lpid_t& root);

    // pin: the header of this page keeps the info about how many slots this
    //      page used up to know. some of these slots might be empty
    //      because when a delete call is made it just marks the slot as free
    //      and the new slots are always reclaimed.
    //      so the header of this page shouldn't be taken as number of partitions
};

// calls to ranges_p are made through ranges_m
class ranges_m : public smlevel_2 {

public:
  
    NORET                        ranges_m()   {};
    NORET                        ~ranges_m()  {};

    static rc_t create(const stid_t stid, lpid_t& pid, const lpid_t& subroot);
    static rc_t create(const stid_t stid, lpid_t& pid, vector<cvec_t*>& keys, vector<lpid_t>& subroots);
    static rc_t add_partition(const lpid_t& pid, cvec_t& key, const lpid_t& root);
    static rc_t delete_partition(const lpid_t& pid, const lpid_t& root_to_delete,
				const lpid_t& root_to_update_old, const lpid_t& root_to_update_new);
    static rc_t fill_ranges_map(const lpid_t& pid, key_ranges_map& partitions);
    static rc_t fill_page(const lpid_t& pid, key_ranges_map& partitions);

};

#endif
