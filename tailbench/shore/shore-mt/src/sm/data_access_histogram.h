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

/** @file:   data_access_histogram.h
 *
 *  @brief:  Definition of the histogram that is used to keep statistics
 *           about data accesses.
 *
 *  @date:   February 2011
 *
 *  @author: Pinar Tozun (pinar)
 *  @author: Ippokratis Pandis (ipandis)
 */


#ifndef _DATA_ACCESS_HISTOGRAM_H
#define _DATA_ACCESS_HISTOGRAM_H

#include "w_defines.h"

#include "key_ranges_map.h"

#ifdef __GNUG__  
#pragma interface
#endif 

using namespace std;




/******************************************************************** 
 *
 * @brief: Error codes returned by data access histogram
 *
 ********************************************************************/

enum {
    hist_ROOT_DOESNT_EXIST             = 0x830007,
    hist_RANGE_DOESNT_EXIST            = 0x830008
};




/******************************************************************** 
 *
 * @class: data_access_histogram
 *
 * @brief: A map of key ranges to how keep frequently they are accessed.
 *         This structure is used by load balancing system with btrees
 *         (regular or mrbtrees doesn't matter).
 *
 ********************************************************************/

class data_access_histogram
{
public:

    // NOTE: pin: lpid_t has comparison functions but they only work correctly if the compared lpid_t are of the
    //            same store, it's easy to change this so this note is also a TODO

    typedef map< lpid_t, map< foo, vector<uint>, cmp_greater > >                 ranges_hist;
    typedef map< lpid_t, map< foo, vector<uint>, cmp_greater > >::iterator       ranges_hist_iter;
    typedef map< lpid_t, map< foo, vector<uint>, cmp_greater > >::const_iterator ranges_hist_citer;
    typedef map< foo, vector<uint>, cmp_greater >::iterator                      sub_ranges_hist_iter;
    typedef map< foo, vector<uint>, cmp_greater >::const_iterator                sub_ranges_hist_citer;
    typedef vector<uint>::iterator                                               sub_ranges_hist_buckets_iter;
    typedef vector<uint>::const_iterator                                         sub_ranges_hist_buckets_citer;  
    
    //    typedef map< lpid_t, map< foo, occ_rwlock, cmp_greater > >                 ranges_locks;
    //    typedef map< lpid_t, map< foo, occ_rwlock, cmp_greater > >::iterator       ranges_locks_iter;
    //    typedef map< lpid_t, map< foo, occ_rwlock, cmp_greater > >::const_iterator ranges_locks_citer;
    //    typedef map< foo, occ_rwlock, cmp_greater >::iterator                      sub_ranges_locks_iter;
    //    typedef map< foo, occ_rwlock, cmp_greater >::const_iterator                sub_ranges_locks_citer;

    typedef map< lpid_t, occ_rwlock >                 root_locks;
    typedef map< lpid_t, occ_rwlock >::iterator       root_locks_iter;
    typedef map< lpid_t, occ_rwlock >::const_iterator root_locks_citer;
    
    typedef map< lpid_t, int >                 gran_map;
    typedef map< lpid_t, int >::iterator       gran_map_iter;
    typedef map< lpid_t, int >::const_iterator gran_map_citer;

    typedef map< lpid_t, vector<foo*> >                 key_values;
    typedef map< lpid_t, vector<foo*> >::iterator       key_values_iter;
    typedef map< lpid_t, vector<foo*> >::const_iterator key_values_citer;
    typedef vector<foo*>::iterator                      sub_key_values_iter;
    typedef vector<foo*>::const_iterator                sub_key_values_citer;
    
private:

    typedef cvec_t Key;

    // to keep the key values in the ranges
    key_values _foo_keys;
    
    // keeps for each subtree a ranges map that indicates which key range is accessed how many times
    ranges_hist _range_accesses;
    
    // percentage that indicates with what granuarity we should collect the data access info for each subtree
    // granularity = 100 means we just want to keep how many times a subtree is accessed not specific ranges
    // granularity = 0 means we want to keep track of the accesses to every single record in that subtree
    gran_map _granularities;

    // incdicates whether this is the central histogram or a histrogram that is kept in sdesc_cache
    // because if it's local (if it's a copy that is kept in the sdesc cache),
    // there is no need to lock the structures since only one thread reaches an sdesc cache at a time
    bool _is_local;


    //// locks ////

    // for thread safety of the whole structure - LEVEL 1
    occ_rwlock _histogram_lock;

    // for thread safety of subtrees - LEVEL 2
    root_locks _root_locks;

    // keeps for each range in _range_accesses a rwlock so that
    // we can maintain this structure with fine granularity - LEVEL 3
    // ranges_locks _range_locks;


    //// locks map management  ////

    // acquire/release the locks that corresponds to key's range in read/write mode
    //w_rc_t _acquire_lock(const lpid_t& root, const foo& kv,
    //			 sub_ranges_lock_iter& sub_lock_iter, bool is_write);
    //w_rc_t _release_lock(sub_ranges_lock_iter& sub_lock_iter, bool is_write);


    //// aging ////
    // @note: pin: later these can be made per sub range but right now I see no use of it so
    //             let's make our lives easier for now

    // index for the current age bucket
    uint _index;

    // specifies how many age-slots the histogram has
    uint _ages;
    
public:

    //// Construction ////
    // Calls one of the initialization functions
    data_access_histogram();
    data_access_histogram(key_ranges_map& krm, const int common_granularity,
			  const uint ages, const bool is_local);
    //data_access_histogram(const data_access_histogram& rhs);
    void initialize(key_ranges_map& krm, const int common_granularity,
		    const uint ages, const bool is_local);

    //// Destruction ////
    ~data_access_histogram();


    //// accesses map management  ////

    // increments the access count of the range that the key belongs to by 1
    w_rc_t inc_access_count(const lpid_t& root, const Key& key);

    // updates the access count of the range that the key belings to by the given amount
    w_rc_t update_access_count(const lpid_t& root, const Key& key, uint amount);

    // add/delete buckets as new granularities or subtrees are added/deleted
    w_rc_t add_bucket(const lpid_t& root, int granularity);
    w_rc_t add_sub_bucket(const lpid_t& root, const Key& key, int amount);
    w_rc_t delete_bucket(const lpid_t& root);
    w_rc_t delete_sub_bucket(const lpid_t& root, const Key& key);
    

    //// granularity map management  ////

    // Updates the statistics collection granularity of the subtree given with root
    // And rearranges the accesses map of that subtree based on the new granularity
    w_rc_t update_granularity(const lpid_t& root, int new_granularity);


    //// operator =
    data_access_histogram& operator=(const data_access_histogram& rhs);
    

    //// aging ////
    void inc_age();

    // TODOs:
    // add bucket / delete bucket functions
    // functions that allows setting krm later
    // finish comments
    // implement bit-by-bit (or byte-by-byte) range partitioner
    
}; // EOF: data_access_histogram




//// locks map management  ////

/****************************************************************** 
 *
 * @fn:      [acquire/release]_[read/write]_lock()
 *
 * @brief:   acquire/release the locks that corresponds to key's range in read/write mode
 *
 * @param:   lpid_t root  -
 * @param:   foo key      - 
 *
 ******************************************************************/
/*
inline w_rc_t data_access_histogram::_acquire_lock(const lpid_t& root, foo& kv,
						   sub_ranges_lock_iter& sub_lock_iter, bool is_write)
{
    ranges_locks_iter lock_iter = _range_locks.find(root);
    if(lock_iter == _range_locks.end()) {
	return (RC(hist_ROOT_DOESNT_EXIST));
    } else {
	sub_lock_iter = (lock_iter->second).find(kv);
	if(sub_lock_iter == (lock_iter->second).end()) {
	    return (RC(hist_RANGE_DOESNT_EXIST));
	} else {
	    if(is_write) {
		(sub_lock_iter->second).acquire_write();
	    } else {
		(sub_lock_iter->second).acquire_read();
	    }
	}
    }
    return RCOK;
}
*/
 /*
inline w_rc_t data_access_histogram::_release_lock(sub_ranges_lock_iter& sub_lock_iter, bool is_write)
{
    if(is_write) {
	(sub_lock_iter->second).release_write();
    } else {
	(sub_lock_iter->second).release_read();
    }
    return RCOK;
}
 */
#endif
