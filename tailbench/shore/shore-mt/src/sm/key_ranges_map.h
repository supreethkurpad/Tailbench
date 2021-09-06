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

/** @file:   key_ranges_map.h
 *
 *  @brief:  Definition of a map of key ranges to partitions used by
 *           baseline MRBTrees.
 *
 *  @notes:  The keys are Shore-mt cvec_t. Thread-safe.  
 *
 *  @date:   July 2010
 *
 *  @author: Pinar Tozun (pinar)
 *  @author: Ippokratis Pandis (ipandis)
 *  @author: Ryan Johnson (ryanjohn)
 */


#ifndef _KEY_RANGES_MAP_H
#define _KEY_RANGES_MAP_H

#include "w_defines.h"

#include <iostream>
#include <cstring> 
#include <map>
#include <vector>
#include <utility>
#include <set>
#include <math.h>
#include <umemcmp.h>

#include <sm_int_2.h>

#ifndef SM_S_H
#include <sm_s.h> // for lpid_t
#endif

#ifdef __GNUG__  
#pragma interface
#endif 

using namespace std;

struct sinfo_s;


/******************************************************************** 
 *
 * @brief: Error codes returned by MRBTrees
 *
 ********************************************************************/

enum {
    mrb_PARTITION_NOT_FOUND           = 0x830001,
    mrb_LAST_PARTITION                = 0x830002,
    mrb_KEY_BOUNDARIES_NOT_ORDERED    = 0x830003,
    mrb_OUT_OF_BOUNDS                 = 0x830004,
    mrb_PARTITION_EXISTS              = 0x830005,
    mrb_NOT_PHYSICAL_MRBT             = 0x830006
};


class foo 
{
public:
    char* _m;
    uint4_t _len;
    bool _alloc;

    foo();
    foo(const foo& v);
    foo(char* m, uint4_t len, bool alloc);
    ~foo();

    friend inline bool operator<(const foo& v1, const foo& v2);
    friend inline bool operator>(const foo& v1, const foo& v2);
    friend inline bool operator==(const foo& v1, const foo& v2);
    friend inline bool operator!=(const foo& v1, const foo& v2);

    foo& operator=(const foo& v);
};


inline bool operator==(const foo& v1, const foo& v2)
{    
    return ((&v1==&v2) || ((v1._len == v2._len) && umemcmp(v1._m,v2._m,v1._len) == 0)); 
}

inline bool operator!=(const foo& v1, const foo& v2)
{
    return ! (v1 == v2);
}

inline bool operator<(const foo& v1, const foo& v2) 
{
    assert (v1._len == v2._len);
    return (umemcmp(v1._m,v2._m,v1._len)<0);
}

inline bool operator>(const foo& v1, const foo& v2) 
{
    assert (v1._len == v2._len);
    return (umemcmp(v1._m,v2._m,v1._len)>0);
}



struct cmp_greater
{
    bool operator()(const foo& a, const foo& b) const;
};

inline bool cmp_greater::operator()(const foo& a, const foo& b) const
{
    return (a>b);
}







/******************************************************************** 
 *
 * @class: key_ranges_map
 *
 * @brief: A map of key ranges to partitions. This structure is used
 *         by the multi-rooted B-tree (MRBTree). 
 *
 * @note:  The specific implementation indentifies each partition through
 *         the lpid_t of the root of the corresponding sub-tree. Hence,
 *         this implementation is for the Baseline MRBTree (non-DORA).
 *
 ********************************************************************/

class key_ranges_map
{
public:

    typedef map<foo, lpid_t, cmp_greater >                 KRMap;
    typedef map<foo, lpid_t, cmp_greater >::iterator       KRMapIt;
    typedef map<foo, lpid_t, cmp_greater >::const_iterator KRMapCIt;

private:

    typedef cvec_t Key;
        
protected:

    // range_init_key -> root of the corresponding subtree
    KRMap _keyRangesMap;
    uint  _numPartitions;

    vector<foo*> _fookeys;

    // for the hack to reduce number of mallocs (if something is put
    // to the map without any space allocation, then in destructor it
    // shouldn't be destroyed, this set is to keep track of this)
    //set<char*> _keyCounts;

    // for thread safety multiple readers/single writer lock
    occ_rwlock _rwlock;

    // Splits the partition where "key" belongs to two partitions. The start of 
    // the second partition is the "key".
//     virtual w_rc_t _addPartition(char* keyS, lpid_t& newRoot);

    // Delete the partition where "key" belongs, by merging it with the 
    // previous partition
     virtual w_rc_t _deletePartitionByKey(const foo& kv, // Input
 					 lpid_t& root1, lpid_t& root2, // Outputs
 					 Key& startKey1, Key& startKey2); // Outputs

public:

    //// Construction ////
    // Calls one of the initialization functions
    key_ranges_map();
    key_ranges_map(const sinfo_s& sinfo, 
                   const cvec_t& minKey, const cvec_t& maxKey, 
                   const uint numParts, 
                   const bool physical);
    key_ranges_map(const key_ranges_map& rhs);
    virtual ~key_ranges_map();


    ////  Initialization ////
    // The default initialization creates numParts partitions of equal size

    // Create a partitioning but does not do any physical changes, uses
    w_rc_t nophy_equal_partitions(const sinfo_s& sinfo, 
                                  const cvec_t& minKey, const cvec_t& maxKey, 
                                  const uint numParts);
    

    ////  Map management ////

    // Splits the partition where "key" belongs to two partitions. The start of 
    // the second partition is the "key".
    w_rc_t addPartition(const Key& key, lpid_t& newRoot);

    // Deletes the partition where "key" belongs by merging it with the previous 
    // partition
    w_rc_t deletePartitionByKey(const Key& key, // Input
				lpid_t& root1, lpid_t& root2, // Outputs
				Key& startKey1, Key& startKey2); // Outputs

    // Deletes the given partition (identified by the pid), by merging it with 
    // the previous partition
    w_rc_t deletePartition(lpid_t& root1, lpid_t& root2, // Outputs, well root2 is also input in this case
			   Key& startKey1, Key& startKey2); // Outputs

    // Gets the partition id of the given key.
    //
    // @note: In the baseline version of the MRBTree each partition is identified
    //        by the lpid_t of the root of the corresponding sub-tree. In the 
    //        DORA version each partition can also be identified by a partition-id 
    w_rc_t getPartitionByUnscrambledKey(const sinfo_s& sinfo, const Key& key, lpid_t& pid);
    w_rc_t getPartitionByKey(const Key& key, lpid_t& pid);

    
    // Returns the list of partitions that cover: 
    // [key1, key2], (key1, key2], [key1, key2), or (key1, key2) ranges
    w_rc_t getPartitions(const Key& key1, bool key1Included,
                         const Key& key2, bool key2Included,                         
                         vector<lpid_t>& pidVec);
    // Returns the list of all root ids in partitions
    w_rc_t getAllPartitions(vector<lpid_t>& pidVec);


    // Returns the range boundaries of a partition in start&end key
    w_rc_t getBoundaries(lpid_t pid, cvec_t& startKey, cvec_t& endKey);


    // Updates the root of the partition starting with key
    w_rc_t updateRoot(const Key& key, const lpid_t& root);
        
    // Returns true if they are same
    bool is_same(const key_ranges_map& krm);

    // Setters
    void setNumPartitions(uint numPartitions);
    void setMinKey(const Key& minKey);
    void setMaxKey(const Key& maxKey);

    // Getters
    uint getNumPartitions() const;
    char* getMinKey() const;
    char* getMaxKey() const;
    KRMap getMap() const;

    // for debugging
    void printPartitions();
    void printPartitionsInBytes(); 

    //
    key_ranges_map& operator=(const key_ranges_map& krm);

    // Helper functions to make equal partitions with platform independence
    static bool isBigEndian();
    static uint distributeSpace(const char* min, const uint minSize,
				const char* max, const uint maxSize,
				const uint numParts, char** subParts);    
    
}; // EOF: key_ranges_map

#endif
