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

/** @file:   key_ranges_map.cpp
 *
 *  @brief:  Implementation of a map of key ranges to partitions used by
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

#ifdef __GNUG__
#           pragma implementation "key_ranges_map.h"
#endif

#include "key_ranges_map.h"

#include "btree.h"
#include "sdesc.h"


foo::foo() 
    : _m(NULL),_len(0),_alloc(false)
{
}

foo::foo(const foo& v)
    : _m(v._m),_len(v._len),_alloc(false)
{
}

foo::foo(char* m, uint4_t len, bool alloc)
{
    assert(m);
    _len = len;
    if (alloc) {
        // deep copy
        _alloc = true;
        _m = (char*) malloc(_len);
        memcpy(_m, m, _len);
    }
    else {
        // shallow copy
        _alloc = false;
        _m = m;
        _len = len;
    }
}

foo::~foo()
{
    if (_alloc && _m) {
        free(_m);
        _m = NULL;
   }
}

foo& foo::operator=(const foo& v)
{
    _m = v._m;
    _len = v._len;
    _alloc = false;
    return (*this);
}


/****************************************************************** 
 *
 * Construction/Destruction
 *
 * @brief: The constuctor calls the default initialization function
 * @brief: The destructor needs to free all the malloc'ed keys
 *
 ******************************************************************/

key_ranges_map::key_ranges_map()
    : _numPartitions(0)
{
    _fookeys.clear();
}

key_ranges_map::key_ranges_map(const sinfo_s& sinfo,
                               const cvec_t& minKey, 
                               const cvec_t& maxKey, 
                               const uint numParts, 
                               const bool physical)
    : _numPartitions(0)
{
    _fookeys.clear();
    w_rc_t r = RCOK;
    if (physical) { 
        r = RC(mrb_NOT_PHYSICAL_MRBT); 
    }
    else {
        r = nophy_equal_partitions(sinfo,minKey,maxKey,numParts);
    }
    if (r.is_error()) { W_FATAL(r.err_num()); }
}

key_ranges_map::~key_ranges_map()
{
    // Delete the allocated keys in the map
    vector<foo*>::iterator iter;

    DBG(<<"Destroying the ranges map: ");

    _rwlock.acquire_write();
    for (iter = _fookeys.begin(); iter != _fookeys.end(); ++iter) {
	if (*iter) { 
            delete (*iter);
            *iter = NULL;
        }
    }
    _rwlock.release_write();    
}


/****************************************************************** 
 *
 * @fn:     nophy_equal_partitions()
 *
 * @brief:  Makes equal length partitions from scratch without any
 *          physical changes
 *
 * @return: The number of partitions that were actually created
 *
 ******************************************************************/

w_rc_t key_ranges_map::nophy_equal_partitions(const sinfo_s& sinfo,
                                              const cvec_t& minKey, 
                                              const cvec_t& maxKey,
                                              const uint numParts)
{
    assert(numParts);

    // 1. scramble min&max keys
    cvec_t* scrambled_key;
    W_DO(btree_m::_scramble_key(scrambled_key, minKey, minKey.count(), sinfo.kc));
    uint minKey_size = scrambled_key->size();
    char* minKey_c = (char*) malloc(minKey_size);
    scrambled_key->copy_to(minKey_c, minKey_size);
    scrambled_key->reset();
    W_DO(btree_m::_scramble_key(scrambled_key, maxKey, maxKey.count(), sinfo.kc));
    uint maxKey_size = scrambled_key->size();
    char* maxKey_c = (char*) malloc(maxKey_size);
    scrambled_key->copy_to(maxKey_c, maxKey_size);

    // 2. determine the partition start keys
    char** subParts = (char**) malloc(numParts*sizeof(char*));
    uint partsCreated = key_ranges_map::distributeSpace(minKey_c, minKey_size,
							maxKey_c, maxKey_size,
							numParts, subParts);

    // 3. add partitions
    _rwlock.acquire_write();
    _keyRangesMap.clear();
    _rwlock.release_write();    
    uint size = (minKey_size < maxKey_size) ? minKey_size : maxKey_size;
    stid_t astid;
    lpid_t root(astid,0);
    for(uint i=0; i<partsCreated; i++) {
	scrambled_key->reset();
	scrambled_key->put(subParts[i], size);
	root.page = i;
	W_DO(addPartition(*scrambled_key,root));
    }

    // 4. delete malloced subParts
    for(uint i=0; i<partsCreated; i++) 
    {
	//delete subParts[i];
        free (subParts[i]);
        subParts[i] = NULL;
    }

    free(subParts);
    free(minKey_c);
    free(maxKey_c);
        
    return (RCOK);
}


/****************************************************************** 
 *
 * @fn:    addPartition()
 *
 * @brief: Splits the partition where "key" belongs to two partitions. 
 *         The start of the second partition is the "key".
 *
 * @param:   cvec_t key     - The starting key of the new partition (Input)
 * @param:   lpid_t newRoot - The root of the sub-tree which maps to the new partition (Input)
 *
 *
 ******************************************************************/

w_rc_t key_ranges_map::addPartition(const cvec_t& key, lpid_t& newRoot)
{
    w_rc_t r = RCOK;
    
    foo kv((char*)key._base[0].ptr,key._base[0].len,false);

    _rwlock.acquire_write();        
    KRMapIt iter = _keyRangesMap.find(kv);

    if (iter==_keyRangesMap.end() ) {
        foo* newkv = new foo((char*)key._base[0].ptr,key._base[0].len,true);
        _keyRangesMap[*newkv] = newRoot;
        _numPartitions++;
        _fookeys.push_back(newkv);
    }
    else {
        r = RC(mrb_PARTITION_EXISTS);
    }
    _rwlock.release_write();
    //printf ("%d\n",_keyRangesMap.size());

    return (r);
}


/****************************************************************** 
 *
 * @fn:    deletePartition{,ByKey}()
 *
 * @brief: Deletes a partition, by merging it with the partition which
 *         is before that, based either on a partition identified or
 *         a key.
 *
 * @param: cvec_t key    - The key which is in the partition to be deleted
 *                         (Input for deletePartitionByKey) 
 * @param: lpid_t root1  - The root of the merged partition which has the lower key values (Output)
 * @param: lpid_t root2  - The root of the merged partition which has the lower key values (Output)
 *                         (Also input for deletePartition)
 * @param: cvec_t startKey1 - The start key of the partition that maps to root1 (Output)
 * @param: cvec_t startKey2 - The start key of the partition that maps to root2 (Output)
 *
 * @note:  Here the startKey1 < startKey2 but in the map they startKey2
 *         comes before startKey1.
 ******************************************************************/

w_rc_t key_ranges_map::_deletePartitionByKey(const foo& kv,
					     lpid_t& root1, lpid_t& root2,
					     Key& startKey1, Key& startKey2)
{
    w_rc_t r = RCOK;

    _rwlock.acquire_write();
    
    KRMapIt iter = _keyRangesMap.lower_bound(kv);
    
    if(iter == _keyRangesMap.end()) {
 	// partition not found, return an error
	_rwlock.release_write();
 	return (RC(mrb_PARTITION_NOT_FOUND));
    }

    root2 = iter->second;
    ++iter;
    if(iter == _keyRangesMap.end()) {
 	--iter;
 	if(iter == _keyRangesMap.begin()) {
 	    // partition is the last partition, cannot be deleted
	    _rwlock.release_write();
 	    return (RC(mrb_LAST_PARTITION));
 	}
 	root1 = root2;
	startKey1.put((*iter).first._m,(*iter).first._len);
    }
    else {
 	startKey1.put((*iter).first._m,(*iter).first._len);
 	root1 = iter->second;
    }
    --iter;
    startKey2.put((*iter).first._m,(*iter).first._len);
    root2 = iter->second;
    _keyRangesMap.erase(iter);
    _numPartitions--;
    
    _rwlock.release_write();
    return (r);
}

w_rc_t key_ranges_map::deletePartitionByKey(const Key& key,
 					    lpid_t& root1, lpid_t& root2,
 					    Key& startKey1, Key& startKey2)
{
    w_rc_t r = RCOK;
    foo kv((char*)key._base[0].ptr,key._base[0].len,false);
    r = _deletePartitionByKey(kv, root1, root2, startKey1, startKey2);
    return (r);
}

w_rc_t key_ranges_map::deletePartition(lpid_t& root1, lpid_t& root2,
				       Key& startKey1, Key& startKey2)
{
    w_rc_t r = RCOK;
    bool bFound = false;

    KRMapIt iter;
    _rwlock.acquire_read();
    for (iter = _keyRangesMap.begin(); iter != _keyRangesMap.end(); ++iter) {
	if (iter->second == root2) {
	    bFound = true;
 	    break;
	}
    }
    _rwlock.release_read();

    if (bFound) {
	r = _deletePartitionByKey(iter->first, root1, root2, startKey1, startKey2);
    } 
    else {
 	return (RC(mrb_PARTITION_NOT_FOUND));
    }

    return (r);
}



/****************************************************************** 
 *
 * @fn:    getPartitionByUnscrambledKey()
 *
 * @brief: Returns the root page id, "pid", of the partition which a
 *         particular "key" belongs to. Before doing so, it scrambles
 *         the "key" according to the sinfo
 *
 * @param: cvec_t key    - Input (unscrambled key)
 * @param: lpid_t pid    - Output
 *
 ******************************************************************/

w_rc_t key_ranges_map::getPartitionByUnscrambledKey(const sinfo_s& sinfo,
                                                    const Key& key,
                                                    lpid_t& pid)
{
    cvec_t* scrambledKey = NULL;
    W_DO(btree_m::_scramble_key(scrambledKey,key,key.count(),sinfo.kc));    
    return (getPartitionByKey(*scrambledKey,pid));
}


/****************************************************************** 
 *
 * @fn:    getPartitionByKey()
 *
 * @brief: Returns the root page id, "pid", of the partition which a
 *         particular "key" belongs to
 *
 * @param: cvec_t key    - Input
 * @param: lpid_t pid    - Output
 *
 ******************************************************************/

w_rc_t key_ranges_map::getPartitionByKey(const Key& key, lpid_t& pid)
{
    foo kv((char*)key._base[0].ptr,key._base[0].len,false);
    _rwlock.acquire_read();
    KRMapIt iter = _keyRangesMap.lower_bound(kv);
    if(iter == _keyRangesMap.end()) {
	// the key is not in the map, returns error.
        _rwlock.release_read();
	return (RC(mrb_PARTITION_NOT_FOUND));
    }
    pid = iter->second;
    _rwlock.release_read();
    return (RCOK);    
}


/****************************************************************** 
 *
 * @fn:    getPartitions()
 *
 * @param: cvec_t key1    - The start key for the partitions list (Input)
 * @param: bool key1Included - Indicates whether key1 should be included or not (Input)
 * @param: cvec_t key2    - The end key for the partitions list (Input)
 * @param: bool key2Included - Indicates whether key2 should be included or not (Input)
 * @param: vector<lpid_t> pidVec  - The list of partitions' root ids (Output)
 *
 * @brief: Returns the list of partitions that cover one of the key ranges:
 *         [key1, key2], (key1, key2], [key1, key2), or (key1, key2) 
 *
 ******************************************************************/

w_rc_t key_ranges_map::getPartitions(const Key& key1, bool key1Included,
                                     const Key& key2, bool key2Included,
                                     vector<lpid_t>& pidVec) 
{
    w_rc_t r = RCOK;
    
    if(key2 < key1) {
	// return error if the bounds are not given correctly
	return (RC(mrb_KEY_BOUNDARIES_NOT_ORDERED));
    }  

    // get start key
    foo a((char*)key1._base[0].ptr,key1._base[0].len,false);

    // get end key
    foo b((char*)key2._base[0].ptr,key2._base[0].len,false);

    _rwlock.acquire_read();

    KRMapIt iter1 = _keyRangesMap.lower_bound(a);
    KRMapIt iter2 = _keyRangesMap.lower_bound(b);

    if (iter1 == _keyRangesMap.end() || iter2 == _keyRangesMap.end()) {
	// at least one of the keys is not in the map, returns error.
        _rwlock.release_read();
	return (RC(mrb_PARTITION_NOT_FOUND));
    }

    while (iter1 != iter2) {
	pidVec.push_back(iter1->second);
	iter1--;
    }

    pidVec.push_back(iter1->second);
    
    _rwlock.release_read();
    return (r);
}

/****************************************************************** 
 *
 * @fn:    getAllPartitions()
 *
 * @brief: Returns the list of the root ids of all partitions
 *
 * @param: vector<lpid_t> pidVec    - Output 
 *
 ******************************************************************/

w_rc_t key_ranges_map::getAllPartitions(vector<lpid_t>& pidVec) 
{
    _rwlock.acquire_read();
    for(KRMapIt iter = _keyRangesMap.begin(); iter != _keyRangesMap.end(); iter++) {
	pidVec.push_back(iter->second);
    }
    _rwlock.release_read();
    return (RCOK);
}


/****************************************************************** 
 *
 * @fn:    getBoundaries()
 *
 * @param: lpid_t pid    - The root of the partition whose boundaries is returned (Input)
 * @param: cvec_t startKey - The start key of the partition's key-range (Output)
 * @param: cvec_t endKey  - The end key of the partition's key-range (Output)
 * @param: bool last      - Indicates whether the partition is the last one,
 *                          the one with the highest key values (Output)
 *
 * @brief: Returns the range boundaries of a partition in start&end key
 *
 ******************************************************************/

w_rc_t key_ranges_map::getBoundaries(lpid_t pid, cvec_t& startKey, cvec_t& endKey) 
{
    KRMapIt iter;
    bool bFound = false;
    
    _rwlock.acquire_read();
    for (iter = _keyRangesMap.begin(); iter != _keyRangesMap.end(); ++iter) {
        if (iter->second == pid) {
            bFound = true;
	    break;
        }
    }
    _rwlock.release_read();
    
    if(!bFound) {
	// the pid is not in the map, returns error.
	return (RC(mrb_PARTITION_NOT_FOUND));
    }

    startKey.set((*iter).first._m,(*iter).first._len);
    if( iter != _keyRangesMap.begin() ) {
	iter--;
        endKey.set((*iter).first._m,(*iter).first._len);
    }
    else {
	endKey.set(cvec_t::pos_inf);
    }
    return (RCOK);
}



/****************************************************************** 
 *
 * @fn:    updateRoot()
 *
 * @param: cvec_t key    - The root of the partition that keeps this key is updated (Input)
 * @param: lpid_t root   - New root value for the updated partition (Input)
 *
 * @brief: Updates the root of the partition starting with key
 *
 ******************************************************************/

w_rc_t key_ranges_map::updateRoot(const Key& key, const lpid_t& root)
{
    foo kv((char*)key._base[0].ptr,key._base[0].len,false);

    _rwlock.acquire_write();
    if(_keyRangesMap.find(kv) != _keyRangesMap.end()) {
        _keyRangesMap[kv] = root;
    } 
    else {
        _rwlock.release_write();
        return (RC(mrb_PARTITION_NOT_FOUND));
    }
    _rwlock.release_write();
    return (RCOK);
}


/****************************************************************** 
 *
 * Helper functions
 *
 ******************************************************************/

void key_ranges_map::printPartitions()
{
    KRMapIt iter;
    uint i = 0;
    _rwlock.acquire_read();
    //DBG(<<"Printing ranges map: ");
    printf("#Partitions (%d)\n", _numPartitions);
    char* content = NULL;
    for (iter = _keyRangesMap.begin(); iter != _keyRangesMap.end(); ++iter, i++) {
	//DBG(<<"Partition " << i << "\tStart key (" << iter->first << ")\tRoot (" << iter->second << ")");        
        content = (char*)malloc(sizeof(char)*(iter->first._len)+1);
        memset(content,0,iter->first._len+1);
        memcpy(content,iter->first._m,iter->first._len);
        printf("Root (%d)\tStartKey (%s)\n", iter->second.page, content);
        free (content);
    }
    _rwlock.release_read();
}

void key_ranges_map::printPartitionsInBytes()
{
    KRMapIt iter;
    uint i = 0;
    _rwlock.acquire_read();
    printf("#Partitions (%d)\n", _numPartitions);
    char* content = NULL;
    for (iter = _keyRangesMap.begin(); iter != _keyRangesMap.end(); ++iter, i++) {
	//DBG(<<"Partition " << i << "\tStart key (" << iter->first << ")\tRoot (" << iter->second << ")");        
        content = (char*)malloc(sizeof(char)*(iter->first._len)+1);
        memset(content,0,iter->first._len+1);
        memcpy(content,iter->first._m,iter->first._len);
	printf("Root (%d): ", iter->second.page);
	for(uint j=0; j<iter->first._len; j++) {
	    printf("%d\t", (uint) content[j]);
	}
	printf("\n");
	free (content);
    }
    _rwlock.release_read();
}

void key_ranges_map::setNumPartitions(uint numPartitions)
{
    // pin: we do not actually need this function
    //      how to adjust the partitions is ambiguous
    _rwlock.acquire_write();
    _numPartitions = numPartitions;
    _rwlock.release_write();
}

void key_ranges_map::setMinKey(const Key& /*minKey*/)
{  
    // pin: not sure who is going to use this function
    assert (0); // IP: -//-

//     _rwlock.acquire_write();
    
//     // insert the new minKey
//     KRMapIt iter = _keyRangesMap.lower_bound(_minKey);
//     if(iter == _keyRangesMap.end()) {
//     	iter--;
//     }
//     _keyRangesMap[_minKey] = iter->second;

//     // delete the partitions that has lower key values than the new minKey
//     _keyRangesMap.erase(iter, _keyRangesMap.end());

//     _rwlock.release_write();
}

void key_ranges_map::setMaxKey(const Key& /*maxKey*/)
{
    // pin: not sure who is going to use this function
    assert (0); // IP: -//-

//     _rwlock.acquire_write();

//     // delete the partitions that has higher key values than the new maxKey
//     KRMapIt iter = _keyRangesMap.lower_bound(_maxKey);
//     _keyRangesMap.erase(_keyRangesMap.begin(), iter);

//     _rwlock.release_write();
}

uint key_ranges_map::getNumPartitions() const
{
    return (_numPartitions);
}

key_ranges_map::KRMap key_ranges_map::getMap() const
{
    return (_keyRangesMap);
}


/****************************************************************** 
 *
 * @fn:    is_same()
 *
 * @brief: Returns true if the two maps are the same.
 *         - They have the same number of partitions
 *         - Each partition has the same starting point
 *
 ******************************************************************/

bool key_ranges_map::is_same(const key_ranges_map& krm)
{
    if (_numPartitions!=krm._numPartitions) { return (false); }

//     if (strcmp(_minKey,krm._minKey)!=0) return (false);
//     if (strcmp(_maxKey,krm._maxKey)!=0) return (false);

    assert (_keyRangesMap.size()==krm.getNumPartitions());

    KRMapIt myIt = _keyRangesMap.begin();
    KRMapCIt urCIt = krm._keyRangesMap.begin();

    for (; myIt != _keyRangesMap.end(); myIt++,urCIt++) {
        if ((*myIt).second!=(*urCIt).second) { return (false); }
        if ((*myIt).first!=(*urCIt).first) { return (false); }
    }
    return (true);
}


key_ranges_map& key_ranges_map::operator=(const key_ranges_map& krm)
{
    DBG(<<"Copying the ranges map: ");

    KRMapCIt mapcit;
    
    _rwlock.acquire_write();
    
    _numPartitions = 0;
    _fookeys.clear();
    _keyRangesMap.clear();
    for (vector<foo*>::const_iterator cit = krm._fookeys.begin() ;
	 cit != krm._fookeys.end(); ++cit) {
	foo* newkv = new foo((*cit)->_m, (*cit)->_len, true);
	mapcit = krm._keyRangesMap.lower_bound(*(*cit));
	assert (mapcit != krm._keyRangesMap.end()); // Should be there	
	_keyRangesMap[*newkv] = (*mapcit).second;
	_fookeys.push_back(newkv);
	_numPartitions++;
    }
    assert (_numPartitions == krm._numPartitions);

    _rwlock.release_write();

    return *this;
}


/****************************************************************** 
 *
 * @fn:    isBigEndian()
 *
 * @brief: Determines whether the architecture is LSB or MSB
 *
 ******************************************************************/

bool key_ranges_map::isBigEndian() 
{
    int i = 1;
    return ((*(char*)&i) == 0);
}


/****************************************************************** 
 *
 * @fn:    distributeSpace()
 *
 * @brief:   Helper function, which tries to evenly distribute the space between
 *           two strings to a certain number of subspaces
 *
 * @param:   const char* min     - The beginning of the space
 * @param:   const int minSize   - The size (in bytes) of the lower space boundary
 * @param:   const char* max     - The end of the space
 * @param:   const int maxSize   - The size (in bytes) of the upper space boundary
 * @param:   const uint numParts - The number of partitions which should be created
 * @param:   char** subParts     - An array of strings with the boundaries of the
 *                                 distribution
 * 
 * @returns: The number of partitions actually created
 *
 ******************************************************************/

uint key_ranges_map::distributeSpace(const char* min, const uint minSize,
				     const char* max, const uint maxSize,
				     const uint numParts, char** subParts) 
{
    // @note: pin: Here partitions might not be of exactly equal lenght but they
    //             will be very very close to being equal. The reason is I prefer
    //             to keep the difference as a 4-byte integer and use integer sum/
    //             division for this operation. However, if there are more than
    //             4 bytes in min/max values, then getting the difference and finding
    //             the space in between are harder. However, I see no big benefit for
    //             taking into account such a situation. I think dividing according to
    //             first 4 bytes (and considering rest of the bytes as 0s) should be
    //             enough to have a nice almost-equal partitions initially

    
    // 0. find the total size to consider (smaller of the two is enough)
    uint size = (minSize < maxSize) ? minSize : maxSize;

    uint diff_size = sizeof(int);
	
    assert(umemcmp(min,max,size)<0); // min should be less than max
	
    // 1. Pass the initial bytes that are equal
    //    However, have a char* that is at least sizeof(int) bytes
    const char* min_p = min;
    const char* max_p = max;
    uint pre_size = 0;
    while(size-pre_size > diff_size && *min_p == *max_p) {
	min_p++;
	max_p++;
	pre_size++;
    }
    
    // 2. Do the partitions
    uint partsCreated = 0;
    
    // 2.1 If the architecture is MSB we have no problem with continuing with integers
    // 2.2 If it's LSB then we should reverse the bytes first because casting them to
    //     integer will reverse the bytes
    if(isBigEndian()) { // 2.1 MSB
	
	uint min_i = *(uint*) min_p;
	uint max_i = *(uint*) max_p;
	
	double diff = (double)(max_i - min_i) / (double)numParts;
	if(diff<1) {
	    diff = 1;
	}
	
	double current_d = min_i;
	double max_d = max_i;
	while(current_d < max_d) {
	    uint current = current_d;
	    subParts[partsCreated] = (char*)malloc(size);
	    memset(subParts[partsCreated], 0, size);
	    if(pre_size > 0) {
		memcpy(subParts[partsCreated], min, pre_size);
	    }
	    memcpy(&(subParts[partsCreated][pre_size]), (char*) (&current), diff_size);
	    current_d = current_d + diff;
	    partsCreated++;
	}
	
    } else { // 2.2 LSB

	char* min_p_rev = (char*) malloc(diff_size);
	char* max_p_rev = (char*) malloc(diff_size);
	for(uint i=0; i<diff_size; i++) {
	    min_p_rev[i] = min_p[diff_size-1-i];
	    max_p_rev[i] = max_p[diff_size-1-i];
	}

       	uint min_i = *(uint*) min_p_rev;
	uint max_i = *(uint*) max_p_rev;

	double diff = (double)(max_i - min_i) / (double)numParts;
	if(diff<1) {
	    diff = 1;
	}
	
	double current_d = min_i;
	double max_d = max_i;
	while(partsCreated < numParts && current_d < max_d) {
	    uint current = current_d;
	    subParts[partsCreated] = (char*)malloc(size);
	    memset(subParts[partsCreated], 0, size);
	    if(pre_size > 0) {
		memcpy(subParts[partsCreated], min, pre_size);
	    }
	    char* current_c = (char*) (&current);
	    for(uint i=0; i<diff_size; i++) {
		subParts[partsCreated][pre_size+i] = current_c[diff_size-1-i];
	    }
	    current_d = current_d + diff;
	    partsCreated++;
	}
	
    }

    return partsCreated;
}
