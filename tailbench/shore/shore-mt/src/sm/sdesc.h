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

/*<std-header orig-src='shore' incl-file-exclusion='SDESC_H'>

 $Id: sdesc.h,v 1.51 2010/06/08 22:28:55 nhall Exp $

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

#ifndef SDESC_H
#define SDESC_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "key_ranges_map.h"

/*
 * This file describes Store Descriptors (sdesc_t).  Store
 * descriptors consist of a persistent portion (sinfo_s) 
 * and the rest, which is transient.
 *
 * Also defined is a store descriptor cache (sdesc_cache_t) that
 * is located in each transaction. 
 *
 * Member functions are defined in dir.cpp.
 */

#ifdef __GNUG__
#pragma interface
#endif


struct sinfo_s {
public:
    typedef smlevel_0::store_t store_t;

    snum_t        store;                // store id
    u_char        stype;                // store_t
    u_char        ntype;                // ndx_t
    u_char        cc;                 // concurrency_t on index

    // The following holds special properties (such as whether logging
    // should be done.  Note that for "real" (multi-page) stores,
    // this is duplicated in the store map structure at the beginning
    // of the volume.  However, for 1-page stores, this is the
    // only place to put it.  If the 1-page store grows then
    // it is needed in creating the new store.
    //

    // fill factors
    // u_char        pff;                // page fill factor in %
    //  removed to make room for cc, above

    u_char        eff;                // extent fill factor in %        
                                // unused an maybe will never be
                                // used

    u_char        isf;                // index split factor

    fill1        _f1;                // keep it 8 byte aligned and leave
    fill2        _f2;            // room for future expansion
    fill4        _f3;        

    /*
     * This is an additional store used by the file facility
     * to store large record pages.  This is only a temporary
     * implementation, so this should disappear in the future.
     *
     * WARNING: For alignment purposes (to prevent uninitialized
     *          holes for purify to complain about), the
     *          following snum_t must be located after pff,eff.
     */
    snum_t        large_store;        // store for large record pages
    
    shpid_t        root;                // root page (of main index)
    
    w_base_t::uint4_t        nkc;                // # components in key
    key_type_s        kc[smlevel_0::max_keycomp];

    sinfo_s()        {};
    sinfo_s(snum_t store_, store_t stype_, 
            u_char eff_, 
            smlevel_0::ndx_t ntype_, u_char cc_, 
            const shpid_t& root_,
            w_base_t::uint4_t nkc_, const key_type_s* kc_) 
    :   store(store_), stype(stype_), ntype(ntype_),
        cc(cc_), eff(eff_),
        isf(50),
        large_store(0),
        root(root_),
        nkc(nkc_)
    {
        w_assert1(nkc < (sizeof(kc) / sizeof(kc[0])));
        memcpy(kc, kc_, (unsigned int)(sizeof(key_type_s) * nkc)); 
        if (nkc < sizeof(kc)) {
            memset(kc+nkc, 0, sizeof(kc)-nkc);
        }
    }

    sinfo_s& operator=(const sinfo_s& other) {
        store = other.store; 
        stype = other.stype; 
        ntype = other.ntype;
        cc = other.cc;
        // pff = other.pff; 
        eff = other.eff;
        isf = other.isf;
	root = other.root;
	nkc = other.nkc;
        memcpy(kc, other.kc, sizeof(kc));
        large_store = other.large_store;
        return *this;
    }
        
    void set_large_store(const snum_t& _store) {large_store = _store;}
};

class histoid_t; // forward ref; defined in histo.h
class append_file_; // forward ref; defined in scan.h
class sdesc_cache_t; // forward

class sdesc_t {
    friend class append_file_i;
    friend class sdesc_cache_t;

private:
    key_ranges_map _partitions;
    bool _partitions_filled;
    map< lpid_t, shpid_t > _pages_with_space;


public:
    typedef smlevel_0::store_t store_t;

    NORET sdesc_t() : _partitions_filled(false), _histoid(0), _last_pid(0), _inherited(false) {};

    NORET ~sdesc_t() { invalidate(); }

    void                init(const stid_t& stid, const sinfo_s& s)
                            {   _stid = stid;
                                _sinfo = s; 
                                _histoid = 0;
				_inherited = false;
                            }

    inline
    const stid_t&       stid() const {return _stid;}

    shpid_t    hog_last_pid() const { return *&_last_pid; }
    void                free_last_pid() const {} // DEAD
    void                set_last_pid(shpid_t p);

    inline
    const lpid_t        root() const {
        lpid_t r(_stid.vol, _stid.store, _sinfo.root);
        return r;
    }

    inline
    const lpid_t        root(const cvec_t& key) {
	lpid_t r;
	partitions().getPartitionByKey(key, r);
	return r;
    }

    // store id for large object pages
    inline
    const stid_t        large_stid() const {
                            return _sinfo.large_store?
                                stid_t(_stid.vol, _sinfo.large_store)
                                : stid_t::null;
                        }
    inline
    const sinfo_s&      sinfo() const {return _sinfo;}

    void                add_store_utilization(histoid_t *h) {
                            _histoid = h;
                        }
    const histoid_t*    store_utilization() const {
                            return _histoid;
                        }
    void                invalidate_sdesc() { invalidate(); }

    void		set_inherited(bool flag) { _inherited = flag; }
    bool		is_inherited() const { return _inherited; }

    friend ostream &operator<<(ostream &os, sdesc_t const &sd);
    
    inline stid_t stid() { return _stid; }
    inline bool has_partitions() { return _partitions_filled; }

    // these two are racy but this is something we can tolerate; a benign race
    // because we'll just use them to fasten the page allocation for PLP-Part and PLP-Leaf
    // therefore, reading or writing a wrong page will be noticed during record insertion
    // and a scan will be done; this is just for doing less of those scans
    inline shpid_t get_page_with_space(const lpid_t& btree_page) {
	map< lpid_t, shpid_t >::iterator iter =  _pages_with_space.find(btree_page);
	if(iter == _pages_with_space.end()) {
	    return 0;
	} else {
	    return _pages_with_space[btree_page];
	}
    }
    inline void set_page_with_space(const lpid_t& btree_page,
				    const shpid_t page) {
	_pages_with_space[btree_page] = page;
    }

    key_ranges_map& partitions();
    key_ranges_map* get_partitions_p();
    rc_t fill_partitions_map();
    rc_t store_partitions();

protected:
    sdesc_t&            operator=(const sdesc_t& other);
    void                invalidate(); 

private:
    NORET sdesc_t(const sdesc_t&) {}; // disabled

    // _sinfo is a cache of persistent info stored in directory index
    sinfo_s                _sinfo;

    //
    // the following fields are transient
    // TODO : before calling or setting _last_pid,  caller
    // should acquire 1-thread mutex; likewise for _histoid
    //
    histoid_t*             _histoid;
    shpid_t                _last_pid; // absolute, not approx
    stid_t                 _stid;   // identifies stores 
    bool		   _inherited;
};

/**\brief Cache of store descriptors used by an smthread/transaction.
 * \details
 * Code is in dir.cpp
 * \todo sdesc_cache_t
 */
class sdesc_cache_t {
public:
    // There is an assumption that an SM interface function will
    // never work on more than max_sdesc files at one time.
    // At this time, the sort code will work on 3 at one time.
    // enum {max_sdesc = 4};
    //
    // Changed this from a constant to a doubling of the cache size
    // when more is needed.
    //
    enum {
                min_sdesc = 4,
                min_num_buckets = 8
    };

    NORET sdesc_cache_t(); 
         ~sdesc_cache_t(); 
    sdesc_t*    lookup(const stid_t& stid);
    void        remove(const stid_t& stid);
    void        remove_all(); // clear all entries from cache
    void	inherit_all(); // mark entries as inherited
    sdesc_t*    add(const stid_t& stid, const sinfo_s& sinfo);

    void  copy( const sdesc_cache_t &other);  
    friend ostream &operator<<(ostream &os, sdesc_cache_t const &sdc);
    

private:
    void        _serialize() const;
    void        _endserial() const;
    w_base_t::uint4_t        _num_buckets() const { return _numValidBuckets; }
    w_base_t::uint4_t        _num_allocated_buckets() const { 
                                return _bucketArraySize; }
    w_base_t::uint4_t        _elems_in_bucket(int i) const { return min_sdesc << i; }
    void        _AllocateBucket(w_base_t::uint4_t bucket);
    void        _AllocateBucketArray(int newSize);
    void        _DoubleBucketArray();

    sdesc_t**                _sdescsBuckets; // array of cached sdesc_t
    w_base_t::uint4_t        _bucketArraySize;// # entries in the malloced array
    w_base_t::uint4_t        _numValidBuckets;// # valid entries
    w_base_t::uint4_t        _minFreeBucket;
    w_base_t::uint4_t        _minFreeBucketIndex;

    w_base_t::uint4_t        _lastAccessBucket;// last sdesc allocated
    w_base_t::uint4_t        _lastAccessBucketIndex;        // last sdesc allocated
};

/*<std-footer incl-file-exclusion='SDESC_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
