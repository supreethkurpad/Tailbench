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

 $Id: dir.cpp,v 1.111 2010/06/08 22:28:55 nhall Exp $

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
//
#define DIR_C

#ifdef __GNUG__
#pragma implementation "dir.h"
#pragma implementation "sdesc.h"
#endif


#include <sm_int_3.h>
#include "histo.h"
#include <btcursor.h>

// for btree_p::slot_zero_size()
#include "btree_p.h"  
#include "btcursor.h"  

#include "ranges_p.h"

#ifdef EXPLICIT_TEMPLATE
// template class w_auto_delete_array_t<snum_t>;
template class w_auto_delete_array_t<sinfo_s>;
template class w_auto_delete_array_t<smlevel_3::sm_store_property_t>;
#endif


/*
 *  Directory is keyed on snum_t. 
 */
static const unsigned int dir_key_type_size = sizeof(snum_t);
static const key_type_s dir_key_type(key_type_s::u, 0, dir_key_type_size);

rc_t
dir_vol_m::_mount(const char* const devname, vid_t vid)
{
    if (_cnt >= max)   return RC(eNVOL);

    if (vid == vid_t::null) return RC(eBADVOL);

    int i;
    for (i = 0; i < max; i++)  {
        if (_root[i].vol() == vid)  return RC(eALREADYMOUNTED);
    }
    for (i = 0; i < max && (_root[i] != lpid_t::null); i++) ;
    w_assert1(i < max);

    W_DO(io->mount(devname, vid));

    stid_t stid;
    stid.vol = vid;
    stid.store = store_id_directory;

    lpid_t pid;
    rc_t rc = io->first_page(stid, pid);
    if (rc.is_error())  {
        W_COERCE(io->dismount(vid, false));
        return RC(rc.err_num());
    }
    _root[i] = pid;
    ++_cnt;
    return RCOK;
}

rc_t
dir_vol_m::_dismount(vid_t vid, bool flush, bool dismount_if_locked)
{
    // We can't dismount volumes used by prepared xcts
    // until they are resolved, which is why we check for locks here.
    int i;
    for (i = 0; i < max && _root[i].vol() != vid; i++) ;
    if (i >= max)  {
        DBG(<<"_dismount: BADVOL " << vid);
        return RC(eBADVOL);
    }
    
    lock_mode_t                m = NL;
    if (!dismount_if_locked)  {
        lockid_t                lockid(vid);
        W_DO( lm->query(lockid, m) );
    }
    // else m == NL and the volume is dismounted regardless of real lock value

    if (m != EX)  {
        if (flush)  {
            W_DO( _destroy_temps(_root[i].vol()));
        }
        if (m != IX && m != SIX)  {
            w_assert3(m != EX);
            W_DO( io->dismount(vid, flush) );
        }
        if (m != IX && m != SIX)  {
            w_assert3(m != EX);
            _root[i] = lpid_t::null;
            --_cnt;
        }
    }
    return RCOK;
}

rc_t
dir_vol_m::_dismount_all(bool flush, bool dismount_if_locked)
{
    FUNC(dir_vol_m::_dismount_all);
    for (int i = 0; i < max; i++)  {
        if (_root[i].valid())  {
            W_DO( _dismount(_root[i].vol(), flush, dismount_if_locked) );
        }
    }
    return RCOK;
}

rc_t
dir_vol_m::_insert(const stid_t& stid, const sinfo_s& si)
{
    if (!si.store)   {
        DBG(<<"_insert: BADSTID " << si.store);
        return RC(eBADSTID);
    }

    int i=0;
    W_DO(_find_root(stid.vol, i));

    vec_t el;
    el.put(&si, sizeof(si));

    vec_t key;
    key.put(&si.store, sizeof(si.store));
    w_assert3(sizeof(si.store) == dir_key_type_size);
    W_DO( bt->insert(_root[i], 1, &dir_key_type,
                     true, t_cc_none, key, el) );

    return RCOK;
}

rc_t
dir_vol_m::_destroy_temps(vid_t vid)
{
    FUNC(dir_vol_m::_destroy_temps);
    rc_t rc;
    int i = 0;
    W_DO(_find_root(vid, i));

    w_assert1(i>=0);
    w_assert1(xct() == 0);

    // called from dismount, which cannot be run in an xct

    // Start a transaction for destroying the files.
    // Well, first find out what ones need to be destroyed.
    // We do this in two separate phases because we can't
    // destroy the entries in the root index while we're
    // scanning the root index. Sigh.

    xct_auto_abort_t xct_auto; // start a tx, abort if not completed

    smksize_t   qkb, qukb;
    uint4_t          ext_used;
    W_DO(io->get_volume_quota(vid, qkb, qukb, ext_used));

    snum_t*  curr_key = new snum_t[ext_used];
    w_auto_delete_array_t<snum_t> auto_del_key(curr_key);

    sinfo_s* curr_value = new sinfo_s[ext_used];
    w_auto_delete_array_t<sinfo_s> auto_del_value(curr_value);

    int num_prepared = 0;
    W_DO( xct_t::query_prepared(num_prepared) );
    lock_mode_t m = NL;

    int j=0;

    {
        bt_cursor_t        cursor(true);
        W_DO( bt->fetch_init(cursor, _root[i], 
                             1, &dir_key_type,
                             true /*unique*/, 
                             t_cc_none,
                             vec_t::neg_inf, vec_t::neg_inf,
                             smlevel_0::ge, 
                             smlevel_0::le, vec_t::pos_inf
                             ) );

        while ( (!(rc = bt->fetch(cursor)).is_error()) && cursor.key())  {
            w_assert1(cursor.klen() == sizeof(snum_t));
            memcpy(&curr_key[j], cursor.key(), cursor.klen());
            memcpy(&curr_value[j], cursor.elem(), cursor.elen());

            stid_t s(vid, curr_key[j]);

            w_assert1(curr_value[j].store == s.store);
            w_assert3(curr_value[j].stype == t_index
                    || curr_value[j].stype == t_file
                    || curr_value[j].stype == t_lgrec
                    );

            if (num_prepared > 0)  {
                lockid_t lockid(s);
                W_DO( lm->query(lockid, m) );
            }
            if (m != EX && m != IX && m != SIX)  {
                DBG(<< s << " saved for destruction or update... "); 
                store_flag_t store_flags;
                W_DO( io->get_store_flags(s, store_flags) );
                if (store_flags & st_tmp)  {
                    j++;
                } else {
                    DBG( << s << " is not a temp store" );
                }
            } else {
#if W_DEBUG_LEVEL > 2
                if (m == EX || m == IX || m == SIX)  {
                    DBG( << s << " is locked" );
                }
#endif 
            }
            } // while
    } // deconstruct the cursor...

    // Ok now do the deed...
    for(--j; j>=0; j--) {
        stid_t s(vid, curr_key[j]);

        DBG(<<"destroying store " << curr_key[j]);
        W_DO( io->destroy_store(s, false) );
        histoid_t::destroyed_store(s, 0);

        W_IGNORE( bf->discard_store(s) );


#if W_DEBUG_LEVEL > 2
        /* See if there's a cache entry to remove */
        sdesc_t *sd = xct()->sdesc_cache()->lookup(s);
        snum_t  large_store_num=0;
        if(sd) {
            if(sd->large_stid()) {
                // Save the store number in the cache
                // for consistency checking below
                large_store_num = sd->large_stid().store;
            }

            /* remove the cache entry */
            DBG(<<"about to remove cache entry " << s);
            xct()->sdesc_cache()->remove(s);
        }
#endif 

        if(curr_value[j].large_store) {
            stid_t t(vid, curr_value[j].large_store);

#if W_DEBUG_LEVEL > 2
            /*
             * Cache might have been flushed -- might not have
             * found an entry.  
             */
            if (sd) {
                // had better have had a large store in the cache
                w_assert3( sd->large_stid());
                // store in cache must match
                w_assert3(large_store_num == curr_value[j].large_store);
            }
#endif 
            DBG(<<"destroying (large) store " << curr_value[j].large_store);

            W_DO( io->destroy_store(t, false) );
            W_IGNORE( bf->discard_store(t) );
#if W_DEBUG_LEVEL > 2
        } else {
            w_assert3(large_store_num == 0);
#endif
        }

        DBG(<<"about to remove directory entry " << s);
        {
            vec_t key, el;
            key.put(&curr_key[j], sizeof(curr_key[j]));
            el.put(&curr_value[j], sizeof(curr_value[j]));
            DBG(<<"about to remove bt entry " << s);
            W_DO( bt->remove(_root[i], 1, &dir_key_type,
                 true, t_cc_none, key, el) );
        }
    }

    W_DO(xct_auto.commit());

    if (rc.is_error()) {
        return RC_AUGMENT(rc);
    }
    return rc;
}

smlevel_3::sm_store_property_t
dir_vol_m::_make_store_property(store_flag_t flag)
{
    sm_store_property_t result = t_bad_storeproperty;

    switch (flag)  {
        case st_regular:
            result = t_regular;
            break;
        case st_tmp:
            result = t_temporary;
            break;
        case st_insert_file:
            result = t_insert_file;
            break;
        default:
            W_FATAL(eINTERNAL);
            break;
    }

    return result;
}

rc_t
dir_vol_m::_access(const stid_t& stid, sinfo_s& si)
{
    int i = 0;
    W_DO(_find_root(stid.vol, i));

    bool found;
    vec_t key;
    key.put(&stid.store, sizeof(stid.store));

    smsize_t len = sizeof(si);
    W_DO( bt->lookup(_root[i], 1, &dir_key_type,
                     true, t_cc_none,
                     key, &si, len, found, true) );
    if (!found)        {
        DBG(<<"_access: BADSTID " << stid.store);
        return RC(eBADSTID);
    }
    w_assert1(len == sizeof(sinfo_s));
    return RCOK;
}

rc_t
dir_vol_m::_remove(const stid_t& stid)
{
    int i = 0;
    W_DO(_find_root(stid.vol,i));

    vec_t key, el;
    key.put(&stid.store, sizeof(stid.store));
    sinfo_s si;
    W_DO( _access(stid, si) );
    el.put(&si, sizeof(si));

    W_DO( bt->remove(_root[i], 1, &dir_key_type,
                     true, t_cc_none, key, el) );

    return RCOK;
}

rc_t
dir_vol_m::_create_dir(vid_t vid)
{

    stid_t stid;
    W_DO( io->create_store(vid, 100/*unused*/, st_regular, stid) );
    w_assert1(stid.store == store_id_directory);

    lpid_t root;
    W_DO( bt->create(stid, root, false) );

    // add the directory index to the directory index
    sinfo_s sinfo(stid.store, t_index, 100, t_uni_btree, t_cc_none,
                  root.page, 0, NULL);
    vec_t key, el;
    key.put(&sinfo.store, sizeof(sinfo.store));
    el.put(&sinfo, sizeof(sinfo));
    W_DO( bt->insert(root, 1, &dir_key_type, true, 
        t_cc_none, key, el) );

    return RCOK;
}

rc_t dir_vol_m::_find_root(vid_t vid, int &i)
{
    if (vid <= 0) {
        DBG(<<"_find_root: BADVOL " << vid);
        return RC(eBADVOL);
    }
    for (i = 0; i < max && _root[i].vol() != vid; i++) ;
    if (i >= max) {
        DBG(<<"_find_root: BADVOL " << vid);
        return RC(eBADVOL);
    }
    // i is left with value to be returned
    return RCOK;
}

rc_t
dir_m::insert(const stid_t& stid, const sinfo_s& sinfo)
{
    W_DO(_dir.insert(stid, sinfo)); 

    // as an optimization, add the sd to the dir_m hash table
    if (xct()) {
        w_assert3(xct()->sdesc_cache());
        sdesc_t *sd = xct()->sdesc_cache()->add(stid, sinfo);
        sd->set_last_pid(sd->root().page);
    }

    return RCOK;
}

rc_t
dir_m::remove(const stid_t& stid)
{
    DBG(<<"remove store " << stid);
    if (xct()) {
        w_assert3(xct()->sdesc_cache());
        xct()->sdesc_cache()->remove(stid);
    }

    W_DO(_dir.remove(stid)); 
    return RCOK;
}

//
// This is a method used only for large obejct sort to
// to avoid copying large objects around. It transfers
// the large store from old_stid to new_stid and destroy
// the old_stid store.
//
rc_t
dir_m::remove_n_swap(const stid_t& old_stid, const stid_t& new_stid)
{
    sinfo_s new_sinfo;
    sdesc_t* desc;

    // read and copy the new store info
    W_DO( access(new_stid, desc, EX) );
    new_sinfo = desc->sinfo();

    // read the old store info and swap the large object store
    W_DO( access(old_stid, desc, EX) );
    new_sinfo.large_store = desc->sinfo().large_store;

    // remove entries in the cache 
    if (xct()) {
        w_assert3(xct()->sdesc_cache());
        xct()->sdesc_cache()->remove(old_stid);
        xct()->sdesc_cache()->remove(new_stid);
    }

    // remove the old entries
    W_DO(_dir.remove(old_stid)); 
    W_DO(_dir.remove(new_stid));

    // reinsert the new sinfo
    W_DO( insert(new_stid, new_sinfo) );

    return RCOK;
}

#include <histo.h>

/*
 * dir_m::access(stid, sd, mode, lklarge)
 *
 *  cache the store descriptor for the given store id (or small
 *     btree)
 *  lock the store in the given mode
 *  If lklarge==true and the store has an associated large-object
 *     store,  lock it also in the given mode.  NB : THIS HAS
 *     IMPLICATIONS FOR LOGGING LOCKS for prepared txs -- there
 *     could exist an IX lock for a store w/o any higher-granularity
 *     EX locks under it, for example.  Thus, the assumption in 
 *     recovering from prepare/crash, that it is sufficient to 
 *     reaquire only the EX locks, could be violated.  See comments
 *     in xct.cpp, in xct_t::log_prepared()
 */

rc_t
dir_m::access(const stid_t& stid, sdesc_t*& sd, lock_mode_t mode, 
        bool lklarge)
{
    xct_t* xd = xct();
#if W_DEBUG_LEVEL > 2
    if(xd) {
        w_assert3(xd->sdesc_cache());
    }
#endif 

    sd = xd ? xd->sdesc_cache()->lookup(stid): 0;
    DBGTHRD(<<"xct sdesc cache lookup for " << stid
        << " returns " << sd);

 again:
    if (! sd) {
        // lock the store
        if (mode != NL) {
            W_DO(lm->lock(stid, mode, t_long,
                                   WAIT_SPECIFIED_BY_XCT));
        }

        sinfo_s  sinfo;
        W_DO(_dir.access(stid, sinfo));
        w_assert3(xd->sdesc_cache());
        sd = xd->sdesc_cache()->add(stid, sinfo);

        // this assert forces an assert check in root() that
        // we want to run
        w_assert3(sd->root().store() != 0);

        // NB: see comments above function header
        if (lklarge && mode != NL && sd->large_stid()) {
            W_DO(lm->lock(sd->large_stid(), mode, t_long,
                                   WAIT_SPECIFIED_BY_XCT));
        }
	INC_TSTAT(dir_cache_miss);
    } else if (sd->is_inherited()) {
	// we can only use inherited sdesc if the corresponding stid
	// lock can be successfully inherited
	if(lm->sli_query(stid)) {
	    sd->set_inherited(false);
	    INC_TSTAT(dir_cache_inherit);
	}
	else {
	    xd->sdesc_cache()->remove(stid);
	    sd = 0;
	    INC_TSTAT(dir_cache_stale);
	}
	goto again;
    } else     {
        // this assert forces an assert check in root() that
        // we want to run
        w_assert3(sd->root().store() != 0);

        //
        // the info on this store is cached, therefore we assume
        // it is IS locked.  Note, that if the sdesc held a lock
        // mode we could avoid other locks as well.  However,
        // This only holds true for long locks that cannot be
        // released.  If they can be released, then this must be
        // rethought.
        //
        if (mode != IS && mode != NL) {
            W_DO(lm->lock(stid, mode,
                           t_long, /*see above comment before changing*/
                           WAIT_SPECIFIED_BY_XCT));
        }

        // NB: see comments above function header
        if (lklarge && mode != IS && mode != NL && sd->large_stid()) {
            W_DO(lm->lock(sd->large_stid(), mode, t_long,
                                   WAIT_SPECIFIED_BY_XCT));
        }
        
	INC_TSTAT(dir_cache_hit);
    }

    /*
     * Add store page utilization info
     */
    if(!sd->store_utilization()) {
        DBGTHRD(<<"no store util for sd=" << sd);
        if(sd->sinfo().stype == t_file) {
            histoid_t *h = histoid_t::acquire(stid);
            sd->add_store_utilization(h);
        }
    }

    w_assert3(stid == sd->stid());
    return RCOK;
}

// only single-thread access now...
inline void
sdesc_cache_t::_serialize() const
{
    //    if(xct()) xct()->acquire_1thread_xct_mutex();
}

inline void
sdesc_cache_t::_endserial() const
{
    //    if(xct()) xct()-> release_1thread_xct_mutex();
}

inline void
sdesc_cache_t::_AllocateBucket(uint4_t bucket)
{
    w_assert3(bucket < _bucketArraySize);
    w_assert3(bucket == _numValidBuckets);

    _sdescsBuckets[bucket] = new sdesc_t[_elems_in_bucket(bucket)];
    _numValidBuckets++;

    DBG(<<"_AllocateBucket");
    for (uint4_t i = 0; i < _elems_in_bucket(bucket); i++)  {
        _sdescsBuckets[bucket][i].invalidate();
    }
}

inline void
sdesc_cache_t::_AllocateBucketArray(int newSize)
{
    sdesc_t** newSdescsBuckets = new sdesc_t*[newSize];
    for (uint4_t i = 0; i < _bucketArraySize; i++)  {
        DBG(<<"AllocatBucketArray : copying sdesc ptrs");
        newSdescsBuckets[i] = _sdescsBuckets[i];
    }
    for (int j = _bucketArraySize; j < newSize; j++)  {
        newSdescsBuckets[j] = 0;
    }
    delete [] _sdescsBuckets;
    _sdescsBuckets = newSdescsBuckets;
    _bucketArraySize = newSize;
}

inline void
sdesc_cache_t::_DoubleBucketArray()
{
    _AllocateBucketArray(_bucketArraySize * 2);
}

sdesc_cache_t::sdesc_cache_t()
:
    _sdescsBuckets(0),
    _bucketArraySize(0),
    _numValidBuckets(0),
    _minFreeBucket(0),
    _minFreeBucketIndex(0),
    _lastAccessBucket(0),
    _lastAccessBucketIndex(0)
{
    _serialize();
    _AllocateBucketArray(min_num_buckets);
    _AllocateBucket(0);
    _endserial();
}

sdesc_cache_t::~sdesc_cache_t()
{
    _serialize();
    for (uint4_t i = 0; i < _num_buckets(); i++)  {
        delete [] _sdescsBuckets[i];
    }

    delete [] _sdescsBuckets;
    _endserial();
}

sdesc_t* 
sdesc_cache_t::lookup(const stid_t& stid)
{
    _serialize();
    //NB: MUST release the 1thread mutex !!!

    if (_sdescsBuckets[_lastAccessBucket][_lastAccessBucketIndex].stid() 
                == stid) {
        _endserial();
        return &_sdescsBuckets[_lastAccessBucket][_lastAccessBucketIndex];
    }

    for (uint4_t i = 0; i < _num_buckets(); i++) {
        for (uint4_t j = 0; j < _elems_in_bucket(i); j++)  {
            if (_sdescsBuckets[i][j].stid() == stid) {
                _lastAccessBucket = i;
                _lastAccessBucketIndex = j;
                _endserial();
                return &_sdescsBuckets[i][j];
            }
        }
    }
    _endserial();
    return NULL;
}

void 
sdesc_cache_t::remove(const stid_t& stid)
{
    _serialize();
    DBG(<<"sdesc_cache_t remove store " << stid);
    for (uint4_t i = 0; i < _num_buckets(); i++) {
        for (uint4_t j = 0; j < _elems_in_bucket(i); j++)  {
            if (_sdescsBuckets[i][j].stid() == stid) {
		DBG(<<"");
                _sdescsBuckets[i][j].invalidate();
		if (i < _minFreeBucket) {
		    _minFreeBucket = i;
		    _minFreeBucketIndex = j;
		}
		else if(i == _minFreeBucket && j < _minFreeBucketIndex) {
		    _minFreeBucketIndex = j;
		}
                _endserial();
                return;
            }
        }
    }
    _endserial();
}

void 
sdesc_cache_t::remove_all()
{
    _serialize();
    for (uint4_t i = 0; i < _num_buckets(); i++) {
        for (uint4_t j = 0; j < _elems_in_bucket(i); j++)  {
            DBG(<<"");
            _sdescsBuckets[i][j].invalidate();
        }
    }
    _minFreeBucket = 0;
    _minFreeBucketIndex = 0;
    _endserial();
}


void 
sdesc_cache_t::inherit_all()
{
    _serialize();
    for (uint4_t i = 0; i < _num_buckets(); i++) {
        for (uint4_t j = 0; j < _elems_in_bucket(i); j++)  {
            DBG(<<"");
            _sdescsBuckets[i][j].set_inherited(true);
        }
    }
    _endserial();
}

#include <sstream>
#include <iostream>

struct pretty_printer {
    ostringstream _out;
    string _tmp;
    operator ostream&() { return _out; }
    operator char const*() { _tmp = _out.str(); _out.str(""); return _tmp.c_str(); }
};
ostream &operator<<(ostream &os, sdesc_t const &sd) {
    return os << sd.stid();
}
ostream &operator<<(ostream &os, sdesc_cache_t const  &sdc) {
    for (uint4_t i = 0; i < sdc._num_buckets(); i++) {
        for (uint4_t j = 0; j < sdc._elems_in_bucket(i); j++)  {
            os << sdc._sdescsBuckets[i][j] << " ";
        }
	os << std::endl;
    }
    return os;
}
char const* db_pretty_print(sdesc_t const* sd, int i=0, char const* s=0) {
    static pretty_printer pp;
    (void) i;
    (void) s;
    pp << *sd;
    return pp;
}
char const* db_pretty_print(sdesc_cache_t const* sdc, int i=0, char const* s=0) {
    static pretty_printer pp;
    (void) i;
    (void) s;
    pp << *sdc;
    return pp;
}

    


sdesc_t* sdesc_cache_t::add(const stid_t& stid, const sinfo_s& sinfo)
{
    _serialize();
    sdesc_t *result=0;

    w_assert3(stid != stid_t::null);

    uint4_t bucket = _minFreeBucket;
    uint4_t bucketIndex = _minFreeBucketIndex;
    while (bucket < _num_buckets())  {
        while (bucketIndex < _elems_in_bucket(bucket))  {
            if (_sdescsBuckets[bucket][bucketIndex].stid() == stid_t::null)  {
                goto have_free_spot;
            }
            bucketIndex++;
        }
	bucketIndex = 0;
        bucket++;
    }

    // none found, add another bucket
    if (bucket == _bucketArraySize)  {
        _DoubleBucketArray();
    }
    _AllocateBucket(bucket);
    bucketIndex = 0;

have_free_spot:

    _sdescsBuckets[bucket][bucketIndex].init(stid, sinfo);
    _minFreeBucket = bucket;
    _minFreeBucketIndex = bucketIndex + 1;

    result =  &_sdescsBuckets[bucket][bucketIndex];

    _endserial();
    return result;
}

void                
sdesc_t::invalidate() 
{
    DBGTHRD(<<"sdesc_t::invalidate store " << _stid);
    _stid = stid_t::null;
    if(_histoid) {
         DBG(<<" releasing histoid & clobbering store util");
         if( _histoid->release() ) { delete _histoid; }
         add_store_utilization(0);
    }
}

void                
sdesc_t::set_last_pid(shpid_t p) 
{
    if(xct()) xct()->acquire_1thread_xct_mutex();
    _last_pid = p;
    if(xct()) xct()-> release_1thread_xct_mutex();
}

sdesc_t& 
sdesc_t::operator=(const sdesc_t& other) 
{
    if (this == &other)
            return *this;

    _stid = other._stid;
    _sinfo = other._sinfo;
    // this last_pid stuff only works in the
    // context of append_file_i. Otherwise, it's
    // not guaranteed to be the last pid.
    _last_pid = other._last_pid;

    if( _histoid && _histoid->release())
        delete _histoid;
    _histoid = 0;

    if (other._histoid) {
        DBGTHRD(<<"copying sdesc_t");
        add_store_utilization(other._histoid->copy());
    }

    _partitions = other._partitions;
    _partitions_filled = other._partitions_filled;
    _pages_with_space = other._pages_with_space;

    return *this;
} 

key_ranges_map& sdesc_t::partitions()
{
    if(!_partitions_filled) {
	fill_partitions_map();
	_partitions_filled = true;
    }
    return _partitions;
}

key_ranges_map* sdesc_t::get_partitions_p()
{
    if(!_partitions_filled) {
	fill_partitions_map();
	_partitions_filled = true;
    }
    return (&_partitions);
}

rc_t sdesc_t::fill_partitions_map() 
{
    W_DO( ranges_m::fill_ranges_map(root(), _partitions) );
    return RCOK;
}

rc_t sdesc_t::store_partitions()
{
    W_DO( ranges_m::fill_page(root(), _partitions) );
    return RCOK;
}
