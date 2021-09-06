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

/*<std-header orig-src='shore' incl-file-exclusion='VOL_H'>

 $Id: vol.h,v 1.98 2010/06/08 22:28:57 nhall Exp $

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

#ifndef VOL_H
#define VOL_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#include <list>
#include <extent.h>


struct volume_hdr_stats_t;
class store_histo_t; // page_h.h
class pginfo_t; // page_h.h

class volhdr_t {
    // For compatibility checking, we record a version number
    // number of the Shore SM version which formatted the volume.
    // This number is called volume_format_version in sm_base.h.
    w_base_t::uint4_t   _format_version;
    sm_diskaddr_t       _device_quota_KB;
    lvid_t              _lvid;
    extnum_t            _ext_size;
    shpid_t             _epid;        // extent pid
    shpid_t             _spid;        // store pid
    extnum_t            _num_exts;
    extnum_t            _hdr_exts;
    shpid_t             _hdr_pages;   // # pages in hdr (we might not use
                                      // entire _hdr_exts), includes entire
                                      // store 0
    w_base_t::uint4_t   _page_sz;    // page size in bytes
public:
    w_base_t::uint4_t   format_version() const { 
                            return _format_version; }
    void            set_format_version(uint v) { 
                        _format_version = v; }

    sm_diskaddr_t   device_quota_KB() const { 
                            return _device_quota_KB; }
    void            set_device_quota_KB(sm_diskaddr_t q) { 
                            _device_quota_KB = q; }

    const lvid_t&   lvid() const { return _lvid; }
    void            set_lvid(const lvid_t &l) { 
                             _lvid = l; }

    extnum_t         ext_size() const { return _ext_size; } 
    void             set_ext_size(extnum_t e) { _ext_size=e; } 

    const shpid_t&   epid() const { return _epid; }
    void             set_epid(const shpid_t& p) { 
                        _epid = p; }

    const shpid_t&   spid() const { return _spid; }
    void             set_spid(const shpid_t& p) { 
                        _spid = p; }

    extnum_t         num_exts() const {  return _num_exts; }
    void             set_num_exts(extnum_t n) {  _num_exts = n; }

    extnum_t         hdr_exts() const {  return _hdr_exts; }
    void             set_hdr_exts(extnum_t n) {  _hdr_exts = n; }

    extnum_t         hdr_pages() const {  return _hdr_pages; }
    void             set_hdr_pages(shpid_t n) {  _hdr_pages = n; }

    w_base_t::uint4_t page_sz() const {  return _page_sz; }
    void            set_page_sz(w_base_t::uint4_t n) {  _page_sz = n; }

};

/*********************************************************************
 * per-volume ext_cache 
 *********************************************************************/
#include <vector>
#include <set>
#include <map>

class vol_t : public smlevel_1 
{
public:
    /*WARNING: THIS CODE MUST MATCH THAT IN sm_io.h!!! */
    typedef mcs_rwlock VolumeLock;
    typedef void *     lock_state;
    
    NORET               vol_t(const bool apply_fake_io_latency = false, 
                                      const int fake_disk_latency = 0);
    NORET               ~vol_t();
    
    static int          max_extents_on_page();

    rc_t                mount(const char* devname, vid_t vid);
    rc_t                dismount(bool flush = true);
    rc_t                check_disk();
    rc_t                check_store_pages(snum_t snum, page_p::tag_t tag);
    rc_t                check_store_page(const lpid_t &pid, page_p::tag_t tag);

    const char*         devname() const;
    vid_t               vid() const ;
    lvid_t              lvid() const ;
    extnum_t            ext_size() const;
    extnum_t            num_exts() const;
    extnum_t            pid2ext(const lpid_t& pid) const;

public:
    int                 fill_factor(snum_t fnum);
 
    bool                is_valid_page_num(const lpid_t& p) const;
    bool                is_valid_store(snum_t f) const;
    bool                is_alloc_ext_of(extnum_t e, snum_t s)const;
    bool                is_alloc_page(const lpid_t& p) const;
    bool                is_alloc_page_of(const lpid_t& p, snum_t s) const {
                                return _is_alloc_page_of(p, s);
                        }

    // used by io_m: must hold volume mutex
    bool                is_valid_page_of(const lpid_t& p, snum_t s) const {
                            return _is_valid_page_of(p,s);
                        }
private:
    bool                _is_alloc_page_of(const lpid_t& p, snum_t s, 
                                bool use_cache=true) const ;
    bool                _is_valid_page_of(const lpid_t& p, snum_t s) const;

public:
    bool                is_alloc_store(snum_t f) const;
    
    rc_t                write_page(shpid_t page, page_s& buf);

    rc_t                write_many_pages(
        shpid_t             first_page,
        const page_s*       buf, 
        int                 cnt);

    rc_t                read_page(
        shpid_t             page,
        page_s&             buf);

    rc_t            alloc_pages_in_ext(
		alloc_page_filter_t *filter,
        bool                append_only,
        extnum_t            ext, 
        int                 eff,
        snum_t              fnum,
        int                 cnt,
        lpid_t              pids[],
        int&                allocated,
        int&                remaining,
        bool&               is_last,
        bool                may_realloc  = false,
        lock_mode_t         desired_lock_mode = IX );

    rc_t            recover_pages_in_ext(
        snum_t              snum,
        extnum_t            ext,
        const Pmap&         pmap,
        bool                is_alloc);
    
    rc_t            store_operation(const store_operation_param&    param);
    rc_t            free_stores_during_recovery(store_deleting_t typeToRecover);
    rc_t            free_exts_during_recovery();
    rc_t            free_page(const lpid_t& pid, bool check_store_membership);
    rc_t            find_free_exts(
        uint                 cnt,
        extnum_t             exts[],
        int&                 found,
        extnum_t            first_ext = 0);
    rc_t            num_free_exts(w_base_t::uint4_t& cnt);
    rc_t            num_used_exts(w_base_t::uint4_t& cnt);

    rc_t            alloc_exts(
        snum_t               num,
        extnum_t             prev,
        int                  cnt,
        const extnum_t       exts[]);


    rc_t            update_ext_histo(const lpid_t& pid, space_bucket_t b);
    rc_t            next_ext(extnum_t ext, extnum_t &res);
    rc_t            dump_exts(ostream &, extnum_t start, extnum_t end);
    rc_t            dump_stores(ostream &, int start, int end);

    rc_t            find_free_store(snum_t& fnum);
    rc_t            alloc_store(
        snum_t                 fnum,
        int                    eff,
        store_flag_t           flags);
    rc_t            set_store_first_ext(
        snum_t                 fnum,
        extnum_t               head);

    rc_t            set_store_flags(
        snum_t                 fnum,
        store_flag_t           flags,
        bool                   sync_volume);
    rc_t            get_store_flags(
        snum_t                 fnum,
        store_flag_t&          flags);
    rc_t            free_store(
        snum_t                fnum,
        bool                  acquire_lock);
    rc_t            free_store_after_xct(snum_t snum);
    rc_t            free_ext_after_xct(extnum_t ext, snum_t&);

    // set_ext_next: recovery-only
    rc_t            set_ext_next(
        extnum_t            ext,
        extnum_t            new_next);

    rc_t             first_ext(snum_t fnum, extnum_t &result);
private:
    bool            _is_valid_ext(extnum_t e) const;

    rc_t            _free_ext_list(
        extnum_t            head,
        snum_t              snum);
    rc_t            _append_ext_list(
        snum_t              snum,
        extnum_t            prev,
        extnum_t            count,
        const extnum_t*     list);
public:
    rc_t            free_exts_on_same_page(
        extnum_t            ext,
        snum_t              snum,
        extnum_t            count);
    rc_t            create_ext_list_on_same_page(
        snum_t              snum,
        extnum_t            prev,
        extnum_t            next,
        extnum_t            count,
        const extnum_t*     list);

public:
    rc_t             init_histo(
        store_histo_t*      h,  
        snum_t              snum,
        pginfo_t*           pages, 
        int&                numpages);

    snum_t            max_store_id_in_use() const;

    // The following functinos return space utilization statistics
    // on the volume or selected stores.  These functions use only
    // the store and page/extent meta information.

    rc_t                     get_volume_meta_stats(
        SmVolumeMetaStats&          volume_stats);

    rc_t                     get_file_meta_stats(
        w_base_t::uint4_t           num_files,
        SmFileMetaStats*            file_stats);

    rc_t                     get_file_meta_stats_batch(
        w_base_t::uint4_t            max_store,
        SmStoreMetaStats**          mapping);

    rc_t                     get_store_meta_stats(
        snum_t                      snum,
        SmStoreMetaStats&           storeStats);
    
    // The following functions return the first/last/next pages in a
    // store.  If "allocated" is NULL then only allocated pages will be
    // returned.  If "allocated" is non-null then all pages will be
    // returned and the bool pointed to by "allocated" will be set to
    // indicate whether the page is allocated.
    rc_t            first_page(
    snum_t                    fnum,
    lpid_t&                   pid,
    bool*                     allocated = NULL);

    rc_t            last_allocated_page(
        snum_t                fnum,
        lpid_t&               pid
        );
    rc_t            last_reserved_page(
        snum_t                fnum,
        lpid_t&               pid,
        bool                  &allocated);

    rc_t            last_extent(
        snum_t               fnum,
        extnum_t&            ext,
        bool*                is_empty=NULL
        );

private:
    // helper function
    rc_t            _last_extent(
        snum_t               fnum,
        extnum_t&            ext,
        extlink_i            &ei, 
        const extlink_t *    &linkp
        );
public:

    rc_t             next_page(
        lpid_t&             pid,
        bool*               allocated = NULL);


    rc_t             next_page_with_space(lpid_t& pid, 
        space_bucket_t       needed);

    rc_t             num_pages(snum_t fnum, w_base_t::uint4_t& cnt);
    rc_t             num_exts(snum_t fnum, w_base_t::uint4_t& cnt);
    bool             is_raw() { return _is_raw; };

    rc_t            sync();

    // format a device (actually, just zero out the header for now)
    static rc_t            format_dev(
        const char*          devname,
        shpid_t              num_pages,
        bool                 force);

    static rc_t            format_vol(
        const char*          devname,
        lvid_t               lvid,
        shpid_t              num_pages,
        bool                 skip_raw_init);

    static rc_t            read_vhdr(const char* devname, volhdr_t& vhdr);
    static rc_t            read_vhdr(int fd, volhdr_t& vhdr);

    static rc_t            write_vhdr(           // SMUF-SC3: moved to public
        int                  fd, 
        volhdr_t&            vhdr, 
        bool                 raw_device);

    static rc_t            check_raw_device(
        const char*          devname,
        bool&                raw);

    

    // methods for space usage statistics for this volume
    rc_t             get_du_statistics(
        struct              volume_hdr_stats_t&,
        bool                audit);

    void            assert_mutex_mine(lock_state *) {}
    void            assert_mutex_notmine(lock_state *) {}

    // Sometimes the sm_io layer acquires this mutex:
    void            acquire_mutex(lock_state* me, bool for_write); // used by sm_io.cpp
    VolumeLock&     vol_mutex() const { return _mutex; } // used by sm_io.cpp

    // fake disk latency
    void            enable_fake_disk_latency(void);
    void            disable_fake_disk_latency(void);
    bool            set_fake_disk_latency(const int adelay);    
    void            fake_disk_latency(long start);    

private:
    char             _devname[max_devname];
    int              _unix_fd;
    vid_t            _vid;
    lvid_t           _lvid;
    extnum_t         _num_exts;
    uint             _hdr_exts;
    uint             _hdr_pages;
    extnum_t         _min_free_ext_num;
    lpid_t           _epid;
    lpid_t           _spid;
    int              _page_sz;  // page size in bytes
    bool             _is_raw;   // notes if volume is a raw device

    mutable VolumeLock _mutex;   

    // fake disk latency
    bool             _apply_fake_disk_latency;
    int              _fake_disk_latency;     


    static shpid_t   ext2pid(extnum_t e);
 public:
    static extnum_t  pid2ext(shpid_t p);
 private:

    // _ext_cache: entries extnum_t  -> snum_t indicate
    // that the given extent is allocated to the given store. 
    // The purpose of the cache is to bypass latching the extent map
    // pages for inspection to find out if an extent or page is allocated
    // to a given store.
    // This is use by histograms.
    typedef std::pair<extnum_t, snum_t> ext2store_entry;
    typedef std::list<ext2store_entry > histo_extent_cache;
    enum { EXT_CACHE_SIZE=16 };
    mutable histo_extent_cache     _histo_ext_cache;
    histo_extent_cache::iterator   histo_ext_cache_find(extnum_t ext) const;
    void                           histo_ext_cache_update(
                                      extnum_t ext, snum_t s) const;
    void                           histo_ext_cache_erase(snum_t s) const;

    // _last_page_cache: entries snum_t  -> extnum_t indicates
    // the given store's last extent in physical order.
    // The purpose of the cache is to bypass searches of the store
    // list to find the last page or last extent of the store.
    // NOTE: it is NOT the same as the last-referenced-extent cache, nor
    // is it the last-allocated-page, since that could be in the middle
    // of the file.
    typedef std::map<snum_t, extnum_t> page_cache;
    mutable page_cache       _last_page_cache;
    extnum_t                 page_cache_find(
                                snum_t snum,
                                extlink_i &ei, 
                                const extlink_t * &linkp
                             ) const;
    void                     page_cache_update(snum_t snum, extnum_t e) const ;
 private:
    bool                     _page_cache_find(snum_t s) const {
                                return ( _last_page_cache[s] != 0);
                            }

 public:
    /*********************************************************************
     * per-volume ext_cache 
     *********************************************************************/
    /*
     * Class ext_cache: a cache that stores pairs {storenum, extent num}
     * If a pair is found in the cache, it means that we found the extent
     * to be allocated to the given store and to have unallocated pages
     * in it.
     *
     * This cache also keeps a count of the number of entries for a given
     * storenum, so we know whether it's worth looking for an extent in here.
     *
     * This is thread-safe because the volume mutex serializes access to it.
     * This corresponds to the change described in 6.2.2 (page 8) of the
     * Shore-MT paper
     */
    class ext_cache_t 
    {
        struct ext_info {
            snum_t snum; // store
            extnum_t ext; // extent number
            ext_info(snum_t s, extnum_t e) : snum(s), ext(e) { }

            bool operator <(ext_info const &other) const {
                compare_snum_t comp;
                if(comp(snum, other.snum)) return true;
                if(comp(other.snum, snum)) return false;
                return ext < other.ext;
            }
        };
        typedef std::map<snum_t, int, compare_snum_t> count_map;
        typedef std::set<ext_info>   cache;
    public:
        typedef cache::iterator      cache_iterator;

    private:
        cache          _cache;
        count_map      _counts; // count per store


    public:
        cache::iterator lower_bound(snum_t snum) { return _cache.lower_bound(
                                                (ext_info(snum, 0))) ;}
        cache::iterator end() { return _cache.end(); }
        int count(snum_t snum) { return _counts[snum]; }
        void erase(cache::iterator pos); 
        void insert(snum_t snum, extnum_t ext) ;
        void erase(snum_t snum, extnum_t ext);
        void erase_all(snum_t snum) ;
        void shutdown() {  _cache.clear(); _counts.clear(); }
        void get_sizes(int &cachesz, int &cachemx, int &cntsz, int &cntmx) const
                {  cachesz = _cache.size(); cachemx = _cache.max_size(); 
                   cntsz = _counts.size(); cntmx = _counts.max_size(); }

    }; 
 private:
    ext_cache_t              _free_ext_cache;
 public:
    void                     shutdown() {   _free_ext_cache.shutdown(); 
                                            _last_page_cache.clear();
                                            _histo_ext_cache.clear(); 
                                        }
    void                     shutdown(snum_t s) {   
                                            _free_ext_cache.erase_all(s); 
                                            _last_page_cache.erase(s);
                                            histo_ext_cache_erase(s); 
                                        }

    // Is the volume's reserved-page cache primed?
    // 
    // If we don't prime it, it will only contain those items
    // we've found by releasing extents in this run.  So when we
    // start up, we need to prime it sometime.  Priming is 
    // expensive so we don't want to do it on mount, and we
    // don't want to re-do it if it's already been done.
    // We can prime and test-for-primed on a per-store basis.
    // The cache gets cleared on dismount. 
    // It gets primed whenever we try to allocate a page.
    bool                     is_primed(snum_t s) const { 
                                     return _page_cache_find(s); }
    // prime cache for given store.
    w_rc_t                   prime_cache(snum_t s);

    // cache of reserved pages in extents by store number:
    ext_cache_t&             free_ext_cache() { return _free_ext_cache; }


    static const char*       prolog[]; // string array for volume hdr

};


/*
 * STORES:
 * Each volume contains a few stores that are "overhead":
 * 0 -- is reserved for the extent map and the store map
 * 1 -- directory (see dir.cpp)
 * 2 -- root index (see sm.cpp)
 *
 * After that, for each file created, 2 stores are used, one for
 * small objects, one for large objects.
 *
 * Each index(btree, rtree) uses one store. 
 */


inline vol_t::vol_t(const bool apply_fake_io_latency, const int fake_disk_latency) 
             : _unix_fd(-1), _min_free_ext_num(1),
               _apply_fake_disk_latency(apply_fake_io_latency), 
               _fake_disk_latency(fake_disk_latency) // IP: default fake io values
{}

inline vol_t::~vol_t() { 
    shutdown();
    w_assert1(_unix_fd == -1); 
}

inline const char* vol_t::devname() const
{
    return _devname;
}

/*
 * NB: pageids are: vol.store.page
 * but that does not mean that the .page part is relative to the
 * .store part.  In fact, the .page is relative to the volume
 * and the .store part ONLY indicates what store owns that page.
 */

inline extnum_t vol_t::pid2ext(shpid_t p)
{
    //snum = 0; or store_id_extentmap
    return (extnum_t) (p / ext_sz);
}

inline shpid_t vol_t::ext2pid(extnum_t ext) 
{
	    // Make sure we convert from the extnum_t to the shpid_t before
    // multiplying (for larger page sizes than we now support...):
    shpid_t tmp = ext;
    return tmp * ext_sz; 
}

inline extnum_t vol_t::pid2ext(const lpid_t& pid) const
{
    w_assert3(pid.vol() == _vid);
    return pid2ext(pid.page);
}

inline vid_t vol_t::vid() const
{
    return _vid;
}

inline lvid_t vol_t::lvid() const
{
    return _lvid;
}

inline extnum_t vol_t::ext_size() const
{
    return ext_sz;
}

inline extnum_t vol_t::num_exts() const
{
    return (extnum_t) _num_exts;
}

inline bool vol_t::_is_valid_ext(extnum_t e) const
{
    return (e < _num_exts);
}

inline bool vol_t::is_valid_page_num(const lpid_t& p) const
{
    return (((unsigned int)( _num_exts) * ext_sz) > p.page);
}

inline bool vol_t::is_valid_store(snum_t f) const
{
    // Can't have more stores than extents, so
    // this is a sufficient check for a reasonable
    // store number, although it doesn't tell if the
    // store is allocated.

    return (f < _num_exts );
}
	
/*<std-footer incl-file-exclusion='VOL_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
