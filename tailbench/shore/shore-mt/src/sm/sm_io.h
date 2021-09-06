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

/*<std-header orig-src='shore' incl-file-exclusion='SM_IO_H'>

 $Id: sm_io.h,v 1.23 2010/06/08 22:28:56 nhall Exp $

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

#ifndef SM_IO_H
#define SM_IO_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

class vol_t;
class sdesc_t;
class extlink_p;
class SmVolumeMetaStats;
class SmFileMetaStats;
class SmStoreMetaStats;
class store_histo_t; // page_h.h
class pginfo_t; // page_h.h
class xct_t; // forward

#ifdef __GNUG__
#pragma interface
#endif

struct volume_hdr_stats_t;

class store_operation_param  {
    friend ostream & operator<<(ostream&, const store_operation_param &);
    
public:
        typedef w_base_t::uint2_t uint2_t;

        typedef smlevel_0::store_operation_t        store_operation_t;
        typedef smlevel_0::store_flag_t                store_flag_t;
        typedef smlevel_0::store_deleting_t        store_deleting_t;

private:
        snum_t                _snum;
        uint2_t                _op;
        fill2                _filler; // for purify
        union {
            struct {
                uint2_t                _value1;
                uint2_t                _value2;
            } values;
            extnum_t extent;
        } _u;


    public:

        store_operation_param(snum_t snum, store_operation_t theOp)
        :
            _snum(snum), _op(theOp)
        {
            w_assert9(_op == smlevel_0::t_delete_store);
            _u.extent=0;
        };

        store_operation_param(snum_t snum, store_operation_t theOp, 
                store_flag_t theFlags, uint2_t theEff)
        :
            _snum(snum), _op(theOp)
        {
            w_assert9(_op == smlevel_0::t_create_store);
            _u.values._value1 = theFlags;
            _u.values._value2 = theEff;
        };
        store_operation_param(snum_t snum, store_operation_t theOp, 
                store_deleting_t newValue, 
                store_deleting_t oldValue = smlevel_0::t_unknown_deleting)
        :
            _snum(snum), _op(theOp)
        {
            w_assert9(_op == smlevel_0::t_set_deleting);
            _u.values._value1=newValue;
            _u.values._value2=oldValue;
        };
        store_operation_param(snum_t snum, store_operation_t theOp, 
                store_flag_t newFlags, 
                store_flag_t oldFlags = smlevel_0::st_bad)
        :
            _snum(snum), _op(theOp)
        {
            w_assert9(_op == smlevel_0::t_set_store_flags);
            _u.values._value1=newFlags;
            _u.values._value2=oldFlags;
        };
        store_operation_param(snum_t snum, store_operation_t theOp, 
                extnum_t theExt)
        :
            _snum(snum), _op(theOp)
        {
            w_assert9(_op == smlevel_0::t_set_first_ext);
            _u.extent=theExt;
        };
        snum_t snum()  const
        {
            return _snum;
        };
        store_operation_t op()  const
        {
            return (store_operation_t)_op;
        };
        store_flag_t new_store_flags()  const
        {
            w_assert9(_op == smlevel_0::t_create_store 
                || _op == smlevel_0::t_set_store_flags);
            return (store_flag_t)_u.values._value1;
        };
        store_flag_t old_store_flags()  const
        {
            w_assert9(_op == smlevel_0::t_set_store_flags);
            return (store_flag_t)_u.values._value2;
        };
        void set_old_store_flags(store_flag_t flag)
        {
            w_assert9(_op == smlevel_0::t_set_store_flags);
            _u.values._value2 = flag;
        }
        extnum_t first_ext()  const
        {
            w_assert9(_op == smlevel_0::t_set_first_ext);
            return _u.extent;
        };
        store_deleting_t new_deleting_value()  const
        {
            w_assert9(_op == smlevel_0::t_set_deleting);
            return (store_deleting_t)_u.values._value1;
        };
        store_deleting_t old_deleting_value()  const
        {
            w_assert9(_op == smlevel_0::t_set_deleting);
            return (store_deleting_t)_u.values._value2;
        };
        void set_old_deleting_value(store_deleting_t old_value)
        {
            w_assert9(_op == smlevel_0::t_set_deleting);
            _u.values._value2 = old_value;
        }
        uint2_t eff()  const
        {
            w_assert9(_op == smlevel_0::t_create_store);
            return _u.values._value2;
        };
        int size()  const
        {
            return sizeof (*this);
        };

    private:
        store_operation_param();
};


// This is not within the namescope of io_m because it's
// used by the volume manager, which doesn't get the
// definition of io_m.
/* Why lock_force(), as opposed to simple lock()?
 * Lock_force forces the lock to be acquired in the core
 * lock table, even if the lock cache contains a parent
 * lock that subsumes the requested lock.
 */
class vol_io_shared : public w_base_t {
    public:
    static rc_t io_lock_force( const lockid_t&         n, 
                    lock_mode_t             m, 
                    lock_duration_t         d, 
                    timeout_in_ms           timeout,  
                    lock_mode_t*            prev_mode = 0, 
                    lock_mode_t*            prev_pgmode = 0, 
                    lockid_t**              nameInLockHead = 0 
                    );
};

/*
 * IO Manager.
 */
class io_m : public smlevel_0 
{
    friend rc_t vol_io_shared::io_lock_force( const lockid_t&         n, 
                    lock_mode_t             m, 
                    lock_duration_t         d, 
                    timeout_in_ms           timeout,  
                    lock_mode_t*            prev_mode = 0, 
                    lock_mode_t*            prev_pgmode = 0, 
                    lockid_t**              nameInLockHead = 0 
                    );
public:
    NORET                       io_m();
    NORET                       ~io_m();
    
    static int                  max_extents_on_page();
    static void                 clear_stats();
    static int                  num_vols();
    
  
    /*
     * Device related
     */
    static bool                 is_mounted(const char* dev_name);
    static rc_t                 mount_dev(const char* device, u_int& vol_cnt);
    static rc_t                 dismount_dev(const char* device);
    static rc_t                 dismount_all_dev();
    static rc_t                 get_lvid(const char* dev_name, lvid_t& lvid);
    static rc_t                 list_devices(
        const char**&                 dev_list, 
        devid_t*&                     devid_list, 
        u_int&                        dev_cnt);

    static rc_t                 get_device_quota(
        const char*                   device, 
        smksize_t&                    quota_KB, 
        smksize_t&                    quota_used_KB);
    

    /*
     * Volume related
     */

    static rc_t                 get_vols(
        int                           start,
        int                           count, 
        char                          **dname, 
        vid_t                         vid[],
        int&                          return_cnt);
    static rc_t                 check_disk(const vid_t &vid);
    static rc_t                 check_store_pages(const stid_t &stid, 
                                                  page_p::tag_t);
    // return an unused vid_t
    static rc_t                 get_new_vid(vid_t& vid);
    static bool                 is_mounted(vid_t vid);
    static vid_t                get_vid(const lvid_t& lvid);
    static lvid_t               get_lvid(const vid_t vid);
    static const char*          dev_name(vid_t vid);
    static lsn_t                GetLastMountLSN();                // used for logging/recovery purposes
    static void                 SetLastMountLSN(lsn_t theLSN);

    static rc_t                 read_page(
        const lpid_t&                 pid,
        page_s&                       buf);
    static void                 write_many_pages(const page_s* bufs, int cnt);
    
    static rc_t                 mount(
         const char*                  device, 
         vid_t                        vid, 
         const bool                   apply_fake_io_latency = false, 
         const int                    fake_disk_latency = 0);
    static rc_t                 dismount(vid_t vid, bool flush = true);
    static rc_t                 dismount_all(bool flush = true);
    static rc_t                 sync_all_disks();


    // fake_disk_latency
    static rc_t                 enable_fake_disk_latency(vid_t vid);
    static rc_t                 disable_fake_disk_latency(vid_t vid);
    static rc_t                 set_fake_disk_latency(
        vid_t                          vid, 
        const int                      adelay);


    static rc_t                 get_volume_quota(
        vid_t                          vid, 
        smksize_t&                     quota_KB, 
        smksize_t&                     quota_used_KB,
        w_base_t::uint4_t&             exts_used
        );
    
    // Allocate a file_p page
    static rc_t                 alloc_a_file_page(
        alloc_page_filter_t            *filter,
        const stid_t&                   stid,
        const lpid_t&                   near,
        lpid_t&                         pids,
        lock_mode_t                     desired_lock_mode,
        bool                            search_file
        );
    // Allocate a single page.
    static rc_t                 alloc_a_page(
        const stid_t&                   stid,
        const lpid_t&                   near,
        lpid_t&                         pids,
        bool                            may_realloc, 
        lock_mode_t                     desired_lock_mode,
        bool                            search_file
        );

    // Allocate the whole group or nothing:
    static rc_t                 alloc_page_group(
        const stid_t&                   stid,
        const lpid_t&                   near,
        int                             cnt,
        lpid_t                          pids[],
        lock_mode_t                     desired_lock_mode
        );

private:
    // alloc pages and return error if don't get them all. Let caller
    // decide what to do.
    // NOTE: I made this private because now I want calls to be
    // to alloc_page_group or to alloc_a_page.
    static rc_t                 _alloc_pages(
        const stid_t&                   stid,
        const lpid_t&                   near,
        int                             cnt,
        int&                            cnt_got,
        lpid_t                          pids[],
        bool                            may_realloc, 
        lock_mode_t                     desired_lock_mode,
        bool                            search_file
        );

    static rc_t                 _free_page(const lpid_t& pid, 
                                        vol_t *v, bool chk_st_mmb);
public:


    static rc_t                 free_page(const lpid_t& pid, bool chk_st_mmb);
    static bool                 is_valid_page_of(const lpid_t& pid, snum_t s);

    static rc_t                 create_store(
        vid_t                          vid, 
        int                            EFF,
        store_flag_t                   flags,
        stid_t&                        stid,
        extnum_t                       first_ext = 0,
        extnum_t                       num_exts = 1);
    static rc_t                 destroy_store(
        const stid_t&                  stid,
        bool                           acquire_lock = true);
    static rc_t                 free_store_after_xct(const stid_t& stid);
    static rc_t                 free_ext_after_xct(const extid_t& extid);
    static rc_t                 get_store_flags(
        const stid_t&                  stid,
        store_flag_t&                  flags);
    static rc_t                 set_store_flags(
        const stid_t&                  stid,
        store_flag_t                   flags,
        bool                           sync_volume = true);
    static bool                 is_valid_store(const stid_t& stid);
    static rc_t                 max_store_id_in_use(
        vid_t                          vid,
        snum_t&                        snum);
    static rc_t                 update_ext_histo(
        const lpid_t&                  pid, 
        space_bucket_t                 b);
    static rc_t                 init_store_histo(
        store_histo_t*                 h, 
        const stid_t&                  stid,
        pginfo_t*                      pages, 
        int&                           numpages);

    
    // The following functinos return space utilization statistics
    // on the volume or selected stores.  These functions use only
    // the store and page/extent meta information.

    static rc_t                 get_volume_meta_stats(
        vid_t                          vid,
        SmVolumeMetaStats&             volume_stats);
    static rc_t                 get_file_meta_stats(
        vid_t                          vid,
        w_base_t::uint4_t              num_files,
        SmFileMetaStats*               file_stats);
    static rc_t                 get_file_meta_stats_batch(
        vid_t                          vid,
        w_base_t::uint4_t              max_store,
        SmStoreMetaStats**             mapping);
    static rc_t                 get_store_meta_stats(
        stid_t                         snum,
        SmStoreMetaStats&              storeStats);

    // The following functions return the first/last/next pages in a
    // store.  If "allocated" is NULL then only allocated pages will be
    // returned.  If "allocated" is non-null then all pages will be
    // returned and the bool pointed to by "allocated" will be set to
    // indicate whether the page is allocated.
    static rc_t                 first_page(
        const stid_t&                    stid,
        lpid_t&                          pid,
        bool*                            allocated = NULL,
        lock_mode_t                      lock = NL);
    static rc_t                 last_page(
        const stid_t&                    stid,
        lpid_t&                          pid,
        bool*                            allocated = NULL,
        lock_mode_t                      mode = NL);
    static rc_t                 next_page(
        lpid_t&                          pid,
        bool*                            allocated = NULL,
        lock_mode_t                      lock = NL);
    static rc_t                 next_page_with_space(
        lpid_t&                          pid, 
        space_bucket_t                   needed,
        lock_mode_t                      lock = NL);

    // this reports du statistics
    static rc_t                 get_du_statistics( // DU DF
        vid_t                            vid,
        volume_hdr_stats_t&              stats,
        bool                             audit);

    // This function sets a milli_sec delay to occur before 
    // each disk read/write operation.  This is useful in discovering
    // thread sync bugs
    static rc_t                 set_disk_delay(
        w_base_t::uint4_t                milli_sec) { 
                                        _msec_disk_delay = milli_sec; 
                                        return RCOK; 
                                    }
  
    //
    // Statistics information
    //
    static void                 io_stats(
        u_long&                         reads, 
        u_long&                         writes, 
        u_long&                         allocs,
        u_long&                         deallocs, 
        bool                            reset);


    static rc_t                 store_operation(
        vid_t                           vid,
        const store_operation_param&    param);

    static rc_t                 free_exts_on_same_page(
        const stid_t&                   stid,
        extnum_t                        ext,
        extnum_t                        count);

    static rc_t                 set_ext_next(
        vid_t                           vid,
        extnum_t                        ext,
        extnum_t                        new_next);

    static rc_t                 free_stores_during_recovery(
        store_deleting_t                typeToRecover);

    static rc_t                 free_exts_during_recovery();

    static rc_t                 create_ext_list_on_same_page(
        const stid_t&                   stid,
        extnum_t                        next,
        extnum_t                        prev,
        extnum_t                        count,
        extnum_t*                       list);

private:

    // This is used to enter and leave the io monitor under normal
    // circumstances.

    class auto_leave_t {
    private:
        xct_t *_x;
        check_compensated_op_nesting ccon;
        void on_entering();
        void on_leaving() const;
    public:
        auto_leave_t(): _x(xct()), ccon(_x, __LINE__, __FILE__) {\
                                       if(_x) on_entering(); }
        ~auto_leave_t()               { if(_x) on_leaving(); }
    };
    // This is used to enter and leave while grabbing the
    // checkpoint-serialization mutex, used on mount
    // and dismount, since a checkpoint records the mounted
    // volumes, it can't be fuzzy wrt mounts and dismounts.
    class auto_leave_and_trx_release_t; // forward decl - in sm_io.cpp
    
    // Prime the volume's caches if not already primed
    static rc_t                 _prime_cache(vol_t *v, snum_t s);

    static int                  vol_cnt;
    static vol_t*               vol[max_vols];
    static w_base_t::uint4_t    _msec_disk_delay;
    static lsn_t                _lastMountLSN;

protected:
    /* lock_force: A function that calls the lock manager, but avoids
     * lock-mutex deadlocks in the process:
     * (Share this with the volume manager.)
     */
    static rc_t                 lock_force(
        const lockid_t&                n,
        lock_mode_t                    m,
        lock_duration_t                d,
        timeout_in_ms                  timeout,
        page_p*                        page = 0,
        lock_mode_t*                   prev_mode = 0,
        lock_mode_t*                   prev_pgmode = 0,
        lockid_t**                     nameInLockHead = 0
    );

public:
    //
    // For recovery & rollback ONLY:
    //
    static rc_t                 recover_pages_in_ext(
        vid_t                            vid,
        snum_t                           snum,
        extnum_t                         ext,
        const Pmap&                      pmap,
        bool                             is_alloc);

    static rc_t                 _recover_pages_in_ext(
        vid_t                            vid,
        snum_t                           snum,
        extnum_t                         ext,
        const Pmap&                      pmap,
        bool                             is_alloc);

    //
    // For debugging ONLY:
    //
    static rc_t                 dump_exts(
        ostream&                        o,
        vid_t                           vid,
        extnum_t                        start,
        extnum_t                        end);
    static rc_t                 dump_stores(
        ostream&                        o,
        vid_t                           vid,
        int                             start,
        int                             end);

private:

    static rc_t                 _mount_dev(const char* device, u_int& vol_cnt);
    static rc_t                 _dismount_dev(const char* device);
    static rc_t                 _get_lvid(const char* dev_name, lvid_t& lvid);
    
    static const char*          _dev_name(vid_t vid);
    static int                  _find(vid_t vid);

    // WARNING: this MUST MATCH the code in vol.h
    // The compiler requires this definition just for _find_and_grab.
    // We could have vol.cpp #include sm_io.h but we really don't want that
    // either.
    typedef mcs_rwlock VolumeLock;
    typedef void *     lock_state;

    static vol_t*               _find_and_grab(
        vid_t                          vid, 
        lock_state*                    me,
        bool                           for_write
    ); 

    static rc_t                 _get_volume_quota(
        vid_t                             vid, 
        smksize_t&                        quota_KB, 
        smksize_t&                        quota_used_KB,
        w_base_t::uint4_t&                exts_used
        );
    
    static vid_t                _get_vid(const lvid_t& lvid);
    static lvid_t               _get_lvid(const vid_t vid);
    static rc_t                 _dismount(vid_t vid, bool flush);
    static rc_t                 _dismount_all(bool flush);
    static rc_t                 _alloc_pages_with_vol_mutex(
        alloc_page_filter_t               *filter,
        vol_t*                            v,
        const stid_t&                     stid, 
        const lpid_t&                     near,
        int                               cnt,
        int &                             cnt_got, // number allocated
        lpid_t                            pids[],
        bool                              may_realloc,
        lock_mode_t                       desired_lock_mode,
        bool                              search_file
        );
    static rc_t                 _create_store(
        vid_t                           vid, 
        int                             EFF, 
        store_flag_t                    flags,
        stid_t&                         stid,
        extnum_t                        first_ext = 0,
        extnum_t                        num_exts = 1);
    static rc_t                 _get_store_flags(
        const stid_t&                   stid,
        store_flag_t&                   flags);
    static rc_t                 _set_store_flags(
        const stid_t&                   stid,
        store_flag_t                    flags,
        bool                            sync_volume);

    static rc_t                 _update_ext_histo(
        const lpid_t&                   pid, 
        space_bucket_t                  b);
    static rc_t                 _init_store_histo(
        store_histo_t*                  h, 
        const stid_t&                   stid,
        pginfo_t*                       pages,
        int&                            numpages);
public:

};




inline int
io_m::num_vols()
{
    return vol_cnt;
}

inline const char* 
io_m::dev_name(vid_t vid) 
{
    auto_leave_t enter;
    return _dev_name(vid);
}

inline lsn_t
io_m::GetLastMountLSN()
{
    return _lastMountLSN;
}

inline void
io_m::SetLastMountLSN(lsn_t theLSN)
{
    w_assert9(theLSN >= _lastMountLSN);
    _lastMountLSN = theLSN;
}


inline rc_t 
io_m::get_volume_quota(
        vid_t                             vid, 
        smksize_t&                    quota_KB, 
        smksize_t&                    quota_used_KB,
        w_base_t::uint4_t&                ext_used
        )
{
    auto_leave_t enter;
    return _get_volume_quota(vid, quota_KB, quota_used_KB, ext_used);
}


inline rc_t 
io_m::dismount_all(bool flush)
{
    auto_leave_t enter;
    return _dismount_all(flush);
}

inline rc_t
io_m::set_store_flags(const stid_t& stid, store_flag_t flags, bool sync_volume)
{
    rc_t r;
    if (stid.store)  {
        auto_leave_t enter;
        r = _set_store_flags(stid, flags, sync_volume);
        // exchanges i/o mutex for volume mutex
    }
    return r;
}

inline rc_t                         
io_m::update_ext_histo(const lpid_t& pid, space_bucket_t b)
{
    // Can't use enter()/leave() because
    // that causes 1thread mutex to be freed, and
    // we don't *really* need the 1thread mutex here, 
    // given that we're not doing any logging AND we
    // already have it.
    rc_t r;
    if (pid._stid.store)  {
        r = _update_ext_histo(pid, b);
    }
    return r;
}

inline rc_t                         
io_m::init_store_histo(store_histo_t* h, const stid_t& stid, 
        pginfo_t* pages, int& numpages)
{
    rc_t r;
    if (stid.store) {
        r = _init_store_histo(h, stid, pages, numpages);
    }
    return r;
}

/*<std-footer incl-file-exclusion='SM_IO_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
