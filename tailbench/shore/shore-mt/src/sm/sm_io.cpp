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

 $Id: sm_io.cpp,v 1.34.2.27 2010/03/25 18:05:16 nhall Exp $

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
#define IO_C

#ifdef __GNUG__
#pragma implementation
#endif

#if W_DEBUG_LEVEL > 1
// breakpoint for debugger
extern "C" void iostophere();
void iostophere() { }
#else
#define iostophere()
#endif

#include "sm_int_2.h"
#include "chkpt_serial.h"
#include "sm_du_stats.h"
#include "device.h"
#include "lock.h"
#include "xct.h"
#include "logrec.h"
#include "logdef_gen.cpp"
#include "crash.h"
#include "vol.h"
#include <auto_release.h>
#include <store_latch_manager.h>
// NOTE : this is shared with btree layer
store_latch_manager store_latches;

#include <new>
#include <map>
#include <set>
#include "sm_vtable_enum.h"

#ifdef EXPLICIT_TEMPLATE
template class vtable_func<vol_t>;
#endif


/*********************************************************************
 *
 *  Class static variables
 *
 *        _msec_disk_delay        : delay in performing I/O (for debugging)
 *        _mutex                  : make io_m a monitor
 *        vol_cnt                 : # volumes mounted in vol[]
 *        vol[]                   : array of volumes mounted
 *
 *********************************************************************/
uint4_t                  io_m::_msec_disk_delay = 0;
int                      io_m::vol_cnt = 0;
vol_t*                   io_m::vol[io_m::max_vols] = { 0 };
lsn_t                    io_m::_lastMountLSN = lsn_t::null;

// used for most io_m methods:
void
io_m::auto_leave_t::on_entering() 
{
    if(_x->update_threads()) _x->start_crit();
    else _x = NULL;
}
void
io_m::auto_leave_t::on_leaving() const
{
    _x->stop_crit();
}

// used for mount/dismount.  Same as auto_leave_t but
// also grabs/releases the checkpoint-serialization mutex.
// The order in which we want to do this precludes just
// inheriting from auto_leave_t.
class io_m::auto_leave_and_trx_release_t {
private:
    xct_t *_x;
    void on_entering() { 
        if(_x) _x->start_crit();
        else _x = NULL;
    }
    void on_leaving() const { _x->stop_crit(); }
public:
    auto_leave_and_trx_release_t() : _x(xct()) {
        chkpt_serial_m::trx_acquire();
        if(_x) on_entering();
    }
    ~auto_leave_and_trx_release_t() {
        if(_x) on_leaving(); 
        chkpt_serial_m::trx_release();
    }
};
/*********************************************************************/

int                        
io_m::max_extents_on_page() 
{
    return vol_t::max_extents_on_page();
}

rc_t
io_m::lock_force(
    const lockid_t&         n,
    lock_mode_t             m,
    lock_duration_t         d,
    timeout_in_ms           timeout, 
    page_p                  *page, // = 0  -- should be an extlink_p
    lock_mode_t*            prev_mode, // = 0
    lock_mode_t*            prev_pgmode, // = 0
    lockid_t**              nameInLockHead // = 0
)
{
    /*
     * Why lock_force(), as opposed to simple lock()?
     * Lock_force forces the lock to be acquired in the core
     * lock table, even if the lock cache contains a parent
     * lock that subsumes the requested lock.
     * The I/O layer needs this to prevent re-allocation of
     * (de-allocated) pages before their time.
     * In other words, a lock serves to reserve a page.
     * When looking for a page to allocate, the lock manager
     * is queried to see if ANY lock is held on the page (even
     * by the querying transaction).
     */

   /* Try to acquire the lock w/o a timeout first */
   rc_t  rc = lm->lock_force(n, m, d, WAIT_IMMEDIATE,
            prev_mode, prev_pgmode, nameInLockHead);
   if(rc.is_error() && (rc.err_num() == eLOCKTIMEOUT)
        && timeout != WAIT_IMMEDIATE) {
       w_assert9(me()->xct());
       if(page && page->is_fixed()) {
          page->unfix();
       }

       rc = lm->lock_force(n, m, d, timeout,
                prev_mode, prev_pgmode, nameInLockHead);

   }
   return rc;
}

/* friend -- called from vol.cpp */
rc_t
vol_io_shared::io_lock_force(
    const lockid_t&         n,
    lock_mode_t             m,
    lock_duration_t         d,
    timeout_in_ms           timeout,
    lock_mode_t*            prev_mode,
    lock_mode_t*            prev_pgmode,
    lockid_t**              nameInLockHead
    )
{
    return smlevel_0::io->lock_force(n,m,d,timeout, 0/*page*/, 
        prev_mode, prev_pgmode, nameInLockHead);
}

io_m::io_m()
{
    _lastMountLSN = lsn_t::null;

}

/*********************************************************************
 *
 *  io_m::~io_m()
 *
 *  Destructor. Dismount all volumes.
 *
 *********************************************************************/
io_m::~io_m()
{
    W_COERCE(_dismount_all(shutdown_clean));
    store_latches.shutdown(); 
}

/*********************************************************************
 *
 *  io_m::find(vid)
 *
 *  Search and return the index for vid in vol[]. 
 *  If not found, return -1.
 *
 *********************************************************************/
int 
io_m::_find(vid_t vid)
{
    if (!vid) return -1;
    uint4_t i;
    for (i = 0; i < max_vols; i++)  {
        if (vol[i] && vol[i]->vid() == vid) break;
    }
    return (i >= max_vols) ? -1 : int(i);
}

inline vol_t * 
io_m::_find_and_grab(vid_t vid, lock_state* _me,
        bool for_write)
{
    if (!vid) {
        DBG(<<"vid " << vid);
        return 0;
    }
    vol_t** v = &vol[0];
    uint4_t i;
    for (i = 0; i < max_vols; i++, v++)  {
        if (*v) {
            if ((*v)->vid() == vid) break;
        }
    }
    if (i < max_vols) {
        w_assert1(*v);
        (*v)->assert_mutex_notmine(_me);
        (*v)->acquire_mutex(_me, for_write);
        w_assert3(*v && (*v)->vid() == vid);
        return *v;
    } else {
        return 0;
    }
}



/*********************************************************************
 *
 *  io_m::is_mounted(vid)
 *
 *  Return true if vid is mounted. False otherwise.
 *
 *********************************************************************/
bool
io_m::is_mounted(vid_t vid)
{
    auto_leave_t enter;
    return (_find(vid) >= 0);
}



/*********************************************************************
 *
 *  io_m::_dismount_all(flush)
 *
 *  Dismount all volumes mounted. If "flush" is true, then ask bf
 *  to flush dirty pages to disk. Otherwise, ask bf to simply
 *  invalidate the buffer pool.
 *
 *********************************************************************/
rc_t
io_m::_dismount_all(bool flush)
{
    for (int i = 0; i < max_vols; i++)  {
        if (vol[i])        {
            if(errlog) {
                errlog->clog 
                    << warning_prio
                    << "warning: volume " << vol[i]->vid() << " still mounted\n"
                << "         automatic dismount" << flushl;
            }
            W_DO(_dismount(vol[i]->vid(), flush));
        }
    }
    
    w_assert3(vol_cnt == 0);

    SET_TSTAT(vol_reads,0);
    SET_TSTAT(vol_writes,0);
    return RCOK;
}




/*********************************************************************
 *
 *  io_m::sync_all_disks()
 *
 *  Sync all volumes.
 *
 *********************************************************************/
rc_t
io_m::sync_all_disks()
{
    for (int i = 0; i < max_vols; i++) {
            if (_msec_disk_delay > 0)
                    me()->sleep(_msec_disk_delay, "io_m::sync_all_disks");
            if (vol[i])
                    vol[i]->sync();
    }
    return RCOK;
}




/*********************************************************************
 *
 *  io_m::_dev_name(vid)
 *
 *  Return the device name for volume vid if it is mounted. Otherwise,
 *  return NULL.
 *
 *********************************************************************/
const char* 
io_m::_dev_name(vid_t vid)
{
    int i = _find(vid);
    return i >= 0 ? vol[i]->devname() : 0;
}




/*********************************************************************
 *
 *  io_m::_is_mounted(dev_name)
 *
 *********************************************************************/
bool
io_m::is_mounted(const char* dev_name)
{
    auto_leave_t enter;
    return dev->is_mounted(dev_name);
}



/*********************************************************************
 *
 *  io_m::_mount_dev(dev_name, vol_cnt)
 *
 *********************************************************************/
rc_t
io_m::mount_dev(const char* dev_name, u_int& _vol_cnt)
{
    auto_leave_t enter;
    FUNC(io_m::_mount_dev);

    volhdr_t vhdr;
    W_DO(vol_t::read_vhdr(dev_name, vhdr));

        /* XXX possible bit-loss */
    device_hdr_s dev_hdr(vhdr.format_version(), 
                         vhdr.device_quota_KB(), vhdr.lvid());
    rc_t result = dev->mount(dev_name, dev_hdr, _vol_cnt);
    return result;
}



/*********************************************************************
 *
 *  io_m::_dismount_dev(dev_name)
 *
 *********************************************************************/
rc_t
io_m::dismount_dev(const char* dev_name)
{
    auto_leave_t enter;
    return dev->dismount(dev_name);
}


/*********************************************************************
 *
 *  io_m::_dismount_all_dev()
 *
 *********************************************************************/
rc_t
io_m::dismount_all_dev()
{
    auto_leave_t enter;
    return dev->dismount_all();
}


/*********************************************************************
 *
 *  io_m::_list_devices(dev_list, devid_list, dev_cnt)
 *
 *********************************************************************/
rc_t
io_m::list_devices(
    const char**&         dev_list, 
    devid_t*&                 devid_list, 
    u_int&                 dev_cnt)
{
    auto_leave_t enter;
    return dev->list_devices(dev_list, devid_list, dev_cnt);
}


/*********************************************************************
 *
 *  io_m::_get_vid(lvid)
 *
 *********************************************************************/
vid_t 
io_m::_get_vid(const lvid_t& lvid)
{
    uint4_t i;
    for (i = 0; i < max_vols; i++)  {
        if (vol[i] && vol[i]->lvid() == lvid) break;
    }

    // egcs 1.1.1 croaks on this stmt:
    // return (i >= max_vols) ? vid_t::null : vol[i]->vid();
    if(i >= max_vols) {
        return vid_t::null;
    } else {
        return vol[i]->vid();
    }
}


/*********************************************************************
 *  io_m::_get_device_quota()
 *********************************************************************/
rc_t
io_m::get_device_quota(const char* device, smksize_t& quota_KB,
                        smksize_t& quota_used_KB)
{
    auto_leave_t enter;
    W_DO(dev->quota(device, quota_KB));

    lvid_t lvid;
    W_DO(_get_lvid(device, lvid));
    if (lvid == lvid_t::null) {
        // no device on volume
        quota_used_KB = 0;
    } else {
        smksize_t _dummy;
        uint4_t          dummy2;
        W_DO(_get_volume_quota(_get_vid(lvid), quota_used_KB, _dummy, dummy2));
    }
    return RCOK;
}


/*********************************************************************
 *
 *  io_m::_get_lvid(dev_name, lvid)
 *
 *********************************************************************/
rc_t
io_m::_get_lvid(const char* dev_name, lvid_t& lvid)
{
    if (!dev->is_mounted(dev_name)) return RC(eDEVNOTMOUNTED);
    uint4_t i;
    for (i = 0; i < max_vols; i++)  {
        if (vol[i] && (strcmp(vol[i]->devname(), dev_name) == 0) ) break;
    }
    lvid = (i >= max_vols) ? lvid_t::null : vol[i]->lvid();
    return RCOK;
}



/*********************************************************************
 *
 *  io_m::_get_vols(start, count, dname, vid, ret_cnt)
 *
 *  Fill up dname[] and vid[] starting from volumes mounted at index
 *  "start". "Count" indicates number of entries in dname and vid.
 *  Return number of entries filled in "ret_cnt".
 *
 *********************************************************************/
rc_t
io_m::get_vols(
    int         start, 
    int         count,
    char        **dname,
    vid_t       vid[], 
    int&        ret_cnt)
{
    auto_leave_t enter;
    ret_cnt = 0;
    w_assert1(start + count <= max_vols);
   
    /*
     *  i iterates over vol[] and j iterates over dname[] and vid[]
     */
    int i, j;
    for (i = start, j = 0; i < max_vols; i++)  {
        if (vol[i])  {
            w_assert0(j < count); // caller's programming error if we fail here
            vid[j] = vol[i]->vid();
            strncpy(dname[j], vol[i]->devname(), max_devname);
            j++;
        }
    }
    ret_cnt = j;
    return RCOK;
}



/*********************************************************************
 *
 *  io_m::_get_lvid(vid)
 *
 *********************************************************************/
lvid_t
io_m::get_lvid(const vid_t vid)
{
    auto_leave_t enter;
    int i = _find(vid);
    return (i >= max_vols) ? lvid_t::null : vol[i]->lvid();
}


/*********************************************************************
 *
 *  io_m::get_lvid(dev_name, lvid)
 *
 *********************************************************************/
rc_t
io_m::get_lvid(const char* dev_name, lvid_t& lvid)
{
    auto_leave_t enter;
    return _get_lvid(dev_name, lvid);
}

/*********************************************************************
 *
 *  io_m::mount(device, vid)
 *
 *  Mount "device" with vid "vid".
 *
 *********************************************************************/
rc_t
io_m::mount(const char* device, vid_t vid,
            const bool apply_fake_io_latency, const int fake_disk_latency)
{
    FUNC(io_m::mount);
    // grab chkpt_mutex to prevent mounts during chkpt
    // need to serialize writing dev_tab and mounts
    auto_leave_and_trx_release_t acquire_and_enter;
    DBG( << "_mount(name=" << device << ", vid=" << vid << ")");
    uint4_t i;
    for (i = 0; i < max_vols && vol[i]; i++) ;
    if (i >= max_vols) return RC(eNVOL);

    vol_t* v = new vol_t(apply_fake_io_latency,fake_disk_latency);  // deleted on dismount
    if (! v) return RC(eOUTOFMEMORY);

    w_rc_t rc = v->mount(device, vid);
    if (rc.is_error())  {
        delete v;
        return RC_AUGMENT(rc);
    }

    int j = _find(vid);
    if (j >= 0)  {
        W_COERCE( v->dismount(false) );
        delete v;
        return RC(eALREADYMOUNTED);
    }
    
    ++vol_cnt;

    w_assert9(vol[i] == 0);
    vol[i] = v;

    if (log && smlevel_0::logging_enabled)  {
        logrec_t* logrec = new logrec_t; //deleted at end of scope
        w_assert1(logrec);

        new (logrec) mount_vol_log(device, vid);
        logrec->fill_xct_attr(tid_t::null, GetLastMountLSN());
        lsn_t theLSN;
        W_COERCE( log->insert(*logrec, &theLSN) );

        DBG( << "mount_vol_log(" << device << ", vid=" << vid 
                << ") lsn=" << theLSN << " prevLSN=" << GetLastMountLSN());
        SetLastMountLSN(theLSN);

        delete logrec;
    }

    SSMTEST("io_m::_mount.1");

    return RCOK;
}

/*********************************************************************
 *
 *  io_m::dismount(vid, flush)
 *  io_m::_dismount(vid, flush)
 *
 *  Dismount the volume "vid". "Flush" indicates whether to write
 *  dirty pages of the volume in bf to disk.
 *
 *********************************************************************/
rc_t
io_m::dismount(vid_t vid, bool flush)
{
    // grab chkpt_mutex to prevent dismounts during chkpt
    // need to serialize writing dev_tab and dismounts

    auto_leave_and_trx_release_t acquire_and_enter;
    return _dismount(vid, flush);
}


rc_t
io_m::_dismount(vid_t vid, bool flush)
{
    FUNC(io_m::_dismount);
    DBG( << "_dismount(" << "vid=" << vid << ")");
    int i = _find(vid); 
    if (i < 0) return RC(eBADVOL);

    W_COERCE(vol[i]->dismount(flush));

    if (log && smlevel_0::logging_enabled)  {
        logrec_t* logrec = new logrec_t; //deleted at end of scope
        w_assert1(logrec);

        new (logrec) dismount_vol_log(_dev_name(vid), vid);
        logrec->fill_xct_attr(tid_t::null, GetLastMountLSN());
        lsn_t theLSN;
        W_COERCE( log->insert(*logrec, &theLSN) );
        DBG( << "dismount_vol_log(" << _dev_name(vid) 
                << endl
                << ", vid=" << vid << ") lsn=" << theLSN << " prevLSN=" << GetLastMountLSN());;
        SetLastMountLSN(theLSN);

        delete logrec;
    }

    delete vol[i];
    vol[i] = 0;
    
    --vol_cnt;
  
    SSMTEST("io_m::_dismount.1");

    DBG( << "_dismount done.");
    return RCOK;
}


/*********************************************************************
 *
 *  io_m::_{enable,disable,set}_fake_disk_latency
 *
 *  Manipulate the disk latency of a volume
 *
 *********************************************************************/
rc_t
io_m::enable_fake_disk_latency(vid_t vid)
{
    auto_leave_t enter;
    int i = _find(vid);
    if (i < 0)  return RC(eBADVOL);

    vol[i]->enable_fake_disk_latency();
    return (RCOK);
}

rc_t
io_m::disable_fake_disk_latency(vid_t vid)
{
    auto_leave_t enter;
    int i = _find(vid);
    if (i < 0)  return RC(eBADVOL);

    vol[i]->disable_fake_disk_latency();
    return (RCOK);
}

rc_t
io_m::set_fake_disk_latency(vid_t vid, const int adelay)
{
    auto_leave_t enter;
    int i = _find(vid);
    if (i < 0)  return RC(eBADVOL);

    if (!vol[i]->set_fake_disk_latency(adelay)) 
      return RC(eBADVOL); // IP: should return more appropriate eror code

    return (RCOK);
}



/*********************************************************************
 *
 *  io_m::_get_volume_quota(vid, quota_KB, quota_used_KB, exts_used)
 *
 *  Return the "capacity" of the volume and number of Kbytes "used"
 *  (allocated to extents)
 *
 *********************************************************************/
rc_t
io_m::_get_volume_quota(vid_t vid, smksize_t& quota_KB, smksize_t& quota_used_KB, uint4_t &used)
{
    int i = _find(vid);
    if (i < 0)  return RC(eBADVOL);

    W_DO( vol[i]->num_used_exts(used) );
    quota_used_KB = used*ext_sz*(page_sz/1024);

    quota_KB = vol[i]->num_exts()*ext_sz*(page_sz/1024);
    return RCOK;
}



/*********************************************************************
 *
 *  io_m::_check_disk(vid)
 *
 *  Check the volume "vid".
 *
 * For use by smsh: extern "C" API
 *********************************************************************/
extern "C" void check_disk(const vid_t &vid);
void check_disk(const vid_t &vid)
{
    W_COERCE(io_m::check_disk(vid));
}

// GRAB_*
// Since the statistics warrant, perhaps we've changed the
// volume mutex to an mcs_rwlock
#define GRAB_R \
    lock_state rme_node; \
    vol_t *v = _find_and_grab(volid, &rme_node, false); \
    if (!v)  return RC(eBADVOL); \
    auto_release_r_t<VolumeLock> release_on_return(v->vol_mutex());

#define GRAB_W \
    lock_state wme_node; \
    vol_t *v = _find_and_grab(volid, &wme_node, true); \
    if (!v)  return RC(eBADVOL); \
    auto_release_w_t<VolumeLock> release_on_return(v->vol_mutex());

rc_t
io_m::check_disk(const vid_t &volid)
{
    auto_leave_t enter;
    GRAB_R;

    W_DO( v->check_disk() );

    return RCOK;
}


/*********************************************************************
 *
 *  io_m::_get_new_vid(vid)
 *
 *********************************************************************/
rc_t
io_m::get_new_vid(vid_t& vid)
{
    auto_leave_t enter;
    for (vid = vid_t(1); vid != vid_t::null; vid.incr_local()) {
        int i = _find(vid);
        if (i < 0) return RCOK;;
    }
    return RC(eNVOL);
}


vid_t
io_m::get_vid(const lvid_t& lvid)
{
    auto_leave_t enter;
    return _get_vid(lvid);
}



/*********************************************************************
 *
 *  io_m::read_page(pid, buf)
 * 
 *  Read the page "pid" on disk into "buf".
 *
 *********************************************************************/
rc_t
io_m::read_page(const lpid_t& pid, page_s& buf)
{
    FUNC(io_m::read_page);

    /*
     *  NO enter() *********************
     *  NEVER acquire mutex to read page
     */

    if (_msec_disk_delay > 0)
            me()->sleep(_msec_disk_delay, "io_m::read_page");

    int i = _find(pid.vol());
    if (i < 0) {
        return RC(eBADVOL);
    }
    DBG( << "reading page: " << pid );

    W_DO( vol[i]->read_page(pid.page, buf) );

    INC_TSTAT(vol_reads);
    buf.pid._stid.vol = pid.vol();

    /*  Verify that we read in the correct page.
     *
     *  w_assert9(buf.pid == pid);
     *
     *  NOTE: that the store ID may not be correct during redo-recovery
     *  in the case where a page has been deallocated and reused.
     *  This can arise because the page will have a new store ID.
     *  If the page LSN is 0 then the page is
     *  new and should have a page ID of 0.
     */
#if W_DEBUG_LEVEL > 2
    if (buf.lsn1 == lsn_t::null)  {
        if(smlevel_1::log && smlevel_0::logging_enabled) {
            w_assert3(buf.pid.page == 0);
        }
    } else {
        w_assert3(buf.pid.page == pid.page && buf.pid.vol() == pid.vol());
    }
    DBG(<<"buf.pid.page=" << buf.pid.page << " buf.lsn1=" << buf.lsn1);
#endif 
    
    return RCOK;
}



/*********************************************************************
 *
 *  io_m::write_many_pages(bufs, cnt)
 *
 *  Write "cnt" pages in "bufs" to disk.
 *
 *********************************************************************/
void 
io_m::write_many_pages(const page_s* bufs, int cnt)
{
    // NEVER acquire monitor to write page
    vid_t vid = bufs->pid.vol();
    int i = _find(vid);
    w_assert1(i >= 0);

    if (_msec_disk_delay > 0)
            me()->sleep(_msec_disk_delay, "io_m::write_many_pages");

#if W_DEBUG_LEVEL > 2
    {
        for (int j = 1; j < cnt; j++) {
            w_assert1(bufs[j].pid.page - 1 == bufs[j-1].pid.page);
            w_assert1(bufs[j].pid.vol() == vid);
        }
    }
#endif 

    W_COERCE( vol[i]->write_many_pages(bufs[0].pid.page, bufs, cnt) );
    INC_TSTAT(vol_writes);
    ADD_TSTAT(vol_blks_written, cnt);
}

rc_t                 
io_m::_prime_cache(vol_t *v, snum_t s)
{
    // Is the cache empty?  If not, return.
    if(v->is_primed(s)) return RCOK;

    // prime cache for given store.
    return v->prime_cache(s);
}

/*********************************************************************
 *
 * Allocate a group; if can't get them all, free what we allocated.
 *  io_m::alloc_page_group(stid, near, 
 *         npages, 
 *         pids[], may_realloc, desired_lock_mode, search_file)
 *
 * Allocate one:
 *  io_m::alloc_a_page(stid, near, 
 *         pids[], may_realloc, desired_lock_mode, search_file);
 *
 * Allocate a group; if can't get them all, return error, having allocated
 * some but not all:
 * private:
 *  io_m::_alloc_pages(stid, near, 
 *         npages, ngot, 
 *         pids[], may_realloc, desired_lock_mode, search_file)
 *
 * Allocate a group; helper for _alloc_pages().
 * private:
 *  io_m::_alloc_pages_with_vol_mutex(vol_t *, stid, near, 
 *         npages, ngot, 
 *         pids[], may_realloc, desired_lock_mode, search_file)
 *
 *  Allocates "npages" pages for store "stid" and return the page id
 *  allocated in "pids[]". If a "near" pid is given, efforts are made
 *  to allocate the pages near it. "Forward_alloc" indicates whether
 *  to start search for free extents from the last extent of "stid".
 *
 *  may_realloc: volume layer uses this to determine whether it
 *  should grab an instant(EX) or long-term(desired_lock_mode) 
 *  lock on the page
 *
 *  search_file: true means go ahead and look for more free pages
 *  in existing extents in the file.  False means if you can't
 *  get the # pages you want right away, just allocate more extents.
 *  Furthermore, all pages returned must be at the end of the file.
 *
 *********************************************************************/

// For now, we need to be able to test if this helps:
static bool do_prime_caches = true;

rc_t
io_m::_alloc_pages(
    const stid_t&        stid,
    const lpid_t&        near_p,
    int                  npages,
    int&                 ngot,
    lpid_t               pids[],
    bool                 may_realloc,
    lock_mode_t          desired_lock_mode,
    bool                 search_file // false-->append-only semantics
    ) 
{
    FUNC(io_m::_alloc_pages);
    auto_leave_t enter;

    /*
     *  Find the volume of "stid"
     */
    DBG(<<"stid " << stid);
    vid_t volid = stid.vol;

    GRAB_W;

    // I hate to do this holding the volume mutex but oh, well...
    if(do_prime_caches) W_DO(_prime_cache(v, stid.store));

    alloc_page_filter_yes_t ok; // accepts any page

    rc_t r = 
        _alloc_pages_with_vol_mutex(
                &ok, 
                v, stid, near_p, npages, ngot, pids, 
            may_realloc, desired_lock_mode, search_file);

    return r;
}

/*
 * alloc_a_file_page: this is for the file layer only; This used
 * to be located in the file layer, which grabbed the anchors
 *  and then descended to
 *  X_DO( io->alloc_a_page(fid, 
                near_p,  // near hint
                allocPid,  // output array
                false,      // not may_realloc
                IX,        // lock mode on allocated page
                search_file
                ), anchor);

    which in turn did

    W_DO( _alloc_pages(ftid, near_p, 
                1, ngot, 
                &allocPid, 
                false, 
                desired_lock_mode, search_file) );

    The  problem with that is that it reversed the normal order of
    mutex-grabbing: 
        volume mutex then anchor 
    (is the way it always was
    before we decided we needed to compensate the file_p allocation.)
    By putting the compensation in the file layer, we created this order:
        anchor then volume mutex
    which gave us potential mutex-mutex deadlocks.

    So we created this method to allow the file layer to accommplish
    the same thing but entirely inside the volume layer's mutex
    protection.
 */

rc_t
io_m::alloc_a_file_page(
    alloc_page_filter_t             *filter,
    const stid_t&                   fid,
    const lpid_t&                   near_p,
    /* 
     * near_p indicates a page already allocated to the file.
     * If search_file==false, we can still look for a page in that
     * near_p's extent as long as that page is after the page indicated by
     * near_p. That is, if search_file==false, we are necessarily appending.
     */
    lpid_t&                         allocPid,
    lock_mode_t                     desired_lock_mode,
    bool                            search_file
)
{
    FUNC(io_m::alloc_a_file_page);
    auto_leave_t enter;
    vid_t volid = fid.vol;

    GRAB_W;

    // I hate to do this holding the volume mutex but oh, well...
    if(do_prime_caches) W_DO(_prime_cache(v, fid.store));

    // Compensate around the page allocation so that
    // the undo of this page allocation is logical in the sense that
    // it checks for the page being empty and might not free the
    // page.

    // When we compensate to that anchor, the underlying
    // code automagically releases the anchor at the time
    // we compensate, but we don't want to do that until
    // we have finished logging the page allocation.
    
    check_compensated_op_nesting ccon(xct(), __LINE__, __FILE__);
    auto_release_anchor_t auto_anchor(true/*and compensate*/, __LINE__); 
    // see xct.h for auto_release_anchor_t.

    int ngot=0;
    W_DO( _alloc_pages_with_vol_mutex(
            filter,
            v, fid, near_p, 1, ngot, 
            &allocPid, 
     /*
     *  The I/O layer locks the page instantly if may_realloc is passed
     *  in by the resource manager.  If not may_realloc, it acquires
     *        a long lock on the page.
     */
            false,  /* may_realloc*/

            desired_lock_mode, search_file));

    w_assert1(ngot == 1);
    w_assert1(filter->accepted());
    filter->check(); // verifies that the page is indeed EX-latched.
    // note: filter holds the file_p 

    /* Compensate around the updates to the extent page
     * that allocated the page. If we croak before this
     * compensation, the page will be deallocated on rollback, but
     * that's ok because we have the page fixed, meaning no other
     * xct could slip in there and do anything with the page.
     */

    auto_anchor.compensate(); // releases anchor ; checks for legit xct
    /*
     * Now that we compensated, if we croak right here, the page won't be
     * deallocated on restart. It will remain in the store, but will be
     * unformatted.  That is a problem. It would be nice to
     * format the page in filter.accept(), assuming there would be
     * no problem with not undoing ANY of the format/page-init record.
     *
     * Another help would be to resurrect undoable_clrs in a way that
     * the compensation and log insert could happen atomically.
     * Filed GNATS 129 about this.
     */
    rc_t rc = log_alloc_file_page(allocPid);
    int count=10;
    while (rc.is_error() && (rc.err_num() == eRETRY) && (--count > 0)) {
        rc = log_alloc_file_page(allocPid);
    }
    if(rc.is_error()) {
        // Couldn't log the allocation; free the page. Note that
        // this just undoes the page allocation in the extents.
        // We still hold the EX latch on the file page, even though
        // we haven't touched it yet.
        fprintf(stderr, "could not log_alloc_file_page\n");
        rc = _free_page(allocPid, v, false /*do NOT check store mmb*/);
    }
    // This would be pretty bad, but quite possible... Maybe we
    // got a lock deadlock -or- say someone else acquired the IX lock
    // on the page... But that shouldn't happen b/c latch_lock_get_slot
    // needs the latch on the page before it grabs the lock, and
    // it gives up trying on this page if it can't get the EX latch.
    // Thus, the only way this should ever happen is something like
    // inability to insert into the log.
    W_COERCE(rc);

    // Ok, assuming we inserted the log_alloc_file_page, then
    // undo will deallocate the page. The file_m hangs onto the EX
    // latch until the page is formatted.
    // A crash between here and page formatting will roll back the
    // entire page allocation.

    w_assert1(filter->accepted());
    filter->check(); // still hold the EX latch
    return RCOK;
}

rc_t
io_m::alloc_a_page(
    const stid_t&        stid,
    const lpid_t&        near_p,
    lpid_t&              pid,
    bool                 may_realloc,
    lock_mode_t          desired_lock_mode,
    bool                 search_file // false-->append-only semantics
    ) 
{
    FUNC(io_m::alloc_a_page);
    int ngot=0;
    return _alloc_pages(stid, near_p, 
            1, ngot, &pid, may_realloc, desired_lock_mode, search_file);
}


/*
 *  alloc_page_group: used by file code to allocate a group of
 *  large-record pages, so it doesn't let you search the file
 *  (why not?)
 */

rc_t
io_m::alloc_page_group(
    const stid_t&        stid,
    const lpid_t&        near_p,
    int                  cnt,
    lpid_t               pids[],
    lock_mode_t          desired_lock_mode
    ) 
{
    FUNC(io_m::alloc_page_group);
    w_assert1(desired_lock_mode==IX);

    int ngot=0;
    DBG(<<" alloc_page_group wants " << cnt << " pages" );
    w_rc_t rc = _alloc_pages(stid, near_p, 
            cnt, ngot, pids, 
            false /*may_realloc*/, 
            desired_lock_mode,
            false /*search_file*/);
    // We've done enter() and leave() now.
    
    // This lets the lgrec.7 case (error
    // in the middle of allocating many pages leaves internally
    // inconsistent file) work when committing after running out
    // of extents; that is, it doesn't leave the file in inconsistent
    // condition.  Commit or abort works with this fix.
    if(ngot < cnt) {
        w_assert1(rc.is_error());
        DBG(<<"alloc_page_group want " << cnt << " got " << ngot
                << " error=" << rc);
        // free what we have so far.
        // Each call does enter() and leave()
        for(int i=0; i < ngot; i++) {
            DBG(<<"alloc_page_group freeing pids["<< i<< " ]=" << pids[i]);
            W_COERCE(free_page(pids[i], false/*checkstore*/));
        }
    } else { 
        w_assert1(!rc.is_error());
    }
    // Return the error if it occurred. We have freed the
    // pages so we have allocated nothing now.  The extents
    // might have been used by other xcts, but not these pages.
    // The caller must make sure this is true in the context
    // of this call.
    return rc;
}

class autoerase_t {
public:
    autoerase_t(vol_t *v, snum_t snum) : _v(v), _store(snum) {}
    ~autoerase_t() {
        extnum_t e;
        size_t i = _list.size();
        while( i-- > 0 ) {
             e = _list.back();
             w_assert1(e); // can't be extent 0
            DBG(<<" erasing (store,extent) " << _store
                    << "," << e);
            _v->free_ext_cache().erase(_store, e);
            _list.pop_front();
        }
    }
    void add(extnum_t e) {
        DBG(<<" adding extent " << e << " for store " << _store);
        _list.push_back(e);
    }

private:
    std::list<extnum_t> _list;
    vol_t* _v;
    snum_t _store;
};


/*
 * io_m::_alloc_pages_with_vol_mutex
 * Allocate pages while holding the volume mutex.
NOTE:
 * This is written quasi-generically, but here I'm putting in
 * a check that might some day become obsolete:
 * As of this writing, there is only one context in which we
 * allocate more than one page in a call: this is when we are
 * allocating for a large record.
 * That means the pages are in the lg_stid store for a file.
 * Since it is possible to fail after having allocated some, but not
 * all, the desired #pages, we have to do something if we don't want
 * to end up with an internally-inconsistent file (inconsistent in that
 * there are pages allocated, with no references to them, which means
 * an audit in get_du_statistics fails -- note that if the store is
 * destroyed, these pages are reclaimed).
 * Rather than have this method determine what to do if the
 * pages can't be allocated, we return the number of pages in fact
 * allocated, and we let the caller decide what to do with an
 * insufficient number.
 */

rc_t
io_m::_alloc_pages_with_vol_mutex(
    alloc_page_filter_t     *filter,
    vol_t *                 v,
    const stid_t&           stid,
    const lpid_t&           near_p,
    /* 
     * near_p indicates a page already allocated to the file.
     * If search_file==false, we can still look for a page in that
     * near_p's extent as long as that page is after the page indicated by
     * near_p. That is, if search_file==false, we are necessarily appending.
     */
    int                     npageswanted,
    int                     &Pcount, 
    lpid_t                  pids[],
    bool                    may_realloc,  
    lock_mode_t             desired_lock_mode,
    bool                    search_file // append-only semantics if false
)
{

    const int               npages(npageswanted);
    Pcount = 0; // number of pages already allocated

    FUNC(io_m::_alloc_pages_with_vol_mutex);
    w_assert2(npages > 0); // Shouldn't get here with 0

    /*
     *  Should we find that we need to allocate new 
     *  extents, we'll need to know the extnum_t of the
     *  last extent in the store, so that the volume layer
     *  can link the new extents to the end of the store.
     *  So we'll stash that information if we learn it 
     *  earlier.
     */
    extnum_t    last_ext_in_store = 0;
    bool        is_last_ext_in_store = false;

    /*
     *  Do we have a near hint?  If so use it to locate
     *  the pid of a page in an extent where we'll start
     *  looking for free space.  (NB: if we want append-only
     *  semantics, we need to pass in last page of store for
     *  the near hint.)
     *  The extent# is really what we're after.  
     *  We will start with an extent number and some idea 
     *  whether or not that extent
     *  contains reserved but unallocted pages.
     */

    extnum_t    extent=0;
    int         nresvd=0; // # free pages thought to be in the store

    bool        not_worth_trying=false;
                   // vol_t::last_page(...,&not_worth_trying)
                   // sets not_worth_trying to true if 
                   // last reserved page is already allocated

    lpid_t      near_pid = lpid_t::null;

    if (&near_p == &lpid_t::eof)  {
        W_DO(v->last_reserved_page(stid.store, near_pid, not_worth_trying));
        w_assert9(near_pid.page);
        is_last_ext_in_store = true;
    } else if (&near_p == &lpid_t::bof)  {
        W_DO(v->first_page(stid.store, near_pid, &not_worth_trying));
        w_assert9(near_pid.page);
    } else if (near_p.page) {
        near_pid = near_p;
    } 

    DBGTHRD(<<"near_pid = " << near_pid);

    /*
     * Got a starting page yet? If not, try last extent used (cached)
     * 
     *  NB: this might not be a good idea because if the
     *  last extent used cached by another tx, there might
     *  be lock conflicts
     */
    if(near_pid.page) {
        /* See if the near_page given is legit */
        DBGTHRD(<<"given near_pid " << near_pid);
        extent = v->pid2ext(near_pid);

        if( extent &&  !v->is_alloc_ext_of(extent, stid.store) ) {
            // Not valid
            near_pid = lpid_t::null;
        }
        nresvd=1; // Assume that the near page's extent isn't empty
    }

    if(!near_pid.page) {
        W_DO(v->last_reserved_page(stid.store,near_pid, not_worth_trying));
        DBGTHRD(<<"got last page = " << near_pid);
        w_assert1(near_pid.page); 
        is_last_ext_in_store = true;

        nresvd = not_worth_trying? 0: 1; //for starters
        if(near_pid.page) {
            extent = v->pid2ext(near_pid);
        } else {
            w_assert1(extent != 0);
        }
        w_assert1(v->is_alloc_ext_of(extent, stid.store));
    }

    if(is_last_ext_in_store) {
        last_ext_in_store = extent;
    }

    w_assert1(v->is_alloc_ext_of(extent, stid.store));


    /*
     * get extent fill factor for store, so that we can use
     * it when allocating pages from extents
     */
    int eff = 100;
#if COMMENT
    eff = v->fill_factor(stid.store);
    if(eff < 100) {
        /* 
         * Not implemented because 1) it doesn't work
         * with our nresvd hint, 2) nowhere does anything
         * pass in anything but 100 through the ss_m interface
         */
        return RC(fcNOTIMPLEMENTED);
    }
#endif


    /* 
     * First thing we must do is try to allocate from the given extent,
     * which is the last extent of the store or is given by the near_pid.
     * Then we must prevent use of the cache in the append-only case.
     */
    if(!search_file) {
        int remaining_in_ext=0;
        int allocated = 0;
        W_DO(v->alloc_pages_in_ext(
                                      filter,
                                      !search_file,
                                      extent,
                                      eff,
                                      stid.store,
                                      npages-Pcount,
                                      pids+Pcount,
                                      allocated,
                                      remaining_in_ext,
                                      is_last_ext_in_store,
                                      may_realloc,
                                      desired_lock_mode));
        W_IFDEBUG1(if(allocated==1) w_assert1(filter->accepted());)
        filter->check();

        Pcount += allocated;
        nresvd -= allocated;

        w_assert3(Pcount <= npages);
        if(Pcount == npages) {
            DBGTHRD( << "allocated " << npages);
            ADD_TSTAT(page_alloc_cnt, Pcount);
            return RCOK;
        }
    }

    // We must not use the cache or the linear search if we are called for
    // append-only policy
    if(search_file)
    {

        /* FRJ: the common case is that we allocate pages much more often
           than we free them. Because the cost to search before each
           allocation grows quadratically, we maintain a cache of recently
           freed extents. There are two possible results of a cache probe:

           1. There are m <= n extents with free pages (n=size of cache)
           and we know which m they are.

           2. There are m > n extents with free pages and we can identify
           only some of them. If we exhaust the list we must search the
           store again to identify the unknown extents.

         */
        bool must_search = false;

        autoerase_t from_ext_cache(v, stid.store);

        // NOTE: I notice that when free_ext_cache (now if volume layer), the
        // page numbers returned seem to be skipping extents.  This leaves
        // fragmented files, but cuts down on contention when many threads are
        // creating a database in parallel.

        // This  was the code before shore-mt:
        // must_search = true;
        // Shore-mt replaced it with the above caching of last
        // extent in store:
        //
        int free_count = v->free_ext_cache().count(stid.store);
        if(free_count > 0) 
        {
            vol_t::ext_cache_t::cache_iterator lo = 
                v->free_ext_cache().lower_bound(stid.store);

            int known;  // counts # of extents tried from those in the cache

            for(known=0; 
                Pcount < npages && lo != v->free_ext_cache().end() 
                && lo->snum == stid.store; 
                known++) 
            {

                int remaining_in_ext=0;
                int allocated=0;

                // Don't trust the cache.
                {
                    extnum_t E = lo->ext;
                    if( ! v->is_alloc_ext_of(E, stid.store) )
                    {
                        // skip this one.
                        continue;
                    }
                }

#if W_DEBUG_LEVEL > 1
                { 
                extnum_t E = lo->ext;
                w_assert1(v->is_alloc_ext_of(E, stid.store));
                DBGTHRD(
                        << " lo->ext " << E
                        << " store " << stid
                        << " free_count " << free_count
                        << " known " << known
                        << " Pcount " << Pcount
                        << " npages " << npages
                        << " lo->ext  " << E);
                }
#endif

                W_DO(v->alloc_pages_in_ext(
                                          filter,
                                          !search_file,
                                          lo->ext,
                                          eff,
                                          stid.store,
                                          npages-Pcount,
                                          pids+Pcount,
                                          allocated,
                                          remaining_in_ext,
                                          is_last_ext_in_store,
                                          may_realloc,
                                          desired_lock_mode));
                W_IFDEBUG1(if(allocated==1) w_assert1(filter->accepted());)
                filter->check();

                // Note: might not have allocated the page: if it
                // would have had to wait for the IX lock on the 
                // extent id, it returns allocated=0,
                // remaining_in_ext = 1, and
                // is_last_ext_in_store = 0.
                 
#if W_DEBUG_LEVEL > 2
                { 
                extnum_t E = lo->ext;
                w_assert1(v->is_alloc_ext_of(E, stid.store));
                DBGTHRD(
                        << " lo->ext " << E
                        << " store " << stid
                        << " allocated " << allocated
                        << " remaining_in_ext " << remaining_in_ext
                        << " is_last_ext_in_store " << is_last_ext_in_store
                        << " may_realloc " << may_realloc
                        << " desired_lock_mode " << desired_lock_mode
                       );
                }
#endif
                if(allocated > 0) {
                    ADD_TSTAT(vol_resv_cache_hit, allocated);
                } else {
                    INC_TSTAT(vol_resv_cache_fail);
                }
                Pcount += allocated;
                nresvd -= allocated;

                if(remaining_in_ext == 0) {
                    // Would like to erase this guy : do it when
                    // we leave scope because we cannot erase
                    // while iterating..
                    from_ext_cache.add(lo->ext);
                    lo++;
                    if(free_count > 0) {
                        // we removed one from the store's
                        // extents in the cach.
                        free_count--;
                        known--;
                    }
                }
                else {
                    // next!
                    ++lo;
                }
                
            }
            
            if(known != free_count) {
                // We didn't allocate all those found in the cache.
                must_search = true;
            }
        }

        if(must_search) 
        {
            INC_TSTAT(io_m_linear_searches);
          /*
             * Allocate from existing extents in the store
             * as long as we reasonably expect to find some
             * free pages there.
             *
             * nresvd is a hint now -- in theory,
             * other txs could be working on this store, but
             * since we've got the volume mutex, no other tx
             * should be able to update it until we're done
             */
                
            extnum_t starting_point = extent;
            DBGTHRD(<<"starting point=" << extent
                    << " nresvd=" << nresvd
            );

#if W_DEBUG_LEVEL > 4
            DBGTHRD( << " must_search " << must_search
                << " nresvd " << nresvd
                << " Pcount " << Pcount
                << " npages " << npages);
#endif

            while ( (nresvd>0) && (Pcount < npages) && (extent != 0) )  {
                INC_TSTAT(io_m_linear_search_extents);

                /*
                // this part of the algorithm is linear in the # of extents 
                // in the store, until nresvd hits 0
                // You can see how many times we went through here
                // by looking at the page_alloc_in_ext stat 
                */

                int  remaining_in_ext=0;
                int  allocated=0;

                is_last_ext_in_store = false;

                W_DO(v->alloc_pages_in_ext(
                            filter,
                            !search_file, // if !search_file, is append_only
                            extent, 
                            eff, // fill factor
                            stid.store,
                            npages - Pcount, 
                            pids + Pcount, 
                            allocated, // returns how many allocated
                            remaining_in_ext,  // return
                            is_last_ext_in_store,  // return
                            may_realloc,
                            desired_lock_mode));

                W_IFDEBUG1(if(allocated==1) w_assert1(filter->accepted());)
                filter->check();
                // Note: might not have allocated the page: if it
                // would have had to wait for the IX lock on the 
                // extent id, it returns allocated=0.
                // In that case, the remaining_in_ext is 1 and
                // is_last_ext_in_store is 0.  The result here
                // is that we will try another extent.

                if(is_last_ext_in_store) {
                    last_ext_in_store = extent;
                }

                DBGTHRD(<<" ALLOCATED " << allocated
                        <<" requested=" << npages-Pcount
                        << " pages in " << extent
                        << " remaining: " << remaining_in_ext
                        << " nresvd= " << nresvd
                        << " is_last_ext_in_store " << stid.store
                            << " = " << is_last_ext_in_store
                        );

                /* 
                 * update hint about whether it's worth trying 
                 * more extents in this store:
                 */
                Pcount += allocated;
                nresvd -= allocated; 

#if W_DEBUG_LEVEL > 4
                // These assertions aren't right, because nresvd
                // is only a hint; could be too small. After all,
                // we figure if it's not hopeless, nresvd is 1 above,
                // but it could have been 8!
                // w_assert9(remaining_in_ext <= nresvd);
                if(remaining_in_ext > nresvd) {
                    nresvd = remaining_in_ext;
                }
                w_assert9(nresvd >= 0);
                w_assert9(remaining_in_ext <= nresvd);
#endif 

                if(Pcount < npages) { 
                   // Need to try more extents
                   extnum_t try_next=0;

                   // is_last_ext_in_store is 
                   // reliable now; it refers to "extent",
                   // not to "last_ext_in_store", which is
                   // 0 or is correct.

                   if(search_file) {
                       if(is_last_ext_in_store) {
                           W_DO(v->first_ext(stid.store, try_next));
                       } else {
                           W_DO(v->next_ext(extent, try_next));
                       }
                    } else {
                       // Break out of loop
                       nresvd = 0;
                    }
                    DBGTHRD(
                        << " try_next " << try_next
                        << " starting_point " << starting_point
                        << " extent " << extent
                        << " nresvd " << nresvd
                       );

                   // Have we come full-circle?
                   if(try_next == starting_point) {
                        // If our hints are right, we should never
                        // get here, or we should get here exactly 
                        // when nresvd goes to 0 
                        // NB: That's a big "if" because we don't
                        // init it to the correct amount when we mount
                        // the volume or open the (existing) store.
                       // Break out of loop
                       nresvd = 0;
                   } else {
                        extent = try_next;
                        INC_TSTAT(extent_lsearch);
                   }
                } /* if (Pcount < npages) */
            } /* while (nresvd > 0 && Pcount < npages) */
        }
    }


    /*
     * OK, we've allocated all we can expect to allocate
     * from existing pages, or we weren't allowed to use
     * existing pages before the given near_p b/c we're appending.
     * If we still need more,
     * we'll allocate new extents to the store
     */
    
    if (Pcount < npages)  
    {
        /*
         *  Allocate new extents & pages in them
         */
        int Xneeded = 0;
        {   /* Step 1:
             *  Calculate #extents Xneeded 
             */
#if W_DEBUG_LEVEL > 2
            int _ext_sz = v->ext_size() * eff / 100;
            w_assert3(_ext_sz > 0);
#endif
            Xneeded = (npages - Pcount - 1) / ext_sz + 1;
        } /* step 1 */

#if W_DEBUG_LEVEL > 4
        DBGTHRD( << " Xneeded " << Xneeded
                    << " npages " << npages
                    << " Pcount " << Pcount);
                    
#endif

        /* Step 2:
         * create an auto-deleted list of extent nums
         * for the volume layer to fill in when it allocates
         */
        extnum_t* extentlist = 0;
        extentlist = new extnum_t[Xneeded]; // auto-del
        if (! extentlist)  W_FATAL(eOUTOFMEMORY);
        w_auto_delete_array_t<extnum_t> autodel(extentlist);

        {   /* Step 3:
             * Locate some free extents
             */
            int numfound=0;

            //Don't bother giving hint about where to start looking--
            //let the volume layer deal with that, because we have
            //no clue
            rc_t rc = v->find_free_exts(Xneeded, extentlist, numfound, 0);
#if W_DEBUG_LEVEL > 2
            if (shpid_t(numfound * v->ext_size()) < shpid_t(npages - Pcount))  {
                //NB: shouldn't get here unless we got an OUTOFSPACE
                // error from the volume layer
                w_assert3(rc.is_error() && (rc.err_num() == eOUTOFSPACE));
                rc.reset();
            }
#endif 
            if (rc.is_error()) {
                /*
                 * If the fill factor is < 100 and we ran
                 * out of space, we need to do something 
                 * else -- go back and revisit the extents
                 * already allocated, but that's not supported
                 * But then, neither is any fill factor other
                 * than 100.
                 */
                if(rc.err_num() == eOUTOFSPACE) {
                    if((eff < 100) ) {
                        W_FATAL(fcNOTIMPLEMENTED);
                    }
                    const char *visible= 
                        "*************************************************";
                    fprintf(stderr, 
                    "\n\n %s\n%s at line %d file %s\n", visible,
                    "  Ran out of space on disk",
                    __LINE__,  __FILE__ );
                    fprintf(stderr, "%s\n",visible);
                    iostophere();

                    return rc;
                } else  {
                    // ???
                    W_FATAL(rc.err_num());
                }
            }

            w_assert1(numfound == Xneeded);

        } /* step 3 -- locate free extents */

        /* 
         * vol_m::alloc_pages_in_ext acquires a long-duration IX lock 
         * on the extent;
         * (this prevents the extent from being deallocated), and it
         * returns w/o allocating the pages if the IX lock cannot 
         * be acquired immediately.
         */


        {   /* Step 4:
             * Allocate the free extents we found.
             * In order to allocate them, we need to know the
             * extnum_t of the last extent in the store.
             * If last_ext_in_store is non-zero, it's credible.
             */
#undef USE_EMPTY_EXT_FOUND
#ifdef USE_EMPTY_EXT_FOUND
            int starting_ext = 0;
            int needed_exts  = Xneeded; // if we use one of the
            // existing extents, we will reduce the count by one; hang
            // onto the original amount
#endif

            if( last_ext_in_store==0 ) 
            {
                // Get the last extent.  If it doesn't
                // contain any allocated pages, we'll use the
                // whole thing.
                //

                // originally choked because, sans Bool& argument to last_page,
                // last_page looks for last allocated page, not last
                // page in last extent in store
                // W_DO(v->last_page(stid.store, near_pid));
                bool is_empty(true);
                W_DO(v->last_extent(stid.store, last_ext_in_store, &is_empty));


                w_assert1(last_ext_in_store != 0);
                w_assert3(v->is_alloc_ext_of(last_ext_in_store, stid.store));
#ifdef USE_EMPTY_EXT_FOUND
                if(is_empty) 
                {
                    // last extent is allocated to the store, but
                    // has no allocated pages.  What we are
                    // going to do here is to cut back by 1 the
                    // number of new extents allocated, and use
                    // this empty extent that we found here,
                    // keeping the order as it would have been:
                    // first alloc pages in this empty extent, then
                    // in the new extents.
                    needed_exts-- ;
                    for(int kk=needed_exts; kk > 0; kk--) 
                    {
                        // move the allocated extents back to
                        // make room for this one,  reserving
                        // the order
                        extentlist[kk] = extentlist[kk-1];
                    }
                    extentlist[0] = last_ext_in_store;
                    starting_ext = 1;
                }
#endif /* USE_EMPTY_EXT_FOUND */
            } // last_ext_in_store is non-zero

#ifdef USE_EMPTY_EXT_FOUND
            if(needed_exts>0)
            {
                // allocate all the extents needed (might be skipping
                // the first one in the list, if it were already
                // allocated to the store.
                W_DO(v->alloc_exts(stid.store,last_ext_in_store, needed_exts,
                        &extentlist[starting_ext]));
#               if W_DEBUG_LEVEL > 2
                for(int kk = starting_ext; kk< needed_exts; kk++) {
                    w_assert3(v->is_alloc_ext_of(extentlist[kk], stid.store));
                }
#               endif
            } 
#else /* NOT USE_EMPTY_EXT_FOUND */
            // original code
            W_DO(v->alloc_exts(stid.store,last_ext_in_store,Xneeded,extentlist));

#if W_DEBUG_LEVEL > 2
            for(int kk = 0; kk< Xneeded; kk++) {
                w_assert3(v->is_alloc_ext_of(extentlist[kk], stid.store));
            }
#endif 

#endif /* NOT USE_EMPTY_EXT_FOUND */

#ifdef USE_EMPTY_EXT_FOUND
            // NOTE: computed nresvd is based on the original Xneeded;
            // if Xneeded != needed_exts, the difference is exactly 1
            // and we are using the entire last extent (which is
            // empty) of the store.
#endif
            nresvd = (Xneeded * v->ext_size());

        } /* step 4 -- allocate the free extents  */

        {   /* Step 5:
             *  Allocate pages in the new extents
             */
            int remaining_in_ext=0;
            int i=0; // number of extents in which to allocate pages
            for (i = 0; i < Xneeded && Pcount < npages; i++)  
            {
                w_assert1(v->is_alloc_ext_of(extentlist[i], stid.store));

                if (i == Xneeded - 1) eff = 100;

                int Pallocated =0;
                W_COERCE(
                    v->alloc_pages_in_ext(
                        filter,
                        !search_file, // if !search_file, is append_only
                        extentlist[i], eff,
                        stid.store, npages - Pcount,
                        pids + Pcount, Pallocated,
                        remaining_in_ext,
                        is_last_ext_in_store, 
                        may_realloc, desired_lock_mode
                        ));
                W_IFDEBUG1(if(Pallocated==1) w_assert1(filter->accepted());)
                filter->check();

                // Note: might not have allocated the page: if it
                // would have to wait for the IX lock on the 
                // extent id, it returns Pallocated=0,
                // remaining_in_ext = 1, and is_last_ext_in_store=0

                w_assert1(v->is_alloc_ext_of(extentlist[i], stid.store));


                DBGTHRD(<<"ALLOCATED " << Pallocated
                    << " pages in " << extentlist[i]
                    << " remaining: " << remaining_in_ext
                    );
                    
                Pcount += Pallocated;

                // for caching info about extents: 
                nresvd -= Pallocated;
                extent = extentlist[i];

                // Added 'Pallocated>0 &&' so we know that the 
                // value remaining_in_ext isn't "arbitrary" - set
                // when we couldn't get the IX lock on the extent in
                // vol_t::alloc_pages_in_ext
                if(Pallocated>0 && remaining_in_ext) {
                    v->free_ext_cache().insert(stid.store, extentlist[i]);
                }

            }  // for loop

            // vol_t::alloc_pages_in_ext
            // returns (arbitrary value) 1 if it doesn't get the
            // IX lock on the extent. But in that ase,
            // we would keep looking, so I think we can safely count
            // on this :
            w_assert2(is_last_ext_in_store);

            if(Pcount != npages || i != Xneeded) {
                fprintf(stderr, "Pcount %d != npages %d; %d !- Xneeded %d",
                        Pcount, npages, i, Xneeded); 
                w_assert1(Pcount == npages);
                w_assert1(i == Xneeded);
            }
            // We can't be sure of this, since remaining_in_ext
            // is set to something arbitrary when  alloc_pages_in_ext
            // couldn't get the IX lock on the extent.
            // w_assert2(nresvd == remaining_in_ext);
        } /* step 5  -- allocate pages in the new extents */
    } /* if Pcount < npages */

// done:

    w_assert1(Pcount == npages);
    DBGTHRD( << "allocated " << npages);
    ADD_TSTAT(page_alloc_cnt, Pcount);

    W_IFDEBUG1(if(npages > 0) w_assert1(filter->accepted());)

    return RCOK;
}


/*********************************************************************
 *
 *  io_m::free_page(pid)
 *
 *  Free the page "pid".
 *
 *********************************************************************/
rc_t
io_m::free_page(const lpid_t& pid, bool check_store_membership)
{
    FUNC(io_m::free_page);
    auto_leave_t enter;

    vid_t volid = pid.vol();
    GRAB_W;
    w_assert3(v->vid() == pid.vol());

    return _free_page(pid, v, check_store_membership);
}

rc_t
io_m::_free_page(const lpid_t& pid, vol_t *v, bool check_store_membership)
{
    // caller must already have grabbed the rw lock via GRAB_W

    /*
     *  NOTE: do not discard the page in the buffer
     *  Caller has EX-latched this page but we do not have a handle
     *  on it here, and we do not need it.
     *  In forward processing, the caller also has an X lock on the
     *  page. 
     *  In undoing of an alloc-page, the caller has an EX lock on the
     *  page if it's undoing an alloc_file_page_log,
     *  OR, if we have called this b/c we couldn't insert the
     *  alloc_file_page_log, an IX lock.  No need to upgrade the lock
     *  b/c the page is EX-latched by the caller.
     *
     *  Can another xct line up behind us to try to allocate
     *  a record on this page and be waiting to grab the latch?  If
     *  so, does it accurately check that the page is still in the
     *  file? latch_lock_get_slot should do all this properly.
     *
     *  If the page never got formatted, you might think it would be
     *  hard to determine if it's ok, but latch_lock_get_slot checks
     *  the extent for the page's allocation status and THAT should
     *  show that it's deallocated by the time the caller frees the EX latch 
     *  on the page.
     */
    DBGTHRD(<<"freeing page " << pid );
    // volume layer has to acquire EX lock on the page and
    // IX lock on the extent to free the page. Both are long locks
    // and can result in a lock timeout.
    W_DO( v->free_page(pid, check_store_membership) );

    DBGTHRD( << "freed pid: " << pid );

    INC_TSTAT(page_dealloc_cnt);

    extnum_t ext = v->pid2ext(pid);
    v->free_ext_cache().insert(pid.stid().store, ext);

    w_assert9(v->vid() == pid.vol());

    return RCOK;
}



/*********************************************************************
 *
 *  io_m::_is_valid_page_of(pid,storenum)
 *  Used by histograms
 *
 *  Return true if page "pid" is valid. False otherwise.
 *  Valid means:  its page number is valid for the volume, and
 *  the page is allocated to the given store, according to the
 *  store's extent numbers
 *
 *********************************************************************/

bool 
io_m::is_valid_page_of(const lpid_t& pid, snum_t s)
{
    FUNC(io_m::is_valid_page_of);
    auto_leave_t enter;
    vid_t volid = pid._stid.vol;

    // essentially GRAB_R but doesn't return RC
    lock_state rme_node ;
    vol_t *v = _find_and_grab(volid, &rme_node, false); 
    if (!v)  return false;
    auto_release_r_t<VolumeLock> release_on_return(v->vol_mutex());

    bool result =  v->is_valid_page_of(pid, s);

    return result;

}


/*********************************************************************
 *
 *  io_m::_set_store_flags(stid, flags)
 *
 *  Set the store flag for "stid" to "flags".
 *
 *********************************************************************/
rc_t
io_m::_set_store_flags(const stid_t& stid, store_flag_t flags, bool sync_volume)
{
    FUNC(io_m::_set_store_flags);
    vid_t volid = stid.vol;
    GRAB_W;

    W_DO( v->set_store_flags(stid.store, flags, sync_volume) );
    if ((flags & st_insert_file) && !smlevel_0::in_recovery())  {
        xct()->AddLoadStore(stid);
    }
    return RCOK;
}


/*********************************************************************
 *
 *  io_m::_get_store_flags(stid, flags)
 *
 *  Get the store flag for "stid" in "flags".
 *
 *********************************************************************/
rc_t
io_m::get_store_flags(const stid_t& stid, store_flag_t& flags)
{
    // Called from bf- can't use monitor mutex because
    // that could cause us to re-enter the I/O layer
    // (embedded calls)
    //
    // v->get_store_flags grabs the mutex
    int i = _find(stid.vol);
    if (i < 0) return RC(eBADVOL);
    vol_t *v = vol[i];

    if (!v)  W_FATAL(eINTERNAL);
    W_DO( v->get_store_flags(stid.store, flags) );
    return RCOK;
}



/*********************************************************************
 *
 *  io_m::create_store(volid, eff, flags, stid, first_ext)
 *  io_m::_create_store(volid, eff, flags, stid, first_ext)
 *
 *  Create a store on "volid" and return its id in "stid". The store
 *  flag for the store is set to "flags". "First_ext" is a hint to
 *  vol_t to allocate the first extent for the store at "first_ext"
 *  if possible.
 *
 *********************************************************************/
rc_t
io_m::create_store(
    vid_t                       volid, 
    int                         eff, 
    store_flag_t                flags,
    stid_t&                     stid,
    extnum_t                    first_ext,
    extnum_t                    num_exts)
{
    rc_t r; 
    { 
        auto_leave_t enter;
    
        r = _create_store(volid, eff, flags, stid, first_ext, num_exts);
        // replaced io mutex with volume mutex
    }

    if(r.is_error()) return r;

    /* load and insert stores get converted to regular on commit */
    if (flags & st_load_file || flags & st_insert_file)  {
        xct()->AddLoadStore(stid);
    }

    return r;
}

rc_t
io_m::_create_store(
    vid_t                       volid, 
    int                         eff, 
    store_flag_t                flags,
    stid_t&                     stid,
    extnum_t                    first_ext,
    extnum_t                    num_extents)
{
    FUNC(io_m::create_store);
    w_assert1(flags);

    GRAB_W;

    /*
     *  Find a free store slot
     */
    stid.vol = volid;
    W_DO(v->find_free_store(stid.store));

    rc_t        rc;

    extnum_t *ext =  0;
    if(num_extents>0) {
        ext = new extnum_t[num_extents]; // auto-del
        w_assert1(ext);
    }

    w_auto_delete_array_t<extnum_t> autodel(ext);

    if(num_extents>0) {
        /*
         *  Find a free extent 
         */
        int found;
        W_DO(v->find_free_exts(num_extents, ext, found, first_ext));
        w_assert2(found == int(num_extents));
        
        if (v->ext_size() * eff / 100 == 0)
            eff = 99 / v->ext_size() + 1;

    }
    
    /*
     * load files are really temp files until commit
     *
     * really want to say 'flags == st_tmp', but must do this
     * since there are assertions which need to know if
     * the file is a load_file
     */
    if (flags & st_load_file)  {
        flags = (store_flag_t)  (flags | st_tmp);
    }

    /*
     *  Allocate the store
     *  RACE NOTE: it's possible that someone might try to use
     *  the store at this point (by happenstance, having what is
     *  now a legit store-id), and we haven't yet acquired a lock
     *  on it.
     */
    DBG(<<"about to alloc_store " << stid.store);
    W_DO( v->alloc_store(stid.store, eff, flags) );

    if(num_extents> 0) {
        W_DO( v->alloc_exts(stid.store, 0/*prev*/, 
                num_extents, ext) );
        w_assert3(v->is_alloc_ext_of(ext[0], stid.store));
    }

    return rc;
}



/*********************************************************************
 * 
 *  io_m::destroy_store(stid, acquire_lock)
 *  io_m::_destroy_store(stid, acquire_lock)
 *
 *  Destroy the store "stid".  This routine now just marks the store
 *  for destruction.  The actual destruction is done at xct completion
 *  by io_m::free_store_after_xct below.
 *
 *  Acquire_lock defaults to true and set to false only by
 *  destroy_temps to destroy stores which might have a share lock on
 *  them by a prepared xct.
 *
 *********************************************************************/
rc_t
io_m::destroy_store(const stid_t& stid, bool acquire_lock) 
{
    FUNC(io_m::destroy_store);
    auto_leave_t enter;
    vid_t volid = stid.vol;

    GRAB_W;

    if (!v->is_valid_store(stid.store)) {
        DBG(<<"destroy_store: BADSTID");
        return RC(eBADSTID);
    }
    W_DO( v->free_store(stid.store, acquire_lock) );
    v->free_ext_cache().erase_all(stid.store); 
    store_latches.destroy_latches(stid); 

    return RCOK;
}



/*********************************************************************
 *
 *  io_m::free_store_after_xct(stid)
 *
 *  free the store.  only called after a xct has completed or during
 *  recovery.
 *
 *********************************************************************/
rc_t
io_m::free_store_after_xct(const stid_t& stid)
{
    auto_leave_t enter;
    vid_t volid = stid.vol;

    GRAB_W;

    W_DO( v->free_store_after_xct(stid.store) );

    return RCOK;
}


/*********************************************************************
 *
 *  io_m::free_ext_after_xct(extid)
 *
 *  free the ext.  only called after a xct has completed.
 *
 *********************************************************************/
rc_t
io_m::free_ext_after_xct(const extid_t& extid)
{
    auto_leave_t enter;
    vid_t volid = extid.vol;

    GRAB_W;

    snum_t owner=0;
    W_DO( v->free_ext_after_xct(extid.ext, owner) );

    DBG(<<"_free_ext_after_xct " << extid );

    // remove from store's free space cache?
    stid_t stid(volid, owner);

    v->free_ext_cache().erase(stid.store, extid.ext);

    return RCOK;
}


/*********************************************************************
 *
 *  io_m::_is_valid_store(stid)
 *
 *  Return true if store "stid" is valid. False otherwise.
 *
 *********************************************************************/
bool
io_m::is_valid_store(const stid_t& stid)
{
    auto_leave_t enter;
    vid_t volid = stid.vol;

    // essentially GRAB_R but doesn't return RC
    lock_state rme_node ;
    vol_t *v = _find_and_grab(volid, &rme_node, false); 
    if (!v)  return false;
    auto_release_r_t<VolumeLock> release_on_return(v->vol_mutex());

    if ( ! v->is_valid_store(stid.store) )   {
        return false;
    }
    
    return v->is_alloc_store(stid.store);
}


/*********************************************************************
 *
 *  io_m::max_store_id_in_use(vid, snum)
 *
 *  Return in snum the maximum store id which is in use on volume vid.
 *
 *********************************************************************/
rc_t
io_m::max_store_id_in_use(vid_t volid, snum_t& snum) 
{
    auto_leave_t enter;
    FUNC(io_m::_max_store_id_in_use);
    GRAB_R;

    snum =  v->max_store_id_in_use();
    return RCOK;
}


/*********************************************************************
 *
 *  io_m::get_volume_meta_stats(vid, volume_stats)
 *
 *  Returns in volume_stats the statistics calculated from the volumes
 *  meta information.
 *
 *********************************************************************/
rc_t
io_m::get_volume_meta_stats(vid_t volid, SmVolumeMetaStats& volume_stats)
{
    auto_leave_t enter;
    FUNC(io_m::_get_volume_meta_stats);
    GRAB_R;

    W_DO( v->get_volume_meta_stats(volume_stats) );
    return RCOK;
}


/*********************************************************************
 *
 *  io_m::get_file_meta_stats(vid, num_files, file_stats)
 *
 *  Returns the pages usage statistics from the stores listed in
 *  the file_stats structure on volume vid.  This routine traverses
 *  the extent list of each file, it can also have random seek
 *  behavior while traversing these..
 *
 *********************************************************************/
rc_t
io_m::get_file_meta_stats(vid_t volid, uint4_t num_files, SmFileMetaStats* file_stats)
{
    auto_leave_t enter;
    GRAB_R;
    W_DO( v->get_file_meta_stats(num_files, file_stats) );
    return RCOK;
}


/*********************************************************************
 *
 *  io_m::get_file_meta_stats_batch(vid, max_store, mapping)
 *
 *  Returns the pages usage statistics from the stores which have
 *  a non-null in the mapping structure indexed by store number
 *  on volume vid.  This routine makes one pass in order of each
 *  extlink page.
 *
 *********************************************************************/
rc_t
io_m::get_file_meta_stats_batch(vid_t volid, uint4_t max_store, SmStoreMetaStats** mapping)
{
    auto_leave_t enter;
    GRAB_R;

    W_DO( v->get_file_meta_stats_batch(max_store, mapping) );
    return RCOK;
}

/*********************************************************************
 *
 *  io_m::get_store_meta_stats_batch(stid_t, stats)
 *
 *  Returns the pages usage statistics for the given store.
 *
 *********************************************************************/
rc_t
io_m::get_store_meta_stats(stid_t stid, SmStoreMetaStats& mapping)
{
    vid_t volid = stid.vol;
    GRAB_R;
    W_DO( v->get_store_meta_stats(stid.store, mapping) );
    return RCOK;
}


/*********************************************************************
 *
 *  io_m::_first_page(stid, pid, allocated, lock)
 *
 *  Find the first page of store "stid" and return it in "pid".
 *  If "allocated" is NULL, narrow search to allocated pages only.
 *  Otherwise, return the allocation status of the page in "allocated".
 *
 *********************************************************************/
rc_t
io_m::first_page(
    const stid_t&        stid, 
    lpid_t&              pid, 
    bool*                allocated,
    lock_mode_t          lock)
{
    auto_leave_t enter;
    FUNC(io_m::_first_page);
    vid_t volid = stid.vol;
    GRAB_R;

    W_DO( v->first_page(stid.store, pid, allocated) );

    if (lock != NL) {
        // Can't block on lock while holding mutex
        W_DO(lock_force(pid, lock, t_long, WAIT_IMMEDIATE));
    }

    return RCOK;
}



/*********************************************************************
 *
 *  io_m::_last_page(stid, pid, allocated, lock)
 *
 *  Find the last page of store "stid" and return it in "pid".
 *  If "allocated" is NULL, narrow search to allocated pages only.
 *  Otherwise, return the allocation status of the page in "allocated".
 *
 *********************************************************************/
rc_t
io_m::last_page(
    const stid_t&        stid, 
    lpid_t&              pid, 
    bool*                allocated,
    lock_mode_t          desired_lock_mode
    )
{
    auto_leave_t enter;
    FUNC(io_m::_last_page);
    vid_t volid=stid.vol;
    GRAB_R;
    
    if(allocated) {
        W_DO( v->last_allocated_page(stid.store, pid) );
        *allocated = true;
    }
    else {
        bool dontreallycare;
        W_DO( v->last_reserved_page(stid.store, pid, dontreallycare) );
    }

    if (desired_lock_mode != NL) {
        // Can't block on lock while holding mutex
        W_DO(lock_force(pid, desired_lock_mode, t_long, WAIT_IMMEDIATE));
    }

    return RCOK;
}


/*********************************************************************
 * 
 *  io_m::_next_page(pid, allocated, lock)
 *
 *  Get the next page of "pid". 
 *  If "allocated" is NULL, narrow search to allocated pages only.
 *  Otherwise, return the allocation status of the page in "allocated".
 *
 *********************************************************************/
rc_t io_m::next_page(
    lpid_t&              pid, 
    bool*                allocated,
    lock_mode_t          lock)
{
    auto_leave_t enter;
    FUNC(io_m::_next_page);
    vid_t volid = pid._stid.vol;
    GRAB_R;

    W_DO( v->next_page(pid, allocated));

    if (lock != NL)
        W_DO(lock_force(pid, lock, t_long, WAIT_IMMEDIATE));

    return RCOK;
}

/*********************************************************************
 * 
 *  io_m::_next_page_with_space(pid, space_bucket_t needed, lock)
 *
 *  Get the next page of "pid" that is in bucket "needed" or higher
 *
 *********************************************************************/
rc_t io_m::next_page_with_space(
    lpid_t&                pid, 
    space_bucket_t         needed,
    lock_mode_t            lock
    )
{
    auto_leave_t enter;
    FUNC(io_m::_next_page_with_space);
    vid_t volid = pid._stid.vol;
    GRAB_R;

    W_DO( v->next_page_with_space(pid, needed));
    DBGTHRD(<<"next page with space is " << pid);

    if (lock != NL) {
        DBGTHRD(<<"locking pid " << pid);
        W_DO(lock_force(pid, lock, t_long, WAIT_IMMEDIATE));
    }

    return RCOK;
}

rc_t                 
io_m::check_store_pages(const stid_t &stid, page_p::tag_t tag)
{
    int i = _find(stid.vol);
    if (i < 0) return RC(eBADVOL);

    vol_t *v = vol[i];
    w_assert9(v->vid() == stid.vol);

    return  v->check_store_pages(stid.store, tag);
}

/*********************************************************************
 *
 *, extnum  io_m::get_du_statistics()         DU DF
 *
 *********************************************************************/
rc_t io_m::get_du_statistics(vid_t volid, volume_hdr_stats_t& _stats, bool audit)
{
    auto_leave_t enter;
    GRAB_R;
    W_DO( v->get_du_statistics(_stats, audit) );

    return RCOK;
}

rc_t
io_m::recover_pages_in_ext(vid_t vid, 
        snum_t snum, extnum_t ext, const Pmap& pmap, bool is_alloc)
{
    auto_leave_t enter;
    return _recover_pages_in_ext(vid, snum, ext, pmap, is_alloc);
}

rc_t
io_m::_recover_pages_in_ext(vid_t volid, 
        snum_t snum, extnum_t ext, const Pmap& pmap, bool is_alloc)
{
    FUNC(io_m::_recover_pages_in_ext);

    GRAB_W;

    w_assert3(v->vid() == volid);
        
    DBGTHRD(<<"recover_pages_in_ext " << ext
        << " map=" << pmap 
        << " is_alloc=" << is_alloc);
    W_COERCE( v->recover_pages_in_ext(snum, ext, pmap, is_alloc) );

    return RCOK;
}

rc_t
io_m::store_operation(vid_t volid, const store_operation_param& param)
{
    FUNC(io_m::store_operation);
    auto_leave_t enter;

    GRAB_W;

    w_assert3(v->vid() == volid);

    W_DO( v->store_operation(param) );

    return RCOK;
}


/*
 * ONLY called during crash recovery, so it doesn't
 * have to grab the mutex
 */
rc_t
io_m::free_exts_on_same_page(const stid_t& stid, extnum_t ext, extnum_t count)
{
    w_assert1(smlevel_0::in_recovery_redo());

    int i = _find(stid.vol);
    if (i < 0) return RC(eBADVOL);

    vol_t *v = vol[i];
    w_assert9(v->vid() == stid.vol);

    W_DO( v->free_exts_on_same_page(ext, stid.store, count) );

    return RCOK;
}

/*
 * ONLY called during crash recovery, so it doesn't
 * have to grab the mutex
 */
rc_t
io_m::set_ext_next(vid_t vid, extnum_t ext, extnum_t new_next)
{
    w_assert9(smlevel_0::in_recovery());

    int i = _find(vid);
    if (i < 0) return RC(eBADVOL);

    vol_t *v = vol[i];
    w_assert9(v->vid() == vid);

    W_DO( v->set_ext_next(ext, new_next) );

    return RCOK;
}


/*
 * for each mounted volume search free the stores which have the deleting
 * attribute set to the typeToRecover.
 * ONLY called during crash recovery, so it doesn't
 * have to grab the mutex
 */
rc_t
io_m::free_stores_during_recovery(store_deleting_t typeToRecover)
{
    w_assert9(smlevel_0::in_recovery());

    for (int i = 0; i < max_vols; i++)  {
        if (vol[i])  {
            W_DO( vol[i]->free_stores_during_recovery(typeToRecover) );
        }
    }

    return RCOK;
}

/*
 * ONLY called during crash recovery, so it doesn't
 * have to grab the mutex
 */
rc_t
io_m::free_exts_during_recovery()
{
    w_assert9(smlevel_0::in_recovery());

    for (int i = 0; i < max_vols; i++)  {
        if (vol[i])  {
            W_DO( vol[i]->free_exts_during_recovery() );
        }
    }

    return RCOK;
}

/*
 * ONLY called during crash recovery, so it doesn't
 * have to grab the mutex
 */
rc_t
io_m::create_ext_list_on_same_page(const stid_t& stid, extnum_t prev, extnum_t next, extnum_t count, extnum_t* list)
{
    w_assert1(smlevel_0::in_recovery_redo());

    int i = _find(stid.vol);
    if (i < 0) return RC(eBADVOL);

    vol_t *v = vol[i];
    w_assert9(v->vid() == stid.vol);

    W_DO( v->create_ext_list_on_same_page(stid.store, prev, next, count, list) );

    return RCOK;
}

rc_t                         
io_m::_update_ext_histo(const lpid_t& pid, space_bucket_t b)
{
    // Don't use enter/leave : see comment in sm_io.h
    int i = _find(pid._stid.vol);
    if (i < 0) return RC(eBADVOL);

    vol_t *v = vol[i];
    w_assert9(v->vid() == pid._stid.vol);

    DBGTHRD(<<"updating histo for pid " << pid);
    W_DO( v->update_ext_histo(pid, b) );
    return RCOK;
}

rc_t                         
io_m::_init_store_histo(store_histo_t *h, const stid_t& stid,
        pginfo_t* pages, int& numpages)
{
    // we have a lock on the store_histo_t
    int i = _find(stid.vol);
    if (i < 0) return RC(eBADVOL);

    vol_t *v = vol[i];
    w_assert9(v->vid() == stid.vol);

    W_DO( v->init_histo(h, stid.store, pages, numpages) );
    return RCOK;
}

ostream& operator<<(ostream& o, const store_operation_param& param)
{
    o << "snum="    << param.snum()
      << ", op="    << param.op();
    
    switch (param.op())  {
        case smlevel_0::t_delete_store:
            break;
        case smlevel_0::t_create_store:
            o << ", flags="        << param.new_store_flags()
              << ", eff="        << param.eff();
            break;
        case smlevel_0::t_set_deleting:
            o << ", newValue="        << param.new_deleting_value()
              << ", oldValue="        << param.old_deleting_value();
            break;
        case smlevel_0::t_set_store_flags:
            o << ", newFlags="        << param.new_store_flags()
              << ", oldFlags="        << param.old_store_flags();
            break;
        case smlevel_0::t_set_first_ext:
            o << ", ext="        << param.first_ext();
            break;
    }

    return o;
}

/*
 * WARNING: dump_exts and dump_stores are for debugging and don't acquire the
 * proper mutexes.  use at your own risk.
 */

rc_t
io_m::dump_exts(ostream& o, vid_t vid, extnum_t start, extnum_t end)
{
    int i = _find(vid);
    if (i == -1)  {
        return RC(eBADVOL);
    }

    W_DO( vol[i]->dump_exts(o, start, end) );

    return RCOK;
}

rc_t
io_m::dump_stores(ostream& o, vid_t vid, int start, int end)
{
    int i = _find(vid);
    if (i == -1)  {
        return RC(eBADVOL);
    }

    W_DO( vol[i]->dump_stores(o, start, end) );

    return RCOK;
}
