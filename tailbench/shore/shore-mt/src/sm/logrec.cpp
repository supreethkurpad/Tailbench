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

     $Id: logrec.cpp,v 1.155 2010/06/15 17:30:07 nhall Exp $

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
#define LOGREC_C
#ifdef __GNUG__
#   pragma implementation
#endif
#include "sm_int_2.h"
#include "logdef_gen.cpp"
// include extent.h JUST to get stnode_p::max
#include "extent.h" 
// include histo.h JUST to get histoid_t::destroyed_store declaration
#include "histo.h"

#include "btree_p.h"
#include "rtree_p.h"

#include <iomanip>
typedef        ios::fmtflags        ios_fmtflags;

#include <new>

enum {         eINTERNAL = smlevel_0::eINTERNAL,
        eOUTOFMEMORY = smlevel_0::eOUTOFMEMORY
        };

/*********************************************************************
 *
 *  logrec_t::cat_str()
 *
 *  Return a string describing the category of the log record.
 *
 *********************************************************************/
const char*
logrec_t::cat_str() const
{
    switch (cat())  {
    case t_logical:
        return "l---";

    case t_logical | t_cpsn:
        return "l--c";

    case t_status:
        return "s---";

    case t_undo:
        return "--u-";

    case t_redo:
        return "-r--";

    case t_redo | t_cpsn:
        return "-r-c";

    case t_undo | t_redo:
        return "-ru-";

    case t_undo | t_redo | t_logical:
        return "lru-";

    case t_redo | t_logical | t_cpsn:
        return "lr_c";

    case t_redo | t_logical : // used in I/O layer 
        return "lr__";

    case t_undo | t_logical | t_cpsn:
        return "l_uc";

    case t_undo | t_logical : 
        return "l-u-";

#if W_DEBUG_LEVEL > 0
    case t_bad_cat:
        // for debugging only
        return "BAD-";
#endif 
    default:
      return 0;
    }
#if W_DEBUG_LEVEL > 2
    cerr << "unexpected log record flags: ";
        if( _cat & t_undo ) cerr << "t_undo ";
        if( _cat & t_redo ) cerr << "t_redo ";
        if( _cat & t_logical ) cerr << "t_logical ";
        if( _cat & t_cpsn ) cerr << "t_cpsn ";
        if( _cat & t_status ) cerr << "t_status ";
        cerr << endl;
#endif 
        
    W_FATAL(smlevel_0::eINTERNAL);
    return "????";
}


/*********************************************************************
 *
 *  logrec_t::type_str()
 *
 *  Return a string describing the type of the log record.
 *
 *********************************************************************/
const char* 
logrec_t::type_str() const
{
    switch (_type)  {
#        include "logstr_gen.cpp"
    default:
      return 0;
    }

    /*
     *  Not reached.
     */
    W_FATAL(smlevel_0::eINTERNAL);
    return 0;
}




/*********************************************************************
 *
 *  logrec_t::fill(pid, len)
 *
 *  Fill the "pid" and "length" field of the log record.
 *
 *********************************************************************/
void
logrec_t::fill(const lpid_t* p, uint2_t tag, smsize_t l)
{
    w_assert9(hdr_sz == _data - (char*) this);
    w_assert9(w_base_t::is_aligned(_data));

    /* adjust _cat */
    xct_t *x = xct();
    if(smlevel_0::in_recovery_undo() ||
        (x && x->state() == smlevel_1::xct_aborting)) 
    {
        _cat |= t_rollback;
    }
    set_pid(lpid_t::null);
    _prev = lsn_t::null;
    _page_tag = tag;
    if (p) set_pid(*p);
    if (l != align(l)) {
        // zero out extra space to keep purify happy
        memset(_data+l, 0, align(l)-l);
    }
    unsigned int tmp = align(l) + hdr_sz + sizeof(lsn_t);
    tmp = (tmp + 7) & -8; // force 8-byte alignment
    w_assert1(tmp <= max_sz);
    _len = tmp;
    if(type() != t_skip) {
        DBG( << "Creat log rec: " << *this 
                << " size: " << _len << " prevlsn: " << _prev );
    }
}



/*********************************************************************
 *
 *  logrec_t::fill_xct_attr(tid, prev_lsn)
 *
 *  Fill the transaction related fields of the log record.
 *
 *********************************************************************/
void 
logrec_t::fill_xct_attr(const tid_t& tid, const lsn_t& last)
{
    _tid = tid;
    if(_prev.valid()) {
        w_assert2(is_cpsn());
    } else {
        _prev = last;
    }
}

/*
 * Determine whether the log record header looks valid
 */
bool
logrec_t::valid_header(const lsn_t & lsn) const
{
    if (_len < hdr_sz || _type > 100 || cat() == t_bad_cat || 
        lsn != *_lsn_ck()) {
        return false;
    }
    return true;
}


/*********************************************************************
 *
 *  logrec_t::redo(page)
 *
 *  Invoke the redo method of the log record.
 *
 *********************************************************************/
void logrec_t::redo(page_p* page)
{
    FUNC(logrec_t::redo);
    DBG( << "Redo  log rec: " << *this 
        << " size: " << _len << " prevlsn: " << _prev );

    switch (_type)  {
#include "redo_gen.cpp"
    }
    
    /*
     *  Page is dirty after redo.
     *  (not all redone log records have a page)
     *  NB: the page lsn in set by the caller (in restart.cpp)
     *  This is ok in recovery because in this phase, there
     *  is not a bf_cleaner thread running. (that thread asserts
     *  that if the page is dirty, its lsn is non-null, and we
     *  have a short-lived violation of that right here).
     */
    if(page) page->set_dirty();
}



/*********************************************************************
 *
 *  logrec_t::undo(page)
 *
 *  Invoke the undo method of the log record. Automatically tag
 *  a compensation lsn to the last log record generated for the
 *  undo operation.
 *
 *********************************************************************/
void
logrec_t::undo(page_p* page)
{
    FUNC(logrec_t::undo);
    DBG( << "Undo  log rec: " << *this 
        << " size: " << _len  << " prevlsn: " << _prev);
    switch (_type) {
#include "undo_gen.cpp"
    }
    xct()->compensate_undo(_prev);
}

/*********************************************************************
 *
 *  logrec_t::corrupt()
 *
 *  Zero out most of log record to make it look corrupt.
 *  This is for recovery testing.
 *
 *********************************************************************/
void
logrec_t::corrupt()
{
    char* end_of_corruption = ((char*)this)+length();
    char* start_of_corruption = (char*)&_type;
    size_t bytes_to_corrupt = end_of_corruption - start_of_corruption;
    memset(start_of_corruption, 0, bytes_to_corrupt);
}

/*********************************************************************
 *
 *  xct_freeing_space
 *
 *  Status Log to mark the end of transaction and the beginning
 *  of space recovery.
 *  Synchronous for commit. Async for abort.
 *
 *********************************************************************/
xct_freeing_space_log::xct_freeing_space_log()
{
    fill(0, 0, 0);
}


/*********************************************************************
 *
 *  xct_end_log
 *  xct_abort_log
 *
 *  Status Log to mark the end of transaction and space recovery.
 *
 *********************************************************************/
xct_end_log::xct_end_log()
{
    fill(0, 0, 0);
}

// We use a different log record type here only for debugging purposes
xct_abort_log::xct_abort_log()
{
    fill(0, 0, 0);
}



/*********************************************************************
 *
 *  xct_prepare_st_log -- start prepare info for a tx
 *  xct_prepare_lk_log -- add locks to the tx
 *  xct_prepare_fi_log -- fin prepare info for the tx
 *
 *  For 2 phase commit.
 *
 *********************************************************************/

xct_prepare_st_log::xct_prepare_st_log(const gtid_t *g, 
        const server_handle_t &h)
{
    fill(0, 0, (new (_data) prepare_info_t(g, h))->size());

}
void
xct_prepare_st_log::redo(page_p *) 
{
    /*
     *  Update xct state 
     */
    const prepare_info_t*         pt = (prepare_info_t*) _data;
    xct_t *                        xd = xct_t::look_up(_tid);
    w_assert9(xd);
    if(pt->is_external==1) {
        W_COERCE(xd->enter2pc(pt->g));
    }
    xd->set_coordinator(pt->h);
}


xct_prepare_lk_log::xct_prepare_lk_log(int num, 
        lock_base_t::lmode_t mode, lockid_t *locks)
{
    fill(0, 0, (new (_data) prepare_lock_t(num, mode, locks))->size());
}

xct_prepare_alk_log::xct_prepare_alk_log(int num, 
        lockid_t *locks,
        lock_base_t::lmode_t *modes
        )
{
    fill(0, 0, (new (_data) prepare_all_lock_t(num, locks, modes))->size());
}

void
xct_prepare_lk_log::redo(page_p *) 
{
    /*
     *  Acquire all the locks within. (all of same mode)
     *  For lm->lock() to work, we have to 
     *  attach the xct
     */
    const prepare_lock_t*         pt = (prepare_lock_t*) _data;
    xct_t *                        xd = xct_t::look_up(_tid);
    me()->attach_xct(xd);
    W_COERCE(xd->obtain_locks(pt->mode, pt->num_locks, pt->name));
    me()->detach_xct(xd);
}

void
xct_prepare_alk_log::redo(page_p *) 
{
    /*
     *  Acquire all the locks within. (different modes)
     *  For lm->lock() to work, we have to 
     *  attach the xct
     */
    const prepare_all_lock_t*         pt = (prepare_all_lock_t*) _data;
    xct_t *                        xd = xct_t::look_up(_tid);
    me()->attach_xct(xd);
    uint4_t i;
    for (i=0; i<pt->num_locks; i++) {
        W_COERCE(xd->obtain_one_lock(pt->pair[i].mode, pt->pair[i].name));
    }
    me()->detach_xct(xd);
}


xct_prepare_fi_log::xct_prepare_fi_log(int num_ex, int num_ix, int num_six, int numextent, 
        const lsn_t &first) 
{
    fill(0, 0, (new (_data) 
        prepare_lock_totals_t(num_ex, num_ix, num_six, numextent, first))->size());
}
void 
xct_prepare_fi_log::redo(page_p *) 
{
    /*
     *  check that the right number of
     *  locks was acquired, and set the state
     *  -- don't set the state any earlier so that
     *  txs that didn't get the prepare fin logged
     *  are aborted during the undo phase
     */
    xct_t *                        xd = xct_t::look_up(_tid);
    const prepare_lock_totals_t* pt = 
            (prepare_lock_totals_t*) _data;
    me()->attach_xct(xd);
    W_COERCE(xd->check_lock_totals(pt->num_EX, pt->num_IX, pt->num_SIX, pt->num_extents));
    xd->change_state(xct_t::xct_prepared);

    w_assert9( xd->first_lsn() == lsn_t::null );
    xd->set_first_lsn(pt->first_lsn);

    me()->detach_xct(xd);
    w_assert9(xd->state() == xct_t::xct_prepared);
}

xct_prepare_stores_log::xct_prepare_stores_log(int num, const stid_t* stids)
{
    fill(0, 0, (new (_data) prepare_stores_to_free_t(num, stids))->size());
}

void
xct_prepare_stores_log::redo(page_p *)
{
    xct_t*        xd = xct_t::look_up(_tid);
    const prepare_stores_to_free_t* info = (prepare_stores_to_free_t*)_data;
    me()->attach_xct(xd);
    for (uint4_t i = 0; i < info->num; i++)  {
        xd->AddStoreToFree(info->stids[i]);
    }
    me()->detach_xct(xd);
    w_assert9(xd->state() == xct_t::xct_prepared);
}


/*********************************************************************
 *
 *  comment_log
 *
 *  For debugging
 *
 *********************************************************************/
comment_log::comment_log(const char *msg)
{
    w_assert1(strlen(msg) < sizeof(_data));
    memcpy(_data, msg, strlen(msg)+1);
    DBG(<<"comment_log: L: " << (const char *)_data);
    fill(0, 0, strlen(msg)+1);
}

void 
comment_log::redo(page_p * W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    DBG(<<"comment_log: R: " << (const char *)_data);
    ; // just for the purpose of setting breakpoints
}

void 
comment_log::undo(page_p * W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    DBG(<<"comment_log: U: " << (const char *)_data);
    ; // just for the purpose of setting breakpoints
}

/*********************************************************************
 *
 *  compensate_log
 *
 *  Needed when compensation rec is written rather than piggybacked
 *  on another record
 *
 *********************************************************************/
compensate_log::compensate_log(lsn_t rec_lsn)
{
    fill(0, 0, 0);
    set_clr(rec_lsn);
}


/*********************************************************************
 *
 *  skip_log partition
 *
 *  Filler log record -- for skipping to end of log partition
 *
 *********************************************************************/
skip_log::skip_log()
{
    fill(0, 0, 0);
}

/*********************************************************************
 *
 *  chkpt_begin_log
 *
 *  Status Log to mark start of fussy checkpoint.
 *
 *********************************************************************/
chkpt_begin_log::chkpt_begin_log(const lsn_t &lastMountLSN)
{
    new (_data) lsn_t(lastMountLSN);
    fill(0, 0, sizeof(lsn_t));
}




/*********************************************************************
 *
 *  chkpt_end_log(const lsn_t &master, const lsn_t& min_rec_lsn)
 *
 *  Status Log to mark completion of fussy checkpoint.
 *  Master is the lsn of the record that began this chkpt.
 *  min_rec_lsn is the lsn of the record that began this chkpt.
 *
 *********************************************************************/
chkpt_end_log::chkpt_end_log(const lsn_t &lsn, const lsn_t& min_rec_lsn)
{
    // initialize _data
    lsn_t *l = new (_data) lsn_t(lsn);
    l++; //grot
    *l = min_rec_lsn;

    fill(0, 0, 2 * sizeof(lsn_t));
}



/*********************************************************************
 *
 *  chkpt_bf_tab_log
 *
 *  Data Log to save dirty page table at checkpoint.
 *  Contains, for each dirty page, its pid and recovery lsn.
 *
 *********************************************************************/

chkpt_bf_tab_t::chkpt_bf_tab_t(
    int                 cnt,        // I-  # elements in pids[] and rlsns[]
    const lpid_t*         pids,        // I-  id of of dirty pages
    const lsn_t*         rlsns)        // I-  rlsns[i] is recovery lsn of pids[i]
    : count(cnt)
{
    w_assert9( SIZEOF(*this) <= logrec_t::data_sz );
    w_assert1(count <= max);
    for (uint i = 0; i < count; i++) {
        brec[i].pid = pids[i];
        brec[i].rec_lsn = rlsns[i];
    }
}


chkpt_bf_tab_log::chkpt_bf_tab_log(
    int                 cnt,        // I-  # elements in pids[] and rlsns[]
    const lpid_t*         pid,        // I-  id of of dirty pages
    const lsn_t*         rec_lsn)// I-  rlsns[i] is recovery lsn of pids[i]
{
    fill(0, 0, (new (_data) chkpt_bf_tab_t(cnt, pid, rec_lsn))->size());
}




/*********************************************************************
 *
 *  chkpt_xct_tab_log
 *
 *  Data log to save transaction table at checkpoint.
 *  Contains, for each active xct, its id, state, last_lsn
 *  and undo_nxt lsn. 
 *
 *********************************************************************/
chkpt_xct_tab_t::chkpt_xct_tab_t(
    const tid_t&                         _youngest,
    int                                 cnt,
    const tid_t*                         tid,
    const smlevel_1::xct_state_t*         state,
    const lsn_t*                         last_lsn,
    const lsn_t*                         undo_nxt)
    : youngest(_youngest), count(cnt)
{
    w_assert1(count <= max);
    w_assert9( SIZEOF(*this) <= logrec_t::data_sz);
    for (uint i = 0; i < count; i++)  {
        xrec[i].tid = tid[i];
        xrec[i].state = state[i];
        xrec[i].last_lsn = last_lsn[i];
        xrec[i].undo_nxt = undo_nxt[i];
    }
}
    
chkpt_xct_tab_log::chkpt_xct_tab_log(
    const tid_t&                         youngest,
    int                                 cnt,
    const tid_t*                         tid,
    const smlevel_1::xct_state_t*         state,
    const lsn_t*                         last_lsn,
    const lsn_t*                         undo_nxt)
{
    fill(0, 0, (new (_data) chkpt_xct_tab_t(youngest, cnt, tid, state,
                                         last_lsn, undo_nxt))->size());
}




/*********************************************************************
 *
 *  chkpt_dev_tab_log
 *
 *  Data log to save devices mounted at checkpoint.
 *  Contains, for each device mounted, its devname and vid.
 *
 *********************************************************************/
chkpt_dev_tab_t::chkpt_dev_tab_t(
    int                 cnt,
    const char          **dev,
    const vid_t*        vid)
    : count(cnt)
{
    w_assert9( sizeof(*this) <= logrec_t::data_sz );
    w_assert1(count <= max);
    for (uint i = 0; i < count; i++) {
        // zero out everything and then set the string
        memset(devrec[i].dev_name, 0, sizeof(devrec[i].dev_name));
        strncpy(devrec[i].dev_name, dev[i], sizeof(devrec[i].dev_name)-1);
        devrec[i].vid = vid[i];
    }
}

chkpt_dev_tab_log::chkpt_dev_tab_log(
    int                 cnt,
    const char                 **dev,
    const vid_t*         vid)
{
    fill(0, 0, (new (_data) chkpt_dev_tab_t(cnt, dev, vid))->size());
}


/*********************************************************************
 *
 *  mount_vol_log
 *
 *  Data log to save device mounts.
 *
 *********************************************************************/
mount_vol_log::mount_vol_log(
    const char*         dev,
    const vid_t&         vid)
{
    const char                *devArray[1];

    devArray[0] = dev;
    DBG(<< "mount_vol_log dev_name=" << devArray[0] << " volid =" << vid);
    fill(0, 0, (new (_data) chkpt_dev_tab_t(1, devArray, &vid))->size());
}


void mount_vol_log::redo(page_p* W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    chkpt_dev_tab_t* dp = (chkpt_dev_tab_t*) _data;

    w_assert9(dp->count == 1);

    // this may fail since this log record is only redone on crash/restart and the
        // user may have destroyed the volume after using, but then there won't be
        // and pages that need to be updated on this volume.
    W_IGNORE(io_m::mount(dp->devrec[0].dev_name, dp->devrec[0].vid));
}


/*********************************************************************
 *
 *  dismount_vol_log
 *
 *  Data log to save device dismounts.
 *
 *********************************************************************/
dismount_vol_log::dismount_vol_log(
    const char*                dev,
    const vid_t&         vid)
{
    const char                *devArray[1];

    devArray[0] = dev;
    DBG(<< "dismount_vol_log dev_name=" << devArray[0] << " volid =" << vid);
    fill(0, 0, (new (_data) chkpt_dev_tab_t(1, devArray, &vid))->size());
}


void dismount_vol_log::redo(page_p* W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    chkpt_dev_tab_t* dp = (chkpt_dev_tab_t*) _data;

    w_assert9(dp->count == 1);
    // this may fail since this log record is only redone on crash/restart and the
        // user may have destroyed the volume after using, but then there won't be
        // and pages that need to be updated on this volume.
    W_IGNORE(io_m::dismount(dp->devrec[0].vid, true));
}



/*********************************************************************
 *
 *  page_init_t : a struct used by other log records.
 *
 *********************************************************************/
class page_init_t {
private:
    // Might as well make these as small as we can.
    uint2_t        _page_tag;
    uint2_t        _page_flag;
    // NEHFIX3: adds store flags to this structure in place of a fill2
    uint2_t        _store_flag; // store_flag_t  in sm_base.h
    fill2          _fill;
public:
    // page_tag_t in page.h
    page_p::tag_t            page_tag() const  {
                               return page_p::tag_t(_page_tag); }

    // page_flag_t in page.h
    page_p::page_flag_t      page_flag() const  {
                               return page_p::page_flag_t(_page_flag); }

    // store_flag_t in sm_base.h
    smlevel_0::store_flag_t  store_flag() const {
                               return smlevel_0::store_flag_t(_store_flag); }

    page_init_t(uint2_t t, uint2_t pf, uint2_t sf): 
        _page_tag(t), _page_flag(pf), _store_flag(sf) {};
    
    int size()  { return sizeof(*this); }
    void redo_init(page_p *p, const lpid_t& pid);
    // no undo
};

void 
page_init_t::redo_init(page_p* page, const lpid_t& pid)
{
    w_assert2( page->is_fixed());
    // NEHFIX3
    // The fix will have set the store flags to whatever is in the
    // store node, which might be wrong. We trust the info in the
    // log records.
    //
    // page->persistent_part().set_page_storeflags(store_flag());
    page->set_store_flags(store_flag()); // through the page_p, thus through the bfcb_t

    // Sigh. The page format call ignores the sf passed in.
    // However, by having it here, we can tell what was originally
    // done with the page. This can help during debugging.
    W_COERCE( page->_format(pid, 
                page_tag(), 
                page_flag(), 
                store_flag()) );
}


/*********************************************************************
 *
 *  page_link_log
 *
 *  Log a page link up operation.
 *
 *********************************************************************/
struct page_link_t {
    shpid_t old_prev, old_next;
    shpid_t new_prev, new_next;

    page_link_t(shpid_t op, shpid_t on, shpid_t np, shpid_t nn)  
    : old_prev(op), old_next(on), new_prev(np), new_next(nn)   {};

    int size()        { return sizeof(*this); }
};

page_link_log::page_link_log(
    const page_p&         page, 
    shpid_t                 new_prev, 
    shpid_t                 new_next)
{
    w_assert2(page.tag() == page_p::t_btree_p);
    fill(&page.pid(), page.tag(),
         (new (_data) page_link_t(page.prev(), page.next(),
                                  new_prev, new_next))->size());
}

void 
page_link_log::redo(page_p* p)
{
    page_link_t* dp = (page_link_t*) _data;
    w_assert9(p->next() == dp->old_next);
    w_assert9(p->prev() == dp->old_prev);

    W_COERCE(p->link_up(dp->new_prev, dp->new_next));
}

void 
page_link_log::undo(page_p* p)
{
    page_link_t* dp = (page_link_t*) _data;
    w_assert9(p->next() == dp->new_next);
    w_assert9(p->prev() == dp->new_prev);
    W_COERCE(p->link_up(dp->old_prev, dp->old_next));
}



/*********************************************************************
 *
 *  page_insert_log
 *
 *  Log insert of a record into a page.
 *
 *********************************************************************/
class page_insert_t {
public:
    int2_t        idx;
    int2_t        cnt;
    enum { max = ((logrec_t::data_sz - 2*sizeof(uint2_t)) - sizeof(page_init_t)) };

    // array of int2_t len[cnt], char data[]
    char        data[max]; 

    page_insert_t(const page_p& page, int idx, int cnt);
    page_insert_t(int idx, int cnt, const cvec_t* vec);
    cvec_t* unflatten(int cnt, cvec_t vec[]);
    int size();
    void redo(page_p* page);
    void undo(page_p* page);
};

page_insert_t::page_insert_t(const page_p& page, int i, int c)
    : idx(i), cnt(c)
{
    if(cnt) {
        w_assert1(cnt * sizeof(int2_t) < sizeof(data));
        int2_t* lenp = (int2_t*) data;
        int total = 0;
        for (i = 0; i < cnt; total += lenp[i++])  {
            // tuple_size yields smsize_t (for large objects)
            // but the slots on a small page can be at most
            // 65K long, so the slots contain shorts for the lengths.
            lenp[i] = (int2_t) page.tuple_size(idx + i);
        }
        w_assert1(total + cnt * sizeof(int2_t) < sizeof(data));

        char* p = data + sizeof(int2_t) * cnt;
        for (i = 0; i < c; i++)  {
            memcpy(p, page.tuple_addr(idx + i), lenp[i]);
            p += lenp[i];
        }
    }
}


page_insert_t::page_insert_t(int i, int c, const cvec_t* v)
    : idx(i), cnt(c)
{
    if(cnt) {
        w_assert1(cnt * sizeof(int2_t) < sizeof(data));
        int2_t* lenp = (int2_t*) data;
        int total = 0;
        for (i = 0; i < cnt; total += lenp[i++])  {
            lenp[i] = v[i].size();
        }
        w_assert1(total + cnt * sizeof(int2_t) < sizeof(data));

        char* p = data + sizeof(int2_t) * cnt;
        for (i = 0; i < c; i++)  {
            p += v[i].copy_to(p);
        }
        w_assert1((uint)(p - data) == total + cnt * sizeof(int2_t));
    }
}

cvec_t* page_insert_t::unflatten(int W_IFDEBUG9(c), cvec_t vec[])
{
    w_assert9(c == cnt);
    int2_t* lenp = (int2_t*) data;
    char* p = data + sizeof(int2_t) * cnt;
    for (int i = 0; i < cnt; i++)  {
        vec[i].put(p, lenp[i]);
        p += lenp[i];
    }
    return vec;
}

int
page_insert_t::size()
{
    int2_t* lenp = (int2_t*) data;
    char* p = data + sizeof(int2_t) * cnt;
    for (int i = 0; i < cnt; i++) p += lenp[i];
    return p - (char*) this;
}


void 
page_insert_t::redo(page_p* page)
{
    if(cnt) {
        cvec_t* vec = new cvec_t[cnt];        // need improvement
        if (! vec)  W_FATAL(eOUTOFMEMORY);
        w_auto_delete_array_t<cvec_t> auto_del(vec);

        W_COERCE(page->insert_expand(idx, cnt, 
                               unflatten(cnt, vec)) );
    }
}

void 
page_insert_t::undo(page_p* page)
{
     if(cnt) {
#if W_DEBUG_LEVEL > 1
        uint2_t* lenp = (uint2_t*) data;
        char* p = data + sizeof(int2_t) * cnt;
        for (int i = 0; i < cnt; i++)  {
            w_assert2(lenp[i] == page->tuple_size(i + idx));
            w_assert2(memcmp(p, page->tuple_addr(i + idx), lenp[i]) == 0);
            p += lenp[i];
        }
#endif 
        W_COERCE(page->remove_compress(idx, cnt));
    }
}

page_insert_log::page_insert_log(
    const page_p&         page, 
    int                 idx, 
    int                 cnt,
    const cvec_t*         vec)
{
    fill(&page.pid(), page.tag(),
                (new (_data) page_insert_t(idx, cnt, vec))->size());
}



void 
page_insert_log::redo(page_p* page)
{
    page_insert_t* dp = (page_insert_t*) _data;
    dp->redo(page);
}


void 
page_insert_log::undo(page_p* page)
{
    page_insert_t* dp = (page_insert_t*) _data;
    dp->undo(page);
}



/*********************************************************************
 *
 *  page_remove_log
 *
 *  Log removal of a record from page.
 *
 *********************************************************************/
typedef page_insert_t page_remove_t;

page_remove_log::page_remove_log(
    const page_p&         page,
    int                 idx, 
    int                 cnt)
{
    fill(&page.pid(), page.tag(),
                (new (_data) page_remove_t(page, idx, cnt))->size());
}

void page_remove_log::redo(page_p* page)
{
    ((page_insert_log*)this)->undo(page);
}

void page_remove_log::undo(page_p* page)
{
    ((page_insert_log*)this)->redo(page);
}
    


/*********************************************************************
 *
 *  page_mark_log
 *
 *  Log a mark operation on a record in page. The record is
 *  marked as removed.
 *
 * XXX        This may be sized incorrectly -- the data is dependent upon
 *        the page size, not the log record size.   Currently, with 
 *        a page size of 32k, the data area is ~32k larger than 64k,
 *        which is the largest amount 'len' can indicate.   Something
 *        needs to be fixed here.
 *
 *********************************************************************/
class page_mark_t {
public:
    int2_t idx;
    uint2_t len;

    /* XXX this is wrong.   If it is an entry on a page, this should
       be page sized based, not log size based.   The data being
       stuffed in has to fit on the page ... not the 3x page which
       a log record needs.   If it can be larger, then int2_t can't
       be used, since 32k pages-> 17 bits are needed.  */

    enum { max = ((logrec_t::data_sz - 2*sizeof(uint2_t)) - sizeof(page_init_t)) };
    char data[max];

    page_mark_t(int i, smsize_t l, const void* d) : idx(i), len((uint2_t)l) {
        w_assert1(l <= sizeof(data));
        memcpy(data, d, l);
    }
    page_mark_t(int i, const cvec_t& v) : idx(i), len((uint2_t)v.size()) {
        w_assert1(v.size() <= sizeof(data));
        if(len>0) {
            v.copy_to(data);
        }
    }
    int size() {
        return data + len - (char*) this;
    }
    void redo(page_p* page);
    void undo(page_p* page);
};

void
page_mark_t::redo(page_p* page)
{
    if(len>0) {
        if((idx >= 0 && idx < page->nslots())==false) {
            // GNATS 102: get more info
            // If this happens again, get more info.
            // This appears to have been fixed with the new
            // file page allocation code.
            w_assert1(idx >= 0 && idx < page->nslots());
        }
        w_assert1( page->tuple_size(idx) == len);
        w_assert1( memcmp(page->tuple_addr(idx), data, len) == 0);
        W_COERCE( page->mark_free(idx) );
    }
}

void
page_mark_t::undo(page_p* page)
{
    if(len>0) {
        cvec_t v(data, len);
        W_COERCE( page->reclaim(idx, v) );
    }
}

page_mark_log::page_mark_log(const page_p& page, int idx)
{
    fill(&page.pid(), page.tag(),
         (new (_data) page_mark_t(idx, page.tuple_size(idx), 
                                 page.tuple_addr(idx)))->size() );
}

void
page_mark_log::redo(page_p* page)
{
    page_mark_t* dp = (page_mark_t*) _data;
    dp->redo(page);
}

void
page_mark_log::undo(page_p* page)
{
    page_mark_t* dp = (page_mark_t*) _data;
    dp->undo(page);
}



/*********************************************************************
 *
 *  page_reclaim_log
 *
 *  Mark a reclaim operation on a record in page. A marked record
 *  is reclaimed this way.
 *
 *********************************************************************/
typedef page_mark_t page_reclaim_t;

page_reclaim_log::page_reclaim_log(const page_p& page, int idx,
                                   const cvec_t& vec)
{
    fill(&page.pid(), page.tag(),
         (new (_data) page_reclaim_t(idx, vec))->size()) ;
}

void
page_reclaim_log::redo(page_p* page)
{
    page_mark_t* dp = (page_mark_t*) _data;
    // reverse the sense of page_mark:
    dp->undo(page);
}

void
page_reclaim_log::undo(page_p* page)
{
    page_mark_t* dp = (page_mark_t*) _data;
    // reverse the sense of page_mark:
    dp->redo(page);
}

/*********************************************************************
 *
 *  page_shift_log
 *
 *  Log a shift from one slots to another in a page.
 *
 *********************************************************************/
class page_shift_t {
public:
    typedef page_s::slot_length_t slot_length_t;

    int2_t                 idx1;        // first slot affected
    slot_length_t         off1;        // 0->offset is from first slot, rest
                                // from second
    slot_length_t         len1;        
    int2_t                 idx2;        // second slot affected
    slot_length_t         off2;        

    NORET                page_shift_t(
        int                     i, 
        slot_length_t             o1, 
        slot_length_t             l1, 
        int                     j, 
        slot_length_t            o2 ) : idx1(i), off1(o1), len1(l1), 
                idx2(j), off2(o2){};

    int                        size()  { return sizeof(*this); }
};

page_shift_log::page_shift_log(
    const page_p&         page,
    int                 idx,
    page_shift_t::slot_length_t                off1, 
    page_shift_t::slot_length_t                len1, 
    int                 idx2,
    page_shift_t::slot_length_t                off2
    )
{
    fill(&page.pid(), page.tag(),
         (new (_data) page_shift_t(idx, off1, len1,
                        idx2, off2))->size());
}


void 
page_shift_log::redo(page_p* page)
{
    page_shift_t* dp = (page_shift_t*) _data;

    W_COERCE(page->shift(dp->idx1, dp->off1, dp->len1, 
        dp->idx2, dp->off2));
}

void
page_shift_log::undo(page_p* page)
{
    page_shift_t* dp = (page_shift_t*) _data;

    W_COERCE(page->shift(dp->idx2, dp->off2, dp->len1, 
        dp->idx1, dp->off1));
}


/*********************************************************************
 *
 *  page_splice_log
 *
 *  Log a splice operation on a record in page.
 *
 *********************************************************************/
struct page_splice_t {
    int2_t                 idx;        // slot affected
    uint2_t                 start;  // offset in the slot where the splice began
    uint2_t                 old_len;// len of old data (# bytes removed or overwritten by
                                // the splice operation
    uint2_t                 new_len;// len of new data (# bytes inserted or written by the splice)
    char                 data[logrec_t::data_sz - 4 * sizeof(int2_t)]; // old data & new data

    NORET                page_splice_t(
        int                     i, 
        uint                     start, 
        uint                     len, 
        const void*             tuple,
        const cvec_t&             v);
    void*                 old_image()   { return data; }
    void*                 new_image()   { return data + old_len; }
    int                        size()  { 
        return data + old_len + new_len - (char*) this; 
    }
};

page_splice_t::page_splice_t(
    int                 i,
    uint                 start_,
    uint                 len_,
    const void*         tuple,
    const cvec_t&         v)
    : idx(i), start(start_), 
      old_len(len_), new_len(v.size())
{
    w_assert1((size_t)(old_len + new_len) < sizeof(data));
    memcpy(old_image(), ((char*)tuple)+start, old_len);
    v.copy_to(new_image());
}

page_splice_log::page_splice_log(
    const page_p&         page,
    int                 idx,
    int                 start, 
    int                 len,
    const cvec_t&         v)
{
    w_assert9(len <= smlevel_0::page_sz);
    fill(&page.pid(), page.tag(),
         (new (_data) page_splice_t(idx, start, len,
                                    page.tuple_addr(idx), v))->size());
}

void 
page_splice_log::redo(page_p* page)
{
    page_splice_t* dp = (page_splice_t*) _data;
#if W_DEBUG_LEVEL > 0
    char* p = ((char*) page->tuple_addr(dp->idx)) + dp->start;
    if(memcmp(dp->old_image(), p, dp->old_len) != 0) {
        fprintf(stderr, "Things go bad in tuple %d\n", dp->idx);
        u_char *old = (u_char *)dp->old_image();
        u_char *tuple = (u_char *)p;
        int len = dp->old_len;
        fprintf(stderr, 
            "Comparison of len %d starts with old image at addr %p,and tuple+start(%d) @ %p\n", 
                len,
                old, dp->start, p);
        // Tell where it starts to go wrong.
        for(int i=0; i < len; i++) {
            if(*old != *tuple) {
                fprintf(stderr, "OK until offset %d\n", i);
                fprintf(stderr, "old image 0x%x page 0x%x\n", *old, *tuple);
            }
            old++;
            tuple++;
            i++;
        }
    }
    w_assert1(memcmp(dp->old_image(), p, dp->old_len) == 0);
#endif 

    const vec_t new_vec_tmp(dp->new_image(), dp->new_len);
    W_COERCE(page->splice(dp->idx, dp->start, dp->old_len, new_vec_tmp));
}

void
page_splice_log::undo(page_p* page)
{
    page_splice_t* dp = (page_splice_t*) _data;
#if W_DEBUG_LEVEL > 1
    if (page->tag() != page_p::t_extlink_p)  {
        // What does this comment mean? TODO
        /* do this only for vol map file. this is due to the fact
         * that page map resides in extlink.
         */
        char* p = ((char*) page->tuple_addr(dp->idx)) + dp->start;
        w_assert1(memcmp(dp->new_image(), p, dp->new_len) == 0);
    }
    w_assert1(dp->new_len <= smlevel_0::page_sz);
    w_assert1(dp->old_len <= smlevel_0::page_sz);
#endif 

    const vec_t old_vec_tmp(dp->old_image(), dp->old_len);
    W_COERCE(page->splice(dp->idx, dp->start, dp->new_len, old_vec_tmp));
}

/*********************************************************************
 *
 *  page_splicez_log
 *
 *  Log a splice operation on a record in page, in which
 *  either the old or the new data (or both, i suppose) are zeroes
 *
 *********************************************************************/


struct page_splicez_t {
    int2_t                 idx;        // slot affected
    uint2_t                 start;  // offset in the slot where the splice began
    uint2_t                 old_len;// len of old data (# bytes removed or overwritten by
                                // the splice operation
    uint2_t                 olen;   // len of old data stored in _data[]
    uint2_t                 new_len;// len of new data (# bytes inserted or written by the splice)
    uint2_t                 nlen;   // len of new data stroed in _data[]
    char                 data[logrec_t::data_sz - 6 * sizeof(int2_t)]; // old data & new data

    NORET                page_splicez_t(
        int                     i, 
        uint                     start, 
        uint                     len, 
        uint                     olen_, 
        uint                     nlen_,
        const void*             tuple,
        const cvec_t&             v);
    void*                 old_image()   { return data; }
    void*                 new_image()   { return data + olen; }
    int                        size()  { 
        return data + olen + nlen - (char*) this; 
    }
};
page_splicez_t::page_splicez_t(
    int                 i,
    uint                 start_,
    uint                 len_,
    uint                 save_,
    uint                 zlen,
    const void*         tuple,
    const cvec_t&         v)
    : idx(i), start(start_), old_len(len_), 
    olen(save_),
    new_len(v.size()),
    nlen(zlen)
{
    w_assert1((size_t)(old_len + new_len) < sizeof(data));
    w_assert1(old_len == olen || olen == 0);
    w_assert1(new_len == nlen || nlen == 0);

    if(olen== old_len) {
        memcpy(old_image(), ((char*)tuple)+start, old_len);
    }
    if(nlen== new_len) {
        w_assert1(nlen == v.size());
        v.copy_to(new_image());
    }

    DBG(<<"splicez log: " 
        << " olen: " << olen
        << " old_len: " << old_len
        << " new_len: " << new_len
    );
}

page_splicez_log::page_splicez_log(
    const page_p&         page,
    int                 idx,
    int                 start, 
    int                 len,
    int                 olen,
    int                 nlen,
    const cvec_t&         v)
{
    // todo: figure out if old or new data are zerovec
    fill(&page.pid(), page.tag(),
         (new (_data) page_splicez_t(idx, start, len, olen, nlen,
                                    page.tuple_addr(idx), v))->size());
}

void 
page_splicez_log::redo(page_p* page)
{
    page_splicez_t* dp = (page_splicez_t*) _data;

#if W_DEBUG_LEVEL > 0
    {
        char* p = ((char*) page->tuple_addr(dp->idx)) + dp->start;


        // one or the other length must be 0, else we would
        // have logged a regular page splice, not a splicez
        w_assert1(dp->olen == 0 || dp->nlen == 0);
        w_assert1( dp->new_len <= smlevel_0::page_sz);
        w_assert1( dp->old_len <= smlevel_0::page_sz);

        // three cases:
        // saved old image, new image is zeroes
        // saved new image, old image is zeroes
        // saved neither image, both were zeroes


        if(dp->old_len > 0) { 
            // there was an old image to consider saving
            if(dp->olen == 0) {
                // the old image was zeroes so we didn't bother saving it.
                // make sure the page is consistent with that analysis
                w_assert1(memcmp(smlevel_0::zero_page, p, dp->old_len) == 0);
            } else {
                // the old image was saved -- check it
                w_assert1(memcmp(dp->old_image(), p, dp->old_len) == 0);
            }
        } // else nothing to compare, since old len is 0 (it was an insert)
    }
#endif 
    vec_t z;

    if(dp->nlen == 0) {
        const zvec_t new_vec_tmp(dp->new_len);
        z.set(new_vec_tmp);
    } else {
        z.set(dp->new_image(), dp->new_len);
    }

    W_COERCE(page->splice(dp->idx, dp->start, dp->old_len, z));
}

void
page_splicez_log::undo(page_p* page)
{
    page_splicez_t* dp = (page_splicez_t*) _data;
#if W_DEBUG_LEVEL > 2
    if (page->tag() != page_p::t_extlink_p)  {
        /* do this only for vol map file. this is due to the fact
         * that page map resides in extlink.
         */
        char* p = ((char*) page->tuple_addr(dp->idx)) + dp->start;

        // one or the other length must be 0, else we would
        // have logged a regular page splice, not a splicez
        w_assert1(dp->olen == 0 || dp->nlen == 0);
        w_assert1( dp->new_len <= smlevel_0::page_sz);
        w_assert1( dp->old_len <= smlevel_0::page_sz);

        // three cases:
        // saved old image, new image is zeroes
        // saved new image, old image is zeroes
        // saved neither image, both were zeroes

        if(dp->nlen == 0) {
            // the new image was zeroes so we didn't bother saving it.
            // make sure the page is consistent with that analysis
            w_assert1(memcmp(smlevel_0::zero_page, p, dp->new_len) == 0);
        } else {
            // the new image was saved
            w_assert1(memcmp(dp->new_image(), p, dp->new_len) == 0);
        }
    }
#endif 
    DBG(<<"splicez undo: "
        << " olen (stored) : " << dp->olen
        << " old_len (orig): " << dp->old_len
        << " nlen (stored) : " << dp->nlen
        << " new_len (orig): " << dp->new_len
    );

    vec_t z;
    if(dp->olen == 0) {
        const zvec_t old_vec_tmp(dp->old_len);
        z.set(old_vec_tmp);
    } else {
        z.set(dp->old_image(), dp->old_len);
    }
    W_COERCE(page->splice(dp->idx, dp->start, dp->new_len, z));
}

/*********************************************************************
 *
 *  page_set_byte_log
 *
 *  Log a logical operation on a byte's worth of bits at index idx
 *
 *********************************************************************/
struct page_set_byte_t {
    uint2_t       idx;
    fill2         filler;
    u_char        old_value;
    u_char        bits;
    int                operation;
    NORET        page_set_byte_t(uint2_t i, u_char old, u_char oper, int op) : 
        idx(i), old_value(old), bits(oper), operation(op) {};
    int                size()   { return sizeof(*this); }
};

page_set_byte_log::page_set_byte_log(const page_p& page, int idx, 
        u_char what, u_char bits, int op)
{    
    w_assert2(page.tag() == page_p::t_extlink_p);
    fill(&page.pid(), page.tag(),
                (new (_data) page_set_byte_t(idx, what, bits, op))->size());
}

void 
page_set_byte_log::undo(page_p* page)
{
    page_set_byte_t* dp = (page_set_byte_t*) _data;
    // restore the old data: do that by splicing in the old data
    W_COERCE( page->set_byte(dp->idx, dp->old_value, page_p::l_set) );
}

void 
page_set_byte_log::redo(page_p* page)
{
    page_set_byte_t* dp = (page_set_byte_t*) _data;
    W_COERCE( page->set_byte(dp->idx, dp->bits, (enum page_p::logical_operation)
        (dp->operation)) );
}


/*********************************************************************
 *
 *  page_image_log
 *
 *  Log page image. Redo only.
 *
 *********************************************************************/
struct page_image_t {
    char image[sizeof(page_s)];
    page_image_t(const page_p& page)  {
        memcpy(image, &page.persistent_part_const(), sizeof(image));
    }
    int size()        { return sizeof(*this); }
};

page_image_log::page_image_log(const page_p& page)
{
    // for now these are the only two page types using this
    w_assert2(page.tag() == page_p::t_btree_p
                ||
              page.tag() == page_p::t_rtree_p);

    fill(&page.pid(), page.tag(), (new (_data) page_image_t(page))->size());
}

void 
page_image_log::redo(page_p* page)
{
    page_image_t* dp = (page_image_t*) _data;
    memcpy(&page->persistent_part(), dp->image, sizeof(dp->image));
}



/*********************************************************************
 *
 *  btree_purge_log
 *
 *  Log a logical purge operation for btree.
 *
 *********************************************************************/
struct btree_purge_t {
    lpid_t        root;
    btree_purge_t(const lpid_t& pid) : root(pid) {};
    int size()        { return sizeof(*this); }
};


btree_purge_log::btree_purge_log(const page_p& page)
{
    fill(&page.pid(), page.tag(),
                (new (_data) btree_purge_t(page.pid()))->size());
}


void
btree_purge_log::undo(page_p* page)
{
    // keep compiler quiet about unused parameter
    if (page) {}

    /* This might seem weird, to undo a purge by purging...
     * but this is used only by bulk-loading, and this
     * turns out to be a no-op, I think.   Probably we should
     * just make this a no-undo record, since we compensate around
     * the freeing of pages anyway; all this is doing is re-setting
     * the hdr.
     */
    btree_purge_t* dp = (btree_purge_t*) _data;
    W_COERCE( smlevel_2::bt->purge(dp->root, false, false));
}


void
btree_purge_log::redo(page_p* page)
{
    /*
     * The freeing-of-pages part was redone by the
     * log records for those operations.  All we have
     * to redo is the change to the root page and the
     * store flags 
     */
    btree_p* bp = (btree_p*) page;
    btree_purge_t* dp = (btree_purge_t*) _data;
    W_COERCE( bp->set_hdr(dp->root.page, 1, 0, 
        (uint2_t)(bp->is_compressed()? 
        btree_p::t_compressed: btree_p::t_none)) );

    /*
     *  Xct for which this is done could have finished,
     *  but that's ok.  In recovery, we don't need to 
     *  attach the xct because there will be log records
     *  (to be redone) that re-set the store flags as
     *  required.
     */
    W_COERCE( smlevel_0::io->set_store_flags(dp->root.stid(), 
        btree_m::bulk_loaded_store_type) );
}




/*********************************************************************
 *
 *   btree_insert_log
 *
 *   Log a logical insert into a btree.
 *
 *********************************************************************/
struct btree_insert_t {
    lpid_t        root;
    int2_t        idx;
    uint2_t        klen;
    uint2_t        elen;
    int2_t        unique;
    char        data[logrec_t::data_sz - sizeof(lpid_t) - 4*sizeof(int2_t)];

    btree_insert_t(const btree_p& page, int idx, const cvec_t& key,
                   const cvec_t& el, bool unique);
    int size()        { return data + klen + elen - (char*) this; }
};

btree_insert_t::btree_insert_t(
    const btree_p&         _page, 
    int                 _idx,
    const cvec_t&         key, 
    const cvec_t&         el,
    bool                 uni)
    : idx(_idx), klen(key.size()), elen(el.size()), unique(uni)
{
    root = _page.root();
    w_assert1((size_t)(klen + elen) < sizeof(data));
    key.copy_to(data);
    el.copy_to(data + klen);
}

btree_insert_log::btree_insert_log(
    const page_p&         page, 
    int                 idx, 
    const cvec_t&         key,
    const cvec_t&         el,
    bool                 unique)
{
    const btree_p& bp = * (btree_p*) &page;
    fill(&page.pid(), page.tag(),
         (new (_data) btree_insert_t(bp, idx, key, el, unique))->size());
}

void 
btree_insert_log::undo(page_p* W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    btree_insert_t* dp = (btree_insert_t*) _data;

    cvec_t key, el;
    key.put(dp->data, dp->klen);
    el.put(dp->data + dp->klen, dp->elen);

    // ***LOGICAL*** don't grab locks during undo
    rc_t rc;
 again:
    rc = smlevel_2::bt->_remove(dp->root, (dp->unique != 0), 
                                     smlevel_0::t_cc_none, key, el); 
    if(rc.is_error()) {
        if(rc.err_num() == smlevel_0::eRETRY) {
            fprintf(stderr, "Retrying undo of btree insert due to eRETRY\n");
            goto again;
        }
        smlevel_2::bt->print(dp->root);
        cerr         << " key=" << key  << endl
                << " el =" << el  << endl
                << " rc =" << rc  
                <<endl;
        W_FATAL(rc.err_num());
    }
}

void
btree_insert_log::redo(page_p* page)
{
    btree_p* bp = (btree_p*) page;
    btree_insert_t* dp = (btree_insert_t*) _data;
    vec_t key, el;
    key.put(dp->data, dp->klen);
    el.put(dp->data + dp->klen, dp->elen);

    // ***PHYSICAL***
    /* for debugging, removed macro
    W_COERCE( bp->insert(key, el, dp->idx) ); 
    */
    w_rc_t rc;
 again:
    rc =  bp->insert(key, el, dp->idx);
    if(rc.is_error()) {
        if(rc.err_num() == smlevel_0::eRETRY) {
            fprintf(stderr, "Retrying redo of btree insert due to eRETRY\n");
            goto again;
        }
        // btree_m::print_key_str(dp->root,false);
        W_FATAL_MSG(fcINTERNAL, << "btree_insert_log::redo " );
    }
}



/*********************************************************************
 *
 *   btree_remove_log
 *
 *   Log a logical removal from a btree.
 *
 *********************************************************************/
typedef btree_insert_t btree_remove_t;

btree_remove_log::btree_remove_log(
    const page_p&         page, 
    int                 idx,
    const cvec_t&         key, 
    const cvec_t&         el,
    bool                 unique)
{
    const btree_p& bp = * (btree_p*) &page;
    fill(&page.pid(), page.tag(),
         (new (_data) btree_remove_t(bp, idx, key, el, unique))->size());
}

void
btree_remove_log::undo(page_p* W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    btree_remove_t* dp = (btree_remove_t*) _data;

    cvec_t key, el;
    key.put(dp->data, dp->klen);
    el.put(dp->data + dp->klen, dp->elen);

    /*
     *  Note: we do not care if this is a unique btree because
     *              if everything is correct, we could assume a 
     *        non-unique btree.
     *  Note to note: if we optimize kvl locking for unique btree, we
     *                cannot assume non-uniqueness.
     */
    // ***LOGICAL*** Don't grab locks during undo
    rc_t rc;
    rc = smlevel_2::bt->_insert(dp->root, (dp->unique != 0), 
                                     smlevel_0::t_cc_none, key, el); 
    if(rc.is_error()) {
        smlevel_2::bt->print(dp->root);
        cerr         << " key=" << key << endl
                << " el =" << el  << endl
                << " rc =" << rc  
                <<endl;
        W_FATAL(rc.err_num());
    }
}

void 
btree_remove_log::redo(page_p* page)
{
    btree_p* bp = (btree_p*) page;
    btree_remove_t* dp = (btree_remove_t*) _data;

#if W_DEBUG_LEVEL > 2
    cvec_t key, el;
    key.put(dp->data, dp->klen);
    el.put(dp->data + dp->klen, dp->elen);

    btrec_t r(*bp, dp->idx);
    w_assert3(el == r.elem());
    w_assert3(key == r.key());
#endif 

    /* for debugging, removed macro
    W_COERCE( bp->remove(dp->idx, bp->is_compressed()) ); // ***PHYSICAL***
    */
    w_rc_t rc;
    rc =  bp->remove(dp->idx, bp->is_compressed()); 
    if(rc.is_error()) {
        // btree_m::print_key_str(dp->root,false);
        w_assert1(0);
    }
}



/*********************************************************************
 *
 *   rtree_insert_log
 *
 *********************************************************************/
struct rtree_insert_t {
    lpid_t        root;
    int2_t        idx;
    uint2_t        klen;
    uint2_t        elen;
    char        data[logrec_t::data_sz - sizeof(lpid_t) - 3*sizeof(int2_t)];

    rtree_insert_t(const rtree_p& page, int idx, const nbox_t& key,
                   const cvec_t& el);
    int size()        { return data + klen + elen - (char*) this; }
};

rtree_insert_t::rtree_insert_t(const rtree_p& _page, int _idx,
                               const nbox_t& key, const cvec_t& el)
: idx(_idx), klen(key.klen()), elen(el.size())
{
    _page.root(root);
    w_assert1((size_t)(klen + elen) < sizeof(data));
    memcpy(data, key.kval(), klen);
    el.copy_to(data + klen);
}

rtree_insert_log::rtree_insert_log(const page_p& page, int idx, 
                                   const nbox_t& key, const cvec_t& el)
{
    const rtree_p& rp = * (rtree_p*) &page;
    fill(&page.pid(), page.tag(),
         (new (_data) rtree_insert_t(rp, idx, key, el))->size());
}

void
rtree_insert_log::undo(page_p* W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    rtree_insert_t* dp = (rtree_insert_t*) _data;

    nbox_t key(dp->data, dp->klen);
    cvec_t el;
    el.put(dp->data + dp->klen, dp->elen);
    W_COERCE( smlevel_2::rt->remove(dp->root, key, el) ); // ***LOGICAL***
}

void
rtree_insert_log::redo(page_p* page)
{
    rtree_p* rp = (rtree_p*) page;
    rtree_insert_t* dp = (rtree_insert_t*) _data;
    
    nbox_t key(dp->data, dp->klen);
    cvec_t el;
    el.put(dp->data + dp->klen, dp->elen);

    W_COERCE( rp->insert(key, el) ); // ***PHYSICAL***
}



/*********************************************************************
 *
 *   rtree_remove_log
 *
 *********************************************************************/
typedef rtree_insert_t rtree_remove_t;

rtree_remove_log::rtree_remove_log(const page_p& page, int idx,
                                   const nbox_t& key, const cvec_t& el)
{
    const rtree_p& rp = * (rtree_p*) &page;
    fill(&page.pid(), page.tag(),
         (new (_data) rtree_remove_t(rp, idx, key, el))->size());
}

void
rtree_remove_log::undo(page_p* W_IFDEBUG9(page))
{
    w_assert9(page == 0);
    rtree_remove_t* dp = (rtree_remove_t*) _data;

    nbox_t key(dp->data, dp->klen);
    cvec_t el;
    el.put(dp->data + dp->klen, dp->elen);
    W_COERCE( smlevel_2::rt->insert(dp->root, key, el) ); // ***LOGICAL***
}

void
rtree_remove_log::redo(page_p* page)
{
    rtree_p* rp = (rtree_p*) page;
    rtree_remove_t* dp = (rtree_remove_t*) _data;

#if W_DEBUG_LEVEL > 2
    nbox_t key(dp->data, dp->klen);
    cvec_t el;
    el.put(dp->data + dp->klen, dp->elen);
    w_assert3(el.cmp(rp->rec(dp->idx).elem(), rp->rec(dp->idx).elen()) == 0);
    w_assert3(memcmp(key.kval(), rp->rec(dp->idx).key(),
                                rp->rec(dp->idx).klen()) == 0);
#endif 

    W_COERCE( rp->remove(dp->idx) ); // ***PHYSICAL***
}

/*********************************************************************
 *
 *  alloc_pages_in_ext_log
 *  free_pages_in_ext_log
 *
 *  Log of page allocations in an extent
 *
 *********************************************************************/

struct pages_in_ext_t  {
    snum_t                snum;
    extnum_t                ext;
    Pmap_Align4                pmap; // grot: for purify -because
                              // with 4-byte extnum_t, gcc keeps
                              // the struct 4-byte aligned

    pages_in_ext_t(snum_t theStore, extnum_t theExt, const Pmap& thePmap);
    int size() const  { return sizeof(*this); }
};


pages_in_ext_t::pages_in_ext_t(snum_t theStore, extnum_t theExt, const Pmap& thePmap)
    : snum(theStore), ext(theExt)
{
    pmap = thePmap;
    w_assert9(&thePmap != 0);
    w_assert9(theExt);
    w_assert9(theStore);
}


alloc_pages_in_ext_log::alloc_pages_in_ext_log(const page_p& page, snum_t snum, extnum_t ext, const Pmap& pmap)
{
    fill(&page.pid(), page.tag(), (new (_data) pages_in_ext_t(snum, ext, pmap))->size());
}


void alloc_pages_in_ext_log::redo(page_p* /*page*/)
{
    pages_in_ext_t* thePages = (pages_in_ext_t*)_data;
    W_COERCE( smlevel_0::io->_recover_pages_in_ext(vid(), thePages->snum, thePages->ext, thePages->pmap, true) );
}


void alloc_pages_in_ext_log::undo(page_p* /*page*/)
{
    pages_in_ext_t* thePages = (pages_in_ext_t*)_data;
    W_COERCE( smlevel_0::io->recover_pages_in_ext(vid(), thePages->snum, thePages->ext, thePages->pmap, false) );
}


free_pages_in_ext_log::free_pages_in_ext_log(const page_p& page, snum_t snum, extnum_t ext, const Pmap& pmap)
{
    fill(&page.pid(), page.tag(), (new (_data) pages_in_ext_t(snum, ext, pmap))->size());
}


void free_pages_in_ext_log::redo(page_p* /*page*/)
{
    pages_in_ext_t* thePages = (pages_in_ext_t*)_data;
    W_COERCE( smlevel_0::io->_recover_pages_in_ext(vid(), thePages->snum, thePages->ext, thePages->pmap, false) );
}


void free_pages_in_ext_log::undo(page_p* /*page*/)
{
    pages_in_ext_t* thePages = (pages_in_ext_t*)_data;
    W_COERCE( smlevel_0::io->recover_pages_in_ext(vid(), thePages->snum, thePages->ext, thePages->pmap, true) );
}



/*********************************************************************
 *
 *  free_ext_list_log
 *
 *  Log of exts all on the same ext_link page which are to be set to
 *  freeing.
 *
 *********************************************************************/

struct free_ext_list_t  {
    stid_t        stid;
    extnum_t        head;
    extnum_t        count;

    free_ext_list_t(const stid_t& theStid, extnum_t theHead, extnum_t theCount)
        : stid(theStid), head(theHead), count(theCount)
        {}
    int size()
        { return sizeof(*this); }
};


free_ext_list_log::free_ext_list_log(const page_p& page, const stid_t& stid, extnum_t head, extnum_t count)
{
    fill(&page.pid(), page.tag(), (new (_data) free_ext_list_t(stid, head, count))->size());
}

void free_ext_list_log::redo(page_p* /*page*/)
{
    free_ext_list_t*        freeExtInfo = (free_ext_list_t*)_data;
    W_COERCE( smlevel_0::io->free_exts_on_same_page(freeExtInfo->stid, freeExtInfo->head, freeExtInfo->count) );
}


/*********************************************************************
 *
 *  set_ext_next_log
 *
 *  Log of setting the next field of an extent list
 *  Redo-only
 *
 *********************************************************************/

struct set_ext_next_t  {
    extnum_t        ext;
    extnum_t        new_next;

    set_ext_next_t(extnum_t theExt, extnum_t theNew_next)
        : ext(theExt), new_next(theNew_next)
        {}
    int size()
        { return sizeof(*this); }
};


set_ext_next_log::set_ext_next_log(const page_p& page, 
        extnum_t ext, extnum_t new_next)
{
    w_assert2(page.tag() == page_p::t_extlink_p);
    fill(&page.pid(), page.tag(), 
        (new (_data) set_ext_next_t(ext, new_next))->size());
}

void set_ext_next_log::redo(page_p* /*page*/)
{
    set_ext_next_t* info = (set_ext_next_t*)_data;
    W_COERCE( smlevel_0::io->set_ext_next(vid(), 
        info->ext, info->new_next) );
}


/*********************************************************************
 *
 *  create_ext_list_log
 *
 *  Log of exts all on the same ext_link page which are to be linked
 *  together.
 *
 *********************************************************************/

struct create_ext_list_t  {
    /* NB: Do NOT put these on the stack -- must be allocated via new */
    stid_t            stid;
    extnum_t            prev;
    extnum_t            next;
    extnum_t            count;
    extnum_t            list[stnode_p::max];

    create_ext_list_t(const stid_t& theStid, extnum_t thePrev, extnum_t theNext, extnum_t theCount, const extnum_t* theList)
        : stid(theStid), prev(thePrev), next(theNext), count(theCount)
        {
            w_assert9(count);
            while (theCount--)
                list[theCount] = theList[theCount];
        }
    int size()
        { return ((char*)&this->list[count] - (char*)this); }
};

create_ext_list_log::create_ext_list_log(
    const page_p&            page,
    const stid_t&            stid,
    extnum_t                    prev,
    extnum_t                    next,
    extnum_t                    count,
    const extnum_t*                list)
{
    fill(&page.pid(), page.tag(), (new (_data) create_ext_list_t(stid, prev, next, count, list))->size());
}

void create_ext_list_log::redo(page_p* /*page*/)
{
    create_ext_list_t*        info = (create_ext_list_t*)_data;
    W_COERCE( smlevel_0::io->create_ext_list_on_same_page(info->stid, info->prev, info->next, info->count, info->list) );
}


/*********************************************************************
 *
 *  store_operation_log
 *
 *  Log of setting the deleting flag in the stnode
 *
 *********************************************************************/

store_operation_log::store_operation_log(const page_p& page, const store_operation_param& param)
{
    fill(&page.pid(), page.tag(), (new (_data) store_operation_param(param))->size());
}

void store_operation_log::redo(page_p* /*page*/)
{
    store_operation_param& param = *(store_operation_param*)_data;
    DBG( << "store_operation_log::redo(page=" << pid() 
        << ", param=" << param << ")" );
    W_COERCE( smlevel_0::io->store_operation(vid(), param) );
}

void store_operation_log::undo(page_p* /*page*/)
{
    store_operation_param& param = *(store_operation_param*)_data;
    DBG( << "store_operation_log::undo(page=" << shpid() << ", param=" << param << ")" );

    switch (param.op())  {
        case smlevel_0::t_delete_store:
            /* do nothing, not undoable */
            break;
        case smlevel_0::t_create_store:
            {
                stid_t stid(vid(), param.snum());
                histoid_t::destroyed_store(stid, 0);
                W_COERCE( smlevel_0::io->destroy_store(stid) );
            }
            break;
        case smlevel_0::t_set_deleting:
            switch (param.new_deleting_value())  {
                case smlevel_0::t_not_deleting_store:
                case smlevel_0::t_deleting_store:
                    {
                        store_operation_param new_param(param.snum(), 
                                smlevel_0::t_set_deleting,
                                param.old_deleting_value(), 
                                param.new_deleting_value());
                        W_COERCE( smlevel_0::io->store_operation(vid(), 
                                new_param) );
                    }
                    break;
                case smlevel_0::t_store_freeing_exts:
                    /* do nothing, not undoable */
                    break;
                case smlevel_0::t_unknown_deleting:
                    W_FATAL(smlevel_0::eINTERNAL);
                    break;
            }
            break;
        case smlevel_0::t_set_store_flags:
            {
                store_operation_param new_param(param.snum(), 
                        smlevel_0::t_set_store_flags,
                        param.old_store_flags(), param.new_store_flags());
                W_COERCE( smlevel_0::io->store_operation(vid(), 
                        new_param) );
            }
            break;
        case smlevel_0::t_set_first_ext:
            /* do nothing, not undoable */
            break;
    }
}




/*********************************************************************
 *
 *  operator<<(ostream, logrec)
 *
 *  Pretty print a log record to ostream.
 *
 *********************************************************************/
#include "logtype_gen.h"
ostream& 
operator<<(ostream& o, const logrec_t& l)
{
    ios_fmtflags        f = o.flags();
    o.setf(ios::left, ios::left);

    const char *rb = l.is_rollback()? "U" : "F"; // rollback/undo or forward

    o << l._tid << ' ';
    W_FORM(o)("%19s%5s:%1s", l.type_str(), l.cat_str(), rb );
    o << "  " << l.pid();

    switch(l.type()) {
        case t_comment : 
                {
                    o << (const char *)l._data;
                }
                break;
        case t_page_format:
                {
                    page_init_t* info = (page_init_t*)l._data;
                    o << " tag=" << info->page_tag()
                      << " page flags=" << info->page_flag()
                      << " store flags=" << info->store_flag();
                }
                break;
        case t_page_reclaim : 
        case t_page_mark : 
                 { 
                    page_mark_t *t = (page_mark_t *)l._data;
                    o << t->idx ;
                }
                break;

        case t_free_ext_list:
                {
                    free_ext_list_t* info = (free_ext_list_t*)l._data;
                    o << " stid=" << info->stid
                      << " head=" << info->head
                      << " count=" << info->count;
                }
                break;
        
        case t_set_ext_next:
                {
                    set_ext_next_t* info = (set_ext_next_t*)l._data;
                    o << " ext=" << info->ext
                      << " new_next=" << info->new_next;
                }
                break;
        
        case t_create_ext_list:
                {
                    create_ext_list_t* info = (create_ext_list_t*)l._data;
                    o << " stid=" << info->stid
                      << " prev=" << info->prev
                      << " next=" << info->next
                      << " count=" << info->count
                      << " list=[";
                    for (unsigned int i = 0; i < info->count; i++)
                        o << ' ' << info->list[i];
                    o << " ]";
                }
                break;

        case t_store_operation:
                {
                    store_operation_param& param = *(store_operation_param*)l._data;
                    o << ' ' << param;
                }
                break;

        case t_alloc_pages_in_ext:
        case t_free_pages_in_ext:
                {
                    pages_in_ext_t* info = (pages_in_ext_t*)l._data;
                    o << " ext=" << info->ext;
                    o << " pmap=" << info->pmap;
                    // Now print the page number
                    { 
                        int num = info->pmap.num_set();
                        int bit = 0;
                        int sz = info->pmap._count;
                        for(int i=0; i< num; i++) {
                            bit = info->pmap.first_set(bit);
                            shpid_t p = info->ext * sz;
                            p += bit;
                            o << " pg# " << p ;
                        }
                    }
                }
                break;
                      

        case t_alloc_file_page:
        default: /* nothing */
                break;
    }

    if (l.is_cpsn())  o << " (" << l.undo_nxt() << ')';
    else  o << " [" << l.prev() << "]";

    o.flags(f);
    return o;
}

/*********************************************************************
 *
 *  page_format_log : a combined page_init and page_insert
 *                    for the idiom, which is used in so many places,
 *                    we combine the two to reduce logging and page-fixes
 *
 ********************************************************************/
class page_format_t {
public:
    page_init_t   _init;
    page_init_t*   init() { return &_init; }

    enum { max = (logrec_t::data_sz - sizeof(page_init_t)) };

    char        data[max]; 

    page_insert_t*    insert() { return (page_insert_t *)data; }
    page_reclaim_t*   reclaim() { return (page_reclaim_t *)&data; }

    // version for init/insert_expand:
    // For all non-space-reservation pages, insert the vector to format
    // the page. 
    page_format_t(  uint2_t tag, uint4_t page_flag, uint4_t store_flag,
                    int idx, 
                    int cnt, 
                    const cvec_t* vec) : 
           _init(tag, page_flag, store_flag) 
       {
           w_assert3(tag != page_p::t_file_p && tag != page_p::t_file_mrbt_p);
           w_assert2(cnt == 1);
           new (data) page_insert_t(idx, cnt, vec);
       }

    // version for init/reclaim (file_p):
    // This version reclaims (allocs) a slot in the (space-reservation-) page
    // and inserts the vector there.
    page_format_t(  uint2_t tag, uint4_t page_flag, uint4_t store_flag,
                    int idx,  
                    const cvec_t* vec) : 
           _init(tag, page_flag, store_flag) 
       {
           w_assert3(tag == page_p::t_file_p || tag == page_p::t_file_mrbt_p);
           zvec_t zv;
           if(vec==0) vec = &zv;
           new (data) page_reclaim_t(idx, *vec);
       }

    int size()  { return _init.size() + 
        ((_init.page_tag() == page_p::t_file_p ||
	  _init.page_tag() == page_p::t_file_mrbt_p) ? reclaim()->size() : insert()->size());
        }
};


// All page types use cnt==1 except file_p, which uses cnt==0
// Because file_p is a space_resv page, it gets redone and undone
// differently from the way the others do.
page_format_log::page_format_log(const page_p& p,
    int                 idx, 
    int                 cnt,
    const cvec_t*       vec)
{
    w_assert3(p.pid() != lpid_t::null);

#if W_DEBUG_LEVEL > 1
    if ( (p.get_store_flags() & p.st_tmp) != 0 ) 
    {

        // Cannot be a stnode_p or an extlink_p, since they
        // are in store 0 and had better be fully logged!

        w_assert2(p.tag() == page_p::t_lgdata_p 
                ||
                p.tag() == page_p::t_lgindex_p 
                ||
                p.tag() == page_p::t_rtree_p 
                ||
                p.tag() == page_p::t_btree_p 
                ||
                p.tag() == page_p::t_file_p
		||
                p.tag() == page_p::t_file_mrbt_p 
                );

    } 
#endif
    if (p.tag() == page_p::t_file_p || p.tag() == page_p::t_file_mrbt_p)  {
        // the 2nd part is a page_reclaim_t
        // only available to file_p
        w_assert2(cnt==0);

        fill(&p.pid(), p.tag(), 
            (new (_data) 
                page_format_t( p.tag(), p.page_flags(), 
                    p.get_store_flags(), idx, vec)
            ) ->size());

    } else {
        //  can't be a file p, but could be any
        //  of the others
        w_assert2(cnt==1);
        w_assert3(
            p.tag() == page_p::t_extlink_p 
            ||
            p.tag() == page_p::t_stnode_p 
            ||
            p.tag() == page_p::t_lgdata_p 
            ||
            p.tag() == page_p::t_lgindex_p 
            ||
            p.tag() == page_p::t_rtree_p 
            ||
            p.tag() == page_p::t_btree_p 
        );

        fill(&p.pid(), p.tag(), 
            (new (_data) 
                page_format_t( p.tag(), p.page_flags(), 
                    p.get_store_flags(), idx, cnt, vec)
        ) ->size());
    }
}


void 
page_format_log::redo(page_p* page)
{
    page_format_t* df = (page_format_t*) _data;
    df->init()->redo_init(page, pid());
    if(page->tag() == page_p::t_file_p  || page->tag() == page_p::t_file_mrbt_p) {
        // reclaim is reverse of page_mark,
        // and reclaim() is really a page_mark_t *
        df->reclaim()->undo(page);
    } else {
        df->insert()->redo(page);
    }
}

void 
page_format_log::undo(page_p* page)
{
    page_format_t* df = (page_format_t*) _data;

    // There is no undo for page_init.
    if(page->tag() == page_p::t_file_p || page->tag() == page_p::t_file_mrbt_p) {
        // reclaim is reverse of page_mark,
        // and reclaim() is really a page_mark_t *
        df->reclaim()->redo(page);
    } else {
        df->insert()->undo(page);
    }
}

alloc_file_page_log::alloc_file_page_log(const lpid_t& p)
{
    // fill size is 0; all we need is the page id
    fill(&p, 0, 0);
}

void
alloc_file_page_log::undo(page_p* W_IFDEBUG1(already_fixed))
{
    w_assert1(already_fixed==NULL);
    // If the tag is 0 (t_bad_p) it's possibly because
    // the page was discarded before we started the rollback.
    // Scenario: create file, ..., destroy file (discard),
    // rollback. Now we are undoing the page allocation for
    // the create-file operation and we don't know if the
    // page is still in use and in the same store. We haven't
    // released the locks yet so noone but this tx could
    // have deleted the page because an EX lock is required
    // to free the page, and that should also prevent another
    // tx from re-allocating the page to another file.
    // See comments in file.cpp, file_m::_undo_alloc_file_page()
    page_p page;
    
    smlevel_0::store_flag_t store_flags = smlevel_0::st_bad;
    w_rc_t rc = page.fix(this->pid(), page_p::t_any_p, LATCH_EX, 
        0/*no flag*/, store_flags, true /* ignore store id */);

    if(rc.is_error()) {
        W_FATAL_MSG(eINTERNAL,<< "Cannot fix page " << this->pid()
                << " because " << rc);
    }

    if(page.lsn() > lsn_ck()) {
        // Page was updated after this and made durable.
        //
        w_assert3(page.pid() == this->pid());
        w_assert3(page.pid()._stid.store == this->pid()._stid.store);
        // Is it possible, per the
        // comment in _undo_alloc_file_page(), that
        // the page was discarded and could be anything?
        // Yes. The assert is bogus:
        // w_assert1(page.tag() == page_p::t_file_p);

        file_p *fp = (file_p *)&page;
        rc = file_m::_undo_alloc_file_page(*fp);
        if(rc.is_error()) {
            W_FATAL_MSG(eINTERNAL,<< "undo alloc of page " << this->pid()
                << " because " << rc);
        }
    } else {
        // We don't have to undo this b/c the page was discarded before
        // the page format and subsequent updates were made durable.
        //
        // This is handled differently from most log records
        // b/c the page we give in the log record is the
        // one being (de)allocated, but it is not being updated.
        // If we use the generic page-fixing-for-log-record-undo code
        // in xct_impl.cpp, the fix will use ignore_store_id=false,
        // and we need to use ignore_store_id=true above.
        //
        // We use this page ONLY to check that it's not still in use.
        // 
    }

}

