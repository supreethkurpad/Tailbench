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

 $Id: smfile.cpp,v 1.62 2010/06/08 22:28:56 nhall Exp $

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
#define SMFILE_C

#include "w.h"
#include "option.h"
#include "sm_int_4.h"
#include "btcursor.h"
#include "lgrec.h"
#include "device.h"
#include "app_support.h"
#include "sm.h"


#if W_DEBUG_LEVEL > 0
#define  FILE_LOG_COMMENT_ON 1
#else
#define  FILE_LOG_COMMENT_ON 0
#endif

/*==============================================================*
 *  Physical ID version of all the storage operations           *
 *==============================================================*/

/*--------------------------------------------------------------*
 *  ss_m::set_store_property()                                  *
 *--------------------------------------------------------------*/
rc_t
ss_m::set_store_property(stid_t stid, store_property_t property)
{
    SM_PROLOGUE_RC(ss_m::set_store_property, in_xct, read_write, 0);
    W_DO( _set_store_property( stid, property) );
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::get_store_property()                                  *
 *--------------------------------------------------------------*/
rc_t
ss_m::get_store_property(stid_t stid, store_property_t& property)
{
    SM_PROLOGUE_RC(ss_m::get_store_property, in_xct,read_only, 0);
    W_DO( _get_store_property( stid, property) );
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::create_file()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::create_file(
    vid_t                          vid, 
    stid_t&                        fid, 
    store_property_t               property,
    shpid_t                        cluster_hint // = 0
)
{
#if FILE_LOG_COMMENT_ON
    W_DO(log_comment("create_file"));
#endif
    SM_PROLOGUE_RC(ss_m::create_file, in_xct, read_write, 0);
    DBGTHRD(<<"create_file " <<vid << " " << property );
    W_DO(_create_file(vid, fid, property, cluster_hint));
    DBGTHRD(<<"create_file returns " << fid);
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::destroy_file()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::destroy_file(const stid_t& fid)
{
    SM_PROLOGUE_RC(ss_m::destroy_file, in_xct,read_write, 0);
    DBGTHRD(<<"destroy_file " <<fid);
    W_DO(_destroy_file(fid));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::create_rec()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::create_rec(const stid_t& fid, const vec_t& hdr,
                 smsize_t len_hint, const vec_t& data, rid_t& new_rid
#ifdef SM_DORA
                 , const bool bIgnoreLocks
#endif
                 )
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "create_rec " << fid;
        W_DO(log_comment(s.c_str()));
    }
#endif
    SM_PROLOGUE_RC(ss_m::create_rec, in_xct,read_write, 0);

    W_DO(_create_rec(fid, hdr, len_hint, data, new_rid
#ifdef SM_DORA
                     , bIgnoreLocks
#endif
                     ));

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::destroy_rec()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::destroy_rec(const rid_t& rid
#ifdef SM_DORA
                  , const bool bIgnoreLocks
#endif
                  )
{
    SM_PROLOGUE_RC(ss_m::destroy_rec, in_xct,read_write, 0);
    DBG(<<"destroy_rec " <<rid);

    W_DO(_destroy_rec(rid
#ifdef SM_DORA
                      , bIgnoreLocks
#endif
                      ));

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::update_rec()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::update_rec(const rid_t& rid, smsize_t start, const vec_t& data)
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "update_rec " << rid;
        W_DO(log_comment(s.c_str()));
    }
#endif
    SM_PROLOGUE_RC(ss_m::update_rec, in_xct,read_write, 0);
    W_DO(_update_rec(rid, start, data));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::update_rec_hdr()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::update_rec_hdr(const rid_t& rid, smsize_t start, const vec_t& hdr)
{
    SM_PROLOGUE_RC(ss_m::update_rec_hdr, in_xct,read_write, 0);
    W_DO(_update_rec_hdr(rid, start, hdr));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::append_rec()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::append_rec(const rid_t& rid, const vec_t& data)
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "append_rec " << rid;
        W_DO(log_comment(s.c_str()));
    }
#endif
    SM_PROLOGUE_RC(ss_m::append_rec, in_xct,read_write, 0);
    W_DO(_append_rec(rid, data));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::truncate_rec()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::truncate_rec(const rid_t& rid, smsize_t amount, bool &should_forward)
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "truncate_rec " << rid;
        W_DO(log_comment(s.c_str()));
    }
#endif
    SM_PROLOGUE_RC(ss_m::truncate_rec, in_xct,read_write, 0);
    W_DO(_truncate_rec(rid, amount, should_forward));
    if (should_forward) {
        // The record is still implemented as large, even though
        // it could fit on a page (though not on this one).
        // It's possible to forward it, but that only seems useful
        // in the context of logical IDs.
    }
    return RCOK;
}

rc_t
ss_m::truncate_rec(const rid_t& rid, smsize_t amount)
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "truncate_rec " << rid;
        W_DO(log_comment(s.c_str()));
    }
#endif
    W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
    bool should_forward;
    W_DO(fi->truncate_rec(rid, amount, should_forward));
    return RCOK;
}


rc_t
ss_m::lvid_to_vid(const lvid_t& lvid, vid_t& vid)
{
    SM_PROLOGUE_RC(ss_m::lvid_to_vid, can_be_in_xct,read_only, 0);
    vid = io->get_vid(lvid);
    if (vid == vid_t::null) return RC(eBADVOL);
    return RCOK;
}

rc_t
ss_m::vid_to_lvid(vid_t vid, lvid_t& lvid)
{
    SM_PROLOGUE_RC(ss_m::lvid_to_vid, can_be_in_xct,read_only, 0);
    lvid = io->get_lvid(vid);
    if (lvid == lvid_t::null) return RC(eBADVOL);
    return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::create_mrbt_file()                                    *
 *--------------------------------------------------------------*/
rc_t
ss_m::create_mrbt_file(
    vid_t                          vid, 
    stid_t&                        fid, 
    store_property_t               property,
    shpid_t                        cluster_hint // = 0
)
{
#if FILE_LOG_COMMENT_ON
    W_DO(log_comment("create_mrbt_file"));
#endif
    SM_PROLOGUE_RC(ss_m::create_mrbt_file, in_xct, read_write, 0);
    DBGTHRD(<<"create_mrbt_file " <<vid << " " << property );
    W_DO(_create_mrbt_file(vid, fid, property, cluster_hint));
    DBGTHRD(<<"create_mrbt_file returns " << fid);
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::destroy_mrbt_file()                                   *
 *--------------------------------------------------------------*/
rc_t
ss_m::destroy_mrbt_file(const stid_t& fid)
{
    SM_PROLOGUE_RC(ss_m::destroy_mrbt_file, in_xct,read_write, 0);
    DBGTHRD(<<"destroy_mrbt_file " <<fid);
    W_DO(_destroy_file(fid));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::create_mrbt_rec()                                     *
 *--------------------------------------------------------------*/
rc_t
ss_m::create_mrbt_rec(const stid_t& fid, const vec_t& hdr,
		      smsize_t len_hint, const vec_t& data, rid_t& new_rid
#ifdef SM_DORA
		      , const bool bIgnoreLocks
#endif
                 )
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "create_mrbt_rec " << fid;
        W_DO(log_comment(s.c_str()));
    }
#endif
    SM_PROLOGUE_RC(ss_m::create_mrbt_rec, in_xct,read_write, 0);

    W_DO(_create_mrbt_rec(fid, hdr, len_hint, data, new_rid
#ifdef SM_DORA
			  , bIgnoreLocks
#endif
                     ));

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::create_mrbt_rec_in_page()                             *
 *--------------------------------------------------------------*/
rc_t
ss_m::create_mrbt_rec_in_page(const stid_t& fid, file_p& page, const vec_t& hdr,
			      smsize_t len_hint, const vec_t& data, rid_t& new_rid, bool& space_found,
			      const bool bIgnoreLocks, const bool bIgnoreLatches)
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "create_mrbt_rec_in_page: file=" << fid << " page=" << page.pid() ;
        W_DO(log_comment(s.c_str()));
    }
#endif
    SM_PROLOGUE_RC(ss_m::create_mrbt_rec_in_page, in_xct,read_write, 0);

    W_DO(_create_mrbt_rec_in_page(fid, page, hdr, len_hint, data, new_rid, space_found,
				  bIgnoreLocks, bIgnoreLatches));

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::create_mrbt_rec_in_page()                             *
 *--------------------------------------------------------------*/
rc_t
ss_m::find_page_and_create_mrbt_rec(const stid_t& fid, const lpid_t& leaf, const vec_t& hdr,
				    smsize_t len_hint, const vec_t& data, rid_t& new_rid,
				    const bool bIgnoreLocks, const bool bIgnoreLatches)
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "find_page_and_create_mrbt_rec: file=" << fid ;
        W_DO(log_comment(s.c_str()));
    }
#endif
    SM_PROLOGUE_RC(ss_m::find_page_and_create_mrbt_rec, in_xct,read_write, 0);
    
    W_DO(_find_page_and_create_mrbt_rec(fid, leaf, hdr, len_hint, data, new_rid,
					bIgnoreLocks, bIgnoreLatches));

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::destroy_mrbt_rec()                                    *
 *--------------------------------------------------------------*/
rc_t
ss_m::destroy_mrbt_rec(const rid_t& rid
#ifdef SM_DORA
		       , const bool bIgnoreLocks, const bool bIgnoreLatches
#endif
                  )
{
    SM_PROLOGUE_RC(ss_m::destroy_mrbt_rec, in_xct,read_write, 0);
    DBG(<<"destroy_mrbt_rec " <<rid);

    W_DO(_destroy_mrbt_rec(rid
#ifdef SM_DORA
                      , bIgnoreLocks, bIgnoreLatches
#endif
                      ));

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::update_mrbt_rec()                                     *
 *--------------------------------------------------------------*/
rc_t
ss_m::update_mrbt_rec(const rid_t& rid, smsize_t start, const vec_t& data,
		      const bool bIgnoreLocks, const bool bIgnoreLatches)
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "update_mrbt_rec " << rid;
        W_DO(log_comment(s.c_str()));
    }
#endif
    SM_PROLOGUE_RC(ss_m::update_mrbt_rec, in_xct,read_write, 0);
    W_DO(_update_mrbt_rec(rid, start, data, bIgnoreLocks, bIgnoreLatches));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::update_mrbt_rec_hdr()                                 *
 *--------------------------------------------------------------*/
rc_t
ss_m::update_mrbt_rec_hdr(const rid_t& rid, smsize_t start, const vec_t& hdr,
			  const bool bIgnoreLocks, const bool bIgnoreLatches)
{
    SM_PROLOGUE_RC(ss_m::update_mrbt_rec_hdr, in_xct,read_write, 0);
    W_DO(_update_mrbt_rec_hdr(rid, start, hdr, bIgnoreLocks, bIgnoreLatches));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::append_mrbt_rec()                                     *
 *--------------------------------------------------------------*/
rc_t
ss_m::append_mrbt_rec(const rid_t& rid, const vec_t& data,
		      const bool bIgnoreLocks, const bool bIgnoreLatches)
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "append_mrbt_rec " << rid;
        W_DO(log_comment(s.c_str()));
    }
#endif
    SM_PROLOGUE_RC(ss_m::append_mrbt_rec, in_xct,read_write, 0);
    W_DO(_append_mrbt_rec(rid, data, bIgnoreLocks, bIgnoreLatches));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::truncate_mrbt_rec()                                   *
 *--------------------------------------------------------------*/
rc_t
ss_m::truncate_mrbt_rec(const rid_t& rid, smsize_t amount, bool &should_forward,
			const bool /*bIgnoreLocks*/, const bool bIgnoreLatches)
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "truncate_mrbt_rec " << rid;
        W_DO(log_comment(s.c_str()));
    }
#endif
    SM_PROLOGUE_RC(ss_m::truncate_mrbt_rec, in_xct,read_write, 0);
    W_DO(_truncate_mrbt_rec(rid, amount, should_forward, bIgnoreLatches));
    if (should_forward) {
        // The record is still implemented as large, even though
        // it could fit on a page (though not on this one).
        // It's possible to forward it, but that only seems useful
        // in the context of logical IDs.
    }
    return RCOK;
}

rc_t
ss_m::truncate_mrbt_rec(const rid_t& rid, smsize_t amount,
			const bool bIgnoreLocks, const bool bIgnoreLatches)
{
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "truncate_mrbt_rec " << rid;
        W_DO(log_comment(s.c_str()));
    }
#endif
    if(!bIgnoreLocks) {
	W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
    }
    bool should_forward;
    W_DO(fi->truncate_mrbt_rec(rid, amount, should_forward, bIgnoreLatches));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_create_mrbt_file()                                   *
 *--------------------------------------------------------------*/
rc_t
ss_m::_create_mrbt_file(vid_t vid, stid_t& fid,
			store_property_t property,
			shpid_t        cluster_hint // = 0
			)
{
    FUNC(ss_m::_create_mrbt_file);
    DBG( << "Attempting to create a file on volume " << vid.vol );

    store_flag_t st_flag = _make_store_flag(property);
    extnum_t first_extent = extnum_t(cluster_hint? cluster_hint / ss_m::ext_sz : 0);

    DBGTHRD(<<"about to create a store starting about extent " << first_extent);
    W_DO( io->create_store(vid, 100/*unused*/, st_flag, fid, first_extent) );

    DBGTHRD(<<"created first store " << fid << " now create 2nd...");

    /*
    // create a store for holding large record pages 
    // always allocates 1 extent -- otherwise
    // asserts fail elsewhere
    // If this fails, we have to by-hand back out the creation
    // of the first store
    */
    stid_t lg_stid;
    w_rc_t rc= io->create_store(vid, 100/*unused*/, 
                st_flag, lg_stid, first_extent, 1);
    if(rc.is_error()) {
        // it would be a problem if this didn't work, but
        // if all else fails, abort should work.
        DBGTHRD(<<"2nd create failed; destroying first= " << fid);
        W_DO( io->destroy_store(fid) );
        return rc;
    }

    DBGTHRD(<<"created 2nd store (for lg recs) " << lg_stid);

    // RACE: theoretically, some other thread could use/destroy
    //       the above stores before the following lock request
    //       is granted.  The only forseable way for this to
    //       happen would be due to a bug in a vas causing
    //       it to destroy the wrong store.  We make no attempt
    //       to prevent this.
    W_DO(lm->lock(fid, EX, t_long, WAIT_SPECIFIED_BY_XCT));

    DBGTHRD(<<"locked " << fid);

    lpid_t first;
    //W_DO( fi->create_mrbt(fid, first) );
    //DBGTHRD(<<"locked &created -- put in store directory: " << fid);

    sinfo_s sinfo(fid.store, t_file, 100/*unused*/, 
           t_bad_ndx_t, t_cc_none/*not used*/, first.page, 0, 0);
    sinfo.set_large_store(lg_stid.store);
    W_DO( dir->insert(fid, sinfo) );

    DBGTHRD(<<"inserted " << fid.store);

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_create_mrbt_rec()                                    *
 *--------------------------------------------------------------*/
rc_t
ss_m::_create_mrbt_rec(const stid_t& fid, const vec_t& hdr, smsize_t len_hint, 
		       const vec_t& data, rid_t& new_rid
#ifdef SM_DORA
		       , const bool bIgnoreLocks
#endif
		       )
{
    FUNC(ss_m::_create_mrbt_rec);
    sdesc_t* sd;

    lock_mode_t lmode = IX;
#ifdef SM_DORA
    if (bIgnoreLocks) lmode = NL;
#endif

    W_DO( dir->access(fid, sd, lmode) );

    DBG( << "create in fid " << fid << " data.size " << data.size());

    W_DO( fi->create_mrbt_rec(fid, len_hint, hdr, data, *sd, new_rid
#ifdef SM_DORA
			      , bIgnoreLocks
#endif
			      ) );

    // NOTE: new_rid need not be locked, since lock escalation
    // or explicit file/page lock might obviate it.

    //cout << "sm create_rec " << new_rid << " size(hdr, data) " << hdr.size() <<  " " << data.size() << endl;

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_create_mrbt_rec_in_page()                            *
 *--------------------------------------------------------------*/
rc_t
ss_m::_create_mrbt_rec_in_page(const stid_t& fid, file_p& page, const vec_t& hdr, smsize_t len_hint, 
			       const vec_t& data, rid_t& new_rid, bool& space_found,
			       const bool bIgnoreLocks, const bool bIgnoreLatches)
{
    FUNC(ss_m::_create_mrbt_rec_in_page);
    sdesc_t* sd;

    lock_mode_t lmode = IX;
    if (bIgnoreLocks) lmode = NL;

    W_DO( dir->access(fid, sd, lmode) );

    DBG( << "create in fid " << fid << " in page " << page.pid() << " data.size " << data.size());

    W_DO( fi->create_mrbt_rec_in_given_page(len_hint, *sd, hdr, data, new_rid, page, space_found, bIgnoreLatches) );

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_find_page_and_create_mrbt_rec()                      *
 *--------------------------------------------------------------*/
rc_t
ss_m::_find_page_and_create_mrbt_rec(const stid_t& fid, const lpid_t& leaf, const vec_t& hdr,
				     smsize_t len_hint, const vec_t& data, rid_t& new_rid,
				     const bool bIgnoreLocks, const bool bIgnoreLatches)
{
    FUNC(ss_m::_find_page_and_create_mrbt_rec);
    sdesc_t* sd_heap;
    sdesc_t* sd_index;

    lock_mode_t lmode = IX;
    if (bIgnoreLocks) lmode = NL;

    W_DO( dir->access(fid, sd_heap, lmode) );
    W_DO( dir->access(leaf._stid, sd_index, lmode) );

    DBG( << "create in fid " << fid << " data.size " << data.size());

    if (sd_index->sinfo().stype != t_index)   return RC(eBADSTORETYPE);
    switch (sd_index->sinfo().ntype)  {
    case t_mrbtree_l:
    case t_uni_mrbtree_l:

	W_DO( fi->create_mrbt_rec_l(leaf, *sd_heap, hdr, data, len_hint, new_rid, bIgnoreLatches) );
	break;
	
    case t_mrbtree_p:
    case t_uni_mrbtree_p:
	 
	W_DO( fi->create_mrbt_rec_p(leaf, *sd_heap, hdr, data, len_hint, new_rid, bIgnoreLatches) );
	break;

    default:
        return RC(eBADNDXTYPE);
    }
    
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_destroy_mrbt_rec()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::_destroy_mrbt_rec(const rid_t& rid
#ifdef SM_DORA
                   , const bool bIgnoreLocks, const bool bIgnoreLatches
#endif
                   )
{
    DBG(<<"_destroy_rec " << rid);

    W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT
#ifdef SM_DORA
                  , 0, 0, 0, bIgnoreLocks // If set it will cause to bIgnoreParents
#endif
                  ));

    W_DO(fi->destroy_rec(rid
#ifdef SM_DORA
			 , bIgnoreLatches
#endif
			 ));
    //cout << "sm destroy_rec " << rid << endl;
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_update_mrbt_rec()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_update_mrbt_rec(const rid_t& rid, smsize_t start, const vec_t& data,
		       const bool bIgnoreLocks, const bool bIgnoreLatches)
{
    if (!bIgnoreLocks) W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
    W_DO(fi->update_rec(rid, start, data, bIgnoreLatches));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_update_mrbt_rec_hdr()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::_update_mrbt_rec_hdr(const rid_t& rid, smsize_t start, const vec_t& hdr,
			   const bool bIgnoreLocks, const bool bIgnoreLatches)
{
    if (!bIgnoreLocks) {	
        W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
    }
    W_DO(fi->splice_hdr(rid, u4i(start), hdr.size(), hdr, bIgnoreLatches));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_append_mrbt_rec()                                    *
 *--------------------------------------------------------------*/
rc_t
ss_m::_append_mrbt_rec(const rid_t& rid, const vec_t& data,
		       const bool bIgnoreLocks, const bool bIgnoreLatches)
{
    sdesc_t* sd;
    lock_mode_t lmode = IX;
    if (bIgnoreLocks) lmode = NL;
    W_DO( dir->access(rid.stid(), sd, lmode) );
    if (!bIgnoreLocks) {	
        W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
    }
    W_DO(fi->append_mrbt_rec(rid, data, *sd, bIgnoreLatches));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_truncate_mrbt_rec()                                  *
 *--------------------------------------------------------------*/
rc_t
ss_m::_truncate_mrbt_rec(const rid_t& rid, smsize_t amount, bool& should_forward,
			 const bool bIgnoreLocks, const bool bIgnoreLatches)
{
    if (!bIgnoreLocks) {	
        W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
    }
    W_DO(fi->truncate_mrbt_rec(rid, amount, should_forward, bIgnoreLatches));
    return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::_set_store_property()                                 *
 *--------------------------------------------------------------*/
rc_t
ss_m::_set_store_property(
    stid_t              stid,
    store_property_t        property)
{

    /*
     * Can't change to a load file. (You can create it as
     * a load file.)
     */
    if (property & st_load_file)  {
        return RC(eBADSTOREFLAGS);
    }

    /*
     * can't change to a t_temporary file. You can change
     * to an insert file, which combines with st_tmp (in that
     * page fix for a page of st_insert_file get the st_tmp
     * OR-ed in to the page's store flags).
     */
    store_flag_t newflags = _make_store_flag(property);
    if (newflags == st_tmp)  {
        return RC(eBADSTOREFLAGS);
    }

    /*
     * find out the current property
     */
    store_flag_t oldflags = st_bad;

    W_DO( io->get_store_flags(stid, oldflags) );

    if (oldflags == newflags)  {
        return RCOK;
    }


    sdesc_t* sd;
    W_DO( dir->access(stid, sd, EX) );  // also locks the store in EX

    if (newflags == st_regular)  {
        /*
         *  Set the io store flags for both stores and discard
         *  the pages from the buffer pool.
         *
         *  Set the flags BEFORE forcing  the stores 
         *  to handle this case: there's another thread
         *  in this xct (the lock doesn't control concurrency
         *  in this case); the other thread is reading pages in this 
         *  file.
         *  A page is read back in immediately after we forced it,
         *  and its page flags are set when the read is done. 
         *
         */
        W_DO( io->set_store_flags(stid, newflags) );

        if (sd->large_stid())  {
            W_DO( io->set_store_flags(sd->large_stid(), newflags) ) ;
        }
    }  else if (newflags == st_insert_file)  {
        // only allow the changing for a regular file, not indices
        if (sd->sinfo().stype != t_file)  {
            return RC(eBADSTOREFLAGS);
        }

        W_DO( io->set_store_flags(stid, newflags) );

        if (sd->large_stid())  {
            W_DO( io->set_store_flags(sd->large_stid(), newflags) );
        }
    }  else  {
        W_FATAL(eINTERNAL);
    }

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_get_store_property()                                 *
 *--------------------------------------------------------------*/
rc_t
ss_m::_get_store_property(
    stid_t              stid,
    store_property_t&   property)
{
    store_flag_t flags = st_bad;
    W_DO( io->get_store_flags(stid, flags) );

    if (flags & st_regular) {
        w_assert2((flags & (st_tmp|st_load_file|st_insert_file)) == 0);
        property = t_regular;
        return RCOK;
    }
    if (flags & st_load_file) {
        // io_m::create_store stuffs the st_tmp flags in with
        // the st_load_file flags when the store is created.
        // It gets converted on commit to st_regular.
        w_assert2((flags & (st_insert_file|st_regular)) == 0);
        w_assert2((flags & st_tmp) == st_tmp);
        property = t_load_file;
        return RCOK;
    }
    
    if (flags & st_insert_file) {
        // Files can't be created as st_insert_file, but they
        // can get changed to this. 
        // They get converted on commit to st_regular.
        // The page fix causes the st_tmp flag to be OR-ed in
        // for the page's store flags.
        // Why these are handled differently, I don't know.
        w_assert2((flags & (st_load_file|st_regular)) == 0);
        w_assert2((flags & st_tmp) == 0);
        property = t_insert_file;
        return RCOK;
    }

    if (flags & st_tmp)  {
        property = t_temporary;
    } else {
        W_FATAL(eINTERNAL);
    }

    return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::_create_file()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::_create_file(vid_t vid, stid_t& fid,
                   store_property_t property,
                   shpid_t        cluster_hint // = 0
                   )
{
    FUNC(ss_m::_create_file);
    DBG( << "Attempting to create a file on volume " << vid.vol );

    store_flag_t st_flag = _make_store_flag(property);
    extnum_t first_extent = extnum_t(cluster_hint? cluster_hint / ss_m::ext_sz : 0);

    DBGTHRD(<<"about to create a store starting about extent " << first_extent);
    W_DO( io->create_store(vid, 100/*unused*/, st_flag, fid, first_extent) );

    DBGTHRD(<<"created first store " << fid << " now create 2nd...");

    /*
    // create a store for holding large record pages 
    // always allocates 1 extent -- otherwise
    // asserts fail elsewhere
    // If this fails, we have to by-hand back out the creation
    // of the first store
    */
    stid_t lg_stid;
    w_rc_t rc= io->create_store(vid, 100/*unused*/, 
                st_flag, lg_stid, first_extent, 1);
    if(rc.is_error()) {
        // it would be a problem if this didn't work, but
        // if all else fails, abort should work.
        DBGTHRD(<<"2nd create failed; destroying first= " << fid);
        W_DO( io->destroy_store(fid) );
        return rc;
    }

    DBGTHRD(<<"created 2nd store (for lg recs) " << lg_stid);

    // RACE: theoretically, some other thread could use/destroy
    //       the above stores before the following lock request
    //       is granted.  The only forseable way for this to
    //       happen would be due to a bug in a vas causing
    //       it to destroy the wrong store.  We make no attempt
    //       to prevent this.
    W_DO(lm->lock(fid, EX, t_long, WAIT_SPECIFIED_BY_XCT));

    DBGTHRD(<<"locked " << fid);

    lpid_t first;
    W_DO( fi->create(fid, first) );
    DBGTHRD(<<"locked &created -- put in store directory: " << fid);

    sinfo_s sinfo(fid.store, t_file, 100/*unused*/, 
           t_bad_ndx_t, t_cc_none/*not used*/, first.page, 0, 0);
    sinfo.set_large_store(lg_stid.store);
    W_DO( dir->insert(fid, sinfo) );

    DBGTHRD(<<"inserted " << fid.store);

    return RCOK;
}


#include "histo.h"

/*--------------------------------------------------------------*
 *  ss_m::_destroy_file()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::_destroy_file(const stid_t& fid)
{
    FUNC(ss_m::_destroy_file);
#if FILE_LOG_COMMENT_ON
    {
        w_ostrstream s;
        s << "_destroy_file " << fid;
        W_DO(log_comment(s.c_str()));
    }
#endif

    sdesc_t* sd;

    DBGTHRD(<<"want to destroy store " << fid);
    W_DO( dir->access(fid, sd, EX) );

    if (sd->sinfo().stype != t_file) return RC(eBADSTORETYPE);

    store_flag_t store_flags;
    W_DO( io->get_store_flags(fid, store_flags) );
   
    DBGTHRD(<<"destroying store " << fid << " store_flags " << store_flags);
    W_DO( io->destroy_store(fid) );
    /* 
     * small-record file page utilization might be cached -
     * so clean up...
     */
    histoid_t::destroyed_store(fid, sd);

    DBGTHRD(<<"destroying store " << sd->large_stid());
    // destroy the store containing large record pages
    W_DO( io->destroy_store(sd->large_stid()) );

    if (store_flags & st_tmp)  { // for temporary
            // we can discard the buffers to save
            // some disk I/O.
            W_IGNORE( bf->discard_store(fid) );
            W_IGNORE( bf->discard_store(sd->large_stid()) );
    }
    
    DBGTHRD(<<"about to remove directory entry " << fid);
    W_DO( dir->remove(fid) );
    DBGTHRD(<<"removed directory entry " << fid);
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_destroy_n_swap_file()                                *
 *      destroy the old file and shift the large object store        *
 *         to new file.                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_destroy_n_swap_file(const stid_t& old_fid, const stid_t& new_fid)
{
    sdesc_t *sd1, *sd2;

    W_DO( dir->access(old_fid, sd1, EX) );
    if (sd1->sinfo().stype != t_file) return RC(eBADSTORETYPE);

    W_DO( dir->access(new_fid, sd2, EX) );
    if (sd2->sinfo().stype != t_file) return RC(eBADSTORETYPE);

    store_flag_t old_store_flags = st_bad;
    store_flag_t new_store_flags = st_bad;
    W_DO( io->get_store_flags(old_fid, old_store_flags) );
    W_DO( io->get_store_flags(new_fid, new_store_flags) );
   
    // destroy the old
    W_DO( io->destroy_store(old_fid) );
    histoid_t::destroyed_store(old_fid, sd1);

    // destroy the store containing large record pages for new file
    W_DO( io->destroy_store(sd2->large_stid()) );

    if (old_store_flags & st_tmp)  { // for temporary and no-log files, 
                // we can discard the buffers to save
                // some disk I/O.
                W_IGNORE( bf->discard_store(old_fid) );
    }
    
    if (new_store_flags & st_tmp)  { // for temporary and no-log  files, 
                // we can discard the buffers to save
                // some disk I/O.
                W_IGNORE( bf->discard_store(sd2->large_stid()) );
    }

    // do the actual swap in directory
    W_DO( dir->remove_n_swap(old_fid, new_fid) );

    return RCOK;
}


/*--------------------------------------------------------------*
 *  ss_m::_create_rec()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_create_rec(const stid_t& fid, const vec_t& hdr, smsize_t len_hint, 
                  const vec_t& data, rid_t& new_rid
#ifdef SM_DORA
                  , const bool bIgnoreLocks
#endif
                 )
{
    FUNC(ss_m::_create_rec);
    sdesc_t* sd;

    lock_mode_t lmode = IX;
#ifdef SM_DORA
    if (bIgnoreLocks) lmode = NL;
#endif

    W_DO( dir->access(fid, sd, lmode) );

    DBG( << "create in fid " << fid << " data.size " << data.size());

    W_DO( fi->create_rec(fid, len_hint, hdr, data, *sd, new_rid
#ifdef SM_DORA
                         , bIgnoreLocks
#endif
                         ) );

    // NOTE: new_rid need not be locked, since lock escalation
    // or explicit file/page lock might obviate it.

    //cout << "sm create_rec " << new_rid << " size(hdr, data) " << hdr.size() <<  " " << data.size() << endl;

    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_destroy_rec()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::_destroy_rec(const rid_t& rid
#ifdef SM_DORA
                   , const bool bIgnoreLocks
#endif
                   )
{
    DBG(<<"_destroy_rec " << rid);

    W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT
#ifdef SM_DORA
                  , 0, 0, 0, bIgnoreLocks // If set it will cause to bIgnoreParents
#endif
                  ));

    W_DO(fi->destroy_rec(rid));
    //cout << "sm destroy_rec " << rid << endl;
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_update_rec()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_update_rec(const rid_t& rid, smsize_t start, const vec_t& data
#ifdef SM_DORA
                  , const bool bIgnoreLocks
#endif
                  )
{
#ifdef SM_DORA
    if (!bIgnoreLocks) W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
    W_DO(fi->update_rec(rid, start, data, bIgnoreLocks));
#else
    W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
    W_DO(fi->update_rec(rid, start, data));
#endif
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_update_rec_hdr()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::_update_rec_hdr(const rid_t& rid, smsize_t start, const vec_t& hdr
#ifdef SM_DORA
                      , const bool bIgnoreLocks
#endif
                      )
{
#ifdef SM_DORA
    if (!bIgnoreLocks) {
#endif
        W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
#ifdef SM_DORA
    }
#endif

    W_DO(fi->splice_hdr(rid, u4i(start), hdr.size(), hdr));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_append_rec()                                                *
 *--------------------------------------------------------------*/
rc_t
ss_m::_append_rec(const rid_t& rid, const vec_t& data)
{
    sdesc_t* sd;
    W_DO( dir->access(rid.stid(), sd, IX) );
    //cout << "sm append_rec " << rid << " size " << data.size() << endl;

    W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
    W_DO(fi->append_rec(rid, data, *sd));
    return RCOK;
}

/*--------------------------------------------------------------*
 *  ss_m::_truncate_rec()                                        *
 *--------------------------------------------------------------*/
rc_t
ss_m::_truncate_rec(const rid_t& rid, smsize_t amount, bool& should_forward)
{
    W_DO(lm->lock(rid, EX, t_long, WAIT_SPECIFIED_BY_XCT));
    W_DO(fi->truncate_rec(rid, amount, should_forward));
    return RCOK;
}


