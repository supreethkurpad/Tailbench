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

 $Id: file.cpp,v 1.201 2010/06/15 17:30:07 nhall Exp $

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

#ifdef __GNUG__
#pragma implementation "file.h"
#pragma implementation "file_s.h"
#endif

#include "sm_int_2.h"
//
#include "btree_p.h"
#include <set>
//
#define FILE_C
#include "lgrec.h"
#include "w_minmax.h"
#include "sm_du_stats.h"

#include "histo.h"
#include "crash.h"

#ifdef EXPLICIT_TEMPLATE
/* Used in sort.cpp, btree_bl.cpp */
template class w_auto_delete_array_t<file_p>;
#endif

lpid_t 
record_t::last_pid(const file_p& page) const
{
    lpid_t last;

#if W_DEBUG_LEVEL > 2
    // make sure record is on the page
    const char* check = (const char*)&page.persistent_part_const();
    w_assert3(check < (char*)this && (check+sizeof(page_s)) > (char*)this);
#endif 

    w_assert3(body_size() > 0);
    if (tag.flags & t_large_0) {
        const lg_tag_chunks_h lg_hdl(page, *(lg_tag_chunks_s*)body());
        last = lg_hdl.last_pid();
    } else if (tag.flags & (t_large_1 | t_large_2)) {
        const lg_tag_indirect_h lg_hdl(page, *(lg_tag_indirect_s*)body(), page_count());
        last = lg_hdl.last_pid();
    } else {
            W_FATAL(smlevel_0::eINTERNAL);
    }

    w_assert3(last.vol() > 0 && last.store() > 0 && last.page > 0);
    return last;
}

lpid_t 
record_t::pid_containing(uint4_t offset, uint4_t& start_byte, const file_p& page) const
{
#if W_DEBUG_LEVEL > 2
    // make sure record is on the page
    const char* check = (const char*)&page.persistent_part_const();
    w_assert3(check < (char*)this && (check+sizeof(page_s)) > (char*)this);
#endif 
    if(body_size() == 0) return lpid_t::null;

    if (tag.flags & t_large_0) {
        shpid_t page_num = offset / lgdata_p::data_sz;
        const lg_tag_chunks_h lg_hdl(page, *(lg_tag_chunks_s*)body());

        start_byte = page_num*lgdata_p::data_sz;
        return lg_hdl.pid(page_num);
    } else if (tag.flags & (t_large_1 | t_large_2)) {
        shpid_t page_num = offset / lgdata_p::data_sz;
        const lg_tag_indirect_h lg_hdl(page, *(lg_tag_indirect_s*)body(), page_count());

        start_byte = page_num*lgdata_p::data_sz;
        return lg_hdl.pid(page_num);
    }
    W_FATAL(smlevel_0::eNOTIMPLEMENTED);
    lpid_t dummy; // keep compiler quit
    return dummy;
}

uint4_t 
record_t::page_count() const
{
    return body_size() == 0 ? 0 : (body_size()-1) / lgdata_p::data_sz +1;
}

inline 
smsize_t 
file_m::_space_last_page(smsize_t rec_sz)
{
    return (lgdata_p::data_sz - rec_sz % lgdata_p::data_sz) %
            lgdata_p::data_sz;
}

inline 
smsize_t 
file_m::_bytes_last_page(smsize_t rec_sz)
{
    return rec_sz == 0 ? 0 :
                         lgdata_p::data_sz - _space_last_page(rec_sz);
}

file_m::file_m () 
{ 
    w_assert1(is_aligned(sizeof(rectag_t)));
    W_COERCE(histoid_t::initialize_table());
}

file_m::~file_m()   
{
    histoid_t::destroy_table();
}

rc_t 
file_m::create(stid_t stid, lpid_t& first_page)
{
    file_p  page;
    DBGTHRD(<<"file_m::create create first page in store " << stid);
    W_DO(_alloc_page( stid, 
        lpid_t::eof,  // hint bof/eof doesn't matter, but if eof,
                      // it will shorten the path a bit
        first_page,   // output
        page,
        true              // immaterial here
        ));
    // page.destructor() causes it to be unfixed
    w_assert3(page.is_fixed());

    histoid_update_t hu(page);
    hu.update();
    DBGTHRD(<<"file_m::create(d) first page is  " << first_page);
    return RCOK;
}


// same as create, except creates a file_mrbt_p as first page instead file_p 
rc_t 
file_m::create_mrbt(stid_t stid, lpid_t& first_page)
{
    file_mrbt_p  page;
    DBGTHRD(<<"file_m::create_mrbt create first page in store " << stid);
    W_DO(_alloc_mrbt_page( stid, 
        lpid_t::eof,  // hint bof/eof doesn't matter, but if eof,
                      // it will shorten the path a bit
        first_page,   // output
        page,
        true              // immaterial here
        ));
    // page.destructor() causes it to be unfixed
    w_assert3(page.is_fixed());

    histoid_update_t hu(page);
    hu.update();
    DBGTHRD(<<"file_m::create_mrbt(d) first page is  " << first_page);
    return RCOK;
}


/* NB: argument order is similar to old create_rec */
rc_t
file_m::create_rec(
    const stid_t&        fid,
    // no page hint
    smsize_t             len_hint,
    const vec_t&         hdr,
    const vec_t&         data,
    sdesc_t&             sd,
    rid_t&               rid // output
    // no forward_alloc
#ifdef SM_DORA
    , const bool bIgnoreParents
#endif
    )
{
    file_p        page;

    // compact stores become a bottleneck
    // and they make the parallel loading of the benchmark databases
    // unable to acquire EX latches, and those db loads don't
    // cope with this situation.
    uint4_t     policy = t_cache | /*t_compact |*/ t_append;

    DBG(<<"create_rec store " << fid);

    W_DO(_create_rec( fid, pg_policy_t(policy), 
                      len_hint, sd, hdr, data, 
                      rid, page
#ifdef SM_DORA
                      , bIgnoreParents
#endif
                      ));
    DBG(<<"create_rec created " << rid);
    return RCOK;
}


rc_t
file_m::create_mrbt_rec(
    const stid_t&        fid,
    // no page hint
    smsize_t             len_hint,
    const vec_t&         hdr,
    const vec_t&         data,
    sdesc_t&             sd,
    rid_t&               rid // output
    // no forward_alloc
#ifdef SM_DORA
    , const bool bIgnoreParents
#endif
    )
{
    file_mrbt_p        page;

    // compact stores become a bottleneck
    // and they make the parallel loading of the benchmark databases
    // unable to acquire EX latches, and those db loads don't
    // cope with this situation.
    uint4_t     policy = t_cache | /*t_compact |*/ t_append;

    DBG(<<"create_mrbt_rec store " << fid);

    W_DO(_create_mrbt_rec( fid, pg_policy_t(policy), 
			   len_hint, sd, hdr, data, 
			   rid, page
#ifdef SM_DORA
			   , bIgnoreParents
#endif
			   ));
    DBG(<<"create_mrbt_rec created " << rid);
    return RCOK;
}


/* NB: order of arguments is same as old create_rec_at_end */
rc_t 
file_m::create_rec_at_end(
        file_p&         page, // in-out -- caller might have it fixed
        uint4_t         len_hint,
        const vec_t&    hdr,
        const vec_t&    data,
        sdesc_t&        sd, 
        rid_t&          rid        // out
)
{
    FUNC(file_m::create_rec_at_end);

    if(hdr.size() > file_p::data_sz - sizeof(rectag_t)) {
        return RC(eRECWONTFIT);
    }
    stid_t        fid = sd.stid();

    DBG(<<"create_rec_at_end store " << fid
            << " page is fixed: " << page.is_fixed()
            );
    W_DO(_create_rec(fid, t_append, len_hint,
        sd, hdr, data, rid, page));
    DBG(<<"create_rec_at_end created " << rid);
    return RCOK;
}


rc_t 
file_m::create_mrbt_rec_at_end(
        file_mrbt_p&         page, // in-out -- caller might have it fixed
        uint4_t         len_hint,
        const vec_t&    hdr,
        const vec_t&    data,
        sdesc_t&        sd, 
        rid_t&          rid        // out
)
{
    FUNC(file_m::create_rec_at_end);

    if(hdr.size() > file_mrbt_p::data_sz - sizeof(rectag_t)) {
        return RC(eRECWONTFIT);
    }
    stid_t        fid = sd.stid();

    DBG(<<"create_mrbt_rec_at_end store " << fid
            << " page is fixed: " << page.is_fixed()
            );
    W_DO(_create_mrbt_rec(fid, t_append, len_hint,
        sd, hdr, data, rid, page));
    DBG(<<"create_mrbt_rec_at_end created " << rid);
    return RCOK;
}


/*
 * Called from create_rec_at_end and from create_rec
 */
rc_t
file_m::_create_rec(
    const stid_t&         fid,
    pg_policy_t           policy,
    smsize_t              len_hint,
    sdesc_t&              sd,
    const vec_t&          hdr,
    const vec_t&          data,
    rid_t&                rid,
    file_p&               page        // in-output
#ifdef SM_DORA
    , const bool          bIgnoreParents
#endif
)
{
    smsize_t        space_needed;
    recflags_t      rec_impl; 

    {
        /*
         * compute space needed, record implementation
         */
        w_assert3(fid == sd.stid());
        smsize_t est_data_len = MAX((uint4_t)data.size(), len_hint);
        rec_impl = file_p::choose_rec_implementation( hdr.size(), 
                                                  est_data_len,
                                                  space_needed);
    }
    DBG(<<"create_rec with policy " << int(policy)
        << " space_needed=" << space_needed
        << " rec_impl=" << int(rec_impl)
        << " page is fixed=" << page.is_fixed()
        );

    bool         have_page = false;

    //
    // First time through from append_file_i this isn't true:
    // if(policy == t_append) {
        // w_assert3(page.is_fixed());
    // }


    { // open scope for hu
        slotid_t        slot = 0;

        DBGTHRD(<<"About to copy sd");
        histoid_update_t hu(&sd);

        if(page.is_fixed()) {
            w_assert2(policy == t_append);
            w_assert2(page.latch_mode() == LATCH_EX);

            rc_t rc = page.find_and_lock_next_slot(space_needed, slot);

            if(rc.is_error()) {
                page.unfix();
                DBG(<<"rc=" << rc);
                if (rc.err_num() != eRECWONTFIT) {
                    // error we can't handle
                    return RC_AUGMENT(rc);
                }
                w_assert3(!page.is_fixed());
                w_assert3(!have_page);
            } else {
                DBG(<<"acquired slot " << slot);
                have_page = true;
                w_assert2(page.is_fixed());
                hu.replace_page(&page);
#if W_DEBUG_LEVEL > 2
                {
                    w_assert3(page.latch_mode() == LATCH_EX);
                    space_bucket_t b = page.bucket();
                    if((page.page_bucket_info.old() != b) && 
                        page.page_bucket_info.initialized()) {
                        W_FATAL_MSG(fcINTERNAL, << "ah ha!");
                    }
                }
#endif
            }
        } 

        if(!have_page) {
            W_DO(_find_slotted_page_with_space(fid, policy, sd, 
                                               space_needed, page, slot
#ifdef SM_DORA
                                               , bIgnoreParents
#endif
                                               ));
            hu.replace_page(&page);
        }

#if W_DEBUG_LEVEL > 2
        {
            space_bucket_t b = page.bucket();
            if((page.page_bucket_info.old() != b) && 
                page.page_bucket_info.initialized()) {
                    W_FATAL_MSG(fcINTERNAL, << "ah ha!");
            }
        }
#endif

        // split into 2 parts so we don't hog the histoid, and
        // so we don't run into double-acquiring it in append_rec.

        w_assert2(page.is_fixed() && page.latch_mode() == LATCH_EX);

        W_DO(_create_rec_in_slot(page, slot, rec_impl, 
            hdr, data, sd, false,
            rid));

        w_assert2(page.is_fixed() && page.latch_mode() == LATCH_EX);

        hu.update();
    } // close scope for hu

    if(rec_impl == t_large_0) {
        W_DO(append_rec(rid, data, sd));
    }
    return RCOK;
}


// here EX latches are kept because create_rec is called before
// index related stuff
rc_t
file_m::_create_mrbt_rec(
    const stid_t&         fid,
    pg_policy_t           policy,
    smsize_t              len_hint,
    sdesc_t&              sd,
    const vec_t&          hdr,
    const vec_t&          data,
    rid_t&                rid,
    file_mrbt_p&               page        // in-output
#ifdef SM_DORA
    , const bool          bIgnoreParents
#endif
)
{
    smsize_t        space_needed;
    recflags_t      rec_impl; 

    {
        /*
         * compute space needed, record implementation
         */
        w_assert3(fid == sd.stid());
        smsize_t est_data_len = MAX((uint4_t)data.size(), len_hint);
        rec_impl = file_mrbt_p::choose_rec_implementation( hdr.size(), 
							   est_data_len,
							   space_needed);
    }
    DBG(<<"create_rec with policy " << int(policy)
        << " space_needed=" << space_needed
        << " rec_impl=" << int(rec_impl)
        << " page is fixed=" << page.is_fixed()
        );

    bool         have_page = false;

    //
    // First time through from append_file_i this isn't true:
    // if(policy == t_append) {
        // w_assert3(page.is_fixed());
    // }


    { // open scope for hu
        slotid_t        slot = 0;

        DBGTHRD(<<"About to copy sd");
        histoid_update_t hu(&sd);

        if(page.is_fixed()) {
            w_assert2(policy == t_append);
            w_assert2(page.latch_mode() == LATCH_EX);

            rc_t rc = page.find_and_lock_next_slot(space_needed, slot);

            if(rc.is_error()) {
                page.unfix();
                DBG(<<"rc=" << rc);
                if (rc.err_num() != eRECWONTFIT) {
                    // error we can't handle
                    return RC_AUGMENT(rc);
                }
                w_assert3(!page.is_fixed());
                w_assert3(!have_page);
            } else {
                DBG(<<"acquired slot " << slot);
                have_page = true;
                w_assert2(page.is_fixed());
                hu.replace_page(&page);
#if W_DEBUG_LEVEL > 2
                {
                    w_assert3(page.latch_mode() == LATCH_EX);
                    space_bucket_t b = page.bucket();
                    if((page.page_bucket_info.old() != b) && 
                        page.page_bucket_info.initialized()) {
                        W_FATAL_MSG(fcINTERNAL, << "ah ha!");
                    }
                }
#endif
            }
        } 

        if(!have_page) {
            W_DO(_find_slotted_mrbt_page_with_space(fid, policy, sd, 
						    space_needed, page, slot
#ifdef SM_DORA
						    , bIgnoreParents
#endif
						    ));
            hu.replace_page(&page);
        }

#if W_DEBUG_LEVEL > 2
        {
            space_bucket_t b = page.bucket();
            if((page.page_bucket_info.old() != b) && 
                page.page_bucket_info.initialized()) {
                    W_FATAL_MSG(fcINTERNAL, << "ah ha!");
            }
        }
#endif

        // split into 2 parts so we don't hog the histoid, and
        // so we don't run into double-acquiring it in append_rec.

        w_assert2(page.is_fixed() && page.latch_mode() == LATCH_EX);

        W_DO(_create_rec_in_slot(page, slot, rec_impl, 
            hdr, data, sd, false,
            rid));

        w_assert2(page.is_fixed() && page.latch_mode() == LATCH_EX);

        hu.update();
    } // close scope for hu

    if(rec_impl == t_large_0) {
        W_DO(append_mrbt_rec(rid, data, sd));
    }
    return RCOK;
}

rc_t
file_m::move_mrbt_rec_to_given_page(
    smsize_t              len_hint,
    const vec_t&          hdr,
    const vec_t&          data,
    rid_t&                rid, // output
    file_p&          page,
    bool&                 space_found,
    const bool            bIgnoreLatches
)
{
    smsize_t        space_needed;
    recflags_t      MAYBE_UNUSED rec_impl; 
    space_found = true;
    {
        /*
         * compute space needed, record implementation
         */
        smsize_t est_data_len = MAX((uint4_t)data.size(), len_hint);
        rec_impl = file_mrbt_p::choose_rec_implementation( hdr.size(), 
							   est_data_len,
							   space_needed);
    }
    DBG(<< "create_rec "
        << " space_needed=" << space_needed
        << " rec_impl=" << int(rec_impl)
        << " page is fixed=" << page.is_fixed()
        );

    { // open scope for hu
        slotid_t        slot = 0;

        histoid_update_t hu(page);

        if(page.is_fixed()) {
	    w_assert2(bIgnoreLatches || page.latch_mode() == LATCH_EX);
	    
            rc_t rc = page.find_and_lock_next_slot(space_needed, slot);

            if(rc.is_error()) {
                page.unfix();
                DBG(<<"rc=" << rc);
                if (rc.err_num() != eRECWONTFIT) {
                    // error we can't handle
                    return RC_AUGMENT(rc);
                }
                w_assert3(!page.is_fixed());
		space_found = false;
		return RCOK;
            } else {
                DBG(<<"acquired slot " << slot);
		w_assert2(page.is_fixed());
                hu.replace_page(&page); // ???
#if W_DEBUG_LEVEL > 2
                {
		    w_assert3(bIgnoreLatches || page.latch_mode() == LATCH_EX);
                    space_bucket_t b = page.bucket();
                    if((page.page_bucket_info.old() != b) && 
                        page.page_bucket_info.initialized()) {
                        W_FATAL_MSG(fcINTERNAL, << "ah ha!");
                    }
                }
#endif
            }
        } 


        // split into 2 parts so we don't hog the histoid, and
        // so we don't run into double-acquiring it in append_rec.

	w_assert2(page.is_fixed() && (bIgnoreLatches || page.latch_mode() == LATCH_EX));

        W_DO(_create_mrbt_rec_in_slot(page, slot, hdr, data, rid, bIgnoreLatches));

        w_assert2(page.is_fixed() && (bIgnoreLatches || page.latch_mode() == LATCH_EX));

        hu.update();
    } // close scope for hu

    return RCOK;
}

rc_t
file_m::create_mrbt_rec_in_given_page(
    smsize_t              len_hint,
    sdesc_t&              sd,
    const vec_t&          hdr,
    const vec_t&          data,
    rid_t&                rid, // output
    file_p&          page,
    bool&                 space_found,
    const bool            bIgnoreLatches
)
{
    smsize_t        space_needed;
    recflags_t      rec_impl; 
    space_found = true;
    {
        /*
         * compute space needed, record implementation
         */
        smsize_t est_data_len = MAX((uint4_t)data.size(), len_hint);
        rec_impl = file_mrbt_p::choose_rec_implementation( hdr.size(), 
							   est_data_len,
							   space_needed);
    }
    DBG(<<"create_rec "
        << " space_needed=" << space_needed
        << " rec_impl=" << int(rec_impl)
        << " page is fixed=" << page.is_fixed()
        );

    { // open scope for hu
        slotid_t        slot = 0;

        histoid_update_t hu(&sd);

        if(page.is_fixed()) {
	    w_assert2(bIgnoreLatches || page.latch_mode() == LATCH_EX);
	    
            rc_t rc = page.find_and_lock_next_slot(space_needed, slot);

            if(rc.is_error()) {
                page.unfix();
                DBG(<<"rc=" << rc);
                if (rc.err_num() != eRECWONTFIT) {
                    // error we can't handle
                    return RC_AUGMENT(rc);
                }
                w_assert3(!page.is_fixed());
		space_found = false;
		return RCOK;
            } else {
                DBG(<<"acquired slot " << slot);
		w_assert2(page.is_fixed());
                hu.replace_page(&page); // ???
#if W_DEBUG_LEVEL > 2
                {
		    w_assert3(bIgnoreLatches || page.latch_mode() == LATCH_EX);
                    space_bucket_t b = page.bucket();
                    if((page.page_bucket_info.old() != b) && 
                        page.page_bucket_info.initialized()) {
                        W_FATAL_MSG(fcINTERNAL, << "ah ha!");
                    }
                }
#endif
            }
        } 


        // split into 2 parts so we don't hog the histoid, and
        // so we don't run into double-acquiring it in append_rec.

	w_assert2(page.is_fixed() && (bIgnoreLatches || page.latch_mode() == LATCH_EX));

        W_DO(_create_rec_in_slot(page, slot, rec_impl, hdr, data, sd, false, rid, bIgnoreLatches));

        w_assert2(page.is_fixed() && (bIgnoreLatches || page.latch_mode() == LATCH_EX));

        hu.update();
    } // close scope for hu

    if(rec_impl == t_large_0) {
        W_DO(append_mrbt_rec(rid, data, sd, bIgnoreLatches));
    }

    return RCOK;
}

rc_t
file_m::destroy_rec_slot(const rid_t rid, file_mrbt_p& page, const bool bIgnoreLatches)
{

    DBGTHRD(<<"destroy_rec_slot");

    /*
     * Find or create a histoid for this store.
     */
    w_assert2(page.is_fixed());
    w_assert2(bIgnoreLatches || page.is_latched_by_me());
    w_assert2(bIgnoreLatches || page.is_mine());

    W_DO( page.destroy_rec(rid.slot) ); // does a page_mark for the slot

    if (page.rec_count() == 0) {
        DBG(<<"Now free page");
        w_assert2(page.is_fixed());
        W_DO(_free_page(page, bIgnoreLatches));
	INC_TSTAT(page_file_mrbt_dealloc);
        return RCOK;
    }

    DBG(<<"Update page utilization");
    /*
     *  Update the page's utilization info in the
     *  cache.
     *  (page_p::unfix updates the extent's histogram info)
     */
    histoid_update_t hu(page);
    hu.update();
    return RCOK;
}

rc_t
file_m::_create_mrbt_rec_in_slot(
    file_p    &page,
    slotid_t       slot,
    const vec_t&   hdr,
    const vec_t&   data,
    rid_t&        rid, // out
    const bool    MAYBE_UNUSED bIgnoreLatches
)
{
    FUNC(_create_mrbt_rec_in_slot);
    //w_assert2(page.is_fixed() && page.is_file_mrbt_p());

    // if bIgnoreLatches = false
          // page is already in the file and locked IX or EX
          // slot is already locked EX
    rid.pid = page.pid();
    rid.slot = slot;

    w_assert3(rid.slot >= 1);

    /*
     * create the record header and ...
     */
    rc_t             rc;
    rectag_t         tag;
    tag.hdr_len = hdr.size();

    // it is small, so put the data in as well
    tag.flags = t_small;
    tag.body_len = data.size();
    w_assert2(page.is_fixed() && (bIgnoreLatches || page.latch_mode() == LATCH_EX));
    rc = page.fill_slot(rid.slot, tag, hdr, data, 100);
    if (rc.is_error())  {
	
#if W_DEBUG_LEVEL >= 2
	fprintf(stderr, 
		"line %d : hdr.sz %lld data.sz %lld usable %d, %d, nfree %d,%d\n", 
                __LINE__, 
                (long long) hdr.size(), (long long) data.size(), 
                page.usable_space(),
                page.usable_space_for_slots(), 
                page.nfree(), page.nrsvd()
		);
#endif

	w_assert1(rc.err_num() != eRECWONTFIT); // NEH changed to assert1
	return RC_AUGMENT(rc);
    }
    
    w_assert3(page.is_fixed());

    return RCOK;
}

rc_t
file_m::create_mrbt_rec_l(
  const lpid_t& leaf,
  sdesc_t& sd,
  const vec_t& hdr,
  const vec_t& data,
  smsize_t len_hint,
  rid_t& new_rid,
  const bool bIgnoreLatches)
{
    FUNC(create_mrbt_rec_l);

    // 0. determine latch mode
    latch_mode_t latch = LATCH_EX;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    } 

    // 1. use the already assumed to be empty page
    shpid_t empty_page = sd.get_page_with_space(leaf);
    file_mrbt_p current_heap_page;
    bool space_found = false;
    if(empty_page) {
	lpid_t heap_page_id;
	heap_page_id._stid = sd.stid();
	heap_page_id.page = empty_page;
	W_DO( current_heap_page.fix(heap_page_id, latch ));
	lpid_t owner_leaf;
	current_heap_page.get_owner(owner_leaf);
	if(owner_leaf == leaf) {
	    W_DO( create_mrbt_rec_in_given_page(len_hint,
						sd,
						hdr,
						data,
						new_rid,
						current_heap_page,
						space_found,
						bIgnoreLatches) );
	}
    }

    if(!space_found) {
	
	// if we come to this point this means the page kept as the empty page is
	// either not empty or that heap page doesn't belong to this leaf anymore
	
	// 2. try to find a file page with empty slot and pointed by leaf
	btree_p leaf_page;
	W_DO( leaf_page.fix(leaf, latch) );
	set<lpid_t> pages; // to not to look at already looked at heap pages
	rid_t current_rid;
	for(int i=0; !space_found && i < leaf_page.nrecs(); i++) {
	    // get a rec from the leaf to find a heap page pointed by this leaf
	    btrec_t rec_leaf(leaf_page, i);
	    rec_leaf.elem().copy_to(&current_rid, sizeof(rid_t));
	    // put the record to the heap page
	    if(pages.find(current_rid.pid) == pages.end()) {
		W_DO( current_heap_page.fix(current_rid.pid, latch ));
		W_DO( create_mrbt_rec_in_given_page(len_hint,
						    sd,
						    hdr,
						    data,
						    new_rid,
						    current_heap_page,
						    space_found,
						    bIgnoreLatches) );
		pages.insert(current_rid.pid);
	    }
	}
    
	// 3. none of the heap pages are empty then create a new one
	if(!space_found) {
	    lpid_t new_page_id;
	    W_DO(_alloc_mrbt_page(sd.stid(),
				  lpid_t::eof,
				  new_page_id,
				  current_heap_page,
				  true) );
	    W_DO( current_heap_page.set_owner(leaf) );
	    W_DO( create_mrbt_rec_in_given_page(len_hint,
						sd,
						hdr,
						data,
						new_rid,
						current_heap_page,
						space_found,
						bIgnoreLatches) );
	    sd.set_page_with_space(leaf, new_page_id.page);
	} else {
	    sd.set_page_with_space(leaf, current_rid.pid.page);
	}

	leaf_page.unfix();

    }
    
    return RCOK;
}

rc_t
file_m::create_mrbt_rec_p(
  const lpid_t& leaf,
  sdesc_t& sd,
  const vec_t& hdr,
  const vec_t& data,
  smsize_t len_hint,
  rid_t& new_rid,
  const bool bIgnoreLatches)
{
    FUNC(create_mrbt_rec_p);

    // 0. determine latch mode
    latch_mode_t heap_latch = LATCH_EX;
    latch_mode_t leaf_latch = LATCH_SH;
    if(bIgnoreLatches) {
	heap_latch = LATCH_NL;
	leaf_latch = LATCH_NL;
    }

    btree_p leaf_page;
    W_DO( leaf_page.fix(leaf, leaf_latch) );
    lpid_t root = leaf_page.root();
    
    // 1. use the already assumed to be empty page
    shpid_t empty_page = sd.get_page_with_space(root);
    file_mrbt_p current_heap_page;
    bool space_found = false;
    if(empty_page) {
	lpid_t heap_page_id;
	heap_page_id._stid = sd.stid();
	heap_page_id.page = empty_page;
	W_DO( current_heap_page.fix(heap_page_id, heap_latch ));
	lpid_t owner;
	current_heap_page.get_owner(owner);
	if(owner == root) {
	    W_DO( create_mrbt_rec_in_given_page(len_hint,
						sd,
						hdr,
						data,
						new_rid,
						current_heap_page,
						space_found,
						bIgnoreLatches) );
	}
    }
    
    if(!space_found) {
	
	// if we come to this point this means the page kept as the empty page is
	// either not empty or that heap page doesn't belong to this leaf anymore

    
	// 1. try to find a file page with empty slot and pointed by this sub-tree
	rid_t current_rid;
	set<lpid_t> pages; // to not to look at already looked at heap pages
	// 1.1 start with the leaf page that the insert will take place
	for(int i=0; !space_found && i < leaf_page.nrecs(); i++) {
	    // get the rec from the leaf_page
	    btrec_t rec_leaf(leaf_page, i);
	    rec_leaf.elem().copy_to(&current_rid, sizeof(rid_t));
	    // move it to new page
	    if(pages.find(current_rid.pid) == pages.end()) {
		W_DO( current_heap_page.fix(current_rid.pid, heap_latch ));
		W_DO( create_mrbt_rec_in_given_page(len_hint,
						    sd,
						    hdr,
						    data,
						    new_rid,
						    current_heap_page,
						    space_found,
						    bIgnoreLatches) );
		pages.insert(current_rid.pid);
	    }
	}
	// 1.2 if no space found then traverse the leaf pages that comes after this leaf page
	if(!space_found && leaf_page.next() != 0) {
	    btree_p next_leaf;
	    lpid_t pid_next_leaf(leaf._stid, leaf_page.next());
	    W_DO( next_leaf.fix(pid_next_leaf, leaf_latch) );
	    int i = 0;
	    while(!space_found) {
		if(i >= next_leaf.nrecs()) {
		    pid_next_leaf.page = next_leaf.next();
		    next_leaf.unfix();
		    if(pid_next_leaf.page != 0) {
			i = 0;
			W_DO( next_leaf.fix(pid_next_leaf, leaf_latch) );
		    } else {
			break;
		    }
		}
		// get the rec from the leaf_page
		btrec_t rec_leaf(next_leaf, i);
		rec_leaf.elem().copy_to(&current_rid, sizeof(rid_t));
		if(pages.find(current_rid.pid) == pages.end()) {
		    // move it to new page
		    W_DO( current_heap_page.fix(current_rid.pid, heap_latch ));
		    W_DO( create_mrbt_rec_in_given_page(len_hint,
							sd,
							hdr,
							data,
							new_rid,
							current_heap_page,
							space_found,
							bIgnoreLatches) );
		    pages.insert(current_rid.pid);
		}
		i++;
	    }
	}
	// 1.3 if still no space found then traverse the leaf pages that comes before this leaf page
	if(!space_found && leaf_page.prev() != 0) {
	    btree_p prev_leaf;
	    lpid_t pid_prev_leaf(leaf._stid, leaf_page.prev());
	    W_DO( prev_leaf.fix(pid_prev_leaf, leaf_latch) );
	    int i = 0;
	    while(!space_found) {
		if(i >= prev_leaf.nrecs()) {
		    pid_prev_leaf.page = prev_leaf.next();
		    prev_leaf.unfix();
		    if(pid_prev_leaf.page != 0) {
			i = 0;
			W_DO( prev_leaf.fix(pid_prev_leaf, leaf_latch) );
		    } else {
			break;
		    }
		}
		// get the rec from the leaf_page
		btrec_t rec_leaf(prev_leaf, i);
		rec_leaf.elem().copy_to(&current_rid, sizeof(rid_t));
		if(pages.find(current_rid.pid) == pages.end()) {
		    // move it to new page
		    W_DO( current_heap_page.fix(current_rid.pid, heap_latch ));
		    W_DO( create_mrbt_rec_in_given_page(len_hint,
							sd,
							hdr,
							data,
							new_rid,
							current_heap_page,
							space_found,
							bIgnoreLatches) );
		    pages.insert(current_rid.pid);
		}
		i++;
	    }
	}
	
	// 2. none of the heap pages are empty then create a new one
	if(!space_found) {
	    lpid_t new_page_id;
	    W_DO( _alloc_mrbt_page(sd.stid(),
				   lpid_t::eof,
				   new_page_id,
				   current_heap_page,
				   true) );
	    W_DO( current_heap_page.set_owner(root) );
	    // retry the insert
	    W_DO( create_mrbt_rec_in_given_page(len_hint,
						sd,
						hdr,
						data,
						new_rid,
						current_heap_page,
						space_found,
						bIgnoreLatches) );
	    sd.set_page_with_space(root, new_page_id.page);
	} else {
	    sd.set_page_with_space(root, current_rid.pid.page);
	}

    }

    leaf_page.unfix();
    
    return RCOK;
}


rc_t
file_m::_find_slotted_page_with_space(
    const stid_t&        stid,
    pg_policy_t          mask,   // might be t_append
    // and if it's exactly t_append, we had better not
    // create a record in the middle of the file.
    // mask & t_append == t_append means strict append semantics.
    sdesc_t&            sd,
    smsize_t            space_needed, 
    file_p&             page,        // output
    slotid_t&           slot        // output
#ifdef SM_DORA
    , const bool bIgnoreParents
#endif
)
{
    uint4_t         policy = uint4_t(mask);

    DBG(<< "_find_slotted_page_with_space needed=" << space_needed 
        << " policy =" << policy
        << " page is fixed =" << page.is_fixed()
        );

    bool                    found=false;
    const histoid_t*        h = sd.store_utilization();

    w_assert2(!page.is_fixed());

    /*
     * First, if policy calls for it, look in the cache
     * The cache is the histoid_t taken from the store descriptor.
     */
    if(policy & t_cache) {
        INC_TSTAT(fm_cache);
        while(!found) {
            pginfo_t        info;
            DBG(<<"looking in cache");
            W_DO(h->find_page(space_needed, found, info, &page, slot
#ifdef SM_DORA
                              , bIgnoreParents
#endif
                              ));

            if(found) {
                w_assert2(page.is_fixed());
                DBG(<<"found page " << info.page() 
                        << " slot " << slot);
                INC_TSTAT(fm_pagecache_hit);
                return RCOK;
            } else {
                // else no candidates -- drop down
                w_assert2(!page.is_fixed());
                break;
            }
        }
    }
    w_assert2(!found);
    w_assert2(!page.is_fixed());

    bool may_search_file = false;

    /*
     * Next, if policy calls for it, search the file for space.
     * We're going to be a little less aggressive here than when
     * we searched the cache.  If we read a page in from disk, 
     * we want to be sure that it will have enough space.  So we
     * bump ourselves up to the next bucket.
     */
    if(policy & t_compact) 
    {
        INC_TSTAT(fm_compact);
        DBG(<<"looking at file histogram");
        smsize_t sn = page_p::bucket2free_space(
                      page_p::free_space2bucket(space_needed)) + 1;
        W_DO(h->exists_page(sn, found));
        if(found) {
            INC_TSTAT(fm_histogram_hit);

            // histograms says there are 
            // some sufficiently empty pages in the file 
            // It's worthwhile to search the file for some space.

            lpid_t                 lpid;

            W_DO(first_page(stid, lpid, NULL/*allocated pgs only*/) );
            DBG(<<"first allocated page of " << stid << " is " << lpid);

            // scan the file for pages with adequate space
            bool                 eof = false;
            while ( !eof ) {
                w_assert3(!page.is_fixed());
                W_DO( page.fix(lpid, LATCH_SH, 0/*page_flags */));

                INC_TSTAT(fm_search_pages);

                DBG(<<"usable space=" << page.usable_space()
                        << " needed= " << space_needed);

                if (page.usable_space_for_slots() >= sizeof(file_p::slot_t) 
                     &&
                   page.usable_space() >= space_needed) 
                {
                    W_DO(h->latch_lock_get_slot( 
                        lpid.page, &page, space_needed,
                        false, // not append-only 
                        found, slot));
                    if(found) {
                        w_assert3(page.is_fixed());
                        DBG(<<"found page " << page.pid().page 
                                << " slot " << slot);
                        INC_TSTAT(fm_search_hit);
                        return RCOK;
                    }
                }
                page.unfix(); // avoid latch-lock deadlocks.

                // read the next page
                DBG(<<"get next page after " << lpid 
                        << " for space " << space_needed);
                W_DO(next_page_with_space(lpid, eof,
                        file_p::free_space2bucket(space_needed) + 1));
                DBG(<<"next page is " << lpid);
            }
            // This should never happen now that we bump ourselves up
            // to the next bucket.
            INC_TSTAT(fm_search_failed);
            found = false;
        } else {
            DBG(<<"not found in file histogram - so don't search file");
        }
        w_assert3(!found);
        if(!found) {
            // If a page exists in the allocated extents,
            // allocate one and use it.
            may_search_file = true;
            // NB: we do NOT support alloc-in-file-with-search
            // -but-don't-alloc-any-new-extents 
            // because io layer doesn't offer that option
        }
    } // policy & t_compact

    w_assert3(!found);
    w_assert3(!page.is_fixed());

    /*
     * Last, if policy calls for it, append (possibly strict) 
     * to the file.  
     * may_search_file==true indicates not strictly appending; 
     * strict append is when
     *     policy is exactly t_append, in which case may_search_file 
     *        had better be false
     */
    if(policy & t_append) 
    {
#if   W_DEBUG_LEVEL > 1
        if(policy == t_append) {
            w_assert1(may_search_file == false);
        }
#endif

        if(may_search_file) {
            INC_TSTAT(fm_append);
        } else {
            INC_TSTAT(fm_appendonly);
        }
        lpid_t        lastpid = lpid_t(stid, sd.hog_last_pid());

        DBG(<<"try to append to file lastpid.page=" << lastpid.page);

        // might not have last pid cached
        if(lastpid.page) {
            DBG(<<"look in last page - which is " << lastpid );
            found = io->is_valid_page_of(lastpid, stid.store);

            // TODO: might get a deadlock here!!!!!

            INC_TSTAT(fm_lastpid_cached);
            sd.free_last_pid();

            if (found) {
                W_DO(h->latch_lock_get_slot( 
                                            lastpid.page, &page, space_needed,
                                            !may_search_file, // append-only
                                            found, slot));
            }
            if(found) {
                w_assert3(page.is_fixed());
                DBG(<<"found page " << page.pid().page 
                        << " slot " << slot);
                INC_TSTAT(fm_lastpid_hit);
                w_assert2(io->is_valid_page_of(page.pid(), stid.store));
                return RCOK;
            }
            DBG(<<"no slot in last page ");
            lastpid.page = 0;
        } else {
            sd.free_last_pid();
        }

        DBGTHRD(<<"allocate new page may_search_file="<< may_search_file  );

        /* Allocate a new page */
        lpid_t        newpid;
        /*
         * Argument may_search_file determines behavior of _alloc_page:
         * if true, it searches existing extents in the file besides
         * the one indicated by the near_pid (lastpid here) argument,
         * It appends extents if it can't satisfy the request
         * with the first extent inspected. 
         *
         * Furthermore, if may_search_file determines the treatment
         * of that first extent inspected: if may_search_file is true,
         * it looks for ANY free page in that extent; else it only
         * looks for free pages at the "end" of the extent, preserving
         * append_t policy.
         */
        W_DO(_alloc_page(stid,  lastpid, newpid,  page, may_search_file)); 
        w_assert3(page.is_fixed());
        w_assert3(page.latch_mode() == LATCH_EX);
        // Now have long-term IX lock on the page

        if(may_search_file) {
            sd.set_last_pid(0); // don't know last page
                // and don't want to look for it now
        } else {
            sd.set_last_pid(newpid.page);
        }

        // Page is already latched, but we don't have a
        // lock on a slot yet.  (Doesn't get doubly-latched by
        // calling latch_lock_get_slot, by the way.)
        W_DO(h->latch_lock_get_slot(
                newpid.page, &page, space_needed,
                !may_search_file,
                found, slot));

        if(found) {
            w_assert3(page.is_fixed());
            DBG(<<"found page " << page.pid().page 
                    << " slot " << slot);
            INC_TSTAT(fm_alloc_pg);
            w_assert2(io->is_valid_page_of(page.pid(), stid.store));
            return RCOK;
        }
        page.unfix();
    }
    w_assert3(!found);
    w_assert3(!page.is_fixed());

    INC_TSTAT(fm_nospace);
    DBG(<<"not found");
    return RC(eSPACENOTFOUND);
}


rc_t
file_m::_find_slotted_mrbt_page_with_space(
    const stid_t&        stid,
    pg_policy_t          mask,   // might be t_append
    // and if it's exactly t_append, we had better not
    // create a record in the middle of the file.
    // mask & t_append == t_append means strict append semantics.
    sdesc_t&            sd,
    smsize_t            space_needed, 
    file_mrbt_p&             page,        // output
    slotid_t&           slot        // output
#ifdef SM_DORA
    , const bool bIgnoreParents
#endif
)
{
    uint4_t         policy = uint4_t(mask);

    DBG(<< "_find_slotted_mrbt_page_with_space needed=" << space_needed 
        << " policy =" << policy
        << " page is fixed =" << page.is_fixed()
        );

    bool                    found=false;
    const histoid_t*        h = sd.store_utilization();

    w_assert2(!page.is_fixed());

    /*
     * First, if policy calls for it, look in the cache
     * The cache is the histoid_t taken from the store descriptor.
     */
    if(policy & t_cache) {
        INC_TSTAT(fm_cache);
        while(!found) {
            pginfo_t        info;
            DBG(<<"looking in cache");
            W_DO(h->find_page(space_needed, found, info, &page, slot
#ifdef SM_DORA
                              , bIgnoreParents
#endif
                              ));

            if(found) {
                w_assert2(page.is_fixed());
                DBG(<<"found page " << info.page() 
                        << " slot " << slot);
                INC_TSTAT(fm_pagecache_hit);
                return RCOK;
            } else {
                // else no candidates -- drop down
                w_assert2(!page.is_fixed());
                break;
            }
        }
    }
    w_assert2(!found);
    w_assert2(!page.is_fixed());

    bool may_search_file = false;

    /*
     * Next, if policy calls for it, search the file for space.
     * We're going to be a little less aggressive here than when
     * we searched the cache.  If we read a page in from disk, 
     * we want to be sure that it will have enough space.  So we
     * bump ourselves up to the next bucket.
     */
    if(policy & t_compact) 
    {
        INC_TSTAT(fm_compact);
        DBG(<<"looking at file histogram");
        smsize_t sn = page_p::bucket2free_space(
                      page_p::free_space2bucket(space_needed)) + 1;
        W_DO(h->exists_page(sn, found));
        if(found) {
            INC_TSTAT(fm_histogram_hit);

            // histograms says there are 
            // some sufficiently empty pages in the file 
            // It's worthwhile to search the file for some space.

            lpid_t                 lpid;

            W_DO(first_page(stid, lpid, NULL/*allocated pgs only*/) );
            DBG(<<"first allocated page of " << stid << " is " << lpid);

            // scan the file for pages with adequate space
            bool                 eof = false;
            while ( !eof ) {
                w_assert3(!page.is_fixed());
                W_DO( page.fix(lpid, LATCH_SH, 0/*page_flags */));

                INC_TSTAT(fm_search_pages);

                DBG(<<"usable space=" << page.usable_space()
                        << " needed= " << space_needed);

                if (page.usable_space_for_slots() >= sizeof(file_p::slot_t) 
                     &&
                   page.usable_space() >= space_needed) 
                {
                    W_DO(h->latch_lock_get_slot( 
                        lpid.page, &page, space_needed,
                        false, // not append-only 
                        found, slot));
                    if(found) {
                        w_assert3(page.is_fixed());
                        DBG(<<"found page " << page.pid().page 
                                << " slot " << slot);
                        INC_TSTAT(fm_search_hit);
                        return RCOK;
                    }
                }
                page.unfix(); // avoid latch-lock deadlocks.

                // read the next page
                DBG(<<"get next page after " << lpid 
                        << " for space " << space_needed);
                W_DO(next_page_with_space(lpid, eof,
                        file_p::free_space2bucket(space_needed) + 1));
                DBG(<<"next page is " << lpid);
            }
            // This should never happen now that we bump ourselves up
            // to the next bucket.
            INC_TSTAT(fm_search_failed);
            found = false;
        } else {
            DBG(<<"not found in file histogram - so don't search file");
        }
        w_assert3(!found);
        if(!found) {
            // If a page exists in the allocated extents,
            // allocate one and use it.
            may_search_file = true;
            // NB: we do NOT support alloc-in-file-with-search
            // -but-don't-alloc-any-new-extents 
            // because io layer doesn't offer that option
        }
    } // policy & t_compact

    w_assert3(!found);
    w_assert3(!page.is_fixed());

    /*
     * Last, if policy calls for it, append (possibly strict) 
     * to the file.  
     * may_search_file==true indicates not strictly appending; 
     * strict append is when
     *     policy is exactly t_append, in which case may_search_file 
     *        had better be false
     */
    if(policy & t_append) 
    {
#if   W_DEBUG_LEVEL > 1
        if(policy == t_append) {
            w_assert1(may_search_file == false);
        }
#endif

        if(may_search_file) {
            INC_TSTAT(fm_append);
        } else {
            INC_TSTAT(fm_appendonly);
        }
        lpid_t        lastpid = lpid_t(stid, sd.hog_last_pid());

        DBG(<<"try to append to file lastpid.page=" << lastpid.page);

        // might not have last pid cached
        if(lastpid.page) {
            DBG(<<"look in last page - which is " << lastpid );
            w_assert2(io->is_valid_page_of(lastpid, stid.store));

            // TODO: might get a deadlock here!!!!!

            INC_TSTAT(fm_lastpid_cached);
            sd.free_last_pid();

            W_DO(h->latch_lock_get_slot( 
                        lastpid.page, &page, space_needed,
                        !may_search_file, // append-only
                        found, slot));
            if(found) {
                w_assert3(page.is_fixed());
                DBG(<<"found page " << page.pid().page 
                        << " slot " << slot);
                INC_TSTAT(fm_lastpid_hit);
                w_assert2(io->is_valid_page_of(page.pid(), stid.store));
                return RCOK;
            }
            DBG(<<"no slot in last page ");
        } else {
            sd.free_last_pid();
        }

        DBGTHRD(<<"allocate new page may_search_file="<< may_search_file  );

        /* Allocate a new page */
        lpid_t        newpid;
        /*
         * Argument may_search_file determines behavior of _alloc_page:
         * if true, it searches existing extents in the file besides
         * the one indicated by the near_pid (lastpid here) argument,
         * It appends extents if it can't satisfy the request
         * with the first extent inspected. 
         *
         * Furthermore, if may_search_file determines the treatment
         * of that first extent inspected: if may_search_file is true,
         * it looks for ANY free page in that extent; else it only
         * looks for free pages at the "end" of the extent, preserving
         * append_t policy.
         */
        W_DO(_alloc_mrbt_page(stid,  lastpid, newpid,  page, may_search_file)); 
        w_assert3(page.is_fixed());
        w_assert3(page.latch_mode() == LATCH_EX);
        // Now have long-term IX lock on the page

        if(may_search_file) {
            sd.set_last_pid(0); // don't know last page
                // and don't want to look for it now
        } else {
            sd.set_last_pid(newpid.page);
        }

        // Page is already latched, but we don't have a
        // lock on a slot yet.  (Doesn't get doubly-latched by
        // calling latch_lock_get_slot, by the way.)
        W_DO(h->latch_lock_get_slot(
                newpid.page, &page, space_needed,
                !may_search_file,
                found, slot));

        if(found) {
            w_assert3(page.is_fixed());
            DBG(<<"found page " << page.pid().page 
                    << " slot " << slot);
            INC_TSTAT(fm_alloc_pg);
            w_assert2(io->is_valid_page_of(page.pid(), stid.store));
            return RCOK;
        }
        page.unfix();
    }
    w_assert3(!found);
    w_assert3(!page.is_fixed());

    INC_TSTAT(fm_nospace);
    DBG(<<"not found");
    return RC(eSPACENOTFOUND);
}


/* 
 * add a record on the given page.  page is already
 * fixed; all we have to do is try to allocate a slot
 * for a record of the given size
 */


rc_t
file_m::_create_rec_in_slot(
    file_p         &page,
    slotid_t       slot,
    recflags_t     rec_impl,         
    const vec_t&   hdr,
    const vec_t&   data,
    sdesc_t&       sd,
    bool           do_append,
    rid_t&        rid, // out
    const bool    MAYBE_UNUSED bIgnoreLatches
)
{
    FUNC(_create_rec_in_slot);
    w_assert2(page.is_fixed() /*&& page.is_file_p()*/);

    // page is already in the file and locked IX or EX
    // slot is already locked EX
    rid.pid = page.pid();
    rid.slot = slot;

    w_assert3(rid.slot >= 1);

    /*
     * create the record header and ...
     */
    rc_t             rc;
    rectag_t         tag;
    tag.hdr_len = hdr.size();

    switch (rec_impl) {
    case t_small:
        // it is small, so put the data in as well
        tag.flags = t_small;
        tag.body_len = data.size();
        w_assert2(page.is_fixed() && (bIgnoreLatches || page.latch_mode() == LATCH_EX));
        rc = page.fill_slot(rid.slot, tag, hdr, data, 100);
        if (rc.is_error())  {

#if W_DEBUG_LEVEL >= 2
            fprintf(stderr, 
            "line %d : hdr.sz %lld data.sz %lld usable %d, %d, nfree %d,%d\n", 
                __LINE__, 
                (long long) hdr.size(), (long long) data.size(), 
                page.usable_space(),
                page.usable_space_for_slots(), 
                page.nfree(), page.nrsvd()
            );
#endif

            w_assert1(rc.err_num() != eRECWONTFIT); // NEH changed to assert1
            return RC_AUGMENT(rc);
        }
        break;
    case t_large_0:
        // lookup the store to use for pages in the large record
        w_assert3(sd.large_stid().store > 0);

        // it is large, so create a 0-length large record
        {
            lg_tag_chunks_s   lg_chunks(sd.large_stid().store);
            vec_t              lg_vec(&lg_chunks, sizeof(lg_chunks));

            tag.flags = rec_impl;
            tag.body_len = 0;
            rc = page.fill_slot(rid.slot, tag, hdr, lg_vec, 100);
            if (rc.is_error()) {
                w_assert1(rc.err_num() != eRECWONTFIT); // NEH changed to assert1
                return RC_AUGMENT(rc);
            } 

            // now append the data to the record
            if(do_append) {
                W_DO(append_rec(rid, data, sd));
            }

        }
        break;
    case t_large_1: case t_large_2:
        // all large records start out as t_large_0
    default:
        W_FATAL(eINTERNAL);
    }
    w_assert3(page.is_fixed() && page.is_file_p());
    return RCOK;
}

/*
 * We have an EX lock on the record, which implies an IX lock on the
 * page
 */

rc_t
file_m::destroy_rec(const rid_t& rid, const bool bIgnoreLatches)
{
    file_p       page;
    record_t*    rec;

    DBGTHRD(<<"destroy_rec");
    latch_mode_t latch = LATCH_EX;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }
    W_DO(_locate_page(rid, page, latch));

    /*
     * Find or create a histoid for this store.
     */
    w_assert2(page.is_fixed());
    w_assert2(bIgnoreLatches || page.is_latched_by_me());
    w_assert2(bIgnoreLatches || page.is_mine());


    W_DO( page.get_rec(rid.slot, rec) );
    DBGTHRD(<<"got rec for rid " << rid);

    if (rec->is_small()) {
        // nothing special
        DBG(<<"small");
    } else {
        DBG(<<"large -- truncate " << rid << " size is " << rec->body_size());
        W_DO(_truncate_large(page, rid.slot, rec->body_size()));
    }

    W_DO( page.destroy_rec(rid.slot) ); // does a page_mark for the slot

    if (page.rec_count() == 0) {
        DBG(<<"Now free page");
        w_assert2(page.is_fixed());
	if(page.tag() == page_p::t_file_p) {
	    INC_TSTAT(page_file_dealloc);
	} else if(page.tag() == page_p::t_file_mrbt_p) {
	    INC_TSTAT(page_file_mrbt_dealloc);
	}
        W_DO(_free_page(page, bIgnoreLatches));
	return RCOK;
    } 

    DBG(<<"Update page utilization");
    /*
     *  Update the page's utilization info in the
     *  cache.
     *  (page_p::unfix updates the extent's histogram info)
     */
    histoid_update_t hu(page);
    hu.update();
    return RCOK;
}

rc_t
file_m::update_rec(const rid_t& rid, uint4_t start, const vec_t& data, const bool bIgnoreLatches)
{
    file_p    page;
    record_t*            rec;

    latch_mode_t page_latch_mode = LATCH_EX;
    if(bIgnoreLatches) {
	page_latch_mode = LATCH_NL;
    }
    DBGTHRD(<<"update_rec");
    W_DO(_locate_page(rid, page, page_latch_mode));

    W_DO( page.get_rec(rid.slot, rec) );

    /*
     *        Do some parameter checking
     */
    if (start > rec->body_size()) {
        return RC(eBADSTART);
    }
    if (data.size() > (rec->body_size()-start)) {
        return RC(eRECUPDATESIZE);
    }

    if (rec->is_small()) {
        W_DO( page.splice_data(rid.slot, u4i(start), data.size(), data) );
    } else {
        if (rec->tag.flags & t_large_0) {
            lg_tag_chunks_h lg_hdl(page, *(lg_tag_chunks_s*)rec->body());
            W_DO(lg_hdl.update(start, data));
        } else {
            lg_tag_indirect_h lg_hdl(page, *(lg_tag_indirect_s*)rec->body(), rec->page_count());
            W_DO(lg_hdl.update(start, data));
        }
    }
    return RCOK;
}

rc_t
file_m::append_rec(const rid_t& rid, const vec_t& data, const sdesc_t& sd)
{
    file_p            page;
    record_t*        rec;
    smsize_t        orig_size;

    w_assert3(rid.stid() == sd.stid());
#if W_DEBUG_LEVEL > 2
    // We should have an EX lock on the record
    {
        lock_mode_t m=NL;
        W_DO( lm->query(rid, m, xct()->tid()) );
        if(m != EX) {
            lock_mode_t mp=NL;
            W_DO( lm->query(rid.pid, mp, xct()->tid()) );
            if(mp != EX) {
                lock_mode_t ms=NL;
                W_DO( lm->query(rid.pid.stid(), ms, xct()->tid()) );
                if(ms != EX) {
                    lock_mode_t mv=NL;
                    W_DO( lm->query(rid.pid.stid().vol, mv, xct()->tid()) );
                    w_assert2(mv==EX);
                    m = mv;
                } else m = ms;
            } else m=mp;
        }
        DBG(<<" append rec to rid " << rid
                << " with lock mode (or parent lock mode)" << m
                << " for tid " << xct()->tid());
    }
#endif

    // NOTE: we grabbed an EX lock in ss_m::_append_rec
    // or create_rec
    DBGTHRD(<<"append_rec");
    W_DO( _locate_page(rid, page, LATCH_EX) );

    /*
     * Find or create a histoid for this store.
     */
    w_assert3(page.is_fixed());
    histoid_update_t hu(page);

    W_DO( page.get_rec(rid.slot, rec));

    orig_size = rec->body_size();

    // make sure we don't grow the record to larger than 4GB
    if ( (record_t::max_len - orig_size) < data.size() ) {
#if W_DEBUG_LEVEL > 2
        cerr << "can't grow beyond 4GB=" << int(record_t::max_len)
                << " orig_size " << int(orig_size)
                << " append.size() " << data.size()
                << endl;
#endif 
        return RC(eBADAPPEND);
    }

    // see if record will remain small
    smsize_t space_needed;
    if ( rec->is_small() &&
        file_p::choose_rec_implementation(rec->hdr_size(), 
            orig_size + data.size(), space_needed) == t_small) {

        if (page.usable_space() < data.size()) { return RC(eRECWONTFIT); }

        // This is append_rec: assume we are adding a slot, not
        // trying to reuse a free-up slot
        if( page.usable_space_for_slots() < sizeof(file_p::slot_t)) {
            return RC(eRECWONTFIT);
        }

        W_DO( page.append_rec(rid.slot, data) );
        // reaquire since may have moved
        W_COERCE( page.get_rec(rid.slot, rec) );
    } else if (rec->is_small()) {

        // Convert the record to a large implementation
        // copy the body to a temporary location

        char *tmp = new char[page_s::data_sz]; //auto-del
        w_auto_delete_array_t<char> autodel(tmp);

        memcpy(tmp, rec->body(), orig_size);
        vec_t   body_vec(tmp, orig_size);

        w_assert3(sd.large_stid().store > 0);

        // it is large, so create a 0-length large record
        lg_tag_chunks_s    lg_chunks(sd.large_stid().store);
        vec_t              lg_vec(&lg_chunks, sizeof(lg_chunks));

        // put the large record root after the header and mark
        // the record as large
        W_DO(page.splice_data(rid.slot, 0, (slot_length_t)orig_size, lg_vec)); 
        W_DO(page.set_rec_flags(rid.slot, t_large_0));
        W_DO(page.set_rec_len(rid.slot, 0));

        // append the original data and the new data
        DBG( << "appending large rid " << rid.slot << " body_vec.size "
                << body_vec.size());
        W_DO(_append_large(page, rid.slot, body_vec));
        DBG( << " set rec len  " << orig_size);
        W_DO(page.set_rec_len(rid.slot, orig_size));
        DBG( << "appending large rid " << rid.slot << " data.size "
                << data.size());
        W_DO(_append_large(page, rid.slot, data));

    } else {
        w_assert3(rec->is_large());
        DBG( << "appending large rid " << rid.slot 
                << "( orig size " << orig_size
                << ")+ data.size "
                << data.size()
                );
        W_DO(_append_large(page, rid.slot, data));
    }
    W_DO( page.set_rec_len(rid.slot, orig_size+data.size()) );

    /*
     *  Update the page's utilization info in the
     *  cache.
     *  (page_p::unfix updates the extent's histogram info)
     */
    DBG(<<"append rec");
    hu.update();
    return RCOK;
}


rc_t
file_m::append_mrbt_rec(const rid_t& rid, const vec_t& data, const sdesc_t& sd, const bool bIgnoreLatches)
{
    file_mrbt_p            page;
    record_t*        rec;
    smsize_t        orig_size;

    w_assert3(rid.stid() == sd.stid());
#if W_DEBUG_LEVEL > 2
    // We should have an EX lock on the record
    {
        lock_mode_t m=NL;
        W_DO( lm->query(rid, m, xct()->tid()) );
        if(m != EX) {
            lock_mode_t mp=NL;
            W_DO( lm->query(rid.pid, mp, xct()->tid()) );
            if(mp != EX) {
                lock_mode_t ms=NL;
                W_DO( lm->query(rid.pid.stid(), ms, xct()->tid()) );
                if(ms != EX) {
                    lock_mode_t mv=NL;
                    W_DO( lm->query(rid.pid.stid().vol, mv, xct()->tid()) );
                    w_assert2(mv==EX);
                    m = mv;
                } else m = ms;
            } else m=mp;
        }
        DBG(<<" append rec to rid " << rid
                << " with lock mode (or parent lock mode)" << m
                << " for tid " << xct()->tid());
    }
#endif

    // NOTE: we grabbed an EX lock in ss_m::_append_rec
    // or create_rec
    DBGTHRD(<<"append_mrbt_rec");
    latch_mode_t latch = LATCH_EX;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }
    W_DO( _locate_page(rid, page, latch) );

    /*
     * Find or create a histoid for this store.
     */
    w_assert3(page.is_fixed());
    histoid_update_t hu(page);

    W_DO( page.get_rec(rid.slot, rec));

    orig_size = rec->body_size();

    // make sure we don't grow the record to larger than 4GB
    if ( (record_t::max_len - orig_size) < data.size() ) {
#if W_DEBUG_LEVEL > 2
        cerr << "can't grow beyond 4GB=" << int(record_t::max_len)
                << " orig_size " << int(orig_size)
                << " append.size() " << data.size()
                << endl;
#endif 
        return RC(eBADAPPEND);
    }

    // see if record will remain small
    smsize_t space_needed;
    if ( rec->is_small() &&
	 file_mrbt_p::choose_rec_implementation(rec->hdr_size(), 
					       orig_size + data.size(), space_needed) == t_small) {

        if (page.usable_space() < data.size()) { return RC(eRECWONTFIT); }

        // This is append_rec: assume we are adding a slot, not
        // trying to reuse a free-up slot
        if( page.usable_space_for_slots() < sizeof(file_p::slot_t)) {
            return RC(eRECWONTFIT);
        }

        W_DO( page.append_rec(rid.slot, data) );
        // reaquire since may have moved
        W_COERCE( page.get_rec(rid.slot, rec) );
    } else if (rec->is_small()) {

        // Convert the record to a large implementation
        // copy the body to a temporary location

        char *tmp = new char[page_s::data_sz]; //auto-del
        w_auto_delete_array_t<char> autodel(tmp);

        memcpy(tmp, rec->body(), orig_size);
        vec_t   body_vec(tmp, orig_size);

        w_assert3(sd.large_stid().store > 0);

        // it is large, so create a 0-length large record
        lg_tag_chunks_s    lg_chunks(sd.large_stid().store);
        vec_t              lg_vec(&lg_chunks, sizeof(lg_chunks));

        // put the large record root after the header and mark
        // the record as large
        W_DO(page.splice_data(rid.slot, 0, (slot_length_t)orig_size, lg_vec)); 
        W_DO(page.set_rec_flags(rid.slot, t_large_0));
        W_DO(page.set_rec_len(rid.slot, 0));

        // append the original data and the new data
        DBG( << "appending large rid " << rid.slot << " body_vec.size "
                << body_vec.size());
        W_DO(_append_large(page, rid.slot, body_vec));
        DBG( << " set rec len  " << orig_size);
        W_DO(page.set_rec_len(rid.slot, orig_size));
        DBG( << "appending large rid " << rid.slot << " data.size "
                << data.size());
        W_DO(_append_large(page, rid.slot, data));

    } else {
        w_assert3(rec->is_large());
        DBG( << "appending large rid " << rid.slot 
                << "( orig size " << orig_size
                << ")+ data.size "
                << data.size()
                );
        W_DO(_append_large(page, rid.slot, data));
    }
    W_DO( page.set_rec_len(rid.slot, orig_size+data.size()) );

    /*
     *  Update the page's utilization info in the
     *  cache.
     *  (page_p::unfix updates the extent's histogram info)
     */
    DBG(<<"append mrbt rec");
    hu.update();
    return RCOK;
}


rc_t
file_m::truncate_rec(const rid_t& rid, uint4_t amount, bool& should_forward)
{
    FUNC(file_m::truncate_rec);
    file_p        page;
    record_t*     rec;
    should_forward = false;  // no need to forward record at this time

    DBGTHRD(<<"trucate_rec");
    W_DO (_locate_page(rid, page, LATCH_EX));

    /*
     * Find or create a histoid for this store.
     */
    w_assert2(page.is_fixed());
    histoid_update_t hu(page);

    W_DO(page.get_rec(rid.slot, rec));

    if (amount > rec->body_size()) 
        return RC(eRECUPDATESIZE);

    uint4_t        orig_size  = rec->body_size();
    uint2_t        orig_flags  = rec->tag.flags;

    if (rec->is_small()) {
        W_DO( page.truncate_rec(rid.slot, amount) );
        rec = NULL; // no longer valid;
    } else {
        W_DO(_truncate_large(page, rid.slot, amount));
        W_COERCE( page.get_rec(rid.slot, rec) );  // re-establish rec ptr
        // 
        // Now see it this record can be implemented as a small record
        //
        smsize_t len = orig_size-amount;
        smsize_t space_needed;
        recflags_t rec_impl;
        rec_impl = file_p::choose_rec_implementation(rec->hdr_size(), len, 
                space_needed);
        if (rec_impl == t_small) {
            DBG( << "rec was large, now is small");

            vec_t data;  // data left in the body
            uint4_t size_on_file_page;
            if (orig_flags & t_large_0) {
                size_on_file_page = sizeof(lg_tag_chunks_s);
            } else {
                size_on_file_page = sizeof(lg_tag_indirect_s);
            }

            if (len == 0) {
                DBG( << "rec is now is zero bytes long");
                w_assert3(data.size() == 0);
                W_DO(page.splice_data(rid.slot, 0, (slot_length_t)size_on_file_page, data));
                // record is now small 
                W_DO(page.set_rec_flags(rid.slot, t_small));
            } else {

                // the the data for the record (must be on "last" page)
                lgdata_p lgdata;
                W_DO( lgdata.fix(rec->last_pid(page), LATCH_SH) );
                w_assert3(lgdata.tuple_size(0) == len);
                data.put(lgdata.tuple_addr(0), len);

                /*
                 * Remember (in lgtmp) the large rec hdr on the file_p 
                 */

        /*
                // This is small: < 60 bytes -- Probably should put on stack 
                char lgtmp[sizeof(lg_tag_chunks_s)+sizeof(lg_tag_indirect_s)];
        */
                char  *lgtmp = new char[sizeof(lg_tag_chunks_s)+
                                        sizeof(lg_tag_indirect_s)];
                if (!lgtmp)
                        W_FATAL(fcOUTOFMEMORY);
                w_auto_delete_array_t<char>        autodel_lgtmp(lgtmp);

                lg_tag_chunks_s* lg_chunks = NULL;
                lg_tag_indirect_s* lg_indirect = NULL;
                if (orig_flags & t_large_0) {
                    memcpy(lgtmp, rec->body(), sizeof(lg_tag_chunks_s));
                    lg_chunks = (lg_tag_chunks_s*) lgtmp;
                } else {
                    memcpy(lgtmp, rec->body(), sizeof(lg_tag_indirect_s));
                    lg_indirect = (lg_tag_indirect_s*) lgtmp;
                }

                // splice body on file_p with data from lg rec
                rc_t rc = page.splice_data(rid.slot, 0, 
                                           (slot_length_t)size_on_file_page, data);
                if (rc.is_error()) {
                    if (rc.err_num() == eRECWONTFIT) {
                        // splice failed ==> not enough space on page
                        should_forward = true;
                        DBG( << "record should be forwarded after trunc");
                    } else {
                        return RC_AUGMENT(rc);
                    }                          
                } else {
                    rec = 0; // no longer valid;

                    // remove rest of data in lg rec
                    DBG( << "removing lgrec portion of truncated rec");
                    if (orig_flags & t_large_0) {
                        // remove the 1 lgdata page
                        w_assert3(lg_indirect == NULL);
                        lg_tag_chunks_h lg_hdl(page, *lg_chunks);
                        W_DO(lg_hdl.truncate(1));
                    } else {
                        // remove the 1 lgdata page and any indirect pages
                        w_assert3(lg_chunks == NULL);
                        lg_tag_indirect_h lg_hdl(page, *lg_indirect, 1/*page_cnt*/);
                        W_DO(lg_hdl.truncate(1));
                    }
                    // record is now small 
                    W_DO(page.set_rec_flags(rid.slot, t_small));
                }
            }
        }
    }
    /*
     *  Update the page's utilization info in the
     *  cache.
     *  (page_p::unfix updates the extent's histogram info)
     */
    DBG(<<"truncate rec");
    hu.update();
    /* 
     * Update the extent histo info before logging anything
     */
    W_DO(page.update_bucket_info());

    W_DO( page.set_rec_len(rid.slot, orig_size-amount) );

    return RCOK;
}


rc_t
file_m::truncate_mrbt_rec(const rid_t& rid, uint4_t amount, bool& should_forward, const bool bIgnoreLatches)
{
    FUNC(file_m::truncate_mrbt_rec);
    file_mrbt_p        page;
    record_t*     rec;
    should_forward = false;  // no need to forward record at this time

    DBGTHRD(<<"trucate_mrbt_rec");
    latch_mode_t latch = LATCH_EX;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }
    W_DO (_locate_page(rid, page, latch));

    /*
     * Find or create a histoid for this store.
     */
    w_assert2(page.is_fixed());
    histoid_update_t hu(page);

    W_DO(page.get_rec(rid.slot, rec));

    if (amount > rec->body_size()) 
        return RC(eRECUPDATESIZE);

    uint4_t        orig_size  = rec->body_size();
    uint2_t        orig_flags  = rec->tag.flags;

    if (rec->is_small()) {
        W_DO( page.truncate_rec(rid.slot, amount) );
        rec = NULL; // no longer valid;
    } else {
        W_DO(_truncate_large(page, rid.slot, amount));
        W_COERCE( page.get_rec(rid.slot, rec) );  // re-establish rec ptr
        // 
        // Now see it this record can be implemented as a small record
        //
        smsize_t len = orig_size-amount;
        smsize_t space_needed;
        recflags_t rec_impl;
        rec_impl = file_mrbt_p::choose_rec_implementation(rec->hdr_size(), len, 
                space_needed);
        if (rec_impl == t_small) {
            DBG( << "rec was large, now is small");

            vec_t data;  // data left in the body
            uint4_t size_on_file_page;
            if (orig_flags & t_large_0) {
                size_on_file_page = sizeof(lg_tag_chunks_s);
            } else {
                size_on_file_page = sizeof(lg_tag_indirect_s);
            }

            if (len == 0) {
                DBG( << "rec is now is zero bytes long");
                w_assert3(data.size() == 0);
                W_DO(page.splice_data(rid.slot, 0, (slot_length_t)size_on_file_page, data));
                // record is now small 
                W_DO(page.set_rec_flags(rid.slot, t_small));
            } else {

                // the the data for the record (must be on "last" page)
                lgdata_p lgdata;
                W_DO( lgdata.fix(rec->last_pid(page), LATCH_SH) );
                w_assert3(lgdata.tuple_size(0) == len);
                data.put(lgdata.tuple_addr(0), len);

                /*
                 * Remember (in lgtmp) the large rec hdr on the file_p 
                 */

        /*
                // This is small: < 60 bytes -- Probably should put on stack 
                char lgtmp[sizeof(lg_tag_chunks_s)+sizeof(lg_tag_indirect_s)];
        */
                char  *lgtmp = new char[sizeof(lg_tag_chunks_s)+
                                        sizeof(lg_tag_indirect_s)];
                if (!lgtmp)
                        W_FATAL(fcOUTOFMEMORY);
                w_auto_delete_array_t<char>        autodel_lgtmp(lgtmp);

                lg_tag_chunks_s* lg_chunks = NULL;
                lg_tag_indirect_s* lg_indirect = NULL;
                if (orig_flags & t_large_0) {
                    memcpy(lgtmp, rec->body(), sizeof(lg_tag_chunks_s));
                    lg_chunks = (lg_tag_chunks_s*) lgtmp;
                } else {
                    memcpy(lgtmp, rec->body(), sizeof(lg_tag_indirect_s));
                    lg_indirect = (lg_tag_indirect_s*) lgtmp;
                }

                // splice body on file_p with data from lg rec
                rc_t rc = page.splice_data(rid.slot, 0, 
                                           (slot_length_t)size_on_file_page, data);
                if (rc.is_error()) {
                    if (rc.err_num() == eRECWONTFIT) {
                        // splice failed ==> not enough space on page
                        should_forward = true;
                        DBG( << "record should be forwarded after trunc");
                    } else {
                        return RC_AUGMENT(rc);
                    }                          
                } else {
                    rec = 0; // no longer valid;

                    // remove rest of data in lg rec
                    DBG( << "removing lgrec portion of truncated rec");
                    if (orig_flags & t_large_0) {
                        // remove the 1 lgdata page
                        w_assert3(lg_indirect == NULL);
                        lg_tag_chunks_h lg_hdl(page, *lg_chunks);
                        W_DO(lg_hdl.truncate(1));
                    } else {
                        // remove the 1 lgdata page and any indirect pages
                        w_assert3(lg_chunks == NULL);
                        lg_tag_indirect_h lg_hdl(page, *lg_indirect, 1/*page_cnt*/);
                        W_DO(lg_hdl.truncate(1));
                    }
                    // record is now small 
                    W_DO(page.set_rec_flags(rid.slot, t_small));
                }
            }
        }
    }
    /*
     *  Update the page's utilization info in the
     *  cache.
     *  (page_p::unfix updates the extent's histogram info)
     */
    DBG(<<"truncate mrbt rec");
    hu.update();
    /* 
     * Update the extent histo info before logging anything
     */
    W_DO(page.update_bucket_info());

    W_DO( page.set_rec_len(rid.slot, orig_size-amount) );

    return RCOK;
}


rc_t
file_m::read_hdr(const rid_t& s_rid, int& len,
                 void* buf, const bool bIgnoreLatches)
{
    rid_t rid(s_rid);
    file_p page;
    
    DBGTHRD(<<"read_hdr");
    latch_mode_t latch = LATCH_SH;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }
    W_DO(_locate_page(rid, page, latch) );
    record_t* rec;
    W_DO( page.get_rec(rid.slot, rec) );
    
    w_assert1(rec->is_small());
    if (rec->is_small())  {
        if (len < rec->tag.hdr_len)  {
            return RC(eBADLENGTH); // not long enough
        }
        if (len > rec->tag.hdr_len) 
            len = rec->tag.hdr_len;
        memcpy(buf, rec->hdr(), len);
    }
    
    return RCOK;
}

rc_t
file_m::read_rec(const rid_t& s_rid,
                 int start, uint4_t& len, void* buf,
		 const bool bIgnoreLatches)
{
    rid_t rid(s_rid);
    file_p page;
    
    DBGTHRD(<<"read_rec");
        latch_mode_t latch = LATCH_SH;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }
    W_DO( _locate_page(rid, page, latch) );
    record_t* rec;
    W_DO( page.get_rec(rid.slot, rec) );
    
    w_assert1(rec->is_small());
    if (rec->is_small())  {
        if (start + len > rec->body_size())  
            len = rec->body_size() - start;
        memcpy(buf, rec->body() + start, (uint)len);
    }
    
    return RCOK;
}

rc_t
file_m::splice_hdr(rid_t rid, slot_length_t start, slot_length_t len, 
		   const vec_t& hdr_data, const bool bIgnoreLatches)
{
    file_p page;
    DBGTHRD(<<"splice_hdr");
    latch_mode_t latch = LATCH_EX;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }
    W_DO( _locate_page(rid, page, latch) );

    record_t* rec;
    W_DO( page.get_rec(rid.slot, rec) );

    // currently header realignment (rec hdr must always
    // have an alignedlength) is not supported
    w_assert3(len == hdr_data.size());
    W_DO( page.splice_hdr(rid.slot, start, len, hdr_data));
    return RCOK;
}

rc_t
file_m::first_page(const stid_t& fid, lpid_t& pid, bool* allocated)
{
    rc_t rc = io->first_page(fid, pid, allocated);
    if (rc.is_error()) {
        w_assert3(rc.err_num() != eEOF);
        if(rc.err_num() == eLOCKTIMEOUT) {
            // pid will have been returned.
            // Lock it while blocking and try again
            DBG(<<"");
            W_DO(lm->lock(pid, IX, t_long, WAIT_SPECIFIED_BY_XCT));
        } else {
            DBG(<<"rc="<< rc);
            return RC_AUGMENT(rc);
        }
    }
    DBGTHRD(<<"first_page is " <<pid);
    return RCOK;
}

rc_t
file_m::last_page(const stid_t& fid, lpid_t& pid, bool* allocated)
{
    FUNC(file_m::last_page);
    rc_t rc;
    do {
        rc = io->last_page(fid, pid, allocated, IX);
        if (rc.is_error()) {
            w_assert3(rc.err_num() != eEOF);
            if(rc.err_num() == eLOCKTIMEOUT) {
                // pid will have been returned.
                // Lock it while blocking and try again
                DBG(<<"");
                W_DO(lm->lock(pid, IX, t_long, WAIT_SPECIFIED_BY_XCT));
            } else {
                DBG(<<"rc="<< rc);
                return RC_AUGMENT(rc);
            }
        }
    } while (rc.is_error());
    DBG(<<"last page is  "<< pid);
    return RCOK;
}

// If "allocated" is NULL then only allocated pages will be
// returned.  If "allocated" is non-null then all pages will be
// returned and the bool pointed to by "allocated" will be set to
// indicate whether the page is allocated.
rc_t
file_m::next_page_with_space(lpid_t& pid, bool& eof, space_bucket_t b)
{
    eof = false;

    DBGTHRD(<<"find next_page_with_space ");

    rc_t rc = io->next_page_with_space(pid, b);
    DBGTHRD(<<"next_page_with_space returns " << rc);
    if (rc.is_error())  {
        if (rc.err_num() == eEOF) {
            eof = true;
        } else {
            return RC_AUGMENT(rc);
        }
    }
#if W_DEBUG_LEVEL > 2
   if(pid.page) {
        file_p page;
        w_assert3(!page.is_fixed());
        W_DO( page.fix(pid, LATCH_SH, 0/*page_flags */));
        DBG(   
                <<" page=" << page.pid().page
                <<" page.usable_space=" << page.usable_space()
                <<" page.usable_space_for_slots=" 
                    << page.usable_space_for_slots()
                <<" page.bucket=" << int(page.bucket())
                <<" need bucket" << int(b)
        );
        if( page.bucket() < b) {
            w_assert3(0);
        }
   }
#endif
    DBG(<<"next page with bucket >= " << int(b) << " is " << pid);
    return RCOK;
}
rc_t
file_m::next_page(lpid_t& pid, bool& eof, bool* allocated)
{
    eof = false;
    rc_t rc = io->next_page(pid, allocated);
    if (rc.is_error())  {
            if (rc.err_num() == eEOF) {
            eof = true;
        } else {
            return RC_AUGMENT(rc);
        }
    }
    return RCOK;
}

rc_t
file_m::_locate_page(const rid_t& rid, file_p& page, latch_mode_t mode)
{
    DBGTHRD(<<"file_m::_locate_page rid=" << rid);

    /*
     * pin the page 
     */
    W_DO(page.fix(rid.pid, mode));

    w_assert2(page.pid().page == rid.pid.page);
    // make sure page belongs to rid.pid.stid
    
    if (page.pid() != rid.pid) {
        page.unfix();
        return RC(eBADPID);
    }

    if (rid.slot < 0 || rid.slot >= page.num_slots())  {
        return RC(eBADSLOTNUMBER);
    }

    return RCOK;
}

//
// Free the page only if it is empty and it is not 
// the first page of the file.
//
// Note: a valid file should always have at least one page allocated.
// Note: This is called in forward processing and in rollback.
// In the rollback case we are undoing a file_alloc_page_log record.
// We consider this to be a nested top-level action; the undo CAN
// fail.  (Because the allocting xct acquires only an IX lock on the
// page, the page can be used immediately by other xcts to create records.)
// (Similarly, another xct could be trying to free the page
// we are doing the same, since they both find the record count to be 0.)
//
rc_t
file_m::_free_page(file_p& page, const bool bIgnoreLatches)
{
    w_assert2(page.is_fixed());
    lpid_t pid = page.pid();

    DBGTHRD(<<"free_page " << pid << ";  rec count=" << page.rec_count());
    if (page.rec_count() == 0) {

        lpid_t first_pid;
        W_DO(first_page(pid.stid(), first_pid, NULL));

        if (pid != first_pid) {
            DBGTHRD(<<"free_page: not first page -- go ahead");

            histoid_remove_t hr(page); // removes it from the heap if possible

            rc_t rc = smlevel_0::io->free_page(pid, false /*check store mmb*/);
            // Could result in a lock timeout, propagated up
            // from the volume layer, when it tries to get an
            // immediate (don't wait) EX lock on the page and
            // IX lock on the extent.
            // If we failed with the immediate locks, now try 
            // longer-term locks:

            // FRJ: actually, we *can't* resolve it because unfixing
            // the page here doesn't unfix the page in
            // alloc_file_page_log::undo
            // and if we hang onto the fixed page while we do the
            // lock_force, we can deadlocok.
            if (0 && rc.err_num() == eLOCKTIMEOUT) {
                page.unfix();
                rc = lm->lock_force(pid, EX, t_long, WAIT_SPECIFIED_BY_XCT);
                if(!rc.is_error()) {
		    latch_mode_t latch = LATCH_EX;
		    if(bIgnoreLatches) {
			latch = LATCH_NL;
		    }
                    // got lock. nothing should go wrong with the latch
                    W_DO(page.conditional_fix(pid, latch));

                    // Re-check.   Because we unfixed the page and
                    // re-fixed it, we have to check that it's
                    // still part of this store.  That's done by
                    // passing in "true" to io_m::free_page
                    if (page.rec_count() == 0) {
                        rc = smlevel_0::io->free_page(pid, 
                                true/*chk store memb*/ );
                        // Could fail to get the lock - eDEADLOCK 
                        // this time.
                        if(rc.is_error()) {
                            if(rc.err_num() == eDEADLOCK) {
                                // Ok give up and let other xct free the
                                // page. We could have two xcts trying to
                                // free the page at the same time.
                                return RCOK;
                            }
                            w_assert1(rc.err_num() != eLOCKTIMEOUT);
                        } 
                    }
                    // else leave it as eOK
                }
            } // if 0 && eLOCKTIMEOUT

            // Bail on eOK or any unacceptable failure
            if(rc.err_num() != eDEADLOCK && rc.err_num() != eLOCKTIMEOUT)
                return rc.reset(); 
        
            // It's OK if we couldn't get the lock. Give up and let
            // other xct free the page. We could have two xcts trying
            // to free the page at the same time.
            // Drop down and return RCOK.
        }
    }
    return RCOK;
}

// Free the page only if 
// it's empty and we can acquire an EX lock immediately.
// This is called during undo.
// We cannot wait for locks or latches. If this is a hot page,
// it won't matter.
// The generic undo code has already latched the page in EX mode.
// The file code should have ensured that we have an IX latch on the
// page.
// If this xct freed the page already, we have an EX lock on 
// the page and we will succeed here.  Freeing the page
// is idempotent, and cannot conflict with any other xct's use of
// the page because of our EX lock.
//
// If this xct did not free the page yet, we have an IX lock but will
// be able to get and EX lock as long as noone else is using the
// page.  If someone else is using the page (has an IX lock on it)
// the page is hot and we will not free it.
//
// If the page was discarded from the buffer pool, the file was
// destroyed already and it must have been done by this xct
// because destroy needs EX lock on the file and this xct has at
// least an IX lock on the file.
//
rc_t
file_m::_undo_alloc_file_page(file_p& page)
{
    FUNC(file_m::_undo_alloc_file_page);

    // Page tag could be *anything*, in that if the
    // page was discarded from the bp before it was
    // made durable as a file_p (i.e., if it was
    // not formatted), its tag is left over
    // from its prior life. 
    // There's a gap in the file_p allocation code 
    // in which that could be the case, and this doesn't
    // cope with it properly.  See GNATS 129. Fixing that
    // will address this.

    if(page.tag() == page_p::t_file_p || page.tag() == page_p::t_file_mrbt_p) {
        return _free_page(page);
    } 
    return RCOK;
}

/**\brief
 * Filter class to determine if we can allocate the given page from
 * the store.
 * Allocating a file page requires that we EX-latch it deep in the
 * io/volume layer so that there will be no race between allocating
 * it and formatting it.
 *
 * So the accept method here does a conditional fix.
 * The reject method unfixes it.
 */
class alloc_file_page_filter_t : public alloc_page_filter_t {
private:
    store_flag_t _flags;
    lpid_t       _pid;
    bool         _was_fixed;
    bool         _is_fixed;
    file_p&      _page;
    void  _reset();
public:
    NORET alloc_file_page_filter_t(store_flag_t flg, file_p &pg);
    NORET ~alloc_file_page_filter_t();
    bool  accept(const lpid_t&);
    void  reject();
    void  check() const;
    bool  accepted() const;
};

NORET alloc_file_page_filter_t::alloc_file_page_filter_t(
        store_flag_t flg, file_p &pg) 
    : alloc_page_filter_t(), _flags(flg), _page(pg) { _reset(); }

NORET alloc_file_page_filter_t::~alloc_file_page_filter_t() {}

void  alloc_file_page_filter_t::_reset()
{
    _pid = lpid_t::null;
    _was_fixed = false;
    _is_fixed = false;
}
bool  alloc_file_page_filter_t::accept(const lpid_t& pid) 
{
    _pid =  pid;
    _was_fixed = _page.is_latched_by_me();

#if W_DEBUG_LEVEL > 4
    {
    w_ostrstream o;
    lsn_t l = lsn_t(0, 1);
    xct_t *xd = xct();
    // Note : formatting a volume gets done outside a tx,
    // so in that case, the lsn_t(0,1) is used.  If DONT_TRUST_PAGE_LSN
    // is turned off, the raw page has lsn_t(0,1) when read from disk
    // for the first time, if, in fact, it's actually read.
    if(xd) {
        l = xd->last_lsn();
        o <<  "tid " << xd->tid() << " last_lsn " << l;
    }
    fprintf(stderr, "accept ? %d %s\n", _pid.page, o.c_str());
    }
#endif

    // Instead of doing the normal conditional_fix, we do _fix
    // so we can bypass the page format; if we do the format,
    // we'll run into problems initializing the lsn on the page, since
    // the assumption is that we have a last_lsn from allocating the
    // page!
    w_rc_t rc = _page.page_p::_fix(true, _pid, _page.t_file_p,
            LATCH_EX, 
            _page.t_virgin, 
            _flags,
            false /* ignore_store_id default value */,
            1 /* refbit default value */);

    if(rc.is_error()) {
        // fprintf(stderr, "NOT accept %d\n", _pid.page);
        _reset();
        check();
        INC_TSTAT(fm_alloc_page_reject);
        return false;
    }
    // fprintf(stderr, "accept %d err %d\n", _pid.page, rc.err_num());
    _is_fixed = true;
    check();
    return true;
}

bool  alloc_file_page_filter_t::accepted() const { return _is_fixed; }
void  alloc_file_page_filter_t::check() const
{
    if(accepted()) {
        /* how can I verify? Let me count the ways ... */
        // maybe not yet formatted w_assert1(_page.pid().page == _pid.page);
        // not yet formatted w_assert1(_page.rsvd_mode());
        w_assert1(_page.is_fixed());
        w_assert1(_page.is_latched_by_me());
        w_assert1(_page.is_mine());
        w_assert1(_page.my_latch()->mode() == LATCH_EX);
    }
}
void  alloc_file_page_filter_t::reject() 
{
    check();
    if(accepted() && !_was_fixed) {
        _page.unfix();
    }
    _reset();
}

rc_t
file_m::_alloc_page(
    stid_t fid,
    const lpid_t& near_p,
    lpid_t& allocPid,
    file_p &page,         // leave it fixed here
    bool search_file     // if false, it indicates strict append
)
{
    /* 
     * near_p indicates a page already allocated to the file.
     * If search_file==false, we can still look for a page in that
     * near_p's extent as long as that page is after the page indicated by
     * near_p. That is, if search_file==false, we are necessarily appending.
     */

    /*
     *  Allocate a new page. 
     *
     *  Originally:Page init (format) was not undoable. They were always
     *  followed by a reclaim to allocate the first slot.
     *  Now: page formats now have 2 parts, and only the part that corresponds
     *  to the page init is undoable; the insert-expand or reclaim part (part 2)
     *  is undoable.  So we cannot compensate around this now.
     *
     *  HOWEVER:   we do need to compensate to avoid the problem of
     *  undoing this allocation from underneath another xct that has
     *  allocated a record on the same page.
     *  Consequently, we'll compensate around this and we'll log
     *  a logical operation for file-page-create. This is not
     *  a problem for any pages except file pages - not for large-object
     *  pages either.
     *  (Not a problem for btrees or rtrees because they compensate around
     *  their page allocation and redo things logically (they might
     *  allocate a different page on the 2nd time around).
     *
     *  Pages get deallocated after commit when the extents are deallocated.
     */

    store_flag_t store_flags;
    {
        /* get the store flags before we descend into the io layer */
        W_DO( io->get_store_flags(fid, store_flags));
        if (store_flags & st_insert_file)  {
            store_flags = (store_flag_t) (store_flags|st_tmp); 
            // is st_tmp and st_insert_file
        }
    }


    {
        // Filter EX-latches the page; we hold the ex latch
        // and return the page latched.
        alloc_file_page_filter_t ok(store_flags, page);
        W_DO(io->alloc_a_file_page(&ok, fid, near_p, allocPid, IX,search_file));
        w_assert1(page.is_mine()); // EX-latched
        // Now we format, since it couldn't be done during accept()
        W_DO(page.format(allocPid, page.t_file_p, page.t_virgin, store_flags));
    }


    /*
     * We expect that even st_tmp pages will have a valid lsn,
     * because the page allocation had to be logged.
     * Valid means it has a file# that's not 0, that is, something
     * was logged for the page, or it was initialized with the
     * transaction's latest lsn.
     * Note that this latest lsn has not been written yet; it's
     * the lsn of the next log record to be written, so it does NOT
     * point to the log record of the alloc_file_page for *this* page.
     */
    w_assert2(page.lsn().valid());
    w_assert2(page.is_mine()); // EX-latched

    INC_TSTAT(page_file_alloc);

    return RCOK;
}


//  same as _alloc_page except takes a file_mrbt_p and sets the tag accordingly
rc_t
file_m::_alloc_mrbt_page(
    stid_t fid,
    const lpid_t& near_p,
    lpid_t& allocPid,
    file_mrbt_p &page,         // leave it fixed here
    bool search_file     // if false, it indicates strict append
)
{

    store_flag_t store_flags;
    {
        /* get the store flags before we descend into the io layer */
        W_DO( io->get_store_flags(fid, store_flags));
        if (store_flags & st_insert_file)  {
            store_flags = (store_flag_t) (store_flags|st_tmp); 
            // is st_tmp and st_insert_file
        }
    }


    {
        // Filter EX-latches the page; we hold the ex latch
        // and return the page latched.
        alloc_file_page_filter_t ok(store_flags, page);
        W_DO(io->alloc_a_file_page(&ok, fid, near_p, allocPid, IX,search_file));
        w_assert1(page.is_mine()); // EX-latched
        // Now we format, since it couldn't be done during accept()
        W_DO(page.format(allocPid, page.t_file_mrbt_p, page.t_virgin, store_flags));
    }


    /*
     * We expect that even st_tmp pages will have a valid lsn,
     * because the page allocation had to be logged.
     * Valid means it has a file# that's not 0, that is, something
     * was logged for the page, or it was initialized with the
     * transaction's latest lsn.
     * Note that this latest lsn has not been written yet; it's
     * the lsn of the next log record to be written, so it does NOT
     * point to the log record of the alloc_file_page for *this* page.
     */
    w_assert2(page.lsn().valid());
    w_assert2(page.is_mine()); // EX-latched

    INC_TSTAT(page_file_mrbt_alloc);
    
    return RCOK;
}


rc_t
file_m::_append_large(file_p& page, slotid_t slot, const vec_t& data)
{
    FUNC(file_m::_append_large);
    smsize_t        left_to_append = data.size();

    DBG(<<" data length " << left_to_append);
    record_t*       rec;

    W_DO( page.get_rec(slot, rec) );

    uint4_t    rec_page_count = rec->page_count();

    smsize_t   space_last_page = _space_last_page(rec->body_size());

    // add data to the last page
    if (space_last_page > 0) {
        lgdata_p last_page;
        W_DO( last_page.fix(rec->last_pid(page), LATCH_EX) );
        w_assert1(last_page.is_fixed());

        uint4_t append_amount = MIN(space_last_page, left_to_append);
        W_DO( last_page.append(data, 0, append_amount) );
        left_to_append -= append_amount;
    }

    // allocate pages to the record
    const smsize_t max_pages = 64;        // max pages to alloc per request
    smsize_t       num_pages = 0;        // number of new pages to allocate
    uint           pages_so_far = 0;        // pages appended so far
    bool           pages_alloced_already = false;
    lpid_t        *new_pages = new lpid_t[max_pages];

    if (!new_pages)
        W_FATAL(fcOUTOFMEMORY);
    w_auto_delete_array_t<lpid_t> ad_new_pages(new_pages);
  
    while(left_to_append > 0) {
        DBG(<<"left_to_append: " << left_to_append);
        pages_alloced_already = false;
        num_pages = (int) MIN(max_pages, 
                              ((left_to_append-1) / lgdata_p::data_sz)+1);
        DBG(<<"num_pages: " << num_pages
            <<" max_pages: " << max_pages
            <<" lgdata_p::data_sz: " << int(lgdata_p::data_sz));
        smsize_t append_cnt = MIN((smsize_t)(num_pages*lgdata_p::data_sz),
                                   left_to_append);
        DBG(<<"append_cnt: " << append_cnt);

        // see if the record is implemented as chunks of pages
        if (rec->tag.flags & t_large_0) {
            // attempt to add the new pages
            const lg_tag_chunks_h lg_hdl(page, *(lg_tag_chunks_s*)rec->body());

            DBGTHRD(<<" allocating " << num_pages << " new pages" );
            W_DO( io->alloc_page_group(lg_hdl.stid(), 
                        lg_hdl.last_pid(),  // hint
                        num_pages, 
                        new_pages,  // output -> array 
                        IX // lock mode
                        )); 

#if W_DEBUG_LEVEL > 2
            DBGTHRD(<< "Requested " << num_pages );
            for(shpid_t j=0; j<num_pages; j++) {
                DBGTHRD(<<  new_pages[j] );
            }
#endif 

            pages_alloced_already = true;
            lg_tag_chunks_s new_lg_chunks(lg_hdl.chunk_ref());
            lg_tag_chunks_h new_lg_hdl(page, new_lg_chunks);

            rc_t rc = new_lg_hdl.append(num_pages, new_pages);
            if (rc.is_error()) {
                if (rc.err_num() != eBADAPPEND) {
                    return RC_AUGMENT(rc);
                } 
                // too many chunks, so convert to indirect-based
                // implementation.  rec->page_count() cannot be
                // used since it will be incorrect while we
                // are appending.
                DBG(<<"converting");
                W_DO( _convert_to_large_indirect(
                            page, slot, lg_hdl.page_count()));

                // record may have moved, so reacquire
                W_COERCE( page.get_rec(slot, rec) );
                w_assert2(rec->tag.flags & (t_large_1|t_large_2));

            } else {
                DBGTHRD(<<"new_lg_hdl= " << new_lg_hdl);
                w_assert3(new_lg_hdl.page_count() == lg_hdl.page_count() + num_pages);
                // now update the actual lg_chunks on the page
                vec_t lg_vec(&new_lg_chunks, sizeof(new_lg_chunks));
                W_DO(page.splice_data(slot, 0, lg_vec.size(), lg_vec));
            }
        } 

        // check agaIn for indirect-based implementation since
        // conversion may have been performed
        if (rec->tag.flags & (t_large_1|t_large_2)) {
            const lg_tag_indirect_h 
                lg_hdl(page, *(lg_tag_indirect_s*)rec->body(), 
                             rec->page_count()+pages_so_far);

            if (!pages_alloced_already) {
                DBGTHRD(<<" allocating " << num_pages << " new pages" );
                W_DO(io->alloc_page_group(
                        lg_hdl.stid(), 
                        lg_hdl.last_pid(), // hint
                        num_pages, 
                        new_pages,  // output -> array
                        IX            // lock mode
                        )); 
                pages_alloced_already = true;
#if W_DEBUG_LEVEL > 2
                DBGTHRD(<< "Requested " << num_pages );
                for(shpid_t j=0; j<num_pages; j++) {
                    DBGTHRD(<<  new_pages[j] );
                }
#endif 
            }

            lg_tag_indirect_s new_lg_indirect(lg_hdl.indirect_ref());
            lg_tag_indirect_h new_lg_hdl(page, new_lg_indirect, rec_page_count+pages_so_far);
            W_DO(new_lg_hdl.append(num_pages, new_pages));
            if (lg_hdl.indirect_ref() != new_lg_indirect) {
                // change was made to the root
                // now update the actual lg_tag on the page
                vec_t lg_vec(&new_lg_indirect, sizeof(new_lg_indirect));
                W_DO(page.splice_data(slot, 0, lg_vec.size(), lg_vec));
            }
        }
        W_DO(_append_to_large_pages(num_pages, new_pages, data, 
				    left_to_append) );
        w_assert3(left_to_append >= append_cnt);
        left_to_append -= append_cnt;

        pages_so_far += num_pages;
    }
    w_assert3(data.size() <= space_last_page ||
            pages_so_far == ((data.size()-space_last_page-1) /
                             lgdata_p::data_sz + 1));
    return RCOK;
}

rc_t
file_m::_append_to_large_pages(int num_pages, const lpid_t new_pages[],
                               const vec_t& data, smsize_t left_to_append)
{
    int append_cnt;

    // Get the store flags in order to pass that info
    // down to for the fix() calls
    store_flag_t        store_flags;
    W_DO( io->get_store_flags(new_pages[0].stid(), store_flags));
    w_assert3(store_flags != st_bad);

    if (store_flags & st_insert_file)  {
        store_flags = (store_flag_t) (store_flags|st_tmp); 
        // is st_tmp and st_insert_file
    }

    for (int i = 0; i<num_pages; i++) {
         
        append_cnt = MIN((smsize_t)lgdata_p::data_sz, left_to_append);
        w_assert3(append_cnt > 0);

        lgdata_p lgdata;
        /* NB:
         * This is quite ugly when logging is considered:
         * The first step results in 2 log records (keep in mind,
         * this is for EACH page in the loop): page_init, page_insert,
         * (NB: (neh) page_init, page_insert have been combined into
         * page_format for lgdata_p)
         * while the 2nd step results in a page splice.
         * We *should* be able to do this in a way that generates
         * ONE log record per page in this loop.
         */

        /* NB: Causes page to be formatted: */
	W_DO(lgdata.fix(new_pages[i], LATCH_EX, lgdata.t_virgin, store_flags) );
    

        //  args:          vec,  starting offset, #bytes
        W_DO(lgdata.append(data, data.size() - left_to_append,
                           append_cnt));
        lgdata.unfix(); // make sure this is done early rather than in ~
        left_to_append -= append_cnt;
    }
    // This assert may not be true since data (and therefore
    // left_to_append) may be larger than the space on num_pages.
    // w_assert3(left_to_append == 0);
    return RCOK;
}

rc_t
file_m::_convert_to_large_indirect(file_p& page, slotid_t slot,
                                   uint4_t rec_page_count)
// note that rec.page_count() cannot be used since the record
// is being appended to and it's body_size() is not accurate. 
{
    record_t*    rec;
    W_COERCE(page.get_rec(slot, rec));

    // const since only page update calls can update a page
    const lg_tag_chunks_h old_lg_hdl(page, *(lg_tag_chunks_s*)rec->body()) ;
    lg_tag_indirect_s lg_indirect(old_lg_hdl.stid().store);
    lg_tag_indirect_h lg_hdl(page, lg_indirect, rec_page_count);
    W_DO(lg_hdl.convert(old_lg_hdl));

    // overwrite the lg_tag_chunks_s with a lg_tag_indirect_s
    vec_t lg_vec(&lg_indirect, sizeof(lg_indirect));
    W_DO(page.splice_data(slot, 0, sizeof(lg_tag_chunks_s), lg_vec));

    //change type of object
    W_DO(page.set_rec_flags(slot, lg_hdl.indirect_type(rec_page_count)));

    return RCOK;
}

rc_t
file_m::_truncate_large(file_p& page, slotid_t slot, uint4_t amount)
{
    record_t*   rec;
    W_COERCE( page.get_rec(slot, rec) );

    uint4_t         bytes_last_page = _bytes_last_page(rec->body_size());
    int             dealloc_count = 0; // number of pages to deallocate
    lpid_t          last_pid;

    // calculate the number of pages to deallocate
    if (amount >= bytes_last_page) {
        // 1 for last page + 1 for each full page
        dealloc_count = 1 + (int)(amount-bytes_last_page)/lgdata_p::data_sz;
    }

    if (rec->tag.flags & t_large_0) {
        const lg_tag_chunks_h lg_hdl(page, *(lg_tag_chunks_s*)rec->body()) ;
        lg_tag_chunks_s new_lg_chunks(lg_hdl.chunk_ref());
        lg_tag_chunks_h new_lg_hdl(page, new_lg_chunks) ;

        DBG(<<" dealloc_count " << dealloc_count);
        if (dealloc_count > 0) {
            W_DO(new_lg_hdl.truncate(dealloc_count));
            w_assert3(new_lg_hdl.page_count() == lg_hdl.page_count() - dealloc_count);
            // now update the actual lg_tag on the page
            vec_t lg_vec(&new_lg_chunks, sizeof(new_lg_chunks));
            W_DO(page.splice_data(slot, 0, lg_vec.size(), lg_vec));
        }
        last_pid = lg_hdl.last_pid();
    } else {
        w_assert3(rec->tag.flags & (t_large_1|t_large_2));
        const lg_tag_indirect_h lg_hdl(page, *(lg_tag_indirect_s*)rec->body(), rec->page_count());
        lg_tag_indirect_s new_lg_indirect(lg_hdl.indirect_ref());
        lg_tag_indirect_h new_lg_hdl(page, new_lg_indirect, rec->page_count());

        DBG(<<" dealloc_count " << dealloc_count);
        if (dealloc_count > 0) {
            W_DO(new_lg_hdl.truncate(dealloc_count));

            // now update the actual lg_tag on the page
            // if the tag has changed (change should only occur from
            // reducing the number of levels of indirection
            if (lg_hdl.indirect_ref() != new_lg_indirect) {
                vec_t lg_vec(&new_lg_indirect, sizeof(new_lg_indirect));
                W_DO(page.splice_data(slot, 0, lg_vec.size(), lg_vec));
            }
        }
        last_pid = lg_hdl.last_pid();
    }

    if (amount < rec->body_size()) {
        /*
         * remove data from the last page
         */
        lgdata_p lgdata;
        W_DO( lgdata.fix(last_pid, LATCH_EX) );
        // calculate amount left on the new last page
        int4_t trunc_on_last_page = amount;
        if (dealloc_count > 0) {
            trunc_on_last_page -= (dealloc_count-1)*lgdata_p::data_sz + bytes_last_page;
        }
        w_assert3(trunc_on_last_page >= 0);
        W_DO(lgdata.truncate(trunc_on_last_page));
    }
    return RCOK;
}


MAKEPAGECODE(file_mrbt_p, file_p)

rc_t file_mrbt_p::format(const lpid_t& pid, tag_t tag, uint4_t flags, 
			 store_flag_t store_flags)
{
    // pin: taken from file_p::format
    
    w_assert3(tag == t_file_mrbt_p);

    file_p_hdr_t ctrl;

/* NOTE: We tried to put DEADBEEF into file page headers
 *  when the page was deleted so we could use asserts about
 *  pages (not being deleted).
 * The problem is that set_hdr was never used before; never worked.
 *  Using set: either reclaim or overwrite, depending on whether
 *  the slot exists. It should exist if formatted, so we don't want
 *  to reclaim it, but I had problems using overwrite too, and I don't
 *  know why.
 */
    ctrl.cluster = DUMMY_CLUSTER_ID;                // dummy cluster ID
    vec_t file_p_hdr_vec;
    file_p_hdr_vec.put(&ctrl, sizeof(ctrl));

    /* first, don't log it */
    W_DO( page_p::_format(pid, tag, flags, store_flags) );

    // always set the store_flag here -- see comments 
    // in bf::fix(), which sets the store flags to st_regular
    // for all pages, and lets the type-specific store manager
    // override (because only file pages can be insert_file)
    //
    // persistent_part().set_page_storeflags(store_flags);
    this->set_store_flags(store_flags); // through the page_p, through the bfcb_t

    // initialize header
    lpid_t owner;
    file_p_hdr_vec.put((char*)(&owner), sizeof(lpid_t));
    W_COERCE(page_p::reclaim(0, file_p_hdr_vec, false/*don't log_it*/));

    /* Now, log as one (combined) record: -- 2nd 0 arg 
     * is to handle special case of reclaim */
    rc_t rc = log_page_format(*this, 0, 0, &file_p_hdr_vec); // file_p
    if(rc.is_error()) {
        discard();
        return rc;
    }

    return RCOK;
 }

/*********************************************************************
 *
 *  file_mrbt_p::shift(idx, rsib)
 *
 *  Shift all entries starting at "idx" to first entry of page "rsib".
 *  Stolen from zkeyed_p. Adapted to file_mrbt_p.
 *
 *********************************************************************/
rc_t
file_mrbt_p::shift(slotid_t idx, file_mrbt_p* rsib)
{
    FUNC(file_mrbt_p::shift);
    //w_assert1(idx >= 0 && idx < nrecs());

    int n = num_slots() - idx;

    DBG(<<"file_mrbt_p::SHIFT "  
	<< " from page " << pid().page << " idx " << idx
        << " #recs " << n
        );

    int start_simple_move=0;
    rc_t rc;

    /* 
     * grot performance hack: do in chunks of up to 
     * tmp_chunk_size slots at a time
     */
    const int tmp_chunk_size = 20;    // XXX magic number
    vec_t *tp = new vec_t[tmp_chunk_size];
    if (!tp)
    {
        return RC(fcOUTOFMEMORY);
    }

    w_auto_delete_array_t<vec_t>    ad_tp(tp);

    for (int i = start_simple_move; i < n && (! rc.is_error()); ) {
        int j;

        // NB: this next for-loop increments variable i !!!
        for (j = 0; j < tmp_chunk_size && i < n; j++, i++)  {
            tp[j].set(page_p::tuple_addr(1 + idx + i),
                  page_p::tuple_size(1 + idx + i));
        }

        // i has been incremented j times, hence the
        // subtraction for the 1st arg to insert_expand():
        rc = rsib->insert_expand(1 + i - j, j, tp); // do it & log it
    }
    if (! rc.is_error())  {
        DBG(<<"Removing " << n << " slots starting with " << 1+idx
            << " from page " << pid().page);
        rc = remove_compress(1 + idx, n);
    }
    DBG(<< " page " << pid().page << " has " << num_slots() << " slots");
    DBG(<< " page " << rsib->pid().page << " has " << num_slots() << " slots");

    return rc.reset();

}


rc_t
file_p::fill_slot(
    slotid_t                idx,
    const rectag_t&         tag, 
    const vec_t&            hdr,
    const vec_t&            data, 
    int                    /*pff*/)
{
    vec_t vec;
    vec.put(&tag, sizeof(tag));

    if(W_EXPECT_NOT(hdr.is_zvec())) {
        // don't bother messing with zvecs 
        // for the header -- for now, we 
        // assume that headers aren't big
        // enough to worry about
        vec.put(zero_page, hdr.size());
    } else {
        vec.put(hdr);
    }

    int hdr_size = hdr.size();
    if (!is_aligned(hdr_size)) {
        vec.put(zero_page, int(align(hdr_size)-hdr_size));
    }
    w_assert3(is_aligned(vec.size()));

    rc_t        rc;
    if(W_EXPECT_NOT(data.is_zvec())) {
        /*
         * 60: roughly the size of a splicez log record
         * it's not worth it to generate the extra unless
         * the amount saved is more than that
         */
        if(data.size() > 60) {
            rc = page_p::reclaim(idx, vec); // gets logged

#if W_DEBUG_LEVEL >= 2
            if(rc.is_error()) {
                fprintf(stderr, 
                "line %d : hdr.sz %lld data.sz %lld usable %d, %d, nfree %d,%d\n", 
                __LINE__, 
                (long long) hdr.size(), (long long) data.size(), usable_space(),
                usable_space_for_slots(), 
                nfree(), nrsvd()
                );
            }
#endif /* W_DEBUG_LEVEL >= 2 */

            if(!rc.is_error()) {
                rc =  page_p::splice(idx, vec.size(), 0, data);
            }
            // AND RETURN
            return rc;

        } else {
            // add a set of zeroes to vec
            vec.put(zero_page, data.size());
        }
    } else {
        // copy all of data to vec
        vec.put(data);
    }
    w_assert2(is_fixed() && latch_mode() == LATCH_EX);
    rc =  page_p::reclaim(idx, vec); // gets logged

#if W_DEBUG_LEVEL >= 2
    if(rc.is_error()) {
        fprintf(stderr, 
        "line %d : sizeof(rectag_t) %lld sizeof(slot_t) %lld vec.sz %lld usable %d, %d, nfree %d,%d, idx %d/%d\n", 
            __LINE__, 
            (long long) sizeof(rectag_t), (long long) sizeof(slot_t), 
            (long long) vec.size(),  usable_space(),
            usable_space_for_slots(), 
            nfree(), nrsvd(), 
            // slot index requested, #slots table has (indicates whether we are
            // requesting a new slot entry)
            idx, nslots()
        );
    }
#endif /* W_DEBUG_LEVEL >= 2 */

    return rc;
}

rc_t
file_p::format(const lpid_t& pid, tag_t tag, uint4_t flags, 
        store_flag_t store_flags)
{
    w_assert3(tag == t_file_p);

    file_p_hdr_t ctrl;

/* NOTE: We tried to put DEADBEEF into file page headers
 *  when the page was deleted so we could use asserts about
 *  pages (not being deleted).
 * The problem is that set_hdr was never used before; never worked.
 *  Using set: either reclaim or overwrite, depending on whether
 *  the slot exists. It should exist if formatted, so we don't want
 *  to reclaim it, but I had problems using overwrite too, and I don't
 *  know why.
 */
    ctrl.cluster = DUMMY_CLUSTER_ID;                // dummy cluster ID
    vec_t file_p_hdr_vec;
    file_p_hdr_vec.put(&ctrl, sizeof(ctrl));

    /* first, don't log it */
    W_DO( page_p::_format(pid, tag, flags, store_flags) );

    // always set the store_flag here -- see comments 
    // in bf::fix(), which sets the store flags to st_regular
    // for all pages, and lets the type-specific store manager
    // override (because only file pages can be insert_file)
    //
    // persistent_part().set_page_storeflags(store_flags);
    this->set_store_flags(store_flags); // through the page_p, through the bfcb_t

    W_COERCE(page_p::reclaim(0, file_p_hdr_vec, false/*don't log_it*/));

    /* Now, log as one (combined) record: -- 2nd 0 arg 
     * is to handle special case of reclaim */
    rc_t rc = log_page_format(*this, 0, 0, &file_p_hdr_vec); // file_p
    if(rc.is_error()) {
        discard();
        return rc;
    }
    return RCOK;
}


rc_t
file_p::find_and_lock_next_slot(
    uint4_t                  space_needed,
    slotid_t&                idx
)
{
    return _find_and_lock_free_slot(true, space_needed, idx);
}
rc_t
file_p::find_and_lock_free_slot(
    uint4_t                  space_needed,
    slotid_t&                idx
)
{
    return _find_and_lock_free_slot(false, space_needed, idx);
}
rc_t
file_p::_find_and_lock_free_slot(
    bool                     append_only,
    uint4_t                  space_needed,
    slotid_t&                idx
#ifdef SM_DORA
    , const bool bIgnoreParents
#endif
    )
{
    FUNC(find_and_lock_free_slot);
    w_assert3(is_file_p() || is_file_mrbt_p());
    slotid_t start_slot = 1;  // first slot to check if free
    // _find_and_lock_free_slot should never be using slot 0 on
    // a file page because that's the header slot; even though
    // the header is unused.  We will hit assertions if we use slot 0.
    rc_t rc;

    DBG(<< "space_needed " << space_needed);
    for (;;) 
    {
        if(append_only) {
            W_DO(page_p::next_slot(space_needed, idx));
        } else {
            W_DO(page_p::find_slot(space_needed, idx, start_slot));
        }
        // could be nslots() - new slot
        w_assert3(idx <= nslots());

        // try to lock the slot, but do not block
        rid_t rid(pid(), idx);

        // IP: For DORA it may ignore to acquire other locks than the RID
    rc = lm->lock(rid, EX, t_long, WAIT_IMMEDIATE
#ifdef SM_DORA
                      , 0, 0, 0, bIgnoreParents
#endif
                      );

        if (rc.is_error())  {
            if (rc.err_num() == eLOCKTIMEOUT) {
                // slot is locked by another transaction, so find
                // another slot.  One choice is to start searching
                // for a new slot after the one just found.  
                // An alternative is to force the allocation of a 
                // new slot.  We choose the alternative potentially
                // attempting to get many locks on a page where
                // we've already noticed contention.

                DBG(<< __LINE__ << " rc=" << rc);
                if(start_slot == nslots()) {
                    return RC(eRECWONTFIT);
                } else {
                    start_slot = nslots();  // force new slot allocation
                }
            } else {
                // error we can't handle
                DBG(<< __LINE__ << " rc=" << rc);
                return RC_AUGMENT(rc);
            }
        } else {
            // found and locked the slot
            DBG(<< "tid " << xct()->tid() <<" found and locked slot " << idx
                    << " for rid " << rid);
            break;
        }
    }
    return RCOK;
}

rc_t
file_p::get_rec(slotid_t idx, record_t*& rec)
{
    DBGTHRD(<<"file_m::get_rec idx " << idx << " page " << pid().page);
    rec = 0;
    if (! is_rec_valid(idx))  {
        return RC(eBADSLOTNUMBER);
    }
    rec = (record_t*) page_p::tuple_addr(idx);
    w_assert2(rec != NULL);
    return RCOK;
}

rc_t
file_p::set_hdr(const file_p_hdr_t& new_hdr)
{
    vec_t v;
    v.put(&new_hdr, sizeof(new_hdr));
    const vec_t hdr_vec_tmp(&new_hdr, sizeof(new_hdr));
    W_DO(page_p::reclaim(0, v)); 
    return RCOK;
}

rc_t
file_p::get_hdr(file_p_hdr_t& hdr) const
{
    if(page_p::nslots() > 0) {
        vec_t v;
        v.put(&hdr, sizeof(hdr));
        v.copy_from(page_p::tuple_addr(0), sizeof(hdr));
        return RCOK;
    }
    return RC(eBADSLOTNUMBER);
}

rc_t
file_p::splice_data(slotid_t idx, slot_length_t start, slot_length_t len, const vec_t& data)
{
    record_t*   rec;
    W_COERCE( get_rec(idx, rec) );
    int         base = rec->body_offset();

    return page_p::splice(idx, base + start, len, data);
}

rc_t
file_p::append_rec(slotid_t idx, const vec_t& data)
{
    record_t*   rec;

    W_COERCE( get_rec(idx, rec) );

    if (rec->is_small()) {
        W_DO( splice_data(idx, (int)rec->body_size(), 0, data) );
        // TODO: why are we doing this? (the get_rec again)
        W_COERCE( get_rec(idx, rec) );
    } else {
        // not implemented here.  see file::append_rec
        return RC(smlevel_0::eNOTIMPLEMENTED);
    }

    return RCOK;
}

rc_t
file_p::truncate_rec(slotid_t idx, uint4_t amount)
{
    record_t*   rec;
    W_COERCE( get_rec(idx, rec) );

    vec_t        empty(rec, 0);  // zero length vector 

    w_assert2(amount <= rec->body_size());
    w_assert1(rec->is_small());
    W_DO( splice_data(idx, (int)(rec->body_size()-amount), 
                      (int)amount, empty) );

    return RCOK;
}

rc_t
file_p::set_rec_len(slotid_t idx, uint4_t new_len)
{
    record_t*   rec;
    W_COERCE( get_rec(idx, rec) );

    vec_t   hdr_update(&(new_len), sizeof(rec->tag.body_len));
    int     size_location = ((char*)&rec->tag.body_len) - ((char*)rec);

    W_DO(splice(idx, size_location, sizeof(rec->tag.body_len), hdr_update) );
    return RCOK;
}

rc_t
file_p::set_rec_flags(slotid_t idx, uint2_t new_flags)
{
    record_t*   rec;
    W_COERCE( get_rec(idx, rec) );

    vec_t   flags_update(&(new_flags), sizeof(rec->tag.flags));
    int     flags_location = ((char*)&(rec->tag.flags)) - ((char*)rec);

    W_DO(splice(idx, flags_location, sizeof(rec->tag.flags), flags_update) );
    return RCOK;
}

slotid_t file_p::next_slot(slotid_t curr)
{
    w_assert3(curr >=0);

    // rids never have slot 0
    for (curr = curr+1; curr < nslots(); curr++) {
        if (is_tuple_valid(curr)) {
            return curr;
        }
    }

    // error
    w_assert2(curr == nslots());
    return 0;
}

// on a file page, the rec count is not the same as the
// slot count; the slot count includes the file page hdr.
int file_p::rec_count()
{
    int nrecs = 0;
    int nslots = page_p::nslots();

    for(slotid_t slot = 1; slot < nslots; slot++) {
        if (is_tuple_valid(slot)) nrecs++;
    }

    return nrecs;
}

recflags_t 
file_p::choose_rec_implementation(
    uint4_t         est_hdr_len,
    smsize_t        est_data_len,
    smsize_t&       rec_size // output: size of stuff going into slotted pg
    )
{
    est_hdr_len = sizeof(rectag_t) + align(est_hdr_len);
    w_assert2(is_aligned(est_hdr_len));
    w_assert2(is_aligned(sizeof(rectag_t)));

    if ( (est_data_len+est_hdr_len) <= file_p::data_sz) {
        // NOTE: file_p::data_sz has already taken into account one slot_t
        // and space for the file_p_hdr_t
        rec_size = est_hdr_len + est_data_len + sizeof(slot_t);
        return(t_small);
    } else {
        rec_size = est_hdr_len + align(sizeof(lg_tag_chunks_s))+sizeof(slot_t);
        return(t_large_0);
    }
     
    W_FATAL(eNOTIMPLEMENTED);
    return t_badflag;  // keep compiler quite
}


recflags_t 
file_mrbt_p::choose_rec_implementation(
    uint4_t         est_hdr_len,
    smsize_t        est_data_len,
    smsize_t&       rec_size // output: size of stuff going into slotted pg
    )
{
    est_hdr_len = sizeof(rectag_t) + align(est_hdr_len);
    w_assert2(is_aligned(est_hdr_len));
    w_assert2(is_aligned(sizeof(rectag_t)));

    if ( (est_data_len+est_hdr_len) <= file_mrbt_p::data_sz) {
        // NOTE: file_mrbt_p::data_mrbt_sz has already taken into account one slot_t
        // and space for the file_p_hdr_t
        rec_size = est_hdr_len + est_data_len + sizeof(slot_t);
        return(t_small);
    } else {
        rec_size = est_hdr_len + align(sizeof(lg_tag_chunks_s))+sizeof(slot_t);
        return(t_large_0);
    }
     
    W_FATAL(eNOTIMPLEMENTED);
    return t_badflag;  // keep compiler quite
}


MAKEPAGECODE(file_p, page_p)

//DU DF
rc_t
file_m::get_du_statistics(
    const stid_t& fid, 
    const stid_t& lfid, 
    file_stats_t& file_stats,
    bool          audit)
{
  FUNC(file_m::get_du_statistics);
    lpid_t        lpid;
    bool          eof = false;
    file_p         page;
    bool        allocated;

    base_stat_t file_pg_cnt = 0;
    base_stat_t unalloc_file_pg_cnt = 0;
    base_stat_t lgdata_pg_cnt = 0;
    base_stat_t lgindex_pg_cnt = 0;

    DBG(<<"analyze file " << fid);
    // analyzes an entire file

    {
        store_flag_t        store_flags;

        //
        // TODO: it would be more efficient if we
        // got the store flags passed down from the
        // caller, because it's more than likely figured out
        // by the caller.
        //
        // get store flags so we know whether we
        // can analyze the file or have to simply
        // count the #extents allocated to it.
        // This is the case for temp files (it's possible
        // that we just recovered and the pages aren't
        // intact, but the extents are, of course.
        // (This is also the case for files that are marked
        // for deletion at the end of this xct, but they
        // don't appear in the directory, so we never
        // get called for such stores.)

        W_DO( io->get_store_flags(fid, store_flags));
        w_assert3(store_flags != st_bad);

        DBG(<<"store flags " << store_flags);
        if(store_flags & st_tmp) {
            SmStoreMetaStats _stats;
            _stats.Clear();

            W_COERCE( io->get_store_meta_stats(fid, _stats) );
            DBG(<<"unalloc_file_pg_cnt +=" << _stats.numReservedPages);
            file_stats.unalloc_file_pg_cnt += _stats.numReservedPages;

            // Now do same for large object file
            // TODO: Why is this not using unalloc_large_page_cnt?
            //       Why do we not set the file_pg_cnt ? 
            // NOTE: we do not audit this file
            _stats.Clear();
            W_COERCE( io->get_store_meta_stats(lfid, _stats) );
            DBG(<<"unalloc_file_pg_cnt +=" << _stats.numReservedPages);
            file_stats.unalloc_file_pg_cnt += _stats.numReservedPages;
            return RCOK;
        }
    }

    W_DO(first_page(fid, lpid, &allocated) );
    DBG(<<"first page of " << fid << " is " 
            << lpid << " (allocate=" << allocated << ")");

    // scan each page of this file (large file lfid is handled later)
    while ( !eof ) {
        if (allocated) {
            file_pg_stats_t file_pg_stats;
            lgdata_pg_stats_t lgdata_pg_stats;
            lgindex_pg_stats_t lgindex_pg_stats;
            file_pg_cnt++;

            // In order to avoid latch-lock deadlocks,
            // we have to lock the page first
            W_DO(lm->lock_force(lpid, SH, t_long, WAIT_SPECIFIED_BY_XCT));

            W_DO( page.fix(lpid, LATCH_SH, 0/*page_flags */));

            W_DO(page.hdr_stats(file_pg_stats));

            DBG(<< "getting slot stats for page " << lpid); 
            W_DO(page.slot_stats(0/*all slots*/, 
                    file_pg_stats,
                    lgdata_pg_stats, 
                    lgindex_pg_stats, 
                    lgdata_pg_cnt,
                    lgindex_pg_cnt));
            page.unfix(); // avoid latch-lock deadlocks.

            if (audit) {
                W_DO(file_pg_stats.audit()); 
                W_DO(lgdata_pg_stats.audit()); 
                W_DO(lgindex_pg_stats.audit()); 
            }
            file_stats.file_pg.add(file_pg_stats);
            file_stats.lgdata_pg.add(lgdata_pg_stats);
            file_stats.lgindex_pg.add(lgindex_pg_stats);

        } else {
            unalloc_file_pg_cnt++;
        }

        // read the next page
        W_DO(next_page(lpid, eof, &allocated) );
        DBG(<<"next page of " << fid << " is " << lpid << " (allocate=" << allocated << ")");

    }

    DBG(<<"analyze large object file " << lfid);
    W_DO(first_page(lfid, lpid, &allocated) );

    DBG(<<"first page of " << lfid << " is " << lpid << " (allocate=" << allocated << ")");

    base_stat_t lg_pg_cnt = 0;
    base_stat_t lg_page_unalloc_cnt = 0;
    eof = false;
    while ( !eof ) {
        if (allocated) {
            lg_pg_cnt++;
        } else {
            lg_page_unalloc_cnt++;
        }
        DBG( << "lpid=" << lpid
            <<" lg_pg_cnt = " << lg_pg_cnt
            <<" lg_page_unalloc_cnt = " << lg_page_unalloc_cnt
        );
        W_DO(next_page(lpid, eof, &allocated) );
        DBG(<<"next page of " << lfid 
                << " is " << lpid << " (allocated=" << allocated << ")");
    }

    if(audit) {
        /*
         * NB: this check is meaningful ONLY if the proper
         * locks were grabbed, i.e., if audit==true.
         * (Otherwise, another tx can be changing the file
         * during the stats-gathering, such as is done in
         * script alloc.2 (one tx is gathering stats,
         * the other rolling back some deletes)
         */
        // lg_pg_cnt:  # pages allocated in extents in the large-object store
        // lgdata_pg_cnt:  # pages needed to hold the user data
        // lgindex_pg_cnt:  # pages referenced in the small-object store
        //                for metadata (index pages of t_large_1,2)
        if(lg_pg_cnt != lgdata_pg_cnt + lgindex_pg_cnt) {
            cerr << "lg_pg_cnt= " << lg_pg_cnt
                 << " BUT lgdata_pg_cnt= " << lgdata_pg_cnt
                 << " + lgindex_pg_cnt= " << lgindex_pg_cnt
                 << " = " << lgdata_pg_cnt + lgindex_pg_cnt
                 << endl;

            base_stat_t in_extents = lg_pg_cnt;
            base_stat_t referenced = lgdata_pg_cnt  + lgindex_pg_cnt;
            if(in_extents < referenced) {
                cerr  << (referenced - in_extents)
                    << " referenced pages are not in fact allocated to store "
                    << endl;
            } else {
                cerr  << (in_extents - referenced)
                    << " allocated pages have to references in the store "
                    << endl;
            }
            W_FATAL_MSG(fcINTERNAL, << "AUDIT FAILURE in large object store!");
        }
    }

    if(audit) {
        w_assert2((lg_pg_cnt + lg_page_unalloc_cnt) % smlevel_0::ext_sz == 0);
        w_assert2(lg_pg_cnt == lgdata_pg_cnt + lgindex_pg_cnt);
    }

    // We've analyzed THIS file, now add in our totals to the
    // collective sum:
    file_stats.file_pg_cnt += file_pg_cnt;
    file_stats.lgdata_pg_cnt += lgdata_pg_cnt;
    file_stats.lgindex_pg_cnt += lgindex_pg_cnt;
    file_stats.unalloc_file_pg_cnt += unalloc_file_pg_cnt;
    file_stats.unalloc_large_pg_cnt += lg_page_unalloc_cnt;
    
    DBG(<<"DONE analyze file " << fid);
    return RCOK;
}

rc_t
file_p::hdr_stats(file_pg_stats_t& file_pg_stats)
{
    // file_p overhead is:
    //        page hdr + first slot (containing file_p specific  stuff)
    file_pg_stats.hdr_bs += hdr_size() + sizeof(page_p::slot_t) + align(tuple_size(0));
    file_pg_stats.free_bs += persistent_part().space.nfree();
    return RCOK;
}

rc_t
file_p::slot_stats(
        slotid_t slot,  // 0 means all slots
        file_pg_stats_t& file_pg,
        lgdata_pg_stats_t& lgdata_pg,
        lgindex_pg_stats_t& lgindex_pg, 
        base_stat_t& lgdata_pg_cnt,
        base_stat_t& lgindex_pg_cnt)
{
    FUNC(file_p::slot_stats);

    slotid_t         start_slot = slot == 0 ? 1 : slot;
    slotid_t         end_slot = slot == 0 ? num_slots()-1 : slot;
    record_t*        rec;
   
    DBG(<<"start_slot=" << start_slot << " end_slot=" << end_slot);
    //scan all valid records in this page
    for (slotid_t sl = start_slot; sl <= end_slot; sl++) {
        if (!is_rec_valid(sl)) {
            file_pg.slots_unused_bs += sizeof(slot_t);
        } else {
            file_pg.slots_used_bs += sizeof(slot_t);
            rec = (record_t*) page_p::tuple_addr(sl);

            file_pg.rec_tag_bs += sizeof(rectag_t);
            file_pg.rec_hdr_bs += rec->hdr_size();
            file_pg.rec_hdr_align_bs += align(rec->hdr_size()) -
                                        rec->hdr_size();

            if ( rec->is_small() ) {
                DBG(<<"small rec");
                file_pg.small_rec_cnt++;
                file_pg.rec_body_bs += rec->body_size();
                file_pg.rec_body_align_bs += align(rec->body_size()) -
                                             rec->body_size();
            } else if ( rec->is_large() ) {
                DBG(<<"large rec");
                file_pg.lg_rec_cnt++;
                // page_count() is the computed #pages based on the
                // #bytes in the record, so it's the total # large data
                // pages that would be needed for this record, regardless
                // what form (t_large_0,1, or 2) it takes.
                // t_large_0, 1, and 2 only describe the format of the
                // metadata in the record body of the file page.
                base_stat_t lgdata_cnt = rec->page_count();
                lgdata_pg_cnt += lgdata_cnt;

                lgdata_pg.hdr_bs += lgdata_cnt * (page_sz - lgdata_p::data_sz);
                lgdata_pg.data_bs += rec->body_size();
                lgdata_pg.unused_bs += lgdata_cnt*lgdata_p::data_sz - rec->body_size();
                if ( rec->tag.flags & t_large_0 ) {
                    file_pg.rec_lg_chunk_bs += align(sizeof(lg_tag_chunks_s));
#ifdef W_TRACE
                    {
                        const lg_tag_chunks_h lg_hdl(*this, 
                                *(lg_tag_chunks_s*)rec->body());
                        DBG(
                            <<", npages " << lg_hdl.page_count()
                            <<", last page " << lg_hdl.last_pid()
                            <<", store " << lg_hdl.stid() );
                    }
#endif
                } else if (rec->tag.flags & (t_large_1|t_large_2)) {
#ifdef W_TRACE
                    {
                        const lg_tag_indirect_h lg_hdl(*this, 
                                *(lg_tag_indirect_s*)rec->body(), 
                                rec->page_count());
                        DBG(
                            <<", last page " << lg_hdl.last_pid()
                            <<", store " << lg_hdl.stid() );
                    }
#endif
                    file_pg.rec_lg_indirect_bs += align(sizeof(lg_tag_indirect_s));
                    base_stat_t lgindex_cnt = 0;
                    if (rec->tag.flags & t_large_1) {
                        lgindex_cnt = 1;
                    } else {
                        lgindex_cnt += (lgdata_cnt-1)/lgindex_p::max_pids+2;
                    }
                    lgindex_pg.used_bs += lgindex_cnt * (page_sz - lgindex_p::data_sz);
                    lgindex_pg.used_bs += (lgindex_cnt-1 + lgdata_cnt)*sizeof(shpid_t);
                    lgindex_pg.unused_bs +=
                        (lgindex_p::data_sz*lgindex_cnt) -
                        ((lgindex_cnt-1 + lgdata_cnt)*sizeof(shpid_t));

                    lgindex_pg_cnt += lgindex_cnt;

                } else {
                    W_FATAL(eINTERNAL);
                }
                DBG(<<"lgdata_cnt (rec->page_count()) = " << lgdata_cnt
                        << " rec is slot " << sl
                        << " lgdata_pg_cnt(sum) = " << lgdata_pg_cnt
                    );
            } else {
                W_FATAL(eINTERNAL);
            }
        }
    }
    DBG(<<"slot_stats returns lgdata_pg_cnt=" << lgdata_pg_cnt
            << " lgindex_pg_cnt=" << lgindex_pg_cnt );
    return RCOK;
}


//
// Override the record tag. This is used for large object sort. 
//
rc_t
file_m::update_rectag(const rid_t& rid, uint4_t len, uint2_t flags)
{
    file_p page;
    
    W_DO(_locate_page(rid, page, LATCH_EX) );
    
    W_DO( page.set_rec_len(rid.slot, len) );
    W_DO( page.set_rec_flags(rid.slot, flags) );
    
    return RCOK;
}
