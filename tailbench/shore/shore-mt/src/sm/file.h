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

/*<std-header orig-src='shore' incl-file-exclusion='FILE_H'>

 $Id: file.h,v 1.102 2010/06/08 22:28:55 nhall Exp $

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

#ifndef FILE_H
#define FILE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

class sdesc_t; // forward

#ifndef FILE_S_H
#include "file_s.h"
#endif

struct file_stats_t;
struct file_pg_stats_t;
struct lgdata_pg_stats_t;
struct lgindex_pg_stats_t;
class  pin_i;

class file_m; // forward
class file_mrbt_p;


/*
 * Page type for a file of records.
 */
class file_p : public page_p {
    friend class file_m;
    friend class pin_i;

public:
    // free space on file_p is page_p less file_p header slot
    enum { data_sz = page_p::data_sz - align(sizeof(file_p_hdr_t)) - 
                                                     sizeof(slot_t),
           min_rec_size = sizeof(rectag_t) + sizeof(slot_t)
           };

    MAKEPAGE(file_p, page_p, 1);          // Macro to install basic functions from page_p.

    int                 num_slots()          { return page_p::nslots(); }

    rc_t                find_and_lock_next_slot(
        uint4_t                    space_needed,
        slotid_t&                  idx);

    rc_t                find_and_lock_free_slot(
        uint4_t                    space_needed,
        slotid_t&                  idx);

    rc_t                _find_and_lock_free_slot(
        bool                       append_only,
        uint4_t                    space_needed,
        slotid_t&                  idx
#ifdef SM_DORA
        , const bool                bIgnoreParents = false
#endif
        );

#define DUMMY_CLUSTER_ID 0

private:
    // Not presently used by file code:
    void                link_up(shpid_t nprv, shpid_t nnxt) {
        page_p::link_up(nprv, nnxt);
    }

    bool                 is_file_p() const;
    bool                 is_file_mrbt_p() const;
    
    rc_t                 set_hdr(const file_p_hdr_t& new_hdr);
    rc_t                 get_hdr(file_p_hdr_t &hdr) const;

    rc_t                 fill_slot(
        slotid_t            idx,
        const rectag_t&     tag,
        const vec_t&        hdr,
        const vec_t&        data,
        int                 pff);

    rc_t                destroy_rec(slotid_t idx);
    rc_t                append_rec(slotid_t idx, const vec_t& data);
    rc_t                truncate_rec(slotid_t idx, uint4_t amount);
    rc_t                set_rec_len(slotid_t idx, uint4_t new_len);
    rc_t                set_rec_flags(slotid_t idx, uint2_t new_flags);

    bool                is_rec_valid(slotid_t idx) { 
                            // rids never have slot 0
                            // but it seems if I change this, I
                            // have to deal with lots of code that
                            // relies on it processing 0 as valid.
                            return  // (idx > 0)  &&
                                    is_tuple_valid(idx); 
                        }

protected: // pin_i uses these
    rc_t                splice_data(
        slotid_t                 idx,
        slot_length_t            start,
        slot_length_t            len,
        const vec_t&             data);

public:
    // 
    // ss_m::_update_rec_hdr calls this
    rc_t                splice_hdr(
        slotid_t                 idx,
        slot_length_t            start,
        slot_length_t            len,
        const vec_t&             data); 

    // sort, pin, ss_m methods use these
    rc_t                get_rec(slotid_t idx, record_t*& rec);
    rc_t                get_rec(slotid_t idx, const record_t*& rec)  {
                                return get_rec(idx, (record_t*&) rec);
                        }

    slotid_t            next_slot(slotid_t curr);  // use curr = 0 for first

    int                 rec_count();

    // get stats on fixed size (ie. independent of the number of
    // records) page headers
    rc_t                hdr_stats(file_pg_stats_t& file_pg);

    // get stats on slot (or all slots if slot==0)
    rc_t                slot_stats(slotid_t slot, 
        file_pg_stats_t&           file_pg,
        lgdata_pg_stats_t&         lgdata_p, 
        lgindex_pg_stats_t&        lgindex_pg,
        base_stat_t&               lgdata_pg_cnt, 
        base_stat_t&               lgindex_pg_cnt);

    // determine how a record should be implemented given page size
    static recflags_t   choose_rec_implementation(
        uint4_t                    est_hdr_len,
        smsize_t                   est_data_len,
        smsize_t&                  rec_size);

private:
    /*
     *        Disable these since files do not have prev and next
     *        pages that can be determined from a page
     */
    shpid_t prev();
    shpid_t next();        //{ return page_p::next(); }
    friend class page_link_log;   // just to keep g++ happy
};

class file_m  : public smlevel_2 {
    friend class alloc_file_page_log;
    friend class btree_impl;
    friend class btree_m;
    
    typedef page_s::slot_length_t slot_length_t;
public:
    NORET file_m();
    NORET ~file_m();

    
    static rc_t create(stid_t stid, lpid_t& first_page);

    static rc_t create_mrbt(stid_t stid, lpid_t& first_page);

    static rc_t create_rec(
                        const stid_t&    fid,
                        // no page hint
                        smsize_t         len_hint,
                        const vec_t&     hdr,
                        const vec_t&     data,
                        sdesc_t&         sd,
                        rid_t&           rid // output
#ifdef SM_DORA
                        , const bool     bIgnoreParents = false
#endif
                    );

    static rc_t create_mrbt_rec(
                        const stid_t&    fid,
                        // no page hint
                        smsize_t         len_hint,
                        const vec_t&     hdr,
                        const vec_t&     data,
                        sdesc_t&         sd,
                        rid_t&           rid // output
#ifdef SM_DORA
                        , const bool     bIgnoreParents = false
#endif
                    );

    static rc_t create_rec_at_end(
                        file_p&                page, // in-out 
                        uint4_t         len_hint,
                        const vec_t&         hdr,
                        const vec_t&         data,
                        sdesc_t&         sd, 
                        rid_t&                 rid        // out
                    );

    static rc_t create_mrbt_rec_at_end(
                        file_mrbt_p&                page, // in-out 
                        uint4_t         len_hint,
                        const vec_t&         hdr,
                        const vec_t&         data,
                        sdesc_t&         sd, 
                        rid_t&                 rid        // out
                    );

    static rc_t create_rec_at_end( stid_t fid, 
                        file_p&        page, // in-out
                        uint4_t len_hint,
                        const vec_t& hdr,
                        const vec_t& data,
                        sdesc_t& sd, 
                        rid_t& rid);

    static rc_t create_rec( stid_t fid, 
                        const lpid_t&        page_hint,
                        uint4_t len_hint,
                        const vec_t& hdr,
                        const vec_t& data,
                        sdesc_t& sd, 
                        rid_t& rid,
                        bool forward_alloc = true
                        );

    static rc_t move_mrbt_rec_to_given_page(
				smsize_t            len_hint,
				const vec_t&        hdr,
                                const vec_t&        data,
                                rid_t&              rid,
                                file_p&             page,        // input
				bool&               space_found,
                                const bool        bIgnoreParents = false);

    static rc_t create_mrbt_rec_in_given_page(
				smsize_t            len_hint,
				sdesc_t&            sd,
				const vec_t&        hdr,
                                const vec_t&        data,
                                rid_t&              rid,
                                file_p&             page,        // input
				bool&               space_found,
                                const bool        bIgnoreParents = false);

    static rc_t create_mrbt_rec_l(
		         const lpid_t& leaf,
			 sdesc_t& sd,
			 const vec_t& hdr,
			 const vec_t& data,
			 smsize_t len_hint,
			 rid_t& new_rid,
			 const bool bIgnoreLatches = false);

    static rc_t create_mrbt_rec_p(
		         const lpid_t& leaf,
			 sdesc_t& sd,
			 const vec_t& hdr,
			 const vec_t& data,
			 smsize_t len_hint,
			 rid_t& new_rid,
			 const bool bIgnoreLatches = false);
	
    static rc_t destroy_rec_slot(const rid_t rid, file_mrbt_p& page, const bool bIgnoreLatches = false);
    
    static rc_t destroy_rec(const rid_t& rid, const bool bIgnoreLatches = false);

    static rc_t update_rec(const rid_t& rid, uint4_t start,
                           const vec_t& data,
                           const bool bIgnoreLatches = false
                           );

    static rc_t append_rec(const rid_t& rid, 
                           const vec_t& data,
                           const sdesc_t& sd);

    static rc_t append_mrbt_rec(const rid_t& rid, 
				const vec_t& data,
				const sdesc_t& sd,
				const bool bIgnoreLatches = false);
        
    static rc_t truncate_rec(const rid_t& rid, uint4_t amount, 
			     bool &should_forward);

    static rc_t truncate_mrbt_rec(const rid_t& rid, uint4_t amount, 
				  bool &should_forward, const bool bIgnoreLatches = false);
    
    static rc_t splice_hdr(rid_t rid, slot_length_t start, slot_length_t len,
                           const vec_t& hdr_data, const bool bIgnoreLatches = false);

    static rc_t read_rec(const rid_t& rid, int start, uint4_t& len, void* buf, const bool bIgnoreLatches = false);
    static rc_t read_rec(const rid_t& rid, uint4_t& len, void* buf, const bool bIgnoreLatches = false)  {
        return read_rec(rid, 0, len, buf, bIgnoreLatches);
    }
    static rc_t read_hdr(const rid_t& rid, int& len, void* buf, const bool bIgnoreLatches = false);

    // The following functions return the first/next pages in a
    // store.  If "allocated" is NULL then only allocated pages will be
    // returned.  If "allocated" is non-null then all pages will be
    // returned and the bool pointed to by "allocated" will be set to
    // indicate whether the page is allocated.
    static rc_t                first_page(
        const stid_t&            fid,
        lpid_t&                  pid,
        bool*                    allocated);

    static rc_t                next_page_with_space(
        lpid_t&                  pid, 
        bool&                    eof, 
        space_bucket_t           b);

    static rc_t                next_page(
        lpid_t&                  pid,
        bool&                    eof,
        bool*                    allocated);

    static rc_t                last_page(
        const stid_t&            fid,
        lpid_t&                  pid,
        bool*                    allocated);

    static rc_t                locate_page(const rid_t& rid, 
                                 file_p& page, 
                                 latch_mode_t mode) {
                                        return _locate_page(rid, page, mode); }

    static rc_t                get_du_statistics(const stid_t& fid, 
                                const stid_t& lg_fid, 
                                file_stats_t& file_stats, 
                                bool audit);

    // for large object sort: override the record tag
    static rc_t update_rectag(const rid_t& rid, uint4_t len, uint2_t flags);

protected:
    static rc_t _find_slotted_page_with_space(
                                const stid_t&   fid,
                                pg_policy_t     mask,
                                sdesc_t&        sd,
                                smsize_t        space_needed, 
                                file_p&         page,       // output
                                slotid_t&       slot        // output
#ifdef SM_DORA
                                , const bool    bIgnoreParents = false
#endif
                    );

    static rc_t _find_slotted_mrbt_page_with_space(
                                const stid_t&   fid,
                                pg_policy_t     mask,
                                sdesc_t&        sd,
                                smsize_t        space_needed, 
                                file_mrbt_p&         page,       // output
                                slotid_t&       slot        // output
#ifdef SM_DORA
                                , const bool    bIgnoreParents = false
#endif
                    );

    static rc_t _create_rec(
                                const stid_t&       fid,
                                pg_policy_t         policy,
                                smsize_t            len_hint,
                                sdesc_t&            sd,
                                const vec_t&        hdr,
                                const vec_t&        data,
                                rid_t&              rid,
                                file_p&             page        // in-output
#ifdef SM_DORA
                                , const bool        bIgnoreParents = false
#endif
                    );

    static rc_t _create_mrbt_rec(
                                const stid_t&       fid,
                                pg_policy_t         policy,
                                smsize_t            len_hint,
                                sdesc_t&            sd,
                                const vec_t&        hdr,
                                const vec_t&        data,
                                rid_t&              rid,
                                file_mrbt_p&             page        // in-output
#ifdef SM_DORA
                                , const bool        bIgnoreParents = false
#endif
                    );
    
    static rc_t _create_rec_given_page(
                                const stid_t        fid, 
                                file_p&             page, 
                                lpid_t&             pid,
                                uint4_t             len_hint,
                                const vec_t&        hdr, 
                                const vec_t&        data, 
                                sdesc_t&            sd, 
                                rid_t&              rid,
                                bool                forward_alloc,
                                bool                search
                        );

    static rc_t _next_page_policy_1(
                    const stid_t&        stid,
                    sdesc_t&             sd,
                    bool                 forward_alloc,
                    lpid_t&              pid);

    static rc_t  _next_page_policy_2(
                    const stid_t&        stid,
                    sdesc_t&             sd,
                    file_p&              page, 
                    bool&                forward_alloc,
                    bool                 search_file,
                    lpid_t&              newpid);

    static rc_t _create_rec_in_slot(
                    file_p&             page,
                    slotid_t            slot,
                    recflags_t          rec_impl,
                    const vec_t&        hdr,
                    const vec_t&        data,
                    sdesc_t&            sd,
                    bool                do_append,
                    rid_t&              rid, // out
		    const bool bIgnoreLatches = false
                    );

    static rc_t _create_mrbt_rec_in_slot(
                    file_p&        page,
                    slotid_t            slot,
		    const vec_t&        hdr,
                    const vec_t&        data,
                    rid_t&              rid, // out
                    const bool          bIgnoreLatches = false);

    static rc_t _undo_alloc_file_page(file_p& page);
    static rc_t _free_page(file_p& page, const bool bIgnoreLatches = false);
    static rc_t _alloc_page(stid_t fid, 
                            const lpid_t& near, lpid_t& pid,
			    file_p &page,
			    bool   search_file
                         );

    static rc_t _alloc_mrbt_page(stid_t fid, 
				 const lpid_t& near, lpid_t& pid,
				 file_mrbt_p &page,
				 bool   search_file
				 );
    
    static rc_t _locate_page(const rid_t& rid, file_p& page, latch_mode_t mode);
    static rc_t _append_large(file_p& page, slotid_t slot, const vec_t& data);
    static rc_t _append_to_large_pages(int num_pages,
                        const lpid_t new_pages[], const vec_t& data,
                        smsize_t left_to_append);
    static rc_t _convert_to_large_indirect(file_p& page, int2_t slotid_t,
                                   uint4_t rec_page_count);
    static rc_t _truncate_large(file_p& page, int2_t slotid_t, uint4_t amount);
    static smsize_t _space_last_page(smsize_t rec_sz); // bytes free on last
    static smsize_t _bytes_last_page(smsize_t rec_sz); // bytes used on last

private:
    // disabled
    file_m(const file_m&);
    file_m& operator=(const file_m&);
};


class file_mrbt_p : public file_p {
    friend class file__m;
    friend class pin_i;

public:
    // free space on file_mrbt_p is file_p less file_mrbt_p owner btree leaf page id
    enum { data_sz = file_p::data_sz - align(sizeof(lpid_t)) };

    MAKEPAGE(file_mrbt_p, file_p, 1);          // Macro to install basic functions from page_p.

    rc_t                 set_owner(const lpid_t& new_owner);
    rc_t                 get_owner(lpid_t &owner) const;

    static recflags_t   choose_rec_implementation(
        uint4_t                    est_hdr_len,
        smsize_t                   est_data_len,
        smsize_t&                  rec_size);
        
    rc_t                 shift(slotid_t snum, file_mrbt_p* rsib);
};

inline rc_t file_mrbt_p::set_owner(const lpid_t& owner)
{
    cvec_t owner_vec;
    owner_vec.put(&owner, sizeof(lpid_t));
    W_DO(file_p::overwrite(0, sizeof(file_p_hdr_t), owner_vec));
    //W_DO(file_p::overwrite(0, 0, owner_vec));
    return RCOK;
}

inline rc_t file_mrbt_p::get_owner(lpid_t& owner) const
{
    owner = *((lpid_t*)(((char*)file_p::tuple_addr(0))+sizeof(file_p_hdr_t)));
    //owner = *((lpid_t*)file_p::tuple_addr(0));
    return RCOK;
}

inline bool file_p::is_file_mrbt_p() const
{
    // all pages in file must be either t_file|t_lgdata|t_lgindex
    w_assert3(tag()&(t_file_mrbt_p|t_lgdata_p|t_lgindex_p)); 
    return (tag()&t_file_mrbt_p) != 0;
}


inline bool file_p::is_file_p() const
{
    // all pages in file must be either t_file|t_lgdata|t_lgindex
    w_assert3(tag()&(t_file_p|t_lgdata_p|t_lgindex_p)); 
    return (tag()&t_file_p) != 0;
}

inline rc_t
file_p::destroy_rec(slotid_t idx)
{
    return page_p::mark_free(idx);
}

inline rc_t
file_p::splice_hdr(slotid_t idx, slot_length_t start, slot_length_t len, const vec_t& data)
{
    record_t* rec;
    rc_t rc = get_rec(idx, rec);
    if (! rc.is_error())  {
        int base = rec->hdr() - (char*) rec; // offset of body
        rc = page_p::splice(idx, base + start, len, data);
    } 
    return rc.reset();
}

/*<std-footer incl-file-exclusion='FILE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
