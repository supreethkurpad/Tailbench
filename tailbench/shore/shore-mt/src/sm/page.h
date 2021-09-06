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

/*<std-header orig-src='shore' incl-file-exclusion='PAGE_H'>

 $Id: page.h,v 1.115 2010/06/15 17:30:07 nhall Exp $

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

#ifndef PAGE_H
#define PAGE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

class stnode_p;
class extlink_p;
class keyed_p;

#ifdef __GNUG__
#pragma interface
#endif

typedef uint1_t        space_bucket_t;

/*--------------------------------------------------------------*
 *  class page_p                                                *
 *  Basic page handle class. This class is used to fix a page        *
 *  and operate on it.                                                *
 *--------------------------------------------------------------*/
class page_p : public smlevel_0 
{

    friend class dir_vol_m;  // for access to page_p::splice();
    
    friend class btree_impl;
    
protected:
    typedef page_s::slot_t slot_t;
    typedef page_s::slot_offset_t slot_offset_t;
    typedef page_s::slot_length_t slot_length_t;

public:
    // -- for page tracing
    //static pthread_mutex_t _glmutex;
    //static ofstream _accesses;
    //static timeval curr_time;
    // --
    
    enum {
        data_sz = page_s::data_sz,
        max_slot = data_sz / sizeof(slot_t) + 2
    };
    enum logical_operation {
        l_none=0,
        l_set, // same as a 1-byte splice
        l_or,
        l_and,
        l_xor,
        l_not
    };
    enum tag_t {
        t_bad_p            = 0,        // not used
        t_extlink_p        = 1,        // extent link page 
        t_stnode_p         = 2,        // store node page
        t_keyed_p          = 3,        // keyed page
        t_zkeyed_p         = 4,        // zkeyed page
        t_btree_p          = 5,        // btree page 
        t_file_p           = 6,        // file page
        t_rtree_base_p     = 7,        // rtree base class page
        t_rtree_p          = 8,        // rtree page
        t_lgdata_p         = 9,        // large record data page
        t_lgindex_p        = 10,       // large record index page
	t_ranges_p         = 11,       // key-ranges info page
	t_file_mrbt_p      = 12,       // file page that has an owner
        t_any_p            = 13        // indifferent
    };
    enum page_flag_t {
        t_virgin        = 0x02,        // newly allocated page
        t_written        = 0x08        // read in from disk
    };

    /* BEGIN handling of bucket-management for pages  with rsvd_mode() */
protected:
    class page_bucket_info_t : smlevel_0 {
        enum { uninit = space_bucket_t(-1) };
    public:
        NORET        page_bucket_info_t() : _old(uninit), _checked(1) { }
        NORET        ~page_bucket_info_t() { w_assert3(_checked==1); }
        bool        initialized() const {  return _old != uninit; }
        void        init(space_bucket_t bucket) { 
            _old =  bucket;
            _checked = 0;
        }
        void        nochecknecessary() { _checked=1; }
        space_bucket_t        old() const { return _old; }
            
    private:
        // keep track of old & new
        space_bucket_t        _old;
        unsigned char        _checked;
    };
    w_rc_t      update_bucket_info();
    void        init_bucket_info(page_p::tag_t p, w_base_t::uint4_t page_flags);
    void        init_bucket_info();

public:
    static space_bucket_t         free_space2bucket(smsize_t free_space);
                        // return max free space that could be on a page
    static smsize_t                 bucket2free_space(space_bucket_t b);
    space_bucket_t                 bucket() const;
    smsize_t                         free_space4bucket() const;
    /* END handling of bucket-management for pages  with rsvd_mode() */


    bool                        rsvd_mode() const;
    static bool                 rsvd_mode(tag_t) ;
    static const char*          tag_name(tag_t t);
    
    const lsn_t&                lsn() const;
    void                        set_lsns(const lsn_t& lsn);
    void                        repair_rec_lsn(bool was_dirty, 
                                        lsn_t const &new_rlsn);
    
    shpid_t                     next() const;
    shpid_t                     prev() const;
    const lpid_t&               pid() const;

    // used when page is first read from disk
    void                        set_vid(vid_t vid);

    smsize_t                    used_space();
    smsize_t                    nfree() const;
    const tid_t &               tid() const; 
    smsize_t                    nrsvd() const; 
    smsize_t                    xct_rsvd() const; 
    // Total usable space on page
    smsize_t                     usable_space();
    // Usable space on page that's usable for expanding the
    // slot table -- makes sense only on rsvd_mode() pages,
    // where the transaction cannot use its reserved space to
    // expand the slot table.
    smsize_t                     usable_space_for_slots() const;
    smsize_t                     contig_space();
    
    rc_t                         check();
    bool                         pinned_by_me() const;

    slotid_t                     nslots() const;
 
    slotid_t                     nvacant() const;
 
    smsize_t                     tuple_size(slotid_t idx) const;
    void*                        tuple_addr(slotid_t idx) const;
    bool                         is_tuple_valid(slotid_t idx) const;

    w_base_t::uint4_t            page_flags() const;
    w_base_t::uint4_t            get_store_flags() const;
    void                         set_store_flags(w_base_t::uint4_t);
    page_s&                      persistent_part();
    const page_s&                persistent_part_const() const;
    bool                         is_fixed() const;
    bool                         is_latched_by_me() const;
    bool                         is_mine() const;
    const latch_t*               my_latch() const; // for debugging
    void                         set_dirty() const;
    bool                         is_dirty() const;

    NORET                        page_p() : _pp(0), _mode(LATCH_NL), _refbit(0) {};
    NORET                        page_p(page_s* s, 
                                        w_base_t::uint4_t store_flags,
                                        int refbit = 1) 
                                 : _pp(s), _mode(LATCH_NL), _refbit(refbit)  {
                                     // _pp->set_page_storeflags(store_flags); 
                                     // If we aren't fixed yet, we'll get an error here
                                     set_store_flags (store_flags);
                                 }
    NORET                        page_p(const page_p& p) { W_COERCE(_copy(p)); }
    virtual NORET                ~page_p();
    void                         destructor();
    page_p&                      operator=(const page_p& p);
    rc_t                         conditional_fix(
        const lpid_t&                   pid, 
        tag_t                           tag,
        latch_mode_t                    mode, 
        w_base_t::uint4_t               page_flags,
        store_flag_t&                   store_flags, // only used if virgin
        bool                            ignore_store_id = false,
        int                             refbit = 1);
    rc_t                         fix(
        const lpid_t&                    pid, 
        tag_t                            tag,
        latch_mode_t                     mode, 
        w_base_t::uint4_t                page_flags,
        store_flag_t&                    store_flags, // only used if virgin
        bool                             ignore_store_id = false,
        int                              refbit = 1) ;
    rc_t                         _fix(
        bool                             conditional,
        const lpid_t&                    pid, 
        tag_t                            tag,
        latch_mode_t                     mode, 
        w_base_t::uint4_t                page_flags,
        store_flag_t&                    store_flags, // only used if virgin
        bool                             ignore_store_id = false,
        int                              refbit = 1);
    void                         unfix();
    void                         discard();
    void                         unfix_dirty();
    // set_ref_bit sets the value to use for the buffer page reference
    // bit when the page is unfixed. 
    void                        set_ref_bit(int value) {_refbit = value;}

    // get EX latch if acquiring it will not block (otherwise set
    // would_block to true.
    void                         upgrade_latch(latch_mode_t m);

    rc_t                         upgrade_latch_if_not_block(
        bool&                            would_block); 

    // WARNING: the clear_page_p function should only be used if
    //                 a page_p was initialized with page_p(page_s* s).
    void                         clear_page_p() {_pp = 0;}
    latch_mode_t                 latch_mode() const;
    bool                         check_lsn_invariant() const;

    enum { _hdr_size = (page_sz - data_sz - 2 * sizeof (slot_t )) };

    static smsize_t                hdr_size() {
        return _hdr_size;
    }

    // this is used by du/df to get page statistics DU DF
    void                        page_usage(
        int&                            data_sz,
        int&                            hdr_sz,
        int&                            unused,
        int&                             alignmt,
        tag_t&                             t,
        slotid_t&                     no_used_slots);

    rc_t                         split_slot(
                                    slotid_t idx, 
                                    slot_offset_t off, 
                                    const cvec_t& v1,
                                    const cvec_t& v2);
    rc_t                        merge_slots(
                                    slotid_t idx, 
                                    slot_offset_t off1, 
                                    slot_offset_t off2);
    rc_t                        shift(
                                    slotid_t idx2, 
                                    slot_offset_t off2, 
                                    slot_length_t len2, //from-slot
                                    slotid_t idx1,  
                                    slot_offset_t off1        // to-slot
                                );

    tag_t                       tag() const;

private:
    w_rc_t                         _copy(const page_p& p) ;
    void                        _shift_compress(slotid_t from, 
                                    slot_offset_t     from_off, 
                                    slot_length_t     from_len,
                                    slotid_t          to, 
                                    slot_offset_t     to_off);


protected:
    struct splice_info_t {
        slot_length_t start; // offset INTO slot
        slot_length_t len;
        const vec_t& data;
        
        splice_info_t(int s, int l, const vec_t& d) : 
                start(s), len(l), data(d)        {};
    };
    
    
    // If a page type doesn't define its 4-argument format(), this
    // method is used by default.  Nice C++ trick, but it's a bit
    // obfuscating, and since the MAKEPAGE macro forces a declaration,
    // if not definition, of the page-type-specific format(), I'm going
    // to rename this _format.
    // Update: I'm removing the log_it argument because it's never used,
    // that is, is always false. 
    
    rc_t                         _format(
        const lpid_t&                     pid,
        tag_t                             tag,
        w_base_t::uint4_t                 page_flags,
        store_flag_t                      store_flags
        );
    rc_t                        link_up(shpid_t prev, shpid_t next);
    
    rc_t                        next_slot(
        w_base_t::uint4_t             space_needed, 
        slotid_t&                     idx
        );
    rc_t                        find_slot(
        w_base_t::uint4_t            space_needed, 
        slotid_t&                    idx,
        slotid_t                     start_search = 0);

    rc_t                        insert_expand(
        slotid_t                     idx,
        int                          cnt, 
        const cvec_t                 *tp, 
        bool                         log_it = true,
        bool                         do_it = true);
    
    rc_t                        remove_compress(slotid_t idx, int cnt);
    rc_t                        mark_free(slotid_t idx);


    // reclaim a slot
    rc_t                        reclaim(slotid_t idx, const cvec_t& vec, 
        bool                        log_it = true);

    rc_t                        set_byte(slotid_t idx, u_char bits,
                                        logical_operation op);
    
    rc_t                        splice(slotid_t idx, 
                                       int cnt, 
                                       splice_info_t sp[]);
    rc_t                        splice(
        slotid_t                       idx,
        slot_length_t                  start,
        slot_length_t                  len, 
        const cvec_t&                  data);

    rc_t                        overwrite(
        slotid_t                       idx,
        slot_length_t                  start,
        const cvec_t&                  data);

    rc_t                        paste(slotid_t idx, 
        slot_length_t                  start, 
        const cvec_t&                  data);

    rc_t                        cut(slotid_t idx, 
                                       slot_length_t start, 
                                       slot_length_t len);

    bool                        fits() const;

    /*  
     * DATA 
     */
    page_s*                     _pp;
    latch_mode_t                _mode;
    int                         _refbit;
    page_bucket_info_t          page_bucket_info;

private:

    void                        _compress(slotid_t idx = -1);

    friend class page_link_log;
    friend class page_insert_log;
    friend class page_remove_log;
    friend class page_splice_log;
    friend class page_splicez_log;
    friend class page_set_byte_log;
    friend class page_set_bit_log;
    friend class page_clr_bit_log;
    friend class page_reclaim_log;
    friend class page_mark_log;
    friend class page_init_log;
    friend class page_mark_t;
    friend class page_init_t;
    friend class page_insert_t;
    friend class page_image_top_log;
    friend class page_image_bottom_log;

};

#define MAKEPAGE(x, base,_refbit_)                                              \
x()  {};                                                                           \
x(page_s* s, w_base_t::uint4_t store_flags) : base(s, store_flags)                      \
{                                                                              \
    /*assert3(tag() == t_ ## x)*/                                              \
}                                                                              \
                                                                              \
~x()  {};                                                                      \
x& operator=(const x& p)    { base::operator=(p); return *this; }              \
rc_t _fix(bool conditional, const lpid_t& pid, latch_mode_t mode,              \
        w_base_t::uint4_t page_flags,                                                   \
        store_flag_t store_flags,                                             \
        bool ignore_store_id,                                                 \
        int                      refbit);                                      \
rc_t fix(const lpid_t& pid, latch_mode_t mode,                                      \
        w_base_t::uint4_t page_flags = 0,                                               \
        store_flag_t store_flags = st_bad,                                    \
        bool ignore_store_id = false,                                         \
        int                      refbit = _refbit_);                              \
rc_t conditional_fix(const lpid_t& pid, latch_mode_t mode,                    \
        w_base_t::uint4_t page_flags = 0,                                               \
        store_flag_t store_flags = st_bad,                                    \
        bool ignore_store_id = false,                                         \
        int                      refbit = _refbit_);                              \
void destructor()  {base::destructor();}                                      \
rc_t format(const lpid_t& pid, tag_t tag, w_base_t::uint4_t page_flags,       \
             store_flag_t store_flags);                                            \
       x(const x&p) : base(p)                                                 \
{ /* assert3(tag() == t_ ## x) */  } 


#define MAKEPAGECODE(x, base)                                                 \
rc_t x::fix(const lpid_t& pid, latch_mode_t mode,                             \
        w_base_t::uint4_t page_flags,                                         \
        store_flag_t store_flags ,                                            \
        bool ignore_store_id ,                                                \
        int           refbit ){                                               \
            return _fix(false, pid, mode, page_flags, store_flags,            \
                    ignore_store_id, refbit);                                 \
        }                                                                     \
rc_t x::conditional_fix(const lpid_t& pid, latch_mode_t mode,                 \
        w_base_t::uint4_t page_flags,                                         \
        store_flag_t store_flags ,                                            \
        bool ignore_store_id ,                                                \
        int  refbit ){                                                        \
            return _fix(true, pid, mode, page_flags, store_flags,             \
                                                 ignore_store_id, refbit);    \
        }                                                                     \
rc_t x::_fix(bool condl, const lpid_t& pid, latch_mode_t mode,                \
                 w_base_t::uint4_t page_flags,                                \
                 store_flag_t store_flags,                                    \
                 bool ignore_store_id,                                        \
                 int refbit)                                                  \
{                                                                             \
    store_flag_t store_flags_save = store_flags;                              \
    w_assert2((page_flags & ~t_virgin) == 0);                                 \
    W_DO( page_p::_fix(condl, pid, t_ ## x, mode, page_flags, store_flags,    \
                                                     ignore_store_id,refbit));\
    if (page_flags & t_virgin)   W_DO(format(pid, t_ ## x, page_flags,        \
                                                     store_flags_save));      \
    w_assert3(tag() == t_ ## x);                                              \
    return RCOK;                                                              \
} 

inline smsize_t 
page_p::free_space4bucket() const {
    // Let the used space be the most liberal number
    return (_pp->space.nfree() - _pp->space.nrsvd())
                        + _pp->space.xct_rsvd();
}

inline space_bucket_t 
page_p::bucket() const {
    return free_space2bucket( free_space4bucket() );
}

inline shpid_t
page_p::next() const 
{
    return _pp->next;
}

inline shpid_t
page_p::prev() const
{
    return _pp->prev;
}

inline const lpid_t&
page_p::pid() const
{
    return _pp->pid;
}

inline void
page_p::set_vid(vid_t vid)
{
    _pp->pid._stid.vol = vid;
}

inline const tid_t & 
page_p::tid() const
{
    return _pp->space.tid(); 
}
inline smsize_t 
page_p::xct_rsvd() const
{
    return _pp->space.xct_rsvd(); 
}
inline smsize_t 
page_p::nrsvd() const
{
    return _pp->space.nrsvd(); 
}
inline smsize_t 
page_p::nfree() const
{
    return _pp->space.nfree(); 
}

inline smsize_t 
page_p::used_space()
{
    return (data_sz + 2 * sizeof(slot_t) - _pp->space.usable(xct())); 
}

inline smsize_t
page_p::usable_space()
{
    return _pp->space.usable(xct()); 
}

inline smsize_t
page_p::usable_space_for_slots() const
{
    return _pp->space.usable_for_new_slots(); 
}

inline smsize_t
page_p::tuple_size(slotid_t idx) const
{
    w_assert3(idx >= 0 && idx < _pp->nslots);
    return _pp->slot(idx).length;
}

inline void*
page_p::tuple_addr(slotid_t idx) const
{
    w_assert3(idx >= 0 && idx < _pp->nslots);
    return (void*) (_pp->data() + _pp->slot(idx).offset);
}

inline bool
page_p::is_tuple_valid(slotid_t idx) const
{
    return (idx >= 0) && 
        (idx < _pp->nslots) && 
        (_pp->slot(idx).offset >=0);
}

inline w_base_t::w_base_t::uint4_t
page_p::page_flags() const
{
    return _pp->page_flags;
}

inline page_s&
page_p::persistent_part()
{
    return *(page_s*) _pp;
}

inline const page_s&
page_p::persistent_part_const() const
{
    return *(page_s*) _pp; 
}

inline bool
page_p::is_fixed() const
{
#if W_DEBUG_LEVEL > 1
    // The problem here is that _pp might be a
    // heap-based page_s, not a buffer-pool frame.
    // Let's call this iff it's a known frame:
    if(_pp && bf_m::get_cb(_pp)) w_assert1(is_latched_by_me());
#endif
    return _pp != 0;
}

inline latch_mode_t
page_p::latch_mode() const
{
#if W_DEBUG_LEVEL > 1
    // The problem here is that I might have more than one
    // holding of the latch at some point, starting out with
    // SH and then upgrading (possibly via double-acquire) it.  
    // The latch itself doesn't hold the mode.
    // of the identities of the holders. But now the page_p thinks
    // its mode is not what the actual latch's mode is.
    // Rather than try to update the page_p's mode, we'll
    // just relax the assert here.
    // if(_pp) w_assert2( ((_mode == LATCH_EX) == mine) || times>1);
    if(_pp) {
        bool mine = is_mine();
        if(_mode == LATCH_EX) w_assert2(mine); 
    }
#endif
    return _pp ? _mode : LATCH_NL;
}

inline bool 
page_p::check_lsn_invariant() const
{
    if(_pp) return bf_m::check_lsn_invariant(_pp);
    return true;
}

inline page_p::tag_t
page_p::tag() const
{
    return (tag_t) _pp->tag;
}

inline void        
page_p::init_bucket_info() { 
    init_bucket_info(tag(), 0); 
}


/*--------------------------------------------------------------*
 *  page_p::nslots()                                                *
 *--------------------------------------------------------------*/
inline slotid_t
page_p::nslots() const
{
    return _pp->nslots;
}

/*--------------------------------------------------------------*
 *  page_p::nvacant()                                           *
 *--------------------------------------------------------------*/
inline slotid_t
page_p::nvacant() const
{
    return _pp->nvacant;
}

/*--------------------------------------------------------------*
 *  page_p::lsn()                                                *
 *--------------------------------------------------------------*/
inline const lsn_t& 
page_p::lsn() const
{
    w_assert1(_pp->lsn1 == _pp->lsn2);
    return _pp->lsn1;
}

/*--------------------------------------------------------------*
 *  page_p::set_lsn()                                                *
 *--------------------------------------------------------------*/
inline void 
page_p::set_lsns(const lsn_t& lsn)
{
    _pp->lsn1 = _pp->lsn2 = lsn;
}

/*--------------------------------------------------------------*
 *  page_p::contig_space()                                        *
 *--------------------------------------------------------------*/
inline smsize_t
page_p::contig_space()        
{ 
    return ((char*) &_pp->slot(_pp->nslots-1)) - (_pp->data() + _pp->end); 
}

/*--------------------------------------------------------------*
 *  page_p::paste()                                                *
 *--------------------------------------------------------------*/
inline rc_t
page_p::paste(slotid_t idx, slot_length_t start, const cvec_t& data)
{
    return splice(idx, start, 0, data);
}

/*--------------------------------------------------------------*
 *  page_p::cut()                                                *
 *--------------------------------------------------------------*/
inline rc_t
page_p::cut(slotid_t idx, slot_length_t start, slot_length_t len)
{
    cvec_t v;
    return splice(idx, start, len, v);
}


/*--------------------------------------------------------------*
 *  page_p::discard()                                                *
 *--------------------------------------------------------------*/
inline void 
page_p::discard()
{
    w_assert3(!_pp || bf->is_bf_page(_pp));
    if (_pp)  bf->discard_pinned_page(_pp);
    _pp = 0;
}

/*********************************************************************
 *
 *  page_p::rsvd_mode()
 *  page_p::rsvd_mode(tag_t)
 *
 *  Determine whether slots/space must be reserved in a page
 *  For now, just file pages need this.
 *
 *********************************************************************/
inline bool
page_p::rsvd_mode(tag_t t) 
{
    if (t == t_file_p || t == t_file_mrbt_p || t == t_ranges_p) {
        return true;
    }
    return false;
}

inline bool
page_p::rsvd_mode()  const
{
    return rsvd_mode(tag());
}

/*--------------------------------------------------------------*
 *  page_p::unfix()                                                *
 *--------------------------------------------------------------*/
inline void 
page_p::unfix()
{
    w_assert2(!_pp || bf->is_bf_page(_pp, true));
    if(_pp) {
        W_COERCE(update_bucket_info());
        bf->unfix(_pp, false, _refbit);
    }
    _pp = 0;
}

/*--------------------------------------------------------------*
 *  page_p::unfix_dirty()                                        *
 *--------------------------------------------------------------*/
inline void
page_p::unfix_dirty()
{
    w_assert2(!_pp || bf->is_bf_page(_pp));
    if (_pp)  {
        W_COERCE(update_bucket_info());
        bf->unfix(_pp, true, _refbit);
    }
    _pp = 0;
}

/*--------------------------------------------------------------*
 *  page_p::set_dirty()                                                *
 *--------------------------------------------------------------*/
inline void
page_p::set_dirty() const
{
    if (bf->is_bf_page(_pp))  W_COERCE(bf->set_dirty(_pp));
}

/*--------------------------------------------------------------*
 *  page_p::is_dirty()                                                *
 *  for debugging                                               *
 *--------------------------------------------------------------*/
inline bool
page_p::is_dirty() const
{
    if (bf->is_bf_page(_pp))  return bf->is_dirty(_pp);
    return false;
}

/*--------------------------------------------------------------*
 *  page_p::overwrite()                                                *
 *--------------------------------------------------------------*/
inline rc_t
page_p::overwrite(slotid_t idx, slot_length_t start, const cvec_t& data)
{
    return splice(idx, start, data.size(), data);
}

/*--------------------------------------------------------------*
 *  page_p::destructor()                                        *
 *--------------------------------------------------------------*/
inline void
page_p::destructor()
{
    if (bf->is_bf_page(_pp))  unfix();
    _pp = 0;
}


/**\brief This class allows the caller of 
 * alloc_pages_in_extent to vote yea or nay on a page that
 * the volume layer might find.
  */ 
class alloc_page_filter_t : public smlevel_0 {
public:
    NORET          alloc_page_filter_t() {}
    virtual  NORET ~alloc_page_filter_t() {}

    virtual bool  accept(const lpid_t&) = 0;
    virtual void  check() const = 0 ;
    virtual bool  accepted() const  = 0;
    virtual void  reject()  = 0;
};

class alloc_page_filter_yes_t: public alloc_page_filter_t {
public:
    bool  accept(const lpid_t&) { return true; }
    void  check() const {}
    bool  accepted() const  { return true; }
    void  reject() {}
    NORET ~alloc_page_filter_yes_t() {}
};

/*<std-footer incl-file-exclusion='PAGE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
