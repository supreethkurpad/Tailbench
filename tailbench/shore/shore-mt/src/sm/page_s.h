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

/*<std-header orig-src='shore' incl-file-exclusion='PAGE_S_H'>

 $Id: page_s.h,v 1.33 2010/05/26 01:20:40 nhall Exp $

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

#ifndef PAGE_S_H
#define PAGE_S_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif


class xct_t;
/**\brief Basic page structure for all pages.
 * \details
 * These are persistent things. There is no hierarchy here
 * for the different page types. All the differences between
 * page types are handled by the handle classes, page_p and its
 * derived classes.
 */
class page_s {
public:
    typedef int2_t  slot_offset_t; // -1 if vacant
    typedef uint2_t slot_length_t;
    typedef slot_offset_t  page_bytes_t; // to avoid explicit sized-types below
    typedef slot_offset_t  slot_index_t; // to avoid explicit sized-types below
    struct slot_t {
        slot_offset_t offset;        // -1 if vacant
        slot_length_t length;
    };
    
    class space_t {
        public:
        space_t()     {};
        ~space_t()    {};
        
        // called on page format, rflag is true for rsvd_mode() pages
        void init_space_t(int nfree, bool rflag)  { 
            _tid = tid_t(0, 0);
            _nfree = nfree;
            _nrsvd = _xct_rsvd = 0;

            _rflag = rflag;
        }
        
        int nfree() const    { return _nfree; }
        // rflag: true for rsvd_mode() pages, that is pages
        // that have space reservation, namely file pages.
        // Space reservation is all about maintaining slot ids.
        bool rflag() const    { return _rflag!=0; }
        
        int             usable_for_new_slots() const {
                            return nfree() - nrsvd();
                        }
        int             usable(xct_t* xd); // might free space
                        // slot_bytes means bytes for new slots
        rc_t            acquire(int amt, int slot_bytes, xct_t* xd,
                                        bool do_it=true);
        void            release(int amt, xct_t* xd);
        void            undo_acquire(int amt, xct_t* xd);
        void            undo_release(int amt, xct_t* xd);
        const tid_t&    tid() const { return _tid; }
        page_bytes_t    nrsvd() const { return _nrsvd; }
        page_bytes_t    xct_rsvd() const { return _xct_rsvd; }


        private:
       
        void _check_reserve();
        
        tid_t    _tid;        // youngest xct contributing to _nrsvd
        /* NB: this use of int2_t prevents us from having 65K pages */
#if SM_PAGESIZE > 32768 
#error Page sizes this big are not supported.
#endif
        page_bytes_t    _nfree;        // free space counter
        page_bytes_t    _nrsvd;        // reserved space counter
        page_bytes_t    _xct_rsvd;    // amt of space contributed by _tid to _nrsvd
        page_bytes_t    _rflag; 
    }; // space_t

    enum {
        footer_sz = (0
              + 2 * sizeof(slot_t) // reserved_slot and slot
              + sizeof(lsn_t)  // lsn2
              ),

        _hdr_sz = (0
              + sizeof(lsn_t)  // lsn1
              + sizeof(lpid_t)     // pid
              + 2 * sizeof(shpid_t)// next, prev 
              + sizeof(uint2_t)     // tag
              + sizeof(fill2)     // _fill2
              + sizeof(space_t)    // space
              + sizeof(slot_index_t)// nslots
              + 2 * sizeof(slot_offset_t)// end, nvacant, 
              + sizeof(fill2) // _fill2b
              + sizeof(w_base_t::uint4_t) // _private_store_flags
              + sizeof(w_base_t::uint4_t) // page_flags
              + 0),
        hdr_sz = (_hdr_sz + align(footer_sz)),
        data_sz = smlevel_0::page_sz - hdr_sz,
        max_slot = data_sz / sizeof(slot_t) + 2
    };

 
    /* NOTE: you can verify these offsets with smsh: enter "sm sizeof" */
    /* offset 0 */
    lsn_t    lsn1;
    /* 8 bytes: offset 8 */
    lpid_t    pid;            // id of the page
    /* 12 bytes: offset 20 */
    shpid_t    next;          // next page
    /* 4 bytes: offset 24 */
    shpid_t    prev;          // previous page
    /* 4 bytes: offset 28 */
    uint2_t    tag;            // page_p::tag_t
    /* 2 bytes: offset 30 */
    fill2      _fill2;  
    /* 2 bytes: offset 32 */

    space_t     space;         // space management
    /* 16 bytes: offset 48 */

    slot_offset_t  end;        // offset to end of data area
    /* 2 bytes: offset 50 */
    slot_index_t  nslots;     // number of slots
    /* 2 bytes: offset 52 */
    slot_offset_t  nvacant;     // number of vacant slots
    /* 2 bytes: offset 54 */
    fill2      _fill2b;        
    /* 2 bytes: offset 56 */
    w_base_t::uint4_t    _private_store_flags;        // page_p::store_flag_t
    /* 4 bytes: offset 60 */

    w_base_t::uint4_t    get_page_storeflags() const { return _private_store_flags;}
    w_base_t::uint4_t    set_page_storeflags(uint4_t f) { 
                         return (_private_store_flags=f);}

    w_base_t::uint4_t    page_flags;        // page_p::page_flag_t
    /* 4 bytes: offset 64 */
    /* MUST BE 8-BYTE ALIGNED HERE */
    union slot_array {
	char     data[data_sz];
	slot_t   slot[max_slot];
    } _slots;

    slot_t &slot(slotid_t idx) { return _slots.slot[max_slot-idx-1]; }
    char *data() { return _slots.data; }
    
    slot_t const &slot(slotid_t idx) const { return _slots.slot[max_slot-idx-1]; }
    char const *data() const { return _slots.data; }
    
    /* offset 8184 */
    /* NOTE: lsn_t now requires 8-byte alignment! */
    lsn_t    lsn2;

    page_s() { }
    ~page_s() { }
};


/*<std-footer incl-file-exclusion='PAGE_S_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
