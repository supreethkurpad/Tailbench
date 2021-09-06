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

/*<std-header orig-src='shore' incl-file-exclusion='LOCK_S_inline_H'>

 $Id: lock_s_inline.h,v 1.13 2010/06/08 22:28:55 nhall Exp $

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

#ifndef LOCK_S_inline_H
#define LOCK_S_inline_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 *  STRUCTURE of a lockid_t
 *
 *    word 0: | lspace  s[0]  |   volid s[1] |
 *    word 1: |       store number           |
 *    word 2: |  page number <or> kvl.g      |
 *    word 3: union {
 *              |  slotid_t s[6] |   0         |
 *              |         kvl.h   w[3]         |    
 *        }
 */

#if W_DEBUG_LEVEL > 0
// This was #included in lock.cpp
#define inline
#else
// This was #included in lock_s.h
#define inline inline
#endif

inline
uint1_t             
lockid_t::lspace_bits() const 
{
    // use high byte
    return (s[0] >> 8);
}
inline
lockid_t::name_space_t         
lockid_t::lspace() const 
{
    return  (lockid_t::name_space_t) lspace_bits();
}

inline
void             
lockid_t::set_lspace(lockid_t::name_space_t value) 
{
    // lspace is high byte
    uint1_t low_byte = s[0] & 0xff;
    w_assert9((low_byte == 1) || (low_byte == 0));
    s[0] = (value << 8) | low_byte;
}

inline
uint2_t             
lockid_t::slot_bits() const 
{
    return  s[slotSindex];
}

inline
uint4_t             
lockid_t::slot_kvl_bits() const 
{
    return  w[slotWindex];
}

inline
slotid_t
lockid_t::slot() const 
{
    w_assert9((s[0]&0xff) == 0);
    return  slotid_t(slot_bits()) ;
}

inline
void             
lockid_t::set_slot(const slotid_t & e) 
{
    w_assert9((s[0]&0xff) == 0);
    w_assert9(sizeof(slotid_t) == sizeof(s[slotSindex]));
    w[slotWindex] = 0; // clear lower part
    s[slotSindex] = e;
}

inline
lockid_t::page_bits_t
lockid_t::page_bits() const 
{
    w_assert9(sizeof(shpid_t) == sizeof(w[pageWindex]));
    w_assert9(sizeof(uint4_t) == sizeof(w[pageWindex]));
    return w[pageWindex];
}

inline
void
lockid_t::set_page_bits(lockid_t::page_bits_t bits) 
{
    DBG(<<"lockid_t::set_page_bits() " << "bits= "  << bits);
    w_assert9(sizeof(shpid_t) == sizeof(w[pageWindex]));
    w_assert9(sizeof(page_bits_t) == sizeof(w[pageWindex]));
    w[pageWindex] = bits;
}

inline
const shpid_t&         
lockid_t::page() const 
{
    w_assert9(lspace() != t_extent);
    w_assert9((s[0]&0xff) == 0);
    w_assert9(sizeof(shpid_t) == sizeof(w[pageWindex]));
    return *(shpid_t *) (&w[pageWindex]);

}

inline
void             
lockid_t::set_page(const shpid_t & p) 
{
    w_assert9(lspace() != t_extent);
    w_assert9((s[0]&0xff) == 0); // low byte
    set_page_bits((page_bits_t)p);
}

inline
vid_t             
lockid_t::vid() const 
{
    return (vid_t) s[1];
}

inline
void             
lockid_t::set_vid(const vid_t & v) 
{
    w_assert9(sizeof(vid_t) == sizeof(s[1]));
    s[1] = (uint2_t) v.vol;
}

inline
uint2_t             
lockid_t::vid_bits() const 
{
    return s[1];
}

inline
const snum_t&         
lockid_t::store() const 
{
    w_assert9(sizeof(snum_t) == sizeof(w[1]));
    w_assert9(lspace() != t_extent);
    w_assert9((s[0]&0xff) == 0); // low byte
    return *(snum_t *) (&w[1]);
}

inline
void             
lockid_t::set_snum(const snum_t & _s) 
{
    w_assert9(sizeof(snum_t) == sizeof(w[1]));
    w_assert9(lspace() != t_extent);
    w_assert9((this->s[0]&0xff) == 0); // low byte
    w[1] = (uint4_t) _s;
}

inline
void             
lockid_t::set_store(const stid_t & _s) 
{
    w_assert9(lspace() != t_extent);
    w_assert9((this->s[0]&0xff) == 0); // low byte
    set_snum(_s.store);
    set_vid(_s.vol);
}

inline
uint4_t             
lockid_t::store_bits() const 
{
    w_assert9(sizeof(uint4_t) == sizeof(w[1]));
    w_assert9(((s[0]&0xff) == 1) || ((s[0]&0xff) == 0));
    // w_assert9(lspace() != t_extent);
    return w[1];
}

inline
const extnum_t&         
lockid_t::extent() const 
{
    w_assert9(sizeof(extnum_t) == sizeof(w[1]));
    w_assert9(lspace() == t_extent);
    w_assert9(((s[0]&0xff) == 1) || ((s[0]&0xff) == 0));
    return *(extnum_t *) (&w[1]);
}

inline
void             
lockid_t::set_extent(const extnum_t & e) 
{
    w_assert9(sizeof(extnum_t) == sizeof(w[1]));
    w_assert9(lspace() == t_extent);
    w_assert9(((s[0]&0xff) == 1) || ((s[0]&0xff) == 0));
    w[1] = (uint4_t) e;
}

inline
uint4_t             
lockid_t::extent_bits() const 
{
    w_assert9(sizeof(uint4_t) == sizeof(w[1]));
    w_assert9(lspace() == t_extent);
    w_assert9(((s[0]&0xff) == 1) || ((s[0]&0xff) == 0));
    return w[1];
}

inline
void            
lockid_t::set_ext_has_page_alloc(bool value) 
{
    // low byte
    w_assert9(lspace() == t_extent);
    w_assert9(((s[0]&0xff) == 1) || ((s[0]&0xff) == 0));
    DBG(<<"s[0] = " << s[0] << " set low byte to " << value);
    uint2_t high_byte = s[0] & 0xff00;
    if(value) {
        s[0] = high_byte | 0x1;
    } else {
        s[0] = high_byte | 0x0;
    }
    w_assert9(((s[0]&0xff) == 1) || ((s[0]&0xff) == 0));
    DBG(<<"s[0] = " << (unsigned int)s[0]);
    w_assert9(lspace() == t_extent);
}

inline
bool            
lockid_t::ext_has_page_alloc() const 
{
    w_assert9(lspace() == t_extent);
    w_assert9(((s[0]&0xff) == 1) || ((s[0]&0xff) == 0));
    return ((s[0] & 0xff) != 0);
}

inline
void            
lockid_t::extract_extent(extid_t &e) const 
{
    w_assert9( lspace() == t_extent);
    w_assert9(((s[0]&0xff) == 1) || ((s[0]&0xff) == 0));
    e.vol = vid();
    e.ext = extent();
}

inline
void            
lockid_t::extract_stid(stid_t &_s) const 
{
    w_assert9(
    lspace() == t_store || 
    lspace() == t_page || 
    lspace() == t_kvl || 
    lspace() == t_record);
    w_assert9((this->s[0]&0xff) == 0);
    _s.vol = vid();
    _s.store = store();
}

inline
void            
lockid_t::extract_lpid(lpid_t &p) const 
{
    w_assert9(lspace() == t_page || lspace() == t_record);
    w_assert9((s[0]&0xff) == 0);
    extract_stid(p._stid);
    p.page = page();
}

inline
void            
lockid_t::extract_rid(rid_t &r) const 
{
    w_assert9(lspace() == t_record);
    w_assert9((s[0]&0xff) == 0);
    extract_lpid(r.pid);
    r.slot = slot();
}

inline
void            
lockid_t::extract_kvl(kvl_t &k) const 
{
    w_assert9((s[0]&0xff) == 0);
    w_assert9(lspace() == t_kvl);
    extract_stid(k.stid);
    k.h = w[pageWindex];
    k.g = w[slotWindex];
}

inline
void
lockid_t::extract_user1(user1_t &u) const
{
    w_assert9((s[0]&0xff) == 0);
    w_assert9(
    lspace() == t_user1 ||
    lspace() == t_user2 ||
    lspace() == t_user3 ||
    lspace() == t_user4);
    u.u1 = s[1];
}

inline
void
lockid_t::extract_user2(user2_t &u) const
{
    w_assert9(
    lspace() == t_user2 ||
    lspace() == t_user3 ||
    lspace() == t_user4);
    extract_user1(u);
    u.u2 = w[1];
}

inline
void
lockid_t::extract_user3(user3_t &u) const
{
    w_assert9(
    lspace() == t_user3 ||
    lspace() == t_user4);
    extract_user2(u);
    u.u3 = w[pageWindex];
}

inline
void
lockid_t::extract_user4(user4_t &u) const
{
    w_assert9(lspace() == t_user4);
    extract_user3(u);
    u.u4 = w[user4Windex];
}

inline
bool
lockid_t::is_user_lock() const
{
    return (lspace() >= t_user1 && lspace() <= t_user4);
}

inline
uint2_t
lockid_t::u1() const
{
    return s[1];
}

inline
void
lockid_t::set_u1(uint2_t u)
{
    s[1] = u;
}

inline
uint4_t
lockid_t::u2() const
{
    return w[1];
}

inline
void
lockid_t::set_u2(uint4_t u)
{
    w[1] = u;
}

inline
uint4_t
lockid_t::u3() const
{
    return w[pageWindex];
}

inline
void
lockid_t::set_u3(uint4_t u)
{
    w[pageWindex] = u;
}

inline
uint4_t
lockid_t::u4() const
{
    return w[user4Windex];
}

inline
void
lockid_t::set_u4(uint4_t u)
{
    w[user4Windex] = u;
}



inline void
lockid_t::zero()
{
    // equiv of
    // set_lspace(t_bad);
    // set-ext_has_page_alloc(0);
    s[0] = (t_bad << 8);

    set_vid(0);
    set_snum(0);
    set_page(0);
    // set_slot(0);
    w[user4Windex] = 0; // have to get entire thing
}


inline NORET
lockid_t::lockid_t()
{
    zero(); 
}

inline NORET
lockid_t::lockid_t(const vid_t& vid)
{
    zero();
    set_lspace(t_vol);
    set_vid(vid);
}

inline NORET
lockid_t::lockid_t(const extid_t& extid)
{
    zero();
    set_lspace(t_extent);
    set_vid(extid.vol);
    set_extent(extid.ext);
}

inline NORET
lockid_t::lockid_t(const stid_t& stid)
{
    zero();
    set_lspace(t_store);
    set_vid(stid.vol);
    set_snum(stid.store);
}

inline NORET
lockid_t::lockid_t(const lpid_t& lpid)
{
    zero();
    set_lspace(t_page);
    set_vid(lpid.vol());
    set_snum(lpid.store());
    set_page(lpid.page);
}

inline NORET
lockid_t::lockid_t(const rid_t& rid)
{
    zero();
    set_lspace(t_record);
    // w[1-3] is assumed (elsewhere) to
    // look just like the following sequence
    // (which is the beginning of a rid_t-- see sm_s.h):
    // shpid_t    page;
    // snum_t    store;
    // slotid_t    slot;
    set_vid(rid.pid.vol());
    set_snum(rid.pid.store());
    set_page(rid.pid.page);
    set_slot(rid.slot);
}

inline NORET
lockid_t::lockid_t(const kvl_t& kvl)
{
    zero();
    set_lspace(t_kvl);
    w_assert9(sizeof(kvl_t) == sizeof(w[1])*2 + sizeof(stid_t));
    set_store(kvl.stid);

    page_bits_t bits = kvl.h;
    set_page_bits(bits);
    w[slotWindex] = kvl.g;
}

inline NORET
lockid_t::lockid_t(const user1_t& u)
{
    zero();
    set_lspace(t_user1);
    s[1] = u.u1;
}

inline NORET
lockid_t::lockid_t(const user2_t& u)
{
    zero();
    set_lspace(t_user2);
    s[1] = u.u1;
    w[1] = u.u2;
}

inline NORET
lockid_t::lockid_t(const user3_t& u)
{
    zero();
    set_lspace(t_user3);
    s[1] = u.u1;
    w[1] = u.u2;
    w[pageWindex] = u.u3;
}

inline NORET
lockid_t::lockid_t(const user4_t& u)
{
    zero();
    set_lspace(t_user4);
    s[1] = u.u1;
    w[1] = u.u2;

    page_bits_t tmp = u.u3;
    set_page_bits(tmp);

    w[slotWindex] = u.u4;
}


inline lockid_t&
lockid_t::operator=(const lockid_t& i)
{
    /* Could use set_xxx(xxx_bits()) but
    * then we have to switch on lspace()
    * in order to cope with extent and kvl
    * special cases 
    */
    w[0] = i.w[0]; 
    w[1] = i.w[1]; 
    set_page_bits(i.page_bits());
    w[slotWindex] = i.w[slotWindex]; 
    return *this;
}

#define COMPARE_LT(a, b) { if(a < b) return true; else if(b < a) return false; }
inline bool
lockid_t::operator<(lockid_t const &lid) const
{

  COMPARE_LT(lspace_bits(), lid.lspace_bits());
  COMPARE_LT(vid_bits(), lid.vid_bits());
  COMPARE_LT(store_bits(), lid.store_bits());
  COMPARE_LT(page_bits(), lid.page_bits());
  COMPARE_LT(slot_kvl_bits(), lid.slot_kvl_bits());
  return false;
}

inline bool
lockid_t::operator==(const lockid_t& lid) const
{
    // ext_has_page_alloc() is true if extent has pages allocated
    // ext_has_page_alloc() does not participate in testing for equality
    return !((lspace_bits() ^ lid.lspace_bits()) | 
    (vid_bits() ^ lid.vid_bits()) | 
    (store_bits() ^ lid.store_bits())  |
    (page_bits() ^ lid.page_bits())   |
    (slot_kvl_bits() ^ lid.slot_kvl_bits()) );
    /*
    the above is the same as the following, but runs 
    faster since it doesn't have conditions on the &&

    return ((lspace_bits() == l.lspace_bits()) && 
        (vid_bits() == l.vid_bits()) &&
        (store_bits() == l.store_bits())  &&
        (page_bits() == l.page_bits())   &&
        (slot_kvl_bits() == l.slot_kvl_bits()) );
    */
}

inline NORET
lockid_t::lockid_t(const lockid_t& i)
{
    (void) this->operator=(i);
}

inline bool
lockid_t::operator!=(const lockid_t& lid) const
{
    return ! (*this == lid);
}


/*
 * Lock ID hashing function
 * This hash function is used for lock caching.
 */


#include <w_hashing.h>

static w_hashing::uhash lockhashfunc;
inline uint4_t
lockid_t::hash() const
{
    // uhash(x) returns a u64.
    // While we'd like to be able to do this:
    // return uint4_t(lockhashfunc(l[0]) + lockhashfunc(l[1]));
    // we cannot include s[0] in its entirety, because it contains
    // the page-alloc bit (or not).
    return uint4_t(
            lockhashfunc(lspace_bits()) + 
            lockhashfunc(w[1]) + 
            lockhashfunc(l[1]));
    
}


inline
ostream& operator<<(ostream& o, const lockid_t::user1_t& u)
{
    return o << "u1(" << u.u1 << ")";
}

inline
ostream& operator<<(ostream& o, const lockid_t::user2_t& u)
{
    return o << "u2(" << u.u1 << "," << u.u2 << ")";
}

inline
ostream& operator<<(ostream& o, const lockid_t::user3_t& u)
{
    return o << "u3(" << u.u1 << "," << u.u2 << "," << u.u3 << ")";
}

inline
ostream& operator<<(ostream& o, const lockid_t::user4_t& u)
{
    return o << "u4(" << u.u1 << "," << u.u2 << "," << u.u3 << "," << u.u4 << ")";
}


/*<std-footer incl-file-exclusion='LOCK_S_inline_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
