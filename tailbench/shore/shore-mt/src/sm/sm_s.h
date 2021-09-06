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

/*<std-header orig-src='shore' incl-file-exclusion='SM_S_H'>

 $Id: sm_s.h,v 1.91 2010/05/26 01:20:43 nhall Exp $

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

#ifndef SM_S_H
#define SM_S_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/**\brief Extent number.
 * \details
 * Used in extid_t.
 */
typedef w_base_t::uint4_t        extnum_t;

#ifndef STHREAD_H
#include <sthread.h>
#endif

#ifndef STID_T_H
#include <stid_t.h>
#endif

#ifdef __GNUG__
#pragma interface
#endif

/**\brief Extent ID.
 * \details
 * Identifies an extent of a volume.  Needed to
 * acquire an extent lock.  Most of the internal interfaces use
 * the extnum_t since the volume is implicit.
 */
class extid_t {
public:
    vid_t        vol; 
    fill2        filler; // because vol is 2 bytes & ext is 4
    extnum_t     ext;

    friend ostream& operator<<(ostream&, const extid_t& x);
    friend istream& operator>>(istream&, extid_t &x); 
};

#define LPID_T
class lpid_t {
public:
    stid_t        _stid;
    shpid_t        page;
    
    lpid_t();
    lpid_t(const stid_t& s, shpid_t p);
    lpid_t(vid_t v, snum_t s, shpid_t p);
    //    operator bool() const;
    bool valid() const;

    vid_t         vol()   const {return _stid.vol;}
    snum_t        store() const {return _stid.store;}
    const stid_t& stid() const {return _stid;}

    // necessary and sufficient conditions for
    // is_null() are determined by default constructor, q.v.
    bool        is_null() const { return page == 0; }

    bool operator==(const lpid_t& p) const;
    bool operator!=(const lpid_t& p) const;
    bool operator<(const lpid_t& p) const;
    bool operator<=(const lpid_t& p) const;
    bool operator>(const lpid_t& p) const;
    bool operator>=(const lpid_t& p) const;
    friend ostream& operator<<(ostream&, const lpid_t& p);
    friend istream& operator>>(istream&, lpid_t& p);

    static const lpid_t bof;
    static const lpid_t eof;
    static const lpid_t null;
};



class rid_t;

/**\brief Short Record ID
 *\ingroup IDS
 * \details
 * This class represents a short record identifier, which is
 * used when the volume id is implied somehow.
 *
 * A short record id contains a slot, a (short) page id, and a store number.
 * A short page id is just a page number (in basics.h).
 *
 * See \ref IDS.
 */
class shrid_t {
public:
    shpid_t        page;
    snum_t        store;
    slotid_t        slot;
    fill2        filler; // because page, snum_t are 4 bytes, slotid_t is 2

    shrid_t();
    shrid_t(const rid_t& r);
    shrid_t(shpid_t p, snum_t st, slotid_t sl) : page(p), store(st), slot(sl) {}
    friend ostream& operator<<(ostream&, const shrid_t& s);
    friend istream& operator>>(istream&, shrid_t& s);
};


#define RID_T

/**\brief Record ID
 *\ingroup IDS
 * \details
 * This class represents a long record identifier, used in the
 * Storage Manager API, but not stored persistently.
 *
 * A record id contains a slot and a (long) page id.
 * A long page id contains a store id and a volume id.
 *
 * See \ref IDS.
 */
class rid_t {
public:
    lpid_t        pid;
    slotid_t        slot;
    fill2        filler;  // for initialization of last 2 unused bytes

    rid_t();
    rid_t(vid_t vid, const shrid_t& shrid);
    rid_t(const lpid_t& p, slotid_t s) : pid(p), slot(s) {};

    stid_t stid() const;

    bool operator==(const rid_t& r) const;
    bool operator!=(const rid_t& r) const;
    bool operator<(const rid_t& r) const;
    
    friend ostream& operator<<(ostream&, const rid_t& s);
    friend istream& operator>>(istream&, rid_t& s);

    static const rid_t null;
};

#include <lsn.h>

struct key_type_s {
    enum type_t {
        i = 'i',                // integer (1,2,4)
        I = 'I',                // integer (1,2,4), compressed
        u = 'u',                // unsigned integer (1,2,4)
        U = 'U',                // unsigned integer (1,2,4), compressed
        f = 'f',                // float (4,8)
        F = 'F',                // float (4,8), compressed
        b = 'b',                // binary (uninterpreted) (vbl or fixed-len)
        B = 'B'                        // compressed binary (uninterpreted) (vbl or fixed-len)
        // NB : u1==b1, u2==b2, u4==b4 semantically, 
        // BUT
        // u2, u4 must be  aligned, whereas b2, b4 need not be,
        // AND
        // u2, u4 may use faster comparisons than b2, b4, which will 
        // always use umemcmp (possibly not optimized). 
    };
    enum { max_len = 2000 };
    char        type;
    char        variable; // Boolean - but its size is unpredictable
    uint2_t        length;        
    char        compressed; // Boolean - but its size is unpredictable
#ifdef __GNUG__        /* XXX ZERO_INIT canidate? */
    fill1        dummy; 
#endif

    key_type_s(type_t t = (type_t)0, char v = 0, uint2_t l = 0) 
        : type((char) t), variable(v), length(l), compressed(false)  {};

    // This function parses a key descriptor string "s" and
    // translates it into an array of key_type_s, "kc".  The initial
    // length of the array is passed in through "count" and
    // the number of elements filled in "kc" is returned through
    // "count". 
    static w_rc_t parse_key_type(const char* s, w_base_t::uint4_t& count, key_type_s kc[]);
    static w_rc_t get_key_type(char* s, int buflen, w_base_t::uint4_t count, const key_type_s *kc);

};
#define null_lsn (lsn_t::null)
#define max_lsn  (lsn_t::max)

inline ostream& operator<<(ostream& o, const lsn_t& l)
{
    return o << l.file() << '.' << l.rba();
}

inline istream& operator>>(istream& i, lsn_t& l)
{
    sm_diskaddr_t d;
    char c;
    w_base_t::uint8_t f;
    i >> f >> c >> d;
    l = lsn_t(f, d);
    return i;
}

inline lpid_t::lpid_t() : page(0) {}

inline lpid_t::lpid_t(const stid_t& s, shpid_t p) : _stid(s), page(p)
{}

inline lpid_t::lpid_t(vid_t v, snum_t s, shpid_t p) :
        _stid(v, s), page(p)
{}

inline shrid_t::shrid_t() : page(0), store(0), slot(0)
{}
inline shrid_t::shrid_t(const rid_t& r) :
        page(r.pid.page), store(r.pid.store()), slot(r.slot)
{}

inline rid_t::rid_t() : slot(0)
{}

inline rid_t::rid_t(vid_t vid, const shrid_t& shrid) :
        pid(vid, shrid.store, shrid.page), slot(shrid.slot)
{}

inline stid_t rid_t::stid() const
{
    return pid.stid();
}

inline bool lpid_t::operator==(const lpid_t& p) const
{
    return (page == p.page) && (stid() == p.stid());
}

inline bool lpid_t::operator!=(const lpid_t& p) const
{
    return !(*this == p);
}

inline bool lpid_t::operator<(const lpid_t& p) const
{
    return _stid == p._stid && page < p.page;
}

inline bool lpid_t::operator<=(const lpid_t& p) const
{
    return _stid == p._stid && page <= p.page;
}

inline bool lpid_t::operator>=(const lpid_t& p) const
{
    return _stid == p._stid && page >= p.page;
}

inline w_base_t::uint4_t w_hash(const lpid_t& p)
{
    return p._stid.vol ^ (p.page + 113);
}

inline w_base_t::uint4_t w_hash(const vid_t v)
{
    return v;
}



inline bool rid_t::operator==(const rid_t& r) const
{
    return (pid == r.pid && slot == r.slot);
}

inline bool rid_t::operator!=(const rid_t& r) const
{
    return !(*this == r);
}

inline bool rid_t::operator<(const rid_t& r) const
{
    return pid < r.pid || (pid == r.pid && slot < r.slot);
}

/*<std-footer incl-file-exclusion='SM_S_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
