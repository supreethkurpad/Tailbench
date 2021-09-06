/*<std-header orig-src='shore' incl-file-exclusion='KEYED_H'>

 $Id: keyed.h,v 1.30.2.4 2010/01/28 04:54:04 nhall Exp $

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

#ifndef KEYED_H
#define KEYED_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

class keyrec_t {
public:
    struct hdr_s {
    uint2_t    klen;
    uint2_t    elen;
    shpid_t    child;
    };
    
    const char* key() const;
    const char* elem() const;
    const char* sep() const;
    smsize_t klen() const     { return _hdr.klen; }
    smsize_t elen() const    { return _hdr.elen; }
    smsize_t slen() const    { return _hdr.klen + _hdr.elen; }
    smsize_t rlen() const    { return _body() + slen() - (char*) this; }
    shpid_t child() const    { return _hdr.child; }
    
private:
    hdr_s     _hdr;
    char*    _body()    const    { return ((char*) &_hdr) + sizeof(_hdr); }
};
    

/*--------------------------------------------------------------*
 *  class keyed_p                            *
 *--------------------------------------------------------------*/
class keyed_p : public page_p {
public:
    
    rc_t            link_up(shpid_t new_prev, shpid_t new_next);

    rc_t             format(
    const lpid_t&             pid,
    tag_t                 tag, 
    uint4_t                flags,
    store_flag_t            store_flags,
    const cvec_t&             hdr);

    rc_t            insert(
    const cvec_t&             key, 
    const cvec_t&             el, 
    int                 slot, 
    shpid_t             child = 0);
    rc_t            remove(int slot);
    rc_t            shift(int snum, keyed_p* rsib);

    
protected:
    
    MAKEPAGE(keyed_p, page_p, 1)
    
    const keyrec_t&         rec(slotid_t idx) const;

    int             rec_size(slotid_t idx) const;
    slotid_t             nrecs() const;
    rc_t            set_hdr(const cvec_t& data);
    const void*         get_hdr() const {
    return page_p::tuple_addr(0);
    }
    
private:
    // disabled
    void*             tuple_addr(int);
    int             tuple_size(int);
    friend class page_link_log;   // just to keep g++ happy
};

inline const char* keyrec_t::key() const    { return _body(); }
inline const char* keyrec_t::elem() const    { return _body() + _hdr.klen; }
inline const char* keyrec_t::sep() const    { return _body(); }

/*--------------------------------------------------------------*
 *  keyed_p::rec()                            *
 *--------------------------------------------------------------*/
inline const keyrec_t& 
keyed_p::rec(slotid_t idx) const
{
    return * (keyrec_t*) page_p::tuple_addr(idx + 1);
}

/*--------------------------------------------------------------*
 *  keyed_p::rec_size()                            *
 *--------------------------------------------------------------*/
inline int
keyed_p::rec_size(slotid_t idx) const
{
    return page_p::tuple_size(idx + 1);
}

/*--------------------------------------------------------------*
 *    keyed_p::nrecs()                        *
 *--------------------------------------------------------------*/
inline slotid_t 
keyed_p::nrecs() const
{
    return nslots() - 1;
}

/*--------------------------------------------------------------*
 *    keyed_p::link_up()                    *
 *--------------------------------------------------------------*/
inline rc_t
keyed_p::link_up(shpid_t new_prev, shpid_t new_next)
{
    return page_p::link_up(new_prev, new_next);
}

/*<std-footer incl-file-exclusion='KEYED_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
