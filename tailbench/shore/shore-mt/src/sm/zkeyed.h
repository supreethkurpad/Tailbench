/*<std-header orig-src='shore' incl-file-exclusion='ZKEYED_H'>

 $Id: zkeyed.h,v 1.34 2010/06/08 22:29:01 nhall Exp $

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

#ifndef ZKEYED_H
#define ZKEYED_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

/*--------------------------------------------------------------*
 *  class zkeyed_p                            *
 *--------------------------------------------------------------*/
class zkeyed_p : public page_p {
public:
    
    rc_t             link_up(
                        shpid_t             new_prev,
                        shpid_t             new_next
                        );

    rc_t             format(
                        const lpid_t&       pid,
                        tag_t               tag,
                        w_base_t::uint4_t   flags,
                        store_flag_t        store_flags,
                        const cvec_t&       hdr
                        );    

    rc_t            insert(
                        const cvec_t&       key, 
                        const cvec_t&       aux, 
                        slotid_t            slot,
                        bool                do_it=true,
                        bool                compress=false
                        );

    rc_t            rewrite(slotid_t slot, int pxl);
    rc_t            remove(slotid_t slot, bool compress=false);
    rc_t            shift(slotid_t snum, zkeyed_p* rsib, bool cmprs=false);

    rc_t            shift(slotid_t snum, slotid_t snum_dest, zkeyed_p* rsib, bool cmprs=false);
    
    /* gnu has a bug -- can't make this protected
     * TODO: see if this is still the case
     */
    rc_t            rec(
                        slotid_t            idx, 
                        cvec_t&             key,
                        const char*&        aux,
                        int&                auxlen
                        ) const;
    rc_t            rec(
                        slotid_t            idx, 
                        int&                prefix_len,
                        int&                prefix_parts,
                        cvec_t&             key,
                        const char*&        aux,
                        int&                auxlen
                        ) const;
    
protected:
    
    MAKEPAGE(zkeyed_p, page_p, 1);

    int                 rec_size(slotid_t idx) const;
    int                 rec_expanded_size(slotid_t idx) const;
    int                 nrecs() const;
    rc_t                set_hdr(const cvec_t& data);
    const void*         get_hdr() const {
                            w_assert3(get_store_flags() != st_tmp);
                            return page_p::tuple_addr(0);
                        }
    
private:
    // disabled
    void*               tuple_addr(int);
    int                 tuple_size(int);
    friend class page_link_log;   // just to keep g++ happy

    smsize_t            min_entry_size() const;
    smsize_t            max_num_entries() const;

    void            dump(slotid_t idx, 
                        int line, 
                        const char *string) const;
};

/*--------------------------------------------------------------*
 *  zkeyed_p::min_entry_size()                    *
 *--------------------------------------------------------------*/
inline
smsize_t            
zkeyed_p::min_entry_size()const
{
    /* 
    // Record part is:
    // 2 for aux part
    // 1 for 1 byte of uncompressed kv
    // uint2_t - pfxlen 
    // uint2_t - keylen 
    //
    // Count slot table entry too.
    */
    return sizeof(slot_t) + align(2+ 1 + 2*sizeof(uint2_t));
}

/*--------------------------------------------------------------*
 *  zkeyed_p::max_num_entries()                    *
 *--------------------------------------------------------------*/
inline
smsize_t            
zkeyed_p::max_num_entries() const
{
    return  (smsize_t)(data_sz/min_entry_size());
}

/*--------------------------------------------------------------*
 *  zkeyed_p::rec_size()                    *
 *--------------------------------------------------------------*/
inline int zkeyed_p::rec_size(slotid_t idx) const
{
    return page_p::tuple_size(idx + 1);
}


/*--------------------------------------------------------------*
 *    zkeyed_p::nrecs()                        *
 *--------------------------------------------------------------*/
inline int zkeyed_p::nrecs() const
{
    return nslots() - 1;
}

/*--------------------------------------------------------------*
 *    zkeyed_p::link_up()                    *
 *--------------------------------------------------------------*/
inline rc_t
zkeyed_p::link_up(shpid_t new_prev, shpid_t new_next)
{
    return page_p::link_up(new_prev, new_next);
}

/*<std-footer incl-file-exclusion='ZKEYED_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
