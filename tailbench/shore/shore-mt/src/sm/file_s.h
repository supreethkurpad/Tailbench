/*<std-header orig-src='shore' incl-file-exclusion='FILE_S_H'>

 $Id: file_s.h,v 1.40 2010/06/08 22:28:55 nhall Exp $

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

#ifndef FILE_S_H
#define FILE_S_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

typedef w_base_t::uint8_t clust_id_t; // not used at this time

class file_p;

enum pg_policy_t { 
    t_append        = 0x01, // retain sort order (cache 0 pages)
    t_cache        = 0x02, // look in n cached pgs 
    t_compact        = 0x04 // scan file for space in pages 
    /* These are masks - the following combinations are supported:
     * t_append    -- preserve sort order
     * t_cache  -- look only in cache - error if no luck (not really sensible)
     * t_compact  -- don't bother with cache (bad idea)
     * t_cache | t_append -- check the cache first, append if no luck
     * t_cache | t_compact -- check the cache first, if no luck,
     *                     search the file if histogram if apropos;
     *                     error if no luck
     * t_cache | t_compact | t_append  -- like above, but append to
     *                file as a last resort
     * t_compact | t_append  -- don't bother with cache (bad idea)
     *
     * Of course, not all combos are sensible.
     */
    
};

enum recflags_t { 
    t_badflag        = 0x00,
    t_forwardroot    = 0x01,     // not used yet
    t_forwarddata    = 0x02,     // not used yet
    t_small        = 0x04,    // simple record
    t_large_0         = 0x08,    // large with short list of chunks
    t_large_1         = 0x10,       // large with 1-level indirection
    t_large_2         = 0x20    // large with 2-level indirection
};
    
struct rectag_t {
    uint2_t    hdr_len;    // length of user header 
    uint2_t    flags;        // enum recflags_t
    smsize_t    body_len;    // true length of the record 
    /* 8 */
};

class record_t {
    friend class file_m;
    friend class file_p;
    friend class pin_i;
    friend class ss_m;
public:
    enum {max_len = smlevel_0::max_rec_len };

    rectag_t     tag;
    char    info[ALIGNON];  // minimal amount of hdr/data for record

    record_t()    {};
    bool is_large() const;
    bool is_small() const;
    int  large_impl() const;

    smsize_t hdr_size() const;
    smsize_t body_size() const;

    const char* hdr() const;
    const char* body() const;

    int body_offset() const { 
        return w_offsetof(record_t,info)+align(tag.hdr_len);
    }

    lpid_t pid_containing(smsize_t offset, smsize_t& start_byte, const file_p& page) const;
private:

    // only friends can use these
    smsize_t  page_count() const;
    lpid_t last_pid(const file_p& page) const;
};

/*
 *  This is the header specific to a file page.  It is stored in 
 *  the first slot on the page.
 */
struct file_p_hdr_t {
    clust_id_t    cluster;
	// See DUMMY_CLUSTER_ID in file.cpp, page.h 
	// It is the default value of the cluster id here.
};

inline const char* record_t::hdr() const
{
    return info;
}

inline bool record_t::is_large() const    
{ 
    return (tag.flags & (t_large_0 | t_large_1 | t_large_2)) != 0; 
}

inline int record_t::large_impl() const    
{ 
    switch ((int)(tag.flags & (t_large_0 | t_large_1 | t_large_2))) {
    case (int)t_large_0: return 0;
    case (int)t_large_1: return 1;
    case (int)t_large_2: return 2;
    default: 
    break;
    }
    return -1;
}

inline bool record_t::is_small() const 
{ 
    return (tag.flags & t_small) != 0; 
}

inline const char* record_t::body() const
{
    return info + align(tag.hdr_len);
}

inline smsize_t record_t::hdr_size() const
{
    return tag.hdr_len;
}

inline smsize_t record_t::body_size() const
{
    return tag.body_len;
}

/*<std-footer incl-file-exclusion='FILE_S_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
