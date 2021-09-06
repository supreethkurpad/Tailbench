/*<std-header orig-src='shore' incl-file-exclusion='PAGE_H_H'>

 $Id: page_h.h,v 1.6.2.4 2010/01/28 04:54:08 nhall Exp $

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

#ifndef PAGE_H_H
#define PAGE_H_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/



/* NB: if you change the size and range of the buckets, you must
 * make sure you adjust ALL THE NECESSARY constants!
 * We are set up to handle up to 16 buckets with 
 * space_bucket_size_in_bits = 4.  If you want to reduce the
 * number of buckets, you can do so as follows:
 * 1) change 
 *      space_bucket_size_in_bits, space_num_buckets
 * 2) change bucket_X_min_free and bucket_X_max_free 
 *    for the buckets you want.  Larger bucket numbers MUST
 *      contain more free bytes. Order is significant and is assumed 
 *     in vol.cpp and elsewhere.
 */
#define HBUCKETBITS 4
enum    { 
#if HBUCKETBITS==4
        space_num_buckets        = 16, // 2**HBUCKETBITS,
#elif HBUCKETBITS==3
        space_num_buckets        = 8, // 2**HBUCKETBITS,
#else
        space_num_buckets        = 4, // 2**HBUCKETBITS,
#endif
      space_bucket_size_in_bits     = HBUCKETBITS, 
      space_bucket_mask     =    (1<<space_bucket_size_in_bits) - 1
    };

/*
* Buckets are defined in terms of free space
* avaliable on a page.
* The smaller the bucket#, the less free space
* there is.
*/
enum {
    page_sz = smlevel_0::page_sz
};
enum {
    /* This should be enough percentages to be useful. */
    percent_07 = (page_sz >> 4),

    percent_12 = (page_sz >> 3),
    percent_19 = (percent_12 | percent_07),

    percent_25 = (page_sz >> 2),
    percent_32 = (percent_25 | percent_07),
    percent_37 = (percent_25 | percent_12),
    percent_44 = (percent_25 | percent_12 | percent_07),

    percent_50 = (page_sz >> 1),

    percent_57 = (percent_50 | percent_07),
    percent_62 = (percent_50 | percent_12),
    percent_69 = (percent_50 | percent_12 | percent_07),
    percent_75 = (percent_50 | percent_25),
    percent_82 = (percent_50 | percent_25 | percent_07),
    percent_87 = (percent_50 | percent_25 | percent_12),
    percent_94 = (percent_50 | percent_25 | percent_12 | percent_07),

    percent_100 = page_sz,

    page_overhead = (page_sz - page_s::data_sz) + 2*sizeof(page_s::slot_t),


#if HBUCKETBITS==4
    bucket_15_max_free = percent_100 - page_overhead,
    bucket_15_min_free = percent_94,

    bucket_14_max_free = bucket_15_min_free -1,
    bucket_14_min_free = percent_87,

    bucket_13_max_free = bucket_14_min_free -1,
    bucket_13_min_free = percent_82,

    bucket_12_max_free = bucket_13_min_free -1,
    bucket_12_min_free = percent_75,

    bucket_11_max_free = bucket_12_min_free -1,
    bucket_11_min_free = percent_69,

    bucket_10_max_free = bucket_11_min_free -1,
    bucket_10_min_free = percent_62,

    bucket_9_max_free = bucket_10_min_free -1,
    bucket_9_min_free = percent_57,

    bucket_8_max_free = bucket_9_min_free -1,
    bucket_8_min_free = percent_50,

    bucket_7_max_free = bucket_8_min_free -1,
    bucket_7_min_free = percent_44,

    bucket_6_max_free = bucket_7_min_free - 1,
    bucket_6_min_free = percent_37,

    bucket_5_max_free = bucket_6_min_free - 1,
    bucket_5_min_free = percent_32,

    bucket_4_max_free = bucket_5_min_free - 1,
    bucket_4_min_free = percent_25,

    bucket_3_max_free = bucket_4_min_free - 1,
    bucket_3_min_free = percent_19, 

    bucket_2_max_free = bucket_3_min_free - 1,
    bucket_2_min_free = percent_12,

    bucket_1_max_free = bucket_2_min_free - 1,
    bucket_1_min_free = percent_07,

#elif HBUCKETBITS==3
    bucket_7_max_free = percent_100 - page_overhead,
    bucket_7_min_free = percent_87,

    bucket_6_max_free = bucket_7_min_free - 1,
    bucket_6_min_free = percent_75,

    bucket_5_max_free = bucket_6_min_free - 1,
    bucket_5_min_free = percent_62,

    bucket_4_max_free = bucket_5_min_free - 1,
    bucket_4_min_free = percent_50,

    bucket_3_max_free = bucket_4_min_free - 1,
    bucket_3_min_free = percent_37, 

    bucket_2_max_free = bucket_3_min_free - 1,
    bucket_2_min_free = percent_25,

    bucket_1_max_free = bucket_2_min_free - 1,
    bucket_1_min_free = percent_12,

#elif HBUCKETBITS==2
    bucket_3_max_free = percent_100 - page_overhead,
    bucket_3_min_free = percent_75, 

    bucket_2_max_free = bucket_3_min_free - 1,
    bucket_2_min_free = percent_50,

    bucket_1_max_free = bucket_2_min_free - 1,
    bucket_1_min_free = percent_25,

#else

#error number of bits for histo buckets not defined properly

#endif /* HBUCKETBITS */

    bucket_0_max_free = bucket_1_min_free - 1,
    bucket_0_min_free = 0,

#if HBUCKETBITS == 4
    space_bucket_mask_high_bits =  
    (  percent_07 | percent_12 | percent_25 | percent_50 )
#elif HBUCKETBITS == 3
    space_bucket_mask_high_bits =  
    ( percent_12 | percent_25 | percent_50 )
#else 
    space_bucket_mask_high_bits =  
    ( percent_25 | percent_50 )
#endif /* HBUCKETBITS */

};    

class pginfo_t {
private:
    smsize_t    _space_left;
    shpid_t        _pgid;
public:
    NORET       pginfo_t():  _space_left(0), _pgid(0) {}
    NORET     pginfo_t(const shpid_t& pg, smsize_t sl): 
            _space_left(sl), _pgid(pg) { }
    NORET       pginfo_t(const pginfo_t& other):  
            _space_left(other._space_left), 
            _pgid(other._pgid) { }

    NORET     ~pginfo_t() { }

    smsize_t  space() const { return _space_left; }
    shpid_t   page() const { return _pgid; }
    void       update_space(smsize_t v) {
            _space_left = v; 
        }
    void set_bucket(const shpid_t& pg, space_bucket_t b);
    void set(const shpid_t& pg, smsize_t v) {
            _space_left = v; 
            _pgid = pg;
        }
    friend ostream &operator<<(ostream&, const pginfo_t&p);
};

inline void 
pginfo_t::set_bucket(const shpid_t& pg, space_bucket_t b) 
{
    // max free space that could be on the page
    _space_left = page_p::bucket2free_space(b); 
    _pgid = pg;
}

inline 
ostream &operator<<(ostream&o, const pginfo_t&p) {
    o << p._pgid << ":" << p._space_left << ends;
    return o;
}

class store_histo_t {
public:
    NORET store_histo_t();
    void decr(space_bucket_t b);
    void incr(space_bucket_t b) {
        bucket[b]++;
    }
    bool exists(space_bucket_t b) const {
        return bucket[b] > 0;
    }
    friend ostream &operator<<(ostream&, const store_histo_t&);
private:
    shpid_t bucket[space_num_buckets];
};
inline NORET 
store_histo_t::store_histo_t() 
{
    for (shpid_t p=0; p < space_num_buckets; p++) {
    // Initialize to 0.
    // This is the structure stored in the transient
    // histoid_t and updated by reading the extent links.
    bucket[p] =  0;
    }
}

inline void 
store_histo_t::decr(space_bucket_t b) 
{
    bucket[b]--;
    if(int(bucket[b])<0) bucket[b] = 0;
}


/*<std-footer incl-file-exclusion='PAGE_H_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
