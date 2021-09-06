/*<std-header orig-src='shore' incl-file-exclusion='SM_DU_STATS_H'>

 $Id: sm_du_stats.h,v 1.21 2010/06/18 21:22:54 nhall Exp $

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

#ifndef SM_DU_STATS_H
#define SM_DU_STATS_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/


/*
 *  Storage Manager disk utilization (du) statistics.
 */

#ifdef __GNUG__
#pragma interface
#endif


/*
 * Common abbreviations:
 *    pg = page
 *    lg = large
 *    cnt = count
 *    alloc = allocated
 *    hdr = header
 *    bs  = bytes
 *      rec = record
 */

typedef w_base_t::base_stat_t base_stat_t;

struct file_pg_stats_t {
    base_stat_t        hdr_bs;

    base_stat_t        slots_unused_bs;    /* invalid slots */
    base_stat_t        slots_used_bs;         /* "valid" slots */

    /* for those slots considered valid only: */
    base_stat_t        rec_tag_bs;            /* record "tag" (sm hdr) */
    base_stat_t        rec_hdr_bs;             /* user-defined hdr */
    base_stat_t        rec_hdr_align_bs;       /* alignment of header */

    /* For small records: */
    base_stat_t        small_rec_cnt;         /* # small-obj records */
    base_stat_t        rec_body_bs;         /* bytes in small-obj recs */
    base_stat_t        rec_body_align_bs;    /* wasted on alignment of 
                         * small records */
    /*
    // For large records;
    // More details of bytes consumed by large records are 
    // in lgdata_pg_stats_t structures.
    */
    base_stat_t        lg_rec_cnt;        /* # large-obj records */
    /* for implementation t_large_0: */
    base_stat_t        rec_lg_chunk_bs;    /* for representing chunks of pages */
    /* for implementation t_large_1 and t_large_2: */
    base_stat_t        rec_lg_indirect_bs;    /* pointers to root of
                         * large record tree */

    base_stat_t        free_bs;        /* unused bytes on page */
        /* question: include invalid slots? */
        /* need audit to add up what *should* add up to # bytes on a page */


    NORET              file_pg_stats_t() {clear();}
    void               add(const file_pg_stats_t& stats);
    void               clear();
    w_rc_t             audit() const; 
    base_stat_t        total_bytes() const;

    void               print(ostream&, const char *) const;/* pretty print */

    friend ostream&    operator<<(ostream&, const file_pg_stats_t& s);
};

struct lgdata_pg_stats_t {
    base_stat_t        hdr_bs;         /* hdr overhead on large data pages */
    base_stat_t        data_bs;         /* user data on large data pgs */
    base_stat_t        unused_bs;         /* leftover on large data pgs */

    NORET             lgdata_pg_stats_t() {clear();}
    void              add(const lgdata_pg_stats_t& stats);
    void              clear();
    w_rc_t            audit() const; 
    base_stat_t       total_bytes() const;
    void              print(ostream&, const char *) const;/* pretty print */
    friend ostream&   operator<<(ostream&, const lgdata_pg_stats_t& s);
};

/* interior pages of large record tree (all space is overhead) */
struct lgindex_pg_stats_t {
    base_stat_t        used_bs;
    base_stat_t        unused_bs;

    NORET              lgindex_pg_stats_t() {clear();}
    void               add(const lgindex_pg_stats_t& stats);
    void               clear();
    w_rc_t             audit() const; 
    base_stat_t        total_bytes() const;
    void               print(ostream&, const char *) const;/* pretty print */
    friend ostream&    operator<<(ostream&, const lgindex_pg_stats_t& s);
};

struct file_stats_t {
    // See file_m::get_du_statistics
    file_pg_stats_t        file_pg;     // byte counts for slots
    lgdata_pg_stats_t    lgdata_pg;   // byte counts for slots
    lgindex_pg_stats_t    lgindex_pg;  // byte counts for slots
    base_stat_t            file_pg_cnt; // allocated to the file
    base_stat_t            lgdata_pg_cnt; // large data pages needed to
                                     // hold the user data, not including
                                     // any metadata/index pages; 
                                     // for t_large_0,1,2
    base_stat_t            lgindex_pg_cnt; // large index pages references in
                                     // large record slots of t_large_1 and 2

    base_stat_t            unalloc_file_pg_cnt; // reserved, being in alloc extents
                        // in the case of tmp files, this is found by
                        // scanning all extents allocated to the store
                        // In the case of non-tmp (stores in the directory)
                        // this is found by calling first-page/next-page
                        // and using the "allocated" return value
    base_stat_t            unalloc_large_pg_cnt; 

    NORET           file_stats_t() {clear();}
    void            add(const file_stats_t& stats);
    void            clear();
    w_rc_t            audit() const; 
    base_stat_t        total_bytes() const;
    base_stat_t        alloc_pg_cnt() const;
    void             print(ostream&, const char *) const;/* pretty print */
    friend ostream&    operator<<(ostream&, const file_stats_t& s);
};

/*
// btree leaf pages
*/
struct btree_lf_stats_t {
    base_stat_t        hdr_bs;        /* page header (overhead) */
    base_stat_t        key_bs;        /* space used for keys      */
    base_stat_t        data_bs;    /* space for data associated to keys */
    base_stat_t        entry_overhead_bs;  /* slot + entry info + align */
    base_stat_t        unused_bs;
    base_stat_t        entry_cnt;
    base_stat_t        unique_cnt;    /* number of unique entries */

    NORET              btree_lf_stats_t() {clear();}
    void               add(const btree_lf_stats_t& stats);
    void               clear();
    w_rc_t             audit() const; 
    base_stat_t        total_bytes() const;
    void               print(ostream&, const char *) const;/* pretty print */
    friend ostream&    operator<<(ostream&, const btree_lf_stats_t& s);
};

/*
// btree interior pages
*/
struct btree_int_stats_t {
    base_stat_t        used_bs;
    base_stat_t        unused_bs;

    NORET              btree_int_stats_t() {clear();}
    void               add(const btree_int_stats_t& stats);
    void               clear();
    w_rc_t             audit() const; 
    base_stat_t        total_bytes() const;
    void               print(ostream&, const char *) const;/* pretty print */
    friend ostream&    operator<<(ostream&, const btree_int_stats_t& s);
};

// returns by bulk load and
// btree_m::get_du_statistics
struct btree_stats_t {
    btree_lf_stats_t    leaf_pg;  // byte counts for leaf pages
    btree_int_stats_t    int_pg;   // byte counts for interior pages

    base_stat_t     leaf_pg_cnt; // level-1 pages found by tree traversal
    base_stat_t     int_pg_cnt;  // level>1 pages found by tree traversal
    base_stat_t     unlink_pg_cnt; // pages allocated but not accounted-for
                                // in a tree traversal; allocated count is
                                // found by first_page/next_page traversal
                                // of the store's extents
                    // Given the way this is computed, it would be
                    // pretty hard to fail an audit.
                    // It would be nice if unlinked pages could be
                    // accounted-for in another way for a meaningful audit.
                     /* unlinked pages are empty and
                    // will be freed the next
                    // time they are encountered
                    // during a traversal
                    */
    base_stat_t     unalloc_pg_cnt; // pages not-allocated by extent-traversal
    base_stat_t     level_cnt;    /* number of levels in btree */

    NORET           btree_stats_t() {clear();}
    void            add(const btree_stats_t& stats);
    void            clear();
    w_rc_t          audit() const; 
    base_stat_t     total_bytes() const;
    base_stat_t     alloc_pg_cnt() const;
    void            print(ostream&, const char *) const;/* pretty print */
    friend ostream& operator<<(ostream&, const btree_stats_t& s);
};

struct rtree_stats_t {
    base_stat_t        entry_cnt;
    base_stat_t        unique_cnt;    /* number of unique entries */
    base_stat_t        leaf_pg_cnt;
    base_stat_t        int_pg_cnt;
    base_stat_t        unalloc_pg_cnt;
    base_stat_t        fill_percent;    /* leaf page fill factor */
    base_stat_t        level_cnt;     /* number of levels in rtree */

    NORET              rtree_stats_t() {clear();}
    void               add(const rtree_stats_t& stats);
    void               clear();
    w_rc_t             audit() const; 
    base_stat_t        total_bytes() const;
    void               print(ostream&, const char *) const;/* pretty print */
    friend ostream&    operator<<(ostream&, const rtree_stats_t& s);
};

struct volume_hdr_stats_t {
    base_stat_t        hdr_ext_cnt;        /* header & extent maps */
    base_stat_t        alloc_ext_cnt;        /* allocated extents 
                         * excludes hdr_ext_cnt */
    base_stat_t        unalloc_ext_cnt;    /* # of unallocated extents */
    base_stat_t        extent_size;         /* # of pages in an extent */

    NORET        volume_hdr_stats_t() {clear();}
    void        add(const volume_hdr_stats_t& stats);
    void        clear();
    w_rc_t        audit() const; 
    base_stat_t        total_bytes() const;
    void         print(ostream&, const char *) const;/* pretty print */
    friend ostream&    operator<<(ostream&, const volume_hdr_stats_t& s);
};

struct volume_map_stats_t {
    btree_stats_t  store_directory;     /* info about every store */
    btree_stats_t  root_index;        /* index mapping strings to IDs */

    NORET          volume_map_stats_t() {clear();}
    void           add(const volume_map_stats_t& stats);
    void           clear();
    w_rc_t         audit() const; 
    base_stat_t    total_bytes() const;
    base_stat_t    alloc_pg_cnt() const;
    base_stat_t    unalloc_pg_cnt() const;
    base_stat_t    unlink_pg_cnt() const;
    void           print(ostream&, const char *) const;/* pretty print */
    friend ostream&    operator<<(ostream&, const volume_map_stats_t& s);
};


struct sm_du_stats_t {
    file_stats_t     file;
    btree_stats_t     btree;
    rtree_stats_t     rtree;
    volume_hdr_stats_t     volume_hdr;    /* header extent info */
    volume_map_stats_t     volume_map;    /* special volume indexes */

    base_stat_t        file_cnt;
    base_stat_t        btree_cnt;
    base_stat_t        rtree_cnt;

    NORET             sm_du_stats_t() {clear();}
    void              add(const sm_du_stats_t& stats);
    void              clear();
    w_rc_t            audit() const; 
    base_stat_t       total_bytes() const;
    void              print(ostream&, const char *) const;/* pretty print */
    friend ostream&   operator<<(ostream&, const sm_du_stats_t& s);
};


class SmVolumeMetaStats
{
    public:
                SmVolumeMetaStats();
    void            Clear();
    void            IncrementPages(int numReserved, int numAlloc);
    SmVolumeMetaStats&    operator+=(const SmVolumeMetaStats& volumeStats);

    base_stat_t        numPages;        // total num pages on volume
    base_stat_t        numSystemPages;        // total num header pages on volume
    base_stat_t        numReservedPages;    // total num pages in allocated exts
    base_stat_t        numAllocPages;        // total num pages allocated to stores
    base_stat_t        numStores;        // total max num of stores in volume
    base_stat_t        numAllocStores;        // total num of stores allocated
};


// Stats for stores about which we don't have store-type info,
// because they aren't in the directory at the time we get du stats for
// them. All we can do with them is collect the number of reserved pages (
// pages in extents allocated to the store) and the number of allocated
// pages in those extents.
class SmStoreMetaStats
{
    public:
    NORET           SmStoreMetaStats();
    void            Clear();
    void            IncrementPages(int numReserved, int numAlloc);
    SmStoreMetaStats&    operator+=(const SmStoreMetaStats& storeStats);

    base_stat_t        numReservedPages; // extents * pages-per-extent
    base_stat_t        numAllocPages;    // pages with alloc bit set
};


class SmFileMetaStats
{
    public:
                SmFileMetaStats();
    void            Clear();
    SmFileMetaStats&    operator+=(const SmFileMetaStats& fileStats);

    snum_t            smallSnum;
    snum_t            largeSnum;
    SmStoreMetaStats    small;
    SmStoreMetaStats    large;
};

inline void SmVolumeMetaStats::Clear()
{
    numPages = 0;
    numSystemPages = 0;
    numReservedPages = 0;
    numAllocPages = 0;
    numStores = 0;
    numAllocStores = 0;
}   

inline SmVolumeMetaStats::SmVolumeMetaStats()
{
    Clear();
}

inline void SmVolumeMetaStats::IncrementPages(int numReserved, int numAlloc)
{
    numReservedPages += numReserved;
    numAllocPages += numAlloc;
}

inline SmVolumeMetaStats& SmVolumeMetaStats::operator+=(const SmVolumeMetaStats& volumeStats)
{
    numPages += volumeStats.numPages;
    numSystemPages += volumeStats.numSystemPages;
    numReservedPages += volumeStats.numReservedPages;
    numAllocPages += volumeStats.numAllocPages;
    numStores += volumeStats.numStores;
    numAllocStores += volumeStats.numAllocStores;

    return *this;
}

inline void SmStoreMetaStats::Clear()
{
    numReservedPages = 0;
    numAllocPages = 0;
}

inline void SmStoreMetaStats::IncrementPages(int numReserved, int numAlloc)
{
    numReservedPages += numReserved;
    numAllocPages += numAlloc;
}

inline SmStoreMetaStats::SmStoreMetaStats()
{
    Clear();
}

inline SmStoreMetaStats& SmStoreMetaStats::operator+=(const SmStoreMetaStats& storeStats)
{
    numReservedPages += storeStats.numReservedPages;
    numAllocPages += storeStats.numAllocPages;

    return *this;
}

inline SmFileMetaStats::SmFileMetaStats()
{
    smallSnum = 0;
    largeSnum = 0;
}

inline void SmFileMetaStats::Clear()
{
    small.Clear();
    large.Clear();
}

inline SmFileMetaStats& SmFileMetaStats::operator+=(const SmFileMetaStats& fileStats)
{
    small += fileStats.small;
    large += fileStats.large;

    return *this;
}



/*<std-footer incl-file-exclusion='SM_DU_STATS_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
