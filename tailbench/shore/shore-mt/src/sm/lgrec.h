/*<std-header orig-src='shore' incl-file-exclusion='LGREC_H'>

 $Id: lgrec.h,v 1.41 2010/05/26 01:20:39 nhall Exp $

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

#ifndef LGREC_H
#define LGREC_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

/*
 * The following *_s structures are stored persistently
 * on pages.
 */
struct lg_chunk_s {
    lg_chunk_s() : first_pid(0), npages(0) {}
    shpid_t     pid(shpid_t pid_num) const { return first_pid+pid_num; }
    shpid_t     last_pid()       const { return first_pid+npages-1; }

    shpid_t    first_pid;    // first page of contiguous chunk
    uint4_t    npages;        // number of pages in chunk
};

struct lg_tag_chunks_s {
    lg_tag_chunks_s(snum_t s) : store(s), chunk_cnt(0) {}

    enum     {max_chunks = 4};

    /*
     *  In the final implementation, large rec pages will be located
     *  in clusters within a file.  A 4byte cluster id will be
     *  sufficient to locate pages.  For now, we store the pages
     *  in a "store", so a stid_t is used.
     */
    //clust_id_t  cluster;         // cluster for pages
    snum_t      store;             // store for pages

    fill2    filler;            // because store is now 4 bytes
    uint2_t       chunk_cnt;          // # of chunks (max==4)
    lg_chunk_s  chunks[max_chunks]; // page-count pairs
};

struct lg_tag_indirect_s {
    lg_tag_indirect_s(snum_t s) : indirect_root(0), store(s) {}

    bool operator==(const lg_tag_indirect_s& l) const
        {return  indirect_root == l.indirect_root &&
         store == l.store; }
    bool operator!=(const lg_tag_indirect_s& l) const { return !(*this == l);}

    shpid_t       indirect_root;
    /*
     *  In the final implementation, large rec pages will be located
     *  in clusters within a file.  A 4byte cluster id will be
     *  sufficient to locate pages.  For now, we store the pages
     *  in a "store", so a stid_t is used.
     */
    snum_t      store;        // store for pages

#if 1
    /*
     * XXX This should go the way of the dodo.   As a side-effect of other
     * data structure sizes changing, the fill here (used to be a fill2)
     * was no longer necessary.   However, nobody noticed, it was left
     * in, and so the fill2 caused the need for a compiler generated 2
     * bytes for alignment.   The uninitialized data caused purify hits.
     * The real solution for this is to completely delete this the next
     * time real work is done on the log.  In the interim, I've switched
     * it to a fill4 to solve the uninitialized hits from purify.  This
     * solution does NOT invalidate any existing logs, since the size
     * still matches.
     */
    fill4    filler;        // for alignment
#endif
};

/*
 * The following *_h structures are handles on the above *_s structures.
 */

class lg_tag_chunks_h {
public:
//    lg_tag_chunks_h(lg_tag_chunks_s& chunks, vid_t vid) :
//        _ptr(chunks), _vid(vid)  {}
    lg_tag_chunks_h(const file_p& p, lg_tag_chunks_s& chunks) :
        _page(p), _cref(chunks) {}

    const lg_tag_chunks_s& chunk_ref() const {return _cref;}

    lpid_t     last_pid()      const
            {return ((_cref.chunk_cnt == 0) ? lpid_t::null : 
                lpid_t(stid(), _last_pid()));}
    lpid_t     pid(uint4_t pid_num) const
            {return lpid_t(stid(), _pid(pid_num));}

    enum     {max_chunks = 4};
    smsize_t    page_count() const 
            { uint4_t cnt = 0;
              for (smsize_t i = 0; i <_cref.chunk_cnt;
                   cnt += _cref.chunks[i].npages, i++) ;
              return cnt;}
    rc_t     append(uint4_t num_pages, const lpid_t new_pages[]);
    rc_t     truncate(uint4_t num_pages);
    rc_t     update(uint4_t start_byte, const vec_t& data) const ;

    stid_t    stid() const {return stid_t(_page.pid().vol(), _cref.store);}
    void     print(ostream &) const;
    friend     ostream& operator<<(ostream&, const lg_tag_chunks_h &);

private:
    shpid_t     _last_pid()      const  
            { w_assert3(_cref.chunk_cnt > 0);
              return _cref.chunks[_cref.chunk_cnt-1].last_pid(); }
    shpid_t     _pid(uint4_t pid_num) const;

    const file_p&    _page;        // page handle
    lg_tag_chunks_s&    _cref;        // chunk handle
};

class lg_tag_indirect_h : public w_base_t {
public:
    lg_tag_indirect_h(const file_p& p, lg_tag_indirect_s& i, uint4_t page_cnt) :
        _page(p), _iref(i), _page_cnt(page_cnt) {}

    const lg_tag_indirect_s& indirect_ref() const {return _iref;}

    lpid_t     last_pid()      const
            {return lpid_t(stid(), _last_pid());}
    lpid_t     pid(uint4_t pid_num ) const
            {return lpid_t(stid(),_pid(pid_num));}
    rc_t     convert(const lg_tag_chunks_h& old_lg_tag);
    rc_t     append(uint4_t num_pages, const lpid_t new_pages[]);
    rc_t     truncate(uint4_t num_pages);
    rc_t     update(uint4_t start_byte, const vec_t& data) const ;

    stid_t    stid() const {return stid_t(_page.pid().vol(), _iref.store);}

    static recflags_t    indirect_type(smsize_t page_count);

private:

    /* 
     * _last_indirect() and _add_new_indirect() both require that
     * the current number of pages in the record be passed in since
     * that info is only available based on the size of the record.
     */
    shpid_t     _last_indirect()  const;
    rc_t     _add_new_indirect(lpid_t& new_pid);

    shpid_t     _last_pid()  const;
    shpid_t     _pid(uint4_t pid_num) const;

    uint4_t    _pages_on_last_indirect() const;

    const file_p&    _page;    // page handle
    lg_tag_indirect_s&    _iref;        // indirect handle
    uint4_t        _page_cnt;    // current # of pages in rec
};

/*
 * These *_p structures are the large record page types. 
 */

class lgdata_p : public page_p {
public:

    enum { data_sz = page_p::data_sz};

    MAKEPAGE(lgdata_p, page_p, 1);
    
    rc_t append(const vec_t& data, uint4_t start, uint4_t amount);
    rc_t update(uint4_t offset /*from start of page*/, const vec_t& data,
        uint4_t start /*in vec*/, uint4_t amount);
    rc_t truncate(uint4_t amount);
    
private:

    /*
     *    Disable these since files do not have prev and next
     *    pages that can be determined from a page
     */
    shpid_t prev();
    shpid_t next();
    friend class page_link_log;   // just to keep g++ happy
};

class lgindex_p : public page_p {
public:

    enum { max_pids = page_p::data_sz / sizeof(shpid_t)};

    MAKEPAGE(lgindex_p, page_p, 1);

    rc_t     append(uint4_t num_pages, const shpid_t new_pids[]); 
    rc_t     truncate(uint4_t num_pages);
    shpid_t    last_pid() const
            { shpid_t* p = (shpid_t*)tuple_addr(0);
              return p[tuple_size(0)/sizeof(shpid_t)-1]; }
    shpid_t    pids(uint4_t pid_num) const 
            { shpid_t* p = (shpid_t*)tuple_addr(0);
              w_assert3(pid_num<tuple_size(0)/sizeof(shpid_t));
              return p[pid_num]; }
    uint4_t        pid_count() const { return tuple_size(0)/sizeof(shpid_t); }
private:

    /*
     *    Disable these since files do not have prev and next
     *    pages that can be determined from a page
     */
    shpid_t prev();
    shpid_t next();
    friend class page_link_log;   // just to keep g++ happy
};

// put inline code here
inline recflags_t lg_tag_indirect_h::indirect_type(smsize_t page_count)
{
    return ((page_count > lgindex_p::max_pids) ? t_large_2 : t_large_1);
}

inline uint4_t lg_tag_indirect_h::_pages_on_last_indirect() const
{
    return (_page_cnt == 0 ? 0 : ((_page_cnt % lgindex_p::max_pids) == 0 ?
                   (uint4_t)lgindex_p::max_pids :
                   (_page_cnt % lgindex_p::max_pids)));
}

/*<std-footer incl-file-exclusion='LGREC_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
