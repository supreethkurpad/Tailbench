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

/*<std-header orig-src='shore'>

 $Id: btree_bl.cpp,v 1.29 2010/06/08 22:28:55 nhall Exp $

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

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define BTREE_C

#include "sm_int_4.h"
// need stored streams
#include "sm.h"

#include "btree_p.h"
#include "btree_impl.h"
#include "sort.h"
#include "sm_du_stats.h"
#include <crash.h>
#include "umemcmp.h"

#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_array_t<record_t *>; 
#endif

const smlevel_0::store_flag_t btree_m::bulk_loaded_store_type 
        = smlevel_0::st_insert_file;

/* 
 * Comparison function for qsort 
 */
static int 
elm_cmp(const void *_r1, const void *_r2)
{
  const record_t **r1 = (const record_t **)_r1;
  const record_t **r2 = (const record_t **)_r2;
  unsigned size1 = (*r1)->body_size();
  unsigned size2 = (*r2)->body_size();

  int d = umemcmp((*r1)->body(), (*r2)->body(), size1 < size2 ? size1 : size2);

  if (d)
    return d;

  return size1 - size2;
}

/*********************************************************************
 *
 *  class btsink_t
 *
 *  Manages bulk load of a btree. 
 *
 *  User calls put() to add <key,el> into the sink, and then
 *  call map_to_root() to map the current root to the original 
 *  root page of the btree, and finalize the bulkload.
 *
 *  CC Note: 
 *        Btree must be locked EX.
 *  Recovery Note: 
 *        Logging is turned off during insertion into the btree.
 *        However, as soon as a page fills up, a physical 
 *        page image log is generated for the page.
 *
 *********************************************************************/
class btsink_t : private btree_m {
public:

    NORET        btsink_t(const lpid_t& root, rc_t& rc);
    NORET        ~btsink_t() {};

    // TODO: pin: while you implement bulk loading also adjust these
    btsink_t() {_bIgnoreLatches = false; }
    rc_t        set(const lpid_t& root, const bool bIgnoreLatches = false);
    rc_t        put_mr_l(const cvec_t& key, const cvec_t& el, bool& new_leaf);

    rc_t        put(const cvec_t& key, const cvec_t& el);
    rc_t        map_to_root();

    uint2_t        height()        { return _height; }
    uint4_t        num_pages()        { return _num_pages; }
    uint4_t        leaf_pages()        { return _leaf_pages; }
private:
    bool        _is_compressed; // prefix compression turned on?
    uint2_t        _height;        // height of the tree
    uint4_t        _num_pages;        // total # of pages
    uint4_t        _leaf_pages;        // total # of leaf pages

    lpid_t        _root;                // root of the btree
    btree_p        _page[20];        // a stack of pages (path) from root
                                // to leaf
    slotid_t        _slot[20];        // current slot in each page of the path
    shpid_t        _left_most[20];        // id of left most page in each level
    int         _top;                // top of the stack

    bool _bIgnoreLatches;

    rc_t        _add_page(int i, shpid_t pid0);
};

/*********************************************************************
 *
 *  btree_impl::_handle_dup_keys(sink, slot, prevp, curp, count, r, pid)
 *
 *  context: called during bulk load
 *
 *  assumptions: entire btre is locked EX, so there's no concurrency
 *
 *
 *********************************************************************/
rc_t
btree_impl::_handle_dup_keys(
    btsink_t*           sink,           // IO- load stack
    slotid_t&           slot,           // IO- current slot
    file_p*             prevp,          // I-  previous page
    file_p*             curp,           // I-  current page
    int&                count,          // O-  number of duplicated keys
    record_t*&          r,              // O-  last record
    lpid_t&             pid,            // IO- last pid
    int                 nkc,
    const key_type_s*   kc,
    bool                lexify_keys,
    const bool          bIgnoreLatches
    )
{
    count = 0;
    int max_rec_cnt = 500;

    record_t** recs = new record_t* [max_rec_cnt];
    if (!recs)  { return RC(eOUTOFMEMORY); }
    w_auto_delete_array_t<record_t*> auto_del_recs(recs);

    bool eod = false, eof = false;
    int i;

    if (slot==1) {
        // previous rec is on the previous page
        W_COERCE( prevp->get_rec(prevp->num_slots()-1, r) );
    } else {
        W_COERCE( curp->get_rec(slot-1, r) );
    }
    recs[count++] = r;

    W_COERCE( curp->get_rec(slot, r) );
    recs[count++] = r;

    slotid_t s = slot;
    while ((s = curp->next_slot(s)))  {
        W_COERCE( curp->get_rec(s, r) );
        if (r->hdr_size() == recs[0]->hdr_size() &&
            !memcmp(r->hdr(), recs[0]->hdr(), r->hdr_size())) {
            if (r->body_size() == recs[0]->body_size() &&
                !memcmp(r->body(), recs[0]->body(), (int)r->body_size())) {
                return RC(eDUPLICATE);
            }
            if (count == max_rec_cnt) {
                max_rec_cnt *= 2;
                record_t** tmp = new record_t* [max_rec_cnt];
                if (!tmp)  { return RC(eOUTOFMEMORY); }
                memcpy(tmp, recs, count*sizeof(recs));
                delete [] recs;
                recs = tmp;
                auto_del_recs.set(recs);
            }
            recs[count++] = r;
        } else {
            eod = true;
            break;
        }
    }

    int page_cnt = 0, max_page_cnt = 200;
    file_p* pages = new file_p[max_page_cnt];
    if (!pages)  { return RC(eOUTOFMEMORY); }
    w_auto_delete_array_t<file_p> auto_del_pages(pages);

    if (!eod)  {
        W_DO( fi->next_page(pid, eof, NULL /* allocated only*/) );
    }

    while (!eof && !eod) {
	latch_mode_t latch = LATCH_SH;
	if(bIgnoreLatches) {
	    latch = LATCH_NL;
	}
        W_DO( pages[page_cnt].fix(pid, latch) );
        s = 0;
        while ((s = pages[page_cnt].next_slot(s)))  {
            W_COERCE( pages[page_cnt].get_rec(s, r) );

            if (r->hdr_size() == recs[0]->hdr_size() &&
               !memcmp(r->hdr(), recs[0]->hdr(), r->hdr_size())) {
                if (r->body_size() == recs[0]->body_size() &&
                    !memcmp(r->body(), recs[0]->body(), (int)r->body_size())) {
                    return RC(eDUPLICATE);
                }
                if (count==max_rec_cnt) {
                    max_rec_cnt *= 2;
                    record_t** tmp = new record_t* [max_rec_cnt];
                    if (!tmp)  { return RC(eOUTOFMEMORY); }
                    memcpy(tmp, recs, count*sizeof(recs));
                    delete [] recs;
                    recs = tmp;
                    auto_del_recs.set(recs);
                }
                recs[count++] = r;
            } else {
                eod = true;
                break;
            }
        }
        page_cnt++;
        if (!eod) {
            W_DO( fi->next_page(pid, eof, NULL /* allocated only*/) );
            if (page_cnt >= max_page_cnt) {
                cerr 
                << "btree_impl::_handle_dup_keys: "
                << " too many duplicate key entries" << endl;
                cerr << "      returning OUT OF SPACE error" << endl;
                return RC(eOUTOFSPACE);

            }
        }
    }

/* Replaced by qsort, below
    // sort the recs : use selection sort
    for (i = 0; i < count - 1; i++) {
        for (int j = i + 1; j < count; j++) {
            vec_t el(recs[i]->body(), (int)recs[i]->body_size());
            if (el.cmp(recs[j]->body(), (int)recs[j]->body_size()) > 0) {
                record_t* tmp = recs[i];
                recs[i] = recs[j];
                recs[j] = tmp;
            }
        }
    }
*/

    qsort(recs, count, sizeof(void*), elm_cmp);

    vec_t key(recs[0]->hdr(), recs[0]->hdr_size());
    for (i = 0; i < count; i++) {
        cvec_t el(recs[i]->body(), (int)recs[i]->body_size());
        if(lexify_keys) {
            cvec_t* real_key=0;
            DBG(<<"");
            W_DO(btree_m::_scramble_key(real_key, key, nkc, kc));
            W_DO( sink->put(*real_key, el) );
        } else {
            DBG(<<"");
            W_DO( sink->put(key, el) );
        }
    }

    if (page_cnt>0) {
        *curp = pages[page_cnt-1];
    }
    slot = s;

    if (eof) pid = lpid_t::null;

    return RCOK;
}

/*********************************************************************
 *
 *  btree_m::purge(root, check_empty, forward_processing)
 *
 *  Remove all pages of a btree except the root. "Check_empty" 
 *  indicates whether to check if the tree is empty before purging.
 #  NOT USED 
 *
 *********************************************************************/
rc_t
btree_m::purge(
    const lpid_t&         root,                // I-  root of btree
    bool                check_empty,
    bool                forward_processing,
    const bool          bIgnoreLatches
    )
{
    if (check_empty)  {
        /* For forward processing, we just make
         * sure that bulk-load is done only on
         * empty b-trees. (For recovery, we blow
         * away the contents of the btree.)
         */
        bool flag;
        W_DO( is_empty(root, flag, bIgnoreLatches) );
        if (! flag)  {
            DBG(<<"eNDXNOTEMPTY");
            return RC(eNDXNOTEMPTY);
        }
    }

    lsn_t anchor;
    xct_t* xd = xct();
    w_assert3(xd);
    check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
    anchor = xd->anchor();

    lpid_t pid;
    X_DO( io->first_page(root.stid(), pid, NULL /* allocated only*/), anchor );
    while (pid.page)  {
        /*
         *  save current pid, get next pid, free current pid.
         */
        lpid_t cur = pid;
        rc_t rc = io->next_page(pid);
        if (cur.page != root.page)  {
            X_DO( io->free_page(cur, false/*check_store_membership*/), anchor );
        }
        if (rc.is_error())  {
            if (rc.err_num() != eEOF)  {
                xd->release_anchor(true LOG_COMMENT_USE("btbl1"));
                return RC_AUGMENT(rc);
            }
            break;
        }
    }

    btree_p page;
    if(bIgnoreLatches) {
	X_DO( page.fix(root, LATCH_NL), anchor );
    } else { 
	X_DO( page.fix(root, LATCH_EX), anchor );
    }
    X_DO( page.set_hdr(root.page, 1, 0, 
        (page.is_compressed()?btree_p::t_compressed:btree_p::t_none)
        ), anchor );

    SSMTEST("btree.bulk.3");
    xd->compensate(anchor,false/*not undoable*/ LOG_COMMENT_USE("btree.bl.3"));
    
    // W_COERCE( log_btree_purge(page) );
    // GNATS 100: you cannot use multiple threads to bulk-load
    // the same btree, but you can use multiple thread-ed xcts to
    // bulk-load different btrees, so it's possible that the
    // log won't get the xct's log mutex right away.
    rc_t rc = log_btree_purge(page);
    while(rc.is_error() && (rc.err_num() == eRETRY)) {
        rc = log_btree_purge(page);
    }
    W_COERCE(rc);

    if(forward_processing) {
        W_DO( io->set_store_flags(root.stid(), bulk_loaded_store_type) );
    }
    return RCOK;
}


/*********************************************************************
 *
 *  btree_m::bulk_load(root, ...)
 *
 *  Bulk load a btree at root using records from store src.
 *
 *  The key and element of each entry is stored in the header and
 *  body part, respectively, of records from src store. 
 *
 *  NOTE: src records must be sorted in ascending key order.
 *  and keys must already have been converted to lexicographic
 *  order (internal format).
 *
 *  Statistics regarding the bulkload is returned in stats.
 *
 *********************************************************************/
rc_t
btree_m::bulk_load(
    const lpid_t&        root,                // I-  root of btree
    int                  nsrcs,                // I- # stores in array above
    const stid_t*        src,                // I-  stores containing new records
    int                  nkc,
    const key_type_s*    kc,
    bool                 unique,                // I-  true if btree is unique
    concurrency_t        cc_unused,        // I-  concurrency control mechanism
    btree_stats_t&       _stats,                 // O-  index stats
    bool                 sort_duplicates, // I - default is true
    bool                 lexify_keys   // I - default is true
    )                
{

    // keep compiler quiet about unused parameters
    if (cc_unused) {}

    w_assert1(kc && nkc > 0);
    DBG(<<"bulk_load "
        << nsrcs << " sources, first=" << src[0] << " index=" << root
        << " sort_dups=" << sort_duplicates
        << " lexify_keys=" << lexify_keys
        );

    /*
     *  Set up statistics gathering
     */
    _stats.clear();
    base_stat_t uni_cnt = 0;
    base_stat_t cnt = 0;

    /*
     *  Btree must be empty for bulkload.
     */
    W_DO( purge(root, true, true) );
        
    /*
     *  Create a sink for bulkload
     */
    rc_t rc;
    btsink_t sink(root, rc);
    if (rc.is_error()) return RC_AUGMENT(rc);

    /*
     *  Go thru the src file page by page
     */
    int i = 0;                // toggle
    file_p page[2];           // page[i] is current page

    const record_t*         pr = 0;        // previous record
    lpid_t pr_pid; // previous pid
    slotid_t pr_s = 0; // previous slot
    int                     src_index = 0;
    bool                    skip_last = false;

    for(src_index = 0; src_index < nsrcs; src_index++) {
        lpid_t               pid;
        bool                 eof = false;
        skip_last = false;

        for (rc = fi->first_page(src[src_index], pid, NULL /* allocated only*/);
             !rc.is_error() && !eof;
              rc = fi->next_page(pid, eof, NULL /* allocated only*/))     {
            /*
             *  for each page ...
             */
            W_DO( page[i].fix(pid, LATCH_SH) );
            w_assert3(page[i].pid() == pid);

            slotid_t s = page[i].next_slot(0);
            if (! s)  {
                /*
                 *  do nothing. skip over empty page, so do not toggle
                 */
                continue;
            } 
            for ( ; s; s = page[i].next_slot(s))  {
                /*
                 *  for each slot in page ...
                 */
                record_t* r;
                W_COERCE( page[i].get_rec(s, r) );

                if(!sort_duplicates) {
                    // free up page asap
                    if (page[1-i].is_fixed())  page[1-i].unfix();
                }

                if (pr) {
                    bool insert_one = false;
                    cvec_t key(pr->hdr(), pr->hdr_size());
		    rid_t rid(pr_pid, pr_s);
                    cvec_t el((char*)(&rid), sizeof(rid_t));
                    DBG(<<"pr->key_size " << pr->hdr_size());
                    DBG(<<"pr->el_size " << el.size());

                    /*
                     *  check uniqueness and sort order
                     *  key is prev, r is curr
                     *  key.cmp(r) tests key cmp r, so 
                     *  <0 means key < r, >0 means key > r
                     */
                    int res = key.cmp(r->hdr(), r->hdr_size());
                    if (res==0) {
                        /*
                         *  key of r is equal to the previous key
                         */
                        if (unique) {
                            return RC(eDUPLICATE);
                        }
                        if(sort_duplicates) {
                            /*
                             * temporary hack for duplicated keys:
                             *  sort the elem in order before loading
                             */
                            int dup_cnt;
                            W_DO( btree_impl::_handle_dup_keys(&sink, s, &page[1-i],
                                                   &page[i], dup_cnt, r, pid,
                                                   nkc, kc,
                                                   lexify_keys) );
                            cnt += dup_cnt;
                            eof = (pid==lpid_t::null);
                            skip_last = eof;
                        } else {
                            /*
                             * The input file was sorted on key/elem
                             * pairs, so go ahead and insert it
                             */
                            insert_one = true;
                        }
                    } else {
                        /*
                         *  key of r is < or > the previous key
                         *  but not a dup.  NB: we can't distinguish
                         *  < or > because we haven't scrambled the 
                         *  keys yet.  We just have to take it that
                         *  the caller indeed sorted the file.
                         */
                        insert_one = true;
                    }
                    if(insert_one) {
                        ++cnt;
                        if(lexify_keys) {
                            DBG(<<"lexify, before sink.put(key, el) key = " 
                                    << key);
                            cvec_t* real_key = 0;
                            W_DO(_scramble_key(real_key, key, nkc, kc));
                            W_DO( sink.put(*real_key, el) );
                        } else {
                            DBG(<<"no lexify, sink.put(key, el) key = " << key);
                            W_DO( sink.put(key, el) );
                        }
                        skip_last = false;
                    } 

                    ++uni_cnt;
                }

                if (page[1-i].is_fixed())  page[1-i].unfix();
                pr = r;
		pr_s = s;
		pr_pid = pid;

                if (!s) break;
            }
            i = 1 - i;        // toggle i
            if (eof) break;
        }
        if (rc.is_error())  {
            return rc.reset();
        }
    }


    if (!skip_last && pr) {
	cvec_t key(pr->hdr(), pr->hdr_size());
	rid_t rid(pr_pid, pr_s);
	cvec_t el((char*)(&rid), sizeof(rid_t));
	if(lexify_keys) {
            cvec_t* real_key;
            W_DO(_scramble_key(real_key, key, nkc, kc));
            DBG(<<"");
            W_DO( sink.put(*real_key, el) );
        } else {
            DBG(<<"");
            W_DO( sink.put(key, el) );
        }
        ++uni_cnt;
        ++cnt;
    }

    if (pr) {
        W_DO( sink.map_to_root() );
    }

    _stats.level_cnt = sink.height();
    _stats.leaf_pg_cnt = sink.leaf_pages();
    _stats.int_pg_cnt = sink.num_pages() - _stats.leaf_pg_cnt;

    _stats.leaf_pg.unique_cnt = uni_cnt;
    _stats.leaf_pg.entry_cnt = cnt;

    DBG(<<"end bulk load OK");
    return RCOK;
}


/*********************************************************************
 *
 *  btree_m::bulk_load(root, sorted_stream, unique, cc, stats)
 *
 *  Bulk load a btree at root using records from sorted_stream.
 *  Statistics regarding the bulkload is returned in stats.
 *
 *********************************************************************/
rc_t
btree_m::bulk_load(
    const lpid_t&        root,                // I-  root of btree
    sort_stream_i&       sorted_stream,        // IO - sorted stream        
    int                  nkc,
    const key_type_s*    kc,
    bool                 unique,                // I-  true if btree is unique
    concurrency_t        cc_unused,        // I-  concurrency control
    btree_stats_t&       _stats)                // O-  index stats
{
    w_assert1(kc && nkc > 0);
    DBG(<<"bulk_load from sorted stream, index=" << root);

    // keep compiler quiet about unused parameters
    if (cc_unused) {}

    /*
     *  Set up statistics gathering
     */
    _stats.clear();
    base_stat_t uni_cnt = 0;
    base_stat_t cnt = 0;

    /*
     *  Btree must be empty for bulkload
     */
    W_DO( purge(root, true, true) );

    /*
     *  Create a sink for bulkload
     */
    rc_t rc;
    btsink_t sink(root, rc);
    if (rc.is_error()) return RC_AUGMENT(rc);

    /*
     *  Allocate space for storing prev keys
     */
    char* tmp = new char[page_s::data_sz];
    if (! tmp)  {
        return RC(eOUTOFMEMORY);
    }
    w_auto_delete_array_t<char> auto_del_tmp(tmp);
    vec_t prev_key(tmp, page_s::data_sz);

    /*
     *  Go thru the sorted stream
     */
    bool pr = false;        // flag for prev key
    bool eof = false;

    vec_t key, el;
    W_DO ( sorted_stream.get_next(key, el, eof) );

    while (!eof) {
        ++cnt;

        if (! pr) {
            ++uni_cnt;
            pr = true;
            prev_key.copy_from(key);
            prev_key.reset().put(tmp, key.size());
        } else {
            // check unique
            if (key.cmp(prev_key))  {
                ++uni_cnt;
                prev_key.reset().put(tmp, page_s::data_sz);
                prev_key.copy_from(key);
                prev_key.reset().put(tmp, key.size());
            } else {
                if (unique) {
                    DBG(<<"");
                    return RC(eDUPLICATE);
                }
                // GNATS 116 BUGBUG: need to sort elems for duplicate keys
                DBG(<<"not unique uni_cnt " << uni_cnt
                        << " pr=" << pr 
                        <<  " matches prev key "

                        );
                fprintf(stderr, 
                "bulk-loading duplicate key into non-unique btree %s\n",
                "requires sorting elements: not implemented");
                return RC(eNOTIMPLEMENTED);
            }
        }

        cvec_t* real_key;
        DBG(<<"");
        W_DO(_scramble_key(real_key, key, nkc, kc));
        W_DO( sink.put(*real_key, el) ); 
        key.reset();
        el.reset();
        W_DO ( sorted_stream.get_next(key, el, eof) );
    }

    DBG(<<"");
    W_DO( sink.map_to_root() );
        
    _stats.level_cnt = sink.height();
    _stats.leaf_pg_cnt = sink.leaf_pages();
    _stats.int_pg_cnt = sink.num_pages() - _stats.leaf_pg_cnt;

    sorted_stream.finish();

    _stats.leaf_pg.unique_cnt = uni_cnt;
    _stats.leaf_pg.entry_cnt = cnt;

    DBG(<<" return OK from bulk load");
    return RCOK;
}


/*********************************************************************
 *
 *  btree_m::mr_bulk_load(root, ...)
 *
 *  Bulk load a mrbtree at root using records from store src.
 *
 *  The key and element of each entry is stored in the header and
 *  body part, respectively, of records from src store. 
 *
 *  NOTE: src records must be sorted in ascending key order.
 *  and keys must already have been converted to lexicographic
 *  order (internal format).
 *
 *  Statistics regarding the bulkload is returned in stats.
 *
 *********************************************************************/
rc_t
btree_m::mr_bulk_load(
    key_ranges_map&      partitions,          // I-  mrbtree partitions
    int                  nsrcs,                // I- # stores in array above
    const stid_t*        src,                // I-  stores containing new records
    int                  nkc,
    const key_type_s*    kc,
    bool                 unique,                // I-  true if btree is unique
    concurrency_t        cc_unused,        // I-  concurrency control mechanism
    btree_stats_t&       _stats,                 // O-  index stats
    bool                 sort_duplicates, // I - default is true
    bool                 lexify_keys,   // I - default is true
    const bool           bIgnoreLatches // I - default is false
    )                
{

    // keep compiler quiet about unused parameters
    if (cc_unused) {}

    w_assert1(kc && nkc > 0);
    DBG(<<"mr_bulk_load "
        << nsrcs << " sources, first=" << src[0]
        << " sort_dups=" << sort_duplicates
        << " lexify_keys=" << lexify_keys
        );

    // pin: to debug
    cout << "bulk loading" << endl;
    
    // set up statistics gathering
    _stats.clear();
    base_stat_t uni_cnt = 0;
    base_stat_t cnt = 0;


    rc_t rc;
    btsink_t sink;
    //int partition = 0;

    // go thru the src file page by page
    int i = 0;                // toggle
    file_p page[2];           // page[i] is current page

    const record_t*         pr = 0;        // previous record
    lpid_t pr_pid; // previous pid
    slotid_t pr_s = 0; // previous slot
    int                     src_index = 0;
    bool                    skip_last = false;
    
    // current subroot to insert
    lpid_t current_root;
    // current start&end key
    cvec_t startKey;
    cvec_t endKey;
    // to mark the root change
    bool root_change = false;
    bool first_root = true;
    bool last_root = false;
    // latch mode
    latch_mode_t latch = LATCH_SH;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }
    
    for(src_index = 0; src_index < nsrcs; src_index++) {
	// pin: to debug
	cout << "src_index: " << src_index << endl;

        lpid_t               pid;
        bool                 eof = false;
        skip_last = false;

        for (rc = fi->first_page(src[src_index], pid, NULL /* allocated only*/);
             !rc.is_error() && !eof;
              rc = fi->next_page(pid, eof, NULL /* allocated only*/))     {

	    // pin: to debug
	    cout << "\tpid: " << pid << endl;
            
	    // for each page ...
            W_DO( page[i].fix(pid, latch) );
            w_assert3(page[i].pid() == pid);

            slotid_t s = page[i].next_slot(0);
            if (! s)  {
		//  do nothing. skip over empty page, so do not toggle
                continue;
            } 

            for ( ; s; s = page[i].next_slot(s))  {

		// pin: to debug
		cout << "\t\tslot: " << s << endl;
	
                // for each slot in page ...
                record_t* r;
                W_COERCE( page[i].get_rec(s, r) );

                if(!sort_duplicates) {
                    // free up page asap
                    if (page[1-i].is_fixed())  page[1-i].unfix();
                }

                if (pr) {
                    bool insert_one = false;
                    cvec_t key(pr->hdr(), pr->hdr_size());
		    rid_t rid(pid, s);
                    cvec_t el((char*)(&rid), sizeof(rid_t));

		    // check for change of the root
		    cvec_t* real_key = 0;
		    if(lexify_keys) {
			DBG(<<"lexify, before getting the right root with the key = " << key);
			// pin: to debug
			cout << "lexify, before getting the right root with the key = " << key << endl;
			W_DO(_scramble_key(real_key, key, nkc, kc));
			cout << "scrambled key " << *real_key << endl; 
			if(!last_root &&
			   !(startKey <= (*real_key) && (*real_key) < endKey)) {
			    cout << "changing root" << endl;
			    partitions.getPartitionByKey(*real_key, current_root);
			    cout << "new root" << endl;
			    root_change = true;
			}
			cout << "root is " << current_root << endl;
		    }
		    else {
			DBG(<<"no lexify before getting the root, key = " << key);
			// pin: to debug
			cout << "no lexify before getting the root, key = " << key << endl;
			if(!last_root &&
			   !(startKey <= key && key < endKey)) {  
			    partitions.getPartitionByKey(key, current_root);
			    root_change = true;
			}
		    }
		    if(root_change) {
			cout << "sink.map_to_root()" << endl;
			if(!first_root) {
			    W_DO( sink.map_to_root() );
			} else {
			    first_root = false;
			}
			cout << "sink map to root finished" << endl;
			// TODO: stats of the old root

			// change to new root
			//partitions.getBoundaries(current_root, startKey, endKey, last_root);
			DBG(<< "index->sub_root =" << current_root 
			    << "startKey =" << startKey
			    << "endKey =" << endKey);

			// pin: to debug
			cout << "index->sub_root =" << current_root 
			     << "startKey =" << startKey
			     << "endKey =" << endKey << endl;
			
			// Btree must be empty for bulkload. // pin: no it doesn't check this now
			//W_DO( purge(current_root, false, true) );

			W_DO(sink.set(current_root, bIgnoreLatches));
			root_change = false;
		    }
		    
                    DBG(<<"pr->hdr_size " << pr->hdr_size());
                    DBG(<<"pr->body_size " << pr->body_size());
		    		    
                    /*
                     *  check uniqueness and sort order
                     *  key is prev, r is curr
                     *  key.cmp(r) tests key cmp r, so 
                     *  <0 means key < r, >0 means key > r
                     */
                    int res = key.cmp(r->hdr(), r->hdr_size());
                    if (res==0) {
                        /*
                         *  key of r is equal to the previous key
                         */
                        if (unique) {
                            return RC(eDUPLICATE);
                        }
                        if(sort_duplicates) {
                            /*
                             * temporary hack for duplicated keys:
                             *  sort the elem in order before loading
                             */
                            int dup_cnt;
                            W_DO( btree_impl::_handle_dup_keys(&sink, s, &page[1-i],
							       &page[i], dup_cnt, r, pid,
							       nkc, kc,
							       lexify_keys, bIgnoreLatches) );
                            cnt += dup_cnt;
                            eof = (pid==lpid_t::null);
                            skip_last = eof;
                        } else {
                            /*
                             * The input file was sorted on key/elem
                             * pairs, so go ahead and insert it
                             */
                            insert_one = true;
                        }
                    } else {
                        /*
                         *  key of r is < or > the previous key
                         *  but not a dup.  NB: we can't distinguish
                         *  < or > because we haven't scrambled the 
                         *  keys yet.  We just have to take it that
                         *  the caller indeed sorted the file.
                         */
                        insert_one = true;
                    }
                    if(insert_one) {
                        ++cnt;
                        if(lexify_keys) {
			    W_DO( sink.put(*real_key, el) );
                        } else {
			    W_DO( sink.put(key, el) );
                        }
			// pin: to debug
			int value;
			key.copy_to(&value, sizeof(value));
			cout << "key: " << value << " el: " << rid << " added" << endl;
			cout << endl;
			skip_last = false;
                    } 

                    ++uni_cnt;
                }

                if (page[1-i].is_fixed())  page[1-i].unfix();
                pr = r;
		pr_s = s;
		pr_pid = pid;
		
                if (!s) break;
            }
            i = 1 - i;        // toggle i
            if (eof) break;
        }
        if (rc.is_error())  {
            return rc.reset();
        }
    }


    if (!skip_last && pr) {
	cvec_t key(pr->hdr(), pr->hdr_size());
	rid_t rid(pr_pid, pr_s);
	cvec_t el((char*)(&rid), sizeof(rid_t));
	cvec_t* real_key = 0;

	if(lexify_keys) {    
            W_DO(_scramble_key(real_key, key, nkc, kc));
            DBG(<<"");
	    if(!last_root &&
	       !(startKey <= (*real_key) && (*real_key) < endKey)) {  
		partitions.getPartitionByKey(*real_key, current_root);
		root_change = true;
	    }
        } else {
            DBG(<<"");
	     if(!last_root &&
		!(startKey <= key && key < endKey)) {  
		partitions.getPartitionByKey(key, current_root);
		root_change = true;
	    }
        }

	if(root_change) {
	    if(!first_root)
		W_DO( sink.map_to_root() );
	    
	    // TODO: stats of the old root
	    
	    // Btree must be empty for bulkload.
	    //W_DO( purge(current_root, false, true) );

	    // change to new root
	    W_DO(sink.set(current_root, bIgnoreLatches));
	    root_change = false;
	}

	if(lexify_keys) {
	    W_DO( sink.put(*real_key, el) );
	} else {
	    W_DO( sink.put(key, el) );
	}

        ++uni_cnt;
        ++cnt;
    }

    if (pr) {
        W_DO( sink.map_to_root() );
    }

    // TODO: handle stats as indicated above
    _stats.level_cnt = sink.height();
    _stats.leaf_pg_cnt = sink.leaf_pages();
    _stats.int_pg_cnt = sink.num_pages() - _stats.leaf_pg_cnt;
    
    _stats.leaf_pg.unique_cnt = uni_cnt;
    _stats.leaf_pg.entry_cnt = cnt;

    DBG(<<"end bulk load OK");
    return RCOK;
}


/*********************************************************************
 *
 *  btree_m::mr_bulk_load(root, sorted_stream, unique, cc, stats)
 *
 *  Bulk load a btree at root using records from sorted_stream.
 *  Statistics regarding the bulkload is returned in stats.
 *
 *********************************************************************/
rc_t
btree_m::mr_bulk_load(
    key_ranges_map&      partitions,          // I-  mrbtree partitions
    sort_stream_i&       sorted_stream,        // IO - sorted stream        
    int                  nkc,
    const key_type_s*    kc,
    bool                 unique,                // I-  true if btree is unique
    concurrency_t        cc_unused,        // I-  concurrency control
    btree_stats_t&       _stats)                // O-  index stats
{
    w_assert1(kc && nkc > 0);
    DBG(<<"bulk_load from sorted stream");

    // keep compiler quiet about unused parameters
    if (cc_unused) {}

    // Set up statistics gathering
    _stats.clear();
    base_stat_t uni_cnt = 0;
    base_stat_t cnt = 0;

    // current subroot to insert
    lpid_t current_root;
    // current start&end key
    cvec_t startKey;
    cvec_t endKey;
    bool last_root = false;
    bool first_root = true;
    
    rc_t rc;
    btsink_t sink;

    // Allocate space for storing prev keys
    char* tmp = new char[page_s::data_sz];
    if (! tmp)  {
        return RC(eOUTOFMEMORY);
    }
    w_auto_delete_array_t<char> auto_del_tmp(tmp);
    vec_t prev_key(tmp, page_s::data_sz);

    // Go thru the sorted stream
    bool pr = false;        // flag for prev key
    bool eof = false;

    vec_t key, el;
    W_DO ( sorted_stream.get_next(key, el, eof) );

    while (!eof) {
        ++cnt;

        if (! pr) {
            ++uni_cnt;
            pr = true;
            prev_key.copy_from(key);
            prev_key.reset().put(tmp, key.size());
        } else {
            // check unique
            if (key.cmp(prev_key))  {
                ++uni_cnt;
                prev_key.reset().put(tmp, page_s::data_sz);
                prev_key.copy_from(key);
                prev_key.reset().put(tmp, key.size());
            } else {
                if (unique) {
                    DBG(<<"");
                    return RC(eDUPLICATE);
                }
                // GNATS 116 BUGBUG: need to sort elems for duplicate keys
                DBG(<<"not unique uni_cnt " << uni_cnt
                        << " pr=" << pr 
                        <<  " matches prev key "

                        );
                fprintf(stderr, 
                "bulk-loading duplicate key into non-unique btree %s\n",
                "requires sorting elements: not implemented");
                return RC(eNOTIMPLEMENTED);
            }
        }

        cvec_t* real_key;
        DBG(<<"");
        W_DO(_scramble_key(real_key, key, nkc, kc));
	
	// check for root change
	if(!last_root && !(startKey <= (*real_key) && (*real_key) < endKey)) {  
	    partitions.getPartitionByKey(*real_key, current_root);
	    if(!first_root) {
		// finish with old root
		W_DO( sink.map_to_root() );
	    } else {
		first_root = false;
	    }
	    // TODO: stats of the old root

	    // change to new root
	    //partitions.getBoundaries(current_root, startKey, endKey, last_root);
	    DBG(<< "index->sub_root =" << current_root 
		<< "startKey =" << startKey
		<< "endKey =" << endKey);
	    // Btree must be empty for bulkload.
	    //W_DO( purge(current_root, false, true) );
	    // update sink
	    W_DO(sink.set(current_root));
	}

        W_DO( sink.put(*real_key, el) ); 
        key.reset();
        el.reset();
        W_DO ( sorted_stream.get_next(key, el, eof) );
    }

    DBG(<<"");
    W_DO( sink.map_to_root() );
        
    _stats.level_cnt = sink.height();
    _stats.leaf_pg_cnt = sink.leaf_pages();
    _stats.int_pg_cnt = sink.num_pages() - _stats.leaf_pg_cnt;

    sorted_stream.finish();

    _stats.leaf_pg.unique_cnt = uni_cnt;
    _stats.leaf_pg.entry_cnt = cnt;

    DBG(<<" return OK from bulk load");
    return RCOK;
}

/*********************************************************************
 *
 *  btree_m::mr_bulk_load_l(root, ...)
 *
 *  Bulk load a mrbtree at root using records from store src.
 *
 *  The key and element of each entry is stored in the header and
 *  body part, respectively, of records from src store. 
 *
 *  NOTE: src records must be sorted in ascending key order.
 *  and keys must already have been converted to lexicographic
 *  order (internal format).
 *
 *  Statistics regarding the bulkload is returned in stats.
 *
 *********************************************************************/

rc_t
btree_m::mr_bulk_load_l(
    key_ranges_map&      partitions,          // I-  mrbtree partitions
    int                  nsrcs,                // I- # stores in array above
    const stid_t*        src,                // I-  stores containing new records
    int                  nkc,
    const key_type_s*    kc,
    bool                 unique,                // I-  true if btree is unique
    concurrency_t        cc_unused,        // I-  concurrency control mechanism
    btree_stats_t&       _stats,                 // O-  index stats
    bool                 sort_duplicates, // I - default is true
    bool                 lexify_keys,   // I - default is true
    const bool           bIgnoreLatches) // I - default is false                
{

    // keep compiler quiet about unused parameters
    if (cc_unused) {}

    w_assert1(kc && nkc > 0);
    DBG(<<"mr_bulk_load "
        << nsrcs << " sources, first=" << src[0]
        << " sort_dups=" << sort_duplicates
        << " lexify_keys=" << lexify_keys
        );

    // set up statistics gathering
    _stats.clear();
    base_stat_t uni_cnt = 0;
    base_stat_t cnt = 0;


    rc_t rc;
    btsink_t sink;

    // go thru the src file page by page
    int i = 0;                // toggle
    file_p page[2];           // page[i] is current page

    const record_t*         pr = 0;        // previous record
    lpid_t pr_pid; // previous pid
    slotid_t pr_s = 0; // previous slot
    int                     src_index = 0;
    bool                    skip_last = false;
    
    // current subroot to insert
    lpid_t current_root;
    // current start&end key
    cvec_t startKey;
    cvec_t endKey;
    // to mark the root change
    bool root_change = false;
    bool first_root = true;
    bool last_root = false;
    // latch mode
    latch_mode_t latch = LATCH_SH;
    if(bIgnoreLatches) {
	latch = LATCH_NL;
    }
    
    lpid_t               pid;
    slotid_t s;
    
    for(src_index = 0; src_index < nsrcs; src_index++) {
        bool                 eof = false;
        skip_last = false;

        for (rc = fi->first_page(src[src_index], pid, NULL /* allocated only*/);
             !rc.is_error() && !eof;
              rc = fi->next_page(pid, eof, NULL /* allocated only*/))     {
            
	    // for each page ...
            W_DO( page[i].fix(pid, latch) );
            w_assert3(page[i].pid() == pid);

            s = page[i].next_slot(0);
            if (! s)  {
		//  do nothing. skip over empty page, so do not toggle
                continue;
            } 

            for ( ; s; s = page[i].next_slot(s))  {
                // for each slot in page ...
                record_t* r;
                W_COERCE( page[i].get_rec(s, r) );

                if(!sort_duplicates) {
                    // free up page asap
                    if (page[1-i].is_fixed())  page[1-i].unfix();
                }

                if (pr) {
                    bool insert_one = false;
                    cvec_t key(pr->hdr(), pr->hdr_size());
		    rid_t rid(pr_pid, pr_s);
                    cvec_t el((char*)(&rid), sizeof(rid_t));

		    // check for change of the root
		    cvec_t* real_key = 0;
		    if(lexify_keys) {
			DBG(<<"lexify, before getting the right root with the key = " << key);
			W_DO(_scramble_key(real_key, key, nkc, kc));
			if(!last_root && !(startKey <= (*real_key) && (*real_key) < endKey)) {  
			    partitions.getPartitionByKey(*real_key, current_root);
			    root_change = true;
			}
		    }
		    else {
			DBG(<<"no lexify before getting the root, key = " << key);
			if(!last_root && !(startKey <= key && key < endKey)) {  
			    partitions.getPartitionByKey(key, current_root);
			    root_change = true;
			}
		    }
		    if(root_change) {
			if(!first_root) {
			    W_DO( sink.map_to_root() );
			} else {
			    first_root = false;
			}
			// TODO: stats of the old root

			// change to new root
			//partitions.getBoundaries(current_root, startKey, endKey, last_root);
			DBG(<< "index->sub_root =" << current_root 
			    << "startKey =" << startKey
			    << "endKey =" << endKey);
		     
			// Btree must be empty for bulkload.
			//W_DO( purge(current_root, false, true) );

			W_DO(sink.set(current_root, bIgnoreLatches));
			root_change = false;
		    }
		    
                    DBG(<<"pr->hdr_size " << pr->hdr_size());
                    DBG(<<"pr->body_size " << pr->body_size());

                    /*
                     *  check uniqueness and sort order
                     *  key is prev, r is curr
                     *  key.cmp(r) tests key cmp r, so 
                     *  <0 means key < r, >0 means key > r
                     */
                    int res = key.cmp(r->hdr(), r->hdr_size());
                    if (res==0) {
                        /*
                         *  key of r is equal to the previous key
                         */
                        if (unique) {
                            return RC(eDUPLICATE);
                        }
                        if(sort_duplicates) {
                            /*
                             * temporary hack for duplicated keys:
                             *  sort the elem in order before loading
                             */
                            int dup_cnt;
                            W_DO( btree_impl::_handle_dup_keys(&sink, s, &page[1-i],
							       &page[i], dup_cnt, r, pid,
							       nkc, kc,
							       lexify_keys, bIgnoreLatches) );
                            cnt += dup_cnt;
                            eof = (pid==lpid_t::null);
                            skip_last = eof;
                        } else {
                            /*
                             * The input file was sorted on key/elem
                             * pairs, so go ahead and insert it
                             */
                            insert_one = true;
                        }
                    } else {
                        /*
                         *  key of r is < or > the previous key
                         *  but not a dup.  NB: we can't distinguish
                         *  < or > because we haven't scrambled the 
                         *  keys yet.  We just have to take it that
                         *  the caller indeed sorted the file.
                         */
                        insert_one = true;
                    }
                    if(insert_one) {
                        ++cnt;
			bool new_leaf = false;
                        if(lexify_keys) {
			    W_DO( sink.put_mr_l(*real_key, el, new_leaf) );
                        } else {
			    W_DO( sink.put_mr_l(key, el, new_leaf) );
                        }
			if(new_leaf && s != 1) {
			    file_p new_page;
			    lpid_t new_page_id;
			    W_DO( file_m::_alloc_page(pid._stid,
						      lpid_t::eof,
						      new_page_id,
						      new_page,
						      true) );
			    // TODO: W_DO( page[i].shift( s, &new_page) );
			    page[i].unfix();
			    new_page.unfix();
			    W_DO( page[i].fix(pid, latch) );
			    if(lexify_keys) {
				W_DO( sink.put_mr_l(*real_key, el, new_leaf) );
			    } else {
				W_DO( sink.put_mr_l(key, el, new_leaf) );
			    }
			}
                        skip_last = false;
                    } 

                    ++uni_cnt;
                }

                if (page[1-i].is_fixed())  page[1-i].unfix();
                pr = r;
		pr_s = s;
		pr_pid = pid;
		
                if (!s) break;
            }
            i = 1 - i;        // toggle i
            if (eof) break;
        }
        if (rc.is_error())  {
            return rc.reset();
        }
    }

    s--;
    
    if (!skip_last && pr) {
	cvec_t key(pr->hdr(), pr->hdr_size());
	rid_t rid(pr_pid, pr_s);
	cvec_t el((char*)(&rid), sizeof(rid_t));
        cvec_t* real_key = 0;

	if(lexify_keys) {    
            W_DO(_scramble_key(real_key, key, nkc, kc));
            DBG(<<"");
	    if(!last_root && !(startKey <= (*real_key) && (*real_key) < endKey)) {  
		partitions.getPartitionByKey(*real_key, current_root);
		root_change = true;
	    }
        } else {
            DBG(<<"");
	     if(!last_root && !(startKey <= key && key < endKey)) {  
		partitions.getPartitionByKey(key, current_root);
		root_change = true;
	    }
        }

	if(root_change) {
	    if(!first_root)
		W_DO( sink.map_to_root() );
	    
	    // TODO: stats of the old root
	    
	    // Btree must be empty for bulkload.
	    //W_DO( purge(current_root, false, true) );

	    // change to new root
	    W_DO(sink.set(current_root, bIgnoreLatches));
	    root_change = false;
	}

	bool new_leaf = false;
	if(lexify_keys) {
	    W_DO( sink.put_mr_l(*real_key, el, new_leaf) );
	} else {
	    W_DO( sink.put_mr_l(key, el, new_leaf) );
	}
	if(new_leaf && s != 1) {
	    file_p new_page;
	    lpid_t new_page_id;
	    W_DO( file_m::_alloc_page(pid._stid,
				      lpid_t::eof,
				      new_page_id,
				      new_page,
				      true) );
	    // TODO: W_DO( page[i].shift(s, &new_page) );
	    page[i].unfix();
	    new_page.unfix();
	    W_DO( page[i].fix(pid, latch) );
	    if(lexify_keys) {
		W_DO( sink.put_mr_l(*real_key, el, new_leaf) );
	    } else {
		W_DO( sink.put_mr_l(key, el, new_leaf) );
	    }
	}

        ++uni_cnt;
        ++cnt;
    }

    if (pr) {
        W_DO( sink.map_to_root() );
    }

    // TODO: handle stats as indicated above
    _stats.level_cnt = sink.height();
    _stats.leaf_pg_cnt = sink.leaf_pages();
    _stats.int_pg_cnt = sink.num_pages() - _stats.leaf_pg_cnt;
    
    _stats.leaf_pg.unique_cnt = uni_cnt;
    _stats.leaf_pg.entry_cnt = cnt;

    DBG(<<"end bulk load OK");
    return RCOK;
}


/*********************************************************************
 *
 *  btree_m::mr_bulk_load_l(root, sorted_stream, unique, cc, stats)
 *
 *  Bulk load a btree at root using records from sorted_stream.
 *  Statistics regarding the bulkload is returned in stats.
 *
 *********************************************************************/
rc_t
btree_m::mr_bulk_load_l(
    key_ranges_map&      /*partitions*/,          // I-  mrbtree partitions
    sort_stream_i&       /*sorted_stream*/,        // IO - sorted stream        
    int                  /*nkc*/,
    const key_type_s*    /*kc*/,
    bool                 /*unique*/,                // I-  true if btree is unique
    concurrency_t        /*cc_unused*/,        // I-  concurrency control
    btree_stats_t&       /*_stats*/)                // O-  index stats
{
    assert (0);      // TODO: how to implement ??
    return RCOK;
}


/*********************************************************************
 *
 *  btsink_t::btsink_t(root_pid, rc)
 *
 *  Construct a btree sink for bulkload of btree rooted at root_pid. 
 *  Any errors during construction in returned in rc.
 *
 *********************************************************************/
btsink_t::btsink_t(const lpid_t& root, rc_t& rc)
    : _is_compressed(false), _height(0), _num_pages(0), _leaf_pages(0),
      _root(root), _top(0)
{
    btree_p rp;
    rc = rp.fix(root, LATCH_SH);
    if (rc.is_error())  return;
    _bIgnoreLatches = false;
    _is_compressed = rp.is_compressed();
    rc = _add_page(0, 0);
    _left_most[0] = _page[0].pid().page;
}


/*********************************************************************
 *
 *  btsink_t::set(root_pid, bIgnoreLatches)
 *
 *  To update the sink for bulk loading as roots change in mrbtrees.
 *  Resets the sink.
 *
 *********************************************************************/
rc_t btsink_t::set(const lpid_t& root, const bool bIgnoreLatches)
{
    _bIgnoreLatches = bIgnoreLatches;
    _is_compressed = false;
    _height = 0;
    _num_pages = 0;
    _leaf_pages = 0;
    _root = root;
    _top = 0;

    rc_t rc;
    btree_p rp;
    if(_bIgnoreLatches)
	    rc = rp.fix(root, LATCH_NL);
    else rc = rp.fix(root, LATCH_SH);
    if (!rc.is_error()) {
	_is_compressed = rp.is_compressed();
	rc = _add_page(0, 0);
	_left_most[0] = _page[0].pid().page;
    }
    return rc;
}


/*********************************************************************
 *
 *  btsink_t::map_to_root()
 *
 *  Map current running root page to the real root page. Deallocate
 *  original running root page after the mapping.
 *
 *********************************************************************/

rc_t
btsink_t::map_to_root()
{
    lsn_t anchor;
    xct_t* xd = xct();
    w_assert1(xd);
    check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
    if (xd)  anchor = xd->anchor();
    
    for (int i = 0; i <= _top; i++)  {
        X_DO( log_page_image(_page[i]), anchor );
    }

    /*
     *  Fix root page
     */
    btree_p rp;
    if(_bIgnoreLatches) {
	    X_DO( rp.fix(_root, LATCH_NL), anchor );
    } else {
	X_DO( rp.fix(_root, LATCH_EX), anchor );
    }
    lpid_t child_pid;
    {
        btree_p cp = _page[_top];
        child_pid = cp.pid();

        _height = cp.level();

        if (child_pid == _root)  {
            /*
             *  No need to remap.
             */
            xd->release_anchor(true LOG_COMMENT_USE("btbl1"));
            return RCOK;
        }

        /*
         *  Shift everything from child page to root page
         */
        X_DO( rp.set_hdr(_root.page, cp.level(), cp.pid0(), 
            (_is_compressed?btree_p::t_compressed:btree_p::t_none)
            ), anchor );
        w_assert1( !cp.next() && ! cp.prev());
        w_assert1( rp.nrecs() == 0);
        X_DO( cp.shift(0, rp), anchor);
    }
    _page[_top] = rp;


    if (xd)  {
        SSMTEST("btree.bulk.2");
        xd->compensate(anchor,false/*not undoable*/ LOG_COMMENT_USE("btree.bl.2"));
    }


    /*
     *  Free the child page. It has been copied to the root.
     */
    W_DO( io->free_page(child_pid, false/*check_store_membership*/) );

    return RCOK;
}



/*********************************************************************
 *
 *  btsink_t::_add_page(i, pid0)
 *
 *  Add a new page at level i. -- actually, it makes the page level
 *   i+1, since the btree ends up with leaves at level 1.
 *  Used only for bulk load, so it turns logging on & off.
 *
 *********************************************************************/
rc_t
btsink_t::_add_page(const int i, shpid_t pid0)
{
    lsn_t anchor;
    xct_t* xd = xct();
    check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
    if (xd) anchor = xd->anchor();

    {

        btree_p lsib = _page[i];
        /*
         *  Allocate a new page.  I/O layer turns logging on when
         *  necessary
         */
        X_DO( btree_impl::_alloc_page(_root, 
                                      i+1, (_page[i].is_fixed() ? _page[i].pid() : _root), 
                                      _page[i], pid0,
                                      false, _is_compressed,
                                      bulk_loaded_store_type, 
                                      _bIgnoreLatches), anchor);

    
        /*
         *  Update stats
         */
        _num_pages++;
        if (i == 0) _leaf_pages++;

        {
            xct_log_switch_t toggle(OFF);
            /*
             *  Link up
             */
            shpid_t left = 0;
            if (lsib.is_fixed() != 0) {
                left = lsib.pid().page;
            }
            DBG(<<"Adding page " << _page[i].pid()
                << " at level " << _page[i].level()
                << " prev= " << left
                << " next=0 " 
                );
            X_DO( _page[i].link_up(left, 0), anchor );

            if (lsib.is_fixed() != 0) {
                // already did:
                // left = lsib.pid().page;

                DBG(<<"Linking page " << lsib.pid()
                    << " at level " << lsib.level()
                    << " prev= " << lsib.prev()
                    << " next= "  << _page[i].pid().page
                    );
                X_DO( lsib.link_up(lsib.prev(), _page[i].pid().page), anchor );

                xct_log_switch_t toggle(ON);
                X_DO( log_page_image(lsib), anchor );
            }
        }
    }

    if (xd)  {
        SSMTEST("btree.bulk.1");
        xd->compensate(anchor,false/*not undoable*/ LOG_COMMENT_USE("btree.bl.1"));
    }

    /*
     *  Current slot for new page is 0
     */
    _slot[i] = 0;
    
    return RCOK;
}




/*********************************************************************
 *
 *  btsink_t::put(key, el)
 *
 *  Insert a <key el> pair into the page[0] (leaf) of the stack
 *
 *********************************************************************/
rc_t
btsink_t::put(const cvec_t& key, const cvec_t& el)
{
    /*
     *  Turn OFF logging. Insertions into btree pages are not logged
     *  during bulkload. Instead, a page image log is generated
     *  when the page is filled (in _add_page()).
     *
     *  NB: we'll turn it off and on several times because while
     *  it's off, no other threads can run in this tx. 
     */
    /* 
     * logging is explicitly turned off -- temporary (st_tmp)
     * btrees aren't possible. 
     * NB: store_flags CAN have the st_tmp bit turned on -- it
     * can be st_insert_file or st_load_file, but cannot be
     * st_tmp only because it cannot be recovered: whole file
     * has to be destroyed by this transaction.
     */

    w_assert3(_page[0].get_store_flags() != st_tmp);

    rc_t rc;
    {
        xct_log_switch_t toggle(OFF);

        /*
         *  Try inserting into the page[0] (leaf)
         */

        rc = _page[0].insert(key, el, _slot[0]++);
    }
    if (rc.is_error()) {
    
        if (rc.err_num() != eRECWONTFIT)  {
            return RC_AUGMENT(rc);
        }
        
        /*
         *  page[0] is full --- add a new page and and re-insert
         *  NB: _add_page turns logging on when it needs to
         */
        W_DO( _add_page(0, 0) );

        {
            xct_log_switch_t toggle(OFF);
            W_COERCE( _page[0].insert(key, el, _slot[0]++) );
        }

        /*
         *  Propagate up the tree
         */
        int i;
        for (i = 1; i <= _top; i++)  {
            {
                xct_log_switch_t toggle(OFF);
                rc = _page[i].insert(key, el, _slot[i]++,
                                     _page[i-1].pid().page);
            }
            if (rc.is_error())  {
                if (rc.err_num() != eRECWONTFIT)  {
                    return RC_AUGMENT(rc);
                }

                /*
                 *  Parent is full
                 *      --- add a new page for the parent and
                 *        --- continue propagation to grand-parent
                 */
                W_DO(_add_page(i, _page[i-1].pid().page));
                
            } else {
                
                /* parent not full --- done */
                break;
            }
        }

        /*
         *  Check if we need to grow the tree
         */
        if (i > _top)  {
            ++_top;
            W_DO( _add_page(_top, _left_most[_top-1]) );
            _left_most[_top] = _page[_top].pid().page;
            {
                xct_log_switch_t toggle(OFF);
                W_COERCE( _page[_top].insert(key, el, _slot[_top]++,
                                         _page[_top-1].pid().page) );
            }
        }
    }

    return RCOK;
}


/*********************************************************************
 *
 *  btsink_t::put_mr_l(key, el)
 *
 *  Insert a <key el> pair into the page[0] (leaf) of the stack
 *  If no space left on leaf, before adding rec to new leaf, remove recs
 *  to another file (return back to the bulk_load function for this.
 *  The indicator is the new_leaf.
 *
 *********************************************************************/
rc_t
btsink_t::put_mr_l(const cvec_t& key, const cvec_t& el, bool& new_leaf)
{
    /*
     *  Turn OFF logging. Insertions into btree pages are not logged
     *  during bulkload. Instead, a page image log is generated
     *  when the page is filled (in _add_page()).
     *
     *  NB: we'll turn it off and on several times because while
     *  it's off, no other threads can run in this tx. 
     */
    /* 
     * logging is explicitly turned off -- temporary (st_tmp)
     * btrees aren't possible. 
     * NB: store_flags CAN have the st_tmp bit turned on -- it
     * can be st_insert_file or st_load_file, but cannot be
     * st_tmp only because it cannot be recovered: whole file
     * has to be destroyed by this transaction.
     */

    w_assert3(_page[0].get_store_flags() != st_tmp);

    rc_t rc;
    if(!new_leaf) {
	    xct_log_switch_t toggle(OFF);
	    
	    /*
	     *  Try inserting into the page[0] (leaf)
	     */
	    
	    rc = _page[0].insert(key, el, _slot[0]++);
	}
    if (new_leaf || rc.is_error()) {
    
        if (rc.err_num() != eRECWONTFIT)  {
            return RC_AUGMENT(rc);
        }

	if(!new_leaf) {
	    new_leaf = true;
	    return RCOK;
	}

	/*
         *  page[0] is full --- add a new page and and re-insert
         *  NB: _add_page turns logging on when it needs to
         */
	new_leaf = false;
        W_DO( _add_page(0, 0) );

        {
            xct_log_switch_t toggle(OFF);
            W_COERCE( _page[0].insert(key, el, _slot[0]++) );
        }

        /*
         *  Propagate up the tree
         */
        int i;
        for (i = 1; i <= _top; i++)  {
            {
                xct_log_switch_t toggle(OFF);
                rc = _page[i].insert(key, el, _slot[i]++,
                                     _page[i-1].pid().page);
            }
            if (rc.is_error())  {
                if (rc.err_num() != eRECWONTFIT)  {
                    return RC_AUGMENT(rc);
                }

                /*
                 *  Parent is full
                 *      --- add a new page for the parent and
                 *        --- continue propagation to grand-parent
                 */
                W_DO(_add_page(i, _page[i-1].pid().page));
                
            } else {
                
                /* parent not full --- done */
                break;
            }
        }

        /*
         *  Check if we need to grow the tree
         */
        if (i > _top)  {
            ++_top;
            W_DO( _add_page(_top, _left_most[_top-1]) );
            _left_most[_top] = _page[_top].pid().page;
            {
                xct_log_switch_t toggle(OFF);
                W_COERCE( _page[_top].insert(key, el, _slot[_top]++,
                                         _page[_top-1].pid().page) );
            }
        }
    }

    return RCOK;
}
