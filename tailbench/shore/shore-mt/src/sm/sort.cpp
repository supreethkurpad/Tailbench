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

 $Id: sort.cpp,v 1.128 2010/06/08 22:28:56 nhall Exp $

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
#define SORT_C

#ifdef __GNUG__
#    pragma implementation "sort_s.h"
#    pragma implementation "sort.h"
#endif


#include "sm_int_4.h"

#ifdef OLDSORT_COMPATIBILITY

#include "lgrec.h"
#include "sm.h"

typedef ssm_sort::key_info_t key_info_t;
typedef ssm_sort::sort_parm_t sort_parm_t;
typedef ssm_sort::sort_keys_t sort_keys_t;

#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_array_t<int2_t>;
template class w_auto_delete_array_t<rid_t>;
template class w_auto_delete_array_t<run_scan_t>;
#endif

extern "C" bool sort_is_instrumented();
bool sort_is_instrumented()
{
#ifdef INSTRUMENT_SORT
    return true;
#else
    return false;
#endif
}
#ifdef INSTRUMENT_SORT

#define INC_TSTAT_SORT(x)       INC_TSTAT(x)
#define ADD_TSTAT_SORT(x, y)    ADD_TSTAT(x, y)
#define GET_TSTAT_SORT(x)       GET_TSTAT(x)
#define SET_TSTAT_SORT(x, y)    SET_TSTAT(x, y)

#else

#define INC_TSTAT_SORT(x)
#define ADD_TSTAT_SORT(x, y)
#define GET_TSTAT_SORT(x)       0
#define SET_TSTAT_SORT(x, y)

#endif

#ifdef INSTRUMENT_SORT
/*
 * Record pertinent stats about malloced space
 */
inline void 
record_malloc(void * W_IFTRACE(p), smsize_t amt) 
{
    base_stat_t a = base_stat_t(amt);
    INC_TSTAT_SORT(sort_mallocs); 
    ADD_TSTAT_SORT(sort_malloc_bytes, (unsigned long)(a)); 
    ADD_TSTAT_SORT(sort_malloc_curr, (unsigned long)(a)); 
    base_stat_t m = GET_TSTAT_SORT(sort_malloc_max);
    if(m < a) { SET_TSTAT_SORT(sort_malloc_max, (unsigned long)(a)); }

    // scratch_used is the highwater mark
    base_stat_t c = GET_TSTAT_SORT(sort_malloc_curr);
    m = GET_TSTAT_SORT(sort_malloc_hiwat);
    if(c > m) {
        // m = c
        SET_TSTAT_SORT(sort_malloc_hiwat, (unsigned long)(c));
    }
    DBG(<<"record_malloc " << p << " size " << amt);
}

inline void 
record_free(void * W_IFTRACE(p), smsize_t amt) 
{
    if(amt) {
        // scratch_used is the highwater mark
        base_stat_t        c = GET_TSTAT_SORT(sort_malloc_curr);
        c -= base_stat_t(amt);
        SET_TSTAT_SORT(sort_malloc_curr, (unsigned long)(c));
    }
    DBG(<<"record_free " << p << " size " << amt);
}

#else

inline void
record_malloc(void *, smsize_t /* amt */)
{
}

inline void
record_free(void *, smsize_t /* amt */ )
{
}

#endif

//
// key struct for sorting stream
//
struct sort_key_t {
    char* val;          // pointer to key
    char* rec;          // pointer to data
    uint2_t klen;         // key length
    uint2_t rlen;         // record data length (if large, in place indx size)

    NORET sort_key_t() {
                val = rec = 0;
                klen = rlen = 0;
          };
    NORET ~sort_key_t() {
                record_free(val, 0);
                delete[] val;
                record_free(rec, 0);
                delete[] rec;
          };
};


struct file_sort_key_t {
    const char* val;    // pointer to key
    const char* rec;    // pointer to data
    uint2_t klen;         // key length
    uint2_t rlen;         // record data length (if large, in place indx size)
    char* hdr;          // pointer to header
    uint4_t blen;         // real body size
    uint2_t hlen;         // record header length

    NORET file_sort_key_t() {
        hdr = 0;
        klen = hlen = rlen = 0;
        blen = 0;
        val = 0;
        rec = 0;
    };
    NORET ~file_sort_key_t() {
        record_free(hdr, 0);
        delete[] hdr;
    }
};

//
// sort descriptor
//
struct sort_desc_t {
    stid_t tmp_fid;     // fid for the temporary file
    sdesc_t* sdesc;         // information about the file

    char** keys;           // keys to be sorted
    char** fkeys;           // file keys to be sorted
    uint rec_count;     // number of records for this run
    uint max_rec_cnt;   // max # of records for this run

    uint uniq_count;    // # of unique entries in runs

    rid_t* run_list;    // store first rid for all the runs
    uint2_t max_list_sz;  // max size for run list array
    uint2_t run_count;    // current size

    uint num_pages;     // number of pages
    uint total_rec;     // total number of records

    PFC comp;           // comparison function
    rid_t last_marker;  // rid of last marker
    rid_t last_rid;     // rid of last record in last run

    NORET sort_desc_t();
    NORET ~sort_desc_t();

    void  free_space() {
        uint total = total_rec < max_rec_cnt ? total_rec : max_rec_cnt;
        if (keys) { 
            for (uint i=0; i<total; i++)  {
                record_free(keys[i], sizeof(sort_key_t));
                delete ((sort_key_t*) keys[i]);
            }
            record_free(keys, max_rec_cnt*sizeof(char *));
            delete [] keys; keys = 0; max_rec_cnt = 0; 
        }
        if (fkeys) { 
            for (uint i=0; i<total; i++) {
                record_free(fkeys[i], sizeof(file_sort_key_t));
                delete ((file_sort_key_t*) fkeys[i]);
            }
            record_free(fkeys, max_rec_cnt*sizeof(char *));
            delete [] fkeys; fkeys = 0; max_rec_cnt = 0; 
        }
        uniq_count = 0;
    }

};


static int
_uint1_rcmp(uint4_t W_IFDEBUG9(klen1), const void* kval1,
            uint4_t W_IFDEBUG9(klen2), const void* kval2)
{
    w_assert9(klen1 == klen2);
    return (* (w_base_t::uint1_t*) kval2) - (* (w_base_t::uint1_t*) kval1);
}

static int
_uint2_rcmp(uint4_t W_IFDEBUG9(klen1), const void* kval1,
            uint4_t W_IFDEBUG9(klen2), const void* kval2)
{
    w_assert9(klen1 == klen2);
    return (* (w_base_t::uint2_t*) kval2) - (* (w_base_t::uint2_t*) kval1);
}

static int
_uint4_rcmp(uint4_t W_IFDEBUG9(klen1), const void* kval1,
            uint4_t W_IFDEBUG9(klen2), const void* kval2)
{
    w_assert9(klen1 == klen2);
    // return (* (w_base_t::uint4_t*) kval2) - (* (w_base_t::uint4_t*) kval1);
    bool ret = (* (w_base_t::uint4_t*) kval2) < (* (w_base_t::uint4_t*) kval1);
    return ret? -1 :
        ((* (w_base_t::uint4_t*) kval2) == (* (w_base_t::uint4_t*) kval1))? 0:
        1;
}

static int
_int1_rcmp(uint4_t W_IFDEBUG9(klen1), const void* kval1,
           uint4_t W_IFDEBUG9(klen2), const void* kval2)
{
    w_assert9(klen1 == klen2);
    return (* (w_base_t::int1_t*) kval2) - (* (w_base_t::int1_t*) kval1);
}


static int
_int2_rcmp(uint4_t W_IFDEBUG9(klen1), const void* kval1,
           uint4_t W_IFDEBUG9(klen2), const void* kval2)
{
    w_assert9(klen1 == klen2);
    return (* (w_base_t::int2_t*) kval2) - (* (w_base_t::int2_t*) kval1);
}


static int
_int4_rcmp(uint4_t W_IFDEBUG9(klen1), const void* kval1,
           uint4_t W_IFDEBUG9(klen2), const void* kval2)
{
    w_assert9(klen1 == klen2);
    // return (* (w_base_t::int4_t*) kval2) - (* (w_base_t::int4_t*) kval1);
    // a - b can overflow: use comparison instead
    bool ret = ((* (w_base_t::int4_t*) kval2) < (* (w_base_t::int4_t*) kval1));
    return ret? -1 : ((* (w_base_t::int4_t*) kval2) == (* (w_base_t::int4_t*) kval1))?
        0 : 1;
}


static int
_float_rcmp(uint4_t W_IFDEBUG9(klen1), const void* kval1,
            uint4_t W_IFDEBUG9(klen2), const void* kval2)
{
    w_assert9(klen1 == klen2);
    // w_base_t::f4_t tmp = (* (w_base_t::f4_t*) kval2) - (* (w_base_t::f4_t*) kval1);
    // a - b can overflow: use comparison instead
    bool res = (* (w_base_t::f4_t*) kval2) < (* (w_base_t::f4_t*) kval1);
    return res ? -1 : (
        ((* (w_base_t::f4_t*) kval1) == (* (w_base_t::f4_t*) kval2))
        ) ? 0 : 1;
}

static int
_double_rcmp(uint4_t W_IFDEBUG9(klen1), const void* kval1,
             uint4_t W_IFDEBUG9(klen2), const void* kval2)
{
    w_assert9(klen1 == klen2);
    w_base_t::f8_t d1, d2;
    // copy kval2 into d1, kval1 into d2 (reverse order)
    memcpy(&d2, kval1, sizeof(w_base_t::f8_t));
    memcpy(&d1, kval2, sizeof(w_base_t::f8_t));
    ADD_TSTAT_SORT(sort_memcpy_cnt,2);
    ADD_TSTAT_SORT(sort_memcpy_bytes, 2*sizeof(w_base_t::f8_t));
    // bool ret =  (* (w_base_t::f8_t*) kval1) < (* (w_base_t::f8_t*) kval2);
    bool ret =  d1 < d2;
    return ret ? -1 : (d1 == d2) ? 0 : 1;
}


static int 
_string_rcmp(uint4_t klen1, const void* kval1, uint4_t klen2, const void* kval2)
{
    unsigned char* p2 = (unsigned char*) kval1;
    unsigned char* p1 = (unsigned char*) kval2;
    int result = 0;
    for (uint4_t i = klen2 < klen1 ? klen2 : klen1;
         i > 0 && ! (result = *p1 - *p2);
         i--, p1++, p2++) ;
    return result ? result : klen2 - klen1;
}

static nbox_t _universe_(2);
static nbox_t _box_(2);


//
// Comparison function for rectangles (based on hilbert curve)
//
static int _spatial_cmp(uint4_t klen1, const void* kval1, uint4_t klen2,
                        const void* kval2)
{
    static nbox_t _universe_(2);
    w_assert9(klen1 == klen2);

    nbox_t box1((char*)kval1, (int)klen1),
           box2((char*)kval2, (int)klen2);
    
    return (box1.hcmp(box2, _universe_));
}
static int _spatial_rcmp(uint4_t klen1, const void* kval1, uint4_t klen2,
                        const void* kval2)
{
    w_assert9(klen1 == klen2);

    nbox_t box1((char*)kval1, (int)klen1),
           box2((char*)kval2, (int)klen2);
    
    return (box2.hvalue(_universe_) - box1.hvalue(_universe_));
//    return (box1.hcmp(box2, _universe_));
}

//
// Get comparison function
//
PFC 
sort_stream_i::get_cmp_func(key_info_t::key_type_t type, bool up) 
{
    if (up) {
      switch(type) {
          // case key_info_t::t_float:        
          case sortorder::kt_f4:        
            return sort_keys_t::f4_cmp;

          case sortorder::kt_f8:        
            return sort_keys_t::f8_cmp;

          // case key_info_t::t_string:
          case sortorder::kt_b:
            return sort_keys_t::string_cmp;

          // case key_info_t::t_spatial:
          case sortorder::kt_spatial:
            return _spatial_cmp;

          // case key_info_t::t_char:         use u1
            // return _char_cmp;
          case sortorder::kt_u1:
            return sort_keys_t::uint1_cmp;

          case sortorder::kt_u2:
            return sort_keys_t::uint2_cmp;

          case sortorder::kt_u4:
            return sort_keys_t::uint4_cmp;

          case sortorder::kt_u8:
            return sort_keys_t::uint8_cmp;

          case sortorder::kt_i1:
            return sort_keys_t::int1_cmp;

          case sortorder::kt_i2:
            return sort_keys_t::int2_cmp;

          //case key_info_t::t_int:
          case sortorder::kt_i4:
          default:                         
            return sort_keys_t::int4_cmp;

          case sortorder::kt_i8:
            return sort_keys_t::int8_cmp;
      }
    } else {
      switch(type) {
              // case key_info_t::t_float:        
              case sortorder::kt_f4:        
                return _float_rcmp;

              case sortorder::kt_f8:        
                return _double_rcmp;

              // case key_info_t::t_string: // TODO: byte? u1?
              case sortorder::kt_b:
                return _string_rcmp;

              case sortorder::kt_spatial:
                return _spatial_rcmp;

              // case key_info_t::t_char:         use u1
              // case sortorder::kt_u1:        
                // return _char_rcmp;
              case sortorder::kt_u1:
                return _uint1_rcmp;

              case sortorder::kt_u2:
                return _uint2_rcmp;

              case sortorder::kt_u4:
                return _uint4_rcmp;

              case sortorder::kt_i1:
                return _int1_rcmp;

              case sortorder::kt_i2:
                return _int2_rcmp;

              // case key_info_t::t_int:
              case sortorder::kt_i4:
              default:                         
                return _int4_rcmp;
      }
    }
}

/* XXX this should be handled as a sized type */
static const int _marker_ = min_int4; 

sort_desc_t::sort_desc_t() 
{
    rec_count = num_pages = total_rec = uniq_count = 0;
    run_count = 0;
    max_list_sz = 20;
    run_list = new rid_t[max_list_sz]; // deleted in ~sort_desc_t

    record_malloc(run_list, max_list_sz*sizeof(rid_t));

    max_rec_cnt = 0;
    keys = 0;
    fkeys = 0;
    last_rid = last_marker = rid_t::null;
    tmp_fid = stid_t::null;
}

sort_desc_t::~sort_desc_t() 
{
    if (run_list)  {
        record_free(run_list, 0);
        delete [] run_list;
    }
    free_space();
}

NORET
run_scan_t::run_scan_t()
{
    eof = true;
    single = false;
    slot = i = 0;
    fp = 0;
}

NORET
run_scan_t::~run_scan_t()
{
    if (fp) {
        record_free(fp, 0);
        delete [] fp;
    }
}

rc_t
run_scan_t::init(rid_t& begin, PFC c, const key_info_t& k, bool unique=false)
{
    i = 0;
    pid = begin.pid;
    slot = begin.slot - 1;
    cmp = c;
    kinfo = k;
    eof = false;

    DBG(<<"init run_scan rid=" << begin);


    // toggle_base = (_unique = unique) ? 2 : 1;
    // toggle in all cases
    _unique = unique;
    toggle_base = 2;

    fp = new file_p[toggle_base]; // deleted in ~run_scan_t
    record_malloc(fp, sizeof(file_p));

    // open scan on the file
    W_DO( fp[0].fix(pid, LATCH_SH) );
    INC_TSTAT_SORT(sort_page_fixes);
    W_DO( next(eof) );

    if (unique) {
        W_DO( ss_m::SSM->fi->next_page(pid, eof, NULL /* allocated only */) );
        INC_TSTAT_SORT(sort_page_fixes);
        if (eof) { 
            single = true; eof = false; 
        } else { 
            single = false; 
            W_DO( fp[1].fix(pid, LATCH_SH) );
            INC_TSTAT_SORT(sort_page_fixes);
        }
    } 

    return RCOK;
}

rc_t
run_scan_t::current(const record_t*& rec)
{
    if (eof) { rec = NULL; }
    else { 
        rec = cur_rec; 
        w_assert1(fp->is_fixed());
        /*
        DBGTHRD(<<" current: i " << i
                << " slot " << slot
                << " pid " << pid
                << " single " << single
            );
        */
    }

    return RCOK;
}

rc_t
run_scan_t::next(bool& end)
{
    end = false;
    if (eof) { end = true; return RCOK; }

/*
    DBGTHRD(<<" next: i " << i
        << " slot " << slot
        << " pid " << pid
        << " single " << single
    );
*/

    // scan next record
    slot = fp[i].next_slot(slot);
    if (slot==0) {
        DBGTHRD(<<"new page");
        cur_rec = NULL;
        if (_unique) { // unique case 
            // here
            if (single) { eof = true; end = true; return RCOK; }
            W_DO( ss_m::SSM->fi->next_page(pid, eof, NULL /* allocated only */) );
            INC_TSTAT_SORT(sort_page_fixes);
            if (eof) { single = true; eof = false; }
            else  {
                W_DO( fp[i].fix(pid, LATCH_SH) );
                INC_TSTAT_SORT(sort_page_fixes);
            }
            // for unique case, we replace the current page and
            // move i to the next page since we always have both
            // pages fixed.
            i = (i+1)%toggle_base;
        } else {
            W_DO( ss_m::SSM->fi->next_page(pid, eof, NULL /* allocated only */) );
            INC_TSTAT_SORT(sort_page_fixes);
            if (eof) { end = true; return RCOK; }
            i = (i+1)%toggle_base;
            W_DO( fp[i].fix(pid, LATCH_SH) );
            INC_TSTAT_SORT(sort_page_fixes);
        }
        slot = fp[i].next_slot(0);
    }

/*
    DBGTHRD(<<" next: i " << i
        << " slot " << slot
        << " pid " << pid
        << " single " << single
    );
*/
    W_DO(fp[i].get_rec(slot, cur_rec));

    w_assert1(cur_rec);
    w_assert1(fp[i].is_fixed());

    if ( cur_rec->body_size()==sizeof(int)
            && *(int*)cur_rec->body() == _marker_
            && cur_rec->hdr_size()==0 ) {
        end = eof = true;
        fp[i].unfix();
        cur_rec = NULL;
    }

    return RCOK;
}

bool
operator>(run_scan_t& s1, run_scan_t& s2)
{
    // get length and keys to be compared
    w_assert1(s1.cur_rec && s2.cur_rec);

    int len1, len2;
    if (s1.kinfo.len==0 && int(s1.kinfo.type)==int(key_info_t::t_string)) {
        // variable size, use hdr size
            len1 = s1.cur_rec->hdr_size(), 
        len2 = s2.cur_rec->hdr_size();
    } else {
        len1 = len2 = (int)s1.kinfo.len;
    }

    INC_TSTAT_SORT(sort_keycmp_cnt);
    return (s1.cmp(len1, s1.cur_rec->hdr(), len2, s2.cur_rec->hdr()) > 0);
}

//
// detect if two records are duplicates (only works for small objects)
//
static bool duplicate_rec(const record_t* rec1, const record_t* rec2)
{
    // check if two records are identical

    if (rec1->body_size() != rec2->body_size() ||
        rec1->hdr_size() != rec2->hdr_size())
        return false;
    
    if (rec1->body_size()>0 && 
        memcmp(rec1->body(), rec2->body(), (int)rec1->body_size()))
        return false;

    if (rec1->hdr_size()>0 && 
        memcmp(rec1->hdr(), rec2->hdr(), rec1->hdr_size()))
        return false;
    
    INC_TSTAT_SORT(sort_duplicates);
    return true;
}

//
// record size (for small object, it's the real size, for large record, it's
// the header size)
//
static inline uint rec_size(const record_t* r)
{
  // NB: The following replacement had to be made for gcc 2.7.2.x
  // with a later release, try the original code again
   return (r->tag.flags & t_small) ? (unsigned int)r->body_size()  : 
                    ((r->tag.flags & t_large_0) ? 
                       sizeof(lg_tag_chunks_s):
                        (unsigned int)(sizeof(lg_tag_indirect_s))
                            );

   /*
   return ((r->tag.flags & t_small) ? (unsigned int)r->body_size() 
                : ((r->tag.flags & t_large_0) ? 
                        sizeof(lg_tag_chunks_s) :
                        sizeof(lg_tag_indirect_s)));
    */
}

static PFC _local_cmp;

static int
qsort_cmp(const void* k1, const void* k2)
{
    INC_TSTAT_SORT(sort_keycmp_cnt);
    return _local_cmp(  ((sort_key_t*)k1)->klen,
                        ((sort_key_t*)k1)->val,
                        ((sort_key_t*)k2)->klen,
                        ((sort_key_t*)k2)->val );
}

static int
fqsort_cmp(const void* k1, const void* k2)
{
    INC_TSTAT_SORT(sort_keycmp_cnt);
    return _local_cmp(        ((file_sort_key_t*)k1)->klen,
                              ((file_sort_key_t*)k1)->val,
                        ((file_sort_key_t*)k2)->klen,
                        ((file_sort_key_t*)k2)->val );
}


static void create_heap(int2_t heap[], int heap_size, int num_runs,
                                run_scan_t sc[])
{
    int r;
    int s, k;
    int winner;
    int tmp;

    for (s=0; s<heap_size; heap[s++] = heap_size) ;
    for (r = 0, k = heap_size >> 1; r < heap_size; r+=2, k++) {
        if (r < num_runs - 1) { /* two real competitors */
            if (sc[r] > sc[r+1])
                heap[k] = r, winner = r+1;
            else
                heap[k] = r+1, winner = r;
        }
        else {
            heap[k] = -1;               /* an artifical key */
            winner = (r>=num_runs)? -1 : r;
        }

        for (s = k >> 1;; s >>= 1) { /* propagate the winner upwards */
            if (heap[s] == heap_size) { /* no one has reach here yet */
                heap[s] = winner; break;
            }

            if (winner < 0) /* a dummy key */
                winner = heap[s], heap[s] = -1;
            else if (sc[winner] > sc[heap[s]]) { 
                tmp = winner; winner = heap[s]; heap[s] = tmp;
            }
        }

    }
}

static int 
heap_top(int2_t heap[], int heap_size, int winner, run_scan_t sc[])
{
    int    s;      /* a heap index */
    int    r;      /* a run num */

    for (s = (heap_size + winner) >> 1; s > 0; s >>= 1) {
        if ((r = heap[s]) < 0)
            continue;
        if (sc[winner].is_eof())
            heap[s] = -1, winner = r;
        else if (sc[winner] > sc[r])
            heap[s] = winner, winner = r;
    }
    return (winner);
}

//
// Remove duplicate entries
//
rc_t 
sort_stream_i::remove_duplicates()
{
    unsigned pos=0, prev=0;
    sd->uniq_count = 0;

    if (!(sd->last_rid==rid_t::null)) {
        // read in the previous rec
        file_p tmp;
        const record_t* _r;
        DBGTHRD(<<"sort_stream_i:remove_duplicates");
        W_DO( fi->locate_page(sd->last_rid, tmp, LATCH_SH) );
        INC_TSTAT_SORT(sort_page_fixes);
        W_COERCE( tmp.get_rec(sd->last_rid.slot, _r) );

        smsize_t hlen = _r->hdr_size();
        smsize_t rlen = _r->body_size();

        if (_r->is_small()) {
            // compare against the previous one
            if (_file_sort) {
                while(pos<sd->rec_count) {
                    file_sort_key_t* fk = (file_sort_key_t*)sd->fkeys[pos];
                    if (fk->hlen==(int)hlen && fk->rlen==(int)rlen) {
                        if (hlen>0 && 
                            memcmp((char*)fk->hdr+fk->klen, 
                                _r->hdr()+fk->klen, hlen-fk->klen))
                            break;
                        if (rlen>0 && memcmp(fk->rec, _r->body(), (int)rlen))
                            break;
                        pos++;
                    } else { break; }
                }
            } else {
                while(pos<sd->rec_count) {
                    sort_key_t* k = (sort_key_t*)sd->keys[pos];
                    if (k->klen==(int)hlen && k->rlen==(int)rlen) {
                        if (hlen>0 && memcmp(k->val, _r->hdr(), hlen))
                            break;
                        if (rlen>0 && memcmp(k->rec, _r->body(), (int)rlen))
                            break;
                            pos++;
                    } else { break; }
                }
            }
        }

        if (pos>=sd->rec_count) {
            // none of them different, done
            return RCOK;
        } else {
            // move current to the first
            prev = pos++;
            sd->uniq_count++;
        }

    } else {
        pos = 1;
        sd->uniq_count++;
    }

    while (1) {
        if (_file_sort) {
          while(pos<sd->rec_count) {
            file_sort_key_t *fk1 = (file_sort_key_t*)sd->fkeys[pos],
                            *fk2 = (file_sort_key_t*)sd->fkeys[pos-1];
            if (fk1->hlen == fk2->hlen && fk1->rlen == fk2->rlen) {
                if (fk1->hlen>0 && 
                    memcmp((char*)fk1->hdr+fk1->klen, (char*)fk2->hdr+fk2->klen,
                           fk1->hlen - fk1->klen))
                    break;
                if (fk1->rlen>0 && memcmp(fk1->rec, fk2->rec, fk1->rlen))
                    break;
                pos++;
            } else { break; }
          }
        } else {
            while(pos<sd->rec_count) {
                sort_key_t *k1 = (sort_key_t*)sd->keys[pos],
                           *k2 = (sort_key_t*)sd->keys[pos-1];
                if (k1->klen == k2->klen && k1->rlen == k2->rlen) {
                    if (k1->klen>0 && memcmp(k1->val, k2->val, k1->klen))
                        break;
                    if (k1->rlen>0 && memcmp(k1->rec, k2->rec, k1->rlen))
                        break;
                    pos++;
                } else { break; }
            }
        }
        if (pos>=sd->rec_count) break;

        // move the entry up
        if (_file_sort) {
            // swap prev & uniq_count - 1
            char *tmp = sd->fkeys[sd->uniq_count-1];
            sd->fkeys[sd->uniq_count-1] = sd->fkeys[prev];
            sd->fkeys[prev] = tmp;
        } else {
            // swap prev & uniq_count - 1
            char* tmp = sd->keys[sd->uniq_count-1];
            sd->keys[sd->uniq_count-1] = sd->keys[prev];
            sd->keys[prev] = tmp;
        }
        sd->uniq_count++;
        prev = pos++;
    }
    if (prev) {
        if (_file_sort) {
            // swap prev & uniq_count - 1
            char *tmp = sd->fkeys[sd->uniq_count-1];
            sd->fkeys[sd->uniq_count-1] = sd->fkeys[prev];
            sd->fkeys[prev] = tmp;
        } else {
            // swap prev & uniq_count - 1
            char* tmp = sd->keys[sd->uniq_count-1];
            sd->keys[sd->uniq_count-1] = sd->keys[prev];
            sd->keys[prev] = tmp;
        }
    }
        
    return RCOK;
}

//
// find out the type of the record: small, large_0, large_1
//
static inline recflags_t
lgrec_type(uint2_t rec_sz)
{
    if (rec_sz==sizeof(lg_tag_chunks_s))
        return t_large_0;
    else if (rec_sz==sizeof(lg_tag_indirect_s))
        return t_large_1;
    
    W_FATAL(fcINTERNAL);
    return t_badflag;
}
        

static 
void QuickSort(char* a[], int cnt, int (*compar)(const void*, const void*) )
{
        /* XXX If you change this above 30, use the dynamic allocation
          code instead of the on-the-stack code */
    const int MAXSTACKDEPTH = 30;
    const int LIMIT = 10;
    static long randx = 1;

    struct qs_stack_item {
        int l, r;
    };
    qs_stack_item stack[MAXSTACKDEPTH]; // MAXSTACKDEPTH is small for now -->
                                  // 30 * 8 - 240 bytes
                                  // so avoid mallocs

    int sp = 0;
    int l, r;
    char* tmp;
    int i, j;
    char* pivot;

    for (l = 0, r = cnt - 1; ; ) {
        if (r - l < LIMIT) {
            if (sp-- <= 0) break;
            l = stack[sp].l, r = stack[sp].r;
            continue;
        }
        randx = (randx * 1103515245 + 12345) & 0x7fffffff;
        randx %= (r-l);
        pivot = a[l + randx];
        for (i = l, j = r; i <= j; )  {
            while (compar(a[i], pivot) < 0) i++;
            while (compar(pivot, a[j]) < 0) j--;
            if (i < j) { tmp=a[i]; a[i]=a[j]; a[j]=tmp; }
            if (i <= j) i++, j--;
        }

        if (j - l < r - i) {
            if (i < r) {
                if(sp >= MAXSTACKDEPTH) {
                    goto error;
                }
                stack[sp].l = i, stack[sp++].r = r;
            }
            r = j;
        } else {
            if (l < j) {
                if(sp >= MAXSTACKDEPTH) {
                    goto error;
                }
                stack[sp].l = l, stack[sp++].r = j;
            }
            l = i;
        }
    }

    for (i = 1; i < cnt; a[j+1] = pivot, i++)
        for (j = i - 1, pivot = a[i];
             j >= 0 && (compar(pivot, a[j]) < 0);
             a[j+1] = a[j], j--) ;

    return;

error:
    // not likely 
    smlevel_0::errlog->clog << fatal_prio
        << "QuickSort: stack too small" <<endl;
    W_FATAL(fcOUTOFMEMORY);
}

//
// Sort the entries in the buffer of the current run, 
// and flush them out to disk page.
//
rc_t 
sort_stream_i::flush_run()
{
    int i;
    if (sd->rec_count==0) return RCOK;

    if (sd->tmp_fid==stid_t::null) {
        // not last pass so use temp file
        if (_once) {
            W_DO( SSM->_create_file(sp.vol, sd->tmp_fid, _property) );
            INC_TSTAT_SORT(sort_files_created);
        } else {
            W_DO( SSM->_create_file(sp.vol, sd->tmp_fid, t_temporary) );
            INC_TSTAT_SORT(sort_files_created);
        }
    }
    
    W_COERCE( dir->access(sd->tmp_fid, sd->sdesc, NL) );
    w_assert1(sd->sdesc);

    file_p last_page;
    
    rid_t rid, first;

    // use quick sort 
    _local_cmp = sd->comp;
    _universe_ = ki.universe;

    if (_file_sort) {
        QuickSort(sd->fkeys, sd->rec_count, fqsort_cmp);
    } else {
        QuickSort(sd->keys, sd->rec_count, qsort_cmp);
        if (ki.len==0 && int(ki.type)!=int(key_info_t::t_string)) {
            ki.len = ((file_sort_key_t*)sd->keys[0])->klen;
        }
    }

    // remove duplicates if needed
    if (sp.unique) {
            W_DO ( remove_duplicates() );
            if (sd->uniq_count==0) return RCOK;
    }

    // load the sorted <key, elem> pair to temporary file
    int count = sp.unique ? sd->uniq_count : sd->rec_count;
    if (_file_sort) {
        for (i=0; i<count; i++) {
            const file_sort_key_t* k = (file_sort_key_t*) sd->fkeys[i];
            vec_t hdr, data(k->rec, k->rlen);
            if (_once) {
                hdr.put((char*)k->hdr+k->klen, k->hlen-k->klen);
                W_DO ( fi->create_rec_at_end(
                    last_page,
                    k->rlen,
                    hdr, data, 
                    *sd->sdesc, rid) );

                INC_TSTAT_SORT(sort_tmpfile_cnt);
                ADD_TSTAT_SORT(sort_tmpfile_bytes, hdr.size() + data.size());
            } else {
                hdr.put(k->hdr, k->hlen);
                    W_DO ( fi->create_rec_at_end( last_page, k->rlen,
                        hdr, data, *sd->sdesc, rid) );
                INC_TSTAT_SORT(sort_tmpfile_cnt);
                ADD_TSTAT_SORT(sort_tmpfile_bytes, hdr.size() + data.size());
            }
            if (k->blen > k->rlen) {
                // HACK:
                // for large object, we need to reset the tag
                // because we only did a shallow copy of the body
                W_DO( fi->update_rectag(rid, k->blen, lgrec_type(k->rlen)) );
             }
            if (rid.slot == 1) ++sd->num_pages;
            if (i == 0) first = rid;
        }
    } else {
        for (i=0; i<count; i++) {
            const sort_key_t* k = (sort_key_t*) sd->keys[i];
            vec_t hdr(k->val, k->klen), data(k->rec, k->rlen);
            W_DO ( fi->create_rec_at_end( last_page, k->rlen,
                    hdr, data, *sd->sdesc, rid) );

            INC_TSTAT_SORT(sort_tmpfile_cnt);
            ADD_TSTAT_SORT(sort_tmpfile_bytes, hdr.size() + data.size());
            if (rid.slot == 1) ++sd->num_pages;
            if (i == 0) first = rid;
        }
    }
    sd->last_rid = rid;

    if (!_once) {
            // put a marker to distinguish between different runs
            vec_t hdr, data((void*)&_marker_, sizeof(int));
            W_DO( fi->create_rec_at_end(
                last_page, sizeof(int), 
                hdr, data, *sd->sdesc, rid) );
        INC_TSTAT_SORT(sort_tmpfile_cnt);
        ADD_TSTAT_SORT(sort_tmpfile_bytes, hdr.size() + data.size());
    }

    sd->last_marker = rid;
    sd->total_rec += sd->rec_count;

    // record first rid for the current run
    if (sd->run_count == sd->max_list_sz) {
        // expand the run list space
        rid_t* tmp = new rid_t[sd->max_list_sz<<1]; // deleted in ~sort_desc_t
        record_malloc(tmp, (sd->max_list_sz << 1)*sizeof(rid_t));

        memcpy(tmp, sd->run_list, sd->run_count*sizeof(rid));
        INC_TSTAT_SORT(sort_memcpy_cnt);
        ADD_TSTAT_SORT(sort_memcpy_bytes, sd->run_count * sizeof(rid));

        record_free(sd->run_list, 0);
        delete [] sd->run_list;
        sd->run_list = tmp;
        sd->max_list_sz <<= 1;
    }
    sd->run_list[sd->run_count++] = first;

    // reset count for new run
    sd->rec_count = 0;
    buf_space.reset();
    return RCOK;
}

rc_t
sort_stream_i::flush_one_rec(const record_t *rec, rid_t& rid, 
                                const stid_t& 
                                , file_p& last_page, 
                                bool to_final_file // for stats only
                                )
{
    // for regular sort
    uint rlen = rec_size(rec);
    vec_t hdr(rec->hdr(), rec->hdr_size()),
          data(rec->body(), (int)rlen);

    W_DO( fi->create_rec_at_end(
        last_page, rlen, hdr, data, 
        *sd->sdesc, rid) );

    if(to_final_file) {
        INC_TSTAT_SORT(sort_recs_created);
        ADD_TSTAT_SORT(sort_rec_bytes,  hdr.size() + data.size());
    } else {
        INC_TSTAT_SORT(sort_tmpfile_cnt);
        ADD_TSTAT_SORT(sort_tmpfile_bytes,  hdr.size() + data.size());
    }
    if (!(rec->tag.flags & t_small)) {
        // large object, patch rectag for shallow copy
        W_DO( fi->update_rectag(rid, rec->tag.body_len, rec->tag.flags) );
    }
    
    return RCOK;
}

rc_t
sort_stream_i::merge(bool skip_last_pass=false)
{
    uint4_t i, j, k;
    bool        to_final_file = false;

//    if (sp.unique) { sp.run_size >>= 1; }

    for (i = sp.run_size-1, heap_size = 1; i>0; heap_size <<= 1, i>>=1) ;
    int2_t* m_heap = new int2_t[heap_size]; // auto-del
    record_malloc(m_heap, heap_size*sizeof(int2_t));
    w_assert1(m_heap);
    w_auto_delete_array_t<int2_t> auto_del_heap(m_heap);

    num_runs = sd->run_count;
    SET_TSTAT_SORT(sort_runs, num_runs);

    if (sd->run_count<=1) return RCOK;

    uint4_t out_parts = sd->run_count;
    int in_parts = 0;
    int num_passes = 1;
    for (i=sp.run_size; i<sd->run_count; i*=sp.run_size, num_passes++) ;
    
    uint4_t in_list_cnt = 0, out_list_cnt = sd->run_count;
    stid_t& out_file = sd->tmp_fid;
    rid_t  *out_list = sd->run_list, 
           *list_buf = new rid_t[out_list_cnt+1], // auto-del
           *in_list = list_buf;
           record_malloc(list_buf, (out_list_cnt+1)*sizeof(rid_t));

    w_assert1(list_buf);
    w_auto_delete_array_t<rid_t> auto_del_list(list_buf);

    SET_TSTAT_SORT(sort_ntapes, out_parts);

    for (i=0; out_parts>1; i++) {

#ifdef COMMENT
        DBG(<< "PHASE(1)? " << i
        << " skip_last_pass=" << skip_last_pass
        << " out_parts=" << out_parts);
#endif /* COMMENT */

        in_parts = out_parts;
        out_parts = (in_parts - 1) / sp.run_size + 1;
        
        { rid_t* tmp = in_list; in_list = out_list, out_list = tmp; }
        in_list_cnt = out_list_cnt;
        out_list_cnt = 0;
        stid_t in_file = out_file;
        bool last_pass = (out_parts==1);

        if (skip_last_pass && last_pass) {
            if (sd->run_list != in_list) {
                // switched, we need to copy the out list
                memcpy(sd->run_list, in_list, (int)in_list_cnt*sizeof(rid_t));
                INC_TSTAT_SORT(sort_memcpy_cnt);
                ADD_TSTAT_SORT(sort_memcpy_bytes, int(in_list_cnt * sizeof(rid_t)));
            }
            num_runs = in_parts;
            sd->tmp_fid = in_file;
            return RCOK;
        }

        if (last_pass) {
            // last pass may not be on temporary file
            // (file should use logical_id)
            W_DO( SSM->_create_file(sp.vol, out_file, sp.property) );
            INC_TSTAT_SORT(sort_files_created);
            to_final_file = true;
        } else {
            // not last pass so use temp file
            W_DO( SSM->_create_file(sp.vol, out_file, t_temporary) );
            INC_TSTAT_SORT(sort_files_created);
            to_final_file = false;
        }


        W_COERCE( dir->access(out_file, sd->sdesc, NL) );
        w_assert1(sd->sdesc);
        file_p last_page;

        int b;

#ifdef COMMENT
        DBG(<< "out_parts= " << out_parts 
        << " b = " << b
        << " in_parts=" << in_parts);
#endif /* COMMENT */

        for (j=b=0; j<out_parts; j++, b+=sp.run_size) {
                

            num_runs = (j==out_parts-1) ? (in_parts-b) : sp.run_size;

            INC_TSTAT_SORT(sort_phases);

#ifdef COMMENT
            DBG(<< "before creating HEAP of " << num_runs 
            <<  "runs, bp.npages="
            << bf->npages() 
            );
#endif /* COMMENT */

            run_scan_t* rs = new run_scan_t[num_runs]; // auto-del
            record_malloc(rs, num_runs*sizeof(run_scan_t));
                w_assert1(rs);
            w_auto_delete_array_t<run_scan_t> auto_del_run(rs);
            for (k = 0; k<num_runs; k++) {
                W_DO( rs[k].init(in_list[b+k], sd->comp, ki, sp.unique) );
                DBG(<<"run[" << k << "].first rid=" << rs[k].page());
            }

#ifdef COMMENT
            DBG(<< "init'd HEAP of " << num_runs <<  "runs, bp.npages="
            << bf->npages() 
            );
#endif /* COMMENT */

            if (num_runs == 1) m_heap[0] = 0;
            else {
                for (k = num_runs-1, heap_size = 1; k > 0;
                        heap_size <<= 1, k >>= 1) ;
                create_heap(m_heap, heap_size, num_runs, rs);
            }

            bool _eof, new_part = true;
            rid_t rid;
            const record_t *rec = 0, *_old_rec = 0;
            bool first_rec = true;

            uint2_t _r;
            int heap_top_count=0;
            for (_r = m_heap[0]; num_runs > 1; 
                 _r = heap_top(m_heap, heap_size, _r, rs)) 
            {
                ++heap_top_count;
                W_DO( rs[_r].current(rec) );
                if (sp.unique) {
                    if (first_rec) {
                            _old_rec = rec;
                            first_rec = false;
                    } else {
                        W_DO(flush_one_rec(_old_rec, rid, out_file, last_page, 
                                to_final_file));
                            _old_rec = rec;
                        if (new_part) {
                            out_list[out_list_cnt++] = rid;
                            new_part = false;
                                }
                    }
                } else {
                    W_DO( flush_one_rec(rec, rid, out_file, last_page,
                    to_final_file) );
                    if (new_part) {
                        out_list[out_list_cnt++] = rid;
                        new_part = false;
                    }
                }
                W_DO( rs[_r].next(_eof) );
                if (_eof) --num_runs;
            }
            DBG(<<"Pulled off heap: " << heap_top_count );

            int tail_of_run=0;
            do { //  for the rest in last run for current merge
                tail_of_run++;
                W_DO( rs[_r].current(rec) );
                if (sp.unique) {
                    if (first_rec) {
                        _old_rec = rec;
                        first_rec = false;
                    } else {
                        W_DO(flush_one_rec(_old_rec, rid, out_file, last_page,
                            to_final_file));
                        _old_rec = rec;
                        if (new_part) {
                            out_list[out_list_cnt++] = rid;
                            new_part = false;
                        }
                    }
                } else {
                    W_DO( flush_one_rec(rec, rid, out_file, last_page,
                        to_final_file) );

                    if (new_part) {
                        out_list[out_list_cnt++] = rid;
                        new_part = false;
                    }
                }

                W_DO( rs[_r].next(_eof) );

            } while (!_eof);
            DBG(<<"tail of run = " << tail_of_run);

            if (sp.unique) {
                W_DO( flush_one_rec(_old_rec, rid, out_file, last_page,
                    to_final_file) );
                if (new_part) {
                    out_list[out_list_cnt++] = rid;
                    new_part = false;
                }
            }
            if (!last_pass) {
                // put a marker to distinguish between different runs
                vec_t hdr, data((void*)&_marker_, sizeof(int));
                W_DO( fi->create_rec_at_end(
                    last_page, sizeof(int), hdr, data,
                    *sd->sdesc, rid) );

                INC_TSTAT_SORT(sort_tmpfile_cnt);
                ADD_TSTAT_SORT(sort_tmpfile_bytes, hdr.size() + data.size());
            }
            
        }
        DBGTHRD(<<"about to destroy " << in_file);
        W_DO ( SSM->_destroy_file(in_file) );
    }

    return RCOK;
}

NORET
sort_stream_i::sort_stream_i() : xct_dependent_t(xct()) 
{
    _file_sort = sorted = eof = false;
    empty = true;
    heap=0;
    sc=0;
    sd = new sort_desc_t;  // deleted in ~sort_stream_i
    record_malloc(sd, sizeof(sort_desc_t));
    _once = false;
    register_me();
}

NORET
sort_stream_i::sort_stream_i(const key_info_t& k, const sort_parm_t& s,
                uint est_rec_sz) : xct_dependent_t(xct())
{
    // TODO: should this enforce in xct and one thread attached?
    _file_sort = sorted = eof = false;
    empty = true;
    heap=0;
    sc=0;
    
    sd = new sort_desc_t;  // deleted in ~sort_stream_i
    record_malloc(sd, sizeof(sort_desc_t));
    ki = k;
    sp = s;

    sp.run_size = (sp.run_size<3) ? 3 : sp.run_size;
    if (est_rec_sz==0) {
        // no hint, just get a rough estimate
        est_rec_sz = (k.est_reclen ? k.est_reclen : 20);
    }

    // The record has to fit into a file page.
    sd->max_rec_cnt = (uint) (file_p::data_sz / 
         (
          align(sizeof(rectag_t)) + align(est_rec_sz)
          /*
           * This was too restrictive: that extra 8 bytes (ALIGNON)
           * was getting added in every time, even if not needed.
          align(sizeof(rectag_t)
               +ALIGNON
               +est_rec_sz)
          */
          +sizeof(page_s::slot_t)
         )
        * sp.run_size);
#ifdef W_TRACE
    if(sd->max_rec_cnt <= 0) {
        cerr 
            << " file_p::data_sz " << file_p::data_sz
            << endl
            << " sizeof(rectag_t) " << sizeof(rectag_t)
            << endl
            << " ALIGNON " << ALIGNON
            << endl
            << " est_rec_sz " << est_rec_sz
            << endl
            << " sizeof(page_s::slot_t) " << sizeof(page_s::slot_t)
            << endl
            << " sp.run_size " << sp.run_size
            << endl;
        cerr << " align(sizeof(rectag_t)+ALIGNON+est_rec_sz) " 
            << align(sizeof(rectag_t)+ALIGNON+est_rec_sz)
            << endl
            << " (align(sizeof(rectag_t)+ALIGNON+est_rec_sz) +sizeof(page_s::slot_t)) "
            << (align(sizeof(rectag_t)+ALIGNON+est_rec_sz)
             +sizeof(page_s::slot_t))
            << endl;
    }
#endif
    w_assert1(sd->max_rec_cnt > 0);

    sd->comp = get_cmp_func(ki.type, sp.ascending);
    _once = false;
    register_me();
}

NORET
sort_stream_i::~sort_stream_i()
{
    if (heap) {
        record_free(heap, 0);
        delete [] heap;
    }
    if (sc) {
        record_free(sc, 0);
        delete [] sc;
    }
    if (sd) {
        if (sd->tmp_fid!=stid_t::null) {
            DBGTHRD(<<"about to destroy " << sd->tmp_fid);
            W_IGNORE ( SSM->_destroy_file(sd->tmp_fid) );
        }
        record_free(sd, 0);
        delete sd;
    }
}

void 
sort_stream_i::finish()
{
    if (heap) {
        record_free(heap, 0);
        delete [] heap; heap = 0;
    }
    if (sc) {
        record_free(sc, 0);
        delete [] sc; sc = 0;
    }
    if (sd) {
        if (sd->tmp_fid!=stid_t::null) {
            if (xct()) {
                DBGTHRD(<<"about to destroy " << sd->tmp_fid);
                    W_COERCE ( SSM->_destroy_file(sd->tmp_fid) );
            }
        }
        record_free(sd, 0);
        delete sd;
        sd = 0;
    }
}

void
sort_stream_i::xct_state_changed(
    xct_state_t         /*old_state*/,
    xct_state_t         new_state)
{
    if (new_state == xct_aborting || new_state == xct_committing)  {
        finish();
    }
}

void
sort_stream_i::init(const key_info_t& k, const sort_parm_t& s, uint est_rec_sz)
{
    _file_sort = sorted = eof = false;
    ki = k;
    sp = s;

    sp.run_size = (sp.run_size<3) ? 3 : sp.run_size;

    if (!est_rec_sz) {
        // no hint, just get a rough estimate
        est_rec_sz = 20;
    }

    sd->max_rec_cnt = (uint) (file_p::data_sz / 
        (align(sizeof(rectag_t)+est_rec_sz)+sizeof(page_s::slot_t))
        * sp.run_size);
    
    // If we already have a list, get rid of it.
    // We'll create a new one when we put.
    if (sd->keys)  {
        for (unsigned i=0; i < sd->rec_count; i++) {
            record_free(sd->keys[i], sizeof(sort_key_t));
            delete ((sort_key_t*) sd->keys[i]);
        }
        // assert the the rest of the list was never allocated.
        for (unsigned i=sd->rec_count; i < sd->max_rec_cnt; i++) {
            w_assert2(sd->keys[i] == NULL);
        }
        record_free(sd->keys, (sd->max_rec_cnt * sizeof(sort_key_t*)));
        delete [] sd->keys;
        sd->keys = 0;
    }

    // If we already have a list, get rid of it.
    // We'll create a new one when we put.
    if (sd->fkeys)  {
        for (unsigned i=0; i<sd->rec_count; i++) {
            record_free(sd->fkeys[i], sizeof(file_sort_key_t));
            delete ((file_sort_key_t*) sd->fkeys[i]);
        }
        // assert the the rest of the list was never allocated.
        for (unsigned i=sd->rec_count; i < sd->max_rec_cnt; i++) {
            w_assert2(sd->fkeys[i] == NULL);
        }
        record_free(sd->fkeys, (sd->max_rec_cnt * sizeof(file_sort_key_t*)));
        delete [] sd->fkeys;
        sd->fkeys = 0;
    }

    sd->rec_count = 0;

    sd->comp = get_cmp_func(ki.type, sp.ascending);

    if (sc) { 
        record_free(sc, 0);
        delete [] sc; sc = 0; 
    }

    if (sd->tmp_fid!=stid_t::null) {
        W_COERCE( SSM->_destroy_file(sd->tmp_fid) );
        sd->tmp_fid = stid_t::null;
    }

    _file_sort = false;
}

rc_t
sort_stream_i::put(const cvec_t& key, const cvec_t& elem)
{
    SM_PROLOGUE_RC(sort_stream_i::put, in_xct, read_write,  0);
    w_assert1(!_file_sort);

    if (sd->rec_count >= sd->max_rec_cnt) {
        // flush current run
        W_DO ( flush_run() );
    }

    if (!sd->keys) {
        sd->keys = new char* [sd->max_rec_cnt]; // deleted in free_space
        record_malloc(sd->keys, sizeof(char *) * sd->max_rec_cnt);
        w_assert1(sd->keys);
        memset(sd->keys, 0, sd->max_rec_cnt*sizeof(char*));
        DBG(<<"allocated and zeroed keys " << (void *)sd->keys
                << " rec_cnt " << sd->rec_count 
                << " max_rec_cnt " << sd->max_rec_cnt 
                << " size " << (sizeof(char *) * sd->max_rec_cnt) );
        INC_TSTAT_SORT(sort_memcpy_cnt);
        ADD_TSTAT_SORT(sort_memcpy_bytes, sd->max_rec_cnt * sizeof(char *));
    }

    sort_key_t* k = (sort_key_t*) sd->keys[sd->rec_count];
    DBG(<<"existing keys " << (void *)(sd->keys)
                << " max_rec_cnt " << sd->max_rec_cnt );
    if (!k) {
        k = new sort_key_t; // deleted in free_space
        record_malloc(k, sizeof(sort_key_t));
        sd->keys[sd->rec_count] = (char*) k;
    } else {
        if (k->val) {
            // we don't know the size
            record_free(k->val, k->klen);
            delete [] k->val;
            k->klen = 0;
        }
        if (k->rec) {
            // we don't know the size
            record_free(k->rec, k->rlen);
            delete [] k->rec; 
            k->rlen = 0;
        }
    }

    // copy key
    k->val = new char[key.size()]; // deleted in ~sort_key_t
    record_malloc(k->val, key.size());
    key.copy_to(k->val, key.size());
    k->klen = key.size();

    // copy elem
    k->rlen = elem.size();
    k->rec = new char[k->rlen]; // deleted in ~sort_key_t
    record_malloc(k->rec, k->rlen);
    elem.copy_to(k->rec, k->rlen);

    sd->rec_count++;

    if (empty) empty = false;

    return RCOK;
}

rc_t
sort_stream_i::file_put(const cvec_t& key, const void* rec, uint rlen,
                        uint hlen, const rectag_t* tag)
{
    w_assert1(_file_sort);

    if (sd->rec_count >= sd->max_rec_cnt) {
        // ran out of array space
        if (_once) {
          if (sd->fkeys) {
            char** new_keys = new char* [sd->max_rec_cnt<<1]; //deleted in free_space
            record_malloc(new_keys, (sd->max_rec_cnt<<1) * sizeof(char *));
            memcpy(new_keys, sd->fkeys, sd->max_rec_cnt*sizeof(char*));
            INC_TSTAT_SORT(sort_memcpy_cnt);
            ADD_TSTAT_SORT(sort_memcpy_bytes, sd->max_rec_cnt * sizeof(char *));

            memset(&new_keys[sd->max_rec_cnt], 0, sd->max_rec_cnt*sizeof(char*));
            INC_TSTAT_SORT(sort_memcpy_cnt);
            ADD_TSTAT_SORT(sort_memcpy_bytes, sd->max_rec_cnt * sizeof(char *));

            // this is needed for shallow deletion of fkeys
            memset(sd->fkeys, 0, sd->max_rec_cnt*sizeof(char*));
            INC_TSTAT_SORT(sort_memcpy_cnt);
            ADD_TSTAT_SORT(sort_memcpy_bytes, sd->max_rec_cnt * sizeof(char *));

            record_free(sd->fkeys, 0);
            delete [] sd->fkeys;
            sd->fkeys = new_keys;
          }
        sd->max_rec_cnt <<= 1;
      } else {
        // flush current run
        W_DO ( flush_run() );
      }
    }

    if (!sd->fkeys) {
        sd->fkeys = new char* [sd->max_rec_cnt];// deleted in free_space
        record_malloc(sd->fkeys, (sd->max_rec_cnt) * sizeof(char *));
        w_assert1(sd->fkeys);
        memset(sd->fkeys, 0, sd->max_rec_cnt*sizeof(char*));
        INC_TSTAT_SORT(sort_memcpy_cnt);
        ADD_TSTAT_SORT(sort_memcpy_bytes, sd->max_rec_cnt * sizeof(char *));
    }

    file_sort_key_t* k = (file_sort_key_t*) sd->fkeys[sd->rec_count];
    if (!k) {
        k = new file_sort_key_t;
        record_malloc(k, sizeof(file_sort_key_t));
        sd->fkeys[sd->rec_count] = (char*) k;
    } else {
        if (k->hdr) { 
            record_free(k->hdr, 0);
            delete [] k->hdr; 
        }
    }

    // copy key and hdr
    k->hdr = new char[key.size()]; // deleted in ~file_sort_key_t
    record_malloc(k->hdr, key.size());
    key.copy_to((void*)k->hdr, key.size());
    k->val = k->hdr;
    k->klen = k->hlen = key.size();

    if (!ki.derived && ki.len > 0) {
        // for non-derived keys, the key will have rec hdr at end
        k->klen -= hlen;
    }

    // set up body
    k->rec = (char *) rec;        /* XXX should arg be char? */
    k->rlen = rlen;
    if (tag && !(tag->flags & t_small))
        k->blen = tag->body_len;
    else 
        k->blen = rlen;

    sd->rec_count++;
    if (empty) empty = false;

    return RCOK;
}

rc_t
sort_stream_i::get_next(vec_t& key, vec_t& elem, bool& end) 
{
    fill4 filler;
    W_DO( file_get_next(key, elem, filler.u4, end) );

    return RCOK;
}


rc_t
sort_stream_i::file_get_next(vec_t& key, vec_t& elem, uint4_t& blen, bool& end) 
{
    end = eof;
    if (eof) {
        finish();
        return RCOK;
    }

    if (!sorted) {
        // flush current run
        W_DO ( flush_run() );

        // free the allocated buffer space for sorting phase
        sd->free_space();

        // sort and merge, leave the final merge to the end
        W_DO ( merge(true) );

        sorted = true;

        if (num_runs==0) {
            end = eof = true;
            finish();
            return RCOK;
        }

        // initialize for the final merge: ???
        if (sc) {
            record_free(sc, 0);
            delete [] sc;
        }
        sc = new run_scan_t[num_runs]; // deleted in ~sort_stream_i
        record_malloc(sc, num_runs* sizeof(run_scan_t));

        uint4_t k;
        for (k = 0; k<num_runs; k++) {
            W_DO( sc[k].init(sd->run_list[k], sd->comp, ki, sp.unique) );
        }

        for (k = num_runs-1, heap_size = 1; k > 0; heap_size <<= 1, k >>= 1) ;
        if (heap) {
            record_free(heap, 0);
            delete [] heap;
        }
        heap = new int2_t[heap_size]; // deleted in ~sort_stream_i 
        record_malloc(heap, heap_size* sizeof(int2_t));
        
        if (num_runs == 1)  {
            r = heap[0] = 0;
        } else {
            create_heap(heap, heap_size, num_runs, sc);
            r = heap[0];
            r = heap_top(heap, heap_size, r, sc);
        }

        old_rec = 0;
    }
    
    // get current record
    const record_t *rec;
    bool part_eof;
    
    W_DO ( sc[r].current(rec) );
    /*
    DBGTHRD(<<"r " << r 
        << " rec: "
        << rec->body()[0]
        << rec->body()[1]
        << rec->body()[2]
        );
    */

    if (sp.unique) {
        if (old_rec) {
            bool same = duplicate_rec(old_rec, rec);
            while (same && num_runs>1) 
            {
                old_rec = rec;
                W_DO ( sc[r].next(part_eof) );
                
                r = heap_top(heap, heap_size, r, sc);
                if (part_eof)
                {
                    num_runs--;
                } else {    
                    W_DO ( sc[r].current(rec) );
                    same = duplicate_rec(old_rec, rec);
                }
            }
            if (same) 
            {
                while (1) 
                {
                    old_rec = rec;
                    W_DO ( sc[r].current(rec) );
                    if (!duplicate_rec(old_rec, rec))
                        break;
                    W_DO ( sc[r].next(part_eof) );
                    if (part_eof) {
                        // end, search in vain, still write the last one out
                        end = eof = true; 
                        finish();
                        return RCOK;
                    }
                }
            }
        }
        old_rec = rec;
    }

    // set up the key for output
    key.put(rec->hdr(), rec->hdr_size());
    elem.put(rec->body(), rec_size(rec));

    if (_file_sort) { blen = rec->body_size(); }

    // prepare for next get
    W_DO ( sc[r].next(part_eof) );

    if (num_runs>1)
        r = heap_top(heap, heap_size, r, sc);

    if (part_eof)  {
        if (--num_runs < 1) {
            eof = true;
        }
    }

    return RCOK;
}

//
// copy large object to a contigous chunk of space
//
static rc_t
_copy_out_large_obj(
        const record_t* rec,
        void*                data,
        uint4_t                start,
        uint4_t                len,
        const file_p&        hdr_page)
{
    lpid_t data_pid;
    lgdata_p page;
    char* buf_ptr = (char*) data;
    uint4_t start_byte, new_start = start, range, left = len, offset;
        
    w_assert1(!rec->is_small());

    while (left>0) {
        data_pid = rec->pid_containing(new_start, start_byte, hdr_page);
        offset = new_start-start_byte;
        range = MIN(lgdata_p::data_sz-offset, rec->body_size()-new_start); 
        range = MIN(left, range); 
        W_DO( page.fix(data_pid, LATCH_SH) );
        INC_TSTAT_SORT(sort_lg_page_fixes);

        memcpy(buf_ptr, (char*) page.tuple_addr(0)+offset, (int)range);
        INC_TSTAT_SORT(sort_memcpy_cnt);
#ifdef INSTRUMENT_SORT
        base_stat_t r = base_stat_t(range);
#endif
        ADD_TSTAT_SORT(sort_memcpy_bytes, (unsigned long)(r));

        buf_ptr += range; 
        new_start += range;
        left -= range;
    }

    w_assert1(left==0);

    return RCOK;
}
    

rc_t
ss_m::_sort_file(const stid_t& fid, vid_t vid, stid_t& sfid,
                sm_store_property_t property,
                const key_info_t& key_info, int run_size,
                bool ascending,
                bool unique, bool destructive
                )
{

    int i, j;

    if (run_size < 3) run_size = 3;

    SET_TSTAT_SORT(sort_run_size, run_size);

    // format sort parameters
    sort_parm_t sp;
    sp.ascending = ascending;
    sp.unique = unique;
    sp.vol = vid;
    sp.property = property;
    sp.destructive = destructive;

    bool large_obj = false;
    stid_t lg_fid;
    sdesc_t* sdesc;
    rid_t rid;
    if (!sp.destructive) {
        // HACK:
        // create a file to hold potential large object 
        // in a non-destructive sort.
        W_DO ( _create_file(vid, lg_fid, property) );
        INC_TSTAT_SORT(sort_files_created);
            W_COERCE( dir->access(lg_fid, sdesc, NL) );
        w_assert1(sdesc);
    }

    file_p last_lg_page;

    key_info_t kinfo = key_info;
    bool fast_spatial = (kinfo.type==sortorder::kt_spatial && !sp.unique);
    if (fast_spatial) { 
        // hack to speed up spatial comparison:
        // use hilbert value (turn into an integer sort)
        kinfo.type = sortorder::kt_i4;
        kinfo.len = sizeof(int);
    }

    w_assert1((kinfo.len>0) || 
                (kinfo.len==0 && int(kinfo.type)==int(key_info_t::t_string)));

    lpid_t pid, first_pid;
    W_DO( fi->first_page(fid, pid, NULL /* allocated only */) );
    first_pid = pid;
   
    
    // determine how many pages are used for the current file to 
    // estimate file size (decide whether all-in-memory is possible)
    bool eof = false;
    SmFileMetaStats file_stats;
    file_stats.smallSnum = fid.store;
    
    W_DO(io->get_file_meta_stats(fid.vol, 1, &file_stats));
    /* XXX possible loss of data */
    int pcount = int(file_stats.small.numAllocPages);

    
    bool once = (pcount<=run_size);
    if (once) run_size = pcount;

    SET_TSTAT_SORT(sort_run_size, run_size);

    // setup a sort stream
    sp.run_size = run_size;
    sort_stream_i sort_stream(kinfo, sp);

    if (once) {
        sort_stream.set_file_sort_once(property);
    } else {
        sort_stream.set_file_sort();
    }

    // iterate through records, put into sort stream
    // the algorithm fix a block of pages in buffer to
    // save the extra cost of copy each tuple from buffer pool
    // to to memory for sorting
    pid = first_pid;
    {
      file_p* fp = new file_p[pcount]; // auto-del
      record_malloc(fp, pcount* sizeof(file_p));
      w_auto_delete_array_t<file_p> auto_del_fp(fp);

      for (eof = false; ! eof; ) {
         for (i=0; i<run_size && !eof; i++) {
            W_DO( fp[i].fix(pid, LATCH_SH) );
            INC_TSTAT_SORT(sort_page_fixes);
            for (j = fp[i].next_slot(0); j; j = fp[i].next_slot(j)) {
                //
                // extract the key, and compress hdr with body
                //
                const record_t* r;
                W_COERCE( fp[i].get_rec(j, r) );

                const void *kval, *hdr, *rec;
                uint rlen, hlen, klen = key_info.len;

                hdr = r->hdr();
                hlen = r->hdr_size();

                if (!r->is_small()) {
                    // encounter large record in sort file:
                    //        has to be destructive sort
                    large_obj = true;
                    if (sp.destructive) {
                        rec = r->body();
                        rlen = rec_size(r);
                    } else {
                        // needs to copy the whole large object
                        rlen = (uint) r->body_size();

                        // copy the large object into another file
                        char* buf = new char[rlen]; // auto-del
                        record_malloc(buf, rlen);
                        w_auto_delete_array_t<char> auto_del_buf(buf);
                        W_DO( _copy_out_large_obj(r, buf, 0, rlen, fp[i]) );

                        vec_t b_hdr, b_rec(buf, rlen);
                        
                        W_COERCE( dir->access(lg_fid, sdesc, NL) );
                        w_assert1(sdesc);
                        W_DO ( fi->create_rec_at_end( last_lg_page,
                                rlen, b_hdr, b_rec, *sdesc, rid) );
                        INC_TSTAT_SORT(sort_recs_created);
                        ADD_TSTAT_SORT(sort_rec_bytes,  b_hdr.size() + b_rec.size());
                        // get its new index info
                        file_p tmp;
                        W_DO( fi->locate_page(rid, tmp, LATCH_SH) );
                        INC_TSTAT_SORT(sort_page_fixes);
                        const record_t* nr;
                        W_COERCE( tmp.get_rec(rid.slot, nr) );

                        rlen = rec_size(nr);
                        void* nbuf = sort_stream.buf_space.alloc(rlen);
                        memcpy(nbuf, nr->body(), rlen);
                        INC_TSTAT_SORT(sort_memcpy_cnt);
                        ADD_TSTAT_SORT(sort_memcpy_bytes, rlen);
                        rec = nbuf;
                    }

                    if (key_info.where==key_info_t::t_hdr) {
                        kval = (char*)hdr + key_info.offset;
                        if (key_info.len == 0)
                        {
                            // special casing for variable lenght key in header
                            klen = hlen;
                        }
                    } else {
                        // has to copy the region of bytes for key
                        void* buf = sort_stream.buf_space.alloc(key_info.len);
                        W_DO( _copy_out_large_obj(r, buf, key_info.offset,
                                                 key_info.len, fp[i]) );
                        kval = buf;
                    }
                } else {
                    rec = r->body();
                    rlen = rec_size(r);
                    kval = (char*) ((key_info.where==key_info_t::t_hdr)
                                 ? hdr : rec) + key_info.offset;
                }

                // construct sort key, append hdr at the end        
                cvec_t key;
                int hvalue;
                if (fast_spatial) {
                    // hack for spatial keys (nbox_t type)
                    // use the hilbert value as the key
                    _box_.bytes2box((const char*)kval, (int)key_info.len);
                    hvalue = _box_.hvalue(key_info.universe);
                    key.put(&hvalue, sizeof(int));
                } else {
                    key.put(kval, (int)klen);
                }

                    
                // another hack: since the sort stream stores keys at
                // the header of intermediate rec, we have to append 
                // original header at end of key.
                if (hlen>0 && !key_info.derived && key_info.len > 0) {
                    key.put(hdr, hlen);
                }

                W_DO ( sort_stream.file_put(key, rec, rlen, hlen, &r->tag) );
            }

            W_DO( fi->next_page(pid, eof, NULL /* allocated only */) );
            INC_TSTAT_SORT(sort_page_fixes);
         }

         // flush each run
         W_DO ( sort_stream.flush_run() );
        INC_TSTAT_SORT(sort_runs);
      }
    }

    last_lg_page.unfix();


    if (once) {
        // get the sfid
        sfid = sort_stream.sd->tmp_fid;
        sort_stream.sd->tmp_fid = stid_t::null;

        if (sfid == stid_t::null) {
            // empty file, creating an empty file
            W_DO ( _create_file(vid, sfid, property) );
            INC_TSTAT_SORT(sort_files_created);
        }
    } else {
        // get sorted stream, write to the final file
        W_DO ( _create_file(vid, sfid, property) );
        INC_TSTAT_SORT(sort_files_created);

        file_p last_page;

        if (!sort_stream.is_empty()) { 
            size_t        tmp_len = 1000;
            char* tmp_buf = new char[tmp_len];
            record_malloc(tmp_buf, tmp_len);

            if (!tmp_buf)
            W_FATAL(fcOUTOFMEMORY);
            w_auto_delete_array_t<char> auto_del_buf(tmp_buf);

            bool eof;
            uint4_t blen;
            vec_t key, hdr, rec;
            W_DO ( sort_stream.file_get_next(key, rec, blen, eof) );

            uint offset = kinfo.len;

            W_COERCE( dir->access(sfid, sdesc, NL) );
            w_assert1(sdesc);

                while (!eof) {
                /* XXX I think it might be possible to not require the
                   temporary buffer to hold the key.  The vec/key stuff
                   can probably just grab the correct bytes without
                   the intermediate copy.  However, that stuff
                   is complex and I have no way of testing it at
                   the moment, so this auto-sizing key stuff is it
                   for now. */
                if (key.size() > tmp_len) {
                    // XXX if keys become large, don't allocate too much
                    size_t        new_len = tmp_len > 1024*512
                                            ?  key.size() + 1024
                                            : tmp_len * 2;
                    char        *new_buf = new char[new_len];
                    if (!new_buf)
                            W_FATAL(fcOUTOFMEMORY);

                    record_malloc(new_buf, new_len - tmp_len);
                    record_free(tmp_buf, 0);
                    delete [] tmp_buf;
                    tmp_buf = new_buf;
                    tmp_len = new_len;
                    auto_del_buf.set(tmp_buf);
                }

                uint hlen;

                if ((hlen=(uint)(key.size()-offset))>0 ) {
                    key.copy_to(tmp_buf, key.size());
                }
                if (hlen>0) {
                        hdr.put(tmp_buf+offset, hlen);
                }
                W_DO ( fi->create_rec_at_end(
                        last_page,
                        rec.size(), hdr, rec, 
                        *sdesc, rid) );
                INC_TSTAT_SORT(sort_tmpfile_cnt);
                ADD_TSTAT_SORT(sort_tmpfile_bytes,  hdr.size() + rec.size());

                if (blen>rec.size()) {
                        // HACK:
                        // for large object, we need to reset the tag
                        // because we only did a shallow copy of the body
                        W_DO( fi->update_rectag(
                                rid, blen, lgrec_type(rec.size())) );
                }
                key.reset();
                hdr.reset();
                rec.reset();
                W_DO ( sort_stream.file_get_next(key, rec, blen, eof) );
                }
          }
    }

    // destroy input file if necessary
    if (sp.destructive) {
        if (large_obj) {
            W_DO ( _destroy_n_swap_file(fid, sfid) );
        } else {
            W_DO ( _destroy_file(fid) );
        }
    } else {
        if (large_obj) {
            W_DO ( _destroy_n_swap_file(lg_fid, sfid) );
        } else {
            W_DO ( _destroy_file(lg_fid) );
        }
    }

    return RCOK;
}

rc_t
ss_m::sort_file(const stid_t& fid, // I - input file id
        vid_t vid,                 // I - volume id for output file
        stid_t& sfid,              // O - output sorted file id
        sm_store_property_t property,
        const key_info_t& key_info,// I - info about sort key
                                   //     (offset, len, type...)
        int run_size,              // I - # pages each run
        bool ascending,                   // I - ascending?
        bool unique,                   // I - eliminate duplicates?
        bool destructive,           // I - destroy the input file?
        bool use_new_sort           // I - true
        )    
{
    /* BEFORE entering SM: */
    if(use_new_sort) {
        // New sort treats run_size as total# pages used,
        // not exactly as run size.
        run_size++;
        return ss_m::new_sort_file(fid, vid, sfid, property,
                        key_info, run_size, ascending, 
                        unique, destructive);
    }

    SM_PROLOGUE_RC(ss_m::sort_file, in_xct, read_write, 0);
    W_DO(_sort_file(fid, vid, sfid, property, key_info, run_size,
                        ascending, unique, destructive));
    return RCOK;
}
#endif /* OLDSORT_COMPATIBILITY */
