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

 $Id: rtree.cpp,v 1.147 2010/06/08 22:28:55 nhall Exp $

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
#define RTREE_C

#ifdef __GNUG__
#    pragma implementation "rtree.h"
#    pragma implementation "rtree_p.h"
#endif

#include <cmath>
#include <sm_int_2.h>
#include <rtree_p.h>
#include "sm_du_stats.h"

#include <crash.h>

#ifndef MIN
#define MIN(x,y) ((x) < (y) ? (x) : (y))
#endif

#ifndef MAX
#define MAX(x,y) ((x) > (y) ? (x) : (y))
#endif

#ifndef ABS
#define ABS(x) ((x) >= 0 ? (x) : -(x))
#endif


#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_array_t<wrk_branch_t>;
#endif


inline void 
SWITCH(int2_t& x, int2_t& y)
{
    { int2_t _temp_ = x; x = y; y = _temp_; }
}

const double MaxDouble = 4.0*max_int4*max_int4;

void 
rtstk_t::drop_all_but_last()
{
    w_assert1(! is_empty());
    while (_top>1) {
        _stk[--_top].page.unfix();
    }
}

ftstk_t::ftstk_t() {
    _top = 0;
    for (int i=0; i<ftstk_chunk; i++) {
        _indirect[i] = NULL;
    }
}

ftstk_t::~ftstk_t() {
    shpid_t MAYBE_UNUSED tmp;
    while (_top>0) {
        tmp = pop();
        // xd->lock()
    }
}

void
ftstk_t::empty_all() {
    while (_top > 0) {
        _top--;
        if (_top>=ftstk_chunk && _top%ftstk_chunk==0) {
            delete _indirect[_top/ftstk_chunk - 1];
        }
    }
}

//
// class rtwork_p
//
// In-memory version of rtree_p. Useful for temporary storage of a page
// before converting to persistent version. Heavy use in rtree bulk load.
//
class rtwork_p : public smlevel_0 {
    page_s* _buf;
    rtree_p _pg;
    nbox_t* mbb;
    
public:
    rtwork_p()
    : _buf(new page_s), _pg(_buf, st_tmp)  {
        w_assert3(_pg.is_fixed());
        mbb = new nbox_t(2);
    }

    rtwork_p(const lpid_t& pid, int2_t l, int2_t d = 2)
    : _buf(new page_s), _pg(_buf, st_tmp) {
            /*
             * Turning off logging makes this a critical section:
             */
            xct_log_switch_t log_off(smlevel_0::OFF);
            W_COERCE( _pg.format(pid, rtree_p::t_rtree_p, _pg.t_virgin,
                st_regular) );
            W_COERCE( _pg.set_level(l) );
            W_COERCE( _pg.set_dim(d) );
            mbb = new nbox_t(d);
        }

    ~rtwork_p() { if (_buf)  delete _buf;  if (mbb) delete mbb; }

    void init(int2_t l, int2_t d = 2) {
        lpid_t pid;
        /*
         * Turning off logging makes this a critical section:
         */
        xct_log_switch_t log_off(smlevel_0::OFF);

        W_COERCE( _pg.format(pid, rtree_p::t_rtree_p, _pg.t_virgin, 
                st_regular) ) ;
        W_COERCE( _pg.set_level(l)) ;
        W_COERCE( _pg.set_dim(d)) ;
        if (!mbb) mbb = new nbox_t(d);
        }

    void swap(rtwork_p& page) {
        rtree_p tmp = _pg; _pg = page._pg; page._pg = tmp;
        page_s* tmp_buf = _buf; _buf = page._buf; page._buf = tmp_buf;
        nbox_t* tmp_box; tmp_box = mbb; mbb = page.mbb; page.mbb = tmp_box;
        }

    const nbox_t& bound() { return *mbb; }

    rc_t                        calc_set_bound() {
        return _pg.calc_bound(*mbb);
    }

    void expn_bound(const nbox_t& key) {
            if(!key.is_Null()) {
                (*mbb) += key;
            }
        }

    rtree_p* rp()                { return &_pg; }  // convert to rtree page
};

//
// class rtld_cache_t
//
// Rtree bulk load cache entry: allow a buffer of 3 pages
// before repacking and flushing out the next page at one
// level.
//
class rtld_cache_t {
    rtwork_p buf[3];
    int2_t _idx;
    nbox_t* last_box;

public:
    rtld_cache_t() { _idx = -1; last_box=NULL; }
    ~rtld_cache_t() { if (last_box) delete last_box; }

    int2_t count() { return _idx+1; }
    void incr_cnt() { _idx++; w_assert1(_idx<3); }

    void init_buf(int2_t lvl);
        
    rtwork_p* top() {
        w_assert1(_idx >= 0);
        return &buf[_idx];
        }

    rtwork_p* bottom() {
        w_assert1(_idx >= 0);
        return &buf[0];
        }

    rc_t        force(
        rtwork_p&             work,
        bool&             out,
        nbox_t*             universe);
};

//
// class rtld_stk_t
//
// Rtree bulk load stack: each entry is the cache entry defined
// above, which corresponds to a level in the Rtree. Top entry
// is for the leaf level.
//
class rtld_stk_t {
    enum { max_ldstk_sz = 10 };
    rtree_p                 rp;
    rtld_cache_t*         layers;
    int                 _top;
    rtld_desc_t         dc;

    void init_next_layer() {
        _top++;
        layers[_top].init_buf(_top+1);
        layers[_top].incr_cnt();
        }

    rc_t                        tmp2real(rtwork_p* tmp, rtree_p* real);

public:
    uint2_t height;
    uint4_t leaf_pages;
    uint4_t num_pages;
    uint4_t fill_sum;

    rtld_stk_t(const rtree_p& rtp, const rtld_desc_t& desc) :
        rp(rtp), 
        layers(0),
        _top(-1),
        height(0),
        leaf_pages(0),
        num_pages(0),
        fill_sum(0)
        {
            layers = new rtld_cache_t[max_ldstk_sz]; 
            dc = desc;
        }

    ~rtld_stk_t() { if (layers) delete[] layers; }

    rc_t                        add_to_stk(
        const nbox_t&                    key,
        const cvec_t&                     el,
        shpid_t                     child,
        int2_t                             level);
    rc_t                        wrap_up();
};

//
// sort function: sort rectangles on specified axis. (-1 for area)
//

static int 
_sort_cmp_area_(const void* k1, const void* k2)
{
    wrk_branch_t &b1 = *(wrk_branch_t *) k1;
    wrk_branch_t &b2 = *(wrk_branch_t *) k2;

    double diff = b1.area - b2.area;

    if (diff > 0.0) return 1;
    else if (diff < 0.0) return -1;
    else return 0;
}


/* Until a method of supporting per-sort (per thread) sort context 
   is developed, only allow one per-axis sort at a time. */

struct sort_context {
    int             _axis;
    queue_based_block_lock_t _sort_context_lock;
};

static struct sort_context sort_context;


static int 
_sort_cmp_axis_(const void* k1, const void* k2)
{
    wrk_branch_t &b1 = *(wrk_branch_t *) k1;
    wrk_branch_t &b2 = *(wrk_branch_t *) k2;
    double        diff;
    int           axis = sort_context._axis;

    diff = (double)b1.rect.bound(axis) - (double)b2.rect.bound(axis);
    if (diff==0.0) {
            diff = (double)(b1.rect.bound(axis + b1.rect.dimension()))
                    - (double)(b2.rect.bound(axis + b2.rect.dimension()));
    }

    if (diff > 0.0) return 1;
    else if (diff < 0.0) return -1;
    else return 0;
}


static void 
quick_sort(wrk_branch_t key[], int2_t num, int axis)
{
    if (axis == -1)
            qsort(key, num, sizeof(wrk_branch_t), _sort_cmp_area_);
    else {
            /* only allow one sort at a time, due to the _axis dependency */
        CRITICAL_SECTION(cs, sort_context._sort_context_lock);
        sort_context._axis = axis;
        qsort(key, num, sizeof(wrk_branch_t), _sort_cmp_axis_);
    }
}


//
// overlap: sum of overlap between target and each box in list[]
//
static double 
overlap(const nbox_t& target, int2_t index, 
        wrk_branch_t list[], int2_t num)
{
    int i;
    double sum = 0.0, area;

    for (i=0; i<num; i++) {
        if (i==index) continue;
        if ((area = (target^list[i].rect).area()) < 0.0 ) continue;
        sum += area;
    }
    return sum;
}

//
// set up header info on rtree page
//
rc_t
rtree_base_p::set_hdr(const rtctrl_t& new_hdr)
{
    vec_t v;
    v.put(&new_hdr, sizeof(new_hdr));
    vec_t hdr_vec_tmp(&new_hdr, sizeof(new_hdr));
    W_DO( overwrite(0, 0, hdr_vec_tmp) );
    return RCOK;
}

rc_t
rtree_p::format(const lpid_t& pid, tag_t tag, 
        uint4_t flags, store_flag_t store_flags)
{
    W_DO( rtree_base_p::format(pid, tag, flags, store_flags) );
    return RCOK;
}

MAKEPAGECODE(rtree_p, rtree_base_p)
MAKEPAGECODE(rtree_base_p, keyed_p)

//
// set up level in header
//
rc_t
rtree_base_p::set_level(int2_t l)
{
    rtctrl_t tmp;
    
    tmp = _hdr();
    if (tmp.level == l) return RCOK;
    tmp.level = l;
    W_DO( set_hdr(tmp) );
    return RCOK;
}

//
// set up dimension in header
//
rc_t
rtree_base_p::set_dim(int2_t d)
{
    rtctrl_t tmp;
    
    tmp = _hdr();
    if (tmp.dim == d) return RCOK;
    tmp.dim = d;
    W_DO( set_hdr(tmp) );
    return RCOK;
}

//
// recalc the bounding box of the whole page
//
rc_t
rtree_p::calc_bound(nbox_t& nb)
{
    int i;
    nbox_t cur;
    nb.nullify();

    for (i=nrecs()-1; i>=0; i--)  {
        cur.bytes2box(rec(i).key(), rec(i).klen());
        if(!cur.is_Null()) {
            nb += cur;
        }
    }

    return RCOK;
}

//
// print out all entries in the current rtree page
//
void 
rtree_p::print()
{
    int i, j;
    nbox_t key;

    for (i=0; i<nrecs(); i++)  {
            key.bytes2box(rec(i).key(), rec(i).klen());
        key.print(cout, level() );

        for (j=0; j<5-level(); j++) cout << "\t";
        if ( is_node() )  
            cout << ":- pid = " << rec(i).child() << endl;
        else 
            cout << "elem=(" << rec(i).elem() << ")" << endl;
    }
    cout << endl;
}

//
// draw all the entries in the current rtree page
//
void 
rtree_p::draw(ostream &DrawFile, nbox_t &CoverAll)
{
    int i;
    nbox_t key;

    for (i=0; i<nrecs(); i++)  {
            key.bytes2box(rec(i).key(), rec(i).klen());
        key.draw(level(), DrawFile, CoverAll);
    }
}

//
// calculate overlap percentage (expensive -- n**2)
//         total overlap area / total area
// 
//
uint2_t
rtree_p::ovp()
{
    int i, j;
    nbox_t key1, key2;
    double all_sum = 0.0, ovp_sum = 0.0, area;

    for (i=0; i<nrecs(); i++)  {
            key1.bytes2box(rec(i).key(), rec(i).klen());
        if ((area = key1.area()) < 0.0) continue;
        all_sum += area;
        for (j=0; j<nrecs(); j++) {
            if (i!=j) {
                key2.bytes2box(rec(j).key(), rec(j).klen());
                if ((area = (key1^key2).area()) < 0.0) continue;
                ovp_sum += area;
            }
        }
    }

    if (all_sum == 0.0) return 0;
    else return (uint2_t) (ovp_sum*50.0 / all_sum);
}

//
// format the rtree page
//
rc_t
rtree_base_p::format(const lpid_t& pid, tag_t tag, 
        uint4_t flags, store_flag_t store_flags)
{
    rtctrl_t rtc;
    rtc.level = 1;
    rtc.dim = 2;
    vec_t vec;
    vec.put(&rtc, sizeof(rtctrl_t));

    W_DO( keyed_p::format(pid, tag, flags, store_flags, vec) );

    return RCOK;
}

//
// pick the optimum branch in non-leaf page
//
void 
rtree_p::pick_optimum(const nbox_t& key, slotid_t& ret_slot)
{
    w_assert3( is_node() );

    ret_slot = -1;
    int i;
    double min_area = MaxDouble;
    int2_t min_idx = -1, count = nrecs();
    wrk_branch_t* work = new wrk_branch_t[count];
    w_assert1(work);
    w_auto_delete_array_t<wrk_branch_t> auto_del_work(work);

    // load the working structure
    for (i=0; i<count; i++) {
        work[i].rect.bytes2box(rec(i).key(), rec(i).klen());
        work[i].area = work[i].rect.area();
        work[i].idx = i;
        if(key.is_Null()) {
            // Just find min-sized box
            if( work[i].area < min_area ) {
                min_area = work[i].area; ret_slot = i;
            }
        } else {
            if( work[i].rect / key && work[i].area < min_area ) {
                min_area = work[i].area; ret_slot = i;
            }
            work[i].area = (work[i].rect+key).area() - work[i].area;
        }
    }

    if (this->level()==2) {
        // for one level above leaf
            double min_diff = MaxDouble;
        if (ret_slot!=-1) { // find one containment with least area
            return;
        }
        // sort according to area difference of expansion
        quick_sort(work, count, -1);

        // find the pick with least overlapping resulting from the insertion
        // a hack to reduce computation
        if(key.is_Null()) {
            min_idx = MIN(32, count)-1;
        } else for (i=MIN(32, count)-1; i>=0; i--) {
            double diff = overlap((key + work[i].rect), i, work, count) -
                                overlap(work[i].rect, i, work, count);
            if (diff < min_diff) { min_diff = diff; min_idx = i; }
        }
    } else {
        // for other levels: find the pick with least expansion and area
        min_idx = 0;
        for (i=1; i<count; i++) {
            double diff = work[min_idx].area - work[i].area;
            if (diff > 0.0) {
                min_idx = i; continue;
            }
            if (diff==0.0 && work[i].rect.area()<work[min_idx].rect.area())
                min_idx = i;
        }
    }
    
    ret_slot = work[min_idx].idx;
}
            
//
// remove REMOVE_RATIO*100% of furthest entries (in terms of center distance)
//
rc_t
rtree_p::ov_remove(rtree_p* dst, const nbox_t& key, const nbox_t& bound)
{
    int i, j;
    int2_t count = this->nrecs();
    int2_t num_rem = (int2_t) ((count+1) * REMOVE_RATIO + 0.5);
    wrk_branch_t* work = new wrk_branch_t[count+1];
    if (!work) return RC(smlevel_0::eOUTOFMEMORY);
    w_auto_delete_array_t<wrk_branch_t> auto_del_work(work);

    // load work structure
    for (i=0; i<count; i++) {
         work[i].rect.bytes2box(rec(i).key(), rec(i).klen());
         work[i].area = -1.0 * (work[i].rect*bound); // center distance
         work[i].idx = i;
    }
    work[count].area = -1.0 * (key * bound); // negative of center distance
    work[count].idx = count;
    work[count].rect = key;

    // sort by "area" (center distance in descending order)
    quick_sort(work, count+1, -1);

    // remove the last num_rem entries
    for (i=0; i<num_rem; i++) {
        int2_t index = work[i].idx;
        // find the largest index of tuple to be removed
        for (j=i+1; j<num_rem; j++)
            if (index < work[j].idx) { SWITCH(index, work[j].idx); }
        if (index != count) {
            // move the current tuple to destination page
            // skip the work[count] which is for the extra entry
            // to be inserted (not in the page yet).
                const rtrec_t& tuple = rec(index);
            W_DO( dst->insert(tuple) );
            W_DO( this->remove(index) );
        }
    }

    return RCOK;
}
            
//
// insert one tuple
//
rc_t
rtree_p::insert(const rtrec_t& tuple)
{
    nbox_t key(tuple.key(), tuple.klen());
    vec_t el((const void*) tuple.elem(), tuple.elen());
    shpid_t child = tuple.child();

    return ( insert(key, el, child) );
}

//
// insert one entry
//
rc_t
rtree_p::insert(const nbox_t& key, const cvec_t& el, shpid_t child)
{
    if (child==0) { w_assert3(is_leaf()); }
    else { w_assert3(!is_leaf()); }

    slotid_t num_slot = 0;
    u_char *smap = new u_char[rtree_p::smap_sz];
    w_auto_delete_array_t<u_char> auto_del(smap);

    // do a search to find the correct position
    if ( search(key, nbox_t::t_exact, smap, num_slot, key.is_Null(), &el, child) ) {
        DBG(<<"duplicate, key=" << key);
        return RC(eDUPLICATE);
    } else {
        keyrec_t::hdr_s hdr;
        hdr.klen = key.klen();
        hdr.elen = el.size();
        hdr.child = child;

        vec_t vec;
        vec.put(&hdr, sizeof(hdr)).put(key.kval(), key.klen()).put(el);
        W_DO( keyed_p::insert_expand( bm_first_set(smap, nrecs()+1, 0) + 1,
                                      1, &vec) );
        return RCOK;
    }
}

//
// remove one tuple
//
rc_t
rtree_base_p::remove(slotid_t slot)
{
    W_DO( keyed_p::remove_compress(slot + 1, 1) );
    return RCOK;
}

/*
 * Helper function for _exact_match(), below
 * Return true if key matches box found in slot 
 */

bool 
rtree_p::_key_match(const nbox_t& key, int slot, bool include_nulls,
        bool& bigger) const
{
    bool result = false;
    const rtrec_t& tuple = rec(slot);
    nbox_t box;

    box.bytes2box(tuple.key(), tuple.klen());

    // So we don't get to the nbox_t comparison operators below
    // if either key or box is Null...
    if(box.is_Null()) {
        if(include_nulls) {
            if(key.is_Null()) {
                // found exact match
                result = true;
            } else {
                // treat as box < key
                bigger = true;
            }
        } else { // ignore nulls
            // treat as box < key and keep looking
            bigger = true;
        }
    } else if(key.is_Null()) {
        if(include_nulls) {
            if(box.is_Null()) {
                // found exact match
                result = true;
            } else {
                // treat as box > key
            }
        } else { // ignore nulls
            result = false;
        }
    } else if (box==key) {
        result = true;
    } else if (box<key) {
        bigger = true;
    }
    return result;
}

//
// exact match:
// Sets bits for all satisfying slots.  
// We can only have >1 satisfying slot if we are scanning,
// i.e., no element is given for comparison.
//
bool 
rtree_p::exact_match(const nbox_t& key, u_char smap[], const cvec_t& el, 
                     const shpid_t child, bool include_nulls)
{
    int low, high, mid;
    int diff;
    int2_t cnt = nrecs();
    nbox_t box;

    DBG(<<"_exact_match: cnt=" << cnt << " include_nulls=" <<include_nulls );
    bm_zero(smap, cnt+1);
    if(key.is_Null() && !include_nulls) return false;

    for (low=mid=0, high=cnt-1; low<=high; ) {
        mid = (low + high) >> 1;
        bool keymatch=false;
        bool keybigger=false;
        keymatch = _key_match(key, mid, include_nulls, keybigger);
        DBG(<<"keymatch=" << keymatch);
        if (keymatch) {
            if (is_leaf()) { // compare elements
                if (el.size()==0) { 
                    DBG(<<" mid=" << mid);
                    bm_set(smap, mid); 

                    /*
                     * Since we are scanning (no element given)
                     * we can have duplicates, but we'll miss
                     * them if we don't go up & down, setting
                     * bits for all that match.
                     */

                    // Go up until we run out.
                    // NB: test is slot < high because of the
                    // ++slot in the _key_match call
                    int slot=mid;
                    while (slot < high && (keymatch = 
                        _key_match(key, ++slot, include_nulls, keybigger))) {
                        bm_set(smap, slot);
                    }

                    // Go down until we run out.
                    // NB: test is slot > 0 because of the
                    // --slot in the _key_match call
                    slot = mid;
                    while (slot > 0 && (keymatch = 
                        _key_match(key, --slot, include_nulls, keybigger))) {
                        bm_set(smap, slot);
                    }
                    return true; 
                }
                const rtrec_t& tuple = rec(mid);
                if ((diff = el.cmp(tuple.elem(), tuple.elen())) > 0) {
                    low = mid + 1;
                } else if (diff < 0) {
                    high = mid - 1;
                } else { 
                    DBG(<<" mid=" << mid);
                    bm_set(smap, mid); 
                    return true; 
                }
            } else { // compare children
                const rtrec_t& tuple = rec(mid);
                if (tuple.child() == child) {
                    bm_set(smap, mid); 
                    return true;
                }
                if (child > tuple.child())
                  low = mid + 1;
                else // child < tuple.child()
                  high = mid -1;
            }
        } else if (keybigger) { // (box < key)  
            low = mid + 1; 
        } else    {
            high = mid - 1;
        }
    }

    bm_set(smap, ((low > mid) ? low : mid));

    return false;
}

// Type of basic spatial comparison func
typedef int(*SPATIAL_CMP_FUNC)(const nbox_t&, const nbox_t&, nbox_t::sob_cmp_t);
//
// basic comparison functions for spatial objects (other than exact match)
//
static int
sob_cmp(const nbox_t& key, const nbox_t& box, nbox_t::sob_cmp_t type)
{
    bool result;

    switch(type) {
        case nbox_t::t_overlap:         // overlap match
            // ignore nulls altogether
            if(key.is_Null() || box.is_Null() ) {
                DBG(<<"sob_cmp Null argument returns false");
                return false;
            }
            result = (box || key);
            break;

        case nbox_t::t_cover:                 // coverage: key covers box
            // ignore nulls altogether
            if(key.is_Null() || box.is_Null() ) {
                DBG(<<"sob_cmp Null argument returns false");
                return false;
            }
            result = (key / box);
            break;

        case nbox_t::t_inside:                // containment: key contained in box
            /* HACK: special case for t_inside, because nbox_t::Null is 
             * inside everything, and we need to do sob_cmp_nulls rather
             * than sob_cmp iff condition is key t_inside X for key.is_Null()
             * even if we're not including nulls in the results.
             * We use this function ONLY if not including nulls in the
             * results, so, first skip nulls in on the page (box):
             */
            if(box.is_Null() ) {
                DBG(<<"sob_cmp box null, returns false");
                return false;
            }
            // Now consider non-nulls - Null key is inside them.
            if(key.is_Null()) {
                DBG(<<"sob_cmp t_inside Null key returns true");
                return true;
            }
            result = (box / key);
            break;

        default:
            W_FATAL(fcINTERNAL);
            result = false;
            break;
    }
    DBG(<<"sob_cmp " << key << " , " << box << "  returns " << result);
    return result;
}
//
// basic functions for spatial objects (other than exact match)
// where nulls are considererd
//
static int
sob_cmp_nulls(const nbox_t& key, const nbox_t& box, nbox_t::sob_cmp_t type)
{
    bool        result;
    switch(type) {
        case nbox_t::t_overlap:         // overlap match
            // Null overlaps everything and everything overlaps Null
            if(box.is_Null() || key.is_Null()) {
                DBG(<<"");
                result = true;
            } else {
                DBG(<<"");
                result =  (box || key);
            }
            break;

        case nbox_t::t_cover:                 // coverage: key covers box
            // Null covers nothing except Null
            if(key.is_Null()) {
                DBG(<<"");
                result = box.is_Null();
            } else if(box.is_Null()) {
                // Everything covers Null, including Null
                DBG(<<"");
                result = true;
            } else {
                DBG(<<"");
                result = (key / box);
            }
            break;

        case nbox_t::t_inside:                // containment: key contained in box
            // Null inside everything, including Null
            if(key.is_Null()) {
                DBG(<<"");
                result = true;
            } else if(box.is_Null()) {
                // Nothing inside Null except Null
                DBG(<<"");
                result = key.is_Null();
            } else {
                DBG(<<"");
                result =  (box / key);
            }
            break;

        default:
            W_FATAL(fcINTERNAL);
            result = false;
            break;
    }
    DBG(<<"sob_cmp_nulls " << key << " , " << box << "  returns " << result);
    return result;
}

/*
//
// Spatial search. Called only by rtree_p::search().

// On input:
// key is the key for the search condition.
// ctype is the operation for the search condition.
// smap is a bitmap allocated by the caller, to be filled-in by
//    this function.
// num_slot is == -1 or != -1.  If == -1, it means
//    we quit after the first satisfying slot is found,
//    and return with num_slot == 0.
// include_nulls tells whether or not we are considering
//    null keys when satisfying the condition.

// When done:
// num_slot is set to number of slots that satisfy the
//    search conditions.  
// smap has the bits set for the satisfying slots
// at least 1 slot satisfies, as indicated by the assertion,
//    but I don't know why that is true.
//
*/

bool 
rtree_p::spatial_srch(const nbox_t& key, nbox_t::sob_cmp_t ctype, 
                               u_char smap[], slotid_t& num_slot,
                               bool include_nulls)
{
    int i;
    bool done = (num_slot == -1);
    num_slot = 0;
    int2_t cnt = nrecs();
    nbox_t box;

    SPATIAL_CMP_FUNC        cmp = include_nulls ?
                                        sob_cmp_nulls : sob_cmp;

    bm_zero(smap, cnt);
    for (i=0; i<cnt; i++) {
        const rtrec_t& tuple = rec(i);
        box.bytes2box(tuple.key(), tuple.klen());
        if ((*cmp)(key, box, ctype)) {
            bm_set(smap, i); 
            num_slot++;
            if (done) break;
        }
    }

    return (num_slot>0);
}

//
// translate operation to appropriate search condition on non-leaf nodes
//
bool 
rtree_p::query(const nbox_t& key, nbox_t::sob_cmp_t ctype, u_char smap[],
                        slotid_t& num_slot,
                        bool include_nulls)
{
    bool found=false;
    w_assert3(!is_leaf());
    nbox_t::sob_cmp_t cond = 
        (ctype==nbox_t::t_exact)? nbox_t::t_inside : nbox_t::t_overlap;
    found = search(key, cond, smap, num_slot, include_nulls);

    DBG(<<"page.query: key= " << key 
        << " include_nulls=" << include_nulls
        << " found= " << found 
        << " num_slot=" << num_slot
        << " cond=" << int(cond)
    );
    return found;
}

//
// leaf and non-leaf level search function
//
bool 
rtree_p::search(
        const nbox_t& key, 
        nbox_t::sob_cmp_t ctype, 
        u_char smap[],
        slotid_t& num_slot, 
        bool  include_nulls,
        const cvec_t* el, 
        const shpid_t child
)
{
    bool found=false;
    switch(ctype) {
        case nbox_t::t_exact:                 // exact match search: binary search
            if(key.is_Null() && !include_nulls) return false;
            num_slot = 1;
            DBG(<<"set num_slot to 1");
            if (!el) {
                cvec_t _dummy;
                found = exact_match(key, smap, _dummy, child, include_nulls);
                break;
            }
            found =  exact_match(key, smap, *el, child, include_nulls);
            break;
          
        case nbox_t::t_overlap:         // overlap match : anything overlapped
                                        // by the key
        case nbox_t::t_cover:                 // coverage: any rct covered by key
        case nbox_t::t_inside:                // containment: any rct covering key
            DBG(<<"num_slot going into spatial_src=" << num_slot);
            found = spatial_srch(key, ctype, smap, num_slot, include_nulls);
            break;
        default:
            found = false;
            break;
    }
    DBG(<<"page.search: key= " << key << " include_nulls=" << include_nulls
        << " found=" << found << " num_slot=" << num_slot
    );
    return found;
}

//
// allocate a page for rtree:
//   This has to be called within a compensating action.
//
rc_t
rtree_m::_alloc_page(
    const lpid_t&        root,
    int2_t                level,
    const rtree_p&        near_p,
    int2_t                dim,
    lpid_t&                pid)
{
    w_assert3(near_p.is_fixed());
    W_DO( io->alloc_a_page(root.stid(), 
        near_p.pid(),  // hint
        pid,        // npages, array for output pids
        true,         // may realloc
        NL,        // ignored
        true        // search file
        ) );

    rtree_p page;
    W_DO( page.fix(pid, LATCH_EX, page.t_virgin) );

    rtctrl_t hdr;
    hdr.root = root;
    hdr.level = level;
    hdr.dim = dim;  
    W_DO( page.set_hdr(hdr) );

    return RCOK;
}

//
// detect if the rtree is empty
//
bool 
rtree_m::is_empty(const lpid_t& root)
{
    rtree_p page;
    W_IGNORE( page.fix(root, LATCH_SH) ); // PAGEFIXBUG
    return (page.nrecs()==0);
}

//
// general search for exact match
//
rc_t
rtree_m::_search(
    const lpid_t&                 root,
    const nbox_t&                 key,
    const cvec_t&                 el,
    bool&                         found,
    rtstk_t&                         pl,
    oper_t                         oper,
    bool                        include_nulls)
{
    //lock_mode_t lmode = (oper == t_read) ? SH : EX;
    latch_mode_t latch_m = (oper == t_read) ? LATCH_SH : LATCH_EX;

    rtree_p page;
    W_DO( page.fix(root, latch_m) );

    // read in the root page
    //    W_DO( lm->lock(root, lmode, t_medium, WAIT_FOREVER) );
    pl.push(page, -1);

    // traverse through non-leaf pages
    W_DO ( _traverse(key, el, found, pl, oper, include_nulls) );

    return RCOK;
}

//
// traverse the tree (for exact match)
//
rc_t
rtree_m::_traverse(
    const nbox_t&        key,
    const cvec_t&         el,
    bool&                 found,
    rtstk_t&                 pl,
    oper_t                 oper,
    bool                include_nulls)
{
    int i;
    //lock_mode_t lmode = (oper == t_read) ? SH : EX;
    latch_mode_t latch_m = (oper == t_read) ? LATCH_SH : LATCH_EX;

    rtree_p page;

    lpid_t pid = pl.top().page.pid();
    int2_t count = pl.top().page.nrecs();
    slotid_t num_slot = 0;
    DBG(<<"init local num_slot to 0");

    u_char *smap = new u_char[rtree_p::smap_sz];
    w_auto_delete_array_t<u_char> auto_del(smap);

    if (! pl.top().page.is_leaf())  {
        // check for containment
        found = ((rtree_p)pl.top().page).search(key, nbox_t::t_inside, smap, num_slot, include_nulls);
        if (!found) { // fail to find in the sub tree
            return RCOK;
        }
        
        int first = -1;
        for (i=0; i<num_slot; i++) {
            first = bm_first_set(smap, count, ++first);
            pl.update_top((int2_t)first); // push to the stack
            
            // read in the child page
            pid.page = pl.top().page.rec(first).child(); 
            W_DO( page.fix(pid, latch_m) );

            // traverse its children
            pl.push(page, -1); // push to the stack
            W_DO ( _traverse(key, el, found, pl, oper, include_nulls) );
            if (found) { return RCOK; }

            // pop the top 2 entries from stack, check for next child
            pl.pop();
        }

        // no luck
        found = false;
        return RCOK;
    }
    
    // reach leaf level: exact match
    found = ((rtree_p)pl.top().page).search(key, nbox_t::t_exact, smap, num_slot, include_nulls, &el);
    if (found) { pl.update_top((int2_t)bm_first_set(smap, count, 0)); }

    return RCOK;
}

//
// traverse the tree in a depth-first fashion (for range query)
//
rc_t
rtree_m::_dfs_search(
    const lpid_t&        root,
    const nbox_t&        key,
    bool&                found,
    nbox_t::sob_cmp_t        ctype,
    ftstk_t&                fl,
    bool                include_nulls)
{
    int i;
    rtree_p page;
    lpid_t pid = root;

    if (fl.is_empty()) { found = false; return RCOK; }

    // read in the page (no lock needed, already locked)
    pid.page = fl.pop();

    W_DO( page.fix(pid, LATCH_SH) );
    slotid_t num_slot = 0;
    slotid_t count = page.nrecs();

    u_char *smap = new u_char[rtree_p::smap_sz];
    w_auto_delete_array_t<u_char> auto_del(smap);

    DBG(<<"_dfs_search");

    if (! page.is_leaf())  {
        // check for condition
        found = page.query(key, ctype, smap, num_slot, include_nulls);
        DBG(<<"page.query: key= " << key << " include_nulls=" << include_nulls
            << " found=" << found << " num_slot=" << num_slot
        );
        if (!found) {
            // fail to find in the sub tree, search for the next
            // W_DO( lm->unlock(page.pid()) );
            return RCOK;
        }

        int first = -1;
        for (i=0; i<num_slot; i++) {
            first = bm_first_set(smap, count, ++first);
            pid.page = page.rec(first).child();
            // lock the child pages
                // W_DO( lm->lock(pid, SH, t_long, WAIT_FOREVER) );
            fl.push(pid.page); // push to the stack
        }
        
        // W_DO( lm->unlock(page.pid()) );

        found = false;
        while (!found && !fl.is_empty()) {
            W_DO ( _dfs_search(root, key, found, ctype, fl, include_nulls) );
        }

        return RCOK;
    }

    //
    // leaf page
    //
    num_slot = -1; // interested only in first slot found
    DBG(<<"num_slot <- -1 for spatial search");

    found = page.search(key, ctype, smap, num_slot, include_nulls);

    if (found) { fl.push(pid.page); }

    return RCOK;
}
        
//
// pick branch for insertion at specified level (for forced reinsert)
//
rc_t
rtree_m::_pick_branch(
    const lpid_t&        root,
    const nbox_t&        key,
    rtstk_t&                pl,
    int2_t                lvl,
    oper_t                oper)
{
    slotid_t slot = 0;
    lpid_t pid = root;
    rtree_p page;
    //lock_mode_t lmode = (oper == t_read) ? SH : EX;
    latch_mode_t latch_m = (oper == t_read) ? LATCH_SH : LATCH_EX;

    if (pl.is_empty()) {
        // W_DO(lm->lock(root, lmode, t_medium, WAIT_FOREVER));
        W_DO( page.fix(root, latch_m) );
        pl.push(page, -1);
    }

    // traverse through non-leaves
    while (pl.top().page.level() > lvl) {
        ((rtree_p)pl.top().page).pick_optimum(key, slot); // pick optimum path
        pl.update_top(slot); // update index
        
            // read in child page
        pid.page = pl.top().page.rec(slot).child();
        // W_DO(lm->lock(pid, lmode, t_medium, WAIT_FOREVER));
        W_DO( page.fix(pid, latch_m) );
        pid = page.pid();
        pl.push(page, -1);
    }

    return RCOK;
}

//
// overflow treatment:
//  remove some entries in the overflow page and reinsert them
//
rc_t
rtree_m::_ov_treat(
    const lpid_t&        root,
    rtstk_t&                pl,
    const nbox_t&        key,
    rtree_p&                ret_page,
    bool*                lvl_split)
{

    int i;
    rtree_p page = pl.top().page;
    int2_t level = page.level();
    w_assert3(page.pid()!=root && !lvl_split[level]);

    // get the bounding box for current page
    const rtrec_t& tuple = pl.second().page.rec(pl.second().idx);
    nbox_t bound(tuple.key(), tuple.klen());

    // forced reinsert
    rtwork_p work_page(page.pid(), page.level(), page.dim());

    W_DO( page.ov_remove(work_page.rp(), key, bound) );
    W_DO( _propagate_remove(pl, false) );
    lvl_split[level] = true;

    pl.drop_all_but_last();
    for (i=0; i<work_page.rp()->nrecs(); i++) {
        // reinsert
        W_DO ( _reinsert(root, work_page.rp()->rec(i), pl, level, lvl_split) );
        pl.drop_all_but_last();
    }

    // search for the right path for the insertion
    W_DO( _pick_branch(root, key, pl, level, t_insert) );
    ret_page = pl.top().page;
    w_assert3(ret_page.level()==level);

    return RCOK;
}

static void sweep_n_split(int axis, wrk_branch_t work[], u_char smap[],
                int& margin, int max_num, int min_num, nbox_t* extra=NULL);

//
// split one page
//
rc_t
rtree_m::_split_page(
    const lpid_t&        root,
    rtstk_t&                pl,
    const nbox_t&        key,
    rtree_p&                ret_page,
    bool*                lvl_split)
{
    int i;
    rtree_p page(pl.pop().page);
    int2_t count = page.nrecs();

    wrk_branch_t* work = new wrk_branch_t[count+1];
    if (!work) return RC(eOUTOFMEMORY);
#if ZERO_INIT
    memset(work, '\0', sizeof(wrk_branch_t) * (count +1) );
#endif
    w_auto_delete_array_t<wrk_branch_t> auto_del_work(work);

    // load up work space
    for (i=0; i<count; i++) {
         const rtrec_t& tuple = page.rec(i);
         work[i].rect.bytes2box(tuple.key(), tuple.klen());
         work[i].area = 0.0;
         work[i].idx = i;
    }
    work[count].area = 0.0;
    work[count].idx = count;
    work[count].rect = key;

    int min_margin = max_int4, margin;

    u_char *save_smap = new u_char[rtree_p::smap_sz];
    w_auto_delete_array_t<u_char> auto_del_save(save_smap);

    u_char *smap = new u_char[rtree_p::smap_sz];
    w_auto_delete_array_t<u_char> auto_del(smap);

    // determine which axis and where to split
    for (i=0; i<key.dimension(); i++) {
        sweep_n_split(i, work, smap, margin, count+1, 
                        (int2_t) (count*MIN_RATIO));
        if (margin < min_margin) {
            min_margin = margin;
            int bytes_of_bits_to_copy = count/8 + 1;
            memcpy(save_smap, smap, bytes_of_bits_to_copy);
        }
    }

    // create a new sibling page
    lpid_t sibling;
    W_DO( _alloc_page(root, page.level(), page, page.dim(), sibling) );
    // W_DO( lm->lock(sibling, EX, t_long, WAIT_FOREVER) );
    rtree_p sibling_p;
    W_DO( sibling_p.fix(sibling, LATCH_EX) );

    // distribute the children to the sibling page
    for (i=count-1; i>=0; i--) {
        if (bm_is_set(save_smap, i)) {
            // shift the tuple to sibling page
            W_DO( sibling_p.insert(page.rec(i)) ); 
            W_DO( page.remove(i) );
        }
    }

      // re-calculate the bounding box
    nbox_t sibling_bound(page.dim()),
           page_bound(page.dim());
    W_DO( page.calc_bound(page_bound) );
    W_DO( sibling_p.calc_bound(sibling_bound) );
    if (bm_is_set(save_smap, count)) {
        ret_page = sibling_p;
        if(!key.is_Null()) {
            sibling_bound += key;
        }
    } else {
        ret_page = page;
        if(!key.is_Null()) {
            page_bound += key;
        }
    }
    
    // now to adjust the higher level
    if (page.pid() == root)  {
        // split root
        // create a duplicate for root
        lpid_t duplicate;
            W_DO( _alloc_page(root, page.level(), page, page.dim(), duplicate) );
            // W_DO(lm->lock(duplicate, EX, t_long, WAIT_FOREVER));
            rtree_p duplicate_p;
        W_DO( duplicate_p.fix(duplicate, LATCH_EX) );
        
        // shift all tuples in root to duplicate
        W_DO( page.shift(0, &duplicate_p) );
        W_DO( page.set_level(page.level()+1) );

        // insert the two children in
        vec_t el((void*)&sibling, 0);
        W_DO( page.insert(sibling_bound, el, sibling.page) );
        W_DO( page.insert(page_bound, el, duplicate.page) );
        
        pl.push(page, -1); // push to stack

        // release pages
        if (sibling_p.pid() != ret_page.pid()) {
            ret_page = duplicate_p;
            // W_DO( lm->unlock(sibling) );
        } else {
            // W_DO( lm->unlock(duplicate) ); }
        }
    } else {

            rtree_p parent(pl.top().page);
        int index = pl.top().idx;
        
        // replace the tuple: result of a different bounding box
        // (should use a method to change the bounding box)
        const rtrec_t& tuple = parent.rec(index);
        nbox_t old_bound(tuple.key(), tuple.klen());
        vec_t el((const void*) tuple.elem(), tuple.elen());
        shpid_t child = tuple.child();
        
        W_DO( parent.remove(index) );
        W_DO( parent.insert(page_bound, el, child) );
        
        // release the page that doesn't contain the new entry
        if (sibling_p.pid() != ret_page.pid()) {
            // W_DO( lm->unlock(sibling) );
        } else {
            // W_DO( lm->unlock(duplicate) ); }
        }

        // insert to the parent
        W_DO( _new_node(root, pl, sibling_bound, sibling_p, lvl_split) );
    }

    return RCOK;
}

//
// insertion of a new node into the current page
//
rc_t
rtree_m::_new_node(
    const lpid_t&        root,
    rtstk_t&                pl,
    const nbox_t&        key,
    rtree_p&                subtree,
    bool*                lvl_split)
{
    rtree_p page(pl.top().page);
    vec_t el((const void*) &subtree.pid().page, 0);
    bool MAYBE_UNUSED split = false;

    rc_t rc = page.insert(key, el, subtree.pid().page);
    if (rc.is_error()) {
        if (rc.err_num() != eRECWONTFIT) return RC_AUGMENT(rc);

        // overflow treatment
        if (page.pid()!=root && !lvl_split[page.level()]) {
            W_DO ( _ov_treat(root, pl, key, page, lvl_split) );
        } else {
                W_DO( _split_page(root, pl, key, page, lvl_split) );
            split = true;
        }

            // insert the new tuple to parent 
        rc = page.insert(key, el, subtree.pid().page);
            if (rc.is_error()) {
            if (rc.err_num() != eRECWONTFIT)  return RC_AUGMENT(rc);
            w_assert1(! split);        
                W_DO( _split_page(root, pl, key, page, lvl_split) );
            W_DO( page.insert(key, el, subtree.pid().page) );
            split = true;
        }
    }

    if (page.pid() != root) {
        // propagate the changes upwards
        W_DO ( _propagate_insert(pl, false) );
    }

    // if (split) DO( lm->unlock(page.pid()) );

    return RCOK;
}

//
// propagate the insertion upwards: adjust the bounding boxes
//
rc_t
rtree_m::__propagate_insert(
    xct_t*                 /*xd*/,
    rtstk_t&        pl
)
{
    nbox_t child_bound(pl.top().page.dim());
    
    for (int i=pl.size()-1; i>0; i--) {
        // recalculate bound for current page for next iteration
        W_DO( ((rtree_p)pl.top().page).calc_bound(child_bound) );
        // W_DO( lm->unlock(pl.top().page.pid()) );
        pl.pop();

        // find the associated entry
        int2_t index = pl.top().idx;
        const rtrec_t& tuple = pl.top().page.rec(index);
        nbox_t old_bound(tuple.key(), tuple.klen());

        /*
         * if already contained, exit
         */
        // NB: everything contains Null, Null contains nothing except Null
        if(child_bound.is_Null())         break;
        if ( old_bound.is_Null() ) {
            old_bound = child_bound;
        } else {
            if (old_bound == child_bound) break;
            if (old_bound / child_bound) break;
            old_bound += child_bound;
        }

        // replace the parent entry with updated key
        vec_t el((const void*) tuple.elem(), tuple.elen());
        shpid_t child = tuple.child();
        W_DO( pl.top().page.remove(index) );
        W_DO( ((rtree_p)pl.top().page).insert(old_bound, el, child) );

    }
    return RCOK;
}

rc_t
rtree_m::_propagate_insert(
    rtstk_t&        pl,
    bool        compensate)
{
    lsn_t anchor;
    xct_t* xd = xct();
    w_assert3(xd);
    check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
    if (xd && compensate) {
        anchor = xd->anchor();
        X_DO(__propagate_insert(xd, pl), anchor);
        SSMTEST("rtree.1");
        xd->compensate(anchor,false/*not undoable*/ LOG_COMMENT_USE("rtree.1"));
    } else {
        W_DO(__propagate_insert(xd, pl));
    }
    
    return RCOK;
}

//
// propagate the deletion upwards: adjust the bounding boxes
//
rc_t
rtree_m::__propagate_remove(
    xct_t*        /*xd not used*/,
    rtstk_t&        pl
)
{
    nbox_t child_bound(pl.top().page.dim());

    for (int i=pl.size()-1; i>0; i--) {
        // recalculate bound for current page for next iteration
        W_DO( ((rtree_p)pl.top().page).calc_bound(child_bound) );
        //W_DO(lm->unlock(pl.top().page.pid()) );
        pl.pop();

        // find the associated entry
        int2_t index = pl.top().idx;
        const rtrec_t& tuple = pl.top().page.rec(index);
        nbox_t key(tuple.key(), tuple.klen());
        if (key==child_bound) { break; } // no more change needed

        // remove the old entry, insert a new one with updated key
        vec_t el((const void*) tuple.elem(), tuple.elen());
        shpid_t child = tuple.child();
        W_DO( pl.top().page.remove(index) );
        W_DO( ((rtree_p)pl.top().page).insert(child_bound, el, child));
    }
    return RCOK;
}

rc_t
rtree_m::_propagate_remove(
    rtstk_t&        pl,
    bool        compensate)
{
    lsn_t anchor;
    xct_t* xd = xct();
    w_assert3(xd);
    check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
    if (xd && compensate)  {
        anchor = xd->anchor();
        X_DO(__propagate_remove(xd,pl), anchor);
        SSMTEST("rtree.2");
        xd->compensate(anchor,false/*not undoable*/ LOG_COMMENT_USE("rtree.2"));
    } else {
        W_DO(__propagate_remove(xd,pl));
    }

    return RCOK;
}

//
// reinsert an entry at specified level
//
rc_t
rtree_m::_reinsert(
    const lpid_t&        root,
    const rtrec_t&        tuple,
    rtstk_t&                pl,
    int2_t                level,
    bool*                lvl_split)
{
    nbox_t key(tuple.key(), tuple.klen());
    vec_t el((const void*) tuple.elem(), tuple.elen());
    shpid_t child = tuple.child();
    bool split = false;

    W_DO( _pick_branch(root, key, pl, level, t_insert) );
        
    rtree_p page(pl.top().page);
    w_assert3(page.level()==level);

    rc_t rc = page.insert(key, el, child);
    if (rc.is_error()) {
        if (rc.err_num() != eRECWONTFIT) return RC_AUGMENT(rc);

        // overflow treatment
        split = false;
        if (page.pid()!=root && !lvl_split[level]) {
            W_DO ( _ov_treat(root, pl, key, page, lvl_split) );
        } else {
                W_DO( _split_page(root, pl, key, page, lvl_split) );
            split = true;
        }
        rc = page.insert(key, el, child);
        if (rc.is_error())  {
            if (rc.err_num() != eRECWONTFIT) return RC_AUGMENT(rc);
            w_assert1(! split);
                W_DO( _split_page(root, pl, key, page, lvl_split) );
            W_DO( page.insert(key, el, child) );
            split = true;
        }
    }  

    // propagate boundary change upwards
    if (!split && page.pid()!=root) {
        W_DO( _propagate_insert(pl, false) );
    }

    // if (split) DO ( lm->unlock(page.pid()) );

    return RCOK;
}

//
// create the rtree root
//
rc_t
rtree_m::create(
    stid_t        stid,
    lpid_t&        root,
    int2_t         dim)
{
    lsn_t anchor;
    xct_t* xd = xct();
    w_assert3(xd);
    check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
    anchor = xd->anchor();

    X_DO( io->alloc_a_page(stid, 
        lpid_t::eof, // hint
        root,         // npages, array for output pids
        true,         // may realloc
        NL,        // ignored
        true         // search file
        ), anchor );

    rtree_p page;
    X_DO( page.fix(root, LATCH_EX, page.t_virgin), anchor );

    rtctrl_t hdr;
    hdr.root = root;
    hdr.level = 1;
    hdr.dim = dim;
    X_DO( page.set_hdr(hdr), anchor );

    SSMTEST("rtree.3");
    xd->compensate(anchor,false/*not undoable*/ LOG_COMMENT_USE("rtree.3"));

    return RCOK;
}

//
// Search for exact match for key: only return the first matched
// This is called ONLY from find_md_assoc() and so the key.is_Null()
// suffices for determining if nulls are returned.  
//
rc_t
rtree_m::lookup(
    const lpid_t&        root,
    const nbox_t&        key,
    void*                el,
    smsize_t&                elen,
    bool&                found )
{
    vec_t elvec;
    rtstk_t pl;

    // do an exact match search
    W_DO ( _search(root, key, elvec, found, pl, t_read, key.is_Null()) );

    if (found) {
            const rtrec_t& tuple = pl.top().page.rec(pl.top().idx);
            if (elen < tuple.elen())  return RC(eRECWONTFIT);
        DBG(<<"tuple.elen==" <<tuple.elen());
            memcpy(el, tuple.elem(), elen = tuple.elen());
    } else {
        DBG(<<"Key not found: " << key );
    }

    return RCOK;
}

//
// insert a <key, elem> pair
//
rc_t
rtree_m::insert(
    const lpid_t&        root,
    const nbox_t&        key,
    const cvec_t&        elem)
{
    
    rtstk_t pl;
    bool found = false, split = false;

    // search for exact match first
    W_DO( _search(root, key, elem, found, pl, t_insert, key.is_Null()) );
    if (found)  {
        DBG(<<"duplicate, key=" << key);
        return RC(eDUPLICATE);
    }
        
    // pick appropriate branch
    pl.drop_all_but_last();
    W_DO( _pick_branch(root, key, pl, 1, t_insert) );
            
    rtree_p leaf(pl.top().page);
    w_assert3(leaf.is_leaf());

    rc_t rc;
    {
        /*
         * Turning off logging makes this a critical section:
         */
        xct_log_switch_t log_off(OFF);
        rc = leaf.insert(key, elem);
    }

    if (rc.is_error())  {
        if (rc.err_num() != eRECWONTFIT)
            return RC_AUGMENT(rc);

        lsn_t anchor;
        xct_t* xd = xct();
        w_assert3(xd);
        check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
        if(xd) anchor = xd->anchor();

        // overflow treatment
        bool lvl_split[rtstk_t::max_rtstk_sz];
        for (int i=0; i<rtstk_t::max_rtstk_sz; i++) lvl_split[i] = false;
        if (leaf.pid()!=root) {
            X_DO ( _ov_treat(root, pl, key, leaf, lvl_split), anchor );
        } else {
                X_DO( _split_page(root, pl, key, leaf, lvl_split), anchor );
            split = true;
        }

        {
            /*
             * Turning off logging makes this a critical section:
             */
            xct_log_switch_t log_off(OFF);
            rc = leaf.insert(key, elem);
        }
            if ( rc.is_error() ) {
            if (rc.err_num() != eRECWONTFIT)  return RC_AUGMENT(rc);
            w_assert1(! split);

                X_DO( _split_page(root, pl, key, leaf, lvl_split), anchor );

            split = true;
            {
                /*
                 * Turning off logging makes this a critical section:
                 */
                xct_log_switch_t log_off(OFF);
                rc = leaf.insert(key, elem);
                if ( rc.is_error() ) {
                    w_assert1(rc.err_num() != eRECWONTFIT);
                    xd->release_anchor(true LOG_COMMENT_USE("rtree1"));
                    return RC_AUGMENT(rc);
                }
            }
        }
        if (xd) {
            SSMTEST("rtree.4");
            xd->compensate(anchor,false/*not undoable*/ LOG_COMMENT_USE("rtree.4"));
        }
    }
        
    // propagate boundary change upwards
    if (!split && leaf.pid()!=root) {
        W_DO( _propagate_insert(pl) );
    }

    // log logical insert 
    W_DO( log_rtree_insert(leaf, 0, key, elem) );

    return RCOK;
}

//
// remove a <key, elem> pair
//
rc_t
rtree_m::remove(
    const lpid_t&        root,
    const nbox_t&        key,
    const cvec_t&        elem)
{
    rtstk_t pl;
    bool found = false;

    W_DO( _search(root, key, elem, found, pl, t_remove, key.is_Null()) );
    if (! found) 
        return RC(eNOTFOUND);
    
    rtree_p leaf(pl.top().page);
    slotid_t slot = pl.top().idx;
    w_assert3(leaf.is_leaf());

    {
        /*
         * Turning off logging makes this a critical section:
         */
        xct_log_switch_t log_off(OFF);
            W_DO( leaf.remove(slot) );
    }

//  W_DO( _propagate_remove(pl) );

    // log logical remove
    W_DO( log_rtree_remove(leaf, slot, key, elem) );

    return RCOK;
}

//
// print the rtree entries
//
rc_t
rtree_m::print(const lpid_t& root)
{
    rtree_p page;
    W_DO( page.fix(root, LATCH_SH) ) ;

    if (root.page == page.root()) {
        // print real root boundary
            nbox_t bound(page.dim());
            W_DO(page.calc_bound(bound));
            cout << "Universe:\n";
            bound.print(cout, 5);
    }
   
    int i;
    for (i = 0; i < 5 - page.level(); i++)  { cout << "\t"; }
    cout << "LEVEL " << page.level() << ", page " 
         << page.pid().page << ":\n";
    page.print();
    
    lpid_t sub_tree = page.pid();
    if (page.level() != 1)  {
        for (i = 0; i < page.nrecs(); i++) {
            sub_tree.page = page.rec(i).child();
            W_DO( print(sub_tree) );
        }
    }
    return RCOK;
}

#ifdef UNDEF
//
// print out leaf boundaries only
//
rc_t 
rtree_m::print(const lpid_t& root)
{
    rtree_p page;
    W_DO( page.fix(root, LATCH_SH) );

    if (page.level() == 1) {
        nbox_t bound(page.dim());
        W_DO(page.calc_bound(bound));
        cout << endl;
        cout << page.nrecs() << endl;
        cout << bound.bound(0) << " " << bound.bound(1) << endl;
        cout << bound.bound(0) << " " << bound.bound(3) << endl;
        cout << bound.bound(2) << " " << bound.bound(3) << endl;
        cout << bound.bound(2) << " " << bound.bound(1) << endl;
        cout << endl;
    } else {
        lpid_t sub_tree = page.pid();
        for (int i = 0; i < page.nrecs(); i++) {
            sub_tree.page = page.rec(i).child();
            W_DO( print(sub_tree) );
        }
    }
    return RCOK;
}
#endif

//
// draw rtree graphically in a gremlin format
//
rc_t
rtree_m::draw(
    const lpid_t&        root,
    ostream                &DrawFile,
    bool                skip)
{
    rtree_p page;
    W_DO( page.fix(root, LATCH_SH) );

    nbox_t rbound(page.dim());
    W_DO(page.calc_bound(rbound));

    nbox_t        CoverAll = rbound;

    W_FORM(DrawFile)("sungremlinfile\n");
    W_FORM(DrawFile)("0 0.00 0.00\n");

    CoverAll.draw(page.level()+1, DrawFile, CoverAll);
    W_DO ( _draw(root, DrawFile, CoverAll, skip) );

    W_FORM(DrawFile)("-1");

    return RCOK;
}

rc_t
rtree_m::_draw(
    const lpid_t&        pid,
    ostream                &DrawFile,
    nbox_t                &CoverAll,
    bool                skip)
{
    rtree_p page;
    W_DO( page.fix(pid, LATCH_SH) );

    if (!skip || page.is_leaf()) page.draw(DrawFile, CoverAll);

    lpid_t sub_tree = page.pid();
    if (page.level() != 1)  {
        for (int i = 0; i < page.nrecs(); i++) {
            sub_tree.page = page.rec(i).child();
            W_DO( _draw(sub_tree, DrawFile, CoverAll, skip) );
        }
    }

    return RCOK;
}

//
// collect rtree statistics: including # of entries, # of
// leaf/non-leaf pages, fill factor, overlapping degree.
//
rc_t
rtree_m::stats(
    const lpid_t&        root,
    rtree_stats_t&        stat,
    uint2_t                size,
    uint2_t*                ovp,
    bool                audit)
{
    rtree_p page;
    W_DO( page.fix(root, LATCH_SH) );
    base_stat_t ovp_sum = 0, fill_sum = 0;
    base_stat_t num_pages_store_scan = 0;

    stat.clear();

    {
        // calculate number of pages alloc/unalloc in the rtree  
        // by scanning the store's list of pages
        lpid_t pid;
        bool allocated;
        rc_t   rc;
        rc = io->first_page(root.stid(), pid, &allocated);
        while (!rc.is_error()) {
            if (allocated) {
                num_pages_store_scan++;
            } else {
                stat.unalloc_pg_cnt++;
            }
            rc = io->next_page(pid, &allocated);
        }
        w_assert3(rc.is_error());
        if (rc.err_num() != eEOF) return rc;
    }

    stat.level_cnt = page.level();

    if (size>0 && ovp) {
        ovp[0] = page.ovp();
    }

    lpid_t sub_tree = page.pid();
    if (page.level() != 1)  {
        for (int i = 0; i < page.nrecs(); i++) {
            sub_tree.page = page.rec(i).child();
            W_DO( _stats(sub_tree, stat, fill_sum, size, ovp) );
            if (size>0 && ovp) {
                if (stat.level_cnt + 1 - page.level() < size) {
                    ovp_sum += ovp[stat.level_cnt - page.level() + 1];
                }
            }
        }
        if (size>0 && ovp) {
            if (stat.level_cnt + 1 - page.level() < size) {
                ovp_sum /= page.nrecs();
            }
        }
            stat.int_pg_cnt++;
        stat.fill_percent = (uint2_t) (fill_sum/stat.leaf_pg_cnt); 
    } else {
        stat.leaf_pg_cnt = 1;
        stat.fill_percent = (page.used_space()*100/rtree_p::data_sz); 
        stat.entry_cnt = page.nrecs();
    }

    if (audit && num_pages_store_scan != (stat.leaf_pg_cnt+stat.int_pg_cnt)) {
        // audit failed
        return RC(fcINTERNAL);
    }

    stat.unique_cnt = stat.entry_cnt;

    return RCOK;
}

rc_t
rtree_m::_stats(
    const lpid_t&        root,
    rtree_stats_t&        stat,
    base_stat_t&        fill_sum,
    uint2_t                size,
    uint2_t*                ovp)
{
    rtree_p page;
    W_DO( page.fix(root, LATCH_SH) );
    uint    ovp_sum = 0;
    
    if (ovp && (stat.level_cnt-page.level()<size)) {
        ovp[stat.level_cnt - page.level()] = page.ovp();
    }

    lpid_t sub_tree = page.pid();
    if (page.level() != 1)  {
        for (int i = 0; i < page.nrecs(); i++) {
            sub_tree.page = page.rec(i).child();
            W_DO( _stats(sub_tree, stat, fill_sum, size, ovp) );
            if (ovp && (stat.level_cnt+1-page.level() < size)) {
                ovp_sum += ovp[stat.level_cnt - page.level() + 1];
            }
        }
        if (ovp && (stat.level_cnt+1-page.level()<size)) {
            ovp_sum /= page.nrecs();
        }
            stat.int_pg_cnt++;
    } else {
        stat.leaf_pg_cnt += 1;
        fill_sum +=  ((uint4_t)page.used_space()*100/rtree_p::data_sz);
        stat.entry_cnt += page.nrecs();
    }

    return RCOK;
}
    

//
// initialize the fetch
//
rc_t
rtree_m::fetch_init(
    const lpid_t&        root,
    rt_cursor_t&        cursor)
{
    bool found = false;
    lpid_t pid = root;
    
    // push root page on the fetch stack
    if (! cursor.fl.is_empty() ) { cursor.fl.empty_all(); }
    cursor.fl.push(pid.page);

    //W_DO(lm->lock(root, SH, t_medium, WAIT_FOREVER));

    DBG(<<"");
    rc_t rc = _dfs_search(root, cursor.qbox, found, 
        cursor.cond, cursor.fl, cursor._include_nulls);
    if (rc.is_error()) {
        cursor.fl.empty_all(); 
        return RC_AUGMENT(rc);
    }

    if (!found) { return RCOK; }

    cursor.num_slot = 0;
    pid.page = cursor.fl.pop();
    W_DO( cursor.page.fix(pid, LATCH_SH) );

    w_assert3(cursor.page.is_leaf());
    DBG(<<"");
    // TODO: this can mean a duplicate search
    found = cursor.page.search(cursor.qbox, cursor.cond, 
                                    cursor.smap, cursor.num_slot,
                                    cursor._include_nulls);
    w_assert3(found);
    cursor.idx = bm_first_set(cursor.smap, cursor.page.nrecs(), 0);
    DBG(<<"leave fetch_init w/ cursor.num_slot=" << cursor.num_slot
        << " cursor.idx=" << cursor.idx);

    return RCOK;
}

//
// fetch next qualified entry
//
rc_t
rtree_m::fetch(
    rt_cursor_t&        cursor,
    nbox_t&                key,
    void*                el,
    smsize_t&                elen,
    bool&                eof,
    bool                skip)
{
    DBG(<<"enter fetch w/ cursor.num_slot=" << cursor.num_slot
            <<" cursor.idx=" << cursor.idx);
    if ((eof = !cursor.page.is_fixed()))  { return RCOK; }

    // get the key and elem
    const rtrec_t& r = cursor.page.rec(cursor.idx);
    key.bytes2box(r.key(), r.klen());
    if (elen >= r.elen())  {
        memcpy(el, r.elem(), r.elen());
        elen = r.elen();
    } 
    else if (elen == 0)  { ; }
    else { return RC(eRECWONTFIT); }

    // advance the pointer
    bool found = false;
    lpid_t root;
    cursor.page.root(root);
    lpid_t pid = cursor.page.pid();

    //        move cursor to the next eligible unit based on 'condition'
    DBG(<<"skip=" << skip << " cursor.idx=" << cursor.idx);
    if (skip && ++cursor.idx < cursor.page.nrecs()) {
        cursor.idx = bm_first_set(cursor.smap, cursor.page.nrecs(),
                                        cursor.idx);
        
        DBG(<<"cursor.idx=" << cursor.idx);
    }
    if (cursor.idx == -1 || cursor.idx >= cursor.page.nrecs())  {
        // W_DO( lm->unlock(cursor.page.pid()) );
        cursor.page.unfix();

        found = false;
        while (!found && !cursor.fl.is_empty()) {
            DBG(<<"doing _dfs_search");
            rc_t rc = _dfs_search(root, cursor.qbox, found, 
                                  cursor.cond, cursor.fl,
                                  cursor._include_nulls);
            if (rc.is_error()) {
                cursor.fl.empty_all();
                return RC_AUGMENT(rc);
            }
        }
        if (!found) {
            return RCOK; 
        } else {
                pid.page = cursor.fl.pop();
                W_DO( cursor.page.fix(pid, LATCH_SH) );
                w_assert3(cursor.page.is_leaf());
            DBG(<<"cursor.num_slot=" << cursor.num_slot);
                found = cursor.page.search(cursor.qbox, cursor.cond, 
                                           cursor.smap, cursor.num_slot,
                                           cursor._include_nulls);
            DBG(<<"cursor.num_slot=" << cursor.num_slot);
            w_assert3(found);
            cursor.idx = bm_first_set(cursor.smap, cursor.page.nrecs(), 0);
            DBG(<<"cursor.idx=" << cursor.idx);
        }
    }
    DBG(<<"leave fetch w/ cursor.num_slot=" << cursor.num_slot
            <<" cursor.idx=" << cursor.idx);
        
    return RCOK;
}

//
// optimal split along one axis: sweep along one axis to find the split line.
//
static void 
sweep_n_split(int axis, wrk_branch_t work[], u_char smap[], int& margin,
                    int max_num, int min_num, nbox_t* extra)
{
    int i,j;
    margin = 0;
    if (min_num == 0) min_num = (int) (MIN_RATIO*max_num + 0.5);
    int split = -1, diff = max_num - min_num;

    // sort along the specified axis
    quick_sort(work, max_num, axis);

    bm_zero(smap, max_num);
    bm_set(smap, work[0].idx);

    nbox_t box1(work[0].rect);
    for (i=1; i<min_num; i++) {
        box1 += work[i].rect;
        bm_set(smap, work[i].idx);
    }

    nbox_t box2_base(work[max_num-1].rect);
    for (i=diff+1; i<max_num-1; i++)
        box2_base += work[i].rect;

    split = min_num;
    double overlap;
    double bound_area = 0;
    double min_ovp = MaxDouble, min_area = MaxDouble;

    // calculate margin and overlap for each distribution
    for (i = min_num; i <= max_num-min_num; i++) {
        nbox_t box2(box2_base);
        for (j=i; j<=diff; j++)
            box2 += work[j].rect;

        margin += (box1.margin()+box2.margin());
        overlap = (box1^box2).area();
        if (extra) {
            overlap += (box1^(*extra)).area();
            overlap += (box2^(*extra)).area();
        }

        if (overlap < min_ovp) {
            min_ovp = overlap;
            min_area = (box1+box2).area();
            split = i;
        } else if (overlap == min_ovp && 
                 (bound_area = (box1+box2).area()) < min_area) {
            min_area = bound_area;
            split = i;
        }

        box1 += work[i].rect;
    }

    for (i = min_num; i < split; i++)
        bm_set(smap, work[i].idx);
}


void 
rtld_cache_t::init_buf(int2_t lvl) 
{
    buf[0].init(lvl);
    buf[1].init(lvl);
    buf[2].init(lvl);
}

//
// force one page out of the load cache: repacking all cached
// pages and flushing the first one.
//
rc_t
rtld_cache_t::force(
    rtwork_p&        ret_p,
    bool&        out,
    nbox_t*        universe)
{
    int i,j;
    out = false;

    if (_idx==0) return RCOK;

    // count the size of all entries
    uint2_t size = 0, cnt = 0;
    for (i=0; i<=1; i++) {
        size += (uint2_t) buf[i].rp()->used_space();
        cnt += buf[i].rp()->nrecs();
    }

    // all tuples fits on one page, compress them to one page
    if (size < rtree_p::data_sz) {
        for (i=1; i>0; i--) {
            for (j=buf[i].rp()->nrecs()-1; j>=0; j--)  {
                W_DO( buf[0].rp()->insert(buf[i].rp()->rec(j)) );
                W_DO( buf[1].rp()->remove(j) );
            }
        }

        W_DO( buf[0].calc_set_bound() );
        W_DO( buf[1].calc_set_bound() );

        if (_idx==2) { buf[1].swap(buf[2]); }
        _idx--;
        return RCOK;
    }

    ret_p.init(buf[0].rp()->level());

    // check if split is necessary
    if ((buf[0].bound()^buf[1].bound()).area()/buf[0].bound().area()
        < 0.01 || universe==NULL) {
        // no need to apply split algorithm
        ret_p.swap(buf[0]); 
        buf[0].swap(buf[1]);
        if (_idx==2) buf[1].swap(buf[2]);
        _idx--;
        out = true;
        if (!last_box) {
            last_box = new nbox_t(ret_p.bound());
            w_assert1(last_box);
        } else {
            *last_box = ret_p.bound();
        }
        return RCOK;
    }
        

    // load up work space
    wrk_branch_t* work = new wrk_branch_t[cnt];
    if (!work) return RC(smlevel_0::eOUTOFMEMORY);
    w_auto_delete_array_t<wrk_branch_t> auto_del_work(work);

    int cnt1 = buf[0].rp()->nrecs(), index = 0;
    for (i=0; i<=1; i++) {
        for (j=0; j<buf[i].rp()->nrecs(); j++) {
            const rtrec_t& tuple = buf[i].rp()->rec(j);
            work[index].rect.bytes2box(tuple.key(), tuple.klen());
            work[index].idx = index;
            index++;
        }
    }

    int margin;
    u_char *smap = new u_char[rtree_p::smap_sz*2];
    w_auto_delete_array_t<u_char> auto_del(smap);

    int min_cnt = cnt - rtree_p::data_sz*buf[0].rp()->nrecs()
                        / buf[0].rp()->used_space();
    min_cnt = (min_cnt + cnt/2) / 2;

    //decide which axis to split
    int x0 = buf[0].bound().center(0);
    int x1 = buf[1].bound().center(0);
    int y0 = buf[0].bound().center(1);
    int y1 = buf[1].bound().center(1);
    bool bounce_x = false, bounce_y = false;

    if (_idx == 2) {
        int x2 = buf[2].bound().center(0);
            int y2 = buf[2].bound().center(1);

        if ((x1-x2)*(x1-x0) > 0) bounce_x = true;
        if ((y1-y2)*(y1-y0) > 0) bounce_y = true;
    }

    // split on the desired axis
    if (bounce_x && bounce_y) 
        if (ABS(x1-x0) >= ABS(y1-y0)) 
            sweep_n_split(1, work, smap, margin, cnt, min_cnt, last_box);
        else 
            sweep_n_split(0, work, smap, margin, cnt, min_cnt, last_box);
    else if (bounce_x)
        sweep_n_split(1, work, smap, margin, cnt, min_cnt, last_box);
    else if (bounce_y)
        sweep_n_split(0, work, smap, margin, cnt, min_cnt, last_box);
    else {
        if (ABS(x1-x0) >= ABS(y1-y0)) 
            sweep_n_split(0, work, smap, margin, cnt, min_cnt, last_box);
        else 
            sweep_n_split(1, work, smap, margin, cnt, min_cnt, last_box);
    }

    // shift tuples from buf[0] and buf[1] to the out page
    for (i=cnt-1; i>=0; i--) {
        if (bm_is_set(smap, i)) {
            int idx = (i>=cnt1)? 1: 0;
            int offset = (i>=cnt1)? i - cnt1: i;
            W_DO( ret_p.rp()->insert(buf[idx].rp()->rec(offset)) );
            W_DO( buf[idx].rp()->remove(offset) );
        }
    }
    
    // shift rest from buf[1] to buf[0]
    for (i=buf[1].rp()->nrecs()-1; i>=0; i--) {
        W_DO( buf[0].rp()->insert(buf[1].rp()->rec(i)) );
        W_DO( buf[1].rp()->remove(i) );
    }

    W_DO( buf[0].calc_set_bound() );
    W_DO( buf[1].calc_set_bound() );
    W_DO( ret_p.calc_set_bound() );
    
    // decide wich one goes out first
    if (_idx == 2) {
        if (universe) {
                if (ret_p.bound().hcmp(buf[0].bound(), *universe) > 0)
                ret_p.swap(buf[0]); 
        } else {
            if ((ret_p.bound()^buf[2].bound()).area()
                     > (buf[0].bound()^buf[2].bound()).area())
                ret_p.swap(buf[0]); 
        }
            buf[1].swap(buf[2]);
    }
        
    _idx--;
    out = true;
    if (!last_box) {
        last_box = new nbox_t(ret_p.bound());
    } else {
        *last_box = ret_p.bound();
    }

    return RCOK;
}

//
// shift tuples from temporal work page to persistent page
// an image log is generated.
//
rc_t
rtld_stk_t::tmp2real(
    rtwork_p*        tmp,
    rtree_p*        real)
{
    {
            xct_log_switch_t toggle(smlevel_0::OFF);

            // shift all tuples in tmp to real
            for (int i=0; i<tmp->rp()->nrecs(); i++) {
            W_DO( real->insert(tmp->rp()->rec(i)) );
            }
    }

    // generate an image log
    {
            xct_log_switch_t toggle(smlevel_0::ON);
            W_DO( log_page_image(*real) );
    }
    return RCOK;
}

//
// heuristic table for fill factor and expansion
//
static const int _h_size = 20;        // expansion factors for 20 fills
static int expn_table[_h_size] = 
{  1636, 1124, 868, 612, 484, 
    356, 292, 228, 196, 164,
    148, 132, 124, 116, 108,
    104, 102, 101, 101, 100
};

//
// determine when to terminate inserting to the current page based
// on fill and expansion factor: if the page reached certain fill,
// and the expansion factor is over certain threshold, then stop
// insertion to the current page.
//
static bool
heuristic_cut(
    rtwork_p*                page,
    const nbox_t&        key)
{
    int offset = (page->rp()->used_space()*20/rtree_p::data_sz);
    if (offset <= 4) return false;
    nbox_t  consider(page->bound()); // copy
    if(!key.is_Null()) {
        consider += key;
    }
    if (consider.area() >
             page->bound().area()*expn_table[offset-1]/100.0)
        return true;
    else 
        return false;
}
    

//
// add an entry to the load stack:
//        If enough space in the current cache page, insert and exit.
//        Otherwise, force one cache page to disk, record the change
//        at the higher level recursively and then insert the new entry.
//
rc_t
rtld_stk_t::add_to_stk(
    const nbox_t&        key,
    const cvec_t&        el,
    shpid_t                child,
    int2_t                level)
{
    w_assert1(level<=_top+1);
    if (level == _top+1) { init_next_layer(); }

    // cache page at current top layer
    rtwork_p* page = layers[level].top();
    
    if (dc.h != 0) {
        // check heuristics 
        if (heuristic_cut(page, key)) { 
            // fill factor and expansion factor reached thresholds,
            // skip to perform the force and the insert.
        } else {
            rc_t rc = page->rp()->insert(key, el, child);
            if (rc.is_error()) {
                // if page full, go to perform the force and the insert. 
                if (rc.err_num() != smlevel_0::eRECWONTFIT) {
                    return RC_AUGMENT(rc);
                }
            } else { 
            // insert successful, record bounding box change on cache page
                page->expn_bound(key);
                return RCOK; 
            }
        }
    } else {
        rc_t rc = page->rp()->insert(key, el, child);
        if (rc.is_error()) {
            // if page full, go to perform the force and the insert. 
            if (rc.err_num() != smlevel_0::eRECWONTFIT) {
                return RC_AUGMENT(rc);
            }
        } else { 
            // insert successful, record bounding box change on cache page
            page->expn_bound(key);
            return RCOK;
        }
    }

    if (layers[level].count()==3) {
        // cache full, force one cache page out to disk
        rtwork_p out_page;
        bool out=false;

        W_DO( layers[level].force(out_page, out, dc.universe) );
        if (out) {
            lpid_t npid;
            vec_t e;

            // write one cache page to disk page
            W_DO( rtree_m::_alloc_page(rp.pid(), level+1,
                                       rp, rp.dim(), npid) );


            rtree_p np;
            W_DO( np.fix(npid, LATCH_EX) );
            W_DO( tmp2real(&out_page, &np) );

            num_pages++;
            if (level==0) { 
                leaf_pages++;
                fill_sum += (uint4_t) (np.used_space()*100/rtree_p::data_sz);
            }

            // insert to the higher level
            W_DO( add_to_stk(out_page.bound(), e, npid.page, level+1) );

        }
    }

    // get the next empty cache page and insert the new entry
    layers[level].incr_cnt();
    W_DO( layers[level].top()->rp()->insert(key, el, child) );
    layers[level].top()->expn_bound(key);

    return RCOK;
}

//
// forcing out all remaining pages in the load cache at the end
//
rc_t
rtld_stk_t::wrap_up()
{
    rtwork_p out_page;
    bool out;
    lpid_t npid;        
    rtree_p np;
    vec_t e;
    
    // examine all non-root layers
    for (int i=0; i<_top; i++) {
        do {
            out = false;
            while (!out && layers[i].count() > 1) {
                W_DO( layers[i].force(out_page, out, dc.universe) );
            }
            if (out) {
                    // write each cache page to disk page
                    W_DO( rtree_m::_alloc_page(rp.pid(), i+1, 
                                        rp, rp.dim(), npid) );
                    W_DO( np.fix(npid, LATCH_EX) );
                    W_DO( tmp2real(&out_page, &np) );
            
                    num_pages++;
                    if (i==0) {
                    leaf_pages++;
                    fill_sum += (uint4_t)(np.used_space()*100/rtree_p::data_sz);
                }

                    // insert to the higher level
                    W_DO( add_to_stk(out_page.bound(), e, npid.page, i+1) );

            }
        } while (layers[i].count() > 1);

        // process the final cache page in the layer
        W_DO(rtree_m::_alloc_page(rp.pid(), i+1, rp, rp.dim(), npid));
        W_DO( np.fix(npid, LATCH_EX) );
        W_DO( tmp2real(layers[i].bottom(), &np) );
            
        num_pages++;
        if (i==0) { 
            leaf_pages++;
            fill_sum += (uint4_t) (np.used_space()*100/rtree_p::data_sz);
        }
        
        // insert to the higher level
        W_DO( add_to_stk(layers[i].bottom()->bound(), e, npid.page, i+1) );
    }

    // now deal with the root layer
    out = false;
    while (!out && layers[_top].count() > 1) {
        W_DO( layers[_top].force(out_page, out, dc.universe) );
    }

    if (out) {
        // more than one cache pages: 
        //        means real root is one layer above
        num_pages++;
        W_DO( rp.set_level(_top+2) );
        rtree_p np;

        do {
            // write each cache page to disk page
            W_DO( rtree_m::_alloc_page(rp.pid(),_top+1,rp,rp.dim(),npid) );
            W_DO( np.fix(npid, LATCH_EX) );
            W_DO( tmp2real(&out_page, &np) );
                    
            num_pages++;
            if (_top==0) {
                leaf_pages++;
                    fill_sum += (uint4_t) (np.used_space()*100/rtree_p::data_sz);
            }

            W_COERCE( rp.insert(out_page.bound(), e, np.pid().page) );

            out = false;
            while (!out && layers[_top].count() > 1) {
                W_DO( layers[_top].force(out_page, out, dc.universe) );
            }
        } while (out);

        // process the final cache page
        W_DO( rtree_m::_alloc_page(rp.pid(), _top+1, rp, rp.dim(), npid) );
        W_DO( np.fix(npid, LATCH_EX) );
        W_DO( tmp2real(layers[_top].bottom(), &np) );
        W_COERCE( rp.insert(layers[_top].bottom()->bound(),
                            e, np.pid().page) );
        num_pages++;
        if (_top==0) {
            leaf_pages++;
            fill_sum += (uint4_t) (np.used_space()*100/rtree_p::data_sz);
        }
        height = _top+2;

    } else {

        // directly copy to root page
        W_DO( rp.set_level(_top+1) );
        W_DO( tmp2real(layers[_top].bottom(), &rp) );
        num_pages++;
        if (_top==0) {
            leaf_pages++;
            fill_sum += (uint4_t) (rp.used_space()*100/rtree_p::data_sz);
        }
        height = _top+1;
    }

    return RCOK;
}

#undef SM_LEVEL
#include <sm_int_4.h>
//
// bulk load: the src file should be already sorted in spatial order.
//
rc_t
rtree_m::bulk_load(
    const lpid_t&         root,        // I- root of rtree
    int                         nsrcs,         // I- store containing new records
    const stid_t*         src,         // I- store containing new records
    const rtld_desc_t&        desc,        // I- load descriptor
    rtree_stats_t&         stats)        // O- index stats
{
    DBG(<<"bulk_load source=" << src << " index=" << root
        );
    stats.clear();
    if (!is_empty(root)) {
         return RC(eNDXNOTEMPTY);
    }

    lsn_t anchor;
    xct_t* xd = xct();
    w_assert3(xd);
    check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
    if (xd) anchor = xd->anchor();

    lpid_t pid;
    rtree_p rp;
    X_DO( rp.fix(root, LATCH_EX), anchor );
    rtld_stk_t ld_stack(rp, desc);

    // W_DO( io->set_store_flags(root.stid(), smlevel_0::st_insert_file) );

    nbox_t key(rp.dim());
    int klen = key.klen();

    /*
     *  go thru the file page by page
     */
    int i = 0;              // toggle
    file_p page[2];         // page[i] is current page
    const record_t* pr = 0; // previous record
    base_stat_t cnt=0, uni_cnt=0;

    for(int src_index=0; src_index<nsrcs; src_index++) {
        X_DO( fi->first_page(src[src_index], pid, NULL/*allocated only*/), anchor );
        for (bool eof = false; ! eof; ) {
            X_DO( page[i].fix(pid, LATCH_SH), anchor );
            for (slotid_t s = page[i].next_slot(0); s; s = page[i].next_slot(s)) {
                const record_t* r;
                W_COERCE( page[i].get_rec(s, r) );

                // key.bytes2box(r->hdr(), klen); // if klen==0 dim will be 0
                w_assert3(r->hdr_size() <= (smsize_t)klen);
                key.bytes2box(r->hdr(), r->hdr_size()); // if klen==0 dim will be 0
                vec_t el(r->body(), (int)r->body_size());

                ++cnt;
                if (!pr) ++ uni_cnt;
                else {
                    // check unique
                    if(r->hdr_size() == 0 &&  pr->hdr_size() == 0) {
                        /* All rtrees handle duplicate keys, but
                         * not dup key/elems
                         */
                        w_assert1(pr->is_small());
                        if (el.cmp(pr->body(), (int)pr->body_size())==0) {
                            // duplicate null entries
                            X_DO(RC(eDUPLICATE), anchor);
                        }
                    } else if (memcmp(pr->hdr(), r->hdr(), klen))  {
                        ++ uni_cnt;
                    }
                }

                X_DO( ld_stack.add_to_stk(key, el, 0, 0), anchor );
                pr = r;
            }
            i = 1 - i;
            X_DO( fi->next_page(pid, eof, NULL/*allocated only*/), anchor );
        }
    }
    if (cnt > 0)
    {
        X_DO( ld_stack.wrap_up(), anchor );
    }

    if (xd)  {
        SSMTEST("rtree.5");
        xd->compensate(anchor,false/*not undoable*/ LOG_COMMENT_USE("rtree.5"));
    }

    stats.entry_cnt = cnt;
    stats.unique_cnt = uni_cnt;
    stats.level_cnt = ld_stack.height;
    stats.leaf_pg_cnt = ld_stack.leaf_pages;
    stats.int_pg_cnt = ld_stack.num_pages - stats.leaf_pg_cnt;
    if(ld_stack.leaf_pages == 0) {
        stats.fill_percent = 0; // no leaves -- less than 1 page
    } else {
        stats.fill_percent = (uint2_t) (ld_stack.fill_sum/ld_stack.leaf_pages);
    }

    return RCOK;
}

//
// bulk load: the stream should be already sorted in spatial order
//
rc_t
rtree_m::bulk_load(
    const lpid_t& root,                // I- root of rtree
    sort_stream_i& sorted_stream,// IO - sorted stream
    const rtld_desc_t& desc,        // I- load descriptor
    rtree_stats_t& stats)        // O- index stats
{
    memset(&stats, 0, sizeof(stats));
    if (!is_empty(root)) {
         return RC(eNDXNOTEMPTY);
    }

    /*
     *  go thru the sorted stream
     */
    uint4_t cnt=0, uni_cnt=0;

    {
            rtree_p rp;
        W_DO( rp.fix(root, LATCH_EX) );
            rtld_stk_t ld_stack(rp, desc);

        // W_DO( io->set_store_flags(root.stid(), smlevel_0::st_insert_file) );

            nbox_t box(rp.dim());
            int klen = box.klen();

            char* tmp = new char[klen];
        w_auto_delete_array_t<char> auto_del_tmp(tmp);

            char* prev_tmp = new char[klen];
        w_auto_delete_array_t<char> auto_del_prev_tmp(prev_tmp);

        bool prev = false;
        bool eof = false;
        vec_t key, el;
        W_DO ( sorted_stream.get_next(key, el, eof) );

        while (!eof) {
            ++cnt;

            key.copy_to(tmp, klen);
            box.bytes2box(tmp, klen);

            if (!prev) {
                prev = true;
                memcpy(prev_tmp, tmp, klen);
                uni_cnt++;
            } else {
                if (memcmp(prev_tmp, tmp, klen)) {
                    uni_cnt++;
                    memcpy(prev_tmp, tmp, klen);
                }
            }

            W_DO( ld_stack.add_to_stk(box, el, 0, 0) );
            key.reset();
            el.reset();
            W_DO ( sorted_stream.get_next(key, el, eof) );
        }
    
        if (cnt > 0)
        {
            W_DO( ld_stack.wrap_up() );
        }

        stats.level_cnt = ld_stack.height;
        stats.leaf_pg_cnt = ld_stack.leaf_pages;
        stats.int_pg_cnt = ld_stack.num_pages - stats.leaf_pg_cnt;
        stats.fill_percent = (uint2_t) (ld_stack.fill_sum / ld_stack.leaf_pages);
    }

    stats.entry_cnt = cnt;
    stats.unique_cnt = uni_cnt;
    return RCOK;
}

