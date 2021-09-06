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

 $Id: newsort.cpp,v 1.47 2010/06/15 17:30:07 nhall Exp $

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
#define NEWSORT_C

#include "sm_int_4.h"
#include "lgrec.h"
#include "sm.h"
#include "pin.h"
#include "prologue.h"

#include <w_heap.h>
#include <umemcmp.h>
#include <new>

#ifdef USE_PURIFY
#include <purify.h>
#endif

typedef ssm_sort::sort_keys_t sort_keys_t;
typedef ssm_sort::key_info_t key_info_t;
typedef ssm_sort::key_cookie_t key_cookie_t;
typedef ssm_sort::factory_t factory_t;
typedef ssm_sort::skey_t skey_t;
typedef ssm_sort::object_t object_t;
typedef ssm_sort::CF CF;
typedef ssm_sort::MOF MOF;
typedef ssm_sort::UMOF UMOF;
typedef ssm_sort::CSKF CSKF;
typedef ssm_sort::run_mgr run_mgr;

// static
key_cookie_t key_cookie_t::null(0);

// For debugging
extern "C" void sortstophere()
{
    assert(0);
}


/* XXX only used to verify proper alignment when strict alignment is
   not enabled.   This should all be rectified, but the strict alignment
   had different meaning previously. */

/* XXX too many ia32 systems only give you x4 alignment on x8 objects
   need to look into this further.   Our alignment model may need
   more tweaking.  And it should take into account machines were we
   can run 4 byte aligned for 8 byte objects and it works too.  Arggh. */

#define STRICT_INT8_ALIGNMENT
#define STRICT_F8_ALIGNMENT

#if defined(I386) 
#define        _ALIGN_F8        0x4
#define        _ALIGN_IU8        0x4
#else
#define        _ALIGN_F8        0x8
#define        _ALIGN_IU8        0x8
#endif

#define        ALIGN_MASK_F8        (_ALIGN_F8-1)
#define        ALIGN_MASK_IU8        (_ALIGN_IU8-1)


const int max_keys_handled = 5; // TODO: make more flexible

class tape_t;

typedef Heap<tape_t *, run_mgr> RunHeap;
#ifdef EXPLICIT_TEMPLATE
template class Heap<tape_t *, run_mgr>;
template class w_auto_delete_t<RunHeap>;
#endif

class no_factory_t : public factory_t {
public:
   void freefunc(const void *, smsize_t ) { }
   void* allocfunc(smsize_t) { 
       W_FATAL_MSG(fcINTERNAL, << "allocfunc"); return 0; }
} _none;
factory_t* factory_t::none = &_none;

class cpp_char_factory_t : public factory_t {
   static int _nallocs;
   static int _nfrees;
public:
   NORET  cpp_char_factory_t() { _nallocs=0; _nfrees=0; }
   NORET  ~cpp_char_factory_t() { }
   void* allocfunc(smsize_t l) { 
        smsize_t   size_in_dbl = l / sizeof(double);
        if(l - size_in_dbl > 0) {
            size_in_dbl++;
        }
        void *p =  new double[size_in_dbl];
        if(!p) W_FATAL(fcOUTOFMEMORY);
        DBG(<<"cpp_char_factory_t.allocfunc(sz=" << l << ") new " << p );
        _nallocs++;
        return (void *)p;
   }
   void freefunc(const void *p, smsize_t 
#ifdef W_TRACE
   t
#endif
   ) { 
        _nfrees++;
        DBG(<<"cpp_char_factory_t.freefunc(sz=" << t << ") delete " << p);
        double *d = (double *)p;
        delete[] d;
   }
} _cpp_vector;
factory_t* factory_t::cpp_vector = &_cpp_vector;
int cpp_char_factory_t::_nallocs =0;
int cpp_char_factory_t::_nfrees =0;

#ifdef INSTRUMENT_SORT

#define INC_STAT_SORT(x)       INC_TSTAT(x)
#define ADD_TSTAT_SORT(x, y)    ADD_TSTAT(x, y)
#define GET_TSTAT_SORT(x)       GET_TSTAT(x)
#define SET_TSTAT_SORT(x, y)    SET_TSTAT(x, y)

#else

#define INC_STAT_SORT(x)
#define ADD_TSTAT_SORT(x, y)
#define GET_TSTAT_SORT(x)       0
#define SET_TSTAT_SORT(x, y)

#endif /* INSTRUMENT_SORT */

#ifdef INSTRUMENT_SORT

inline void
record_malloc(smsize_t amt)
{
    unsigned a = (unsigned )(amt);
    INC_STAT_SORT(sort_mallocs); 
    ADD_TSTAT_SORT(sort_malloc_bytes, a); 
    ADD_TSTAT_SORT(sort_malloc_curr, a); 
    base_stat_t m = GET_TSTAT_SORT(sort_malloc_max);
    if(m < a) { SET_TSTAT_SORT(sort_malloc_max, a);}

    // scratch_used is the highwater mark
    unsigned c = GET_TSTAT_SORT(sort_malloc_curr);
    m = GET_TSTAT_SORT(sort_malloc_hiwat);
    if(c > m) {
        // m = c
        SET_TSTAT_SORT(sort_malloc_hiwat, c);
    }
}

inline void 
record_free(smsize_t amt) 
{
    // scratch_used is the highwater mark
    unsigned c = GET_TSTAT_SORT(sort_malloc_curr);
    c -= (unsigned )(amt);
    SET_TSTAT_SORT(sort_malloc_curr, c);
}
#else

inline void 
record_malloc(smsize_t /* amt */) 
{
}

inline void 
record_free(smsize_t /* amt */ ) 
{
}

#endif /* INSTRUMENT_SORT */

class limited_space : public smlevel_top, public factory_t 
{
    // This might seem a bit of a misnomer, but
    // classes derived from this do two things:
    // They account for their memory use, and
    // they have the means to guarantee not to
    // use more than some given amount of memory.
    //
    // run_mgr::_prepare_key is the starting point for
    // factories: it choose to use either the run_mgr's limited_space
    // or to use cpp_vector if it's clear that the limited_space isn't
    // usable under the circumstances (it rand out of room, or
    // the key is bigger than a page, for example).

    // The memory chunks allocated here are 8-byte aligned
private:
    double *        _d_scratch;     // 8-byte-aligned scratch buffer
    char *          _scratch;       // ptr to unused portion of scratch buffer
    char *          _reset_point;   // between sort phases, we reset to here
    smsize_t        _left;          // how much left in the scratch buffer
    smsize_t        _buffer_sz;     // original size of scratch buffer in bytes
    smsize_t        _buf_hiwat;     // original size of scratch buffer in bytes

    void _create_buf();

public:
    /* XXX really want alignment to align(_d_scratch) */
    /* XXXX these should use the align tools */        
    static smsize_t   _align(smsize_t amt) { /* align to 8 bytes */
        if(amt & 0x7) { amt &= ~0x7; amt += 0x8; }
        return amt;
    }
    static char *   _alignAddr(char *cp) { /* align to 8 bytes */
        ptrdiff_t arith = (ptrdiff_t) cp;
        if (arith & 0x7) {
                arith &= ~0x7;
                arith += 0x8;
        }
        return (char *) arith;
    }

    NORET limited_space(smsize_t buffer_sz) : 
        _d_scratch(0),
        _scratch(0), 
        _reset_point(0), 
        _left(0), 
        _buffer_sz(buffer_sz),
        _buf_hiwat(0)
    {
        /* Round up buffer size to nearest 1K */
        /* XXX use align tools;magic number */
        if(_buffer_sz & 0x3ff) {
            _buffer_sz &= ~0x3ff;
            _buffer_sz += 0x400;
        }
        record_malloc(sizeof(*this));
        _create_buf();
    }

    NORET ~limited_space() {
        delete[] _d_scratch;
        record_free((_buffer_sz / sizeof(double))*sizeof(double));
    }

    void set_reset_point() {
        _reset_point = _scratch;
    }
    void reset() {
    DBG(<<"RESET!");
        _scratch = _reset_point;
        _left = _scratch ? _buffer_sz : 0;
    }

    smsize_t left() const { return _left; }
    smsize_t bufhiwat() const { return _buf_hiwat; }

    /*
     * borrow_buf() + keep_buf() are used before & after calling
     * the derived-key callback, when the buffer is needed, but 
     * how much is needed is not known until the some part of the
     * buffer has been used.
     */ 
    char * borrow_buf(smsize_t& how_much_left) {
        char *result = _alignAddr(_scratch);
        int lost_to_alignment = int(result - _scratch);
        DBG(<<"borrow_buf : lost to alignment " << lost_to_alignment);
        how_much_left = _left - lost_to_alignment;
        // Don't update _scratch or _left. 
        // Let keep_buf() do that.
        return result;
    }
    w_rc_t keep_buf(smsize_t amt, const char * b);
    /*
     * get_buf is called when it is known a priori how much
     * space is needed 
     */
    w_rc_t get_buf(smsize_t amt, char *&result);
    /*
     * give_buf frees the space only if it's at the
     * end.  We can only free in stack-order.
     */
    w_rc_t give_buf(smsize_t amt, char *where);

    void freefunc(const void*p, smsize_t l);
    void* allocfunc(smsize_t l);
}; /* class limited_space */


void  *
limited_space::allocfunc(smsize_t l) {
    char *p=0;
    w_rc_t rc = get_buf(l, p);
    DBG(<<"get_buf for sort key in allocfunc returns rc=" << rc);
    if(rc.is_error() && (rc.err_num() == eINSUFFICIENTMEM))  {
        p = 0;
    }
    DBG(<<"limited_space::allocfunc(sz=" << l << ") gets " << (void *)p);
    return (void *)p;
}

void 
limited_space::freefunc(const void*p, smsize_t l) {
    w_rc_t rc = give_buf(l, (char *)p);
    if(rc.is_error() && rc.err_num() != eBADSTART) {
        // must be in merge phase - things don't
        // get alloc/dealloc in stack order then.
        W_COERCE(rc);
    }
    // DBG(<<"limited_space::freefunc(sz=" << l << ") gives " << p);
}


w_rc_t 
limited_space::give_buf(smsize_t amt, char *where) 
{
    // DBG(<<"give buf where=" << (void *)where << " amt=" << amt);
    amt = _align(amt);
    if(where + amt != _scratch) {
        return RC(eBADSTART);
    }
    _scratch = where;
    _left += amt;
    // DBG(<<"give buf new scratch=" << (void*)_scratch);
    return RCOK;
}

w_rc_t 
limited_space::get_buf(smsize_t amt, char *&result) 
{
    // DBG(<<"get_buf " << amt);
    amt = _align(amt);
    if(smsize_t((_scratch + amt) - (char *)_d_scratch) > _buffer_sz) {
        DBG(<<"");
        return RC(eINSUFFICIENTMEM);
    }
    result = _scratch;
    _scratch += amt;
    _left -= amt;
    if(_buffer_sz - _left > _buf_hiwat) 
            _buf_hiwat = (_buffer_sz - _left);
    w_assert3(_alignAddr(_scratch) == _scratch);
    return RCOK;
}

w_rc_t 
limited_space::keep_buf(smsize_t amt, const char * W_IFDEBUG3(b)) 
{
    DBG(<<"enter keep_buf: amt=" << amt << " _scratch = " << (void*)_scratch);
    char *newscratch = _alignAddr(_scratch);
    w_assert3(newscratch == b);

    smsize_t lost_to_alignment = smsize_t(newscratch - _scratch);
    
    amt = _align(amt);

    if(smsize_t((newscratch + amt) - (char *)_d_scratch) > _buffer_sz) {
        DBG(<<"");
        return RC(eINSUFFICIENTMEM);
    }
    _scratch = newscratch + amt;
    _left -= lost_to_alignment;
    _left -= amt;

    if((_buffer_sz - _left) > _buf_hiwat) 
            _buf_hiwat = (_buffer_sz - _left);
    w_assert3(_alignAddr(_scratch) == _scratch);

    DBG(<<" leave keep_buf: amt=" << amt << " new _scratch = " << (void*)_scratch);
    return RCOK;
}


void 
limited_space::_create_buf() 
{
    w_assert3(_left == 0);
    /*
     * Create buffer for scratch use.  
     */
    DBG(<<"new" << _buffer_sz / sizeof(double));
    _d_scratch = new double[_buffer_sz / sizeof(double)];
    record_malloc((_buffer_sz / sizeof(double))*sizeof(double));
    if(!_d_scratch) {
        DBG(<<"");
        W_FATAL(eOUTOFMEMORY);
    }
    _scratch = (char *)_d_scratch;
    _left = _buffer_sz;
    w_assert3(_alignAddr(_scratch) == _scratch);
}


/*
 * Class to add functions that only sm can call -- things
 * we don't want exported to users
 */
class sm_object_t : public object_t 
{
public:
    NORET sm_object_t() : object_t() { invalidate(); }
    NORET sm_object_t(file_p&fp, slotid_t s):object_t() 
            { _construct(fp, s); }
    /// points to buffer pool
    void  construct(file_p&fp, slotid_t s)
            { _construct(fp, s); }
    /// points to scratch memory
    void  construct_from_bufs(
            const void *hdr, smsize_t hdrlen, factory_t* hf,
            const void *body, smsize_t bodylen, factory_t* _bf
            ) {
                _construct(hdr, hdrlen, hf, body, bodylen, _bf);
            }

    void  replace(const object_t& o) { _replace(o); }
    void  callback_prologue() const { _callback_prologue(); }
    void  callback_epilogue() const { _callback_epilogue(); }
    void  invalidate() { _invalidate(); }
};

const sm_object_t no_object;
const object_t& object_t::none = no_object;

/*
 * Class to add functions that only sm can call -- things
 * we don't want exported to users
 */
class sm_skey_t : public skey_t 
{
public:
    // base class default constructor is protected:
    NORET sm_skey_t() : skey_t() {}

    NORET sm_skey_t(sm_object_t&o, smsize_t off, smsize_t l, bool h) 
            : skey_t(o, off, l, h) { }

    smsize_t offset() const { return _offset; }
    void    construct(sm_object_t& o, smsize_t off, smsize_t l, bool h) {
            _construct(&o, off, l, h);
        }
    void          construct(void *buf, smsize_t off, smsize_t l, factory_t* f) {
            _construct(buf, off, l, f);
        }
    void    replace(const skey_t& k) { _replace(k); }
    void    replace_relative_to_obj(const object_t &o, const skey_t& k) { 
            _replace_relative_to_obj(o, k); 
    }
    void    invalidate() { _invalidate(); }
    bool    is_in_large_obj() const { return  is_in_obj() && 
                (!is_in_hdr()) &&
                _obj->is_valid() && _obj->is_in_buffer_pool()
                && (_obj->contig_body_size() != _obj->body_size());

                }
};


/*
 * class fib_t : computes Fibonacci numbers of a given order
 * Actually, given x, we are producing the numbers of "order x-1".
 * So the input "order" here is really the #input tapes.  It's a misnomer.
 */
class fib_t 
{
private:
    int         _order;
    int         _levels;
    int*        _f;
    int*        _p; // penultimate level
    int                _total;

    void _swap(int*& a, int*& b) {
        int *tmp = a;
        a = b;
        b = tmp;
    }

    // given previous level, compute next and return total for next
    int _compute_level(int level, int *_last, int *_next);
    void _print_level(ostream& o, int *row) const {
        int t = 0;
        for(int j=0; j<_order; j++) {
            o << " " << row[j];
            t += row[j];
        }
        o << " total=" <<t <<endl;
        o << "--done--" <<endl;
    }

public:
    NORET fib_t(int order);

    NORET ~fib_t() {
        record_free(this->size());
        delete[] _f;
        delete[] _p;
    }
    int        levels() const { return _levels; }
    int        order() const { return _order; }
    int        total() const { return _total; }
    int        num(int i) const { return _f[i]; }
    int        dummies(int i) const { return _p[i]; }

    // goal is the total # runs we have to distribute
    // compute enough levels to distribute that total #
    int compute(int goal);

    int compute_dummies(int goal);

    void print(ostream& o) const {
        o << "fib_t #s of order " << _order <<endl;
        _print_level(o, _f);
    }

    size_t size() const {
        return sizeof(fib_t) + (sizeof(int)*2*(_order));
    }
};
NORET 
fib_t::fib_t(int order) : _order(order), _f(0), _p(0) 
{
    w_assert1(order >= 2);
    DBG(<<"new" << sizeof(int) * order);
    _f = new int[order];
    // NB: record_malloc is below 
    if(!_f) { DBG(<<""); W_FATAL(ss_m::eOUTOFMEMORY); }

    DBG(<<"new" << sizeof(int) * order);
    _p = new int[order];
    // NB: record_malloc is below 
    if(!_p) { DBG(<<""); W_FATAL(ss_m::eOUTOFMEMORY); }

    for(int j=0; j<order; j++) { _p[j]= -1; }
    _compute_level(0, _p, _f); // compute into _f
    // _print_level(cout, _f);

    record_malloc(this->size());
}

int 
fib_t::compute(int goal)
{
    int*        _last = _f; // last one already computed
    int*        _next = _p; // scratch
    int         level = 0; // already computed
    _total = 1; // total of level 0

    while( (_total = _compute_level(++level, _last, _next)) < goal) {
        _swap(_last, _next);
    }
    _f = _next;
    _p = _last;
    _levels = level;
    return _total;
}
int 
fib_t::compute_dummies(int goal) 
{
    // Now, compute # dummies for each column and put them in _p
    int j;
    for(j = 0; j < _order; j++) _p[j] = 0;
    int d = _total - goal;
    w_assert3(j == _order);
    j = 0;
    while(d-- > 0) {
        _p[j++]++;
        w_assert3(_p[j-1] <= _f[0]); // _f[0] highest of all
        if(j == _order) j = 0;
    }
    return _total - goal;
}

int 
fib_t::_compute_level(int level, int *_last, int *_next)
{
    int total = 0;
    if(level == 0) {
        int j;
        for(j = 1; j < _order; j++) {
            _next[j] = 0;
        }
        _next[0] = 1;

    } else {
        int base = _last[0];
        int last = 0;

        for(int j = _order-1; j >=0 ; j--) {
            _next[j] = base + last;
            last = _last[j];
            total += _next[j];
        }
    }
#if W_DEBUG_LEVEL > 1
    if(0) {
        _print_level(cout, _next);
    }
#endif 
    return total;
}

// run_t: keep info about each run. We need to know its
// first and last pages and first slot on first page, last slot on last page
// to indicate the span of the run.  A run cannot start or end mid-page,
// since the run is a set of complete pages.
class run_t 
{
    shpid_t          _first_page;
    shpid_t          _last_page;
    slotid_t         _first_slot;
    slotid_t         _last_slot;
public:
    shpid_t          first_page() const { return _first_page; }
    shpid_t          last_page() const { return _last_page; }
    slotid_t         first_slot() const { return _first_slot; }
    slotid_t         last_slot() const { return _last_slot; }

    void set_first(const shpid_t& p, const slotid_t& s) {
        _first_page = p;
        _first_slot = s;
    }
    void set_first(const rid_t& x) {
        set_first(x.pid.page, x.slot);
    }
    void set_last(const shpid_t& p, const slotid_t& s) {
        _last_page = p;
        _last_slot = s;
    }
    void set_last(const rid_t& x) {
        set_last(x.pid.page, x.slot);
    }
    size_t size_in_bytes() const {
        return sizeof(run_t);
    }
    bool   empty() const {
        return _first_slot == 0;
    }
    w_rc_t next(file_p& fp) {
        DBG(<<" run_t::next: ENTER first=" 
                        << first_page() << "." << first_slot()
           << " last= " << last_page() << "." << last_slot() );

        w_assert3(first_page() != 0);
        _first_slot++;

        /* Might be after last item in run */
        if(_first_page == _last_page) {
            if(_first_slot >  _last_slot) {
                _first_page = 0;
                _first_slot = 0;
            }
            w_assert3(_first_slot < fp.nslots());
        } else  {
            if(_first_slot >= fp.nslots()) {
                bool eof;
                lpid_t pid = fp.pid();
                w_assert1(pid.page == _first_page);
                W_DO(ss_m::fi->next_page(pid, eof, NULL /*only allocated pages*/));
                DBG(<<" run_t::next page after " 
                    << _first_page << " is " << pid);
                INC_STAT_SORT(sort_page_fixes);
                _first_page = pid.page;
                _first_slot = (eof?  0 : 1);
            }
        }
        DBG(<<" run_t::next is " << _first_page << "." << _first_slot);
        return RCOK;
    }

    friend ostream & operator<< (ostream &o,  const run_t& r) ;
};

ostream & operator<< (ostream &o, const run_t& r)  
{
    const char *isempty = r.empty() ? " Empty" : "";
    const char *isdummy = (r._first_page==0) ? " Dummy" : "";
    o << r._first_page << "." << r._first_slot
      << "-> " << r._last_page << "." << r._last_slot 
      << isdummy
      << isempty;
    return o;
}

/*
 * Keeps track of a "phase" in a polyphase merge
 * Keeps info about runs on tapes, but does not manage the
 * tapes themselves.
 */
class phase_mgr : public smlevel_top
{
    const fib_t* _fib;
    int         _order;        // number of "tapes" including output tape
    int         _level; // number of passes with fixed-size runs
    int*        _f;        // distribution of runs to tapes
    int         _total; // total # runs in the whole mess
    int         _ototal; // original total # runs in the whole mess
    int         _high;
    int         _low;
    int         _way;  // have an n-way merge at this point
    int         _target; // target tape# for output

private:
    void _compute_high_low() {
        _high = 0;
        _low = _total;
        _way = 0;
        _total = 0;
        for(int i=0; i<= _order; i++) {
            if(_f[i] > _high) _high = _f[i];
            if(_f[i]>0) {
                if (_f[i] < _low) _low = _f[i];
                _way++;
                _total += _f[i];
            }
        }
    }
    void _add_dummies() {
        for(int i=0; i< _order; i++) {
            _f[i] += _fib->dummies(i);
            _ototal += _f[i];
        }
        // record_free is in its destructor
        delete _fib;
        _fib=0;
    }

public:
    // distribute  
    NORET phase_mgr(const fib_t *f) : 
        _fib(f),
        _order(f->order()),
        _level(f->levels()), 
        _f(0), 
        _total(f->total()), 
        _ototal(0),
        _high(0), 
        _low(0), _way(0), _target(_order)
        {
            DBG(<<"new" << sizeof(int) * (_order+1));
            _f = new int[_order+1];
            // NB: record_malloc is done below
            if(!_f) { DBG(<<""); W_FATAL(eOUTOFMEMORY); }

            for(int i=0; i< _order; i++) {
                // Start by distributing real runs
                _f[i] = (f->num(i) - f->dummies(i));
            }
            _f[_order] = 0; // _target 
            //  Add dummy runs separately, since
            //  they are different
            _compute_high_low(); // to enable _alloc_runs for target tape

            record_malloc(this->size());
        }
    void sort_phase_done() {
            _add_dummies();
            _compute_high_low();// recompute
        }
    NORET ~phase_mgr() {
            record_free(this->size());
            delete[] _f;
            delete _fib;
        }

    size_t size() {
        return sizeof(phase_mgr) + (sizeof(int)*2*(_order+1));
    }
    int order() const{ return _order; }
    int total() const{ return _total; }
    int phases_left() const{ return _level; }
    int way() const{ return _way; }
    int high() const{ return _high; }        // lgst # runs on any tape
    int low() const  { return _low; }        // smallest non-zero # runs on any tape
    bool last() const { return (_high == 1)? true : false; }
    bool done() const { return (_way == 1)? true : false; }
    int num(int i) const { return _f[i]; }
    int target() const { return _target; }

    void finish_phase() 
    {
        DBG(<<"finish_phase");
        int old_target = _target;
        w_assert3(_f[old_target] == 0);
        _target = -1;
        for(int i=0; i<= _order; i++) {
            _f[i] -= low();
            if(_f[i] <= 0) {
                _f[i] = 0;
            }
            if(i != old_target) {
                if(_f[i]==0) {
                    if(_target < 0) {
                        _target = i;
                    }
                }
            }
        }
        _f[old_target] = low(); 

        _compute_high_low();
        _level--;
        w_assert3( phases_left() >= 0);
        if( phases_left() == 1) {
            w_assert3(high() == 1);
            w_assert3(way() > 1);
            w_assert3(total() > 1);
        }
        if( phases_left() == 0) {
            w_assert3(high() == 1);
            w_assert3(way() == 1);
            w_assert3(total() == 1);
        }
    }
    void print(ostream &o) const {
        for(int i=0; i<order()+1; i++) {
            o << " " <<num(i)
            ;
        }
        if(last()) o << " LAST " ;
        if(done()) o << " DONE " ;
        o <<endl;
    }
};


/*
 * class meta_header_t
 * holds info that gets written into the meta-record(in tmp file)'s 
 * header.  
 */ 
class meta_header_t 
{
public:
    // NB: 4Gig limit: can't stable-sort more than 4 billion records
    typedef uint4_t        ordinal_number_t;

private:
    //*********************************************************
    // BEGIN persistent part
    //*********************************************************
    struct _persistent {
        /*
         * Minimal idenfication info for each original record 
         */
        ordinal_number_t        _ordinal; // 4-8 bytes - to make it a
                                          // stable sort
        shpid_t                _page;    //4 bytes
        slotid_t               _slot;    //2 bytes
        uint1_t                _nkeys;   //1 byte
        /*
         * Compress all Booleans into one byte/short/word/
         */
        typedef uint1_t       booleans_t;
        booleans_t            _booleans;
        enum { 
            _is_dup=0x1, 
            _has_index_key=0x2, 
            _must_compare_in_pieces = 0x8  // NB: uses 5 bits!
        };
          //
          // _is_dup means this key was marked as a duplicate
          // _has_index_key means we copied out a separate index key
          //         in the temp file
          // _must_compare_in_pieces for key k means the key
          //    points into buffer pool and key is spread across pages
          // 
        /* 
         * When we write a metarecord to a temp file,
         * we write in the metarecord's header:
         *         _persistent.
         * we write in the metarecord's body:
         * SORT KEYS:
         *    If keys are compared in buffer pool, nothing (length,off
         *       are in _persistent)
         *    else copy of sort keys (lengths are in _persistent).
         * INDEX KEY: -- iff index key is different from sort key.
         * LG META: -- iff object is large and !deep copy
         * WHOLE OBJECT HDR: iff object is carried along and hdr is used
         * WHOLE OBJECT BODY: iff object is carried along 
         * 
         * To keep track of these, we stash the following sizes in
         * _persistent:
         */ 
        smsize_t                 _hdrsize; // length of hdr if carrying object
        // NB: this isn't needed but it's here for sanity checking:
        smsize_t                 _bodysize; // length of body if carrying object
        smsize_t                 _lgmeta; // length of lgmetadata if large and
                                        // !deep copy.
        smsize_t                 _ikeysize; // index key length

        smsize_t                 _lenoff[max_keys_handled*2];
           // compress length of key and offset into object/hdr
           // into this one word for each sort key.

        NORET _persistent() :
            _ordinal(0), 
            _page(0), 
            _slot(0), 
            _nkeys(0), 
            _booleans(0), 
            _hdrsize(0), _bodysize(0), _lgmeta(0), _ikeysize(0)
            {
                for(int i=0; i<max_keys_handled*2; i++) {
                    _lenoff[i] = 0;
                }
            }
        NORET _persistent(const _persistent &other) :
            _ordinal(0), 
            _page(other._page), 
            _slot(other._slot), 
            _nkeys(other._nkeys), 
            _booleans(other._booleans), 
            _hdrsize(other._hdrsize), _bodysize(other._bodysize),
                _lgmeta(other._lgmeta),
                _ikeysize(other._ikeysize)
            {
                        W_FATAL_MSG(fcINTERNAL, << "shouldn't use copy constructor");
                        for(int i = 0; i < max_keys_handled*2; i++) {
                                _lenoff[i] = other._lenoff[i];
                        }
            }

    } __persistent_part;

public:
    ordinal_number_t ordinal() const { return __persistent_part._ordinal; }
    void             set_ordinal();
    static void      clr_ordinal();

    shpid_t          page() const  { return __persistent_part._page; }
    slotid_t         slot() const  { return __persistent_part._slot; }
    int              nkeys() const { return int(__persistent_part._nkeys); }
    smsize_t         hdrsize() const { return smsize_t(__persistent_part._hdrsize); }
    smsize_t         bodysize() const { return smsize_t(__persistent_part._bodysize); }
    smsize_t         lgmetasize() const { return smsize_t(__persistent_part._lgmeta); }
    smsize_t         ikeysize() const { return smsize_t(__persistent_part._ikeysize); }

    void         set_hdrsize(smsize_t s) { __persistent_part._hdrsize = s; }
    void         set_bodysize(smsize_t s) { __persistent_part._bodysize = s; }
    void         set_lgmetasize(smsize_t s) { __persistent_part._lgmeta = s; }
    void         set_ikeysize(smsize_t s) { __persistent_part._ikeysize = s; }
    // size of stuff written in a meta_record with nk keys
    smsize_t        persistent_size() const {
        return (sizeof(__persistent_part) - 
                ((max_keys_handled - __persistent_part._nkeys)*sizeof(smsize_t)*2));
    }

    smsize_t&         __len(int k) { return __persistent_part._lenoff[k<<1]; }
    smsize_t&         __off(int k) { return __persistent_part._lenoff[(k<<1)+1]; }
    const smsize_t&         __const_len(int k) const{ 
                        return __persistent_part._lenoff[k<<1]; }
    const smsize_t&         __const_off(int k) const{ 
                        return __persistent_part._lenoff[(k<<1)+1]; }

    bool is_dup() const { return __persistent_part._booleans & 
                        _persistent::_is_dup; }
    void mark_dup() { __persistent_part._booleans |= 
                        _persistent::_is_dup; }
    void unmark_dup() { __persistent_part._booleans &= ~
                        _persistent::_is_dup; }

    bool pieces(int k) const { return 
                        (__persistent_part._booleans & 
                        ( _persistent::_must_compare_in_pieces<<k)) ?
                                true:false; 
                }
    void mark_pieces(int k) { 
                        __persistent_part._booleans |= 
                                ( _persistent::_must_compare_in_pieces << k); 
                }
    void unmark_pieces(int k) { 
                        __persistent_part._booleans &= 
                                ~(_persistent::_must_compare_in_pieces<<k); 
                }

    void set(file_p &_fp, slotid_t slot) {
            __persistent_part._page = _fp.pid().page;
            __persistent_part._slot = slot;
    }
    void set_nkey(int nk) { __persistent_part._nkeys = nk; }

    void unmarshal_sortkeys() {
        for(int k=0; k < nkeys(); k++) {
            const sm_skey_t& sk = sort_key(k);
            __len(k) = sk.size();
            __off(k) = sk.offset(); 
        }
    }
    void marshal_sortkeys(factory_t& fact);
    //*********************************************************
    // END persistent part
    //*********************************************************

private:

    sm_object_t              _whole_object;
    sm_skey_t                _sort_key[max_keys_handled];
    sm_skey_t                _index_key;
    sm_skey_t                _lgmetadata; // not really a key but ok
public:
    const sm_skey_t&         lgmetadata() const { return _lgmetadata; }
    sm_skey_t&               lgmetadata_non_const() { return _lgmetadata; }
    const sm_object_t&       whole_object() const { return _whole_object; }
    sm_object_t&             whole_object_non_const() { return _whole_object; }
    const sm_skey_t&         sort_key(int i) const { return _sort_key[i]; }
    sm_skey_t&               sort_key_non_const(int i) { return _sort_key[i]; }
    const sm_skey_t&         index_key() const { return _index_key; }
    sm_skey_t&               index_key_non_const() { return _index_key; }

public:
    NORET meta_header_t() :
            __persistent_part(), 
            _whole_object()   
        {
            w_assert1(unsigned(max_keys_handled) < (sizeof(
                        __persistent_part._booleans)*8)-1);
        for(int i = 0; i < max_keys_handled; i++) {
#if W_DEBUG_LEVEL > 1
        {
            /* Make max_keys_handled bits fit into
             * the space given in booleans_t. This is a 
             * sanity check in event that someone re-compiles
             * with a larger max_keys_handled without changing
             * the size of booleans_t.
             */
            uint4_t tmp;
            tmp = _persistent::_must_compare_in_pieces << (max_keys_handled-1);
            _persistent::booleans_t boo;
            boo = _persistent::_must_compare_in_pieces << (max_keys_handled-1);
            w_assert3(uint4_t(boo) == tmp);
        }
#endif
        }
    }

    NORET ~meta_header_t() { }

private:  
    // copy constructor disabled
    NORET meta_header_t(const meta_header_t &other) ;

public:
    void  freespace() {
        // DBG(<<"meta_header_t::freespace: ordinal=" << ordinal());
        /*
        // Free space in opposite order of
        // that allocated. Gets 
        // ALLOCATED as follows:
            whole_object()
            sort keys
            index key
            large object metadata
        */
        // DBG(<<"freespace lgmeta");
        lgmetadata_non_const().freespace();
        // DBG(<<"freespace index key");
        index_key_non_const().freespace();
        for(int k=nkeys()-1; k >=0; k--) {
            // DBG(<<"freespace sort key " << k);
            sort_key_non_const(k).freespace();
            unmark_pieces(k);
        }
        // DBG(<<"freespace whole obj");
        whole_object_non_const().freespace();
    }

    void assert_nobuffers() const { 
        for(int k=0; k < nkeys(); k++) {
            sort_key(k).assert_nobuffers();
        }
        index_key().assert_nobuffers();
        whole_object().assert_nobuffers();
    }

    void assert_consistent() const { 
        for(int k=0; k < nkeys(); k++) {
            w_assert3(sort_key(k).consistent_with_object(whole_object()));
        }
        w_assert3(index_key().consistent_with_object(whole_object()));
    }
    bool  is_consistent() const { assert_consistent(); return true; }

    void  clear()         {
        __persistent_part._nkeys = 0;
        __persistent_part._page = 0;
        __persistent_part._slot = 0;
        unmark_dup();
    }

    bool is_null() const { 
        for(int i=0; i<nkeys(); i++) { 
            if(sort_key(i).size() > 0) return false; 
        }
        return true; 
    }

    const slotid_t&        slotid() const {  return __persistent_part._slot; }
    const shpid_t&         shpid() const {  return __persistent_part._page; }

    shrid_t                shrid(snum_t store) const { 
                                return shrid_t(__persistent_part._page, 
                                store, __persistent_part._slot); } 


    void move_sort_keys(meta_header_t& other);
private:
}; /* class meta_header_t */

void 
meta_header_t::marshal_sortkeys(
        factory_t& fact // preferred factory to use.  Is _scratch_space
                        // or, failing that, cpp_vector
        ) 
{
    for(int k=0; k < nkeys(); k++) {
        sm_skey_t& sk = sort_key_non_const(k);
        /* Only if it's compared in pieces do we
         * have an in-buffer-pool key
         */ 
        if(pieces(k)) {
            // Must be in body, hence last argument==false
            sk.construct(whole_object_non_const(),
                    __const_off(k), __const_len(k), false);
        } else {
            // create a new sm_skey_t
            // After writing to a tmp file, we 
            // have an in-memory copy of the key for 
            // all cases of !pieces
            // 
            // NB: key can be null, hence 0 length.

            char *buffer = 0;
            factory_t *f = &fact;
            if(__const_len(k) > 0) {
                DBG(<<"allocfunc for sort key " << k
                    <<" ordinal=" << ordinal() );
                buffer = (char *)fact.allocfunc(__const_len(k));
                DBG(<<"allocfunc failed for key " << k );
                if(buffer==0) {
                    DBG(<<"different allocfunc for sort key " << k
                            <<" ordinal=" << ordinal() );
                    f = factory_t::cpp_vector;
                    buffer = (char *)f->allocfunc(__const_len(k));
                }
                w_assert3(buffer);
                sk.construct((void *)buffer, 0, __const_len(k), f);
            } else {
                sk.construct(0, 0, 0, factory_t::none);
                // is valid though...
            }
        }
    }
}

void 
meta_header_t::move_sort_keys(meta_header_t& other) 
{
    // clobbers other.sort_key(i) for each i
    DBG(<<"move_sort_keys");
    other.assert_consistent();

    assert_consistent();
    freespace();
    assert_consistent();
    assert_nobuffers();

    // Does not free space in other.whole_object()
    whole_object_non_const().replace(other.whole_object());

    set_nkey(other.nkeys());
    for(int i=0; i<nkeys(); i++) {
        sm_skey_t& k = sort_key_non_const(i);
        DBG(<<"move sort key " << i);
        // Must be careful here: keys must be made
        // to point to THIS->whole_object() rather than
        // OTHER->whole_object().
        // k.replace(other.sort_key_non_const(i));
        k.replace_relative_to_obj(whole_object(), 
                    other.sort_key_non_const(i));
        other.sort_key_non_const(i).invalidate();
        other.sort_key(i).assert_nobuffers();
    }
    assert_consistent();

    other.whole_object_non_const().invalidate();
}


inline void 
meta_header_t::set_ordinal() { __persistent_part._ordinal = ++me()->get__ordinal(); }
inline void 
meta_header_t::clr_ordinal() { me()->get__ordinal() = 0; }

class tape_t : public smlevel_top 
{
public:
    int              _tape_number;
private:
    snum_t          _store;
    vid_t           _vol;
    int             _maxruns;
    int             _first_run;
    int             _last_run; // really first run after last run
    run_t*          _list;
    int             _count_put;
    int             _count_get;

    /* metadata about *FIRST* meta-record in the first run */
    meta_header_t *_meta;
    bool           _primed_for_input; // false: meta isn't up-to-date
    bool           _primed_for_output; 
    file_p         _metafp;        // for first page of each run (meta-records)
    file_p         _origfp;        // for preparing pages of pieces() 
                                // NB: when we use this, we're 
                                // possibly doubling the # BP pages
                                // hogged.
    lgdata_p      _lgpage; // for large-object handling

    sdesc_t*      _sd;


    w_rc_t        _next_meta_rid() {
        return _list[first_run()].next(metafp());
    }

    w_rc_t         _create_tmpfile(vid_t v);
    w_rc_t         _rewind(bool create_new);
public:
    NORET tape_t () : 
#ifdef W_TRACE
        _tape_number(0),
#endif
        _store(0), _vol(0), _maxruns(0), 
        _first_run(-1), _last_run(0), _list(0),
        _count_put(0),
        _count_get(0),
        _meta(0), 
        _primed_for_input(false), _primed_for_output(false),
        _sd(0)
        { 
        }

    NORET ~tape_t () {
        // delete[] the  key space that was malloced
        _metafp.unfix();
        _origfp.unfix();
        _lgpage.unfix();
        W_COERCE(_rewind(false));

        DBG(<<"~tape "<<_tape_number);

        record_free(sizeof(run_t)* _maxruns);
        delete[] _list;
        _list = 0;
    }

    void  inc_count_put() { _count_put++; }
    void  inc_count_get() { _count_get++; }
    void  clr_count_put() { _count_put=0; }
    void  clr_count_get() { _count_get=0; }
    int   count_put() const { return _count_put; }
    int   count_get() const { return _count_get; }

#if W_DEBUG_LEVEL > 2
    shrid_t  real_shrid(snum_t store) const { 
                        return _meta->shrid(store); 
                    }
    void     _checksd() const {
                        if(_store != 0) {
                            w_assert3(tmp_fid() == sdesc()->stid());
                        } else {
                            w_assert3(_sd == 0);
                        }
                    }
#else
    inline void _checksd() const {}
#endif 

    const meta_header_t *meta() const { return _meta; }
    int                first_run() const { return _first_run; }
    int                last_run() const { return _last_run; }
    bool               primed_for_output()const { return _primed_for_output; }
    bool               primed_for_input()const { return _primed_for_input; }
    stid_t             tmp_fid() const { return stid_t(_vol, _store); }

    // return the rid of the last item of the first run remaining to be read
    // i.e., current run
    rid_t              run_end() const {
                            rid_t rid(_vol, shrid_t(_list[first_run()].last_page(),
                                    _store, 
                                    _list[first_run()].last_slot()));
                           return rid;
                       }
    // return the rid of the first item of the first run remaining to be read
    // i.e., current run
    rid_t              run_beg() const {
                         rid_t rid(_vol, shrid_t(_list[first_run()].first_page(),
                                    _store, 
                                    _list[first_run()].first_slot()));
                         return rid;
                       }
    rid_t              meta_rid() const { return run_beg(); }
    sdesc_t*           sdesc() const { return _sd; }
    bool               is_dummy_run() const {
                         if(_list[first_run()].first_page() == 0) return true;
                         return false;
                       }
    // curr_run_empty(): current run is empty.
    // Current run must be legit else we get assertion failure
    // A dummy run will be empty.
    bool               curr_run_empty() const { 
                           // Don't call this 
                           w_assert1( (first_run() >=0) && 
                                (first_run() < last_run()));
                           return _list[first_run()].empty(); 
                       }

    // is_empty(): contains no (more) runs
    // Does not check current run to see if we're at the end of it.
    // We have to have completed_run()-ed past the last run for this to 
    // be true.
    bool              is_empty() const {
                          // First case: nothing ever written to the tape
                          // first_run() is -1 and last_run() is 0
                          if (last_run() == 0)  {
                              w_assert3(first_run() == -1);
                              return true;
                          }

                          // All runs written have been read and we've
                          // completed_run()-ed past them all.
                          if (first_run() >= last_run()) return true;

                          return false;
                          /* REMOVED THIS CASE -- presence of dummy run
                          // now means tape is not empty. 
                          **
                          // We just read the last run, but haven't 
                          // primed past it yet.
                          // first_run() < last_run() 
                          // first_run() could be -1 and last_run() > 0
                          if ( (first_run() == last_run() - 1) &&
                                  curr_run_empty() ) return true;
                          return false; 
                          */
                      }

    size_t           size_in_bytes() const {
                        return sizeof(tape_t) + (sizeof(run_t)*_maxruns);
                     }

    void             init_vid(vid_t v);
    void             alloc_runs(int m);

    file_p&          metafp() { return _metafp; }
    file_p&          origfp() { return _origfp; }
    void             release_page(file_p&f) { f.unfix(); }
    void             set_store(stid_t f) {
                        _store = f.store;
                        _vol = f.vol;
                     }
    snum_t           get_store()const { return _store; }
    void             add_dummy_run() {
                        _list[last_run()].set_first(0,0);
                        // _list[last_run()].set_last(0,0);
                        DBG(<<"ADD DUMMY RUN" << _tape_number);
                        _last_run++; // is really next_run
                     }
    void             add_run_first(const shpid_t& p, const slotid_t& s) {
                        DBG(<<"START WRITING RUN " << last_run()
                        << " on tape  " << _tape_number
                        << " store " << _store 
                        );
                        _list[last_run()].set_first(p,s);
                    }
    void             add_run_last(const shpid_t& p, const slotid_t& s) {
                        _list[last_run()].set_last(p,s);
                        DBG(<<"FINISH WRITING RUN " << last_run() 
                            << " on tape  " << _tape_number
                            << " store " << _store 
                            << " with now " << _count_put << " records" 
                            );
                        _last_run++;
                    }

    void             completed_run() {
                        DBG(<<"COMPLETED (READING) RUN " << first_run()
                        << " on tape " << _tape_number 
                        << " store " << _store 
                        << " with " << _count_put << " records" 
                        << " of which " << _count_get << " were read" 
                        );
                        _first_run++;
                        w_assert1(first_run() >= 0);
                        w_assert3(first_run() <= last_run());
                        // could be last
                    }

    void               prepare_tape_buffer(meta_header_t *m);
    void               prime_tape_for_input();
    w_rc_t             prime_tape_for_output();

    w_rc_t             prime_record(const sort_keys_t &info, run_mgr&,
                            factory_t&);
    w_rc_t             pin_orig_rec(file_p& ifile_page, stid_t&f, 
                            record_t *&rec, rid_t&        r); 

    friend ostream & operator<< (ostream &o,  const tape_t& t) ;

    void            check_tape_file(bool printall) const;
};

ostream & operator<< (ostream &o, const tape_t& t)  
{
    o << t._tape_number
        <<" store "  << t._store
        <<" puts "  << t._count_put
        <<" gets "  << t._count_get
        <<" curr_run"  << t._first_run
        << endl
        ;
    int i = t._first_run;
    if(i<0) i=0;
    for(; i < t._last_run; i++) {
        run_t& R = t._list[i];
        o <<", (" << R << ")" << endl ;
    }
    return o;
}

void  tape_t::check_tape_file(bool W_IFDEBUG3(printall)) const
{
#if W_DEBUG_LEVEL > 2
    // Read the entire file and verify that
    // the runs actually span the entire file, that
    // all the objects are accounted for.
    // If the argument printall is true, print
    // all the info we find as we go.
    int     bad=0;
    stid_t  fid(_vol, _store);
    lpid_t  pid;
    rid_t   first_rid;
    rid_t   last_rid;
    file_p  page;
    slotid_t slot=0;
    slotid_t last_slot=0;

    // There's always a first page, btw.
    bool is_first=true;
    bool eof=false;
    int  numrecords=0;
    if(printall) {
        DBG(<< endl <<"START check_tape_file " << _tape_number);
    }

    W_COERCE( fi->first_page(fid, pid, NULL /* allocated only */) );
    while (! eof)  {
        w_assert1(pid.page != 0);
        W_COERCE(page.fix(pid, LATCH_SH));
        // Get first valid slot on this page.
        slot = page.next_slot(0);

        if(is_first) {
            // check against info about first page, slot
            // First allocated page might be empty
            if(slot) {
                first_rid = rid_t(pid, slot);
                is_first = false;
            }
        }

        // Count the records
        while(slot) {
            numrecords++;
            last_slot = slot;
            slot = page.next_slot(slot);
        }

        // save info about last slot found on this page
        last_rid = rid_t(pid, last_slot);

        if(printall) {
            DBG(<<"check_tape_file: page " << pid
                    << " first valid slot (so far) " << first_rid
                    << " last valid slot (so far) " << last_rid
                    << " cumu #recs" << numrecords
            );
        }

        page.unfix();
        W_COERCE(fi->next_page(pid, eof, NULL /* allocated only*/));
    }

    // check info about last slot, first slot against
    // info in the first, last run_t
    // First, must find first non-dummy run
    int f = 0;
    while (_list[f].first_page() == 0) f++;
    
    stid_t s(_vol, _store);
    {
        run_t &R = _list[f];
        lpid_t p(s, R.first_page());
        rid_t rid = rid_t(p, R.first_slot());
        if(rid != first_rid) {
            bad++;
            if(printall) {
                DBG( << "BOGUS TAPE: found first_rid  " << first_rid
                    << " tape: " << *this);
            }
        }
    }
    if(last_run()>0) 
    {
        run_t &R = _list[last_run()-1];
        lpid_t p(s, R.last_page());
        rid_t rid = rid_t(p, R.last_slot());
        if(rid != last_rid) {
            bad++;
            if(printall) {
                DBG( << "BOGUS TAPE: found last_rid  " << last_rid
                    << " tape: " << *this);
            }
        }
    }
    // Check number of records
    if(numrecords != count_put()) {
        bad++;
        if(printall) {
            DBG( << "BOGUS TAPE: found " << numrecords 
            << " records, but count_put() is " << count_put());
        }
    }

    // re-do and print all the info.
    if(bad>0 && !printall) check_tape_file(true);
    if(printall) {
        DBG(<<"END check_tape_file " << _tape_number << endl << endl);
    }
#endif
}

class ssm_sort::run_mgr : public smlevel_top, xct_dependent_t 
{
public:
    /* Required for Heap */
    bool gt(const tape_t *, const tape_t *) const;

private:
    /****************** data members **************************/
    limited_space &    _scratch_space;
    stid_t             _ifid;

    int                _NRUNS;
    int                _NTAPES; // # input tapes + 1 output tape
    int                _numslots;
    int                _next_run; // first run# after last valid run#
                     // across all tapes
    int                _M;
    tape_t*            _tapes;
    phase_mgr*         _phase;

    sort_keys_t &info;
    meta_header_t** rec_list;        // used only in qsort part
    meta_header_t* rec_first;        // used in qsort & merge
    meta_header_t* rec_curr;        // used only in qsort part
    meta_header_t* rec_last;        // used only in qsort part
    meta_header_t* rec_first_tofree;// where to start looking for
                                // stuff to free

    bool        _recompute;
    bool        _meta_malloced;
    int         _save_pin_count;
    bool        _aborted;       // because xct state changed, presumably
                                // by another thread

    file_p        last_inputfile_page_read;

    /***************** private methods *****************/

    void xct_state_changed(xct_state_t oldstate, xct_state_t newstate);

    /* 
     * For first (quicksort) phase only
     */
                // called by flush_run only
    void         _QuickSort(meta_header_t* a[], int cnt);

                // called by output_single_run only
    record_t *  _rec_in_run(file_p *fp, file_p*& fpout, meta_header_t *m)const;

                // called by put_rec only, when object is first encountered
                // while collecting runs from input file.
    w_rc_t         _prepare_key( 
                    sm_object_t&        obj,
                    int                 k, 
                    file_p&             small,
                    const record_t&     rec,
                    bool&               compare_in_pieces, // out
                    sm_skey_t&          kd //out
                    );

                // called by output_single_run and merge
    w_rc_t         _output_index_rec(
                    stid_t&         ofid,
                    rid_t&        orig,
                    const meta_header_t*        m,
                    record_t*        rec,
                    file_p&        last_page_written,
                    sdesc_t*        sd
                    ) const;

    w_rc_t         _output_pinned_rec(
                    stid_t&         ofid,
                    rid_t&        orig,
                    record_t*        rec,
                    file_p&        orig_rec_page,
                    file_p&        last_page_written,
                    sdesc_t*        sd,
                    bool&        swap
                    ) const;
   w_rc_t         _output_rec(
                    stid_t&         ,
                    meta_header_t *m,        
                    file_p&        last_page_written,
                    sdesc_t*        sd,         // cached
                    bool&        swap
                    ) const ;

                // called by flush_run only when !once
                // during first phase only
    tape_t*       _next_tape(); // used during sort phase

    /* 
     * Used in both phases:
     */
    CF             _keycmp(int i) const { return info.keycmp(i); }
    int            _KeyCmp(const meta_header_t *k1, const meta_header_t* k2) const;


    /*
     * used only in merge phases
     */
    w_rc_t         _merge(
                    RunHeap*                h,
                    meta_header_t*        l, 
                    bool                 last_pass, 
                    tape_t*                 outtape,          
                    stid_t                 outfile, 
                    bool&                 swap
                    );

    w_rc_t         _output_metarec(
                    meta_header_t*        m,
                    tape_t*                t,
                    rid_t&                result_rid
                    );

    void        _clear_meta_buffers(bool);
public:
    NORET ~run_mgr() {
        DBG(<<"_NTAPES");
        if(_NTAPES) {
            record_free(_NTAPES * _tapes[0].size_in_bytes()); 
        }
        delete[] _tapes;
        _tapes = 0;

        // record_free is done in destructor
        delete _phase;
        _phase = 0;

        DBG(<<" ~run_mgr");
        _clear_meta_buffers(true);
    }

    NORET run_mgr(
        class limited_space &ss,
        const stid_t&        _ifid, // input file id
        int                 _run_size,  // in pages
        int                 _num_runs,  // # runs generated in first phase
        int                 _ntapes,  // max # merged at once
        tape_t*             _tapeslist,
        smsize_t            _min_rec_size, 
        sort_keys_t &       _info, 
        w_rc_t&             result
        );

    int         nkeys() const { return info.nkeys(); }

                // Called in first (quicksort) phase only
    w_rc_t         put_rec(file_p &, slotid_t );


    void           prologue();        // to flush_run
    w_rc_t         flush_run(bool flush);
    void           epilogue();        // to flush_run

                // when all fits in memory (1 run)
    w_rc_t         output_single_run(file_p* fp, stid_t, bool&);

                // when multiple runs
    w_rc_t         merge(stid_t, bool&);

                // called by merge
    w_rc_t         insert(RunHeap* , tape_t* , int ) const;

                // for callback
    void          callback_prologue() const {
#if W_DEBUG_LEVEL > 1
                    /*
                     * leaving SM
                     */
                    // escape const-ness of the method
                    int *save_pin_count = (int *)&_save_pin_count;
                    *save_pin_count = me()->pin_count();
                    w_assert3(_save_pin_count == 0);
#endif 
#if W_DEBUG_LEVEL > 2
                    // NB: this debug level had better match
                    // that in prologue.h for updating in_sm
                    // else you'll get assertion errors
                    me()->check_pin_count(0);
                    me()->in_sm(false);
#endif
                }
    void          callback_epilogue() const {
#if W_DEBUG_LEVEL > 2
                    /*
                     * re-entering SM
                     */
                    // NB: this debug level had better match
                    // that in prologue.h for updating in_sm
                    // else you'll get assertion errors
                    me()->check_actual_pin_count(_save_pin_count);
                    me()->in_sm(true);
#endif 
                }
    const stid_t& ifid() { return _ifid; }
};



NORET 
run_mgr::run_mgr(
    class limited_space &ss,
    const stid_t& __ifid, // input file
    int         _run_size, 
    int         _nruns,  // i.e., #file pages / run_size in pages
    int         _ntapes,  // max # runs to be merged at once
    tape_t*     _tapeslist,
    smsize_t    _min_rec_sz, 
    sort_keys_t &_info, 
    w_rc_t        &result
    )
    :  
    xct_dependent_t(xct()),
    _scratch_space(ss),
    _ifid(__ifid),
    _NRUNS(_nruns),
    _NTAPES(_ntapes),  // input + 1 output
    _numslots(0),
    _next_run(0),
    _M(_run_size),
    _tapes(_tapeslist),
    _phase(0),
    info(_info), 
    rec_list(0), rec_first(0), rec_curr(0), rec_last(0),
    _recompute(false),
    _meta_malloced(false),
    _save_pin_count(0),
    _aborted(false)
{
    /*
     * numslots: estimate (get upper bound on) the number of slots in 
     * all the input pages for a run, so we can allocate meta_header_t
     * structs for all the objects in a run.
     */
    {
        smsize_t space_needed;
#ifdef W_TRACE
        int  t = 
#else
        (void)
#endif
            file_p::choose_rec_implementation(0,/*est hdr len */
                _min_rec_sz,  /* est data len */
                space_needed // output
                );
        _numslots = int(file_p::data_sz) / align(space_needed);
        w_assert3(_numslots > 0);
        DBG(    <<"_min_rec_sz= " << _min_rec_sz
                << " space_needed= " << space_needed
                << " _numslots= " << _numslots
                << " kind= " << t
                );
    }
    _numslots *= _M; // in pages

    record_malloc(sizeof(*this));

    if(_NTAPES > 0) {
        DBG(<<"new fib_t" << sizeof(fib_t));
        int order = _NTAPES-1;
        fib_t *fib = new fib_t(order); // input only
        // record_malloc is done in constructor
        if(!fib) {
            DBG(<<"");
            result = RC(eOUTOFMEMORY);
            return;
        }

        fib->compute(_NRUNS);
        DBG(<<"fib for goal " << _NRUNS);
        fib->compute_dummies(_NRUNS);

        DBG(<<"new phase_mgr" << sizeof(phase_mgr));
        _phase = new phase_mgr(fib);
        // record_malloc is done in constructor
        if(!_phase) {
            DBG(<<"");
            result = RC(eOUTOFMEMORY);
            return;
        }

        // Later, after we've written the runs to the
        // tapes, we'll call
        // _phase->sort_phase_done(); 
        // to add dummy runs.  For now, all we need to do
        // is allocate space for the runs.
        // If we decide that we cannot process so many tapes
        // at once, we'll re-compute fib #s, create a new
        // phase mgr, redistribute the runs from the extra tapes
        // to the fewer tapes, and go from there.

        int j;
        int low=0;
        for (j=0; j< order; j++) {
            DBG(<<" alloc num=" << fib->num(j) 
                << " dummies=" << fib->dummies(j)
                << " max # runs for tape " << j );
            _tapes[j].alloc_runs(fib->num(j));
            record_malloc(_tapes[j].size_in_bytes());

            if(low==0) {
                low = fib->num(j);
            } else {
                if(fib->num(j) < low) low = fib->num(j) ;
            }
        }
        w_assert3(low != 0);

        // The first target tape needs enough space for N runs, where
        // N varies from  fib->low() --> 0
        w_assert3(j == order);
        DBG(<<" alloc " << low  << " max # runs for tape " << j );
        _tapes[j].alloc_runs(low); 
    }

    /*
     * NB: it would be nice to grab only the amt of space
     * needed for the number of keys used 
     * but then the compiler couldn't do our arithmetic on rec_curr, etc
     */
    rec_first = new meta_header_t[_numslots];
    if(!rec_first) {
        result = RC(eOUTOFMEMORY);
        return;
    }
    record_malloc(_numslots * sizeof(meta_header_t));

    /* set up list of ptrs (for _QuickSort) to the metadata 
     * not needed for merge phase.
     */
    typedef meta_header_t *metaheaderptr; // for VC
    rec_list = new  metaheaderptr[_numslots];
    if(!rec_list) {
        result = RC(eOUTOFMEMORY);
        return;
    }
    record_malloc(_numslots * sizeof(meta_header_t *));

    rec_curr = rec_first;
    DBG(<<" rec_curr is now " << rec_curr);
    rec_last = rec_first + _numslots - 1; // last usable one
    // but that's based on an accurate min_rec_size given
    DBG(<<" rec_last is now " << rec_last << " based on _numslots " 
            << _numslots);

    _scratch_space.set_reset_point();
    register_me();
}

void
run_mgr::_clear_meta_buffers(bool deletethem) 
{
    DBG(<<"_clear_meta_buffers");
    if(rec_first) {
        for(meta_header_t *m=rec_last;m>=rec_first;m--) {
            m->unmark_dup();
            m->freespace();
        }
        if(deletethem) {
            delete[] rec_first;
            rec_first = 0;
            rec_last = 0;
            DBG(<<" rec_last is now " << rec_last );

            record_free(sizeof(meta_header_t) * _numslots);

            delete[] rec_list;
            record_free(sizeof(meta_header_t *) * _numslots);
            rec_list = 0;
        }
    }

    // Now we're in the sort phase
    _meta_malloced = true;
}

/*
 * Insert the meta-record at the head of the first run on the tape
 * into the heap
 */
w_rc_t 
run_mgr::insert(RunHeap* runheap, tape_t* r, int/*not used*/)  const
{
    DBG(<<"insert tape " << r->_tape_number);
    if(_aborted) return RC(eABORTED);

    runheap->AddElement(r); 
    w_assert2(runheap->NumElements() > 0);

    return RCOK;
}

void 
run_mgr::xct_state_changed(xct_state_t , xct_state_t new_state)
{
    if (new_state != xct_active) {
        _aborted = true;
    }
}

//
// record size (for small object, it's the real size, for large record, it's
// the header size)
//
static inline smsize_t rec_size(const record_t* rec)
{
  // NB: The following replacement had to be made for gcc 2.7.2.x
  // with a later release, try the original code again
   return (rec->tag.flags & t_small) ? smsize_t (rec->body_size())  :
                    ((rec->tag.flags & t_large_0) ?
                       smsize_t(sizeof(lg_tag_chunks_s)):
                        smsize_t(sizeof(lg_tag_indirect_s))
                            );

   /*
   return ((rec->tag.flags & t_small) ? (unsigned int)rec->body_size()
                : ((rec->tag.flags & t_large_0) ?
                        sizeof(lg_tag_chunks_s) :
                        sizeof(lg_tag_indirect_s)));
    */
}

/*
 * Called when orig record is first read read in
 * for initial processing (quicksort phase).
 */
w_rc_t 
run_mgr::put_rec(file_p &fp, slotid_t slot)
{
    FUNC(run_mgr::put);
    /*
     * Check for aborted xct
     */
    if(_aborted) return RC(eABORTED);

    /*
     * A sanity check
     */
    int nkeys = info.nkeys();
    if(info.is_for_index()) {
        if(nkeys > 1) {
            return RC(eBADARGUMENT);
        }
    }

    w_assert3(fp.is_fixed());
    const record_t *rec; 
    W_DO(fp.get_rec(slot, rec));
    rid_t        rid(fp.pid(), slot);

    DBG(<<"put_rec: rid " << rid);
    if(rec_curr > rec_last) {
#if W_DEBUG_LEVEL > 0
        {
        w_ostrstream s;
        s << 
            "Internal error: @ " << __LINE__
            << " " << __FILE__
            << endl
            <<  "_numslots " << _numslots
            << " rec_curr " << rec_curr
            << " rec_last " << rec_last
            << " limit " << int(rec_last - rec_first + 1)
            << " sizeof(*rec_last)"  << sizeof(*rec_last)
            << " rid " << rid
            << " for slot slot  " << slot
            << endl
            ;

        s << 
            "Bad estimate for minimum record size yields poor space use"
            << endl;
        s << 
            "Use a smaller hint for minimum record size to be safe.";

            fprintf(stderr,  "%s\n", s.c_str());
        }
        sortstophere();
#endif
        return RC(eINTERNAL);
    }

    rec_curr->set_nkey(info.nkeys());
    rec_curr->set_ordinal();

    sm_object_t& object = rec_curr->whole_object_non_const();
    object.construct(fp, slot);
    {
        /*
         * MOF CALLBACK
         *
         * Apply Marshal Object Function (MOF) if provided; in any case,
         * set up whole_object() to point to disk or to marshalled 
         * stuff.
         */
        MOF marshal = info.marshal_func();
        if(marshal != sort_keys_t::noMOF) {
            DBG(<<"MOF " << rid);
            // statically allocated: factory_t::none
            sm_object_t newobject;

            callback_prologue();
            W_DO( (*marshal)(rid, object, info.marshal_cookie(), &newobject) );
            callback_epilogue();
            INC_STAT_SORT(sort_mof_cnt);

            // does object.freespace and takes on newobject's factory.
            object.replace(newobject);
        }
    }

    /*
     * For each key, do 
     * 1) callback if necessary to get the key
     * 2) if no callback, see if we have to copy the
     *    key to memory anyway 
     */

    for(int k=0; k<nkeys; k++) {
        DBG(<<"inspecting key " << k);
        CSKF create_sort_key = info.keycreate(k);

        /*
         * CSKF CALLBACK for SORT KEYS
         */
        sm_skey_t&        key = rec_curr->sort_key_non_const(k);

        key.invalidate();

        if(create_sort_key!= sort_keys_t::noCSKF) {
            INC_STAT_SORT(sort_getinfo_cnt);
            DBG(<<"CSKF " << rid);
            // statically allocated: factory_t::none
            skey_t         newkey;

            rec_curr->assert_consistent();

            callback_prologue();
            W_DO( (*create_sort_key)(
                    rid,
                    object,
                    info.cookie(k),
                    _scratch_space,  // to populate newkey if not from object
                    &newkey) );
            callback_epilogue();

            // does key.freespace and takes on newkey's factory.
            key.replace(newkey);

            rec_curr->assert_consistent();

        } else {
            // noCSKF. Is fixed.
            w_assert3(info.is_fixed(k));

            // Get sort key either from pinned record or
            // from marshalled object.

            if(info.in_hdr(k)) {
                if(object.hdr_size() < info.offset(k)+ info.length(k)) {
                    return RC(eBADLENGTH);
                }
                if(object.hdr_size() < info.offset(k)) {
                    return RC(eBADSTART);
                }
            } else {
                if(object.body_size() < info.offset(k)) {
                    // might be large object
                    if(rec->body_size() < info.offset(k)) {
                        return RC(eBADSTART);
                    } else if(rec->body_size() < info.offset(k) + info.length(k)) {
                        return RC(eBADLENGTH);
                    }
                }
            }
            // copy location info directly
            key_location_t& tmp = info.get_location(k);

            key.construct(
                object, tmp._off, tmp._length,
                info.in_hdr(k));
        }
        w_assert3(key.is_valid());

        if(key.is_in_obj()) {
            bool must_compare_in_pieces=false;

            /* 
             * If the record has a null key, avoid doing some work
             * Else, see if we need to make a copy of it.
             */
            if(key.size() > 0)  {
                DBG(<<"sort key in buffer pool, size is " << key.size());
                W_DO(_prepare_key(rec_curr->whole_object_non_const(), 
                        k, fp, 
                        *rec, 
                        must_compare_in_pieces, key));
            }

            if(must_compare_in_pieces) {
                _recompute = true;
                rec_curr->mark_pieces(k);
            }
            DBG(<<"sort key in BP, size is " << key.size());
        } else {
            DBG(<<"sort key in mem, size is " << key.size());
        }
    } // for each of the keys

    /* 
     * Grab the index key for output.  Do it now so that
     * we don't re-pin the original object at the end.
     */
    if(info.is_for_index() )  {
        DBG(<<" is_for_index");
        w_assert3(nkeys == 1);
        /* 
         * CSKF CALLBACK for INDEX KEY
         *
         * Call back to get the index key if necessary.
         * We *could* do this only in output_metarec,
         * avoiding excess callbacks for duplicates, but
         * if we do that, we have inconsistency between put_rec
         * and prime_record (one creates index key and the other
         * does not).  So it's (for the time being) safer to
         * do it this way. Later we can add an argument to output_metarec
         * to distinguish the cases, and move this code there. 
         */
        const sm_skey_t& sortkey = rec_curr->sort_key(0); 
        smsize_t        ikeysize=sortkey.size();
        DBG(<<"sortkey(0).size is " << ikeysize);

        CSKF lex_ikey = info.lexify_index_key();

        if((ikeysize > 0) &&
           // non-null real key
            (lex_ikey != sort_keys_t::noCSKF)) {

            DBG(<<" lexify " << rid);

            INC_STAT_SORT(sort_lexindx_cnt); 

            rec_curr->assert_consistent();

            // statically allocated : factory_t::none
            skey_t                indexkey;

            callback_prologue();
            W_DO( (*lex_ikey)(rid,
                object, 
                info.lexify_index_key_cookie(), 
                _scratch_space, // to populate indexkey if not from object
                &indexkey) );
            callback_epilogue();
            // does index_key_non_const().freespace, takes on indexkey's factory.
            rec_curr->index_key_non_const().replace(indexkey);
            rec_curr->assert_consistent();
        }  else {
            rec_curr->index_key_non_const().freespace();
            rec_curr->index_key_non_const().invalidate();
        }
        rec_curr->set_ikeysize(ikeysize);
    }


    /* 
     * Save metadata for large object if needed.
     * It's needed if 
     *   output is a copy of the file
     */
    if(info.is_for_file() && rec->is_large() && !info.deep_copy()) {
        DBG(<<" is_for_file, large, !deep_copy");
        char *                buffer = 0;
        smsize_t         length = rec_size(rec);

        /* 
         * Get enough space to save both the rectag_t and the
         * large-object metadata.
         * The rectag_t is stored before the 
         * hdr on the slotted page.
         *
         * In the meta_header_t.lgmetadata() we store both parts
         * as a SINGLE vector piece because we allocate only one
         * part to free.
         */
        smsize_t needed = length + sizeof(rectag_t);
        DBG(<<"get_buf for sort key ordinal=" << rec_curr->ordinal());
        W_DO(_scratch_space.get_buf(needed, buffer));
        w_assert3(buffer);

        rec_curr->lgmetadata_non_const().construct(buffer,
                                    0, needed, &_scratch_space);

        // Just make a raw copy of the large-object metadata
        memcpy(buffer, &rec->tag, sizeof(rectag_t));
        memcpy(buffer+sizeof(rectag_t), rec->body(), length);

        // put into persistent area length of body proper
        // since the size of the rectag_t is a compile-time constant.
        rec_curr->set_lgmetasize(needed);
        DBG(<<" lgmetasize= " << needed);
    } else {
        // wipe out any such size from prior use of meta_header_t
        // but keep the buffer around for future use.
        rec_curr->set_lgmetasize(0);
        DBG(<<" lgmetasize= " << 0);
    }

    DBG(<<"rec_curr->set(fp,slot), slot= " << slot);

    rec_curr->set(fp, slot);
#if W_DEBUG_LEVEL > 2
    if(rec_curr->index_key().is_valid()) {
        if(rec_curr->index_key().size() > 0) {
            if(rec_curr->index_key().is_in_obj() == false) {
                w_assert3(rec_curr->index_key().ptr(0) != 0);
            }
        }
    }
#endif

    // sanity check
    rec_curr->assert_consistent();

    rec_curr++;
    DBG(<<" rec_curr is now " << rec_curr);

#if W_DEBUG_LEVEL > 2
    {
                // << " key(0)=" << (void*)(m->sort_key(0).ptr(0)) 
        meta_header_t* m= rec_curr - 1;
        DBG(<<"end put: orig rec=" << 
                m->shrid(_ifid.store)
                << " len(0)=" << m->sort_key(0).size() 
                << " in_obj(0)=" << 
                        (char*)((m->sort_key(0).is_in_obj())?"true":"false") 
                << " offset(0)=" << m->sort_key(0).offset() 
                << " pieces(0)=" << m->pieces(0) 
                );
        if( (m->lgmetasize() > 0) && !info.deep_copy()) {
            const rectag_t *rectag =(const rectag_t *)(m->lgmetadata().ptr(0));
            w_assert3(rectag);
            w_assert3((rectag->flags & t_small) == 0);
        }
    }
#endif 
    DBG(<<"end put_rec fp.pid=" << fp.pid() << " slot " << slot);
    return RCOK;
}


void 
run_mgr::prologue() 
{
    // Reset key list ptr
    rec_curr = rec_first;
    DBG(<<" rec_curr is now rec_first: " << rec_curr);
}


void
run_mgr::epilogue()
{
    DBG(<<"run_mgr::epilogue");
    _clear_meta_buffers(false);
    _scratch_space.reset(); // free all the key space

}

tape_t* 
run_mgr::_next_tape() 
{
    DBG(<<"next_tape: next run is " << _next_run);
    // keep track of current run during sort phase
    // Called in each flush to determine where to write
    // the run.
    int j= ++_next_run;
    for(int i=0; i<_phase->order(); i++) {
        DBG(<<"j = " << j << ", subtracting " << _phase->num(i));
        j -= _phase->num(i);
        if(j <= 0) {
            tape_t *result = &_tapes[i];
            if(!result->primed_for_output()) {
                W_COERCE(result->prime_tape_for_output());
            }
            result->_checksd();
            DBG(<<"Found result tape " << i << " is fixed " <<
                    result->metafp().is_fixed());
            return result;
        }
    }
    // Must be done.
    w_assert3(_next_run == _phase->total());
    w_assert3(0);
    return 0;
}

w_rc_t
run_mgr::_output_metarec(
    meta_header_t*        m,
    tape_t*               t,
    rid_t&                result_rid
)
{
    me()->get___metarecs()++;
    t->inc_count_put();
    FUNC(run_mgr::_output_metarec);
    w_auto_delete_array_t<char>         autodeltmp;
    if(_aborted) return RC(eABORTED);

    DBG(<<"tape #" << t->_tape_number
            << " page-is-fixed=" << t->metafp().is_fixed());
    m->assert_consistent();

    w_assert3(t->primed_for_output());


    DBG(<<"tape #" << t->_tape_number
            << " page-is-fixed=" << t->metafp().is_fixed());
    {
        // copy info to the persistent part.
        meta_header_t *non_const = (meta_header_t *)m;
        non_const->unmarshal_sortkeys(); 
    }

    vec_t         hdr, data;
    data.reset();
    hdr.reset().put(m, m->persistent_size());

    /*
     * What we write to the tmp file is this:
     *
     * metadata -> hdr
     *   we statically know (from run_mgr.info) if this is for_index
     *   or for_file.
     * sort key(s) -> body[0...n]   ***UNLESS all keys compared in pieces.
     *   In that case, we'll do the comparisons in the
     *   buffer pool in both sort and merge phases.
     *   (We know from metadata how many of these there are and their lengths.)
     * AND
     *
     * Case for_index:
     *   output index key with lexify already applied -> body[n+1]
     *
     * Case for_file, !deep_copy (independent of carry_obj): large
     *          object metadata
     *
     * Case for_file, carry_obj:
     *   whole object hdr -> body[n+1]
     *   whole object body -> body[n+2]
     *
     * Case for_file, no carry_obj:
     *   nothing (will do it by re-pinning at end)
     *
     * This stuff is read back in prime_record()
     */

    bool carry_large_object = false;
    int body_parts=0;
    smsize_t body_len=0;
    for(int k=0; k < nkeys(); k++) {
       if( !m->pieces(k)) {
            // TODO: don't write out key if carry_obj and
            // key isn't derived, but is just offset into
            // that object we're writing out.
            // 
            const sm_skey_t &sortkey = m->sort_key(k);
            if(sortkey.size() > 0) {
                body_parts++;
                body_len +=  sortkey.size();
                data.put(sortkey.ptr(0), sortkey.size());
            }
       }
    }
    DBG(<<" sort keys fill " 
        << body_parts << " body parts " 
        << body_len << " bytes of body"
        );
    if(info.is_for_index()) {
        w_assert3(nkeys() == 1);
        DBG(<<"is_for_index: key size "  << m->ikeysize());
        if(m->ikeysize() > 0) {
            const sm_skey_t &ikey = m->index_key();
            body_parts++;
            body_len +=  ikey.size();
            /* We can safely assume that index key is not huge */
            if(ikey.is_in_large_obj()) {
                /* Have to allocate some space and copy it - argh */
                char *tmp = new char[ikey.size()];
                if(!tmp) {
                    return RC(smlevel_0::eOUTOFMEMORY);
                }
                // Delete space when we are done
                autodeltmp.set(tmp);
                vec_t v(tmp, ikey.size());
                W_DO(ikey.copy_out(v));
                data.put(v);
            } else {
                data.put(ikey.ptr(0), ikey.size());
            }
            DBG(<<" including index key, " 
                << body_parts << " body parts " 
                << body_len << " bytes of body"
                );
        }
    } else {
        // for file
        DBG(<<"is_for_file: lgmetasize="  
                << m->lgmetasize()
                << " lgmetadata.size()="
                << m->lgmetadata().size());

        if( (m->lgmetasize() > 0) && !info.deep_copy()) {
            // write object metadata 
            // what's copied into lgmetadata() is both rectag_t
            // AND lgdata
#if W_DEBUG_LEVEL > 2
            const rectag_t *rectag =(const rectag_t *)(m->lgmetadata().ptr(0));
            w_assert3((rectag->flags & t_small) == 0);
            w_assert3((rectag->flags & (t_large_0|t_large_1|t_large_2)) != 0);
#endif
            data.put(m->lgmetadata().ptr(0), m->lgmetadata().size());
            w_assert3( m->lgmetadata().size() > 0);
            body_parts ++;
            body_len +=  m->lgmetadata().size();
        }

        if(info.carry_obj()) {
            sm_object_t &object = m->whole_object_non_const();
            {
                /* UMOF CALLBACK */
                /* Prepare the object for writing to disk */
                lpid_t anon(ifid(), m->shpid());
                rid_t  rid(anon, m->slotid());
                UMOF unmarshal = info.unmarshal_func();
                if(unmarshal != sort_keys_t::noUMOF) {
                    // statically allocated. factory_t::none
                    sm_object_t newobject;
                    callback_prologue();
                    W_DO( (*unmarshal)(
                                rid, 
                                object, 
                                info.marshal_cookie(), 
                                &newobject) );
                    w_assert3( !object.is_in_buffer_pool());
                    callback_epilogue();
                    INC_STAT_SORT(sort_umof_cnt);
                    // does object.freespace, takes on new object
                    object.replace(newobject);
                }
            }
            if( object.hdr_size()> 0) {
                data.put(object.hdr(0), object.hdr_size());
                body_parts++;
                body_len +=  object.hdr_size();
                m->set_hdrsize(object.hdr_size());
            }
            if(object.body_size() > 0) {
                if(object.body_size() > object.contig_body_size()) {
                    carry_large_object = true;
                } else {
                    // Object is not large here, so body(0)
                    // is non-null
                    w_assert3(object.body(0) != 0 ||
                            object.body_size() == 0);
                    if(object.body_size() > 0) {
                        data.put(object.body(0), object.body_size());
                        body_parts++;
                        body_len +=  object.body_size();
                    }
                    m->set_bodysize(object.body_size());
                }
            }

            DBG(<<" including whole object, " 
                << body_parts << " body parts " 
                << body_len << " bytes of body"
                );
        }  
    }

    // Freespace is done later, after return from
    // calling this funcion.
    t->_checksd();

    DBG(<<"tape #" << t->_tape_number
            << " page-is-fixed=" << t->metafp().is_fixed());

    smsize_t size_hint = hdr.size() + data.size();
    W_DO ( fi->create_rec_at_end(
        t->metafp(),
        size_hint, hdr, data, 
        *(t->sdesc()), result_rid) );

    DBG(<<"Created tmp (OUTPUT METAREC) rec " << result_rid
        << " for original record " << m->shrid(_ifid.store)
        << " hdr.size() == " << hdr.size()
        << " data.size() == " << data.size()
        << " page is fixed == " 
        << (const char *)(t->metafp().is_fixed() ? "yes" : "no")
        ); 
    /*  Handle special case for carrying large object */
    if(carry_large_object) {
        W_FATAL(eNOTIMPLEMENTED); // yet
    }
    INC_STAT_SORT(sort_tmpfile_cnt); //# metarecs written
    ADD_TSTAT_SORT(sort_tmpfile_bytes, (hdr.size() + data.size()));

    return RCOK;
}

inline void
check_reclist_for_duplicates(meta_header_t * W_IFDEBUG2(l)[], 
        int W_IFDEBUG2(n))
{
#if W_DEBUG_LEVEL > 1
    // Slow and stupid
    for(int i=0; i < n; i++) {
        meta_header_t* m=l[i];
        for(int j=i+1; j < n; j++) {
            w_assert0(l[j] != m);
        }
    }
#endif
}
w_rc_t 
run_mgr::flush_run(bool flush_to_tmpfile)
{   
    me()->get___metarecs()=0;
    if(_aborted) return RC(eABORTED);
    {
        xct_t *_victim_ignored = 0;
        W_DO(xct_log_warn_check_t::check(_victim_ignored));
    }

    int nelements;
    // (Re-)create the key list 
    // Make the ptrs point to the meta_header_t items 
    // for _QuickSort
    {
        int i=0;
        meta_header_t* m=rec_first;
        for(; m < rec_curr; m++) {
            rec_list[i++] = m;
        }
        nelements = i;
    }
    w_assert1(nelements == (rec_curr - rec_first));
    if(_aborted) return RC(eABORTED);
    check_reclist_for_duplicates(rec_list, nelements);
    DBG(<<"flush_run nelements=" << nelements 
            << " flush_to_tmpfile=" << flush_to_tmpfile);
    _QuickSort( rec_list, nelements );
    check_reclist_for_duplicates(rec_list, nelements);

    if(flush_to_tmpfile) {
        tape_t *t = _next_tape();
        DBG(<<"flush to tmpfile on tape #" << t - _tapes);
        /* Write the run to the (next) temp file, 
         * and remember the rid of the first item in the run.
         *
         * Write only the rid + the following (we have 2 cases) for
         * each key:
         *  1) lexico and must compare in pieces : write the
         *           fact that we must compare in pieces
         *  2) other: write the key
         * The metadata go into the header, the key into the body.
         */


        file_p       last_page_written;
        rid_t        rid;
        bool         is_first = true;
        w_rc_t       rc;

        int i;
        int num_recs_output=0;
        int num_recs_elim=0;
        for(i=0; i < nelements; i++) {
            meta_header_t *m = rec_list[i];

            if(m->is_dup()) {
                if(info.is_unique() ||
                  (info.null_unique() && m->is_null())) {
                    num_recs_elim++;
                    DBG(<<"skip: ELIM eliminating orig rid= " << m->shrid(_ifid.store));
                    INC_STAT_SORT(sort_duplicates);
                    continue; // skip
                }
            }

            DBG(<<"**** about to output metarec to tape " << t->_tape_number
                    << " with page-is-fixed=" << t->metafp().is_fixed());
            rc = _output_metarec(m, t, rid);
            if(rc.is_error()) {
                DBG(<<"******************* got error from output_metarec");
                break; // out of loop so we can freespace
            }
            num_recs_output++;

            if(is_first) {
                /* Remember the id of the first record in the run */
                DBG(<<" first record of this run is " << rid 
                        << " from ordinal " << m->ordinal());
                t->add_run_first(rid.pid.page, rid.slot);
                is_first = false;
            }
        }
        for(i= nelements-1; i>=0; i--) {
            meta_header_t *m = rec_list[i];
            // DBG(<<"freespace i" << i);
            m->freespace(); // TODO: consider saving the space
        }

        w_assert0(num_recs_output + num_recs_elim == nelements); // should be after the next statement

        if(rc.is_error()) return rc;

        w_assert2(!is_first);
        // they cannot *all* be duplicates.  There
        // must be at least one.
        // rid could be first and last, though.
        
        DBG(<<" last record of this run is " << rid );
        t->add_run_last(rid.pid.page, rid.slot);
        t->release_page(t->metafp());
    }
    DBG(<<" this run: have output " << me()->get___metarecs() << " metarecs");
    return RCOK;
}


/* was static & not thread safe */
__thread long randx = 1; /* TODO : use other random methods we have */

/*
 * Called by flush_run 
 */

void 
run_mgr::_QuickSort( 
    meta_header_t*   a[], 
    int              cnt
)
{
    DBG(<<"_QuickSort " << cnt);
    /*
     * NB: Presently won't work for long longs, given standard 
     * comparison functions, since it uses 
     * integers for
     * comparisons.  
     * Key comparisons must return -1, 0, +1, not
     * the difference between the two keys.
     *
     * NOTE: the int8_cmp and uint8_cmp have been fixed to return
     * proper limited values.
     * So this should be ok now.
     */

    /* XXX If you change this to be > 30, use the dynamic allocation
          code instead of the on-the-stack code */
    const int MAXSTACKDEPTH = 30;
    const int LIMIT = 10;


    struct qs_stack_item {
        int l, r;
    };

    DBG(<<"new stack" << sizeof(qs_stack_item) * MAXSTACKDEPTH);
    qs_stack_item *stack = new qs_stack_item[MAXSTACKDEPTH];
    if (!stack) {
        DBG(<<"");
        W_FATAL(eOUTOFMEMORY);
    }
    record_malloc(MAXSTACKDEPTH * sizeof(qs_stack_item));

    // array, stack indexes 
    int sp = 0;
    int l, r;
    meta_header_t* tmp;
    int i, j;
    meta_header_t* pivot;
    int pivoti; // index of pivot

    for (l = 0, r = cnt - 1; ; ) {
        if (r - l < LIMIT) {
            if (sp-- <= 0) break;
            l = stack[sp].l, r = stack[sp].r;
            continue;
        }
        randx = (randx * 1103515245 + 12345) & 0x7fffffff;
        randx %= (r-l); // modulo difference
        DBG(<<"randx=" << randx << " modulus(r-l)=" << (r-l));

        pivoti = l+randx;
        if(pivoti==r) pivoti--; // avoid pivot pt at either end
        if(pivoti==l) pivoti++;

        pivot = a[pivoti];
        for (i = l, j = r; i <= j; )  {
            // if(a[i] == pivot) w_assert3(i == pivoti);
            while ((i != pivoti) && (_KeyCmp(a[i], pivot) < 0)) i++;

            // if(a[j] == pivot) w_assert3(j == pivoti);
            while ((pivoti != j) && (_KeyCmp(pivot, a[j]) < 0)) j--;

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
             j >= 0 && (_KeyCmp(pivot, a[j]) < 0);
             a[j+1] = a[j], j--) /* empty for*/ ;

    delete [] stack;
    record_free(MAXSTACKDEPTH * sizeof(qs_stack_item));

#if W_DEBUG_LEVEL > 1
    DBG(<<"_QuickSort verifying");
    for(int i=0; i < cnt-1; i++) {
        w_assert2(_KeyCmp(a[i], a[i+1]) <= 0);
    }
    DBG(<<"_QuickSort verify DONE");
#endif
    return;

error:
    delete [] stack;
    record_free(MAXSTACKDEPTH * sizeof(qs_stack_item));
    // not likely 
    cerr << "_QuickSort: stack too small" <<endl;
    DBG(<<"");
    W_FATAL(eOUTOFMEMORY);
}

#define PRINT_KEYCMP 0
/*
 * Class blob - deals with pinning and pin state of an object. 
 *
 * prime() sets it up to work with a particular key.
 * There are 2 cases: a) the key is used in pieces(),
 * in which case it is read from disk with a pin_i, and
 * b) the key has already been copied from a tmp record
 * into a mem buffer.  There are no other cases in which
 * blob is used.  Blob is used ONLY in keycmp() and in
 * prime_record() and in _output_index_rec().  
 * In prime_record() the meta_headers 
 * that it uses are all marked as pieces(). These are meta_headers
 * that describe the tmp record that we're going to read from
 * disk with the blob.
 * In key comparisons, there are 2 cases: first pass (before
 * anything's written to tmp files) and later passes.  During
 * the first pass, all the meta_headers have ptrs into the
 * buffer pool IFF the key in question fits entirely in the
 * small-object page.  The key is marked pieces() or is
 * copied to in-mem buffer otherwise.
 *
 * TODO: verify this in put_rec
 * In _output_index_rec() we are looking only at the index
 * key (what gets written to the output file).  It can be large.
 *
 */

class blob  : public w_base_t
{
    rid_t                  _rid1;
    const char*            _key1;
    pin_i                  _p1;
    int                    k; // active key
    smsize_t               _last1; // end of comparision
    smsize_t               _s1;        // start offset into record
    smsize_t               _ps1; // start -- offset into page
    smsize_t               _pl1; // pinned/available length
    smsize_t               _len1;

    int                    _save_pin_count;
    bool                   _in_large_object; // true -> we're
                                // scanning large pages
                                // else in header or in small object
                                // or in allocated mem;
    
public:
    blob(const rid_t& rid) : 
        _rid1(rid),
        _key1(0),
        k(-1),
        _last1(0), _s1(0), _ps1(0), _pl1(0), _len1(0), _save_pin_count(0),
        _in_large_object(false)
    {
    }

    ~blob() { _p1.unpin(); }

    smsize_t hdr_size() const {
        return _p1.hdr_size();
    }
    smsize_t body_size() const {
        w_assert3(_p1.pinned());
        return _p1.body_size();
    }
    void prime(const sm_skey_t &key);
    void next(const char *&key1, smsize_t& l);
    bool more() {
        return (_s1 < _last1)?true:false;
    }
    void consumed(smsize_t l) {
        _s1 += l;
        _len1 -= l;
    }

private:
                // for callback
    void          callback_prologue() const {
#if W_DEBUG_LEVEL > 2
                    /*
                     * leaving SM
                     */
                    // escape const-ness of the method
                    int *save_pin_count = (int *)&_save_pin_count;
                    *save_pin_count = me()->pin_count();
                    w_assert3(_save_pin_count == 0);
                    me()->check_pin_count(0);
                    me()->in_sm(false);
#endif 
                }
    void          callback_epilogue() const {
#if W_DEBUG_LEVEL > 2
                    /*
                     * re-entering SM
                     */
                    me()->check_actual_pin_count(_save_pin_count);
                    me()->in_sm(true);
#endif 
                }
        
};

inline void 
blob::prime(const sm_skey_t &key) 
{
    _len1 = key.size();
    // For our purposes, if key len is 0, treat as _in_large_object==false
    if(key.size() == key.contig_length()) _in_large_object = false;
    else _in_large_object = true;

    w_rc_t rc;
    if(_in_large_object) {
        callback_prologue();
        rc = _p1.pin(_rid1, key.offset(), SH);
        callback_epilogue();
        if(rc.is_error()) {
                W_FATAL_MSG(fcINTERNAL, << "Cannot pin " << _rid1
                    << " at offset " << key.offset() << endl << " rc=" << rc);
        }
        INC_STAT_SORT(sort_rec_pins);
        _last1 = _len1 + key.offset();
        // _s1 is starting offset into pinned pages of object
        // It moves from off(k) toward last
        _s1 = key.offset();
    } else {
        _key1 = (const char *)key.ptr(0);
        // key.offset() is buffer len, not offset
        // _len1 = key.size() is length of key
        _last1 = _len1 + 0; // offset into buffer is 0
        _s1 = 0;         // offset
    }
}

inline void 
blob::next(const char *&key1, smsize_t& l) 
{
    w_rc_t rc;
#if W_DEBUG_LEVEL > 2
    if(_s1 >= _last1) {
        // shouldn't get here
        w_assert3(0);
    } else 
#endif 
    if(_in_large_object) {
        _ps1 = _s1 - _p1.start_byte();
        _pl1 = _p1.length() - _ps1;
        if(_pl1 == 0) {
            //pin next page
            bool eof;
            DBG(<<"Getting next page of record " << _p1.rid());
            callback_prologue();
            rc = _p1.next_bytes(eof);
            callback_epilogue();
            if(rc.is_error()) {
                        W_FATAL_MSG(fcINTERNAL,
                        << "Cannot get next_bytes " << _rid1
                        << endl
                        << " rc= " <<rc);
            }
            INC_STAT_SORT(sort_rec_pins);
            _ps1 = _s1 - _p1.start_byte();
            _pl1 = _p1.length() - _ps1;
        }
        _pl1 = (_len1 < _pl1)? _len1 : _pl1; // min
        key1 = _p1.body() + _ps1;
        l = _pl1;

        DBG( <<"RID: rid " << _rid1 );
        DBG( <<"  START BYTES: "  << _s1);
        DBG( <<"  #PINNED BYTES left: " << _pl1 );
        DBG( <<"  START BYTES in pg: " << _ps1);
    } else {
        DBG( <<"RID: rid " << _rid1 );
        DBG( <<"  START BYTES: "  << _s1);
        DBG( <<"  BYTES left: " << _len1);
        key1 = _key1 + _s1;
        l = _len1;
    }
}
/*************************************************************************/

/*
 * Methods of tape_t
 */

void
tape_t::alloc_runs(int m)
{
    _maxruns = m;
    DBG(<<"new" << sizeof(run_t) * (_maxruns));
    _list = new run_t[_maxruns];
    record_malloc(sizeof(run_t)* _maxruns);
    if(!_list) { DBG(<<""); W_FATAL(eOUTOFMEMORY); }
}

void         
tape_t::init_vid( vid_t v) 
{
    DBG(<<" init_vid " << v);
#ifdef W_TRACE
    _tape_number = GET_TSTAT_SORT(sort_files_created);
#endif /* W_TRACE */
    INC_STAT_SORT(sort_files_created);
    _vol = v;
}

w_rc_t 
tape_t::_rewind(bool create_new)
{
    vid_t        v = _vol;
    if(_store != 0) {
        DBG(<<" destroying tmp file " << tmp_fid());
        W_DO( SSM->_destroy_file(tmp_fid()));
        // _destroy_file removes the store descriptor
    }
    _sd = 0;
    set_store(stid_t::null);
    if(create_new) {
        return  _create_tmpfile(v);
    }
    return RCOK;
}

w_rc_t 
tape_t::_create_tmpfile(vid_t v)
{
    stid_t        tmpfid;
    W_DO( SSM->_create_file(v, tmpfid, t_temporary) );
    DBG(<<" created tmp file " << tmpfid);
    w_assert3(_sd == 0);
    W_DO( dir->access(tmpfid, _sd, NL) );
    set_store(tmpfid);
    return  RCOK;
}

w_rc_t 
tape_t::prime_tape_for_output()
{
    DBG(<<"PRIME_TAPE_FOR_OUTPUT "  << _tape_number );
    w_assert2( ! metafp().is_fixed());
    w_assert1(is_empty());

    clr_count_put();
    clr_count_get();

    release_page(origfp());

    _first_run = -1;
    _last_run = 0;

    w_assert1(_maxruns > 0);

    /*
     * Blow away the old temp file and create a new one
     * so we don't suck up all the disk space.
     */
    W_DO (_rewind(true));
    // Make sure it has a fid and sdescriptor
    w_assert1(_store != 0);
    w_assert1(_sd != 0);

    // _meta can be null
    _primed_for_output = true;
    _primed_for_input = false;
    w_assert2( ! metafp().is_fixed());
    W_IFDEBUG3(check_tape_file(false));
    return RCOK;
}

void 
tape_t::prime_tape_for_input()
{
    DBG(<<"PRIME_TAPE_FOR_INPUT "  << *this );
    w_assert3( ! metafp().is_fixed());
    // could have dummy runs
    // w_assert1(!curr_run_empty());
    //
    // If we've were last writing to this tape, _first_run
    // will be -1.  If we've been reading from it,
    // leave _first_run alone; continue where we left off.
    if(_first_run < 0) _first_run = 0;
    W_IFDEBUG3(check_tape_file(false));
    clr_count_get();
}

void 
tape_t::prepare_tape_buffer(meta_header_t *m)
{
    w_assert3( ! metafp().is_fixed());
    _meta = m; // one in the list rec_first[]
    DBG(<<"PREPARE tape " << _tape_number << "  with meta_header struct");
#if W_DEBUG_LEVEL > 2
    m->assert_nobuffers();
#endif
}

/**\brief Prepare (for heap insertion) the object_t at the front of the tape.
 * @param[in] info Arguments to the sort_file
 * @param[in] R Needed for callback_prologue and callback_epilogue
 * @param[in] fact Preferred factory used; if fails to allocate, uses
 *             cpp_vector 
 *
 * Called in second and subsequent phases (heapsort/merge phases).
 *
 * Read in the next record from the tape; set up the _meta->object
 * to reference that record; allocate scratch space as necessary and
 * marshal the object.  In other words, do whatever we need to do
 * with this record (populating an sm_object_t for it) so that we can
 * do a key-comparison with it). Actually, we insert it into a heap,
 * which does the key comparison to maintain the heap condition.
 *
 * The populated sm_object_t is in tape_t::_meta->_whole_object.
 *
 * \note: if we marshalled in put_rec (Quicksort phase) but
 * didn't unmarshal into the temp files for the various runs,
 * we will have changed the marshal function here to noMOF
 * for this phase.  If user did provide an unmarshal function,
 * we'll have unmarshalled to the temp files and we'll re-marshal here.
 *
 */
w_rc_t 
tape_t::prime_record(
        const sort_keys_t &info,
        run_mgr &R,
        factory_t &fact // called with _scratch_space, preferred factory
        )
{
    DBG(<<"PRIME_RECORD_FOR_INPUT "  << *this);

    // We're done with old orig fp now.
    release_page(origfp());

    w_assert3(_meta);

    int nk = info.nkeys();

    // Read in this meta-record
    rid_t        _rid = meta_rid();
    DBG(<<" prime_record reading in meta_rid() " << _rid);
    lpid_t        old_pid;
    
    if(metafp().is_fixed()) {
        old_pid = metafp().pid();
    }

    if(old_pid.page != _rid.pid.page) {
        DBG(<<"Fixing metafp() with page " << _rid.pid);
        W_DO(metafp().fix(_rid.pid, LATCH_SH));
        w_assert1(metafp().tag() == page_p::t_file_p);
        INC_STAT_SORT(sort_page_fixes_2);
    }


    /*
     * read in meta data -- see how much buffer space
     * we need for keys.
     */
    int              k=0;
    record_t*        rec;
    slotid_t         slot = _rid.slot;

    DBG(<<" prime_record getting rec " << metafp().pid() << "." << slot );
    me()->get___metarecs_in()++;

    inc_count_get();

    W_DO(metafp().get_rec(slot, rec));
    w_assert3(rec->hdr_size() <= sizeof(meta_header_t));
    w_assert3(_meta); w_assert3(rec->hdr());
    memcpy(_meta, rec->hdr(), rec->hdr_size());
    w_assert3(rec->hdr_size() == _meta->persistent_size());

    DBG(<<"prime_record: got metadata for ordinal=" << _meta->ordinal());

    INC_STAT_SORT(sort_memcpy_cnt);
    ADD_TSTAT_SORT(sort_memcpy_bytes, _meta->persistent_size());

    /*
     * Allocate space for all the parts in this order:
     * whole_object()
     * sort keys
     * index key
     * large object metadata
     */
    
    /*
     * Set up initially invalid object descriptor
     */
    sm_object_t& object = _meta->whole_object_non_const();

    /* 
     * Allocate some space for the object.
     * Set up the object descriptor.
     * We won't read in the data until later, but
     * we want the space allocated first, because
     * we're trying to alloc space in same order in
     * put_rec and prime_rec,
     * so we can free in opposite order in output_metarec
     * and output_rec.
     */
    smsize_t header_length = _meta->hdrsize();
    smsize_t body_length = _meta->bodysize();
    smsize_t chars_read = 0;
    {
        /*
         * Allocate space in which to store the 
         * header & body that we read from the temp object.
         */
        char *buf = 0;
        smsize_t whole_len = 
                limited_space::_align(header_length) + 
                limited_space::_align(body_length);

        // See if we can re-use the space already allocated,
        // if any is there.
        factory_t *f = &fact;
        bool must_alloc = true;
        if(object.is_valid() && !object.is_in_buffer_pool()) {
            if((object.body_size() >= limited_space::_align(body_length))
             && (object.hdr_size() >= limited_space::_align(header_length)) )
            {
                // YES we can re-use the space
                must_alloc = false;
            } else {
                // Nope, cannot. Try to free it.
                object.freespace(); // is now invalid 
            }
        } 
        if(must_alloc) {
            DBG(<<"allocfunc for ordinal=" << _meta->ordinal() );
            buf = (char *)f->allocfunc(whole_len);
            if(buf==0) {
                f = factory_t::cpp_vector;
                buf = (char *)f->allocfunc(whole_len);
                if(buf==0) {
                    W_FATAL(eOUTOFMEMORY);
                }
            }
            object.construct_from_bufs(
                    // header allocated from f
                    buf, header_length, f,
                    // body is within header so don't deallocate
                    buf + limited_space::_align(header_length),
                    body_length, factory_t::none);
                    
        }
    }
    w_assert3(object.body(0) != 0);
    w_assert3(object.hdr(0) != 0);

    /*
     * Allocate space for the sort keys and fill them in.
     */
    smsize_t         offset = 0; // offset into tmp record
    {
        _meta->marshal_sortkeys(fact);  

        /*
         * Sort key space is allocated but not filled in.
         * In the case of pieces(k) for sort_key(k), 
         * the "where" ptr isn't set.  In the non-pieces case,
         * the copies haven't been made.
         */

        DBG(
            << this
            <<" :Read in tmp rec " << _rid
            << " for original record " << _meta->shpid() 
                    << "." << _meta->slotid()
            ); 

        /*
         * set up a meta-metaheader to describe this temp 
         * record so we can use a blob to read the data from
         * the record and stuff it into the space allocated.
         * For this to work, we the meta-metaheader has
         * all keys set up as pieces()
         */
        {
            /*
             * Do the sort keys
             */
            sm_object_t   metaobject(metafp(), slot);

            blob        blob(_rid);

            smsize_t klen = 0;  
            smsize_t offset_into_metaobject = offset;
            for(k=0; k < nk; k++) {
                sm_skey_t fakekey(metaobject, offset_into_metaobject, 
                            _meta->sort_key(k).size(),
                            false/*not in hdr*/);
                offset_into_metaobject += _meta->sort_key(k).size();

                /*
                 * If this key is ! pieces, it'll have a
                 * copy in the body of the metarecord
                 */
                if(_meta->pieces(k)) {
                    /*
                     * The key comparison function does
                     * everything in the buffer pool and doesn't
                     * need the "where" ptr.
                     */
                } else {

            // TODO: don't write out key if carry_obj and
            // key isn't derived, but is just offset into
            // that object we're writing out.
            // 
                    /*
                     * Read in the key from the body of the record
                     */
                    const sm_skey_t &sk = _meta->sort_key(k);
                    klen = sk.size();
                    DBG(<<"klen=" << klen);
                    w_assert3(rec->body_size() >= smsize_t(offset + klen));
                    if(klen > 0) {
                        char *buf = (char *)sk.ptr(0); // already allocated
                        const char *where = 0; // ptr into tmp rec
                        smsize_t pl1;

                        blob.prime(fakekey);

                        smsize_t kl = klen;
                        // keep track of length, since it can be 0
                        while( blob.more() && kl>0 ) {
                            blob.next(where, pl1);
                            w_assert3(buf); w_assert3(where);
                            if(pl1 > kl) pl1 = kl;
                            memcpy(buf, where, pl1);
                            INC_STAT_SORT(sort_memcpy_cnt);
                            buf += pl1;
                            blob.consumed(pl1);
                            chars_read += pl1;
                            kl -= pl1;
                        }
                        offset += klen;
                    }
                }
                    // deconstruct fakekey
            }
        }
    }

    if(info.is_for_index()) {
        w_assert3(! info.carry_obj());
        DBG(<<"ikeysize is " << _meta->ikeysize());
        sm_skey_t &ik = _meta->index_key_non_const();
        if(_meta->ikeysize() > 0) {
            /*
             * Make space for the index key.  The index key
             * is just about all that's left in the body of the metarecord.
             */
            smsize_t length = _meta->ikeysize();
            w_assert3(rec->body_size() - offset >= length);
            char *buffer = 0;
            factory_t *f = &fact;
            DBG(<<"allocfunc for ordinal=" << _meta->ordinal() );
            buffer = (char *)f->allocfunc(length);
            if(buffer==0) {
                f = factory_t::cpp_vector;
                buffer = (char *)f->allocfunc(length);
                if(buffer==0) {
                    W_FATAL(eOUTOFMEMORY);
                }
            }
            w_assert3(buffer);
            ik.construct((void *)buffer, smsize_t(0), length, f);

            /*
             * Do the index key - pretend it's key #0
             * in the blob meta
             */
            sm_object_t   metaobject(metafp(), slot);
            sm_skey_t     fakekey(metaobject, offset, 
                            _meta->index_key().size(), false/*not in hdr*/);
            offset += _meta->index_key().size();

            /* Create a blob to read the faked key from the meta object */
            blob          blob(_rid);

            /* Read in the index key from the body */
            blob.prime(fakekey);
            w_assert3(nk == 1);

            char *buf = (char *)ik.ptr(0); // in-mem ptr 
            const char *where = 0; // ptr into tmp rec
            smsize_t pl1;
            // keep track of length, since it can be 0
            while( blob.more() && length > 0) {
                blob.next(where, pl1);
                w_assert3(buf); w_assert3(where);
                if(pl1 > length) pl1=length;
                memcpy(buf, where, pl1);
                INC_STAT_SORT(sort_memcpy_cnt);
                buf += pl1;
                blob.consumed(pl1);
                chars_read += pl1;
                length -= pl1;
            }
            DBG(<< chars_read << " chars in index key" );
        } else {
            ik.freespace();
            ik.invalidate();
        }
    } else {
        /*
         * is_for_file()
         * 
         * If apropos, read in large-object metadata
         */
        smsize_t lgmeta_length = _meta->lgmetasize();
        DBG(<<"lgmeta_length is " << lgmeta_length);
        if(lgmeta_length) {
            w_assert3(! info.deep_copy());
            // The record described by the rec is large, not
            // the meta-rec!
            // w_assert3( rec->is_large() );
            char *buffer = 0;
            factory_t *f = &fact;
            DBG(<<"allocfunc for ordinal=" << _meta->ordinal() );
            buffer = (char *)f->allocfunc(lgmeta_length);
            if(buffer==0) {
                f = factory_t::cpp_vector;
                buffer = (char *)f->allocfunc(lgmeta_length);
                if(buffer==0) {
                    W_FATAL(eOUTOFMEMORY);
                }
            }
            w_assert3(buffer);
            _meta->lgmetadata_non_const().construct(buffer, 0, 
                    lgmeta_length, f);
            _meta->set_lgmetasize(lgmeta_length);
            DBG(<<" prime_record: lgmetasize=" << lgmeta_length);

            {
                /*
                 * Fake it to look like a key
                 */
                sm_object_t   metaobject(metafp(), slot);
                sm_skey_t     fakekey(metaobject, 
                                        chars_read, 
                                        lgmeta_length,
                                        false/*not in hdr*/);
                
                /* Create a blob to read the faked key from the meta object */
                blob        blob(_rid);

                const char *where = 0; // ptr into tmp rec
                const char *buf = buffer;
                smsize_t pl1;
                blob.prime(fakekey);
                while( blob.more()) {
                    blob.next(where, pl1);
                    w_assert3(buf); w_assert3(where);
                    memcpy((void *)buf, where, pl1);
                    INC_STAT_SORT(sort_memcpy_cnt);
                    buf += pl1;
                    blob.consumed(pl1);
                    chars_read += pl1;
                }
            }

#if W_DEBUG_LEVEL > 2
            const rectag_t *rectag =(const rectag_t *)buffer;
            w_assert3((rectag->flags & t_small) == 0);
            w_assert3((rectag->flags & (t_large_0|t_large_1|t_large_2)) != 0);
#endif
        } else {
            // wipe out any such size from prior use of meta_header_t
            // but keep the buffer around for future use.
            _meta->set_lgmetasize(0);
        }

        if (info.carry_obj()) {
            /* Read in the hdr and whole object from the 
             * body.  Fake out the blob.  Pretend the
             * hdr is key 0, body is key 1.
             */
            sm_object_t   metaobject(metafp(), slot);
            sm_skey_t     fakekey1(metaobject, offset, header_length,
                                                    false/*not in hdr*/);
            offset += header_length;

            sm_skey_t          fakekey2(metaobject, offset, body_length,
                                                    false/*not in hdr*/);

            /*
             * Now that the metablob is set up to read the hdr, body,
             * let's do it!  
             * Create a blob to read the faked keys from the meta object
             * and read into object.hdr(0)
             */
            blob        blob(_rid);

            const char *where = 0; // ptr into tmp rec
            const char *buf = (const char *)object.hdr(0);
            /*
             * Space was allocated above, so object.hdr(0)
             * had better not be null
             */
            w_assert3(object.hdr(0) != 0);
            smsize_t pl1;
            blob.prime(fakekey1);
            while( blob.more() ) {
                blob.next(where, pl1);
                w_assert3(buf); w_assert3(where);
                memcpy((void *)buf, where, pl1);
                INC_STAT_SORT(sort_memcpy_cnt);
                buf += pl1;
                blob.consumed(pl1);
                chars_read += pl1;
            }

            /* Now read the body into the object.body() */
            buf = (const char *)object.body(0);
            w_assert3(buf != 0);

            blob.prime(fakekey2);
            while( blob.more()) {
                blob.next(where, pl1);
                w_assert3(buf); w_assert3(where);
                memcpy((void *)buf, where, pl1);
                INC_STAT_SORT(sort_memcpy_cnt);
                buf += pl1;
                blob.consumed(pl1);
                chars_read += pl1;
            }
            //TODO: re-use space in the meta_header_t attached
            // to each tape, if possible
        } else {
            // Not for index & not carrying object along
            w_assert3(object.hdr_size() == 0);
            w_assert3(object.body_size() == 0);
        }
    }

    /*
     * Now that we've read in the object, we can call user's
     * marshal function if apropos.  
     */
    {
        /* MOF CALLBACK */
        lpid_t anon(R.ifid(), _meta->shpid());
        rid_t rid(anon, _meta->slotid());
        MOF marshal = info.marshal_func();
        if(marshal != sort_keys_t::noMOF) {
            // statically allocated. factory_t::none
            sm_object_t newobject;

            R.callback_prologue();
            W_DO( (*marshal)(rid, 
                        object, 
                        info.marshal_cookie(), 
                        &newobject) );
            R.callback_epilogue();
            INC_STAT_SORT(sort_mof_cnt);
            // does object.freespace, takes on newobject
            object.replace(newobject);
        } else if(info.carry_obj() && !info.deep_copy())  {
            // TODO: carry and !deep copy-->  and
            // !marshaled -> carry along only the metadata
            // Munge the object descriptor ??
            DBG(<<" ****************** TODO");
        }
    }
    _primed_for_input = true;
    _primed_for_output = false;
    W_DO(_next_meta_rid());

    ADD_TSTAT_SORT(sort_memcpy_bytes, chars_read);

    _meta->assert_consistent();

    return RCOK;
}

w_rc_t 
tape_t::pin_orig_rec(file_p& ifile_page, 
        stid_t &ifid, record_t *&rec, rid_t &r) 
{
    w_assert3(primed_for_input());

    r = rid_t(lpid_t(ifid.vol, ifid.store, _meta->shpid()), _meta->slotid());
    DBG(<<" pin_orig_rec for run with meta_rid " 
        << meta_rid()
        << " original rid = " << r
        );

    if( ! ifile_page.is_fixed() ) {
        // fix the page
        W_DO(ifile_page.fix(r.pid, LATCH_SH));
        INC_STAT_SORT(sort_page_fixes);
    } else {
        // fixed
        if( ifile_page.pid() != r.pid) {
            ifile_page.unfix();
            W_DO(ifile_page.fix(r.pid, LATCH_SH));
            INC_STAT_SORT(sort_page_fixes);
        } // else have right page
    }
    w_assert1(ifile_page.tag() == page_p::t_file_p);
    w_assert3(ifile_page.pid() == r.pid);
    w_assert3(ifile_page.nslots() > r.slot);
    W_DO(ifile_page.get_rec(r.slot, rec));
    return RCOK;
}

/*
 * Methods of run_mgr
 */

int    
run_mgr::_KeyCmp(const meta_header_t *_k1, const meta_header_t* _k2) const
{ 
    w_rc_t rc;
    if(_k1 == _k2) {
#ifdef PRINT_KEYCMP
        DBG(<<"KEYCMP: self");
#endif /* PRINT_KEYCMP */
        return 0;
    }

    INC_STAT_SORT(sort_keycmp_cnt);
    /* CF CALLBACK */
    callback_prologue();

    int partial_result = 0;
    const meta_header_t &k1 = *_k1;
    const meta_header_t &k2 = *_k2;

#ifdef PRINT_KEYCMP
    DBG(<<"KEYCMP "
        << " rid " << k1.shrid(_ifid.store)
        << " with " 
        << " rid " << k2.shrid(_ifid.store)
    );
#endif /* PRINT_KEYCMP */
    w_assert3(
        (k1.shpid() != k2.shpid()) || (k1.slotid() != k2.slotid()) 
        );

    int _nkeys = this->nkeys();

    for(int k=0; k<_nkeys; k++ ) {
        const sm_skey_t& kd1 = k1.sort_key(k);
        const sm_skey_t& kd2 = k2.sort_key(k);
        smsize_t  len1 = kd1.size();
        smsize_t  len2 = kd2.size();

#ifdef PRINT_KEYCMP
        DBG(<<"KEYCMP "
            << " len1 " << len1
            << " len2 " << len2
        );
#endif /* PRINT_KEYCMP */
        if(len1 == 0 || len2 == 0) {
            // one or more is null.  The longer (non-null)
            // one wins, unless, of course they are both null,
            // in which case they are ==
            partial_result = len1 - len2;
            if(partial_result != 0) {
                DBG(<<"partial_result = " << partial_result);
                goto done;
            } // else go on to check next key
        } else {
            // neither is null - have to compare keys
            CF cmp = this->_keycmp(k);

            
            rid_t        rid1(lpid_t(_ifid.vol, _ifid.store, 
                                    k1.shpid()), k1.slotid()); 
            rid_t        rid2(lpid_t(_ifid.vol, _ifid.store, 
                                    k2.shpid()),k2.slotid()); 
            blob        b1(rid1);
            blob        b2(rid2);

            const char *  key1;
            const char *  key2;
            smsize_t          pl1;
            smsize_t          pl2;

            b1.prime(k1.sort_key(k));  // tell what key interests us
            b2.prime(k2.sort_key(k));  // tell what key interests us

            while( b1.more()  && b2.more() ) {
                b1.next(key1, pl1);
                b2.next(key2, pl2);
                smsize_t len  = (pl1 < pl2) ? pl1 : pl2; // min

                DBG(<<"*cmp parts of len " << len);

                partial_result = (*cmp) (len, key1, len, key2);

                if(partial_result != 0) {
                    DBG(<<"partial_result = " << partial_result);
                    goto done;
                } 
                b1.consumed(len);
                b2.consumed(len);
            } // while
            if( b1.more()) {
                // key1 is longer, therefore greater
                partial_result = 1;
#ifdef PRINT_KEYCMP
                DBG(<<"key1 longer" ); 
#endif /* PRINT_KEYCMP */
                goto done;
            } else if (b2.more()) {
                // key2 is longer, therefore greater
                partial_result = -1;
#ifdef PRINT_KEYCMP
                DBG(<<"key2 longer" ); 
#endif /* PRINT_KEYCMP */
                goto done;
            } // else go on to next key
        }
    }

    if(partial_result == 0) {
#ifdef PRINT_KEYCMP
         DBG(<<"DUPLICATE " << partial_result );
#endif /* PRINT_KEYCMP */
        /* 
         * Duplicate! Keys match.
         *  if we're eliminating duplicates, mark one as a duplicate.
         *  if duplicates are allowed, sort on key,oid pairs. One might
         *  think this would yield a quasi-stable sort (iff the pages 
         *  were ordered to begin with), but we must sort with umemcmp,
         *  not a legit rid_t::operator== comparison (because the
         *  bulk-loaded btrees do that).
         */
        w_assert3((k1.shpid() != k2.shpid()) || (k1.slotid() != k2.slotid()));

        /* If we've been told this should be a stable sort (e.g.,
         * used NOT for btree index, but for order-by query)
         * let that override.
         * In either case, we must decide which of the 2 duplicates
         * comes first and which (the other one) gets "marked" as a duplicate.
         *
         * r <-- 1 if k1 "bigger"
         *      -1 if k2 "bigger"
         */
        int            r = 0; 
        bool           sort_by_rid = false;

        if(this->info.is_unique()  ||
          (this->info.null_unique() && k1.is_null())) {
           sort_by_rid = true;
           DBG(<<" unique or null_unique");
        } else if(info.is_stable()) {
           r = (k1.ordinal() > k2.ordinal())? 1 : -1;
           DBG(<<" stable");
        } else if(info.is_for_index()) {
            DBG(<<" for_index");
            // WHY do we care about this?
            // Because in the bulk-load code, it's assumed that
            // the objects appear in rid-order.  But if we're
            // using the output for bulk-loading, we should
            // specify for_index or sort on 1 key + value==2nd key,
            // and eliminate duplicates.

            /* 
             * Which has the larger rid?  Now, the problem here is
             * that we don't scramble the rids, so they are compared
             * (in the btree lookups) with byte-compares.  Thus, we
             * had better order them the same here.  This pretty-much
             * hoses any hope at sort-stability in this context.
             */
            sort_by_rid = true;

        } // else not stable, not for btree bulk-load
        // so we don't care. 
        //
        DBG(<<" sort_by_rid " << sort_by_rid);

        if(sort_by_rid) {
            r = umemcmp(&k1.shpid(), &k2.shpid(), sizeof(shpid_t));
            if(r==0) {
                w_assert2(k1.slotid() != k2.slotid());
                r = umemcmp(&k1.slotid(), &k2.slotid(), sizeof(slotid_t));
            } else {
                // a place for a gdb breakpoint
                DBG(<<"");
            }
        }

        if(this->info.is_unique() ||
          (this->info.null_unique() && k1.is_null())) {
            /* 
             * If we marked the 2nd one as a duplicate  -- this would work 
             * for QuickSort, but not for the merge, because the heap
             * compares things in "random" order, meaning that sometimes
             * it compares a,b and sometimes b,a; thus, everything 
             * could be marked duplicate.   
             * Instead, we mark the one with the larger rid.
             */

            // Sneak around const-ness
            meta_header_t *non_const = (meta_header_t *) ((r < 0) ? _k2 : _k1);
            non_const->mark_dup();

#ifdef PRINT_KEYCMP
            DBG(<<"MARK " << " orig rid= " << non_const->shrid(_ifid.store));
#endif /* PRINT_KEYCMP */
        } else {
           partial_result = r;
           DBG(<<"STABLE override, partial result now " << partial_result );
        }
    }
done:
#ifdef PRINT_KEYCMP
    DBG(<<"KEYCMP returning " 
            << partial_result << " is_ascending:" << info.is_ascending() );
#endif /* PRINT_KEYCMP */

    callback_epilogue();

    if (partial_result > 0) partial_result = 1;
    else if(partial_result < 0) partial_result = -1;
    // else partial result is 0;
    if(info.is_ascending()) return partial_result ;
    else return (0-partial_result);
}


/*
 * Called by _put_rec only
 */
w_rc_t
run_mgr::_prepare_key(
    sm_object_t&        object,
    int                 k, 
    file_p &            fp,
    const record_t&     rec,
    bool&               compare_in_pieces,
    sm_skey_t&          kdesc // in-out
)
{
    if(_aborted) return RC(eABORTED);
    /*
     * This function is called to figure out if we have
     * to make a copy of the key despite the fact that
     * it seems we could otherwise just compare in the
     * buffer pool. 
     * If we need to copy the key, we do. If possible, we
     * copy it into the preallocated bufffer.  If not, we
     * malloc the space and set the key_descriptor's free
     * function accordingly.
     */
    smsize_t         offset         = kdesc.offset();
    smsize_t         length         = kdesc.size();
    bool        in_hdr = info.in_hdr(k);

    bool         must_copy = false;
    bool         must_malloc = false;
    bool         all_on_one_page = true;

    compare_in_pieces = false;

    if(!in_hdr) {
        if(rec.is_large()) {
            smsize_t pgoffset=0;
            lpid_t pid1 = rec.pid_containing(offset, pgoffset, fp);
            lpid_t pid2 = rec.pid_containing(offset+length, pgoffset, fp);
            if(pid1!=pid2) {
                all_on_one_page = false;
            }
        }
    }
    if (all_on_one_page) {
        if( !info.is_aligned(k) ) {
            must_copy = true; // due to alignment (small rec)
        } else if(rec.is_small()) {
            must_copy = false; 
        } else {
            must_copy = true; // large object - we can't keep the pg fixed
            if(length <= _scratch_space.left()) {
                /* Fits into the buffer given */
            } else {
                must_malloc = true;
            }
        }
    } else {
        /* key is split across 2 or more pages */
        w_assert3(! rec.is_small());
        if(info.is_lexico(k)) {
            must_copy = false;
            compare_in_pieces = true;
            w_assert1(info.is_aligned(k));
        } else {
            must_copy = true; // split across two or more pages
            if(length <= _scratch_space.left()) {
                /* Fits into the buffer given */
            } else {
                /* doesn't fit, but can't compare in pieces - error */
                must_malloc = true;
            }
        }
    }
    DBG(<<"must_copy=" << must_copy
        << " compare_in_pieces=" <<  compare_in_pieces);

    char *buffer = 0;
    factory_t* fact  = factory_t::none;
    if(must_copy) {
        /* possible reasons:
         * (small or in_hdr), not aligned
         * (large, not lexico) since we can't keep the lg page pinned.
         *      Key might be split over 2 or more pages
         */
        if(must_malloc) {
            fact = factory_t::cpp_vector;
            buffer = (char *)fact->allocfunc(length);
        } else {
            fact = &_scratch_space;
            W_DO(_scratch_space.get_buf(length, buffer));
        }

        vec_t dest(buffer, length);
        W_DO(object.copy_out(false, offset, length, dest));
        kdesc.construct(buffer, 0, length, fact);
    } else {
        /* possible scenarios:
         * 1) small, aligned, all on one page
         * 2) in_hdr(hence, all on one page), aligned
         * 3) large, lexico 
         *
         * No need to do anything.
         */
        w_assert3(kdesc.is_in_obj());
    }
    DBG(<<"leave _prepare_key, keyloc=" << W_ADDR(buffer));

    // unpins lgpage TODO does not! it's an argument!!
    return RCOK; // unpins
}

w_rc_t
run_mgr::output_single_run(
    file_p*        fplist,                // list
    stid_t         ofid,
    bool&         swap
)
{
    if(_aborted) return RC(eABORTED);
    /* 
     * Move records from the input file 
     * to the output file based on the order in
     * the single in-memory run.
     */ 
    file_p*        fp;        

    SET_TSTAT_SORT(sort_runs, 1);

    sdesc_t* sd=0;
    W_COERCE( dir->access(ofid, sd, EX) );

    vec_t         hdr, data;
    file_p        last_page_written;
    record_t*     rec;
    meta_header_t* last_rec_written_key=0;

    int nelements = rec_curr - rec_first;
    DBG(<<"output single run with " << nelements << " items");
    for(int i=0; i < nelements; i++) {
        meta_header_t *m = rec_list[i];

        m->assert_consistent();
        if( (!m->is_dup()) &&  (info.is_unique() ||
                             (info.null_unique() && m->is_null())) 
          ) {
            // Compare with prior record.  Multiple Qsorts can 
            // miss duplicates, so we still have to do this
            // comparison.

            int j = -1;
            if(last_rec_written_key && (last_rec_written_key->shpid() != 0)) {
                DBG(<<"keycmp for DUP elimination");
                j = _KeyCmp(last_rec_written_key, m);
            }
            if(j==0) {
                meta_header_t *non_const = (meta_header_t *) m;
                non_const->mark_dup();  // NOW it is!
                DBG(<<"MARK " << m->shrid(_ifid.store));
                w_assert3(m->is_dup());
            }
        } 

        if(m->is_dup()) {
            DBG(<<"eliminating orig rid= " << m->shrid(_ifid.store));
            INC_STAT_SORT(sort_duplicates);
            continue; // skip
        }

        rid_t     rid(lpid_t(_ifid.vol, _ifid.store, m->shpid()), m->slotid());
        rec =     _rec_in_run(fplist, fp, m);

        DBG(<<"i " << i << "/" << nelements << ": outputting rid " << rid);

        if(info.is_for_index()) {
            DBG(<<"outputting to index " << ofid << " rid " << rid );
            W_DO(_output_index_rec(ofid, rid, m, rec, last_page_written, sd));
        } else {
            w_assert2(fp->pid().page == rid.pid.page);
            DBG(<<"outputting to file " << ofid << " rid " << rid );
            W_DO(_output_pinned_rec(ofid, rid, rec, 
                *fp, last_page_written, sd,
                swap));
        }
        m->assert_consistent();
        last_rec_written_key = m;
    }
    DBG(<<"");

    return RCOK;
}


/*
 * Called by output_single_run and _merge
 * to write the index info to the output file
 */
w_rc_t
run_mgr::_output_index_rec(
    stid_t&         W_IFDEBUG3(ofid),
    rid_t&        rid,
    const meta_header_t*        m,
    record_t*        ,        // TODO: use "rec"
    file_p&        last_page_written,
    sdesc_t*        sd         // cached
) const 
{
    if(_aborted) return RC(eABORTED);

    m->assert_consistent();

    w_rc_t        rc;
    vec_t        hdr, data;
    const sm_skey_t& ikey = m->index_key();
    char *        result = new char[ikey.size()];;
    /*
     * concatenate the keys --
     * had better not be big pieces()
     * since that wouldn't fit into a hdr anyway, but
     * small across-page-boundary pieces() are accommodated
     */
    { // only 1 key for index output
        if(ikey.is_in_obj()) {

            rid_t                tmprid(
                                    lpid_t(_ifid.vol, _ifid.store, m->shpid()),
                                                            m->slotid());
            blob                  b1(tmprid);
            const char *          key1;
            char *                r = result;                 
            smsize_t              pl1;

            b1.prime(m->index_key());

            // Entire key has to fit on a page (for indexing)
            // so we have to be able to do these copies
            w_assert3(ikey.size() <= ss_m::page_sz);

            while( b1.more()) {
                b1.next(key1, pl1);
                DBG(<<"key=" << 0 << " pl1=" << pl1);
                memcpy((void *)r, key1, pl1);
                r += pl1;
                b1.consumed(pl1);
            } // while
            w_assert3(smsize_t(r-result) == ikey.size());
            hdr.put(result, ikey.size());
        } else {
            hdr.put(ikey.ptr(0), ikey.size());
        }
    }

    if(!rc.is_error()) {
        /* 
         * get the oid 
         */
        data.reset().put(&rid, sizeof(rid));

        rid_t        newrid;
        smsize_t size_hint = hdr.size() + data.size();
        w_assert3(ofid ==  sd->stid());
        rc =  fi->create_rec_at_end( last_page_written,
                size_hint, hdr, data, *sd, newrid);
        if(rc.is_error() && rc.err_num() == eRECWONTFIT) {
            RC_PUSH(rc, eBADKEY);
        }
        if(!rc.is_error()) {
            DBG(<<"Created rec OUTPUT INDEX " << newrid
                << " hdr_size = " << hdr.size()
                << " body_size = " << data.size()
            );
            INC_STAT_SORT(sort_recs_created);
            ADD_TSTAT_SORT(sort_rec_bytes, size_hint);
        }
    }
    delete[] result;
    return rc;
}

/*
 * Called by output_single_run and _merge
 */
w_rc_t
run_mgr::_output_pinned_rec(
    stid_t&         W_IFDEBUG3(ofid),
    rid_t&        /*oldrid*/,        // not used
    record_t*        rec,
    file_p&        orig_rec_page,
    file_p&        last_page_written,
    sdesc_t*        sd,         // cached
    bool&        swap
) const 
{
    if(_aborted) return RC(eABORTED);

#ifdef W_TRACE
    if(!rec->is_small()) {
        DBG(<<"rec->tag.flags=" << rec->tag.flags);
        smsize_t start;
        lpid_t tmp = rec->pid_containing(0, start, 
                orig_rec_page);
        DBG(<<"first page of orig object =" << tmp);
    }
#endif

    vec_t        hdr, data;

    hdr.put(rec->hdr(), rec->hdr_size());

    smsize_t          reclen;
    smsize_t    size_hint = hdr.size();
    if( rec->tag.flags & t_small || !info.deep_copy() ) {
        /*
         * NB: large object !deep_copy is done below
         */
        reclen = rec_size(rec);
        // NB: rec_size gets size of the portion of the
        // body that sits in the slotted page. 
        data.put(rec->body(), reclen);
        size_hint += data.size();
    } else {
        reclen = smsize_t(rec->body_size());
        size_hint += reclen;
    }

    rid_t        newrid;
    w_assert3(ofid ==  sd->stid());
    W_DO ( fi->create_rec_at_end( last_page_written,
            size_hint, hdr, data, *sd, newrid) );

    DBG(<<"Created OUTPUT PINNED rec " << newrid
        << " hdr_size = " << hdr.size()
        << " body_size = " << data.size()
    );
    INC_STAT_SORT(sort_recs_created);
    ADD_TSTAT_SORT(sort_rec_bytes, size_hint);

    if( !(rec->tag.flags & t_small)) {
        if(info.deep_copy()) {
            // TODO: deep copy is implemented in a stupid and inefficient
            // way - it should be done deeply in lgrec without all
            // this appending going on in separate alloc_page and
            // append calls.  But for now, since noone is using deep
            // copy, we'll not put forth the effort.
            // 
            lgdata_p        lg;
            for(smsize_t offset = 0; offset < reclen;) {
                /* 
                 * Set up data vector to point into successive pages
                 * of the original rec
                 */

                smsize_t start_byte=0;
                lpid_t lgpid = rec->pid_containing(offset, start_byte, 
                                orig_rec_page);
                w_assert3(start_byte == offset);
                W_DO(lg.fix(lgpid, LATCH_SH));
                data.reset().put(lg.tuple_addr(0), lg.tuple_size(0));
                W_DO(fi->append_rec(newrid, data, *sd));
                offset += lg.tuple_size(0);
                lg.unfix();
            }
        } else {
            // shallow copy
            DBG(<<"shallow copy to " << newrid << " real len=" 
                << rec->tag.body_len << " slotted page part len=" << data.size());

            // large object: patch rec tag 
            W_DO( fi->update_rectag(newrid, 
                rec->tag.body_len, rec->tag.flags) );

#ifdef W_TRACE
        {
            record_t *rec;
            bool         didfix=false;
            if(! last_page_written.is_fixed()) { 
                W_DO(last_page_written.fix(newrid.pid, LATCH_SH));
                didfix=true;
            }
            W_DO(last_page_written.get_rec(newrid.slot, rec));
            if(rec->is_small()) {
                DBG(<<"new obj is small");
            } else  {
                smsize_t start;
                lpid_t tmp = rec->pid_containing(0, start,  
                    last_page_written);
                DBG(<<"first page of new obj =" << tmp);
            }
            if(didfix) last_page_written.unfix();
        }
#endif /* W_TRACE */
            swap = true;
        }
    }
    return RCOK;
}

/*
 * Called by _merge
 */

w_rc_t
run_mgr::_output_rec(
    stid_t&         W_IFDEBUG3(ofid),
    meta_header_t *m,        
    file_p&       last_page_written,
    sdesc_t*      sd,         // cached
    bool&         swap
) const 
{
    if(_aborted) return RC(eABORTED);
    m->assert_consistent();

    sm_object_t& object = m->whole_object_non_const();        
    const vec_t hdr(object.hdr(0), object.hdr_size());
    vec_t         data;

    smsize_t      reclen;
    smsize_t      size_hint = hdr.size();
    // NB: is_large is a misnomer, as the object could have been
    // carried along and be a large object with deep copy turned on.
    bool         is_large = (m->lgmetadata().size() > 0)?true:false;
    bool         must_do_deep_copy = false;

    const rectag_t*        rectag = 0; 
    if( is_large && !info.deep_copy() ) {
        /*
         * NB: large object !deep_copy is completed below
         */
        reclen = m->lgmetasize();
        const char *c = (const char *)m->lgmetadata().ptr(0);
        rectag =(const rectag_t *)c;
        w_assert3((rectag->flags & t_small) == 0);
        w_assert3((rectag->flags & (t_large_0|t_large_1|t_large_2)) != 0);
        c += sizeof(rectag_t);
        reclen -= sizeof(rectag_t);
        data.put(c, reclen);
        size_hint += data.size();
    } else {
        if(object.body_size() > object.contig_body_size()) {
            must_do_deep_copy = true;
        } else {
            /* Object is small or of size 0 or in mem */
            w_assert3(object.body(0) != 0 ||
                    object.body_size() == 0);
            if(object.body_size() > 0) {
                data.put(object.body(0), object.body_size());
            }
        }
        size_hint += object.body_size();
    }

    rid_t        newrid;
    w_assert3(ofid ==  sd->stid());

    W_DO ( fi->create_rec_at_end( last_page_written,
            size_hint, hdr, data, *sd, newrid) );

    DBG(<<"Created OUTPUT REC " << newrid
        << " hdr_size = " << hdr.size()
        << " body_size = " << data.size()
    );
    INC_STAT_SORT(sort_recs_created);
    ADD_TSTAT_SORT(sort_rec_bytes, size_hint);

    if( is_large) {
        w_assert3(rectag!=0);
        if(! info.deep_copy()) {
            // Finish shallow copy: set final body length
            // to the correct value, and stash flags

            // large object: patch rec tag 
            W_DO( fi->update_rectag(newrid, rectag->body_len, rectag->flags) );

#ifdef W_TRACE
        {
            record_t          *rec;
            bool         didfix=false;
            if(! last_page_written.is_fixed()) { 
                W_DO(last_page_written.fix(newrid.pid, LATCH_SH));
                didfix=true;
            }
            W_DO(last_page_written.get_rec(newrid.slot, rec));
            if(!is_large) {
                DBG(<<"new obj is small");
            } else  {
                smsize_t start;
                lpid_t tmp = rec->pid_containing(0, start,  
                    last_page_written);
                DBG(<<"first page of new obj =" << tmp);
            }
            if(didfix) last_page_written.unfix();
        }
#endif /* W_TRACE */
            swap = true;
        }
    }
    if(must_do_deep_copy) {
            return RC(eNOTIMPLEMENTED); // yet
    }
    return RCOK;
}


/* new sort - physical version */
rc_t                        
ss_m::sort_file(
    const stid_t&                   fid,         // input file
    const stid_t&                   sorted_fid, // output file -- 
                                            // created by caller--
                                            // can be same as input file
    int                             nvids,        // array size for vids
    const vid_t*                    vid,         // array of vids for temp
    sort_keys_t&                    kl, // key_location_t &
    smsize_t                        min_rec_sz, // for estimating space use
    int                             run_size,   // # pages to use for a run
    int                             tmp_space // # pages VM to use for scratch 
)
{
    SM_PROLOGUE_RC(ss_m::sort_file, in_xct, read_write, 0);

    w_rc_t rc = 
    _sort_file(fid,     sorted_fid, nvids, vid, 
                        kl, min_rec_sz, run_size,
                        tmp_space
                        );

    DBG(<<" returning from sort_file " << rc);
    return rc;
}



/*
 * Used in sort phase only
 * Given an array of file_p (frames), 
 * locate the record described by m in those frames.
 */
record_t *
run_mgr::_rec_in_run(
    file_p*                fp,// list
    file_p*&                fpout,// list
    meta_header_t*        m
) const
{
    record_t *rec;
    // Locate a record, given what we have stored in m 
    for(int i=0; i<_M; i++) {
            DBG(<<"_rec_in_run looking at i=" << i
                << " page " << fp[i].pid().page
                << " page has " << fp[i].nslots()
                << " slots"
                );
        if(fp[i].pid().page == m->shpid()) {
            w_assert3(fp[i].nslots() > m->slotid());
            DBG(<<" page " << fp[i].pid()
                << " slot " << m->slotid());
            w_rc_t rc = fp[i].get_rec(m->slotid(), rec);
            if(rc.is_error()) {
                W_COERCE(rc);
            }
            fpout = &fp[i];
            return rec;
        }
    }
    W_FATAL(eINTERNAL);
    return 0;
}

/* new sort internal, physical */
rc_t                        
ss_m::_sort_file(
    const stid_t&             ifid, // input file
    const stid_t&             ofid, // output file -- 
                              // created by caller--
                              // cannot be same as input file
    int                       nvids, // array size for vids
    const vid_t*              vids, // array of vids for temp
    sort_keys_t&              info1,  // key_location_t &
    smsize_t                  min_rec_sz, // for estimating space use
    int                       run_size,// #file pages to use for a run
                              // or merge -- max # buffer-pool
                              // pages we may hog
    int                       scratch_mem
)
{
    // Clear out ordinal for this sort.
    meta_header_t::clr_ordinal();

    FUNC(_sort_file);
    /*
     * Two-phase merge sort: we create as few runs as we can (R), with
     * runs of varying sizes (in # objects) - based on input run_size,
     * which tells max #buffer-pool page frames(M) we can use.  We use this
     * to determine the #pages we can read to form a single run.
     * Each "run" is quick-sorted in memory, written to a temp
     * file. (Now it is truly a run). We remember where each run starts by
     * keeping track of (page,slot) for its start and end.
     *
     * The number M also determines the number of logical "tapes" we can use
     * for an W=(M-1)-way merge, because we need a frame for each input tape
     * and a frame for the output tape.  In fact, if the objects are
     * are large and we can only do in-buffer-pool compares, we need
     * 2 frames per tape, so the merge is W=((M-1)/2) -way, and the
     * number of "tapes" is W+1.
     *
     * We use the Fibonacci numbers of order W-1 to determine the
     * ideal number of runs & the distribution of the runs onto the tapes.
     * We insert dummy runs as needed, and then do a polyphase merge sort
     * on the "tapes".
     */   

    int nkeys = info1.nkeys();

    /*
     * Sanity checks for arguments.
     */
    if(info1.is_for_index()) {
        if(info1.nkeys() > 1) {
            // Only support single-key indexes
            DBG(<<"");
            return RC(eBADARGUMENT);
        }
        if(info1.is_stable())  {
            // stable might violate rid-order 
            // for btree bulk-loading
            DBG(<<"");
            return RC(eBADARGUMENT);
        }

        // Output is not a copy of input.
        // These make no sense.
        if(info1.deep_copy()) {
            DBG(<<"");
            return RC(eBADARGUMENT);
        }
        //
        // We're indexing the file -have to keep it around
        if(!info1.keep_orig()) {
            DBG(<<"");
            return RC(eBADARGUMENT);
        }
        // NB: can only have one key for index output
        /* 
         * Choices for index key:
         * Sort key, as used by sort - noCSKF
         * Some other key - whether or not munged
         */
        if(info1.lexify_index_key()== sort_keys_t::noCSKF)  {
            //  index key is sort key
        } else {
            //  anything to check? TOOD
        }
    }
    if(info1.is_stable())  {
        if(info1.is_for_index()) {
            // stable might violate rid-order 
            // for btree bulk-loading
            DBG(<<"");
            return RC(eBADARGUMENT);
        }
    }
    if(ofid == ifid) {
        DBG(<<"");
        return RC(eBADARGUMENT);
    }
    for(int k=0; k<nkeys; k++) {
        if(! info1.is_fixed(k)) {
            // Must supply a CSKF
            if( (!info1.keycreate(k)) ||
                (info1.keycreate(k) == sort_keys_t::noCSKF)) {
                DBG(<<"");
                return RC(eBADARGUMENT);
            }
        } else {
            // fixed
            if(info1.keycreate(k) &&
                (info1.keycreate(k) != sort_keys_t::noCSKF)) {
                /*
                 * Why do we have this supplied? it won't be called
                 * DBG(<<"");
                 */
                return RC(eBADARGUMENT);
            }
        }

        /*
         * t_aligned: key location adequately aligned (relative
         *            to the beginning of the record, which is 4-byte aligned) 
         *         for in-buffer-pool comparisons with the given comparison
         *            function.  Copy to a contiguous buffer is unnecessary iff
         *            the entire key happens to be on one page (which is always
         *            the case with small records).   If a key is in a fixed location
         *        but not adequately aligned (e.g., always at offset 3 for int)
         *        a copy will be done.   
         *        Alignment has no effect on whether a CSKF is called.
         */

        /*
         * t_lexico: key can be spread across pages and  the comparison 
         *            function (CF) can be called on successive 
         *            segments of the key -- copy to a contiguous buffer unnecessary.
         *            There cannot be any alignment requirement, so aligned must
         *        be true.
         *        This *MUST* be used for large keys, any keys that can
         *        cross page boundaries and are "reasonably large" (read:
         *         larger than what fits in the scratch space given.)
         *      NB: keys that are very large cannot be used as index keys.
         */
        if(info1.is_lexico(k)) {
            if(! info1.is_aligned(k) ) {
                DBG(<<"");
                return RC(eBADARGUMENT);
            }
        }

        /*
         *  t_hdr: key is at offset in hdr rather than in body.  This means
         *        that large objects are never inspected for keys, and 
         *         in-buffer-pool comparisons can be made.  It is obviously
         *        fastest if fixed & aligned.
         */

        /*
         *  Key comparison function:
         *        required, period. 
         */
        if(!info1.keycmp(k)) {
            DBG(<<"");
            return RC(eBADARGUMENT);
        } else {
            smsize_t len=0;
            if( (info1.keycmp(k) == sort_keys_t::int8_cmp) ||
                    (info1.keycmp(k) == sort_keys_t::uint8_cmp))  {
                len = sizeof(w_base_t::int8_t);
            } else if( (info1.keycmp(k) == sort_keys_t::int4_cmp) ||
                    (info1.keycmp(k) == sort_keys_t::uint4_cmp))  {
                len = sizeof(w_base_t::int4_t);
            } else if( (info1.keycmp(k) == sort_keys_t::int2_cmp) ||
                    (info1.keycmp(k) == sort_keys_t::uint2_cmp))  {
                len = sizeof(w_base_t::int2_t);
            } else if( (info1.keycmp(k) == sort_keys_t::int1_cmp) ||
                    (info1.keycmp(k) == sort_keys_t::uint1_cmp))  {
                len = sizeof(w_base_t::int1_t);
            } else if (info1.keycmp(k) == sort_keys_t::f8_cmp) {
                len = sizeof(w_base_t::f8_t);
            } else if (info1.keycmp(k) == sort_keys_t::f4_cmp) {
                len = sizeof(w_base_t::f4_t);
            } else if(info1.keycmp(k) == sort_keys_t::string_cmp) {
#if 0
                // Not necessarily true. If isn't lexico, we might
                // lexify and THEN use string_cmp on the result.
                if(! info1.is_lexico(k)) {
                    DBG(<<"");
                    // String comparison should be marked as already
                    // lexicographically ordered
                    return RC(eBADARGUMENT);
                }
#endif
            }
            if(len && info1.is_fixed(k) && (info1.length(k) != len)) {
                DBG(<<"");
                return RC(eBADARGUMENT);
            }
        }
    }

    // Set swap_large_object_store if we noticed any large objects
    // while scanning the input file.
    bool swap_large_object_store = false;

    // Once means we don't have to mess with the polyphase merge
    // sort at all, because it all fits in the buffer pool at once.
    bool once=false;

    if(nvids <= 0) {
        DBG(<<"");
        return RC(eBADARGUMENT);
    }

    DBG(<<"run_size= " << run_size);

    int                M;
    int                NRUNS;
    int                NTAPES=0;
    {
        // determine M, W

        base_stat_t         pcount=0;
        base_stat_t         largepcount=0;
        {
            // Determine how many pages are used for the current file. 
            // If we can do this all in memory with only one run, we
            // can skip writing to a tmp file; we can write directly
            // to the output file.


            SmFileMetaStats file_stats;
            file_stats.smallSnum = ifid.store;
        
            W_DO(io->get_file_meta_stats(ifid.vol, 1, &file_stats));
            pcount = file_stats.small.numAllocPages;
            largepcount = file_stats.large.numAllocPages;
        } // end determine page counts
    
        if(largepcount>0) {
            run_size--;
                       // steal a page for large objects
                    // -- at least one is needed for run_mgr::put
                    // We might still use at least 2 more for 
                    // key comparisons, but those are not hogged.
            DBG(<<"run_size= " << run_size);
        }
        M = run_size - 1; // max # frames minus 1 for output of runs
                        // either to final file or to tmp files
        once = (int(pcount)<=M);
        DBG(<<"M= " << M);
        if (once) {
            DBG(<<"ONCE! Fits in one run");
            if(M < 1) {
                // Min run size is 3 if we need to merge
                DBG(<<"");
                return RC(eINSUFFICIENTMEM); 
            }
            if(M > int(pcount)) {
                M = int(pcount);
                DBG(<<"M= " << M);
            }
            nvids = 0;
        } else if(M < 2) {
            // (Need 2-way merge at a minimum)
            DBG(<<"NOT ONCE! Needs at least 2-way merge");
            return RC(eINSUFFICIENTMEM); 
        } 

        // NRUNS is total # runs we'll create in the quicksort phase
        NRUNS = int(pcount) / M;

        DBG(<<" pcount=" << pcount
            <<" M=" << M
            <<" NRUNS=" << NRUNS
                );

        if( (NRUNS * M) < int(pcount)) {
            NRUNS++;
            DBG(<<"NRUNS= " << NRUNS);
        }

        if(!once) {
            /*
             * We compute NTAPES here, based on the assumption that there
             * aren't any keys in large objects that must be 
             * compared in the buffer-pool during merge.  If we find that
             * there are such beasts, we'll recompute W and redistribute the
             * runs.
             */
            // max# frames - 1 for final output file.
            NTAPES = run_size; // (NTAPES-1)-way merges are done
        }
    }
    SET_TSTAT_SORT(sort_run_size, M);


    /* Convert scratch_mem to some close # pages */
    {
        int s = scratch_mem / ss_m::page_sz;
        s *= ss_m::page_sz;  
        if(s < scratch_mem) s+= ss_m::page_sz;
        scratch_mem = s;
    }
    limited_space scratch_space(scratch_mem);
    DBG(<<"scratch_space " << scratch_mem << " bytes");

    /*
     * We should allocate these in the run_mgr
     * constructor, but then we'd just have to
     * pass in the vid array and its size, so that
     * the tapes could be initialized with the
     * volume ids.
     */
    tape_t * tapes =0;
    if(NTAPES > 0) {
        DBG(<<"new tape_t[" << NTAPES << "] takes " << sizeof(tape_t) * NTAPES
                << " bytes"
                );
        tapes = new tape_t[NTAPES]; 
        if(!tapes) {
            DBG(<<"");
            W_FATAL(eOUTOFMEMORY);
        }
        record_malloc(NTAPES * tapes[0].size_in_bytes()); 

        int v = 0;
        for (int j=0; j<NTAPES; j++) {
            v = j % nvids;
            tapes[j].init_vid(vids[v]);
        }
        DBG(<<"new" << NTAPES * sizeof(tape_t));
    }

    {
        w_rc_t rc;
        run_mgr run(
            scratch_space,
            ifid, 
            M,         // max #pages in an initial run 
            NRUNS,        // # such runs in the file
            NTAPES,        // input + output
            tapes,
            min_rec_sz, 
            info1, 
            rc);
        if(rc.is_error()) return rc.reset();

        /*
         * Sort phase:
         * Read input file, fixing run_size pages at a time.
         * For each run, create a set of ptrs to the record keys,
         * then do a quick sort.
         */
        lgdata_p lgpage; // for large-object processing

        // array of pages that forms a run
        DBG(<<"new file_ps use " << M << " pages");
        file_p* fp = new file_p[M]; // auto-del
        if(!fp) {
            DBG(<<"");
            W_FATAL(eOUTOFMEMORY);
        }
        record_malloc(M * sizeof(file_p));
        w_auto_delete_array_t<file_p> auto_del_fp(fp);

        {
            lpid_t pid, first_pid;
            W_DO( fi->first_page(ifid, pid, NULL /* allocated only */) );
            first_pid = pid;
            int        run_number = 0;

            DBG(<<"first page is " << pid);

            bool eof;
            int  numrecords=0;
            for (eof = false; ! eof; )  {
                DBG(<<"{ run prologue"); /*}*/
                run.prologue();
                int numelements=0;
                for (int i=0; i<M && !eof; i++) {
                    {   /* check for log overrun */
                        xct_t *_victim_ignored = 0;
                        W_DO(xct_log_warn_check_t::check(_victim_ignored));
                    }
                
                    W_DO( fp[i].fix(pid, LATCH_SH) );
                    INC_STAT_SORT(sort_page_fixes);
                    DBG(<<"page " << pid << " contains "
                        << fp[i].nslots() << " slots (maybe not all full)" );
#if W_DEBUG_LEVEL > 1
                    { int k=0;
                        for (slotid_t j = fp[i].next_slot(0); 
                            j; 
                            j = fp[i].next_slot(j)) k++;
                        DBG(<<"page " << pid << " contains "
                        << k << " full slots" );
                    }
#endif 

                    /*
                     * for each object on the page
                     */
                    for (slotid_t j = fp[i].next_slot(0); 
                            j; 
                            j = fp[i].next_slot(j)) {
                        /*
                         * locate the keys, perform transformations
                         * on them, copy to buffer space if needed, etc.
                         * run_mgr::put_rec deals with all that.
                         */
                        W_DO(run.put_rec(fp[i], j)); 
                        numelements++;
                        numrecords++;
                    }
                    DBG(<<"get next page after pid=" << pid);
                    W_DO(fi->next_page(pid, eof, NULL /* allocated only*/));
                    INC_STAT_SORT(sort_page_fixes);
                 } // for run


                 // sort & flush each run
                 if(once && (++run_number > 1)) {
                    W_FATAL(eINTERNAL);
                 }
                 DBG(<<"once=" << once 
                         << " run_number=" << run_number
                         << " numelements=" << numelements
                         << " numrecords(input)=" << numrecords
                         );

                 // If we can't do this in one run, flush_run will
                 // send everything to a tmp file, one per run.
                 W_DO(run.flush_run(!once));

                 if(!once) {
                     run.epilogue();
                }
                /* {  */ DBG(<<"run epilogue }"); 
             } // for !eof
        } // sort phase

        DBG(<<"sort phase over ");

        UMOF unmarshal = info1.unmarshal_func();
        MOF marshal = info1.marshal_func();
        if((unmarshal == sort_keys_t::noUMOF) && (marshal != sort_keys_t::noMOF) ) {
            // We didn't use an unmarshal function
            // to write object to disk, so if we did
            // write anything to disk, we'd better read it
            // back in w/o applying the marshal function.
            // This only makes sense if we're not producing
            // the object in its entirety from carry_object,
            // i.e., we're producing for an index load, 
            // or we're going to pin the orig object (that really
            // makes no sense).
            info1.set_object_marshal(sort_keys_t::noMOF, sort_keys_t::noUMOF, 
                    key_cookie_t::null);
        }

        /* 
         * Merge phase
         * If we had more than one run, we've got to merge.
         * We re-use the meta_header_t* -- one for each run.
         */

        if (once) {
            W_DO(run.output_single_run(fp, ofid, swap_large_object_store));
            run.epilogue(); // clean up
            delete[] fp;
            auto_del_fp.set(0);
            record_free(sizeof(file_p) * M);

        } else {
            // free up the fixed pages.
            // We'll use another set in the merge.
            delete[] fp;
            auto_del_fp.set(0);
            record_free(sizeof(file_p) * M);

            W_DO(run.merge(ofid, swap_large_object_store ));
        }
    } // scope of run_mgr

    /* 
     * Destroy the input file if apropos.  
     */
    bool destroy = false;

    if(!info1.keep_orig()) {
        /* destroy input file */
        w_assert3(!info1.is_for_index());

        /* did we do a deep copy? */
        if(info1.deep_copy()) {
            /* yes - don't swap */
            destroy = true;
        } else {
            if (swap_large_object_store) {
                DBG(<<"destroy n swap");
                // ifid is old file, ofid is new file
                W_DO ( _destroy_n_swap_file(ifid, ofid) );
            } else {
                destroy = true;
            }
        }
    } else {
        /* keeping orig  -- don't destroy input file */
        w_assert3(info1.deep_copy() || info1.is_for_index());
    }
    if(destroy) {
        W_DO ( _destroy_file(ifid) );
    }

    DBG(<<" returning from sort_file");
    return RCOK;
}

w_rc_t
run_mgr::merge(
    stid_t         ofid,
    bool&         swap
)
{
    if(_aborted) return RC(eABORTED);

    w_assert3(_phase);
    w_assert3( ! _phase->done());

    DBG(<<" { BEGIN run_mgr::merge " << ofid << " swap=" << swap); /*}*/

    if(_recompute) {
        // For the moment, see if it works w/o a
        // space restriction
        w_assert1(0);

if(0) {  // TODO: implement this
        // There are some records that must be compared
        // in pieces in the buffer pool, so we have to
        // hog more buffer-pool pages.
        int newntapes=(_NTAPES-1)/2;
        // minimum of 2
        if (newntapes < 2) {
            DBG(<<"");
            return RC(eINSUFFICIENTMEM);
        }

        DBG(<<"RECOMPUTE new fib_t" << sizeof(fib_t));
        fib_t *fib = new fib_t(newntapes-1); // input only
        // record_malloc is in constructor
        if(!fib) {
            DBG(<<"");
            W_FATAL(eOUTOFMEMORY);
        }
        fib->compute(newntapes);
        fib->compute_dummies(newntapes);
        DBG(<<"RECOMPUTE new phase_mgr" << sizeof(phase_mgr));
        phase_mgr* newmgr = new phase_mgr(fib);
        // record_malloc is in constructor
        if(!newmgr) {
            DBG(<<"");
            W_FATAL(eOUTOFMEMORY);
        }

        // TODO:
        // redistribute runs from excess tapes to the
        // fewer ones that we now have
        // This means reallocating  run space on the tapes
        W_FATAL(eNOTIMPLEMENTED);

        delete _phase; // deletes its fib if necessary,
                // does record_free
        _phase = newmgr;
} // end "if(0)"
    }

    _phase->sort_phase_done(); 
        // sort_phase_done computed #dummy runs and adjusted num(t).
        // Now we have to add them to the tapes

    int t;

    for(t = 0; t < _phase->order(); t++) {
        tape_t *tp = &_tapes[t];
        int        runs = _phase->num(t) - tp->last_run();
        DBG(<<"Adding " << runs << " dummies to tape " << t);
        while( runs-- > 0 ) { tp->add_dummy_run(); }
    }

    w_assert3(_next_run > 1);
    SET_TSTAT_SORT(sort_runs, _next_run);

    DBG(<<"new runheap" << sizeof(RunHeap));
    RunHeap *runheap = new RunHeap(*this, _NTAPES);
    w_auto_delete_t<RunHeap> autodelheap(runheap);
    record_malloc(sizeof(runheap));

    SET_TSTAT_SORT(sort_ntapes, _NTAPES);
    SET_TSTAT_SORT(sort_phases, 0);

    /*
     * delete the meta_header_t that we're not going
     * to need, and the list of ptrs to them, now that
     * we're done with quicksort.  
     */
    DBG(<<"run_mgr::mid-merge");
    _clear_meta_buffers(true);

    w_assert3(_phase->order() == _NTAPES-1);

    rec_first = new meta_header_t[_NTAPES+1]; // a new set - different size

    /*
     * Need one meta_header_t for 
     * space to save key of last final record written
     * (for duplicate elim).  
     */
    meta_header_t *last_rec_written_key = rec_first; 
    /*
     * Give the tapes the meta_header_t structures to use.
     */
    meta_header_t *m = last_rec_written_key+1; 
    for(t = 0; t < _NTAPES; t++) {
        tape_t *tp = &_tapes[t];

        m->set_nkey(info.nkeys());
        w_assert3(m > last_rec_written_key);
        tp->prepare_tape_buffer(m);
        m++;
    }
    w_assert3((m > rec_first) && (m-rec_first == _NTAPES+1));


    /* 
     * do { merge phase } 
     * while(phase manager says not done) 
     */
    do{

        DBG( << _phase->way() << "-way merge:  TO " << _phase->target() );
        tape_t *tp;
        tape_t *tp_limit = _tapes + _NTAPES;

        tape_t*        output_tape = &_tapes[_phase->target()];
        W_DO(output_tape->prime_tape_for_output());
        w_assert1(output_tape->is_empty());

        for(tp = _tapes; tp < tp_limit;  tp++) {
            if(tp != output_tape)  {
                tp->prime_tape_for_input(); 
                DBG( << " from " << *tp);
                // Can be empty if we eliminated lots of dups
                // w_assert1(!tp->is_empty());
            }
        }

        /*
         * Do a phase : merge L runs from each of input tapes
         * to a string of L runs on the output tape.  L is the
         * smallest # runs on any of the tapes.  The phase
         * manager knows what that number is.
         */
        for (int runs=_phase->low(); runs > 0; runs--) {
            // runheap is empty
            w_assert3(runheap->NumElements() == 0);


            me()->get___metarecs_in() = 0;

            /* 
             * Prepare the first records on each tape, if there
             * is one (not a dummy run) and
             * stuff the top records into the heap
             */
            for(tp = _tapes; tp < tp_limit;  tp++) {
                if(tp != output_tape)  {
                    // Can be empty if we eliminated lots of dups
                    // w_assert1(!tp->is_empty());
                    if(!tp->is_empty() && !tp->is_dummy_run()) {
                        // This assumes that the run is not empty (dummy)
                        DBG(<<"prime_record on tp " << tp->get_store());
                        W_DO(tp->prime_record(info, *this, _scratch_space));
                        w_assert3(tp->real_shrid(_ifid.store).page!= 0);
                        W_DO(insert(runheap, tp, nkeys()));
                    }
                }
            }


            /*
             * Merge first run on each tape
             */
            DBG(<< "run " << runs <<": _merge first run " 
                    << int(tp_limit - _tapes) << " tapes, output tape=" << output_tape);
            W_DO( _merge( runheap, last_rec_written_key, 
                        _phase->last(), output_tape, ofid,  swap) );

            /*
             * Move on to next run on each tape except target
             */
            for(tp = _tapes; tp < tp_limit;  tp++) {
#if W_DEBUG_LEVEL > 1
                if(tp == output_tape)  {
                    DBG(<<" output tape  "  << *tp);
                    // Unless this is the last pass,
                    // output tape should not be empty.
                    // If last pass, it's still empty from 
                    // penultimate pass
                    if(_phase->last()) {
                        w_assert3(tp->is_empty());
                    } else {
                        // Could be empty if we have lots of duplicates
                        // w_assert3(!tp->is_empty());
                    }
                }
#endif 
                if(tp != output_tape)  {
                    // tp->is_empty() could be true earlier,
                    // if we have lots & lots of duplicates
                    DBG(<<" input tape  "  << *tp);
                    if(!tp->is_empty()) {
                        w_assert1(tp->curr_run_empty());
                        DBG(<< "************* ???? completed run #" << runs);
                        tp->completed_run(); // gets next run
                    }
                }
            } /* for all tapes, move on to next run */

            // runheap is empty
            w_assert3(runheap->NumElements() == 0);
        } // for runs

        last_rec_written_key->freespace();

#if W_DEBUG_LEVEL > 1
        {
            /* At this point, exactly one of the input tapes should be empty 
            * except in the last phase, all should be empty
            */
            int found = 0;
            for(tp = _tapes; tp < tp_limit;  tp++) {
                if(tp == output_tape)  {
                    if(_phase->last()) {
                        // it's still empty from penultimate pass
                        w_assert3(tp->is_empty());
                    } else {
                        // just wrote to it
                        // and we can't possibly have
                        // *everything* in the file be a dup,
                        // since we have to have at least one result
                        w_assert3(!tp->is_empty());
                    }
                } else {
                    // 
                    if(_phase->last()) {
                        // no more to read
                        w_assert3(tp->is_empty());
                    } else {
                        // more to read
                        if(found>0) {
                            w_assert3(!tp->is_empty());
                        }
                    }
                }
                if(tp->is_empty()) { found++; }
            }
            // Thus...
            if(!_phase->last()) {
                // One of the input tapes should be empty,
                // the output and the rest should not be empty
                w_assert3(found == 1);
            } else {
                // Would be ALL, since, in last phase, 
                // we copied the  orig recs to the final output file and
                // didn't use the tape.
                w_assert3(found == _NTAPES);
            }
        }
#endif 
        INC_STAT_SORT(sort_phases);

        _phase->finish_phase();
    } while (!_phase->done());

    w_assert3(runheap->NumElements() == 0);
    /*{*/DBG(<<"END run_mgr::merge " << ofid << " swap=" << swap << "}"
            );
    return RCOK;
}

w_rc_t
run_mgr::_merge(
    RunHeap*        runheap,
    meta_header_t *last_rec_written_key,
    bool           last_pass,
    tape_t        *target,
    stid_t         ofid,
    bool&          swap
)
{
    if(_aborted) return RC(eABORTED);
    me()->get___metarecs() = 0;

    /*
     * Merge: Pluck off the smallest record from the heads
     * of the runs.   Write the output to the target tape if !last_pass,
     * to the file identified by ofid if last_pass
     *
     * At the beginning of the each pass, we clear the info about the
     * last_rec_written_key - this forces us to write at least one
     * record for each pass, and avoids the problem of eliminating
     * all copies of some set of duplicates by comparing with
     * the last item written to a run other than the set we're
     * looking at.  It's less efficient in the case of 99% duplicates,
     * but it's safe.
     */
    last_rec_written_key->freespace(); // from prior calls
    last_rec_written_key->clear();
    last_rec_written_key->set_nkey(info.nkeys());
    w_assert3(last_rec_written_key->shpid() == 0);
    last_rec_written_key->assert_nobuffers();
    last_rec_written_key->assert_consistent();

    DBG(<<" { _merge: runheap->NumElements()=" << runheap->NumElements()); /*}*/ 
    if(last_rec_written_key) {
        DBG(<<" last_rec_written_key ordinal is " << last_rec_written_key->ordinal());
    }

    tape_t*        top;
    file_p&        last_page_written = target->metafp(); // of output file
    file_p         last_page_read;           // of original input file
    bool           is_first = true; // for marking output to target
    rid_t          newrid;

    sdesc_t*         sd=0;
    if(last_pass) {
        W_COERCE( dir->access(ofid, sd, EX) );
    }

    int nrecords_processed = 0;

    while(runheap->NumElements() > 0) {
        // Find smallest item, pluck it off, 
        DBG( << " nrecords_processed " << nrecords_processed
                <<" runheap->NumElements()=" << runheap->NumElements()
            );

        top = runheap->RemoveFirst();

        nrecords_processed++;
        const meta_header_t *tm = top->meta();
        w_assert3(tm > last_rec_written_key);

        DBG(<<"top/tm is consistent: " << tm->ordinal());
        tm->assert_consistent();
        last_rec_written_key->assert_consistent();

        if( (!tm->is_dup()) && 
            (info.is_unique() ||
            (info.null_unique() && tm->is_null()) ) ) {
            // Compare with prior record.  Heap management misses
            // a lot of duplicates, so we still have to do this
            // comparison.

            int j = -1;
            DBG(<< "last_rec_written_key->shpid() = " << 
                last_rec_written_key->shpid()
                << "." << 
                last_rec_written_key->slot()
                );
            if(last_rec_written_key->shpid() != 0) {
                DBG(<<"keycmp for DUP elimination");
                j = _KeyCmp(last_rec_written_key, tm);
            }
            if(j == 0) {
                meta_header_t *non_const = (meta_header_t *) tm;
                non_const->mark_dup();  // NOW it is!
                DBG(<<"MARK " << tm->shrid(_ifid.store));
                w_assert3(tm->is_dup());
            } else {
                // If this is the first in a new set of runs,
                // it could be false
                w_assert3(j<0 || is_first);
            }
        } else {
            DBG(<<"not uniq case " << tm->ordinal());
        }

        if(!tm->is_dup()) {
            DBG(<<"not duplicate " << tm->ordinal());

            meta_header_t *non_const = (meta_header_t *) tm;
            non_const->assert_consistent();
            last_rec_written_key->assert_consistent();

            if(last_pass) {
                lpid_t pid(ifid(), tm->shpid());
                rid_t  orig(pid, tm->slotid());
                // Write the record represented by top
                // to the output file.  
                // last_page_read is the original file page
                DBG(<<"LAST PASS ordinal:" << tm->ordinal());

                if(info.is_for_index()) {
                    DBG(<<"OUTPUT INDEX REC orig "  << orig << " ordinal " << tm->ordinal() );
                    W_DO(_output_index_rec(ofid, orig, tm, 
                        0,/* record_t */
                        last_page_written, sd));

                } else {
                    /* info.is_for_file()
                     * Case 1:
                     *  !deep_copy and large object
                     *  write out the meta data, regardless whether
                     *  we've been carrying along something.
                     * (no unmarshal needed)
                     * Case 2:
                     * small object or deep copy of large object,
                     *  carry:
                     * unmarshal, then write object
                     * Case 3:
                     * small object or deep copy of large object,
                     *  nocarry:
                     *  pin orig object, no marshal - just copy 
                     */
                    if((tm->lgmetadata().size() > 0) && !info.deep_copy()) {
                        // Case 1. Object is large.  Whether or not
                        // we are carrying the object along, we are
                        // copying only the metadata, so we don't have
                        // to mess with unmarshal.
                        // 
                        // output_rec handles shallow copy
                        DBG(<<"OUTPUT REC orig "  << orig << " ordinal " << tm->ordinal() );
                        W_DO(_output_rec(ofid, non_const, last_page_written, 
                          sd, swap));
                    } else {
                            // Cases 2, 3: object is small or
                        // it's large and needs a deep copy.
                        // See if we need to unmarshal:
                        if(info.carry_obj()) {
                            /* UMOF CALLBACK */
                            /* Prepare the object for writing to disk */
                            UMOF unmarshal = info.unmarshal_func();
                            if(unmarshal != sort_keys_t::noUMOF) {
                                sm_object_t&obj = 
                                    non_const-> whole_object_non_const();

                                // statically allocated: factory_t::none
                                sm_object_t newobj;

                                callback_prologue();
                                W_DO( (*unmarshal)(orig, 
                                    obj,
                                    info.marshal_cookie(), 
                                    &newobj) );
                                callback_epilogue();
                                INC_STAT_SORT(sort_umof_cnt);

                                // does obj.freespace(), takes on newobj
                                obj.replace(newobj);

                                /* NB: might not be consistent now */
                                non_const->assert_consistent();
                                // TODO: what to do about this?
                                DBG(<<"what to do here???");
                            }
                            DBG(<<"OUTPUT REC orig "  << orig << " ordinal " << tm->ordinal() );
                            W_DO(_output_rec(ofid, non_const, 
                                    last_page_written, sd, swap));
                        } else {
                            /* We didn't carry the object along,
                             * and we're doing a deep copy/shallow copy of small
                             * rec,  so we have to pin the original record and 
                             * copy it.
                             */
                            record_t*        rec;
                            rid_t            _orig;
                            W_DO(top->pin_orig_rec(last_page_read, _ifid, rec, _orig));
                            DBG(<<"OUTPUT PINNED REC orig "  << orig << " ordinal " << tm->ordinal() );
                            W_DO(_output_pinned_rec(ofid, _orig, rec, 
                                    last_page_read, last_page_written, sd,
                                    swap));
                        } // !carry obj
                    } // case 2, 3
                } // info.is_for_file
            } else {
                DBG(<<"OUTPUT METAREC not last pass");
                /*
                DBG(<<"**** about to output metarec to tape " << target->_tape_number
                    << " with page-is-fixed=" << target->metafp().is_fixed());
                */
                W_DO(_output_metarec(non_const, target, newrid));
                if(is_first) {
                    /* Remember the id of the first record in the run */
                    target->add_run_first(newrid.pid.page, newrid.slot);
                    is_first = false;
                }
            }
            last_rec_written_key->assert_consistent();
            non_const->assert_consistent();

            last_rec_written_key->move_sort_keys(*non_const);
            DBG(<<"SAVED rid for dup chk " 
                    << last_rec_written_key->shrid(_ifid.store));
            w_assert3(! tm->is_dup());

            non_const->freespace(); // TODO: consider saving the space
            non_const->assert_consistent(); 

        } else {
            // skip writing the record
            DBG(<<"skip: ELIM rid= " << tm->shrid(_ifid.store));
            INC_STAT_SORT(sort_duplicates);
            meta_header_t *non_const = (meta_header_t *) tm;
            non_const->freespace(); // TODO: consider saving the space
            non_const->assert_consistent(); 
        } 

        // get its next rid, put that into the heap
        if( top->curr_run_empty() ) {
            // Don't have to mess with the heap
            top->release_page(top->metafp());
        } else {
            // there's another record in the run --
            // stuff it into the heap
            DBG(<<"prime_record on tp " << top->get_store());
            W_DO(top->prime_record( info, *this, _scratch_space)); 
                        // fill in the metadata and bump meta-rid
            DBG(<<"pushing onto heap tp " << top->get_store());
            W_DO(insert(runheap, top, nkeys()));
            w_assert3(runheap->NumElements() > 0);
        }
        DBG(<<"end of loop");
    }
    /*{*/DBG(<<"_merge : nrecords_processed =" << nrecords_processed 
            << " metarecs read in " << me()->get___metarecs_in()
            << " metarecs out " << me()->get___metarecs()
            << "}");
    if(!last_pass) {
        // Either we've written something to the output tape
        // or all we've seen are duplicates and we've eliminated
        // them.
        w_assert3(!is_first ||
            (last_rec_written_key->shpid() != 0) );
        if(!is_first) {
            //  we've written something to the output tape 
            target->add_run_last(newrid.pid.page, newrid.slot);
            target->release_page(target->metafp());
        }
    }
    return RCOK;
}


/*
 * NB: Heap sorts so that top has the "highest"
 * object wrt this function, gt.  So we reverse
 * the meaning of gt so that the lowest is at
 * the top of the heap.  We'd do the normal thing
 * if we wanted a descending sort.
 */
bool
run_mgr::gt(const tape_t* a, const tape_t* b) const 
{
    // Called from Heapify.
    // Heap expects us to return true iff a > b
    // and the heap normally sorts in descending order
    //
    // Now, _KeyCmp returns normal expectation (j>0 if a>b)
    // if we're doing an ascending sort; the reverse if
    // descending.
    // The Heap normally does a descending sort, so we simply
    // return the reverse of what Heap expects, always.

    int j;
    if(a==b) {
        j = 0; 
        w_assert0(0);
    } else {
        // _KeyCmp marks 1 as duplicate if apropos
        j = _KeyCmp(a->meta(), b->meta());
        //  j is  ( <0,  ==0,  >0) if
        //        (a<b, a==b, a>b), respectively
    }
    DBG(<<"gt: reversing: return " << ((j <=0)?"true":"false"));
    // don't return true if == 0 - that causes unnecessary swaps
    // return (j <= 0) ? true : false; 
    return (j < 0)  ? true : false;  
}

#ifdef OLDSORT_COMPATIBILITY
/*
 * For compatibility with old sort:
 */
w_rc_t 
stringCSKF(
    const rid_t&         ,
    const object_t&        obj,
    key_cookie_t         cookie,  // interpreted as a Boolean value,
    // true meaning that the key is in the header 
    factory_t&                 ,
    skey_t*                 out
)
{
    bool in_hdr = (cookie.make_ptr() == key_cookie_t(1).make_ptr());
    // For this test, we assume that the 
    // entire hdr or body is the string key.

    smsize_t length = in_hdr? obj.hdr_size() : obj.body_size();
    new(out) skey_t(obj, 0, length, in_hdr);

    return RCOK;
}

struct hilbert_cooky {
    bool in_hdr;
    nbox_t* universe;
};

static w_rc_t 
hilbert (
    const rid_t&        ,  // record id
    const object_t&         obj,
    key_cookie_t            cookie,  //  type info for whole object
    factory_t&             internal,
    skey_t*                out
) 
{
    struct hilbert_cooky *h = (struct hilbert_cooky *)cookie.make_ptr();
    w_assert1(obj.is_in_buffer_pool());

    // length of output
    smsize_t length = sizeof(int);

    nbox_t box(2);
    smsize_t klen = box.klen();

    nbox_t& universe = *(h->universe);
    // The data form a box. Materialize it.

    if(h->in_hdr) {
        const char *ptr = (const char *)obj.hdr(0);
        box.bytes2box(ptr, klen);
    } else {
            char *ptr = new char[klen]; // klen isn't big
        if(!ptr) {
            return RC(smlevel_top::eOUTOFMEMORY);
        }
        vec_t v(ptr, klen);
        w_rc_t rc = obj.copy_out(false,0,klen, v);
        box.bytes2box((const char *)ptr, klen);
        delete[] ptr;
        if(rc.is_error()) return rc.reset();
    }
    int hvalue = box.hvalue(universe);

    void *buf = internal.allocfunc(length);
    memcpy(buf, &hvalue, length);
    // buffer length matches key length
    new(out) skey_t(buf, 0, length, internal);

    return RCOK;
}


rc_t                        
ss_m::new_sort_file(
    const stid_t&             in_fid, 
    vid_t                     out_vid, 
    stid_t&                   out_fid, 
    store_property_t          property,
    const key_info_t&         ki, 
    int                      run_size,
    bool                     ascending,
    bool                     unique,   // = false,
    bool                     destructive //  = false,
) 
{
    // Do NOT enter SM
    /* create output file */
    W_DO(create_file(out_vid, out_fid, property));

    W_DO(_new_sort_file(in_fid, 
                        out_fid,
                        ki,
                        run_size, 
                        ascending,
                        unique, 
                        !destructive));
    return RCOK;
}

rc_t                        
ss_m::_new_sort_file(
    const stid_t&             in_fid, 
    const stid_t&             out_fid, 
    const key_info_t&         ki, 
    int                       run_size,
    bool                      ascending, 
    bool                      unique,
    bool                      keep_orig
) 
{
    // Do NOT enter SM
    key_info_t            kinfo = ki; 
    sort_keys_t           kl(1); // 1 key

    key_cookie_t          cookies(0);
    nbox_t                _universe_(kinfo.universe);

    CF cmp = sort_stream_i::get_cmp_func(kinfo.type, true);

    struct hilbert_cooky C;
    C.in_hdr = (kinfo.where == key_info_t::t_hdr)?true:false;

    /* Convert key_info_t to sort_keys_t */
    if(kinfo.type == sortorder::kt_spatial) {
        /* spatial -- convert to i4 sort w/ derived key (Hilbert value) */
        kinfo.len = sizeof(int);
        C.universe = &_universe_;

        cmp = sort_keys_t::int4_cmp;
    }
    cookies =  key_cookie_t(&C);

    if(kinfo.type == sortorder::kt_spatial) {
        kl.set_sortkey_derived(0,
            hilbert,
            cookies,
            C.in_hdr,
            true,  // aligned
            true,  // lexico
            cmp
        );
    } else if(kinfo.type == sortorder::kt_b) {
        // We don't know the length, so we'll have to
        // call a CSKF for these keys.
        kl.set_sortkey_derived(0,
            stringCSKF,
            key_cookie_t(C.in_hdr?1:0),
            C.in_hdr,
            true,  // aligned
            true,  // lexico
            cmp
        );
    } else {
        kl.set_sortkey_fixed(0,
            kinfo.offset, 
            kinfo.len,
            C.in_hdr,
            true, // aligned
            false, // lexico
            cmp
        );
    }

    if( kl.set_ascending(ascending) // default
                || kl.set_unique(unique) ) {
        return RC(eBADARGUMENT);
    }

    if (kl.set_null_unique(false)) {
            // there's not really anything comparable -
        // so cut out the excess keycmps
        return RC(eBADARGUMENT);
    }

    // If !unique, set stable  to true, ow make sure it's 
    // false so the key cmp func will return 0 for matching keys.
    // kl.set_stable(!unique);
    // Override: old sort doesn't gurarantee sort stability
    kl.set_stable(false);

    // args are deep_copy, keep_orig, carry_obj

    (void) kl.set_for_file(keep_orig, keep_orig,  false);   // CARRY
        // last arg:
        // true -> carry object
        // false --> repin & copy object at end

    w_assert3(!kl.is_for_index());

    W_DO(sort_file(in_fid, 
            out_fid, 
            1, &out_fid.vol,
            kl,
            kinfo.est_reclen,
            run_size,
            ss_m::page_sz * run_size // essentially unlimited for now
        ));
    DBG(<<" returning from new_sort_file");

    return RCOK;
}
#endif /* OLDSORT_COMPATIBILITY */

//
// Comparison function for strings 
// Compares bytes as unsigned quantities
//
int 
sort_keys_t::string_cmp(
        uint4_t klen1, const void* kval1, uint4_t klen2, const void* kval2)
{
    unsigned char* p1 = (unsigned char*) kval1;
    unsigned char* p2 = (unsigned char*) kval2;
    // i is the counter and goes down, but
    // the strings are inspected from the front to the rear
    int result = 0;
    // i is the length of the shorter string
    uint4_t i = klen1 < klen2 ? klen1 : klen2;

    for (;
         i > 0 &&                // haven't hit the end of the shorter string
         ! (result = *p1 - *p2); // while strings are equal
         i--, p1++, p2++) ;
    // If one string is a subset of the other, 
    // consider the longer one to be the larger.
    return result ? result : klen1 - klen2;
}


//
// Comparison function for 8-byte unsigned integers
//
int sort_keys_t::uint8_cmp(uint4_t W_IFDEBUG3(klen1), const void* kval1,
                           uint4_t W_IFDEBUG3(klen2), const void* kval2)
{
    w_assert3(klen1 == klen2);
    w_assert3(klen1 == sizeof(w_base_t::uint8_t));
    // a - b can overflow: use comparison instead
#ifdef STRICT_INT8_ALIGNMENT
    w_base_t::uint8_t        u1;
    w_base_t::uint8_t        u2;
    memcpy(&u1, kval1, sizeof(u1));
    memcpy(&u2, kval2, sizeof(u2));
    ADD_TSTAT_SORT(sort_memcpy_cnt, 2);
    ADD_TSTAT_SORT(sort_memcpy_bytes, 2*sizeof(w_base_t::uint8_t));
#else
    w_assert3(((ptrdiff_t)kval1 & ALIGN_MASK_IU8) == 0);
    w_assert3(((ptrdiff_t)kval2 & ALIGN_MASK_IU8) == 0);
    const w_base_t::uint8_t        &u1 = *(const w_base_t::uint8_t *) kval1;
    const w_base_t::uint8_t        &u2 = *(const w_base_t::uint8_t *) kval2;
#endif
    bool ret =  u1 < u2;
    return ret ? -1 : (u1 == u2) ? 0 : 1;
}

//
// Comparison function for 8-byte integers
//
int sort_keys_t::int8_cmp(uint4_t W_IFDEBUG3(klen1), const void* kval1,
                          uint4_t W_IFDEBUG3(klen2), const void* kval2)
{
    w_assert3(klen1 == klen2);
    w_assert3(klen1 == sizeof(w_base_t::int8_t));
    // a - b can overflow: use comparison instead
#ifdef STRICT_INT8_ALIGNMENT
    w_base_t::int8_t        i1;
    w_base_t::int8_t        i2;
    memcpy(&i1, kval1, sizeof(i1));
    memcpy(&i2, kval2, sizeof(i2));
    ADD_TSTAT_SORT(sort_memcpy_cnt, 2);
    ADD_TSTAT_SORT(sort_memcpy_bytes, 2*sizeof(w_base_t::int8_t));
#else
    w_assert3(((ptrdiff_t)kval1 & ALIGN_MASK_IU8) == 0);
    w_assert3(((ptrdiff_t)kval2 & ALIGN_MASK_IU8) == 0);
    const w_base_t::int8_t        &i1 = *(const w_base_t::int8_t *) kval1;
    const w_base_t::int8_t        &i2 = *(const w_base_t::int8_t *) kval2;
#endif
    bool ret =  i1 < i2;
    return ret ? -1 : (i1 == i2) ? 0 : 1;
}

//
// Comparison function for 4-byte unsigned integers
//
int sort_keys_t::uint4_cmp(uint4_t W_IFDEBUG3(klen1), const void* kval1, 
                           uint4_t W_IFDEBUG3(klen2), const void* kval2)
{
    w_assert3(klen1 == klen2);
    w_assert3(klen1 == sizeof(w_base_t::uint4_t));
    bool ret = (* (w_base_t::uint4_t*) kval1) < (* (w_base_t::uint4_t*) kval2);
    return ret? -1 :
        ((* (w_base_t::uint4_t*) kval1) == (* (w_base_t::uint4_t*) kval2))? 0:
        1;
}
//
// Comparison function for 4-byte integers
//
int sort_keys_t::int4_cmp(uint4_t W_IFDEBUG3(klen1), const void* kval1,
                          uint4_t W_IFDEBUG3(klen2), const void* kval2)
{
    w_assert3(klen1 == klen2);
    w_assert3(klen1 == sizeof(w_base_t::int4_t));
    // a - b can overflow: use comparison instead
    // return (* (w_base_t::int4_t*) kval1) - (* (w_base_t::int4_t*) kval2);

    bool ret =  (* (w_base_t::int4_t*) kval1) < (* (w_base_t::int4_t*) kval2);
    return ret? -1 : 
    ((* (w_base_t::int4_t*) kval1) == (* (w_base_t::int4_t*) kval2) )? 0 : 1;
}
//
// Comparison function for 2-byte unsigned integers
//
int sort_keys_t::uint2_cmp(uint4_t W_IFDEBUG3(klen1), const void* kval1,
                           uint4_t W_IFDEBUG3(klen2), const void* kval2)
{
    w_assert3(klen1 == klen2);
    w_assert3(klen1 == sizeof(w_base_t::uint2_t));
    return (* (w_base_t::uint2_t*) kval1) - (* (w_base_t::uint2_t*) kval2);
}

//
// Comparison function for 2-byte integers
//
int sort_keys_t::int2_cmp(uint4_t W_IFDEBUG3(klen1), const void* kval1,
                          uint4_t W_IFDEBUG3(klen2), const void* kval2)
{
    w_assert3(klen1 == klen2);
    w_assert3(klen1 == sizeof(w_base_t::int2_t));
    return (* (w_base_t::int2_t*) kval1) - (* (w_base_t::int2_t*) kval2);
}

//
// Comparison function for 1-byte unsigned integers and characters.
// NB: we use unsigned 1-byte compare for characters because the
// native char comparison on some machines is unsigned, some signed,
// and we want predictable results here.
//
int sort_keys_t::uint1_cmp(uint4_t W_IFDEBUG3(klen1), const void* kval1,
                           uint4_t W_IFDEBUG3(klen2), const void* kval2)
{
    w_assert3(klen1 == klen2);
    w_assert3(klen1 == sizeof(w_base_t::uint1_t));
    return (* (w_base_t::uint1_t*) kval1) - (* (w_base_t::uint1_t*) kval2);
}

int sort_keys_t::int1_cmp(uint4_t W_IFDEBUG3(klen1), const void* kval1,
                          uint4_t W_IFDEBUG3(klen2), const void* kval2)
{
    w_assert3(klen1 == klen2);
    w_assert3(klen1 == sizeof(w_base_t::int1_t));
    // return (* (w_base_t::int1_t*) kval1) - (* (w_base_t::int1_t*) kval2);
    //
    w_base_t::int1_t *k1 = (w_base_t::int1_t*) (kval1);
    w_base_t::int1_t *k2 = (w_base_t::int1_t*) (kval2);
    int res = *k1 - *k2;
    res =  (res < 0) ? -1 : (res == 0) ? 0: 1;
    DBG(<<"int1_cmp " << int(*k1) << " " << int(*k2) << " res " << res);
    return res;
}

//
// Comparison function for floats
//
int sort_keys_t::f4_cmp(uint4_t W_IFDEBUG3(klen1), const void* kval1,
                        uint4_t W_IFDEBUG3(klen2), const void* kval2)
{
    w_assert3(klen1 == klen2);
    w_assert3(klen1 == sizeof(w_base_t::f4_t));
    // w_base_t::f4_t tmp = (* (w_base_t::f4_t*) kval1) - (* (w_base_t::f4_t*) kval2);
    // a - b can overflow: use comparison instead
    bool res = (* (w_base_t::f4_t*) kval1) < (* (w_base_t::f4_t*) kval2);
    return res ? -1 : (
        ((* (w_base_t::f4_t*) kval1) == (* (w_base_t::f4_t*) kval2))
        ) ? 0 : 1;
}

int sort_keys_t::f8_cmp(uint4_t W_IFDEBUG3(klen1), const void* kval1,
                        uint4_t W_IFDEBUG3(klen2), const void* kval2)
{
    w_assert3(klen1 == klen2);
    w_assert3(klen1 == sizeof(w_base_t::f8_t));

#ifdef STRICT_F8_ALIGNMENT
    w_base_t::f8_t d1;
    w_base_t::f8_t d2;
    memcpy(&d1, kval1, sizeof(w_base_t::f8_t));
    memcpy(&d2, kval2, sizeof(w_base_t::f8_t));
    ADD_TSTAT_SORT(sort_memcpy_cnt, 2);
    ADD_TSTAT_SORT(sort_memcpy_bytes, 2*sizeof(w_base_t::f8_t));
#else
    w_assert3(((ptrdiff_t)kval1 & ALIGN_MASK_F8) == 0);
    w_assert3(((ptrdiff_t)kval2 & ALIGN_MASK_F8) == 0);
    const w_base_t::f8_t &d1 = *(const w_base_t::f8_t *) kval1;
    const w_base_t::f8_t &d2 = *(const w_base_t::f8_t *) kval2;
#endif

    bool ret =  d1 < d2;
    return ret ? -1 : (d1 == d2) ? 0 : 1;
}

/*
 * Default lexify functions - get the help of SortOrder
 */

#include <lexify.h>

w_rc_t sort_keys_t::f8_lex(const void *d, smsize_t , void * res){
    w_base_t::f8_t dbl;
    memcpy(&dbl, d, sizeof(w_base_t::f8_t));
    SortOrder.dbl_lexify(dbl, res, SortOrder.Dperm);
    return RCOK;
}
w_rc_t sort_keys_t::f4_lex(const void *d, smsize_t , void * res){
    SortOrder.float_lexify(*(w_base_t::f4_t *)d, res, SortOrder.Fperm);
    return RCOK;
}
w_rc_t sort_keys_t::u8_lex(const void *d, smsize_t , void * res){
    SortOrder.int_lexify(d, false, 8, res, SortOrder.I8perm);
    return RCOK;
}
w_rc_t sort_keys_t::i8_lex(const void *d, smsize_t , void * res){
    SortOrder.int_lexify(d, true, 8, res, SortOrder.I8perm);
    return RCOK;
}
w_rc_t sort_keys_t::u4_lex(const void *d, smsize_t , void * res){
    SortOrder.int_lexify(d, false, 4, res, SortOrder.I4perm);
    return RCOK;
}
w_rc_t sort_keys_t::i4_lex(const void *d, smsize_t , void * res){
    SortOrder.int_lexify(d, true, 4, res, SortOrder.I4perm);
    return RCOK;
}
w_rc_t sort_keys_t::u2_lex(const void *d, smsize_t , void * res){
    SortOrder.int_lexify(d, false, 2, res, SortOrder.I2perm);
    return RCOK;
}
w_rc_t sort_keys_t::i2_lex(const void *d, smsize_t , void * res){
    SortOrder.int_lexify(d, true, 2, res, SortOrder.I2perm);
    return RCOK;
}
w_rc_t sort_keys_t::u1_lex(const void *d, smsize_t , void * res){
    SortOrder.int_lexify(d, false, 1, res, SortOrder.I1perm);
    return RCOK;
}
w_rc_t sort_keys_t::i1_lex(const void *d, smsize_t , void * res){
    SortOrder.int_lexify(d, true, 1, res, SortOrder.I1perm);
    return RCOK;
}

/*
 * generic_CSKF: either copies or lexifies a key.
 * It takes the cookie to be a ptr to a generic_CSKF_cookie,
 * which tells it which lexify function to call (if any),
 * and also tells it the length and offset of the key.
 * One normally expects the user to provide the entire
 * function for this, but we have this generic version just
 * for simplifying the handling of basic types for backward
 * compatibility.
 */
w_rc_t 
sort_keys_t::generic_CSKF(
    const rid_t&    ,
    const object_t& in_obj,
    key_cookie_t    cookie,  // type info
    factory_t&      internal,
    skey_t*         out
)
{
    generic_CSKF_cookie *c = (generic_CSKF_cookie *)cookie.make_ptr();
    if(c->length <= 0) { return RCOK; }

    /* LEXFUNC CALLBACK */
    LEXFUNC  lex;
    lex = c->func;

    /* If the key is entirely accessible and we don't have to 
     * lexify it, just create the key as an offset into the object.
     * If we have to lexify it, and it's accessible, we simply
     * lexify from the object into a new buffer.  If we have
     * to lexify and the object crosses page boundaries, we 
     * copy out and then lexify.
     */

    //  Create a key for this object as offset into object 
    new(out) skey_t(in_obj, c->offset, c->length, c->in_hdr);
    if(lex != noLEXFUNC) {
       // have to lexify
        char  *buf = (char *)internal.allocfunc(c->length);

        if(out->contig_length() == out->size()) {
            // it's all accessible at once

            W_DO((*lex)(out->ptr(0), c->length, buf));
        } else {
            // not all accessible - copy out
            void *tmp = (void *)new char[c->length];
            if(!tmp) {
                    return RC(smlevel_0::eOUTOFMEMORY);
            }
            vec_t v(tmp, c->length);
            W_DO(out->copy_out(v));
            W_DO((*lex)(tmp, c->length, buf));
        }

        // re-initialized skey_t: buffer length matches key length
        new(out) skey_t(buf, 0, c->length, internal);
    }
    return RCOK;
}

w_rc_t
generic_MOF(
    const rid_t&        ,
    const object_t&         obj_in,
    key_cookie_t        ,
    object_t*                    obj_out
)
{
    /* just for the sake of testing, let's just say that 
     * all we do here is copy to memory.  factory 
     * guarantees 8-byte alignment.  
     */
    factory_t &fact = *factory_t::cpp_vector;

    smsize_t hdrlen = obj_in.hdr_size();
    smsize_t bodylen = obj_in.body_size();

    char *hdrbuf=0, *bodybuf=0;
    if(hdrlen>0){
        hdrbuf = (char *)fact.allocfunc(hdrlen);
        if(!hdrbuf) return RC(smlevel_0::eOUTOFMEMORY);
        memcpy(hdrbuf, obj_in.hdr(0), hdrlen);
    }
    if(bodylen>0){
        bodybuf = (char *)fact.allocfunc(bodylen);
        if(!bodybuf) return RC(smlevel_0::eOUTOFMEMORY);
        vec_t v(bodybuf, bodylen);
        W_DO(obj_in.copy_out(false, 0, bodylen, v));
    }
    new (obj_out) object_t( hdrbuf, hdrlen, fact,
                        bodybuf, bodylen, fact);
    return RCOK;
}

w_rc_t
generic_UMOF(
    const rid_t&        rid,
    const object_t&         obj_in,
    key_cookie_t        cookie,
    object_t*                    obj_out
)
{
    return generic_MOF(rid, obj_in, cookie, obj_out);
}

    
w_rc_t 
sort_keys_t::
noLEXFUNC (const void *from, smsize_t howmuch , void *to) 
{
    memcpy((void *)to, (const void *)from, howmuch);
    return RCOK;
}

w_rc_t 
sort_keys_t::noCSKF(
    const rid_t&        ,
    const object_t&           ,
    key_cookie_t        ,  // type info
    factory_t&                ,
    skey_t*
)
{ 
    w_assert3(0); 
    return RCOK; 
}

w_rc_t 
sort_keys_t::noMOF (
        const rid_t&         ,  // record id
        const object_t&        ,
        key_cookie_t        ,  // type info
        object_t*
)
{ w_assert3(0); return RCOK; }

w_rc_t 
sort_keys_t::noUMOF (
        const rid_t&         ,  // orig record id of object in buffer
        const object_t&        ,
        key_cookie_t        ,  // type info
        object_t*
)
{ 
    w_assert3(0); 
    return RCOK; 
}

/***************************************************************************/

NORET 
object_t::object_t() :
    _valid(false),
    _in_bp(false),
    _rec(0),
    _fp(0),
    _hdrfact(factory_t::none),
    _hdrlen(0),
    _hdrbuf(0),
    _bodyfact(factory_t::none),
    _bodylen(0),
    _bodybuf(0)
{ }

void  
object_t::_construct(file_p& fp, slotid_t slot)
{
    _valid = true;
    _in_bp = true;
    _rec = 0;
    W_COERCE(fp.get_rec(slot, _rec));
    _hdrlen = _rec->hdr_size();
    _bodylen = _rec->body_size();
    _fp = &fp;
}

void 
object_t::_construct(const void *hdr, smsize_t hdrlen, factory_t* hf,
        const void *body, smsize_t bodylen, factory_t* _bf) 
{
   _valid=true; 
   _in_bp=false;
   _rec = 0;
   _fp=0;
   _hdrfact=hf; _hdrlen=hdrlen; _hdrbuf=hdr;
   _bodyfact=_bf; _bodylen=bodylen; _bodybuf=body;
}

NORET object_t::~object_t() 
{
    // _rec = 0;
}

void 
object_t::_replace(const object_t& other)
{
    freespace();
    assert_nobuffers();

    _valid = true;
    _in_bp = other._in_bp;
    _rec = other._rec;
    _fp = other._fp;
    _hdrfact = other._hdrfact;
    _bodyfact = other._bodyfact;
    _hdrbuf = other._hdrbuf; 
    _bodybuf = other._bodybuf; 
    _hdrlen = other._hdrlen; 
    _bodylen = other._bodylen;
    _save_pin_count = 0;
}

const void *
object_t::hdr(smsize_t offset) const
{
    const char *h = 0;
    if(_in_bp) {
        h = _rec->hdr();
    } else {
        h = (const char *)_hdrbuf;
    }
    return (const void *)(h+offset);
}

const void *
object_t::body(smsize_t offset) const
{
    const char *b = 0;
    if(_in_bp) {
        if(_rec->is_large()) {
            // Shouldn't ever call this
            // W_FATAL_MSG(fcINTERNAL, << "object_t::body");
            // Nope: I think no reason we shouldn't be able to sort
            // with large records ... 
        } 
        b = _rec->body();
    } else {
        b = (const char *)_bodybuf;
    }
    return (const void *)(b+offset);
}

void  
object_t::freespace() 
{
    if(_in_bp) {
            // nothing to free or unpin
        w_assert3(!_bodybuf);
        w_assert3(!_hdrbuf);
    } else {
        // NB: here we assume _bodylen == buf len or doesn't
        // matter
        if(_bodybuf) _bodyfact->freefunc(_bodybuf, _bodylen);
        _bodybuf=0;
        _bodylen = 0;
        if(_hdrbuf) _hdrfact->freefunc(_hdrbuf, _hdrlen);
        _hdrbuf=0;
        _hdrlen = 0;
    }
    _invalidate();
}

void  
object_t::assert_nobuffers() const
{
    // assert should hold regardless of _valid
    // and _in_bp
    w_assert1(_hdrbuf == 0);
    w_assert1(_bodybuf == 0);
}

w_rc_t  
object_t::copy_out(
        bool in_hdr, 
        smsize_t offset, 
        smsize_t length, 
        vec_t&dest)  const
{
    w_assert3(length <= dest.size());
    if(!is_in_buffer_pool()) {
        // this is the simple case
        if(in_hdr) {
            w_assert3(offset + length <= _hdrlen);
            char *where = ((char *)_hdrbuf) + offset;
            dest.copy_from( where, length, 0 );
        } else {
            w_assert3(offset + length <= _bodylen);
            char *where = ((char *)_bodybuf) + offset;
            dest.copy_from( (void *)where, length, 0 );
        }
    } else {
        // is in buffer pool.


        if(in_hdr) {
            w_assert3(offset + length <= _rec->hdr_size());
            dest.copy_from( _rec->hdr() + offset, length, 0 );
        } else if(_rec->is_small()) {
            w_assert3(offset + length <= _rec->body_size());
            dest.copy_from( _rec->body() + offset, length, 0 );
        } else {
            /* large object */
            w_assert3(offset + length <= _rec->body_size());

            w_assert3(_fp != 0);
            smsize_t         pageoffset= 0;
            smsize_t    pinned_length = 0;
            lpid_t pid = _rec->pid_containing(offset, pageoffset, *_fp);
            // pageoffset is byte# (relative to object) of 
            // first byte of this page
            pinned_length = MIN(((smsize_t)lgdata_p::data_sz), 
                    _rec->body_size()-pageoffset);

            pageoffset = offset - pageoffset;
            // pageoffset is now offset (relative to page)
            // of the offset of interest

            w_assert3(pageoffset < pinned_length);
            DBG(<<"in body(large) pageoffset=" << pageoffset 
                    << " pinned length=" << pinned_length);

            lgdata_p lgpage;
            W_DO(lgpage.fix(pid, LATCH_SH));
            INC_STAT_SORT(sort_lg_page_fixes);

            /* Do the copy here */
            const char *body=0;

            smsize_t vecoffset = 0;
            smsize_t left = length;
            smsize_t thispart=0;
            while(left > 0) {
                thispart = left < (pinned_length-pageoffset) 
                        ? left : 
                        pinned_length - pageoffset;

                DBG(<<"copy_out " << pageoffset 
                        << " offset, " << thispart << " bytes ");

                body = (char *)lgpage.tuple_addr(0);
                dest.copy_from(body + pageoffset, thispart, vecoffset);
                INC_STAT_SORT(sort_memcpy_cnt);
                ADD_TSTAT_SORT(sort_memcpy_bytes, thispart);

                vecoffset += thispart;
                left -= thispart;
                offset += thispart;

                lgpage.unfix();
                if(left > 0) {
                    w_assert3(_rec->is_large());
                    w_assert3(offset < _rec->body_size()); // because left>0
                    smsize_t junk = smsize_t(lgdata_p::data_sz);
                    pinned_length = MIN( junk, (_rec->body_size()-offset));
                    DBG(<<"copy_out: offset " << offset );
                    lpid_t pid;
                    pid = _rec->pid_containing(offset, pageoffset, *_fp);
                    DBG(<<"Pinning large page " << pid
                        << " for object offset " << offset
                        << " at page offset " << pageoffset);
                    W_DO(lgpage.fix(pid, LATCH_SH));
                    INC_STAT_SORT(sort_lg_page_fixes);
                    pageoffset = offset - pageoffset;
                    w_assert3(pageoffset == 0);
                }
            }
            INC_STAT_SORT(sort_keycpy_cnt);
        }
    }
    return RCOK;
}

smsize_t  
object_t::contig_body_size() const
{
    if(is_in_buffer_pool()) {
            if(_rec->is_large()) {
            return 0;
        } else {
            return _rec->body_size();
        }
    } else {
            return _bodylen;
    }
}

/**************************************************************************
 skey_t
**************************************************************************/
void            
skey_t::_invalidate() 
{
    _valid=false; 
    _in_obj=false; 
    _obj=0;
    _length=0;
    _offset=0;
    _in_hdr=false;
    _fact = factory_t::none; 
    _buf=0;
}

const void *
skey_t::ptr(smsize_t offset) const
{
    if(is_in_obj()) {
        if(_in_hdr) {
            return _obj->hdr(_offset + offset);
        } else {
            return _obj->body(_offset + offset);
        }
    } else {
        return (void *)((char *)(_buf) + _offset + offset);
    }
}
void            
skey_t::freespace()
{
    // NB: assume _buf length == _length
    // or else length doesn't matter to freefunc
    if(_buf) _fact->freefunc(_buf, _length);
    _buf = 0;
}

void            
skey_t::assert_nobuffers()const
{
    // Should be the case regardless of _valid
    if(is_in_obj()) {
        _obj->assert_nobuffers();
    } else {
        w_assert1(_buf == 0);
    }
}

smsize_t  
skey_t::contig_length() const
{
    // Return length of key that's on
    // the pinned page
    if(is_in_obj()) {
        smsize_t ol = 0;
        if(_in_hdr) {
            ol = _obj->hdr_size();
        } else {
            ol = _obj->contig_body_size();
        }
        w_assert3((_offset + _length <= ol) || ol==0);
        if(ol >= _offset + _length) {
            // small object
            return _length;
        } else {
            // large object
            w_assert3(ol==0); 
            return ol;
        }
    }
    return _length;
}

void 
skey_t::_replace(const skey_t&other)
{
    freespace();
    assert_nobuffers();
    if(other.is_in_obj()) {
        // Get around constness
        _construct(other._obj, other._offset, other._length, other._in_hdr);
    } else {
        _construct(other._buf, other._offset, other._length, other._fact);
    }
}

void 
skey_t::_replace_relative_to_obj(const object_t &o, const skey_t&other)
{
    freespace();
    // assert_nobuffers not apropos here, if relative to an object,
    // because we might have an obj ptr that we're about to wipe out,
    // and assert_nobuffers also asserts that the object has no buffers.
    //
    // assert_nobuffers();

    if(other.is_in_obj()) {
        // Get around constness
        _construct(&o, other._offset, other._length, other._in_hdr);
    } else {
        _construct(other._buf, other._offset, other._length, other._fact);
    }
}

w_rc_t  
skey_t::copy_out(vec_t&dest)  const
{
    w_assert3(is_valid());
    if(is_in_obj()) {
        W_DO(_obj->copy_out(_in_hdr, _offset, _length, dest));
    } else {
        dest.copy_from(ptr(0), _length);
    }
    return RCOK;
}

