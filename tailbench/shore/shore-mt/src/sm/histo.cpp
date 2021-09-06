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

 $Id: histo.cpp,v 1.18 2010/06/15 17:30:07 nhall Exp $

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
#define HISTO_C
#ifdef __GNUG__
#   pragma implementation "histo.h"
#endif

#include <histo.h>
#include <vol.h>

// Search: find the smallest element that is Cmp.ge
// If none found return -1
// else return index of the element found .
// Root of search is element i

template<class T, class Cmp>
SearchableHeap<T, Cmp>::SearchableHeap(const Cmp& cmpFunction, 
                       int initialNumElements)
: Heap<T,Cmp>(cmpFunction, initialNumElements)
{
}

template<class T, class Cmp>
int SearchableHeap<T, Cmp>::Search(int i, const T& t)
{
    w_assert3(HeapProperty(0));
    DBGTHRD(<<"Search starting at " << i
    << ", numElements=" << numElements
    );
    if(i > numElements-1) return -1;

    int parent = i; // root
    // First, check sibling of parent if parent != root
    if (parent >0 && (RightSibling(parent) < numElements))  {
    DBGTHRD(<<"check right sibling: " << RightSibling(parent));
    if (cmp.ge(elements[RightSibling(parent)], t)) {
        return RightSibling(parent);
    }
    }

    DBGTHRD(<<"parent=" << parent);
    if (cmp.ge(elements[parent], t))  {
    DBGTHRD(<<"LeftChild(parent)=" << LeftChild(parent)
        << " numElements " << numElements);
    int child = LeftChild(parent);
    while (child < numElements)  {
        DBGTHRD(<<"child=" << child);
        if (cmp.ge(elements[child], t))  {
        DBGTHRD(<<"take child=" << child);
        parent = child;
        } else  {
        child =  RightSibling(child);
        DBGTHRD(<<"try RightSibling:=" << child);
        if((child < numElements) &&
            (cmp.ge(elements[child], t)) )  {
            parent = child;
            DBGTHRD(<<"take child=" << parent);
        } else {
            return parent;
        }
        }
        child = LeftChild(parent);
    }  
        return parent;
    }
    DBGTHRD(<<"parent=" << parent);
    return -1; // none found
}

template<class T, class Cmp>
int SearchableHeap<T, Cmp>::Match(const T& t) const
{
    w_assert3(HeapProperty(0));
    for (int i = 0;  i < numElements; i++) {
    if (cmp.match(elements[i], t)) {
        return i;
    }
    }
    return -1; // none found
}

ostream &operator<<(ostream &o,
            const SearchableHeap<pginfo_t, histoid_compare_t> &sh)
{
    sh.Print(o);
    return o;
}

#ifdef EXPLICIT_TEMPLATE
class histoid_t;
template class Heap<pginfo_t, histoid_compare_t>;
template class SearchableHeap<pginfo_t, histoid_compare_t>;
template class w_hash_t<histoid_t, occ_rwlock, stid_t>;
template class w_hash_i<histoid_t, occ_rwlock, stid_t>;
template class w_list_t<histoid_t, occ_rwlock>;
template class w_list_i<histoid_t, occ_rwlock>;
#endif
typedef class w_hash_i<histoid_t, occ_rwlock, stid_t> w_hash_i_histoid_t_stid_t_iterator;

// no point leaving this in the .h -- everything that uses it is in
// this file, and we can change them w/o a full recompile if they're here.
enum {     root = 0, // root of heap
        pages_in_heap = 20, 
        buckets_in_table = 11,
        max_stores_in_table = 33 };

/**\brief Locks synchronizing access to hash table for histids.
 * \details
 * - htab_mutex_writer
 *   Used in CRITICAL_SECTION allow only one
 *   thread inside destroyed_store at a time.
 * - htab_mutex: read-write lock 
 *   - acquired in read mode to print the cache and
 *     in histoid:::acquire()
 *   - acquired in write mode inside destroyed_store
 *     which is ensured to have at most 1 thread as 
 *     it's a critical section protected by htab_mutex_writer.
 *     This is done because the occ_rwlock does not allow multiple
 *     threads to acquire_write. It's racy in that respect.
 */
queue_based_block_lock_t    histoid_t::htab_mutex_writer;
occ_rwlock                  histoid_t::htab_mutex;

w_hash_t<histoid_t, occ_rwlock, stid_t> *    histoid_t::htab = NULL;
int                         histoid_t::initialized = 0;

NORET         
histoid_t::histoid_t (stid_t s)
: cmp(s),
  refcount(0)
{
    DBGTHRD(<<"create histoid_t for store " << s
    << " returns this=" << this);
    pgheap = new SearchableHeap<pginfo_t, histoid_compare_t>(
    cmp, pages_in_heap);
    w_assert2(!_in_hash_table()); 
}

NORET         
histoid_t::~histoid_t () 
{
    DBGTHRD(<<"destruct histoid_t for store " << cmp.key
    << " destroying this=" << this);
    w_assert3(!_in_hash_table()); 
    w_assert1(refcount == 0);
    delete pgheap;
    pgheap = 0;
}

rc_t histoid_t::initialize_table()
{
    w_assert1(htab == NULL);
    w_assert0(initialized==0);
    htab = new w_hash_t<histoid_t, occ_rwlock, stid_t>( buckets_in_table,
                        W_HASH_ARG(histoid_t, cmp.key, link), &htab_mutex);

    if(htab) initialized++;
    return MAKERC(!htab, eOUTOFMEMORY);
}

void
histoid_t::destroy_table()
{
    // Empty the hash table
    // Called at shutdown from file_m's destructor.
    //

    {
        // Don't let me interfere with destroyed_store.
        CRITICAL_SECTION(cs, htab_mutex_writer);
        // Don't let me interfere with readers.
        htab_mutex.acquire_write();
        histoid_t* h(NULL);
        w_hash_i_histoid_t_stid_t_iterator iter(*htab);

        while ((h = iter.next()))  {
            bool   gotit = false;
            h->_grab_mutex_cond(gotit);
            if(gotit) {
                w_assert1(h->refcount == 0);
                DBGTHRD(<<"removing " << *h);
                w_assert1(h->_in_hash_table()); 
                w_assert1( htab->num_members() >= 1);
                htab->remove(h);
                w_assert2(!h->_in_hash_table()); 
                h->_release_mutex();
                delete h;
            } else {
                // Should never happen
                W_FATAL(fcINTERNAL);
            }
        } 
        // is empty
        htab_mutex.release_write();
    }

    w_assert1(htab->num_members() == 0);
    delete htab;
    htab = NULL;
    initialized--;
    w_assert0(initialized==0);
}

void    
histoid_t::_victimize(int howmany) 
{
    // Throw away (howmany) entries with 0 ref counts
    // Called with howmany > 0 only when we exceed
    // the desired table size 
    DBGTHRD(<<"_victimize " << howmany );

    // NB: ASSUMES CALLER HOLDS htab_mutex!!!
    // NB: the mutex is a read mutex, which can be held by
    // multiple threads!

    w_hash_i_histoid_t_stid_t_iterator iter(*htab);

    histoid_t *h = 0;
    while( (h = iter.next()) ) {
        bool gotit=false;
        h->_grab_mutex_cond(gotit);
        if(gotit)  {
            // Had better recheck for being in hash table,
            // since we only have the read mutex, alas.
            if((h->refcount == 0) && h->_in_hash_table()) 
            {
                DBGTHRD(<<"deleting " << h);
                w_assert1( htab->num_members() >= 1);
                htab->remove(h); // h->link.detach(), decrement _cnt 
                w_assert1(!h->_in_hash_table()); 
                h->_release_mutex();
                DBGTHRD(<<"deleting " << h);
                delete h;
                if(--howmany == 0) break;
            } else {
                h->_release_mutex();
            }
        }
    }
}

static bool histo_pin_if_pinned(int volatile* refcount) {
    int old_pin_cnt = *refcount;
    while(1) {
        if(old_pin_cnt == 0)
            return false;
        int new_pin_cnt = atomic_cas_32((unsigned*) refcount, old_pin_cnt, old_pin_cnt+1);
        if(new_pin_cnt == old_pin_cnt)
            return true;

        old_pin_cnt = new_pin_cnt;
    }
}


//
// In order to enforce 
// ref counts, we do this grotesque
// violation of const-ness here
//
histoid_t*
histoid_t::copy() const
{
    DBGTHRD(<<"incr refcount for store " << cmp.key 
    << " from " << refcount);
    histoid_t *I = (histoid_t *) this;
    if(!histo_pin_if_pinned(&I->refcount)) {
        I->_grab_mutex();
        atomic_inc(I->refcount);
        I->_release_mutex();
    }
    return I;
}

histoid_t*
histoid_t::acquire(const stid_t& s) 
{
    DBGTHRD(<<"acquire histoid for store " << s);
    htab_mutex.acquire_read();
    histoid_t *h = htab->lookup(s);
    if(h) {
        DBGTHRD(<<"existing store " << s);
        bool pinned = histo_pin_if_pinned(&h->refcount);
        if(!pinned) {
          h->_grab_mutex();
          w_assert1(h->refcount >= 0);
          DBGTHRD(<<"incr refcount for " << s
              << " from " << h->refcount);
          atomic_inc(h->refcount);// give reference before unlocking lookup table
        }
        htab_mutex.release_read();
        if(!pinned)
          h->_release_mutex();

    } else {
        // Release the read lock, grab a write lock for
        // these updates:
        htab_mutex.release_read();

        if(htab->num_members() >= max_stores_in_table) {
            // See if we can free up some space.
            // Only one thread can request the
            // htab_mutex in write mode. 
            CRITICAL_SECTION(cs, htab_mutex_writer);
            // Wait for the readers to drain out.
            htab_mutex.acquire_write();
            // recheck after acquiring write mode...
            if(htab->num_members() >= max_stores_in_table) 
            {

                // choose a replacement while we have the htab_mutex,
                // throw it out

                _victimize(1 + (htab->num_members() - max_stores_in_table));
            }
            htab_mutex.release_write();
        }

        // Create a new histoid for the given store
        DBGTHRD(<<"construct histoid_t");
        h = new histoid_t(s);
        w_assert2(h->refcount == 0);
        w_assert2(!h->_in_hash_table()); 

        {
            // Only one thread can request the
            // htab_mutex in write mode. 
            CRITICAL_SECTION(cs, htab_mutex_writer);
            // Wait for the readers to drain out.
            htab_mutex.acquire_write();

            htab->push(h); // insert into hash table
            w_assert2(h->_in_hash_table()); 
            htab_mutex.release_write();
        }
        h->_grab_mutex();

        DBGTHRD(<<"incr refcount for " << s
            << " from " << h->refcount
            << " for " << h);

        // give reference before unlocking lookup table!
        w_assert2(h->refcount == 0);
        long new_count = atomic_inc_nv(h->refcount);
        w_assert0(new_count == 1);

        h->_insert_store_pages(s);
        h->_release_mutex();
    }
    w_assert2(h);
    w_assert2(h->refcount >= 1);
    DBGTHRD(<<"acquired, store=" << s 
    << " this=" << h
    << " refcount= " << h->refcount
    );
    return h;
}

void 
histoid_t::_insert_store_pages(const stid_t& s) 
{
    DBGTHRD(<<"_insert_store_pages histoid " << this << " for store " << s);
    w_assert3(_in_hash_table()); 
    w_assert3(_have_mutex());
    w_assert3(pgheap->NumElements() == 0);
    w_assert3(pgheap->HeapProperty(0));

    /*
     * Scan the store's extents and initialize the histogram
     */
    pginfo_t     pages[pages_in_heap];
    int         numpages = pages_in_heap;
    W_COERCE(io->init_store_histo(&histogram, s, pages, numpages));
    // DBGTHRD(<<"histogram for store " << cmp.key << "=" << histogram << endl );
    DBGTHRD(<<"_insert_store_pages  " << this << " for store " << cmp.key );
    DBGTHRD(<<"io_m scan found " << numpages << " pages" );

    /*
     * init_store_histo filled numpages pginfo_t, where
     * numpages is a number it produced. The pginfo_ts contain
     * bucket#s rather than space used/free, so we'll convert
     * them here, as we stuff them into the heap.
     */
    pginfo_t    p;
    for(int i=0; i<numpages; i++) {
        DBGTHRD(<<"page index." << i << "=pid " << pages[i].page()
            << " space is " << pages[i].space());
        DBGTHRD(<<"pgheap.AddElementDontHeapify page " 
            <<pages[i].page() << " space " << pages[i].space());
        pgheap->AddElementDontHeapify(pages[i]); // makes a copy
    }
    pgheap->Heapify();
    w_assert3(pgheap->NumElements() == numpages);
    w_assert3(_have_mutex());
}

bool
histoid_t::release()
{
    DBGTHRD(<<"release: decr refcount for store " 
    << cmp.key 
    << " this=" << this
    << " from " << refcount);

    long new_count = atomic_dec_nv(refcount);
    w_assert0(new_count >= 0);
    bool deleteit = ((new_count==0) && !_in_hash_table());
    return deleteit;
}

#if W_DEBUG_LEVEL > 2
#define TRACEIT
#endif
#ifdef TRACEIT
struct trace_info {
    int line;
    int members;
    histoid_t *h;
    // trace_info(int l, int m, histoid_t*x) : line(l), members(m), h(x) {}
};
#endif

#ifdef TRACEIT
const int  TRACE_NUM=10;
static __thread struct trace_info TRACE_LINE[TRACE_NUM]; // DEBUGGING
static __thread int TRACE_IDX=0; // DEBUGGING
#define T(l,c,h)\
{ trace_info &x=TRACE_LINE[TRACE_IDX];  \
    x.line=l; x.members=c; x.h=h; TRACE_IDX++; }
#else
#define T(l,c,h)
#endif

void     
histoid_t::destroyed_store(const stid_t&s, sdesc_t*sd) 
{
    DBGTHRD(<<"histoid_t::destroyed_store " << s);

    bool success = false;
#ifdef TRACEIT
   TRACE_IDX=0;
#endif
    while (!success) {
        // Only one thread can request the
        // htab_mutex in write mode. 
        CRITICAL_SECTION(cs, htab_mutex_writer);
        // Wait for the readers to drain out.
        htab_mutex.acquire_write();
        histoid_t *h = htab->lookup(s);
T(__LINE__, htab->num_members(), h);
        if(h) {
            DBGTHRD(<<"lookup found " << h
                << " refcount " << h->refcount);

            h->_grab_mutex_cond(success);
            if(success) {
#if W_DEBUG_LEVEL >0
                // ref count had better be no more than 1
                // because it takes an EX lock
                // on the store to be able to destroy it.
                if(sd)
                    w_assert1(h->refcount == 1);
                else
                    w_assert1(h->refcount == 0 || h->refcount == 1);
#endif
T(__LINE__, htab->num_members(), h);

                DBGTHRD(<<"removing " << h );

                w_assert1( h->_in_hash_table()); 
T(__LINE__, htab->num_members(), h);
                w_assert1( htab->num_members() >= 1);
                htab->remove(h); // h->link.detach(), decrement _cnt 
T(__LINE__, htab->num_members(), h);
                w_assert2( !h->_in_hash_table()); 
                h->_release_mutex();
            }
        } else {
           success = true; // is gone already
        }

        // Out of critical section: done messing
        // with hash table, h is no longer in it
        // and we have a lock on h (if h is non-null)
        htab_mutex.release_write();
        cs.exit();

        if(h) {
            w_assert1( !h->_in_hash_table()); 
            DBGTHRD(<<"h=" << *h);
            if(sd || smlevel_0::in_recovery() ) {
                w_assert1(h->refcount >= 1);
                if(h->release()) {
                    DBGTHRD(<<"deleting " << h);
                    delete h;
                } else {
                    w_assert3(0);
                }
            } else if(h->refcount == 0) {
                DBGTHRD(<<"deleting " << h);
                delete h;
            } else {
                /* We don't have sd, so we can't wipe out
                 * the reference to h, so we can't
                 * delete it, even though we have removed
                 * it from the table.
				 *
                 * The problem here is that it WILL get
                 * cleaned up if we're in fwd/rollback (when
                 * the dir cache gets invalidated), but
                 * NOT if we're in crash-recovery/rollback
                 * or in pseudo-crash-recovery/rollback as 
                 * realized by ssh "sm restart" command.  
                 */
T(__LINE__, htab->num_members(), h);
#if W_DEBUG_LEVEL >= 0
                fprintf(stderr, 
		"GNATS 108 histoid cleanup problem. Refcount %d sd %p in_recov %d\n",
				h->refcount ,
				sd,
				smlevel_0::in_recovery()
				);
                // croak here
                // RE-insert this to debug : w_assert0(0);
#endif
                 DBGTHRD(<<"ROLLBACK! can't delete h= " << h);
            }
        } 
        w_assert1(success);
    }
    if(sd) {
        // wipe out pointer to h
        sd->add_store_utilization(0);
    }
}



/* 
 * Search the heap for a page with given page id
 *
 * Accessory to _find_page_return_info, find_page
 * etc.
 * DOES NOT FREE MUTEX!!! Caller must do that!
 */
int
histoid_t::__find_page_in_heap(
    bool               insert_if_not_found,
    const shpid_t&     pid
)
{
    DBGTHRD(<<"this " << this << " __find_page_in_heap " << pid );

    int hook;
    while(1) {
        w_assert3(pgheap->HeapProperty(0));
        for(hook = root; hook < pgheap->NumElements(); hook++) {
            DBG(<<"checking at hook=" << hook
                << " page=" << pgheap->Value(hook).page() );
            if(pgheap->Value(hook).page() == pid) {
                DBGTHRD(<<"grabbing hook " << hook
                    << " page " << pgheap->Value(hook).page()
                    << " space " << pgheap->Value(hook).space()
                    );
                return hook;
            }
        }
        if(insert_if_not_found) {
            /* Not found: create one & insert it */
            pginfo_t info(pid,0); // unknown size 
            w_assert3(_have_mutex());
            DBG(<<"insert if not found");
            install(info);
            // search to get the hook
        } else {
            return nohook;
        }
    }
    // should never happen
    W_FATAL(eINTERNAL);
    return (-1); // to keep compiler happy
}

/********************************************************************
 * _find_page_return_info: 
 * Look up page in heap (w/o changing heap) with __find_page_in_heap,
 * which returns hook<0 if not found, hook >= 0 if found.
 * Arg create_if_not_found refers to the pginfo_t.
 *
 * Returns result: 
 *    found_in_table = true iff the page was in the heap
 *    If it was found in the heap, the entry is removed and
 *      stuffed into the pginfo_t structure.
 *    If it was NOT found in the heap, the pginfo_t is
 *      initialized only if create_if_not_found is true.
 *      It is NOT stuffed into the heap however.
 *
 * In any case, after this is called, the entry is NOT in the
 * heap.
 *              
********************************************************************/
void
histoid_t::_find_page_return_info(
    bool               init_pginfo_if_not_found,
    const shpid_t&     pid,
    bool&              found_in_table,
    pginfo_t&          result
)
{
    // Don't create a pginfo_t, insert, then remove
    _grab_mutex();
    int hook = __find_page_in_heap(false, pid);
    w_assert3(_have_mutex());

    found_in_table = bool(hook>=0);
    if(hook < 0/*!found_in_table*/ && init_pginfo_if_not_found) {
        result = pginfo_t(pid,0); // unknown size 
    } else if(hook >= 0 /*found_in_table*/) {
        result = pgheap->RemoveN(hook);
        DBGTHRD(<<"this " << this << " _find_page_return_info, pgheap.RemoveN page " 
            <<result.page() 
            << " space " << result.space());
        w_assert3(pgheap->HeapProperty(0));
    }
#if W_DEBUG_LEVEL > 1
    if(found_in_table)
        w_assert2(pgheap->Match(result) == -1);
    else 
        w_assert2(pgheap->Match(pginfo_t(pid,0)) == -1);
#endif 
    _release_mutex();
}

static __thread struct { extnum_t e; snum_t s; } last_extent = {0,0};

/*
 * Search the heap for a page that contains enough space. 
 * If found, hang onto the mutex for the histoid.
 */
w_rc_t
histoid_t::find_page(
    smsize_t         space_needed, 
    bool&            found,
    pginfo_t&        info, 
    file_p*          pagep,     // input
    slotid_t&        idx    // output iff found
#ifdef SM_DORA
    , const bool     bIgnoreParents
#endif
) const
{
    DBGTHRD(<<"histoid_t::find_page in store " << cmp.key
    << " w/ at least " << space_needed << " free bytes "
    << " refcount=" << refcount
    );

    w_assert1(refcount >= 0);

    pginfo_t     tmp(0, space_needed);
    int          skip = root;
    int          hook;

    found =  false;

    while(!found) {
        _grab_mutex();
        hook = pgheap->Search(skip, tmp);
        DBGTHRD(<<"Search returns hook " << hook);
        if(hook >= root) {
            // legit
            w_assert3(_have_mutex());
            w_assert3(pgheap->HeapProperty(0));
            info = pgheap->RemoveN(hook);
            w_assert3(pgheap->HeapProperty(0));
            DBGTHRD(<<"pgheap.RemoveN page " 
                <<info.page() << " space " << info.space());

            _release_mutex();

            DBGTHRD(<<"histoid_t::find_page FOUND " << info.page());
            bool       success=false;
            shpid_t    pg = info.page();
            extnum_t   ext = vol_t::pid2ext(pg);
            {
                lpid_t pid(cmp.key, pg);
                // Now that latch_lock_get_slot checks
                // is_valid_page_of, we'll skip this check here,
                // under the assumption that the failure case can
                // take longer if we speed up the normal case.
                // (I saw about a 2.3 % fail rate in one run of
                // the Xneworder benchmarks with 1->12 threads, 
                // and about a smaller fail rate in the
                // other benchmarks -- looking at the page-moved-type 
                // failures, not the page-full failures.)
                //
                // success = io->is_valid_page_of(pid, cmp.key.store);
                success = true;
                if(success) {
                    last_extent.e = ext;
                    last_extent.s = cmp.key.store;
#if W_DEBUG_LEVEL > 1
                } else {
                    fprintf(stderr, "^^^ is_valid_page_of() returned false!\n");
#endif
                }
                DBGTHRD(<<" is valid page:" << success);
                if(!success) skip = hook;
            }
            // RACE: between last check for valid page of and
            // the following latch/lock, the page could have been freed.
            // So latch_lock_get_slot has to verify that the page is still
            // a valid page for this store.
            if(success) {
                success=false;
                W_DO(latch_lock_get_slot(pg, pagep, space_needed,
                    false, // not append-only
                    success, idx
#ifdef SM_DORA
                                         , bIgnoreParents
#endif
                                         ));
                if(success) {
                    // checking here ONLY so we can tell the path taken 
                    w_assert2(pagep->pid().page == pg);
                    w_assert2(pagep->pid().stid().store == cmp.key.store);
                    lpid_t pid(cmp.key, pagep->pid().page);
                    w_assert2(io->is_valid_page_of(pid, cmp.key.store));
                }
            }
            if(success) {
                found = true;
                w_assert2(pagep->is_fixed());
                w_assert2(idx != 0);
                w_assert2(cmp.key.store != 0);
                w_assert2(pagep->pid().page == pg);
                DBGTHRD(<<"histoid_t::find_page FOUND " << pagep->pid().page);

                lpid_t pid(cmp.key, pagep->pid().page);
                w_assert2(io->is_valid_page_of(pid, cmp.key.store));
                return RCOK;
            } else {
                  if(last_extent.e == ext && last_extent.s == cmp.key.store)
                            last_extent.s = snum_t(-1);
                  skip = hook;
            }
        } else {
            _release_mutex();
            // No suitable pages in cache
            DBGTHRD(<<"histoid_t::find_page NOT FOUND in cache");
            w_assert3(!found);
            return RCOK;
        }
    } // while !found
    // Should never get here
    W_FATAL(eINTERNAL);
    return RCOK;
}

w_rc_t        
histoid_t::latch_lock_get_slot(
    shpid_t&     shpid,
    file_p*      pagep, 
    smsize_t     space_needed,
    bool         append_only,
    bool&        success,
    slotid_t&    idx    // only meaningful if success
#ifdef SM_DORA
    , const bool bIgnoreParents
#endif
) const 
{
    success    = false;
    lpid_t    pid(cmp.key, shpid);
    /*
     * conditional_fix doesn't re-fix if it's already
     * fixed in desired mode; if mode isn't sufficient,
     * is upgrades rather than double-fixing
     */
    // rc_t rc = pagep->conditional_fix(pid, LATCH_EX, 0, st_bad, true);
    store_flag_t        junk = st_bad;
    rc_t rc = ((page_p *)pagep)->conditional_fix(pid, page_p::t_any_p,
                        LATCH_EX, 0, junk, true);


    DBGTHRD(<<"rc=" << rc);
    if(rc.is_error()) {
        // used to be stTIMEOUT
        // Now can be stTIMEOUT or stINUSE
        if(rc.err_num() == smthread_t::stINUSE ||
            rc.err_num() == smthread_t::stTIMEOUT) {
            // someone else has it latched - give up
            // on trying to allocate from it. We haven't
            // covered this page with a lock or a latch.
            INC_TSTAT(fm_page_nolatch);
            return RCOK;
        } 
        // error we can't handle
        DBGTHRD(<<"rc=" << rc);
        INC_TSTAT(fm_error_not_handled);
        return RC_AUGMENT(rc);
    } 

    w_assert3(pagep->is_fixed());

    if(pagep->pid().stid() != cmp.key) 
    {
        // Someone else raced in
        DBGTHRD(<<"stid changed to " << pagep->pid().stid());
        // reject this page
        pagep->unfix();
        INC_TSTAT(fm_page_moved);
        return RCOK; 
    } else {
        /* while we have the page fixed, we have to check
         * its store membership. The above check only catches
         * a changed allocation status if it were re-allocated to
         * a different store, which shows up on the page only
         * as it's formatted.
         * It doesn't catch the case in which the page is freed from
         * the store but not re-formatted.  In theory, our caches shouldn't
         * find it as an allocated page and we should have been forced
         * to allocate it first, but...
         *
         * The problem here is that if we check that first, we
         * won't have the protection of a lock on the page.
         * What we really need to do is to IX-lock the page iff
         * the page is allocated to the store.  The layering of
         * histo/file/sm_io/vol makes that difficult.
         *
         * We grab an IS lock, which we'll upgrade to an IX if
         * the page is used (one of its records is locked). 
         * The down side is that this will prevent the page from
         * being freed until we commit.
         *
         * Note that if the page has changed stores, this lock will not
         * conflict with that page lock, however, all we need to do
         * here is to protect ourselves  from the page being freed
         * from THIS store while we are doing this check of the
         * extent's allocation-to-store info.
         */
        rc_t rc = lm->lock_force(pid, IS, t_long, WAIT_IMMEDIATE);
        if(rc.is_error()) {
            DBGTHRD(<<"rc=" << rc);
            pagep->unfix();
            // couldn't get the lock so someone has it ex-locked.
            INC_TSTAT(fm_page_nolock);
            return RCOK; 
        }
         
        if( ! io_m::is_valid_page_of(pid, pid._stid.store)) {
            DBGTHRD(<<"page no longer in store " << cmp.key);
            // reject this page
            //
            pagep->unfix();
            INC_TSTAT(fm_page_invalid);
            return RCOK; 
        }
        /* Try to acquire the lock */
        DBGTHRD(<<"Try to acquire slot & lock ");

        rc = pagep->_find_and_lock_free_slot(append_only,
                                             space_needed, idx
#ifdef SM_DORA
                                             , bIgnoreParents
#endif
                                             );
        DBGTHRD(<<"rc=" <<rc);

        if(rc.is_error()) {
            DBGTHRD(<<"rc=" << rc);

            pagep->unfix();

            if (rc.err_num() == eRECWONTFIT) {
                INC_TSTAT(fm_page_full);
                return RCOK; 

            } else if (rc.err_num() == ePAGECHANGED) {
                // fprintf(stderr, 
                // "-*-*- Oops! Tried to use a deallocated page %d.%d.%d!\n",
                // pid.vol().vol, pid.store(), pid.page);
                INC_TSTAT(fm_page_moved);
                return RCOK;
            } 
            W_FATAL_MSG(rc.err_num(), << " Error not handled");
            // error we can't handle
            INC_TSTAT(fm_error_not_handled);
            return RC_AUGMENT(rc);
        }
        DBGTHRD(<<"acquired slot " << idx);
        INC_TSTAT(fm_ok);
        success = true;
        // idx is set
    }
    w_assert2(pagep->is_fixed());
    return RCOK;
}

void        
histoid_t::install(const pginfo_t &info)
{
    DBGTHRD("install info in heap" << info);
    w_assert1( _in_hash_table() );
    bool do_release = false;
    bool do_install = true;
    if(!_have_mutex()) {
        _grab_mutex();
        do_release = true;
    }

    // Install new info for page
    // NB: assumes it's not already there 

#if W_DEBUG_LEVEL > 2
    {   // verify that the page isn't already in the heap
        pginfo_t tmp(info.page(), 0);
        w_assert3(pgheap->Match(tmp) == -1);
    }
#endif 

    w_assert3(_have_mutex());

    w_assert3(pgheap->HeapProperty(0));
    int n = pgheap->NumElements();
    if(n >= pages_in_heap) {
        // remove one iff this is greater than
        // one of the smallest there (e.g. last one)
        pginfo_t&    t = pgheap->Value(n-1);
        if( ! cmp.gt(info, t) ) {
            DBGTHRD(<<"bypassing install: new space " 
            << info.space()
            << " space at bottom of heap =" << 
            t.space());
            do_install = false;
        } 
        pginfo_t p = pgheap->RemoveN(n-1); // makes a copy
    }
    if(do_install) {
        DBGTHRD(<<"pgheap.AddElement page" << info.page()
            << " space " << info.space()
        );
        pgheap->AddElement(info); // makes a copy
    }
    if(do_release) {
        _release_mutex();
    }
    w_assert3(pgheap->HeapProperty(0));
}

/*
 * update_page - called when we added/removed something
 * to/from a page, so we will want to cause an
 * entry to be in the table, whether or not it was
 * there before.
 */
void        
histoid_t::update_page(const shpid_t& pid, smsize_t amt)
{
    DBGTHRD(<<"update_page");
    _grab_mutex();
    int hook = __find_page_in_heap(true, pid); // insert if not found


    // hang onto mutex while updating...
    w_assert3(_have_mutex());

    w_assert3(pgheap->HeapProperty(0));
    pginfo_t&    t = pgheap->Value(hook);

    DBGTHRD(<<"histoid_t::update_page hook " 
        << hook 
        << " page " <<  t.page()
        << " amt " << amt);

    smsize_t    old = t.space();
    if(amt != old) {
        DBGTHRD(<<"before update space: " << t.space());
        t.update_space(amt);
        DBGTHRD(<<"after update space: " << t.space());
        if(old < amt) {
            DBGTHRD(<<"pgheap.IncreasedN hook " << hook
            << " page " << t.page()
            << " space " << t.space());
            pgheap->IncreasedN(hook);
        } else {
            DBGTHRD(<<"pgheap.DecreasedN hook "  << hook
            << " page " << t.page()
            << " space " << t.space());
            pgheap->DecreasedN(hook);
        }
    }
    w_assert3(pgheap->HeapProperty(0));
    _release_mutex();
}


w_rc_t        
histoid_t::exists_page(
    smsize_t space_needed,
    bool&    found
) const
{
    space_bucket_t b = file_p::free_space2bucket(space_needed);
    _grab_mutex();
    // DBGTHRD(<<"histogram for store " << cmp.key << "=" << histogram << endl );
    DBGTHRD(<<"exists_page store " << cmp.key);
    while (! (found = histogram.exists(b)) 
                && b < (space_num_buckets-1)) b++;
    _release_mutex();
    return RCOK;
}

void        
histoid_t::bucket_change(
    smsize_t    old_space_left,
    smsize_t    new_space_left
) 
{
    space_bucket_t ob = file_p::free_space2bucket(old_space_left);
    space_bucket_t nb = file_p::free_space2bucket(new_space_left);
    if(ob != nb) {
        _grab_mutex();
        histogram.decr(ob);
        histogram.incr(nb);
        _release_mutex();
        DBGTHRD(<<"changed bucket" << histogram );
    }
}



ostream &histoid_t::print(ostream &o) const
{
    o << this;
    o  << " key=" << cmp.key
      << " refcount=" << refcount;
    if (_in_hash_table())
        o << ", hashed";
    o << ", #pages=" << pgheap->NumElements();
    o << " [" << histogram << ']';

    return o;
}

ostream &operator<<(ostream&o, const histoid_t&h)
{
    return h.print(o);
}


ostream &histoid_t::print_cache(ostream &o, bool locked)
{
    if (initialized>0 && locked)
        htab_mutex.acquire_read();

    o << "HISTOID_T::PRINT_CACHE { " << endl;

    o << "histoid_m:";
    if (initialized>0)
        o << " initialized";

    if (htab) {
        o << ' ' << htab->num_members() << " entries" << endl;

        w_hash_i_histoid_t_stid_t_iterator    iter(*htab);
        histoid_t                *h;

        while ((h = iter.next()))  {
            if (locked)
                h->_grab_mutex();
            o << '\t' << *h << endl;
            if (locked)
                h->_release_mutex();
        }
    }
    o << "END PRINT_CACHE } " << endl;

    if (initialized>0 && locked)
        htab_mutex.release_read();
    
    return o;
}

histoid_remove_t::histoid_remove_t(file_p &pg)
: _found_in_table(false),  // _found_in_table ==> want to reinstall
  _page(&pg),
  _h(NULL) 
{
    w_assert2(_page->is_fixed());
    lpid_t    _pid = _page->pid();
    DBGTHRD(<<"histoid_remove_t constructor from page " << pg.pid()
            << " this " << this);
    _h = new histoid_t(_pid._stid);
    w_assert2(_h->refcount == 0);
    w_assert1(! _h->_in_hash_table()); 

    // Look for the page in the heap, remove it if it's there
    _h->_find_page_return_info(false, _page->pid().page, _found_in_table, _info);
    w_assert1(! _h->_in_hash_table()); 
    w_assert1( !_h->_have_mutex());
    delete _h; 
}


histoid_update_t::histoid_update_t(sdesc_t *sd)
: _found_in_table(false),  // _found_in_table ==> want to reinstall
  _page(NULL),
  _h(NULL) 
{
    _h = sd->store_utilization()->copy();
    _info.set(0,0); // unknown size
    _old_space = 0; // unknown
    DBGTHRD(<<"histoid_update_t constructor from sd " << *this);
}


histoid_update_t::histoid_update_t(file_p& pg)
: _found_in_table(false), // _found_in_table ==> will want to reinstall
  _page(&pg),
  _h(NULL)
{ 
    w_assert3(_page->is_fixed());
    lpid_t    _pid = _page->pid();
    DBGTHRD(<<"histoid_update_t constructor from page " << pg.pid()
            << " this " << this);

    _h = histoid_t::acquire(_pid.stid()); // incr ref count,
    // puts in hash table
    w_assert2(_h->refcount >= 1);

    DBGTHRD(<<"histoid_update_t constructor this " << this);
    _h->_find_page_return_info(true, _pid.page, _found_in_table, _info);
    _old_space = _info.space();

    /* update bucket info, since we have the page fixed */
    smsize_t current_space = pg.free_space4bucket();
    if(current_space != _info.space()) {
        /*
         * How can the value be wrong?  Let me count the ways:
         * 1) it might have been
         *    taken from the BUCKET in the extent's histogram,
         *    by init_store_histo
         * and
         *    1.0) extent map might have it wrong -maybe has
         *    never been set  (init to bucket 0)
         *    1.1) the extent's bucket info isn't logged, so after
         *    crash/recovery it could be just plain WRONG, and, 
         *    as long as the pages aren't updated, it will remain wrong.
         * 2) This thread might have updated the page without yet
         *    unfixing the page at the time we are looking at it
         * 3) This could be a newly -allocated page that's
         *    got old histo info left in the extlink and this very
         *    call to this function is to update that info.
         */

#if W_DEBUG_LEVEL>4
        if (!(
            (page_p::free_space2bucket(current_space) == 
              page_p::free_space2bucket(_info.space()))
              ||
              (page_p::free_space2bucket(_info.space()) == 0)
          )) {
              // should put a counter here
            }
#endif /* W_DEBUG_LEVEL */

        // update _old_space and _info:
        _info.set(_info.page(), (_old_space = current_space));
    }

    DBGTHRD(<<"histoid_update_t constructor this " << this << ":" << *this);
}

NORET 
histoid_update_t::~histoid_update_t() 
{
    DBGTHRD(<<"~histoid_update_t");
    DBGTHRD(<<"DESTRUCT histoid_update_t: " << *this);
    if(_h) {
        if(_found_in_table) {
            // want to reinstall page
            w_assert3(!_h->_have_mutex());
            if(_page && _page->is_fixed())  {
                w_assert3(_info.page() == _page->pid().page);
            }
            DBGTHRD(<<"calling bucket change for page " << _info.page());
            _h->bucket_change(_old_space, _info.space());
            DBGTHRD(<<"destructor, error case, found in table, put back");
            _h->install(_info); 
        }
        if(_h->release()) {
            // won't get deleted if it's in the hash table
            delete _h; 
        }
        _h = 0;
    }
    DBGTHRD(<<"end ~histoid_update_t "); 
}

ostream &
operator<<(ostream&o, const histoid_update_t&u)
{
    o << " info: page= " << u._info.page() << " space=" << u._info.space();
    o << " found in table: " << u._found_in_table;
    o << " old space: " << u._old_space;
    if(u._h) {
        o << endl << "\thistoid_t:" << *u._h << endl;
    }
    return o;
}

void  
histoid_update_t::replace_page(file_p *p, bool reinstall) 
{
    if(_page) {
        DBGTHRD(<<"replace page " << _page->pid().page
            << " refcount=" << _h->refcount
        );
    }

    // Re-install OLD page if we found it there and
    // we're not discarding it due to its being
    // entirely full
    bool differs = (_info.page() != p->pid().page) ;
    if(differs || p != _page) {
        if(_found_in_table && reinstall) {
            w_assert3(!_h->_have_mutex());
            DBGTHRD(<<"bucket change for page " << _info.page());
            if(_page && _page->is_fixed()) {
                w_assert3(_info.page() == _page->pid().page);
            }
            _h->bucket_change(_old_space, _info.space());
            DBGTHRD(<<"replace page ");
            _h->install(_info); 
        }
        _page = p;
        DBGTHRD(<<"replaced old page with " 
            << _page->pid().page
            << " refcount= " << _h->refcount
            );
         // establish new page in table:
         _h->_find_page_return_info(true, _page->pid().page, _found_in_table, _info);
    }
}

void  
histoid_update_t::remove() 
{
    FUNC(histoid_update_t::remove);
    // called when page is deallocated
    if(_page && _page->pid().page) {
        DBGTHRD(<<"histoid_update_t remove() (page free)" << *this);
        _h->_find_page_return_info(false, _page->pid().page, _found_in_table, _info);
        w_assert2(!_found_in_table);
        w_assert2(_info.page() == _page->pid().page);
    }
    DBGTHRD(<<"remove page " << _page->pid().page
                    << " refcount= " << _h->refcount);
    _page = 0;
    _found_in_table = 0;
    DBGTHRD(<<"bucket change for page " << _info.page());
    w_assert2(_h);

    _h->bucket_change(_old_space, _info.space());
    _info.set(0,0); // unknown size
    if(_h->release()) { 
        delete _h; 
    }
    _h = 0;
}

void  
histoid_update_t::update() 
{
    // called when page is updated
    w_assert3(_h);
    w_assert3(_h->refcount > 0);
    w_assert3(_page->is_fixed());
    w_assert3(_page->latch_mode() == LATCH_EX);

    smsize_t newamt = _page->usable_space_for_slots();

    DBGTHRD(<<"update() page " << _page->pid().page
            << " newamt=" << newamt
            << " refcount=" << _h->refcount
            );

    DBGTHRD(<<"bucket change for page " << _info.page());
    w_assert3(_info.page() == _page->pid().page);

    _h->bucket_change(_old_space, newamt);
    _h->update_page(_page->pid().page, newamt);
    /*
     * If the page wasn't originally in the heap, it 
     * is now, so be sure we don't try to put it in
     * a 2nd time when this object is destroyed.
     * _found_in_table means a) page was found there, and
     * b) we removed it and so we want to put it back.
     */
    _found_in_table = false;
}

bool     
histoid_compare_t::match(const pginfo_t& left, const pginfo_t& right) const
{
    if(left.page() == right.page()) {
        return true;
    }
    return false;
}

bool     
histoid_compare_t::gt(const pginfo_t& left, const pginfo_t& right) const
{
    // return true if left > right
    // So pages with more free space drifts to the top
    // If 2 pages have same free space, but one 
    // is presently in the buffer pool, it drifts to the top.

    DBGTHRD(<<"histoid_compare_t::gt left=" 
        << left.page() << ":" << left.space()
        << " right= " 
        << right.page() << ":" << right.space()
        );
    if( left.space() > right.space() ) {
        DBGTHRD(<< " returning true" );
        return true;
    }
    DBGTHRD(<< " returning false" );
    return false;

#ifdef COMMENT
// TODO: figure out how to include this -- the problem
// with it is that when the HeapProperty is being checked,
// it calls the same function.
// Might have to pass a flag or create a different
// function for use with HeapProperty

    if( left.space() < right.space() ) return false;

    // Both pages have same space
    lpid_t p(key(), right.page());
    if ( !smlevel_0::bf->has_frame(p)) return true;

    // right is cached.  If left is cached, they are truly
    // equal; if left is not cached, right > left; in either
    // case, left is not > right, so we can return false.
    return false;
#endif /* COMMENT */
}

bool     
histoid_compare_t::ge(const pginfo_t& left, const pginfo_t& right) const
{
    // return true if left >= right

    DBGTHRD(<<"histoid_compare_t::ge left=" 
        << left.page() << ":" << left.space()
        << " right= " 
        << right.page() << ":" << right.space()
        );
    if( left.space() >= right.space() ) {
        DBGTHRD(<< " returning true" );
        return true;
    }
    DBGTHRD(<< " returning false" );
    return false;
}

// WARNING: this function assumes that a thread only locks one histoid
// at a time. AFAIK this is the case, and there are assertions to
// verify as well.
//static __thread queue_based_lock_t::ext_qnode histoid_me = EXT_QNODE_INITIALIZER;
static __thread queue_based_lock_t::ext_qnode histoid_me = QUEUE_EXT_QNODE_INITIALIZER;

void
histoid_t::_release_mutex() const
{
    w_assert2(_histoid_mutex.is_mine(&histoid_me));
    _histoid_mutex.release(&histoid_me);
}
void
histoid_t::_grab_mutex() const
{
    w_assert2( ! _histoid_mutex.is_mine(&histoid_me));
    _histoid_mutex.acquire(&histoid_me);
}
void
histoid_t::_grab_mutex_cond(bool& got) const
{
    w_assert2( ! _histoid_mutex.is_mine(&histoid_me));
    got = _histoid_mutex.attempt(&histoid_me);
}

bool
histoid_t::_have_mutex() const
{
    return _histoid_mutex.is_mine(&histoid_me);
}
