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

  $Id: page.cpp,v 1.147 2010/06/15 17:30:07 nhall Exp $

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
#define PAGE_C
#ifdef __GNUG__
#   pragma implementation "page.h"
#   pragma implementation "page_s.h"
#   pragma implementation "page_h.h"
#endif
#include <sm_int_1.h>
#include <page.h>
#include <page_h.h>

// -- for page tracing
//pthread_mutex_t page_p::_glmutex = PTHREAD_MUTEX_INITIALIZER;
//ofstream page_p::_accesses("page_accesses.txt");
//timeval page_p::curr_time;
// --

w_base_t::w_base_t::uint4_t
page_p::get_store_flags() const
{
    return _pp->get_page_storeflags();
}

void
page_p::set_store_flags(w_base_t::uint4_t f)
{
    // If fixed in ex mode, set through the buffer control block
    // Do not set if not fixed EX.
    bfcb_t *b = bf_m::get_cb(_pp);
    if(b && is_mine()) {
        b->set_storeflags(f);
    } else {
        // If this isn't a buffer-pool page, we don't care about
        // keeping any bfcb_t up-to-date. 
        // This is used when we are constructing a page_p with
        // a buffer on the stack. This happens in formatting
        // a volume, for example. 
        _pp->set_page_storeflags(f);
    }
}

// For debugging: tracking down oddities
extern "C" void pagestophere()
{
}


/*********************************************************************
 *
 *  page_s::space_t::_check_reserve()
 *
 *  Check if reserved space on the page can be freed
 *
 *********************************************************************/
inline void page_s::space_t::_check_reserve()
{
    w_assert3(rflag());
    w_assert3(nfree() >= nrsvd());
    w_assert3(nrsvd() >= xct_rsvd());
    w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);

    if (_tid < xct_t::oldest_tid())  {
        /*
         *  youngest xct to reserve space is completed and
         * because it's < oldest tx, this means all
         * tx to reserve space have completed.
         *  --- all reservations can be released.
         */
        _tid = tid_t();
        _nrsvd = _xct_rsvd = 0;
    }
    w_assert3(nfree() >= nrsvd());
    w_assert3(nrsvd() >= xct_rsvd());
    w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);
}

/*********************************************************************
 *
 *  page_s::space_t::usable(xd)
 *
 *  Compute the usable space on the page for the transaction xd.
 *  Might free up space (by calling _check_reserve)
 *
 *********************************************************************/
int
page_s::space_t::usable(xct_t* xd) 
{
    w_assert3(nfree() >= nrsvd());
    w_assert3(nrsvd() >= xct_rsvd());
    w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);

    if (rflag()) _check_reserve();
    int avail = nfree() - nrsvd();

    if (rflag()) {
        if(xd)  {
            if (xd->state() == smlevel_1::xct_aborting)  
            {
                /*
                 *  An aborting transaction can use all reserved space
                 */
                avail += nrsvd();
            } else if (xd->tid() == _tid) {
                /*
                 *  An active transaction can reuse all space it freed.
                 *  NOTE: transaction could be in rollback_work
                 *  or rollback in recovery undo-pass.  Its state is active
                 *  in those 2 cases.  We might want to treat these the
                 *  same as the aborting case, above. (??)
                 */
                avail += xct_rsvd();
            }
        } else if (smlevel_0::redo_tid &&
                        *smlevel_0::redo_tid == _tid) {
            /*
             *  This is the same as an active transaction (above)
             *  during a restart.
             *  Doesn't it seem I should be able to use ANY 
             *  reserved space in redo?  
             *  In order to debug any such case, we leave it like this.
             *  As I type, I'm thinking that if all is working correctly,
             *  we should never have insufficient space by doing this.
             */
            avail += xct_rsvd();
        }
    }
    w_assert3(nfree() >= nrsvd());
    w_assert3(nrsvd() >= xct_rsvd());
    w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);

    return avail;
}



/*********************************************************************
 *
 *  page_s::space_t::undo_release(amt, xd)
 *
 *  Undo the space release (i.e. re-acquire) for the xd. Amt bytes
 *  are reclaimed.
 *
 *  NOTE: this is called ONLY for a transaction that is being
 *  rolled back, either because it has aborted (state is xct_aborting)
 *  or because we've called rollback and we are in_recovery_undo() (state
 *  is still xct_active).
 *
 *  And it is called ONLY in one place: space_t::acquire
 *
 *  This could get called for an aborting transaction by, say,
 *  when undoing a page_mark (and that might be the only way),
 *  and a page_mark is logged by mark_free, which is what would
 *  happen if the tx freed a slot.
 *  So, in this example, a tx in forward processing destroys a record
 *  (page_p::mark_free(idx)), which logs a page_mark; then this tx
 *  aborts, so the mark_free is undone, which calls space_t::acquire.
 *  When the record was destroyed, the space for the slot array entry
 *  was not recycled if this is a rsvd_mode page.
 *  In undo, the space_t::acquire recognizes that this 
 *  reacquire does NOT in fact  have to reacquire the slot_t entry; 
 *  it just has to reacquire the space for the record.
 *
 *********************************************************************/
void 
page_s::space_t::undo_release(int amt, xct_t* xd)
{
    DBG(<<"{space_t::undo_release  amt=" << amt 
                    << " _tid=" << _tid
                    << " nfree()=" << nfree()
                    << " nrsvd()=" << nrsvd()
                    << " xct_rsvd()=" << xct_rsvd()
       );
    w_assert2(nfree() >= nrsvd());
    w_assert2(nrsvd() >= xct_rsvd());
    w_assert2(nfree() >= 0);
    w_assert2(xct_rsvd() >= 0);
    w_assert2(nrsvd() >= 0);

    _nfree -= amt;

    // NB: in rollback, we don't free the slot bytes, so they don't get
    // added into the _nrsvd. Thus the _nrsvd can end up smaller than
    // the amount that we are wanting to reacquire.  
    _nrsvd -= amt;
    if (xd && xd->tid() == _tid) 
            _xct_rsvd -= amt;

    DBG(<<" space_t::undo_release amt=" << amt 
                    << " _tid=" << _tid
                    << " nfree()=" << nfree()
                    << " nrsvd()=" << nrsvd()
                    << " xct_rsvd()=" << xct_rsvd()
                    << "}"
                    );
    w_assert1(nrsvd() >= 0);
    w_assert1(xct_rsvd() >= 0);
    w_assert1(nfree() >= 0);
    w_assert3(nrsvd() >= xct_rsvd());
    w_assert1(nfree() >= nrsvd()); // NEW
}


/*********************************************************************
 *
 *  page_s::space_t::undo_acquire(amt, xd)
 *
 *  Undo the space acquire (i.e. re-release) for the xd. Amt 
 *  bytes are freed.
 *
 *  Called:
 *  A) when we did an acquire but subsequently failed (on logging)
 *  and find we have to manually undo the acquire.  This manual-undoing
 *  is because the space_t changes are unlogged.
 *  (The page should be EX-latched during all this, so there shouldn't 
 *  be anything racy about it).
 *  A-1) page_p::reclaim : rsvd_mode()
 *  A-2) page_p::insert_expand : !rsvd_mode()
 *  A-3) page_p::splice : could be either one
 *  B) to effect a space_t::release in the aborting case -- gets called
 *      only in the rsvd_mode() case here.
 *
 *  The reason we'd treat the aborting case differently from the
 *  forward-processing case is that in the forward case,
 *  we might update the _tid, (if the releasing tx is younger than
 *  that on the page), whereas in the rollback case, we do not
 *  update the _tid.
 *  In either case, we do update the transaction's reserved space if
 *  this is the tx that matches that on the page.
 *
 *********************************************************************/
void 
page_s::space_t::undo_acquire(int amt, xct_t* xd)
{
    DBG(<<"{space_t::undo_acquire amt=" << amt 
                        << " _tid=" << _tid
                        << " nfree()=" << nfree()
                        << " nrsvd()=" << nrsvd()
                        << " xct_rsvd()=" << xct_rsvd()
                        );
    w_assert3(nfree() >= nrsvd());
    w_assert3(nrsvd() >= xct_rsvd());
    w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);

    _nfree += amt;

    /*
    * We shouldn't be touching the reserved-space info unless this
    * is a rsvd_mode() page, in which case rflag() would be true.
    * Update the _nrsvd only if this tx is the one noted on the page
    * or is OLDER than that one.  If this xct is YOUNGER than the
    * one noted on the page (the youngest one to release in 
    * forward processing) we won't increase the _nrsvd. 
    * That means that we won't be able to re-use this space if we
    * subsequently try again to acquire (case A above)
    * or have to undo a release (case B above). 
    * WHY:
    * If the _tid on the page is older than this xct's tid, then
    * I/this xct did NOT reserve space on this page, and so
    * I will not need any to finish my abort.
    * Furthermore, the youngest tx to 
    * reserve space on the page might have committed (_tid would be null),
    * and zeroed the _nrsvd, so I don't want to increase the reserved
    * space and throw off anyone else who wants to use it.
    * On the other hand, if I did or could have reserved space on the
    * page, I'll need to use it and so I'd better keep _nrsvd up-to-date.
    * All this should apply to rollback_work as well.
    */
    if (rflag() && xd && _tid >= xd->tid())  
    {
        _nrsvd += amt; // update highwater mark below

        if (_tid == xd->tid())
            _xct_rsvd += amt;

    }
    DBG(<<"space_t::undo_acquire amt=" << amt 
                        << " _tid=" << _tid
                        << " nfree()=" << nfree()
                        << " nrsvd()=" << nrsvd()
                        << " xct_rsvd()=" << xct_rsvd()
                        << "}"
                        );
    w_assert3(nfree() >= nrsvd());
    w_assert3(nrsvd() >= xct_rsvd());
    w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);
}



/*********************************************************************
 *
 *  page_s::space_t::acquire(amt, xd, do_it)
 *
 *  Acquire amt bytes for the transaction xd.
 *  The amt bytes will affect the reserved space on the page. 
 *  The slot_bytes will not change the reserved space on the
 *  page.  This is necessary, since space for slots for destroyed
 *  records cannot be returned to the free pool since slot
 *  numbers cannot change.  (See comments for space_t::release)
 *
 *  If do_it is false, don't actually do the update (OR rollback), just
 *  return RCOK or RC(smlevel_0::eRECWONTFIT);
 *
 *********************************************************************/
rc_t
page_s::space_t::acquire(int amt, int slot_bytes, xct_t* xd,
        bool do_it /*=true */)
{

#ifdef W_TRACE
    {
    tid_t _tmp; // in support of debug print
    if(xd) _tmp=xd->tid(); // in support of debug print
    DBG(<<"{space_t::acquire amt=" << amt << " slot_bytes=" << slot_bytes
                        << " _tmp=" << _tmp
                        << " nfree()=" << nfree()
                        << " nrsvd()=" << nrsvd()
                        << " xct_rsvd()=" << xct_rsvd()
                        << " doit=" << do_it
                        );
    }
#endif
    w_assert2(nfree() >= nrsvd());
    w_assert2(nrsvd() >= xct_rsvd());
    w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);

    // NOTE: !do_it trumps all
    if(do_it) if (
        rflag() &&  // rflag(): this is a space-reserving page (e.g. file_p)
         xd && 
         ((xd->state() == smlevel_1::xct_aborting)
          || smlevel_0::in_recovery_undo())
        )  
        // NOTE: if, in recovery and there is more than one xct to roll back,
        // it uses rollback(to-lsn) to make sure they are all rolled back
        // in chronological order.  This kind of rollback happens with the
        // xct state as xct_active. So we can be rolling back here, while
        // the state is active.  Similarly if the client invokes
        // rollback_work.
    {
        /*
         *  For aborting transaction ...
         */
        undo_release(amt, xd);
        DBG(<<"aborting tx -- did undo release of " << amt << "}" );

        w_assert3(nfree() >= nrsvd());
        w_assert3(nrsvd() >= xct_rsvd());
        w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);

        return RCOK;
    }
    
    int avail = usable(xd);
    int total = amt + slot_bytes;
    int avail4slots = usable_for_new_slots();

    if (avail < total 
            || avail4slots < slot_bytes
    )  
    {

        DBG(<<"eRECWONTFIT }" );
        DBG(<< "space_t::acquire @ " << __LINE__
            << " : avail " << avail
            << " amt " << amt
            << " avail for slots " << avail4slots
            << " slot_bytes " << slot_bytes); 
        return RC(smlevel_0::eRECWONTFIT);
    }
    if( !do_it)  return RCOK;
    
    /*
     *  Consume the space
     */

    w_assert1(nfree() >= total);
    _nfree -= total;

    /*
    * Q: Why were we not subtracting total below?
    * A: because the slot_bytes space cannot be re-used, and we
    * are keeping track of what CAN be re-used with nrsvd and xct_rsvd.
    * All new slot-array entries have to come out of nfree-nrsvd. That way,
    * if a tx has reserved most of the page, we can't have the slot array
    * expand into that space, interfering with rollback.
    */
    if (rflag() && xd && xd->tid() == _tid) {
        w_assert1(nrsvd() >= xct_rsvd());
        if (amt > xct_rsvd())  
        {
            /*
             *  Use all of xct_rsvd()
             */
            _nrsvd -= xct_rsvd();
            _xct_rsvd = 0;
        } else {
            /*
             *  Use portion of xct_rsvd()
             */
            _nrsvd -= amt;
            _xct_rsvd -= amt;
        }
    }

    DBG(<<" space_t::acquire amt=" << amt 
                        << " slot_bytes=" << slot_bytes
                        << " _tid=" << tid()
                        << " nfree()=" << nfree()
                        << " nrsvd()=" << nrsvd()
                        << " xct_rsvd()=" << xct_rsvd()
                        << "}"
                        );
    
    w_assert2(nfree() >= nrsvd());
    w_assert2(nrsvd() >= xct_rsvd());
    w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);
    return RCOK;
}
        

/*********************************************************************
 *
 *  page_s::space_t::release(amt, xd)
 *
 *  Release amt bytes on the page for transaction xd.
 *  Amt does not include bytes consumed by the slot.  
 *  --> The caller has to enforce this.
 *  Those bytes are never returned to the free space except
 *  A) when manually undoing an acquire while still holding the
 *  page EX-latched and 
 *  B) when this isn't a rsvd_mode()/rflag() page  
 *  The slots are marked free, and the file code searches for a free slot to
 *  nab before it tries to allocate a new one.  So slot bytes
 *  are consumed with space_t::acquire but never released.
 *
 *  For rflag() cases :
 *  If we are in forward processing, this is the case of removing
 *  a record, say, in which case we increase the reserved space, and
 *  we might be the youngest tx to release space on this page, so we
 *  might update the _tid, which resets the _xct_rsvd.
 *  But if we are rolling back,  we handle things differently,
 *  so we call undo_acquire in that case.
 *
 *********************************************************************/
void page_s::space_t::release(int amt, xct_t* xd)
{
    DBG(<<"{space_t::release amt=" << amt 
                        << " _tid=" << _tid
                        << " nfree()=" << nfree()
                        << " nrsvd()=" << nrsvd()
                        << " xct_rsvd()=" << xct_rsvd()
                       );
    w_assert3(nfree() >= nrsvd());
    w_assert3(nrsvd() >= xct_rsvd());
    w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);

    if (rflag() && xd && 
            (xd->state() == smlevel_1::xct_aborting
            || smlevel_0::in_recovery_undo() 
            )
        // NOTE: if, in recovery and there is more than one xct to roll back,
        // it uses rollback(to-lsn) to make sure they are all rolled back
        // in chronological order.  This kind of rollback happens with the
        // xct state as xct_active. So we can be rolling back here, while
        // the state is active.  Similarly if the client invokes
        // rollback_work.
        // This case needs to be treated like the abort case
        // for the reason that we use undo_acquire rather than release,
        // see the comments for space_t::undo_acquire()
    )  
    {
        /*
         *  For aborting transaction ...
         */
        undo_acquire(amt, xd);

        w_assert3(nfree() >= nrsvd());
        w_assert3(nrsvd() >= xct_rsvd());
        w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);
        DBG(<< "space_t::release @ " << __LINE__
                << ": nrsvd() "  << nrsvd()
                << " nfree() " << nfree()
                << " amt " << amt
                << " xct_rsvd() " << xct_rsvd());

        return;
    }
    
    if (rflag()) _check_reserve();
    _nfree += amt;
    if (rflag()) {
        if (xd)  {
            _nrsvd += amt;        // reserve space for this xct
            if ( _tid == xd->tid())  {
                                        // I am still the youngest...
                _xct_rsvd += amt;         // add to my reserved
            } else if ( _tid < xd->tid() ) {
                                        // I am the new youngest.
                _tid = xd->tid();        // assert: _tid >= xct()->tid
                                        // until I have completed
                _xct_rsvd = amt;
            }
        }
    }
    DBG(<<"space_t::release amt=" << amt 
                        << " _tid=" << _tid
                        << " nfree()=" << nfree()
                        << " nrsvd()=" << nrsvd()
                        << " xct_rsvd()=" << xct_rsvd()
                        << "}" );
    w_assert3(nfree() >= nrsvd());
    w_assert3(nrsvd() >= xct_rsvd());
    w_assert1(nfree() >= 0 && nrsvd() >= 0 && xct_rsvd() >= 0);
}

/*--------------------------------------------------------------*
 *  page_p::repair_rec_lsn()
 *
 *  Ensures the buffer pool's rec_lsn for this page is no larger than
 *  the page lsn. If not repaired, the faulty rec_lsn can lead to
 *  recovery errors. Invalid rec_lsn can occur when
 *  1) there are unlogged updates to a page -- extlink_t::update_pbucketmap 
 *  and log redo, for instance, or updates to a tmp page.
 *  2) when a clean page is fixed in EX mode but an update is never
 *  made and the page is unfixed.  Now the rec_lsn reflects the tail
 *  end of the log but the lsn on the page reflects something earlier.
 *  At least in this case, we should expect the bfcb_t to say the
 *  page isn't dirty.
 *  3) when a st_tmp page is fixed in EX mode and an update is 
 *  made and the page is unfixed.  Now the rec_lsn reflects the tail
 *  end of the log but the lsn on the page reflects something earlier.
 *  In this case, the page IS dirty.
 *
 *  FRJ: I don't see any evidence that this function is actually
 *  called because of (3) above...
 *--------------------------------------------------------------*/
void
page_p::repair_rec_lsn(bool was_dirty, lsn_t const &new_rlsn) {
    bfcb_t* bp = bf_m::get_cb(_pp);
    const lsn_t &rec_lsn = bp->curr_rec_lsn();
	w_assert2(is_latched_by_me());
	w_assert2(is_mine());
    if(was_dirty) {
        // never mind!
        w_assert0(rec_lsn <= lsn());
    }
    else {
        w_assert0(rec_lsn > lsn());
        if(new_rlsn.valid()) {
            w_assert0(new_rlsn <= lsn());
            w_assert2(bp->dirty());
            bp->set_rec_lsn(new_rlsn);
			INC_TSTAT(restart_repair_rec_lsn);
        }
        else {
            bp->mark_clean();
        }
    }
}

/*********************************************************************
 *
 *  page_p::tag_name(tag)
 *
 *  Return the tag name of enum tag. For debugging purposes.
 *
 *********************************************************************/
const char* 
page_p::tag_name(tag_t t)
{
    switch (t) {
    case t_extlink_p: 
        return "t_extlink_p";
    case t_stnode_p:
        return "t_stnode_p";
    case t_keyed_p:
        return "t_keyed_p";
    case t_btree_p:
        return "t_btree_p";
    case t_rtree_p:
        return "t_rtree_p";
    case t_file_p:
        return "t_file_p";
    case t_ranges_p:
        return "t_ranges_p";
    case t_file_mrbt_p:
        return "t_file_mrbt_p";
    default:
        W_FATAL(eINTERNAL);
    }

    W_FATAL(eINTERNAL);
    return 0;
}



/*********************************************************************
 *
 *  page_p::_format(pid, tag, page_flags, store_flags, log_it)
 *
 *  Called from page-type-specific 4-argument method:
 *    xxx::format(pid, tag, page_flags, store_flags)
 *
 *  Format the page with "pid", "tag", and "page_flags" 
 *        and store_flags (goes into the log record, not into the page)
 *        If log_it is true, it issues a page_init log record
 *
 *********************************************************************/
rc_t
page_p::_format(const lpid_t& pid, tag_t tag, 
               uint4_t             page_flags, 
               store_flag_t /*store_flags*/
               ) 
{
    uint4_t             sf;

    w_assert3((page_flags & ~t_virgin) == 0); // expect only virgin 
    /*
     *  Check alignments
     */
    w_assert3(is_aligned(data_sz));
    w_assert3(is_aligned(_pp->data() - (char*) _pp));
    w_assert3(sizeof(page_s) == page_sz);
    w_assert3(is_aligned(_pp->data()));

    /*
     *  Do the formatting...
     *  ORIGINALLY:
     *  Note: store_flags must be valid before page is formatted
     *  unless we're in redo and DONT_TRUST_PAGE_LSN is turned on.
     *  NOW:
     *  store_flags are passed in. The fix() that preceded this
     *  will have stuffed some store_flags into the page(as before)
     *  but they could be wrong. Now that we are logging the store
     *  flags with the page_format log record, we can force the
     *  page to have the correct flags due to redo of the format.
     *  What this does NOT do is fix the store flags in the st_node.
     * See notes in bf_m::_fix
     *
     *  The following code writes all 1's into the page (except
     *  for store-flags) in the hope of helping debug problems
     *  involving updates to pages and/or re-use of deallocated
     *  pages.
     */
    sf = _pp->get_page_storeflags(); // save flags
#if defined(SM_FORMAT_WITH_GARBAGE) || defined(ZERO_INIT)
    /* NB -- NOTE -------- NOTE BENE
    *  Note this is not exactly zero-init, but it doesn't matter
    * WHAT we use to init each byte for the purpose of purify or valgrind
    */
    memset(_pp, '\017', sizeof(*_pp)); // trash the whole page
#endif

    // _pp->set_page_storeflags(sf); // restore flag
    this->set_store_flags(sf); // changed to do it through the page_p, bfcb_t
    // TODO: any assertions on store_flags?

#if W_DEBUG_LEVEL > 2
    if(
     (smlevel_0::operating_mode == smlevel_0::t_in_undo)
     ||
     (smlevel_0::operating_mode == smlevel_0::t_forward_processing)
    )  // do the assert below
    w_assert3(sf != st_bad);
#endif 

    /*
     * Timestamp the page with an lsn.
     * Old way: page format always started it out with this low lsn:
     *       _pp->lsn1 = _pp->lsn2 = lsn_t(0, 1);
     * Unfortunately, none of the page updates get logged if the page
     * is st_tmp, so we can end up with the following bogus situation
     * if a regular page gets deallocated and re-used as a tmp page
     * without any checkpoints happening, but with the page being flushed
     * to disk, then restart. 
     *
     * The restart code says: is this log record(for a page update while
     * the page was regular)'s lsn > the page's lsn? if so, redo the
     * log record.  Ah, but formatting the page as st_tmp makes *sure* that's 
     * the case, because the lsn is set to be low and stays that way.
     *
     * Unfortunately, sometimes the log record should NOT be applied, and
     * in particular, it should not here.
     *
     * So we'll timestamp the page with the latest lsn from this xct.
     * We can be sure that this xct has a non-null lsn because the
     * page allocation had to be logged.
     */
    lsn_t l = lsn_t(0, 1);
    xct_t *xd = xct();
    // Note : formatting a volume gets done outside a tx,
    // so in that case, the lsn_t(0,1) is used.  If DONT_TRUST_PAGE_LSN (always,
    // now)
    // is turned off, the raw page has lsn_t(0,1) when read from disk
    // for the first time, if, in fact, it's actually read.
    if(xd) {
        l = xd->last_lsn();
        w_assert2(l.valid() || !log);
    }
#if W_DEBUG_LEVEL > 1
    if(sf == st_tmp)
    {
        // We never format a volume with st_tmp
        w_assert2(l.valid() || !log);
    }
    if(!l.valid()) {
        w_assert2( xd == NULL || !log );
    }
#endif

    _pp->lsn1 = _pp->lsn2 = l;
    _pp->pid = pid;
    _pp->next = _pp->prev = 0;
    _pp->page_flags = page_flags;
     w_assert3(tag != t_bad_p);
    _pp->tag = tag;  // must be set before rsvd_mode() is called
    _pp->space.init_space_t(data_sz + 2*sizeof(slot_t), rsvd_mode() != 0);
    _pp->end = _pp->nslots = _pp->nvacant = 0;


    if(_pp->tag != t_file_p || _pp->tag != t_file_mrbt_p) {
        /* 
         * Could have been a t_file_p when fix() occured,
         * but format could have changed it.  Make the
         * bucket check unnecessary.
         */
        // check on unfix is notnecessary 
        page_bucket_info.nochecknecessary();
    }

    return RCOK;
}

/*********************************************************************
 *
 *  page_p::_fix(bool,
 *    pid, ptag, mode, page_flags, store_flags, ignore_store_id, refbit)
 *
 *
 *  Fix a frame for "pid" in buffer pool in latch "mode". 
 *
 *  "Ignore_store_id" indicates whether the store ID
 *  on the page can be trusted to match pid.store; usually it can, 
 *  but if not, then passing in true avoids an extra assert check.
 *  "Refbit" is a page replacement hint to bf when the page is 
 *  unfixed.
 *
 *  NB: this does not set the tag() to ptag -- format does that
 *
 *********************************************************************/
rc_t
page_p::_fix(
    bool                condl,
    const lpid_t&        pid,
    tag_t                ptag,
    latch_mode_t         m, 
    uint4_t              page_flags,
    store_flag_t&        store_flags,//used only if page_flags & t_virgin
    bool                 ignore_store_id, 
    int                  refbit)
{
    w_assert3(!_pp || bf->is_bf_page(_pp, false));
    store_flag_t        ret_store_flags = store_flags;

    // -- for page tracing
    //pthread_mutex_lock(&_glmutex);
    //gettimeofday(&curr_time, NULL);
    //_accesses << "Page: " << pid << " Thread: " << pthread_self() << " Latch mode: " << m
    //	      << " Time: sec-> " << curr_time.tv_sec << " usec-> " << curr_time.tv_usec << endl;
    //pthread_mutex_unlock(&_glmutex);
    // --
    
    if (store_flags & st_insert_file)  {
        store_flags = (store_flag_t) (store_flags|st_tmp); 
        // is st_tmp and st_insert_file
    }
    /* allow these only */
    w_assert1((page_flags & ~t_virgin) == 0);

    if (_pp && _pp->pid == pid) 
        {
        if(_mode >= m)  {
            /*
             *  We have already fixed the page... do nothing.
             */
        } else if(condl) {
              bool would_block = false;
              bf->upgrade_latch_if_not_block(_pp, would_block);
              if(would_block)
                       return RC(sthread_t::stINUSE);
              w_assert2(_pp && bf->is_bf_page(_pp, true));
              _mode = bf->latch_mode(_pp);
        } else {
            /*
             *  We have already fixed the page, but we need
             *  to upgrade the latch mode.
             */
            bf->upgrade_latch(_pp, m); // might block
                        w_assert2(_pp && bf->is_bf_page(_pp, true));
            _mode = bf->latch_mode(_pp);
            w_assert3(_mode >= m);
        }
    } else {
        /*
         * wrong page or no page at all
         */

        if (_pp)  {
            /*
             * If the old page type calls for it, we must
             * update the space-usage histogram info in its
             * extent.
             */
            W_DO(update_bucket_info());
            bf->unfix(_pp, false, _refbit);
            _pp = 0;
        }


        if(condl) {
            W_DO( bf->conditional_fix(_pp, pid, ptag, m, 
                      (page_flags & t_virgin) != 0,  // no_read
                      ret_store_flags,
                      ignore_store_id, store_flags) );
                        w_assert2(_pp && bf->is_bf_page(_pp, true));
        } else {
            W_DO( bf->fix(_pp, pid, ptag, m, 
                      (page_flags & t_virgin) != 0,  // no_read
                      ret_store_flags,
                      ignore_store_id, store_flags) );
                        w_assert2(_pp && bf->is_bf_page(_pp, true));
        }

#if W_DEBUG_LEVEL > 2
        if( (page_flags & t_virgin) != 0  )  {
            if(
             (smlevel_0::operating_mode == smlevel_0::t_in_undo)
             ||
             (smlevel_0::operating_mode == smlevel_0::t_forward_processing)
            )  // do the assert below
            w_assert3(ret_store_flags != st_bad);
        }
#endif 
        _mode = bf->latch_mode(_pp);
        w_assert3(_mode >= m);
    }

    _refbit = refbit;
    // if ((page_flags & t_virgin) == 0)  {
        // file pages must have have reserved mode set
        // this is a bogus assert, since
        // the definition of rsvd_mode()
        // became tag() == t_file_p
        // w_assert3((tag() != t_file_p) || (rsvd_mode()));
    // }
    init_bucket_info(ptag, page_flags);
    w_assert3(_mode >= m);
    store_flags = ret_store_flags;

    w_assert2(is_fixed());
    w_assert2(_pp && bf->is_bf_page(_pp, true));
    return RCOK;
}

bool                         
page_p::is_latched_by_me() const
{
    return _pp ? bf->fixed_by_me(_pp) : false;
}

const latch_t *                         
page_p::my_latch() const
{
    return _pp ? bf->my_latch(_pp) : NULL;
}

bool                         
page_p::is_mine() const
{
    return _pp ? bf->is_mine(_pp) : false;
}

rc_t 
page_p::fix(
    const lpid_t&        pid, 
    tag_t                ptag,
    latch_mode_t         mode, 
    uint4_t              page_flags,
    store_flag_t&        store_flags, 
    bool                 ignore_store_id, 
    int                  refbit 
)
{
    return _fix(false, pid, ptag, mode, page_flags, store_flags,
                    ignore_store_id, refbit); 
}

rc_t 
page_p::conditional_fix(
    const lpid_t&         pid, 
    tag_t                ptag,
    latch_mode_t         mode, 
    uint4_t                 page_flags, 
    store_flag_t&         store_flags,
    bool                 ignore_store_id, 
    int                   refbit 
)
{
    return _fix(true, pid, ptag, mode, page_flags, store_flags,
            ignore_store_id, refbit);
}

/*********************************************************************
 *
 *  page_p::link_up(new_prev, new_next)
 *
 *  Sets the previous and next pointer of the page to point to
 *  new_prev and new_next respectively.
 *
 *
 *********************************************************************/
rc_t
page_p::link_up(shpid_t new_prev, shpid_t new_next)
{
    /*
     *  Log the modification
     */
    W_DO( log_page_link(*this, new_prev, new_next) );

    /*
     *  Set the pointers
     */
    _pp->prev = new_prev, _pp->next = new_next;

    return RCOK;
}

/*********************************************************************
 *
 *  page_p::mark_free(idx)
 *
 *  Mark the slot at idx as free.
 *  This sets its length to 0 and offset to 1.
 *  This is called from 
 *  1) file_p::destroy_rec, never with index 0.
 *  2) page_mark_t::redo / page_mark_log::redo
 *  3) A page_mark_t is also a page_reclaim_t, so this is also called
 *     from  page_reclaim_t::undo / page_reclaim_log::undo.
 *
 *  Redo never logs anything, so the page_mark_t can only appear in the
 *  log from a page reclaim being undone (3 above), 
 *  or from forward processing of a destroy rec. (1 above).
 *
 *  Destroy_rec never destroys slot 0
 *  but page reclaim can reclaim slot 0 (for the file_p_hdr_t).
 *  Consequently, if in fwd processing and not undoing, we are destroying
 *  a record and idx > 0,
 *  but if undoing a reclaim, we can have idx == 0; this happens while undoing
 *  a page format (part-2 of the page format).
 *   (Note that in redo we can be redoing the undo of a reclaim so in
 *   redo pass, idx can be anything.)
 *
 *  The page format is NOT compensated-around (only the allocation of the
 *  page in an extent is compensated), so if we are undoing a page allocation,
 *  this page might remain in the file, even while the
 *  page format gets undone. So while we have this latched, we have to
 *  check that there are no other slots in use before we clobber slot 0.
 *
 *********************************************************************/
rc_t
page_p::mark_free(slotid_t idx)
{
    /*
     *  Only valid for pages that need space reservation
     */
    w_assert1( rsvd_mode() );

    /*
     * A transaction must be attached.
     */
    w_assert2( in_recovery_redo() || xct() );

    /*
     *  More sanity checks
     */
#if W_DEBUG_LEVEL > 0
    if (!(idx >= 0 && idx < _pp->nslots) ) {
        // Dump some info before we hit the assert
        // directly below this.
        w_ostrstream s;
        s   << " slot " << idx
            << " #pages slots " << _pp->nslots
            << " page " << _pp->pid
            << " tag " << tag()
            << " page lsn " << _pp->lsn1
            ;
        fprintf(stderr, "Bogus mark free: %s\n", s.c_str());
    }
#endif

    w_assert1(idx >= 0 && idx < _pp->nslots);
    w_assert1(_pp->slot(idx).offset >= 0);


    /*
     * Should we deallocate the page?
     *
     * This check necessary only for file_p.
     *
     * If idx==0 then we are either in redo
     * or undo of a format of a new file page.
     * Later in the undo process we'll try to deallocate this
     * page. However, it will only do the dealloc for pages which we can
     * lock in EX mode.
     *
     * FRJ: We only set EX mode if there are no other slots in use for this
     * page *and* if no other trx holds a lock, thus preventing us
     * from deallocating a page somebody else is using. (Note that this
     * is not sufficient to ensure that the page hasn't been deallocated
     * already.)
     */
    if(idx == 0 && (tag() == t_file_p || tag() == t_file_mrbt_p)) 
    {
      w_assert1(latch_mode() == LATCH_EX);
      /*
       * Slot 0 is file_p_hdr_t, never a user-created record.
       * If this is slot 0, we are being called from redo, or
       * undo of a page format.
       *
       */
      if(_pp->nslots - _pp->nvacant > 1/*slot 0 == page header*/) {
          // 1 for slot header. More than one means don't remove slot 0
          return RCOK;
      } // OW drop through and remove slot 0 

    }

    /*
     *  Log the action
     */
    W_DO( log_page_mark(*this, idx) ); /* mark idx free */

    /*
     *  Release space and mark free
     *  We do not release the space for the slot table entry. The
     *  slot table is never shrunk on reserved-space pages.
     */
    _pp->space.release(int(align(_pp->slot(idx).length)), xct());
    _pp->slot(idx).length = 0;
    _pp->slot(idx).offset = -1;
    ++_pp->nvacant;

    return RCOK;
}
    


/*********************************************************************
 *
 *  page_p::reclaim(idx, vec, log_it)
 *
 *  Reclaim the slot at idx and fill it with vec. The slot could
 *  be one that was previously marked free (mark_free()), or it
 *  could be a new slot at the end of the slot array.
 *
 *  Callers: file manager (file.h, file.cpp)
 *
 *********************************************************************/

rc_t
page_p::reclaim(slotid_t idx, const cvec_t& vec, bool log_it)
{
#ifdef W_TRACE
{
    xct_t* xd = xct(); // in support of debug print
    tid_t _tid; // in support of debug print
    if(xd) _tid=xd->tid(); // in support of debug print
    DBG(<<"{reclaim  idx=" << idx << " vec.size=" << vec.size()
                        << " _tid=" << _tid
                        << " nfree()=" << nfree()
                        << " nrsvd()=" << nrsvd()
                        << " xct_rsvd()=" << xct_rsvd()
                        );
}
#endif /* W_TRACE */
    /*
     *  Only valid for pages that need space reservation
     *  This means that the kind of page is a file_p.
     *  That's crucial for checking the page header, below.
     */
    w_assert1( rsvd_mode() );

    /*
     *  Sanity check
     */
    w_assert1(idx >= 0 && idx <= _pp->nslots);

    /*
     *  Compute # bytes needed. If idx is a new slot, we would
     *  need space for the slot as well.
     */
    smsize_t need = align(vec.size());
    smsize_t need_slots = (idx == _pp->nslots) ? sizeof(slot_t) : 0;

    /*
     *  Acquire the space ... return error if failed.
     */
    W_DO(_pp->space.acquire(need, need_slots, xct()));

    // Always true:
    // if( rsvd_mode() ) {
        W_DO(update_bucket_info());
    // }

    if(log_it) {
        /*
         *  Log the reclaim. 
         */
        w_rc_t rc = log_page_reclaim(*this, idx, vec); // acquire slot idx
        if (rc.is_error())  {
            /*
             *  Cannot log ... manually release the space acquired
             */

            _pp->space.undo_acquire(need, xct());

            // Always true:
            // if( rsvd_mode() ) {
                W_COERCE(update_bucket_info());
            // }
            DBG(<<"log page reclaim failed at line " <<  __LINE__ );
            return RC_AUGMENT(rc);
        }
    }

    /*
     *  Log has already been generated ... the following actions must
     *  succeed!
     */
    // Q : why is need_slots figured in contig_space() ?
    // A : because need_slots is 0 if we aren't allocating
    // a new slot.
    

    if (contig_space() < need + need_slots) {
        /*
         *  Shift things around to get enough contiguous space
         */
        _compress((idx == _pp->nslots ? -1 : idx));
    }
    w_assert1(contig_space() >= need + need_slots);
    
    slot_t& s = _pp->slot(idx);
    if (idx == _pp->nslots)  {
        /*
         *  Add a new slot
         */
        _pp->nslots++;
    } else {
        /*
         *  Reclaim a vacant slot
         */
        w_assert1(s.length == 0);
        w_assert1(s.offset == -1);
        w_assert1(_pp->nvacant > 0);
        --_pp->nvacant;
    }
    
    /*
     *  Copy data to page
     */
    // make sure the slot table isn't getting overrun
    char* target = _pp->data() + (s.offset = _pp->end);
    w_assert3((caddr_t)(target + vec.size()) <= 
              (caddr_t)&_pp->slot(_pp->nslots-1));
    vec.copy_to(target);
    _pp->end += int(align( (s.length = vec.size()) ));

    W_IFDEBUG3(W_COERCE(check()));

#ifdef W_TRACE
{
    xct_t* xd = xct(); // in support of debug print
    tid_t _tid; // in support of debug print
    if(xd) _tid=xd->tid(); // in support of debug print
    DBG(<<" reclaim  idx=" << idx << " vec.size=" << vec.size()
                        << " _tid=" << _tid
                        << " nfree()=" << nfree()
                        << " nrsvd()=" << nrsvd()
                        << " xct_rsvd()=" << xct_rsvd()
                        << "}" );

}
#endif /* W_TRACE*/
    
    return RCOK;
}

 

   
/*********************************************************************
 *
 *  page_p::find_slot(space_needed, ret_idx, start_search)
 *
 *  Find a slot in the page that could accomodate space_needed bytes.
 *  Return the slot in ret_idx.  Start searching for a free slot
 *  at location start_search (default == 0).
 *
 *********************************************************************/
rc_t
page_p::find_slot(uint4_t space_needed, 
        slotid_t& ret_idx, slotid_t start_search)
{
    /*
     *  Only valid for pages that need space reservation
     *  Also, even if start_search is at 0, we will never
     *  return slot 0 because it's always used for the
     *  file_p_hdr_t (file_s.h), which contains nothing but
     *  the cluster id 
     */
    w_assert1( rsvd_mode() ); // t_file_p

    /*
     *  Check for sufficient space.
     *  usable_space_for_slots() takes into account that 
     *  space reserved by this xct cannot be used to expand the
     *  slot table.
     */
    if ( usable_space() < space_needed 
            ||
        usable_space_for_slots() < sizeof(slot_t)  )
    {
        return RC(eRECWONTFIT);
    }

    /*
     * usable_space() has side effect of possibly
     * freeing space and changing the bucket
     * so we have to update the bucket info
     */
    W_DO(update_bucket_info());


    /*
     *  Find a vacant slot (could be a new slot)
     */
    slotid_t idx = _pp->nslots;
    if (_pp->nvacant) {
        for (slotid_t i = start_search; i < _pp->nslots; ++i) {
            if (_pp->slot(i).offset == -1)  {
                w_assert3(_pp->slot(i).length == 0);
                idx = i;
                break;
            }
        }
    }
    
    // slot must be > 0 since we will already have
    // assigned slot 0 to the file_p_hdr_t.
    w_assert2(idx > 0);
    w_assert3(idx <= _pp->nslots);

    ret_idx = idx;

    // now that we know if we need to get a new slot, we had
    // better check again.
    if(idx == _pp->nslots) {
        if ( usable_space() < space_needed  + sizeof(slot_t) )
        {
            return RC(eRECWONTFIT);
        }
    }

    return RCOK;
}

rc_t
page_p::next_slot(
    uint4_t space_needed, 
    slotid_t& ret_idx
)
{
    /*
     *  Only valid for pages that need space reservation
     */
    w_assert1( rsvd_mode() ); // t_file_p

    /*
     *  Check for sufficient space
     *  usable_space_for_slots() takes into account that 
     *  space reserved by this xct cannot be used to expand the
     *  slot table.
     */
    
    if ( usable_space_for_slots() < sizeof(slot_t) ||
            usable_space() < space_needed)   
    {
        return RC(eRECWONTFIT);
    }

    /*
     *  Find a vacant slot at the end of the
     *  slot table or get a new slot
     */
    slotid_t idx = _pp->nslots;
    if (_pp->nvacant) {
        // search backwards, stop at first non-vacant
        // slot, and return lowest vacant slot above that
        for (int i = _pp->nslots-1; i>=0;  i--) {
            if (_pp->slot(i).offset == -1)  {
                w_assert3(_pp->slot(i).length == 0);
            } else {
                idx = i+1;
                break;
            }
        }
    }
    
    w_assert3(idx >= 0 && idx <= _pp->nslots);
    ret_idx = idx;

    // now that we know if we need to get a new slot, we had
    // better check again.
    if(idx == _pp->nslots) {
        if ( usable_space() < space_needed  + sizeof(slot_t) )
        {
            return RC(eRECWONTFIT);
        }
    }

    return RCOK;
}



/*********************************************************************
 *
 *  page_p::insert_expand(idx, cnt, vec[], bool log_it, bool do_it)
 *  If we think of a page as <hdr><slots/data> ... <slot-table>,
 *  "left" means higher-numbered slots, and "right" means lower-
 *  numbered slots.
 *
 *  Insert cnt slots starting at index idx. Slots on the left of idx
 *  are pushed further to the left to make space for cnt slots. 
 *  By this it's meant that the slot table entries are moved; the
 *  data themselves are NOT moved.
 *  Vec[] contains the data for these new slots. 
 *
 *  If !do_it, just figure out if there's adequate space
 *  If !log_it, don't log it
 *
 *********************************************************************/
rc_t
page_p::insert_expand(slotid_t idx, int cnt, const cvec_t *vec, 
        bool log_it, bool do_it)
{
    w_assert1(! rsvd_mode() ); // file pages can't do this
    w_assert1(idx >= 0 && idx <= _pp->nslots);
    w_assert1(cnt > 0);

    /*
     *  Compute the total # bytes needed 
     */
    uint total = 0;
    int i;
    for (i = 0; i < cnt; i++)  {
        total += int(align(vec[i].size()) + sizeof(slot_t));
    }

    /*
     *  Try to get the space ... could fail with eRECWONTFIT
     */
    DBG(<<"page_p::insert_expand idx=" << idx
                << " cnt=" << cnt << " log_it " << log_it
                << " do_it " << do_it); 
    W_DO( _pp->space.acquire(total, 0, xct(), do_it) );
    if(! do_it) return RCOK;

    w_assert3(! rsvd_mode() );        // Keep this here for clarity
    /*
     * NB: Don't have to update the bucket info because
     * this isn't called for rsvd_mode() pages.  If we decide
     * to use this for rsvd_mode() pages, have to update 
     * the bucket info here and below when logging fails.
     */

    if(log_it) {
        /*
         *  Log the insertion
         */
        rc_t rc = log_page_insert(*this, idx, cnt, vec);
        if (rc.is_error())  {
            /*
             *  Log failed ... manually release the space acquired
             */
            DBG(<<"page_p::insert_expand manually release space acquired");
            _pp->space.undo_acquire(total, xct());
            return RC_AUGMENT(rc);
        }
    }

    /*
     *  Log has already been generated ... the following actions must
     *  succeed!
     */

    if (contig_space() < total)  {
        /*
         *  Shift things around to get enough contiguous space
         */
        _compress();
        w_assert3(contig_space() >= total);
    }

    if (idx != _pp->nslots)    {
        /*
         *  Shift left neighbor slots further to the left
         */
        memmove(&_pp->slot(_pp->nslots + cnt - 1),
                &_pp->slot(_pp->nslots - 1), 
                (_pp->nslots - idx) * sizeof(slot_t));
    }

    /*
     *  Fill up the slots and data
     */
    register slot_t* p = &_pp->slot(idx);
    for (i = 0; i < cnt; i++, p--)  {
        p->offset = _pp->end;
        p->length = vec[i].copy_to(_pp->data() + p->offset);
        _pp->end += int(align(p->length));
    }

    _pp->nslots += cnt;
    
    W_IFDEBUG3( W_COERCE(check()) );

    return RCOK;
}




/*********************************************************************
 *
 *  page_p::remove_compress(idx, cnt)
 *
 *  Remove cnt slots starting at index idx. Up-shift slots after
 *  the hole to fill it up.
 *  If we think of a page as <hdr><slots/data> ... <slot-table>,
 *  "up" means low-numbered slots, and "after" means higher-
 *  numbered slots.
 *
 *********************************************************************/
rc_t
page_p::remove_compress(slotid_t idx, int cnt)
{
    w_assert1(! rsvd_mode() );
    w_assert1(idx >= 0 && idx < _pp->nslots);
    w_assert1(cnt > 0 && cnt + idx <= _pp->nslots);

#if W_DEBUG_LEVEL > 2
    int old_num_slots = _pp->nslots;
#endif 

    /*
     *  Log the removal
     */
    W_DO( log_page_remove(*this, idx, cnt) );

    /*
     *        Compute space space occupied by tuples
     */
    register slot_t* p = &_pp->slot(idx);
    register slot_t* q = &_pp->slot(idx + cnt);
    int amt_freed = 0;
    for ( ; p != q; p--)  {
        w_assert3(p->length < page_s::data_sz+1);
        amt_freed += int(align(p->length) + sizeof(slot_t));
    }

    /*
     *        Compress slot array
     */
    p = &_pp->slot(idx);
    q = &_pp->slot(idx + cnt);
    for (slot_t* e = &_pp->slot(_pp->nslots); q != e; p--, q--) *p = *q;
    _pp->nslots -= cnt;

    /*
     *  Free space occupied
     */
    _pp->space.release(amt_freed, xct());

    
#if W_DEBUG_LEVEL > 2
    W_COERCE(check());
    // If we've compressed more than what we expected,
    // we'll catch that fact here.
    w_assert3(old_num_slots - cnt == _pp->nslots);
#endif
    return RCOK;
}


/*********************************************************************
 *
 *  page_p::set_byte(slotid_t idx, op, bits)
 *
 *  Logical operation on a byte's worth of bits at offset idx.
 *
 *********************************************************************/
rc_t
page_p::set_byte(slotid_t idx, u_char bits, logical_operation op)
{
    w_assert3(latch_mode() == LATCH_EX );
    /*
     *  Compute the byte address
     */
    u_char* p = (u_char*) tuple_addr(0) + idx;
    // doesn't compile under vc++
    // DBG(<<"set_byte old_value=" << (unsigned(*p)) );

    /*
     *  Log the modification
     */
    W_DO( log_page_set_byte(*this, idx, *p, bits, op) );

    switch(op) {
    case l_none:
        break;

    case l_set:
        *p = bits;
        break;

    case l_and:
        *p = (*p & bits);
        break;

    case l_or:
        *p = (*p | bits);
        break;

    case l_xor:
        *p = (*p ^ bits);
        break;

    case l_not:
        *p = (*p & ~bits);
        break;
    }

    return RCOK;
}


/*********************************************************************
 *
 *  page_p::splice(idx, cnt, info[])
 *
 *  Splice the tuple at idx. "Cnt" regions of the tuple needs to
 *  be modified.
 *
 *********************************************************************/
rc_t
page_p::splice(slotid_t idx, int cnt, splice_info_t info[])
{
    DBGTHRD(<<"page_p::splice idx=" <<  idx << " cnt=" << cnt);
    for (int i = cnt; i >= 0; i--)  {
        // for now, but We should use safe-point to bail out.
        W_COERCE(splice(idx, info[i].start, info[i].len, info[i].data));
    }
    return RCOK;
}




/*********************************************************************
 *
 *  page_p::splice(idx, start, len, vec)
 *
 *  Splice the tuple at idx. 
 *  The range of bytes from start to start+len is replaced with vec. 
 *  If size of "vec" is less than  "len", the rest of the tuple is 
 *      moved in to fill the void.
 *  If size of "vec" is more than "len", the rest of the tuple is
 *      shoved out to make space for "vec". 
 *  If size of "vec" is equal  to "len", then those bytes are simply 
 *      replaced with "vec".
 *
 *********************************************************************/
rc_t
page_p::splice(slotid_t idx, slot_length_t start, slot_length_t len, const cvec_t& vec)
{
    FUNC(page_p::splice);
    DBGTHRD(<<"{page_p::splice idx=" <<  idx 
                    << " start=" << start
                    << " len=" << len
           );
    int vecsz = vec.size();
    w_assert1(idx >= 0 && idx < _pp->nslots);
    w_assert1(vecsz >= 0);

	// TEMP: to catch a problem in which we are using a page splice on an
	// extent map page, which seems wrong
	if ((idx==0) && (pid().page == 1)) 
	{
			static int count(0);
			count++;
			DBGTHRD(<< "count " << count);
	}

    slot_t& s = _pp->slot(idx);                // slot in question

    // Integrity check: the range start -> start+len must be in the
    // existing slot.
#if W_DEBUG_LEVEL > 1
    if((start + len <= s.length)==false) {
        cerr << "Assertion failure on page " 
            << _pp->pid
            << "lsn " <<_pp->lsn1
            << "s.length " << s.length
            << "buf start + len = " << start + len
            << endl;
    }
#endif
    w_assert1(start + len <= s.length);

    /*
     * need           : actual amount needed
     * adjustment : physical space needed taking alignment into acct 
     */
    int need = vecsz - len;
    int adjustment = int(align(s.length + need) - align(s.length));

    if (adjustment > 0) {
        /*
         *  Need more ... acquire the space
         */
        DBG(<<"space.acquire adjustment=" <<  adjustment );
        W_DO(_pp->space.acquire(adjustment, 0, xct()));
        if(rsvd_mode()) {
            W_DO(update_bucket_info());
        }
    }

    /*
     *  Figure out if it's worth logging
     *  the splice as a splice of zeroed 
     *  old or new data
     *
     *  osave is # bytes of old data to be saved
     *  nsave is # bytes of new data to be saved
     *
     *  The new data must be in a zvec if we are to 
     *  skip anything.  The old data are inspected.
     */
    int  osave=len, nsave=vec.size();
    bool zeroes_found=false;

    DBG(
        <<"start=" << start
        <<" len=" << len
        <<" vec.size = " << nsave
    ); 
    if(vec.is_zvec()) {
        DBG(<<"splice in " << vec.size() << " zeroes");
        nsave = 0; zeroes_found = true;
    }

    /*
    // Find out if the start through start+len are all zeroes.
    // Not worth this effort it if the old data aren't larger than
    // the additional logging info needed to save the space.
    */
#define FUDGE 0
    // check old
    if ((size_t)len > FUDGE + (2 * sizeof(int2_t))) {
        char        *c;
        int        l;
        for (l = len, c = (char *)tuple_addr(idx)+start;
            l > 0  && *c++ == '\0'; l--)
                ;

        DBG(<<"old data are 0 up to byte " << len - l
                << " l = " << l
                << " len = " << len
                );
        if(l==0) {
            osave = 0;
            zeroes_found = true;
        }
    }
    w_assert3(len <= smlevel_0::page_sz);

    /*
     *  Log the splice that we're about to do.
     */
    rc_t rc;

    if(zeroes_found) {
        DBG(<<"Z splice avoid saving old=" 
                << (len - osave) 
                << " new= " 
                << (vec.size()-nsave));
        rc = log_page_splicez(*this, idx, start, len, osave, nsave, vec);
    } else {
        DBG(<<"log splice idx=" <<  idx << " start=" << start
              << " len=" << len );
        rc = log_page_splice(*this, idx, start, len, vec);
    }
    if (rc.is_error())  {
        DBG(<<"LOG FAILURE rc=" << rc );
        /*
         *  Log fail ... release any space we acquired
         */
        if (adjustment > 0)  {
            _pp->space.undo_acquire(adjustment, xct());
            if(rsvd_mode()) {
                W_DO(update_bucket_info());
            }
        }
        return RC_AUGMENT(rc);
    }
    DBGTHRD(<<"adjustment =" << adjustment);

    if (adjustment == 0) {
        /* do nothing */

    } else if (adjustment < 0)  {
        /*
         *  We need less space: the tuple  got smaller.
         */
        DBG(<<"release:  adjustment=" << adjustment );
        _pp->space.release(-adjustment, xct());
        if(rsvd_mode()) {
            W_DO(update_bucket_info());
        }
        
    } else if (adjustment > 0)  {
        /*
         *  We need more space. Move tuple of slot idx to the
         *  end of the page so we can expand it.
         */
        w_assert3(need > 0);
        if (contig_space() < (uint)adjustment)  {
            /*
             *  Compress and bring tuple of slot(idx) to the end.
             */
            _compress(idx);
            w_assert1(contig_space() >= (uint)adjustment);
            
        } else {
            /*
             *  There is enough contiguous space for delta
             */
            if (s.offset + page_s::slot_offset_t(align(s.length)) == _pp->end)  {
                /*
                 *  last slot --- we can simply extend it 
                 */
            } else if (contig_space() > align(s.length + need)) {
                /*
                 *  copy this record to the end and expand from there
                 */
                memcpy(_pp->data() + _pp->end,
                       _pp->data() + s.offset, s.length);
                s.offset = _pp->end;
                _pp->end += int(align(s.length));
            } else {
                /*
                 *  No other choices. 
                 *  Compress and bring tuple of slot(idx) to the end.
                 */
                _compress(idx);
            }
        }

        _pp->end += adjustment; // expand
    } 

    /*
     *  Put data into the slot
     */
    char* p = _pp->data() + s.offset;
    if (need && (s.length != start + len))  {
        /*
         *  slide tail forward or backward
         *  NEH: we have to be careful here that we
         *  don't clobber what we are moving.  If the
         *  distance to be moved is greater than  
         *  the amount to be moved, we are safe, 
         *  but if there's some overlap, we
         *  have to move the overlapped part FIRST
         */
        int distance_moved = need;
        int amount_moved = s.length - start - len;

        w_assert1(amount_moved > 0);

        if(distance_moved > 0 && (amount_moved > distance_moved)) {
            /* 
             * do it in partial moves: first the part in the overlap,
             * in small enough chunks that there's no overlap: move
             * it all in distance_moved-sized chunks, starting at the tail
             */
            int  chunksize = distance_moved;
            int  amt2move = amount_moved; 
            char *from = ((p + start + len) + amt2move) - chunksize; 
            char *to= from + distance_moved;

            while (amt2move > 0) {
                w_assert1(amt2move > 0);
                w_assert1(chunksize > 0);
                DBG(<<"copying " << chunksize 
                        << " amt left=" << amt2move
                        << " from =" << W_ADDR(from)
                        << " to =" << W_ADDR(to)
                        );
                memcpy(to, from, chunksize);
                amt2move -= chunksize;
                chunksize = amt2move > chunksize ? chunksize : amt2move;
                to -= chunksize;
                from -= chunksize;
            }
            w_assert3(amt2move == 0);
        } else { 
            /*
             * if distance_moved < 0, we're moving left and won't
             * have any overlap
             */
            memcpy(p + start + len + distance_moved, 
               p + start + len, 
               amount_moved);
        }
    }
    if (vecsz > 0)  {
        w_assert3((int)(s.offset + start + vec.size() <= data_sz));
        // make sure the slot table isn't getting overrun
        w_assert3((caddr_t)(p + start + vec.size()) <= (caddr_t)&_pp->slot(_pp->nslots-1));
                
        vec.copy_to(p + start);
    }
    _pp->slot(idx).length += need;


#if W_DEBUG_LEVEL > 2
    W_COERCE(check());
#endif 
    DBGTHRD(<<"page_p::splice idx=" <<  idx 
                    << " start=" << start
                    << " len=" << len
                    << "}"
           );

    return RCOK;
}


/*********************************************************************
 *
 *  page_p::merge_slots(idx, off1, off2)
 *
 *  Merge tuples idx, idx+1, removing whatever's at the end of
 *  tuple idx, and the beginning of idx+2.   We cut out from
 *  off1 to the end of idx (inclusive), 
 *  and from the beginning to off2 (not inclusive) of idx+1.
 *
 *********************************************************************/
rc_t
page_p::merge_slots(slotid_t idx, slot_offset_t off1, slot_offset_t off2)
{
    W_IFDEBUG3( W_COERCE(check()) );
    int idx2 = idx+1;

    w_assert1(idx >= 0 && idx < _pp->nslots);
    w_assert1(idx2 >= 0 && idx2 < _pp->nslots);

    /*
     *  Log the merge as two splices (to save the old data)
     *  and a shift
     */
    rc_t rc;
    {
        slot_t& s = _pp->slot(idx);

        /* 
         * cut out out the end of idx
         */
        DBG(<<"cut " << idx << ","
                << off1 << ","
                << s.length-off1);
        W_DO(cut(idx, off1, s.length-off1));

        /* 
         * cut out out the beginning of idx2
         */
        DBG(<<"cut " << idx2 << ","
                << 0 << ","
                << off2 );

        W_DO(cut(idx2, 0, off2));
    }
    slot_t& s = _pp->slot(idx);
    slot_t& t = _pp->slot(idx2);

    DBG(<<"shift " << idx2 << "," << 0 << ","
            << t.length << "," << idx << "," << s.length);

    W_IFDEBUG3( W_COERCE(check()) );
    /* 
     * Shift does a _shift_compress 
     */
    rc =  page_p::shift(idx2, 0, t.length, idx, s.length);
    W_IFDEBUG3( W_COERCE(check()) );

    W_DO(remove_compress(idx2, 1));

    W_IFDEBUG3( W_COERCE(check()) );
    return rc;
}

/*********************************************************************
 *
 *  page_p::split_slot(idx, off,  vec_t v1, vec_t v2)
 *
 *  Split slot idx at offset off (origin 0)
 *     (split is between off-1 and off) into two slots: idx, idx+1 
 *  AND insert v1 at the end of slot idx, 
 *  AND insert v2 at the beginning of slot idx+1.
 *
 *********************************************************************/
rc_t
page_p::split_slot(slotid_t idx, slot_offset_t off, const cvec_t& v1,
        const cvec_t& v2)
{
    FUNC(page_p::split_slot);
    W_IFDEBUG3( W_COERCE(check()) );
    
    /*
     *  Pre-compute # bytes needed.
     */
    smsize_t need = align(v1.size()) + align(v2.size());
    smsize_t need_slots = sizeof(slot_t);

    // Acquire it, then immediately release it, because
    // the funcs that we call below will acquire it.
    // We do this just so that we can avoid hand-undoing the
    // parts of this function if we should get a RECWONTFIT
    // part-way through this mess.
    DBG(<<"v1.size " << v1.size() << " v2.size " << v2.size());
    DBG(<<"aligned: " << align(v1.size()) << " " << align(v2.size()));
    DBG(<<"slot size: " << sizeof(slot_t));
    DBG(<<"needed " << need << " need_slots " << need_slots);
    DBG(<<" usable is now " << _pp->space.usable(xct()) );
    DBG(<<" nfree() " << _pp->space.nfree()
                <<" nrsvd() " << _pp->space.nrsvd()
                <<" xct_rsvd() " << _pp->space.xct_rsvd()
                <<" _tid " << _pp->space.tid()
                <<" xct() " << xct()->tid()
    );
    if((int)(need + need_slots) > _pp->space.usable(xct())) {
                return RC(smlevel_0::eRECWONTFIT);
    }
    
    /*
     * add a slot at idx+1, and put v2 in it.
     */
    int idx2 = idx+1;
    W_DO(insert_expand(idx2, 1, &v2, true));
    DBG(<<" usable is now " << _pp->space.usable(xct()) );
    DBG(<<" nfree() " << _pp->space.nfree()
        <<" nrsvd() " << _pp->space.nrsvd()
        <<" xct_rsvd() " << _pp->space.xct_rsvd()
        <<" _tid " << _pp->space.tid()
        <<" xct() " << xct()->tid()
    );

    W_IFDEBUG3( W_COERCE(check()) );

    w_assert1(idx >= 0 && idx < _pp->nslots);
    w_assert1(idx2 >= 0 && idx2 < _pp->nslots);

    slot_t& s = _pp->slot(idx);
    slot_t& t = _pp->slot(idx2);

    DBG(<<"shift " << idx2 << "," << 0 << ","
            << t.length << "," << idx << "," << s.length);

    /* 
     * Shift does a _shift_compress 
     */
#if W_DEBUG_LEVEL > 2
    int savelength1 = s.length;
    int savelength2 = t.length;
#endif
    w_assert3(savelength2 == (int)v2.size());

    W_COERCE(page_p::shift(idx, off, s.length-off, idx2, t.length));
    DBG(<<" usable is now " << _pp->space.usable(xct()) );
    DBG(<<" nfree() " << _pp->space.nfree()
        <<" nrsvd() " << _pp->space.nrsvd()
        <<" xct_rsvd() " << _pp->space.xct_rsvd()
        <<" _tid " << _pp->space.tid()
        <<" xct() " << xct()->tid()
    );

    w_assert3(slot_offset_t(s.length) == off);
    w_assert3(slot_offset_t(t.length) == (savelength1-off) + savelength2);
    W_IFDEBUG3( W_COERCE(check()) );

    /*
     *  v2 was already put into idx2, so insert v1 at the
     *  end of idx
     */
    DBG(<<"paste " << idx << "," << off << "," << v1.size());
    W_COERCE(paste(idx, off, v1));

    W_IFDEBUG3( W_COERCE(check()) );
    return RCOK;
}

/*********************************************************************
 *
 *  page_p::shift(idx2, off2,  len2, idx1, off1)
 *
 *  Shift len2 bytes from slot idx2 to slot idx1, starting
 *    with the byte at off2 (origin 0).
 *  Shift them to slot idx1 starting at off1 (the first byte moved
 *  becomes the off1-th byte (origin 0) of idx1. 
 *
 *********************************************************************/
rc_t
page_p::shift(
    slotid_t idx2, slot_offset_t off2, slot_length_t len2, //from-slot
    slotid_t idx1,  slot_offset_t off1        // to-slot
)
{
    W_IFDEBUG3( W_COERCE(check()) );

    /* 
     * shift the data from idx2 into idx1
     */
    w_rc_t rc;
    rc = log_page_shift(*this, idx2, off2, len2, idx1, off1);
    if (rc.is_error())  {
        return RC_AUGMENT(rc);
    }

    /*
     *  We need less space -- compute adjustment for alignment
     */
    {
        slot_t& t = _pp->slot(idx1); // to
        slot_t& s = _pp->slot(idx2); // from

        int adjustment = 
            (        // amount needed after
                align(s.length - len2) // final length of idx1(from)
                +
                align(t.length + len2) // final length of idx2(to)
            ) - ( // amount needed before
                align(s.length) 
                +
                align(t.length) 
            );

        DBG(<<"page_p::shift adjustment=" << adjustment);
        if (adjustment > 0)  {
            W_DO(_pp->space.acquire(adjustment, 0,  xct()));
            if( rsvd_mode() ) {
                W_DO(update_bucket_info());
            }
        } else {
            _pp->space.release( -adjustment, xct());
            if( rsvd_mode() ) {
                W_DO(update_bucket_info());
            }
        }
    }
        
    /*
     *  Compress and bring tuple of slots idx1, idx2 to the end.
     */
    _shift_compress(idx2, off2, len2, idx1, off1);

    W_IFDEBUG3( W_COERCE(check()) );

    return RCOK;
}


/*********************************************************************
 *
 *  page_p::_shift_compress(from, off-from, n, to, off-to)
 *
 *  Shift n bytes from slot from, starting at offset off-from
 *  to slot to, inserting them at(before) offset off-to.
 *
 *  Compresses the page in the process, putting from, to 
 *  at the end, in that order.
 *  
 *********************************************************************/
void
page_p::_shift_compress(slotid_t from, 
        slot_offset_t  from_off, 
        slot_length_t from_len,
        slotid_t to, 
        slot_offset_t  to_off)
{
    DBG(<<"end before " << _pp->end);
    /*
     *  Scratch area and mutex to protect it.
     */
    static queue_based_block_lock_t page_shift_compress_mutex;
    static char shift_scratch[sizeof(_pp->_slots.data)];

    /*
     *  Grab the mutex
     */
    CRITICAL_SECTION(cs, page_shift_compress_mutex);
    
    w_assert3(from >= 0 && from < _pp->nslots);
    w_assert3(to >= 0 && to < _pp->nslots);
    
    /*
     *  Copy data area over to scratch
     */
    memcpy(&shift_scratch, _pp->data(), sizeof(shift_scratch));

    /*
     *  Move data back without leaving holes
     */
    register char* p = _pp->data();
    slotid_t  nslots = _pp->nslots;
    for (slotid_t  i = 0; i < nslots; i++) {
        if (i == from)  continue;         // ignore this slot for now
        if (i == to)  continue;         // ignore this slot, too
        slot_t& s = _pp->slot(i);
        if (s.offset != -1)  {                 // it's in use
            w_assert3(s.offset >= 0);
            memcpy(p, shift_scratch+s.offset, s.length);
            s.offset = p - _pp->data();
            p += align(s.length);
        }
    }

    /*
     *  Move specified slots: from, to
     */
    {
        slot_offset_t        firstpartoff;
        slot_length_t        firstpartlen;
        slot_offset_t        middleoff;
        slot_length_t        middlelen;
        slot_offset_t        secondpartoff;
        slot_length_t        secondpartlen;
        char*                base_p;
        slot_offset_t        s_old_offset;

        /************** from **********************/
        slot_t& s = _pp->slot(from);
        DBG(<<" copy from slot " << from
                << " with tuple size " << tuple_size(from)
                << " offset " << s.offset
                );
        w_assert3(from_off <= slot_offset_t(s.length));
        w_assert3(s.offset != -1); // it's in use
        w_assert3(s.length <= from_off + from_len); 

        // copy firstpart: 0 -> from_off
        // skip from_off -> from_off + from_len
        // copy secondpart: from_off + from_len -> s.length
        firstpartoff = 0;
        firstpartlen = from_off;
        middleoff = firstpartoff + firstpartlen;
        middlelen = from_len;
        secondpartoff = middlelen + firstpartlen;
        secondpartlen = s.length - secondpartoff;
        base_p = p;


        if(firstpartlen) {
            DBG(<<"memcpy("
                << W_ADDR(p) << ","
                << W_ADDR(shift_scratch + s.offset + firstpartoff) << ","
                << firstpartlen );
            memcpy(p, shift_scratch + s.offset + firstpartoff, firstpartlen);
            p += firstpartlen;
        }
        // skip middle
        if(secondpartlen) {
            DBG(<<"memcpy("
                << W_ADDR(p) << ","
                << W_ADDR(shift_scratch + s.offset + secondpartoff) << ","
                << secondpartlen );
            memcpy(p, shift_scratch + s.offset + secondpartoff , secondpartlen);
            p += secondpartlen;
        }

        s.length -= middlelen;                // XXXX
        s_old_offset = s.offset;
        s.offset = base_p - _pp->data();
        p = base_p + align(s.length);

        /************** to **********************/
        slot_t& t = _pp->slot(to);
        DBG(<<" copy into slot " << to
                << " with tuple size " << tuple_size(to)
                << " offset " << t.offset
                );
        w_assert3(t.offset != -1); // it's in use
        w_assert3(to_off <= slot_offset_t(t.length));

        // copy firstpart: 0 -> to_off-1
        // copy middle from s
        // copy secondpart: to_off -> t.length

        firstpartoff = 0;
        firstpartlen = to_off;
        secondpartoff = to_off; 
        secondpartlen = t.length - secondpartoff;
        base_p = p;

        if(firstpartlen) {
            DBG(<<"before memcpy: t.offset " << t.offset
                << " firstpartoff " << firstpartoff
                << " firstpartlen " << firstpartlen);
            DBG(<<"memcpy("
                << W_ADDR(p) << ","
                << W_ADDR(shift_scratch + t.offset + firstpartoff) << ","
                << firstpartlen );
            memcpy(p, shift_scratch + t.offset + firstpartoff, firstpartlen);
            p += firstpartlen;
        }
        w_assert1(middlelen);
        DBG(<<"before memcpy: s.offset " << s_old_offset
                << " middleoff " << middleoff
                << " middlelen " << middlelen
                );
        DBG(<<"memcpy("
            << W_ADDR(p) << ","
            << W_ADDR(shift_scratch + s_old_offset + middleoff) << ","
            << middlelen );
        memcpy(p, shift_scratch + s_old_offset + middleoff, middlelen);
        p += middlelen;

        if(secondpartlen) {
            DBG(<<"memcpy("
                << W_ADDR(p) << ","
                << W_ADDR(shift_scratch + t.offset + secondpartoff) << ","
                << secondpartlen );
            memcpy(p, shift_scratch + t.offset + secondpartoff, secondpartlen);
            p += secondpartlen;
        }

        t.offset = base_p - _pp->data();
        t.length += middlelen;                // XXXX
        p = base_p + align(t.length);
    }
    _pp->end = p - _pp->data();
    DBG(<<"end after " << _pp->end);

    /*
     *  Page is now compressed with a hole after _pp->end.
     *  to is now the last slot, and it's got data in slot "from"
     */
}

/*********************************************************************
 *
 *  page_p::_compress(idx)
 *
 *  Compress the page (move all holes to the end of the page). 
 *  If idx != -1, then make sure that the tuple of idx slot 
 *  occupies the bytes of occupied space. Tuple of idx slot
 *  would be allowed to expand into the hole at the end of the
 *  page later.
 *  
 *********************************************************************/
void
page_p::_compress(slotid_t idx)
{
    /*
     *  Scratch area and mutex to protect it.
     */
    static queue_based_block_lock_t page_compress_mutex;
    static char scratch[sizeof(_pp->_slots.data)];

    /*
     *  Grab the mutex
     */
    CRITICAL_SECTION(cs, page_compress_mutex);
    
    w_assert3(idx < 0 || idx < _pp->nslots);
    
    /*
     *  Copy data area over to scratch
     */
    memcpy(&scratch, _pp->data(), sizeof(scratch));

    /*
     *  Move data back without leaving holes
     */
    register char* p = _pp->data();
    slotid_t nslots = _pp->nslots;
    for (slotid_t i = 0; i < nslots; i++) {
        if (i == idx)  continue;         // ignore this slot for now
        slot_t& s = _pp->slot(i);
        if (s.offset != -1)  {
            w_assert3(s.offset >= 0);
            memcpy(p, scratch+s.offset, s.length);
            s.offset = p - _pp->data();
            p += align(s.length);
        }
    }

    /*
     *  Move specified slot
     */
    if (idx >= 0)  {
        slot_t& s = _pp->slot(idx);
        if (s.offset != -1) {
            w_assert3(s.offset >= 0);
            memcpy(p, scratch + s.offset, s.length);
            s.offset = p - _pp->data();
            p += align(s.length);
        }
    }

    _pp->end = p - _pp->data();

    /*
     *  Page is now compressed with a hole after _pp->end.
     */
}




/*********************************************************************
 *
 *  page_p::pinned_by_me()
 *
 *  Return true if the page is pinned by this thread (me())
 *
 *********************************************************************/
bool
page_p::pinned_by_me() const
{
    return bf->fixed_by_me(_pp);
}

/*********************************************************************
 *
 *  page_p::check()
 *
 *  Check the page for consistency. All bytes should be accounted for.
 *
 *********************************************************************/
bool page_check_enabled = true; // see vol.cpp
rc_t
page_p::check()
{
  // Volume formats request that we not check pages while they're at
  // work. IF that causes a problem, remove this check.
  if(!page_check_enabled)
    return RCOK;
  
    /*
     *  Map area Each Byte in map corresponds
     *  to a byte in the page.
     */
    char *map = me()->get_page_check_map(); // thread-local stg

    /*
     *  Zero out map
     */
    memset(map, 0, SM_PAGESIZE);
    
    /*
     *  Compute our own end and nfree counters. Mark all used bytes
     *  to make sure that the tuples in page do not overlap.
     */
    int END = 0;
    int NFREE = data_sz + 2 * sizeof(slot_t);

    slot_t* p = &_pp->slot(0);
    for (int i = 0; i < _pp->nslots; i++, p--)  {
        int len = int(align(p->length));
        int j;
        for (j = p->offset; j < p->offset + len; j++)  {
            w_assert1(map[j] == 0);
            map[j] = 1;
        }
        if (END < j) END = j;
        NFREE -= len + sizeof(slot_t);
    }

    /*
     *  Make sure that the counters matched.
     */
    w_assert1(END <= _pp->end);
    w_assert1(_pp->space.nfree() == NFREE);
    w_assert1(_pp->end <= page_s::slot_offset_t(data_sz + 2 * sizeof(slot_t) - 
                           sizeof(slot_t) * _pp->nslots));

    /*
     *  Done 
     */
    //    mutex.release();

    return RCOK;
}




/*********************************************************************
 *
 *  page_p::~page_p()
 *
 *  Destructor. Unfix the page.
 *
 *********************************************************************/
page_p::~page_p()
{
    destructor();
}



/*********************************************************************
 *
 *  page_p::operator=(p)
 *
 *  Unfix my page and fix the page of p.
 *
 *********************************************************************/
w_rc_t
page_p::_copy(const page_p& p) 
{
    _refbit = p._refbit;
    _mode = p._mode;
    _pp = p._pp;
    page_bucket_info.nochecknecessary();
    if (_pp) {
        if( bf->is_bf_page(_pp)) {
            /* NB: 
             * But for the const-ness of p,
             * we would also update p.page_bucket_info if
             * it were out-of-date, but
             * for the time being, let's be
             * sure that we aren't refixing
             * file pages (those keeping track
             * of buckets).  (Note that we  do a refix
             * every time we log something and need to 
             * hang onto _last_mod_page).
             */
            space_bucket_t b = p.bucket();
            if((p.page_bucket_info.old() != b) && 
                p.page_bucket_info.initialized()) {
                DBG(<<"");
                w_assert3(! p.rsvd_mode());
            }
            W_DO(bf->refix(_pp, _mode));
            init_bucket_info();
        }
    }
    return RCOK;
}

page_p& 
page_p::operator=(const page_p& p)
{
    if (this != &p)  {
        if(_pp) {
            if (bf->is_bf_page(_pp))   {
                W_COERCE(update_bucket_info());
                bf->unfix(_pp, false, _refbit);
                _pp = 0;
            }
            page_bucket_info.nochecknecessary();
        }

        W_COERCE(_copy(p));
    }
    return *this;
}


/*********************************************************************
 *
 *  page_p::upgrade_latch(latch_mode_t m)
 *
 *  Upgrade latch, even if you have to block 
 *
 *********************************************************************/
void
page_p::upgrade_latch(latch_mode_t m)
{
    w_assert3(bf->is_bf_page(_pp));
    bf->upgrade_latch(_pp, m);
    _mode = bf->latch_mode(_pp);
}

/*********************************************************************
 *
 *  page_p::upgrade_latch_if_not_block()
 *
 *  Upgrade latch to EX if possible w/o blocking
 *
 *********************************************************************/
rc_t
page_p::upgrade_latch_if_not_block(bool& would_block)
{
    w_assert3(bf->is_bf_page(_pp));
    bf->upgrade_latch_if_not_block(_pp, would_block);
    if (!would_block) _mode = LATCH_EX;
    return RCOK;
}

/*********************************************************************
 *
 *  page_p::page_usage()
 *
 *  For DU DF.
 *
 *********************************************************************/
void
page_p::page_usage(int& data_size, int& header_size, int& unused,
                   int& alignment, page_p::tag_t& t, slotid_t& no_slots)
{
    // returns space allocated for headers in this page
    // returns unused space in this page

    // header on top of data area
    const int hdr_sz = page_sz - data_sz - 2 * sizeof (slot_t );

    data_size = header_size = unused = alignment = 0;

    // space occupied by slot array
    int slot_size =  _pp->nslots * sizeof (slot_t);

    // space used for headers
    if ( _pp->nslots == 0 )
             header_size = hdr_sz + 2 * sizeof ( slot_t );
    else header_size = hdr_sz + slot_size;
    
    // calculate space wasted in data alignment
    for (int i=0 ; i<_pp->nslots; i++) {
        // if slot is not vacant
        if ( _pp->slot(i).offset != -1 ) {
            data_size += _pp->slot(i).length;
            alignment += int(align(_pp->slot(i).length) -
                             _pp->slot(i).length);
        }
    }
    // unused space
    if ( _pp->nslots == 0 ) {
          unused = data_sz; 
    } else {
        unused = page_sz - header_size - data_size - alignment;
    }
//        W_FORM(cout)("hdr_sz = %d header_size = %d data_sz = %d 
//                data_size = %d alignment = %d unused = %d\n",
//                hdr_sz, header_size, data_sz, data_size,alignment,unused);
                            

    t        = tag();        // the type of page 
    no_slots = _pp->nslots;  // nu of slots in this page

    assert(data_size + header_size + unused + alignment == page_sz);
}

void        
page_p::init_bucket_info(page_p::tag_t ptag, 
    uint4_t                page_flags
)
{ 
    // so that when/if called with unknown (t_any_p) tag,
    // we can get the tag off the page (assuming it's been
    // formatted)
    if(ptag == page_p::t_any_p) ptag = tag(); 
    if(rsvd_mode(ptag)) {
        /*
         * If the page type calls for it, we must init
         * space-usage histogram info in the page handle.
         */
        space_bucket_t        b;

        if(page_flags & t_virgin) {
            // called from fix before formatting is done--
            // give it the max bucket size
            b = (space_num_buckets-1);
        } else {
            b = bucket();
        }
        w_assert3(b != space_bucket_t(-1)); // better not look like uninit
        page_bucket_info.init(b);
        // check on unfix is necessary 
    } else {
        // check on unfix is notnecessary 
        page_bucket_info.nochecknecessary();
    }
}

w_rc_t        
page_p::update_bucket_info() 
{ 
    /*
     * DON'T CALL UNLESS _pp is non-null
     * and it's a legit bf page.
     */
    w_assert2(_pp);
    w_assert2(bf->is_bf_page(_pp));

    if(rsvd_mode() && bf->is_bf_page(_pp, true) ) {
        // Is legit frame and is in hash table
        // even if page is clean

        /*
        // Old comment:
        // This doesn't work quite that way when we don't trust the page
        // lsn, because the extent/store head pages might be out-of-sync
        // with this page if we're in redo and
        // we later be re-formatting or reallocating this page.
        // Upate: I (neh) don't think this is limited to the 
        // DONT_TRUST_PAGE_LSN case,
        // because we might still have to apply a page format soon,
        // even in the do-trust-page-lsn case. 
        //
        // In the redo scenario it doesn't really make sense to waste any
        // time keeping the histograms up-to-date anyway.  They won't
        // be used until we are done with recovery.  We could probably
        // skip the maintenance in the undo case also, but it's more
        // likely to yield some useful cached information in the
        // histograms.
        */
        if(
             (smlevel_0::operating_mode == smlevel_0::t_in_undo)
             ||
             (smlevel_0::operating_mode == smlevel_0::t_forward_processing)
        )  
        {
            space_bucket_t b = bucket();
            if((page_bucket_info.old() != b) && 
                page_bucket_info.initialized()) {
                DBG(<<"updating extent histo for pg " <<  pid());
                W_DO(io->update_ext_histo(pid(), b));
                DBG(<<"page_bucket_info.init " << int(b));
                w_assert2(b != space_bucket_t(-1)); 
                    // better not look like uninit
                page_bucket_info.init(b);
            }
        }
        page_bucket_info.nochecknecessary();
    }

    return RCOK;
}


/*
 * Returns MAX free space that could be on the page
 */
smsize_t         
page_p::bucket2free_space(space_bucket_t b) 
{
    static smsize_t X[space_num_buckets] = {
        bucket_0_max_free,
        bucket_1_max_free,
        bucket_2_max_free,
        bucket_3_max_free,
#if HBUCKETBITS>=3
        bucket_4_max_free,
        bucket_5_max_free,
        bucket_6_max_free,
        bucket_7_max_free,
#if HBUCKETBITS==4
        bucket_8_max_free,
        bucket_9_max_free,
        bucket_10_max_free,
        bucket_11_max_free,
        bucket_12_max_free,
        bucket_13_max_free,
        bucket_14_max_free,
        bucket_15_max_free
#endif
#endif
    };
    return X[b];
}

space_bucket_t 
page_p::free_space2bucket(smsize_t sp) 
{
    // for use in gdb :
    uint4_t mask = space_bucket_mask_high_bits;

    // A page that has this amt of free space
    // is assigned to the following bucket.
    // Also, if we need a page with this much 
    // free space, what bucket would that be?

    switch(sp & mask) {
    case bucket_0_min_free: return 0;
    case bucket_1_min_free: return 1;
    case bucket_2_min_free: return 2;
#if HBUCKETBITS>=3
    case bucket_3_min_free: return 3;
    case bucket_4_min_free: return 4;
    case bucket_5_min_free: return 5;
    case bucket_6_min_free: return 6;
#if HBUCKETBITS>=4
    case bucket_7_min_free: return 7;
    case bucket_8_min_free: return 8;
    case bucket_9_min_free: return 9;
    case bucket_10_min_free: return 10;
    case bucket_11_min_free: return 11;
    case bucket_12_min_free: return 12;
    case bucket_13_min_free: return 13;
    case bucket_14_min_free: return 14;
#endif
#endif
    }


#if W_DEBUG_LEVEL > 4
    DBG(<<"free_space2bucket: space =" << unsigned(sp)
        << " masked= " << unsigned(sp&mask) );
    DBG(<<"bucket 0 =" << unsigned(bucket_0_min_free ));
    DBG(<<"bucket 1 =" << unsigned(bucket_1_min_free ));
    DBG(<<"bucket 2 =" << unsigned(bucket_2_min_free ));
    DBG(<<"bucket 3 =" << unsigned(bucket_3_min_free ));
#if HBUCKETBITS>=3
    DBG(<<"bucket 4 =" << unsigned(bucket_4_min_free ));
    DBG(<<"bucket 5 =" << unsigned(bucket_5_min_free ));
    DBG(<<"bucket 6 =" << unsigned(bucket_6_min_free ));
    DBG(<<"bucket 7 =" << unsigned(bucket_7_min_free ));
#if HBUCKETBITS>=4
    DBG(<<"bucket 8 =" << unsigned(bucket_8_min_free ));
    DBG(<<"bucket 9 =" << unsigned(bucket_9_min_free ));
    DBG(<<"bucket 10 =" << unsigned(bucket_10_min_free ));
    DBG(<<"bucket 11 =" << unsigned(bucket_11_min_free ));
    DBG(<<"bucket 12 =" << unsigned(bucket_12_min_free ));
    DBG(<<"bucket 13 =" << unsigned(bucket_13_min_free ));
    DBG(<<"bucket 14 =" << unsigned(bucket_14_min_free ));
    DBG(<<"bucket 15 =" << unsigned(bucket_15_min_free ));
#endif
#endif
#endif /* W_DEBUG_LEVEL */

        
#if HBUCKETBITS>=4
    smsize_t maximum = bucket_15_min_free;
    space_bucket_t result = 15;
#elif HBUCKETBITS>=3
    smsize_t maximum = bucket_7_min_free;
    space_bucket_t result = 7;
#else
    smsize_t maximum = bucket_3_min_free;
    space_bucket_t result = 3;
#endif

    w_assert0(sp < smlevel_0::page_sz && sp >= maximum);
    return result;
}


ostream &
operator<<(ostream& o, const store_histo_t&s)
{
    for (shpid_t p=0; p < space_num_buckets; p++) {
        o << " " << s.bucket[p] << "/" ;
    }
    o<<endl;
    return o;
}
