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

/*<std-header orig-src='shore' incl-file-exclusion='PIN_H'>

 $Id: pin.h,v 1.89 2010/06/08 22:28:55 nhall Exp $

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

#ifndef PIN_H
#define PIN_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#ifndef FILE_S_H
#include <file_s.h>
#endif /* FILE_S_H */

#include <page_alias.h>

/* DOXYGEN Documentation */
/**\addtogroup SSMPIN  
 * You may pin (force to remain in the buffer pool at a fixed location)
 * portions (no larger than a page) of a record for short periods of time
 * while you operate on them.   You may step through a large record pinning
 * a sequence of such portions.
 *
 * \b Use \b of \b the \b pin_i \b requires \b care.  
 * \b Take \b care \b to \b observe \b the \b following \b constraints:
 *
 * - You may not operate on the in-buffer-pool copy directly, as the
 * only storage manager knows the format of these data. You may
 * operate on these pinned data through the class pin_i.
 *
 * - Do not hold page latches (keep a page pinned) for
 * long periods (while a thread sleeps, awaits I/O, or otherwise
 * blocks long-term. Operating system scheduling
 * of threads is not under your control for this purpose).  
 * Latches are meant to be short-term. Holding a latch for
 * a long time interferes with other aspects of the storage manager, 
 * including buffer-pool cleaning.  
 *
 * - A latch is held by a thread, not by a transaction.
 * Under no circumstances can a pin_i be passed from thread to thread.
 *
 * - It is dangerous to operate on a record through the static
 * storage manager methods (such as append_rec) while holding records pinned
 * through pin_i.  This can lead to invalid pin_i with undefined results
 * when they are used; doing so with concurrent threads in a single transaction
 * can lead to undetectable deadlocks (latch-latch deadlocks, for example).
 */


class file_p;
class lgdata_p;
class record_t;

/***********************************************************************
   The pin_i class (located in pin.h) is used to 
   You mahn pin ranges of bytes in
   a record.  The amount pinned (of the record body) can be determined
   with the start_byte() and length() functions.  Access to the pinned
   region is via the body() function.  The header is always pinned if
   any region is pinned and can be accessed via hdr().
   
   next_bytes() is used to get access to the next pinnable
   region of the record.
   
   ~pin_i() will unpin the record.  Pin() and unpin() can also 
   be used to change which record is pinned (pin() will unpin the
   currently pinned record()).
  
   For large records, data pages will not actually be pinned until
   body() is called.  Therefore, to just read record
   headers, pinning with start 0 will not cause any additional IO.
  
   The repin function efficiently re-pins a previously unpinned record
   and efficiently repins a record even while it is pinned.  This is
   useful after append_rec and truncate_rec calls to repin the record
   since its location may have changed.

   NOTE ON LOCK MODE PARAMETERS:
      The pin_i, pin, repin functions all take a lock mode parameter
      that specifies how the record should initially be locked.  The
      options are SH and EX.  EX should be used when
      the pinned record will be eventually updated (through update_rec,
      unpdate_rec_hdr, append_rec, or truncate_rec).  Using EX in these
      cases will improve performance and reduce the risk of deadlock,
      but is not necessary for correctness.

   WARNING:
     The pin_i structure for a pinned record is no longer valid after
     any append, truncate, create operation for ANY record on the page
     that is pinned.  To enforce this a debugging check is made that
     compares the page's current lsn with its value when the record was
     pinned.  Therefore, update_rec calls must also have a repin call
     performed.

   For efficiency  (to avoid repinning), the
   ss_m::update_rec and ss_m::update_rec_hdr functions are also
   provided by pin_i.  These can be called on any pinned record
   regardless of where and how much is pinned.  If a pin_i was
   previously pinned and then upinned, a call to
   pin_i::update_rec[_hdr] will temporarily repin the record and then
   unpin it.  Therefore, after any pin_i call that updates the record,
   the state of the pin_i (either pinned or not) remains the same.

 **********************************************************************/
/**\brief Pin records in the buffer pool and operate on them.
 * \ingroup SSMPIN
 * \details
 * Certain operations on the records referenced by a pin_i may invalidate
 * the pin_i. For example, if you pin a record, then truncate it or
 * append to it while holding a pin_i, the pin_i must be considered
 * invalidated because appending to the record might necessarily require
 * moving it.
 *
 * The pin functions take a lock mode parameter that tells the
 * storage manager how to lock the record initially. 
 * The options are SH and EX.  
 * EX should be used when the pinned record will be 
 * updated (through update_rec, unpdate_rec_hdr, append_rec, 
 * or truncate_rec).  
 * Using EX in these cases will improve performance and 
 * reduce the risk of deadlock, but it is not necessary for correctness.
 *
 * If you pin with SH, and subsequently modify the record through pin_i, 
 * the pin_i method(s) will
 * upgrade locks as necessary to maintain ACID properties.
 *
 * These methods will not perform needless unfix/refix operations: you
 * may pin many small records on the same page in sequence and avoid
 * unfixing the page between pins.
 */
class pin_i : public smlevel_top {
    friend class scan_file_i;
public:
    /**\cond skip */
    enum flags_t { 
        pin_empty                = 0x0,
        pin_rec_pinned                = 0x01,
        pin_hdr_only                = 0x02, 
        pin_separate_data        = 0x04,
        pin_lg_data_pinned        = 0x08  // large data page is pinned
    };
    /**\endcond skip */
    
    /// Constructor.  Does not pin anything until pin() is called.
    NORET        pin_i() {_init_constructor();}

    /// Destructor. Unpins anything currently pinned.
    NORET        ~pin_i();

    // These methods pin portions of a record beginning at start
    // the actual location pinned (returned by start_byte), may
    // be <= start.
    // (They are smart enough not to unfix/refix the page
    // if the prior state has a record pinned on the same page
    // as the indicated record.)
    //
    /**\brief Pin a portion of the record starting at a given location. 
     * \details
     * @param[in] rid  ID of the record of interest
     * @param[in] start  Offset of the first byte of interest.
     * @param[in] lmode  Lock mode to use.
     * Pin the page containing the first byte of interest.
     * A record lock in the given mode is acquired (if it is not
     * already subsumed by a coarser lock or by a higher lock mode).
     *
     * Only the slotted page containing the record header is fixed at
     * this point.   Its latch mode is inferred from the lock mode.
     * If any part of the record is pinned, the slotted
     * page containing the header is also fixed.
     * Thus, if the record is large (or very large), data pages won't
     * be fixed in the buffer pool until the body() method is called.
     */
    rc_t        pin(
        const rid_t &          rid,
        smsize_t               start,
        lock_mode_t            lmode = SH,
        const bool             bIgnoreLatches = false);

    /**\brief Pin a portion of the record starting at a given location. 
     * \details
     * Pin a record with the given lock mode and latch mode.
     * See pin(rid, start, lock_mode);
     */
    rc_t        pin(
        const rid_t &          rid,
        smsize_t               start,
        lock_mode_t            lock_mode,
        latch_mode_t           latch_mode);

    /**\brief Unpin whatever record was pinned.  */
    void       unpin();

    /**\brief True if the running thread owns this pin_i */
    bool       is_mine() const; // only if owning thread

    /**\brief
     * Set the reference bit to use for the buffer frame containing 
     * the pinned body page when the page is unpinned.  
     * \details
     * @param[in] value  0 or greater.
     * A value of 0 is a "hate" hint indicating that
     * the frame can be reused as soon as necessary.  
     * By default, a value of 1 is used indicating the page will be cached 
     * until at least 1 sweep of the buffer clock hand has passed.
     * Higher values cause the page to remain cached longer.
     */
    void       set_ref_bit(int value);

    /**\brief Efficiently repin a record after is size has changed or
     * after it has been unpinned.
     * \details
     * @param[in] lmode  SH or EX
     */
    rc_t       repin(lock_mode_t lmode = SH);


    /**\brief Pin the next range of bytes  in the record
     * \details
     * @param[out] eof  Set to true if there are no more bytes to pin.
     * When eof is reached, the previously pinned range remains pinned.
     */
    rc_t       next_bytes(bool& eof); 

    /**\brief True if something currently pinned. */
    bool       pinned() const     { return _flags & pin_rec_pinned; }

    /**\brief True if the entire record pinned. */
    bool       pinned_all() const 
                    { return pinned() && _start==0 && _len==body_size();}

    /**\brief  True if record is pinned and the pin_i is valid
     * \details
     * The pin_i is valid if it is up-to-date with the LSN on
     * the page.  In other words, use this to verify that the page has not been
     *  updated since it was pinned by this pin_i
     */
    bool       up_to_date() const
                    { return pinned() && (_hdr_lsn == _get_hdr_lsn());}

    /**\brief Return the byte-offset (within the record) the  of the pinned portion */
    smsize_t   start_byte() const { _check_lsn(); return _start;}
    /**\brief Return the length of the pinned portion */
    smsize_t   length() const     { _check_lsn(); return _len;}
    /**\brief Return the size of the pinned record's header */
    smsize_t   hdr_size() const   { _check_lsn(); return _rec->hdr_size();}
    /**\brief Return the size of the pinned record's body */
    smsize_t   body_size() const  { _check_lsn(); return _rec->body_size();}
    /**\brief True if the record is too large to fit on a file page */
    bool       is_large() const  { _check_lsn(); return _rec->is_large();}
    /**\brief True if the record is small enough to fit on a file page */
    bool       is_small() const  { _check_lsn(); return _rec->is_small();}
    /**\brief The kind of large-record implementation used for the pinned 
     * record 
     * \details
     * Values returned are: 0, 1, or 2.
     * - 0 means "large" : ~8KB - ~21 GB
     * - 1 means "1-level index" - up to ~16.GB 
     * - 2 means "2-level index" - up to ~33 GB
     * - 3-level and deeper indexes are not supported.
     */
    int        large_impl() const  { _check_lsn(); return _rec->large_impl();}

    /**\brief Return the record ID of the pinned record */
    const rid_t&     rid() const {_check_lsn(); return _rid;}

    /**\brief Return a pointer to the pinned record's header in the buffer pool.
     * \details
     * \attention
     * Do NOT update anything directly in the buffer pool. This returns a
     * const string because it is for the purpose of reading or copy-out.
     */
    const char*      hdr() const
                        { _check_lsn(); return pinned() ? _rec->hdr() : 0;}

    /**\brief Return a pointer into the pinned-record-portion in the buffer pool.
     * \details
     * \attention
     * Do NOT update anything directly in the buffer pool. This returns a
     * const string because it is for the purpose of reading or copy-out.
     */
    const char*      body();

    // These record update functions duplicate those in class ss_m
    // and are more efficient.  They can be called on any pinned record
    // regardless of where and how much is pinned.
    /**\brief Overwrite a portion of the pinned record with new data.
     * \details
     * @param[in] start The offset from the beginning of the record of the
     * place to perform the update.
     * @param[in] data A vector containing the data to place in the record
     * at location \a start.
     * @param[out] old_value deprecated
     * The portion of the record containing the start byte need not
     * be pinned before this is called.
     */
    rc_t    update_rec(smsize_t start, const vec_t& data, int* old_value = 0
#ifdef SM_DORA
                       , const bool bIgnoreLocks = false
#endif
                       );

    rc_t    update_mrbt_rec(smsize_t start, const vec_t& data, int* old_value = 0,
			    const bool bIgnoreLocks = false,
			    const bool bIgnoreLatches = false);

    /**\brief Update the pinned record's header.
     * \details
     * @param[in] start The offset from the beginning of the header of the
     * place to perform the update.
     * @param[in] hdr A vector containing the data to place in the header
     * at location \a start.
     */
    rc_t    update_rec_hdr(smsize_t start, const vec_t& hdr
#ifdef SM_DORA
                           , const bool bIgnoreLocks = false
#endif 
                           );

    /**\brief Append to a pinned record.
     * \details
     * @param[in] data A vector containing the data to append to the record's
     * body.
     * The end of the record need not be pinned before this is called.
     */
    rc_t    append_rec(const vec_t& data);

    rc_t    append_mrbt_rec(const vec_t& data,
			    const bool bIgnoreLocks = false,
			    const bool bIgnoreLatches = false);
    
    /**\brief Shorten a record.
     * \details
     * @param[in] amount Number of bytes to chop off the end of the 
     * pinned record's body.
     * The end of the record need not be pinned before this is called.
     */
    rc_t    truncate_rec(smsize_t amount);

    const record_t* rec() const { _check_lsn(); return _rec;}
  
    /**\brief Return a pointer to the page containing the record.
     * \details
     * This allows you to read the entire page.  
     * \attention
     * Do NOT update anything directly in the buffer pool. This returns a
     * const string because it is for the purpose of reading or copy-out.
     */
    const char* hdr_page_data();

    /**\brief Return the ID of the page containing the given byte of the record 
     * \details
     * @param[in] offset The offset from the beginning of the record of the
     * byte of interest
     * @param[out] start_byte The offset from the beginning of the page of
     * the byte of interest
     * \return The page ID of the page containing the
     * byte of interest. 
     */
    lpid_t   page_containing(smsize_t offset, smsize_t& start_byte) const;

private:

    void        _init_constructor(); // companion to constructor

    rc_t         _pin_data();

    const char* _body_large();
    
    rc_t        _pin(const rid_t &rid, smsize_t start, lock_mode_t m, 
                    latch_mode_t l);

    rc_t        _pin(const rid_t &rid, smsize_t start, lock_mode_t m,
                     const bool bIgnoreLatches = false);

    rc_t        _repin(lock_mode_t lmode, int* old_value = 0
#ifdef SM_DORA
                       , const bool bIgnoreLocks = false
#endif
                       );

    file_p*     _get_hdr_page_no_lsn_check() const {
                        return pinned() ? &_hdr_page() : 0;}
    file_p*     _get_hdr_page() const { 
                        _check_lsn(); return _get_hdr_page_no_lsn_check();}

    // NOTE: if the _check_lsn assert fails, it usually indicates that
    // you tried to access a pinned record after having updated the
    // record, but before calling repin.
    // The _set_lsn() function is used to reset the lsn to the page's
    // new value, after an update operation.
    //
#if W_DEBUG_LEVEL > 1
    void         _check_lsn() const {w_assert2(up_to_date());}
    // these are in scan.cpp and pin.cpp respectively
    // so that pin.h is #include-able by client code, and
    // these are inlined in those files that use them.
    void         _set_lsn(); 
    void         _set_lsn_for_scan();
#else
    void         _check_lsn() const {}
    void         _set_lsn() {}
    void         _set_lsn_for_scan() {}
#endif

    const lsn_t& _get_hdr_lsn() const;

    rid_t                _rid;
    smsize_t             _len;
    smsize_t             _start;
    record_t*            _rec;
    w_base_t::uint4_t    _flags;  // this cannot be flags_t since it uses
    // | to generate new flags not in the enum 
    // _hdr_lsn is used to record the lsn on the page when
    // the page is pinned.  When compiled with -DDEBUG, all pin_i
    // operations check that the hdr page's _lsn1 has not changed
    // (ie. to verify that the pinned record has not moved)
    lsn_t                 _hdr_lsn;
    lock_mode_t           _lmode;  // current locked state

    /*
     *        Originally pin_i contained the _hdr_page and _hdr_page data
     *        members commented out below.  This required that users #include
     *        sm_int.h (ie. the whole world), generating large .o's.
     *        Instead, we have the corresponding "alias" byte arrays and
     *        member functions which cast these to the correct page type.
     *        Only pin.cpp uses these functions.  This greatly reduces the
     *        number of .h files users need to include.
     *
     *  Asserts in pin_i constructors verify that the _alias members
     *  are large enough to hold file_p and lgdata_p. 
     */
    //file_p        _hdr_page;
    //lgdata_p      _data_page;
    file_p&         _hdr_page() const;
    lgdata_p&       _data_page() const;
    /* see comment above 4 reason 4 alias */
    char            _hdr_page_alias[PAGE_ALIAS_FILE];
    char            _data_page_alias[PAGE_ALIAS_LGDATA];

    // disable
    NORET        pin_i(const pin_i&);
    NORET        pin_i& operator=(const pin_i&);

public:
	/**\cond skip */
	// Put inside pin_i only for the purpose of namescoping.
	static latch_mode_t lock_to_latch(lock_mode_t m);
	/**\endcond skip */
};

/**\cond skip */
inline latch_mode_t pin_i::lock_to_latch(lock_mode_t m) {
	switch(m) {
	case SH:
	case UD:
	case NL:
		return LATCH_SH;
	case EX:
		return LATCH_EX;

	default:
		W_FATAL(smlevel_0::eNOTIMPLEMENTED);
	}
	return LATCH_NL; // never gets here
};
/**\endcond skip */

/*<std-footer incl-file-exclusion='PIN_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
