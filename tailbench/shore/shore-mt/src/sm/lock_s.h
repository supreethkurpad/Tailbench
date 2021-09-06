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

/*<std-header orig-src='shore' incl-file-exclusion='LOCK_S_H'>

 $Id: lock_s.h,v 1.73 2010/06/15 17:30:07 nhall Exp $

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

#ifndef LOCK_S_H
#define LOCK_S_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

/**\cond skip */
class lock_base_t : public smlevel_1 {
public:
    // Their order is significant.

    enum status_t {
        t_no_status = 0,
        t_granted = 1,
        t_converting = 2,
        t_waiting = 4
    };

    typedef smlevel_0::lock_mode_t lmode_t;

    typedef smlevel_0::lock_duration_t duration_t;

    enum {
        MIN_MODE = NL, MAX_MODE = EX,
        NUM_MODES = MAX_MODE - MIN_MODE + 1
    };

    static const char* const         mode_str[NUM_MODES];
    static const char* const         duration_str[t_num_durations];
    static const bool                compat[NUM_MODES][NUM_MODES];
    static const lmode_t             supr[NUM_MODES][NUM_MODES];
};
/**\endcond skip */

#ifndef LOCK_S
/*
typedef lock_base_t::duration_t lock_duration_t;
// typedef lock_base_t::lmode_t lock_mode_t; lock_mode_t defined in basics.h
typedef lock_base_t::status_t status_t;

#define LOCK_NL         lock_base_t::NL
#define LOCK_IS         lock_base_t::IS
#define LOCK_IX         lock_base_t::IX
#define LOCK_SH         lock_base_t::SH
#define LOCK_SIX        lock_base_t::SIX
#define LOCK_UD         lock_base_t::UD
#define LOCK_EX         lock_base_t::EX

#define LOCK_INSTANT         lock_base_t::t_instant
#define LOCK_SHORT         lock_base_t::t_short
#define LOCK_MEDIUM        lock_base_t::t_medium
#define LOCK_LONG         lock_base_t::t_long
#define LOCK_VERY_LONG        lock_base_t::t_very_long
*/
#endif

/**\brief The means of identifying a desired or held lock
 *
 * \details
 * Lock manager requests (acquire, release, query) take an argument
 * of this kind to identify the entity to be locked. Not all
 * lockids refer to anything extant.
 *
 * The structure of the lockid_t is such that several entity types
 * are inferred, and the lockid_t has constructors from these entities'
 * identifier classes, as well as methods to modify the lockid while
 * retaining its inferred-entity type.  The enumerator type name_space_t
 * identifies the entity type of the lock_id, and is returned by the
 * method space:
 *
 * - t_vol : 
 *   - volume lock  
 *   - no parent in hierarchy
 *   - constructed from vid_t
 *   - can extract vid_t with vid()
 *
 * - t_store : 
 *   - store lock   (file lock, index lock)
 *   - parent in hierarchy is volume lock
 *   - constructed from stid_t
 *   - can extract snum_t with store() and vid_t with vid()
 *   - can extract stid with extract_stid()
 *
 * - t_page : 
 *   - page lock   
 *   - parent in hierarchy is store lock
 *   - constructed from lpid_t
 *   - can extract shpid_t with page(), snum_t with store() and vid_t with vid()
 *   - can extract stid with extract_stid()
 *   - can extract lpid with extract_lpid()
 *
 * - t_kvl : 
 *   - key-value lock   
 *   - parent in hierarchy is store lock
 *   - constructed from kvl_t (which is constructed from a vec_t : see 
 *       vec_t::make_kvl)
 *   - can extract kvl_t with extract_kvl()
 *   - can extract snum_t with store() and vid_t with vid()
 *   - can extract stid with extract_stid()
 *
 * - t_record : 
 *   - lock on record in file
 *   - parent in hierarchy is page lock
 *   - constructed from rid_t
 *   - can extract slotid_t with slot(), shpid_t with page(), 
 *         snum_t with store() and vid_t with vid()
 *   - can extract rid_t with extract_rid()
 *   - can extract stid with extract_stid()
 *   - can extract lpid with extract_lpid()
 *
 * - t_extent : 
 *   - lock on extent 
 *   - not in a hierarchy
 *   - constructed from extid_t
 *   - can extract extnum_t with extent()
 *   - can extract extid_t with extract_extent()
 *
 * - t_user1 : 
 *   - lock on user-defined entity 1 
 *   - not in a hierarchy
 *   - constructed from user1_t
 *   - can extract user1_t with extract_user1()
 *
 * - t_user2 : 
 *   - lock on user-defined entity 2
 *   - parent in hierarchy is user-defined entity 1
 *   - constructed from user2_t
 *   - can extract user2_t with extract_user2()
 *   - can extract user1_t with extract_user1()
 *
 * - t_user3 : 
 *   - lock on user-defined entity 3
 *   - parent in hierarchy is user-defined entity 2
 *   - constructed from user3_t
 *   - can extract user3_t with extract_user3()
 *   - can extract user2_t with extract_user2()
 *   - can extract user1_t with extract_user1()
 *
 * - t_user4 : 
 *   - lock on user-defined entity 4
 *   - parent in hierarchy is user-defined entity 3
 *   - constructed from user4_t
 *   - can extract user4_t with extract_user4()
 *   - can extract user3_t with extract_user3()
 *   - can extract user2_t with extract_user2()
 *   - can extract user1_t with extract_user1()
 *
 * \attention  The enumeration values of name_space_t have functional
 * significance; they are use to compute parents in the hierarchy, 
 * so modify them with care!
 *
 * The user-defined entity types are for use by a server; they offer
 * a hierarchy.
 *
 */
class lockid_t {
public:
    //
    // The lock graph consists of 6 nodes: volumes, stores, pages, key values,
    // records, and extents. The first 5 of these form a tree of 4 levels.
    // The node for extents is not connected to the rest. The name_space_t
    // enumerator maps node types to integers. These numbers are used for
    // indexing into arrays containing node type specific info per entry (e.g
    // the lock caches for volumes, stores, and pages).
    //
    /**\cond skip */
    enum { NUMNODES = 6 };
    // The per-xct cache only caches volume, store, and page locks.
    enum { NUMLEVELS = 4 };
    /**\endcond skip */

    enum name_space_t { // you cannot change these values with impunity
        t_bad                = 10,
        t_vol                = 0,
        t_store              = 1,  // parent is 1/2 = 0 t_vol
        t_page               = 2,  // parent is 2/2 = 1 t_store
        t_kvl                = 3,  // parent is 3/2 = 1 t_store
        t_record             = 4,  // parent is 4/2 = 2 t_page
        t_extent             = 5,
        t_user1              = 6,
        t_user2              = 7,  // parent is t_user1
        t_user3              = 8,  // parent is t_user2
        t_user4              = 9   // parent is t_user3
    };

    static const int cached_granularity = t_page;

    typedef uint4_t          page_bits_t;

    /**\brief User-defined entity 1 */
    struct user1_t  {
        uint2_t                u1;
        user1_t() : u1(0)  {}
        user1_t(uint2_t v1) : u1(v1)  {}
    };

    /**\brief User-defined entity 2 */
    struct user2_t : public user1_t  {
        uint4_t                u2;
        user2_t() : u2(0)  {}
        user2_t(uint2_t v1, uint4_t v2): user1_t(v1), u2(v2)  {}
    };

    /**\brief User-defined entity 3 */
    struct user3_t : public user2_t  {
        uint4_t                u3;
        user3_t() : u3(0)  {}
        user3_t(uint2_t v1, uint4_t v2, uint4_t v3)
                : user2_t(v1, v2), u3(v3)  {}
    };

    /**\brief User-defined entity 4 */
    struct user4_t : public user3_t  {
        uint4_t                u4;
        user4_t() : u4(0)  {}
        user4_t(uint2_t v1, uint4_t v2, uint4_t v3, uint4_t v4)
                : user3_t(v1, v2, v3), u4(v4)  {}
    };

    enum { pageWindex=2, slotWindex=3, user4Windex=3 };
    enum { pageSindex=4, slotSindex=6, user4Sindex=6 };

private:
    union {
        // 
        w_base_t::uint8_t l[2]; 
                      // l[0,1] are just to support the hash function.

        w_base_t::uint4_t w[4]; 
                      // w[0] == s[0,1] see below
                      // w[1] == store or extent or user2
                      // w[2] == page or user3
                      // w[3] contains slot (in both s[6] and s[7]) or user4
        w_base_t::uint2_t s[8]; 
                      // s[0] high byte == lspace (lock type)
                      // s[0] low byte == boolean used for extent-not-freeable
                      // s[1] vol or user1
                      // s[2,3]==w[1] 
                      // s[4,5]== w[2] see above
                      // s[6] == slot
                      // s[7] == slot copy 
    };

public:
    /**\brief comparison operator for lockid_t, used by lock manager */
    bool operator<(lockid_t const &p) const;
    /**\brief equality operator for lockid_t*/
    bool operator==(const lockid_t& p) const;
    /**\brief inequality operator for lockid_t*/
    bool operator!=(const lockid_t& p) const;
    friend ostream& operator<<(ostream& o, const lockid_t& i);
public:
    /// Used by lock cache
    uint4_t           hash() const; // used by lock_cache
    /// clear out the lockid - initialize to mean nothing
    void              zero();

private:

    //
    // lspace -- contains enum for type of lockid_t
    //
public:
    /// return the kind of entity this describes
    name_space_t      lspace() const;
private:
    void              set_lspace(lockid_t::name_space_t value);
    uint1_t           lspace_bits() const;

    //
    // vid - volume
    //
public:
    /// extract volume id lockid whose lspace() == t_vol or has parent with lspace() == t_vol
    vid_t             vid() const;
private:
    void              set_vid(const vid_t & v);
    uint2_t           vid_bits() const;

    //
    // store - stid
    //
public:
    /// extract store number lockid whose lspace() == t_store or has parent with lspace() == t_store
    const snum_t&     store() const;
private:
    void              set_snum(const snum_t & s);
    void              set_store(const stid_t & s);
    uint4_t           store_bits() const;

    //
    // extent -- used only when lspace() == t_extent
    //
public:
    /// extract extent number lockid whose lspace() == t_extent 
    const extnum_t&   extent() const;
private:
    void              set_extent(const extnum_t & e);
    uint4_t           extent_bits() const;

    //
    // page id
    //
public:
    /// extract short page number lockid whose lspace() == t_page or has parent with lspace()==t_page 
    const shpid_t&    page() const;
private:
    void              set_page(const shpid_t & p);
    uint4_t           page_bits() const ;
    void              set_page_bits(lockid_t::page_bits_t);
    //
    // slot id
    //
public:
    /// extract slot number lockid whose lspace() == t_record 
    slotid_t          slot() const;
private:
    void              set_slot(const slotid_t & e);
    uint2_t           slot_bits() const;
    uint4_t           slot_kvl_bits() const;

    //
    // user1
    //
public:
    /// extract uint2_t from whose lspace() == t_user1 
    uint2_t           u1() const;
private:
    void              set_u1(uint2_t i);

    //
    // user2
    //
public:
    /// extract uint4_t from whose lspace() == t_user2 
    uint4_t           u2() const;
private:
    void              set_u2(uint4_t i);

    //
    // user3
    //
public:
    /// extract uint4_t from whose lspace() == t_user3 
    uint4_t           u3() const;
private:
    void              set_u3(uint4_t i);

    //
    // user4
    //
public:
    /// extract uint4_t from whose lspace() == t_user4 
    uint4_t           u4() const;
private:
    void              set_u4(uint4_t i);

public:

    /**\brief for extent locks only, used by volume manager
     * \details
     * \anchor FREEEXTLOCK
     * Set to true when the extent contains an uncommitted allocated page.
     * This is done when a page is allocated in the extent; it is used
     * for special handling of extent locks in the lock manager.
     *
     * The special handling requires a little background:
     *
     * - extent locks are not in the hierarchy
     * - extent IX locks are acquired when a page in the extent is allocated 
     *   (or recovered, in recovery case)
     * - extent IX locks are acquired when a page in the extent is freed 
     * - extent IX locks are acquired when allocating the extent to a store
     * - extent EX locks are acquired when deallocating all the extents in 
     *   a store (destroying the store), at which time we have an EX lock on
     *   the store.
     *
     * - We need some way to tell when an extent can be freed. It can be
     *   freed when there are no pages allocated in the extent.  
     *   Presumably we would have any transaction that deallocated a page
     *   in the extent try to deallocate the extent (i.e., if there are
     *   no more pages in the extent). But it doesn't acquire an EX lock
     *   on the extent, so another transaction could slip in and allocate
     *   pages in the extent.
     * - If the transaction (that deleted one or more pages in an extent) 
     *   tries to deallocate the extent at the transaction end, it would
     *   have to inspect the extent link to tell if the extent has no
     *   pages in it, and it would still have to do something to avoid races
     *   with other transactions.
     *
     * The special handling is this:
     * - Transactions that allocate pages in an extent set the bit in the
     *   \b lock saying there are pages allocated in the extent.
     * - Transactions that deallocate pages in an extent clear the bit in the
     *   \b lock when there are no more pages allocated in the extent.
     * - This bypasses a page fix for inspecting the extlink_t.
     * - Synchronization for this bit in the lockid is provided by the
     *   volume manager; the bit is set and cleared only in the volume manager,
     *   which is a monitor.
     * - At transaction end, a transaction that has an IX lock on an
     *   extent tries to upgrade it to an EX if the extent lock indicates
     *   that no pages are allocated in the extent.  If it succeeds,
     *   the lock manager can delete the extent while holding the EX lock,
     *   then free the lock.
     *
     * When at the end of transaction, lock_m::release_duration() is
     * called, the lock manager returns eFOUNDEXTTOFREE when it encounters
     * an extent lock with this bit set.
     *
     */
    void              set_ext_has_page_alloc(bool value);
    /**\brief for extent locks only, used by lock manager
     * \details
     * Returns true when the extent contains an uncommitted allocated page.
     * See discussion in set_ext_has_page_alloc, above.
     */
    bool              ext_has_page_alloc() const ;

    // construct vacuous lockid_t
    NORET             lockid_t() ;    
    /// construct from volume id
    NORET             lockid_t(const vid_t& vid);
    /// construct from full extent id
    NORET             lockid_t(const extid_t& extid);    
    /// construct from full store id
    NORET             lockid_t(const stid_t& stid);
    /// construct from full page id
    NORET             lockid_t(const lpid_t& lpid);
    /// construct from full record id
    NORET             lockid_t(const rid_t& rid);
    /// construct from kvl_t (hash of key,value, from vec_t)
    NORET             lockid_t(const kvl_t& kvl);
    /// copy constructor
    NORET             lockid_t(const lockid_t& i);        
    /// construct from user1_t
    NORET             lockid_t(const user1_t& u);        
    /// construct from user2_t
    NORET             lockid_t(const user2_t& u);        
    /// construct from user3_t
    NORET             lockid_t(const user3_t& u);        
    /// construct from user4_t
    NORET             lockid_t(const user4_t& u);        

    /// extract a full extent id from an extent lock
    void              extract_extent(extid_t &e) const;
    /// extract a full store id from a store, page, or record lock
    void              extract_stid(stid_t &s) const;
    /// extract a full page id from a page or record lock
    void              extract_lpid(lpid_t &p) const;
    /// extract a full record id from a record lock
    void              extract_rid(rid_t &r) const;
    /// extract a kvl_t (hash of key,value) from a key-value lock
    void              extract_kvl(kvl_t &k) const;
    /// extract a user1_t from user-defined entity 1, 2, 3, or 4
    void              extract_user1(user1_t &u) const;
    /// extract a user2_t from user-defined entity 2, 3, or 4
    void              extract_user2(user2_t &u) const;
    /// extract a user3_t from user-defined entity 3 or 4
    void              extract_user3(user3_t &u) const;
    /// extract a user3_t from user-defined entity 4
    void              extract_user4(user4_t &u) const;

    /// Return true if type is t_user1, t_user2, t_user3 or t_user4
    bool              is_user_lock() const;

    /**\brief Nullify all parts of the lockid that apply to children in
    * the hierarchy. 
    *
    * If the given space isn't in the hierarchy, this generates a fatal error.
    */
    void              truncate(name_space_t space);

    /// copy operator
    lockid_t&         operator=(const lockid_t& i);

};

/** \example lockid_test.cpp*/

ostream& operator<<(ostream& o, const lockid_t::user1_t& u);
ostream& operator<<(ostream& o, const lockid_t::user2_t& u);
ostream& operator<<(ostream& o, const lockid_t::user3_t& u);
ostream& operator<<(ostream& o, const lockid_t::user4_t& u);

istream& operator>>(istream& o, lockid_t::user1_t& u);
istream& operator>>(istream& o, lockid_t::user2_t& u);
istream& operator>>(istream& o, lockid_t::user3_t& u);
istream& operator>>(istream& o, lockid_t::user4_t& u);



#if W_DEBUG_LEVEL>0
#else
#include "lock_s_inline.h"
#endif

// This is used in probe() for lock cache
inline w_base_t::uint4_t w_hash(const lockid_t& id)
{
    return id.hash();
}

/*<std-footer incl-file-exclusion='LOCK_S_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
