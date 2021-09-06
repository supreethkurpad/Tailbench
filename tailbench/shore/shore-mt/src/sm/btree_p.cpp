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

 $Id: btree_p.cpp,v 1.36 2010/06/08 22:28:55 nhall Exp $

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

#ifdef __GNUG__
#       pragma implementation "btree_p.h"
#endif

#include "sm_int_2.h"

#include "btree_p.h"
#include "btree_impl.h"
#include "sm_du_stats.h"
#include <crash.h>


/*********************************************************************
 *
 *  btree_p::distribute(right_sibling, left_heavy, slot, 
 *            addition, split_factor)
 *
 *  Spill this page over to right_sibling. 
 *  Based on the "slot" into which an "additional" bytes 
 *  would be inserted after the split, and the "split_factor", 
 *  routine computes the a new "slot" for the insertion after the
 *  split and a boolean flag "left_heavy" to indicate if the
 *  new "slot" is in the left or right sibling.
 *
 *********************************************************************/
rc_t
btree_p::distribute(
    btree_p&    rsib,        // IO- target page
    bool&     left_heavy,    // O-  true if insert should go to left
    slotid_t&     snum,        // IO- slot of insertion after split
    smsize_t    addition,    // I-  # bytes intended to insert
    int     factor,        // I-  % that should remain
    const bool bIgnoreLatches)
{
    w_assert3(is_fixed());
    w_assert3(rsib.is_fixed());
    if(!bIgnoreLatches) {
	w_assert3(latch_mode() == LATCH_EX);
	w_assert3(rsib.latch_mode() == LATCH_EX);
    }
    w_assert1(snum >= 0 && snum <= nrecs());
    /*
     *  Assume we have already inserted the tuple into slot snum
     *  of this page, calculate left and right page occupancy.
     *  ls and rs are the running total of page occupancy of
     *  left and right sibling respectively.
     */
    addition += sizeof(page_s::slot_t);
    int orig = used_space();
    int ls = orig + addition;
    const int keep = factor * ls / 100; // nbytes to keep on left page

    int flag = 1;        // indicates if we have passed snum

    /*
     *  i points to the current slot, and i+1-flag is the first slot
     *  to be moved to rsib.
     */
    int rs = rsib.used_space();
    int i;
    for (i = nrecs(); i >= 0; i--)  {
        int c;
    if (i == snum)  {
        c = addition, flag = 0;
    } else {
        c = int(align(rec_size(i-flag)) + sizeof(page_s::slot_t));
    }
    ls -= c, rs += c;
    if ((ls < keep && ls + c <= orig) || rs > orig)  {
        ls += c;
        rs -= c;
        if (i == snum) flag = 1;
        break;
    }
    }

    /*
     *  Calculate 1st slot to move and shift over to right sibling
     */
    i = i + 1 - flag;
    if (i < nrecs())  {
    W_DO( shift(i, rsib) );
    w_assert3( rsib.pid0() == 0);
    }
    SSMTEST("btree.distribute.1");

    w_assert3(i == nrecs());
    
    /*
     *  Compute left_heavy and new slot to insert additional bytes.
     */
    if (snum == nrecs())  {
    left_heavy = flag != 0;
    } else {
    left_heavy = (snum < nrecs());
    }

    if (! left_heavy)  {
    snum -= nrecs();
    }

#if W_DEBUG_LEVEL > 2
    btree_p& p = (left_heavy ? *this : rsib);
    w_assert1(snum <= p.nrecs());
    w_assert1(p.usable_space() >= addition);

    DBG(<<"distribute: usable_space() : left=" << this->usable_space()
    << "; right=" << rsib. usable_space() );

#endif 
    
    return RCOK;
}



/*********************************************************************
 *
 *  btree_p::unlink(...)
 *
 *  Unlink this page from its neighbor. 
 *  We assume that this is called from a top-level action.
 *
 *********************************************************************/
rc_t
btree_p::_unlink(btree_p &rsib, const bool bIgnoreLatches)
{
    DBG(<<" unlinking page: "  << pid()
    << " nrecs " << nrecs()
    );
    w_assert3(is_fixed());
    if(!bIgnoreLatches) {
	w_assert3(latch_mode() == LATCH_EX);
    }
    if(rsib.is_fixed()) {
        // might not have a right sibling
        w_assert3(rsib.is_fixed());
	if(!bIgnoreLatches) {
	    w_assert3(rsib.latch_mode() == LATCH_EX);
	}
    }
    lpid_t  lsib_pid = pid(); // get vol, store
    lsib_pid.page = prev();
    shpid_t rsib_page = next();

    /*
     * Mohan: set the SM bit on the deleted page.
     */
    W_DO( set_smo() ); 
    SSMTEST("btree.unlink.1");
    /*
     * We don't bother updating the next(), prev()
     * pointers on *this* page
     */
    {
        if(rsib.is_fixed()) {
            W_DO( rsib.link_up(prev(), rsib.next()) );
            SSMTEST("btree.unlink.2");
            rsib.unfix();
        }
        unfix(); // me

        btree_p lsib;
        if(lsib_pid.page) {
            INC_TSTAT(bt_links);
	    latch_mode_t mode = LATCH_EX;
	    if(bIgnoreLatches) {
		mode = LATCH_NL;
	    }
            W_DO( lsib.fix(lsib_pid, mode) ); 
            SSMTEST("btree.unlink.3");
            W_DO( lsib.link_up(lsib.prev(), rsib_page) );
        }

    }
    SSMTEST("btree.unlink.4");
    w_assert3(! rsib.is_fixed());
    w_assert3( ! is_fixed());
    return RCOK;
}

rc_t
btree_p::unlink_and_propagate(
    const cvec_t&     key,
    const cvec_t&     elem,
    btree_p&          rsib,
    lpid_t&           parent_pid,
    lpid_t const&     root_pid,
    const bool bIgnoreLatches)
{
#if W_DEBUG_LEVEL > 2
    W_DO(log_comment("start unlink_and_propagate"));
#endif 
    w_assert3(this->is_fixed());
    w_assert3(rsib.is_fixed() || next() == 0);

    if(root_pid == pid())  {
        unfix();
    } else {
        INC_TSTAT(bt_cuts);
        
        lpid_t  child_pid = pid(); 
        int       lev  = this->level();

        lsn_t anchor;
        xct_t* xd = xct();
        if(xd) check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
        if (xd)  anchor = xd->anchor();

        /* 
         * remove this page if it's not the root
         */

        X_DO(_unlink(rsib, bIgnoreLatches), anchor);
        w_assert3( !rsib.is_fixed());
        w_assert3( !is_fixed());

        SSMTEST("btree.propagate.d.1");

        /* 
         * cut_page out of parent as many times as
         * necessary until we hit the root.
         */

        if (parent_pid.page) {
            cvec_t     null;
            btree_p    parent;
            slotid_t   slot = -1;
            bool       found_key;
            bool       total_match;

            w_assert3( ! is_fixed());

	    latch_mode_t mode = LATCH_EX;
	    if(bIgnoreLatches) {
		mode = LATCH_NL;
	    }
            X_DO(parent.fix(parent_pid, mode), anchor);
            X_DO(parent.search(key, elem, found_key, total_match, slot), anchor)

            // might, might not:w_assert3(found_key);
            if(!total_match) {
                slot--;
            }
            if(slot<0) {
                w_assert3(parent.pid0() == child_pid.page);
            } else {
                w_assert3(parent.child(slot) == child_pid.page);
            }

            X_DO(btree_impl::_propagate(root_pid, key, elem, 
					child_pid, lev, true, bIgnoreLatches), anchor);
            parent.unfix();
        }
        SSMTEST("btree.propagate.d.1");

        if (xd)  {
            xd->compensate(anchor,false/*not undoable*/LOG_COMMENT_USE("btree.prop.3"));
        }
        SSMTEST("btree.propagate.d.3");
    }
    w_assert3( ! is_fixed());
#if W_DEBUG_LEVEL > 2
    W_DO(log_comment("end propagate_split"));
#endif 
    return RCOK;
}

/*********************************************************************
 *
 *  btree_p::cut_page(child_pid, slot)
 *
 *  Remove the child page's key-pid entry from its parent (*this). The
 *  action is compensated by the caller.
 *  If slot < 0, we're removing the entry in pid0, and in that case,
 *  we move the child(0) into pid0, so pid0 isn't empty unless
 *  the whole thing is empty.
 *
 *********************************************************************/
rc_t
btree_p::cut_page(lpid_t & W_IFDEBUG3(child_pid) , slotid_t slot)
{
    // slot <0 means pid0
    // this page is fixed, child is not
    w_assert3(is_fixed());
    
    lpid_t cpid = pid(); 
    cpid.page = (slot < 0) ? pid0() : child(slot);

    w_assert3(child_pid == cpid);

    /*
     *  Free the child
     */
    W_DO( io->free_page(cpid, false/*checkstore*/) );

    SSMTEST("btree.propagate.d.6");

    /*
     *  Remove the slot from the parent
     */
    if (slot >= 0)  {
        W_DO( remove(slot, this->is_compressed()) );
    } else {
        /*
         *  Special case for removing pid0 of parent. 
         *  Move first sep into pid0's place.
         */
        shpid_t pid0 = 0;
        if (nrecs() > 0)   {
            pid0 = child(0);
            W_DO( remove(0, this->is_compressed()) );
            SSMTEST("btree.propagate.d.4");
        }
        W_DO( set_pid0(pid0) );
        slot = 0;
    }
    SSMTEST("btree.propagate.d.5");

    /*
     *  if pid0 == 0, then page must be empty
     */
    w_assert3(pid0() != 0 || nrecs() == 0);

    return RCOK;
}


/*********************************************************************
 *
 *  btree_p::set_hdr(root, level, pid0, flags)
 *
 *  Set the page header.
 *
 *********************************************************************/
rc_t
btree_p::set_hdr(shpid_t root, int l, shpid_t pid0, uint2_t flags)
{
    btctrl_t hdr;
    hdr.root = root;
    hdr.pid0 = pid0;
    hdr.level = l;
    hdr.flags = flags;

    vec_t v;
    v.put(&hdr, sizeof(hdr));
    W_DO( zkeyed_p::set_hdr(v) );
    return RCOK;
}


/*********************************************************************
 *
 *  btree_p::set_root(root)
 *
 *  Set the root field in header to "root".
 *
 ********************************************************************/
rc_t
btree_p::set_root(shpid_t root)
{
    const btctrl_t& tmp = _hdr();
    W_DO( set_hdr(root, tmp.level, tmp.pid0, tmp.flags) );
    return RCOK;
}


/*********************************************************************
 *
 *  btree_p::set_pid0(pid0)
 *
 *  Set the pid0 field in header to "pid0".
 *
 ********************************************************************/
rc_t
btree_p::set_pid0(shpid_t pid0)
{
    const btctrl_t& tmp = _hdr();
    W_DO(set_hdr(tmp.root, tmp.level, pid0, tmp.flags) );
    return RCOK;
}

/*********************************************************************
 *
 *  btree_p::set_flag( flag_t flag, bool compensate )
 *
 *  Mark a leaf page's delete bit.  Make it redo-only if
 *  compensate==true.
 *
 *********************************************************************/
rc_t
btree_p::_set_flag( flag_t f, bool compensate)
{
    FUNC(btree_p::set_flag);
    DBG(<<"SET page is " << this->pid() << " flag is " << int(f));

    lsn_t anchor;
    xct_t* xd = xct();
    if(xd) check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
    if(compensate) {
        if (xd)  anchor = xd->anchor();
    }

    const btctrl_t& tmp = _hdr();
    // X_DO if compensate, W_DO if not.  Instead, we just 
    // do what the macros do 
    {
        w_rc_t __e = set_hdr(tmp.root, tmp.level, tmp.pid0, 
                    (uint2_t)(tmp.flags | f));
        if (__e.is_error()) {
            if(xd && compensate)
            {
                xd->rollback(anchor);
                xd->release_anchor(true LOG_COMMENT_USE("btreep1"));
            }
            return RC_AUGMENT(__e);
        }
    }

    if(compensate) {
        if (xd) xd->compensate(anchor,false/*not undoable*/LOG_COMMENT_USE("btree._set_flag"));
    }
    return RCOK;
}

/*********************************************************************
 *
 *  btree_p::_clr_flag(flag_t f, bool compensate)
 *  NB: if(compensate), these are redo-only.  
 *  This might be not the most-efficient way to do this,
 *  but it should work and it means I don't have to descend all the
 *  way to the log level with a new function.
 *
 *********************************************************************/
rc_t
btree_p::_clr_flag(flag_t f, bool compensate)
{
    FUNC(btree_p::clr_flag);
    DBG(<<"CLR page is " << this->pid() << " flag = " << int(f));

    lsn_t anchor;
    xct_t* xd = xct();
    check_compensated_op_nesting ccon(xd, __LINE__, __FILE__);
    if(compensate) {
        if (xd)  anchor = xd->anchor();
    }

    const btctrl_t& tmp = _hdr();
    X_DO( set_hdr(tmp.root, tmp.level, tmp.pid0, 
                (uint2_t)(tmp.flags & ~f)), anchor );

    if(compensate) {
        if (xd) xd->compensate(anchor,false/*not undoable*/ LOG_COMMENT_USE("btree._clr_flag"));
    }
    return RCOK;
}


/*********************************************************************
 *
 *  btree_p::format(pid, tag, page_flags, store_flag_t store_flags)
 *
 *  Format the page.
 *
 *********************************************************************/
rc_t
btree_p::format(
    const lpid_t&     pid, 
    tag_t         tag, 
    uint4_t         page_flags,
    store_flag_t    store_flags
    )
    
{
    btctrl_t ctrl;
    ctrl.root = 0;
    ctrl.level = 1;
    ctrl.flags = 0;
    ctrl.pid0 = 0; 

    vec_t vec;
    vec.put(&ctrl, sizeof(ctrl));

    W_DO( zkeyed_p::format(pid, tag, page_flags, store_flags, vec) );
    return RCOK;
}

/*********************************************************************
 *
 *  btree_p::search(key, el, found_key, found_key_elem, ret_slot)
 *
 *  Search for <key, el> in this page. Return true in "found_key" if
 *  the key is found, and  true in found_key_elem if
 *  a total match is found. 
 *  Return the slot 
 *     --in which <key, el> resides, if found_key_elem
 *      --where <key,elem> should go, if !found_key_elem
 *  Note: if el is null, we'll return the slot of the first <key,xxx> pr
 *     when found_key but ! found_key_elem
 *  NB: this works because if we put something in slot, whatever is
 *      there gets shoved up.
 *  NB: this means that given an ambiguity at level 1: aa->P1 ab->P2,
 *      when we're searching the parent for "abt", we'll always choose
 *      P2. 
 *  
 *********************************************************************/
rc_t
btree_p::search(
    const cvec_t&     key,
    const cvec_t&     el,
    bool&         found_key, 
    bool&         found_key_elem, 
    slotid_t&         ret_slot    // origin 0 for first record
) const
{
    FUNC(btree_p::search);
    DBG(<< "Page " << pid()
        << " nrecs=" << nrecs()
        << " search for key " << key
    );
    
    /*
     *  Binary search.
     */
    found_key = false;
    found_key_elem = false;
    int mi, lo, hi;
    for (mi = 0, lo = 0, hi = nrecs() - 1; lo <= hi; )  {
        mi = (lo + hi) >> 1;    // ie (lo + hi) / 2

        btrec_t r(*this, mi); 
        int d;
        DBG(<<"(lo=" << lo
            << ",hi=" << hi
            << ") mi=" << mi);

        if ((d = r.key().cmp(key)) == 0)  {
            DBG( << " r=("<<r.key()
                << ") CMP k=(" <<key
                << ") = d(" << d << ")");

            found_key = true;
            DBG(<<"FOUND KEY; comparing el: " << el 
                << " r.elem()=" << r.elem());
            d = r.elem().cmp(el);

           // d will be > 0 if el is null vector
            DBG( << " r=("<<r.elem()
                << ") CMP e=(" <<el
                << ") = d(" << d << ")");
        } else {
            DBG( << " r=("<<r.key()
                << ") CMP k=(" <<key
                << ") = d(" << d << ")");
        }

        // d <  0 if r.key < key; key falls between mi and hi
        // d == 0 if r.key == key; match
        // d >  0 if r.key > key; key falls between lo and mi

        if (d < 0) 
            lo = mi + 1;
        else if (d > 0)
            hi = mi - 1;
        else {
            ret_slot = mi;
            found_key_elem = true;
            DBG(<<"");
            return RCOK;
        }
    }
    ret_slot = (lo > mi) ? lo : mi;
    /*
     * Returned slot is always <= nrecs().
     *
     * Since rec(nrecs()) doesn't exist this means that an unfound
     * item would belong at the end of the page if we're returning nrecs().
     * (This shouldn't happen at the leaf level except at the last leaf,
     * i.e., end of the index, because in searches of interior nodes, 
     * the search will push us toward the higher of the two
     * surrounding leaves, if a value were to belong between
     * leaf1.highest and leaf2.lowest.
     */ 
#if W_DEBUG_LEVEL > 2
    w_assert3(ret_slot <= nrecs());
    if(ret_slot == nrecs() ) {
        w_assert3(!found_key_elem);
        // found_key could be true or false
    }
    if(found_key) w_assert3(ret_slot <= nrecs());
    if(found_key_elem) w_assert3(ret_slot < nrecs());
#endif 

    DBG(<<" returning slot " << ret_slot
        <<" found_key=" << found_key
        <<" found_key_elem=" << found_key_elem
    );
    return RCOK;
}

/*********************************************************************
 *
 *  btree_p::insert(key, el, slot, child)
 *
 *  For leaf page: insert <K=key, E=el> at "slot"
 *  For node page: insert <K=(key, el), E=(key.size(), child pid)> at "slot"
 *  For both kinds of pages, an entry is:
 *     key len(/prefix)=4 bytes, K, E
 *
 *********************************************************************/
rc_t
btree_p::insert(
    const cvec_t&     key,    // I-  key
    const cvec_t&     el,    // I-  element
    slotid_t         slot,    // I-  slot number
    shpid_t         child,    // I-  child page pointer
    bool                do_it   // I-  just compute space or really do it
)
{
    FUNC(btree_p::insert);
    DBG(<<"insert " << key 
        << " into page " << pid()
        << " at slot "  << slot
        << " REALLY? " << do_it
        << " nrecs="  << nrecs()
    );

    cvec_t sep(key, el);
    
    int2_t klen = key.size();
    cvec_t attr;
    attr.put(&klen, sizeof(klen));
    if (is_leaf()) {
        w_assert3(child == 0);
    } else {
        w_assert3(child);
        attr.put(&child, sizeof(child));
    }
    SSMTEST("btree.insert.1");

    return  zkeyed_p::insert(sep, attr, slot, do_it, 
            do_it?this->is_compressed():false);
}


/*********************************************************************
 * 
 *  btree_p::child(slot)
 *
 *  Return the child pointer of tuple at "slot". Applies to interior
 *  nodes only.
 *
 *********************************************************************/
shpid_t 
btree_p::child(slotid_t slot) const
{
    vec_t sep;
    const char* aux;
    int auxlen;

    W_COERCE( zkeyed_p::rec(slot, sep, aux, auxlen) );
    w_assert3(is_node() && auxlen == 2 + sizeof(shpid_t));

    shpid_t child;
    memcpy(&child, aux + 2, sizeof(shpid_t));
    return child;
}

// Stats on btree leaf pages
rc_t
btree_p::leaf_stats(btree_lf_stats_t& _stats)
{
    btrec_t rec[2];
    int r = 0;

    _stats.hdr_bs += (hdr_size() + sizeof(page_p::slot_t) + 
             align(page_p::tuple_size(0)));
    _stats.unused_bs += persistent_part().space.nfree();

    int n = nrecs();
    _stats.entry_cnt += n;

    for (int i = 0; i < n; i++)  {
    rec[r].set(*this, i);
    if (rec[r].key() != rec[1-r].key())  {
        /* BUG BUG : if r is the first record on the
         * page, we can't do this comparison, and we
         * don't know if the two are different.
         * so this count is rather meaningless
         */
        _stats.unique_cnt++;
    }

    if( ! is_compressed()) {
        _stats.key_bs += rec[r].klen();
        _stats.data_bs += rec[r].elen();
        _stats.entry_overhead_bs += (align(this->rec_size(i)) - 
                       rec[r].klen() - rec[r].elen() + 
                       sizeof(page_s::slot_t));
    } else {
        /*
         * Prefix compression might have encompassed all of 
         * the key and part of the elem.  If the key is shorter
         * than the prefix, this is so, in which case, 
         * consider key space to be 0, and subtract the apropos
         * portion of the prefix from the element length.
         */
        _stats.key_bs += (rec[r].klen() > rec[r].plen()) ?
                (rec[r].klen() - rec[r].plen()) : 0
            ;
        _stats.data_bs += (rec[r].klen() > rec[r].plen()) ? 
                 rec[r].elen() :
                 rec[r].elen() - (rec[r].plen() - rec[r].klen())
             ; 

        DBG(<<"old entry_overhead_bs: " << _stats.entry_overhead_bs);
        DBG(<<"entry_overhead_bs: slot:" << sizeof(page_s::slot_t)
            << " rec.aligned: " <<align(this->rec_size(i))
            << " klen: " <<rec[r].klen()
            << " elen: " <<rec[r].elen()
            << " plen: " <<rec[r].plen()
        );

        _stats.entry_overhead_bs += sizeof(page_s::slot_t) +
                    (align(this->rec_size(i)) - 
                       ((rec[r].klen()+ rec[r].elen())
                           - rec[r].plen())
                    );
    }
    r = 1 - r;
    }
    return RCOK;
}

// Stats on btree interior pages
rc_t
btree_p::int_stats(btree_int_stats_t& _stats)
{
    _stats.unused_bs += persistent_part().space.nfree();
    _stats.used_bs += page_sz - persistent_part().space.nfree();
    return RCOK;
}

/*********************************************************************
 *
 *  btrec_t::set(page, slot)
 *
 *  Load up a reference to the tuple at "slot" in "page".
 *  NB: here we are talking about record, not absolute slot# (slot
 *  0 is special on every page).   So here we use ORIGIN 0
 *
 *********************************************************************/
btrec_t& 
btrec_t::set(const btree_p& page, slotid_t slot)
{
    FUNC(btrec_t::set);
    w_assert3(slot >= 0 && slot < page.nrecs());
    /*
     *  Invalidate old _key and _elem.
     */
    _key.reset();
    _elem.reset();

    const char* aux;
    int     auxlen;
    vec_t     sep;
    //
    // sep is the combined key-value pair
    // aux/auxlen is the meta-data: for leaves, it's a 2-byte
    //         length indicating the length of the key within sep,
    //          and for nodes, it's the above 2-byte value plus a 
    //          page id.
    //
    /* OVERHEAD per-slot overhead includes 4-byte slot table entry */
    
    if(page.is_compressed()) {
        int pxp; // # parts to the prefix
        W_COERCE( page.zkeyed_p::rec(slot, _prefix_bytes, pxp, sep, aux, auxlen) );
        DBG(<<"slot " << slot << " has " << _prefix_bytes << " prefix bytes");
    } else {
        W_COERCE( page.zkeyed_p::rec(slot, sep, aux, auxlen) );
    }

    if (page.is_leaf())  {
        w_assert3(auxlen == 2);
    } else {
        w_assert3(auxlen == 2 + sizeof(shpid_t));
    }

    // materialize the key length
    int2_t k;
    memcpy(&k, aux, sizeof(k));
    size_t klen = k;

#if W_DEBUG_LEVEL > 2
    int elen_test = sep.size() - klen;
    w_assert3(elen_test >= 0);

    smsize_t elen = sep.size() - klen;
#endif 

    sep.split(klen, _key, _elem);
    w_assert3(_key.size() == klen);
    w_assert3(_elem.size() == elen);

    if (page.is_node())  {
        w_assert3(auxlen == 2 + sizeof(shpid_t));
        memcpy(&_child, aux + 2, sizeof(shpid_t));
    }
    DBG(<<"slot " << slot << " has plen " << plen() );

    return *this;
}

smsize_t                        
btree_p::overhead_requirement_per_entry =
            2 // for the key length (in btree_p)
            +
            4 // for the key length (in zkeyed_p)
            +
            sizeof(shpid_t) // for the interior nodes (in btree_p)
            ;

smsize_t         
btree_p::max_entry_size = // must be able to fit 2 entries to a page
    (
        ( (smlevel_0::page_sz - 
              (
                 page_p::_hdr_size +
                 // leave off footer for this computation
                 sizeof(page_p::slot_t) + // page_p add back in one slot
                 sizeof(lsn_t) + // page_p: add back in lsn
                 align(sizeof(btree_p::btctrl_t))
              )
           ) >> 1) 
        - 
        overhead_requirement_per_entry
    ) 
    // round down to aligned size
    & ~ALIGNON1 
    ;

/*
 * Delete flag is only changed during 
 * forward processing.  Its purpose is to
 * force an inserting tx to await a POSC before
 * consuming space freed by a delete.  The reason
 * we have to do that is to avoid the case in which
 * a crash occurs while a smo was in progress, 
 * and rolling back the delete has to re-traverse the
 * tree to do a page split.  Rolling back a delete must
 * NOT require a page split unless we KNOW that a SMO
 * is not in progress. 
 */
rc_t             
btree_p::set_delete() 
{ 
    xct_t* xd = xct();
    if (xd && xd->state() == smlevel_1::xct_active) {
        return _set_flag(t_delete, true); 
    } else {
        return RCOK;
    }
}
rc_t             
btree_p::clr_delete() { 
    xct_t* xd = xct();
    if (xd && xd->state() == smlevel_1::xct_active) {
        return _clr_flag(t_delete, true); 
    } else {
        return RCOK;
    }
}

MAKEPAGECODE(btree_p, zkeyed_p)

void
btree_p::print(
    sortorder::keytype kt, // default is as a string
    bool print_elem 
)
{
    int i;
    btctrl_t hdr = _hdr();
    const int L = 3;

    for (i = 0; i < L - hdr.level; i++)  cout << '\t';
    cout << pid0() << "=" << pid0() << endl;

    for (i = 0; i < nrecs(); i++)  {
    for (int j = 0; j < L - hdr.level; j++)  cout << '\t' ;

    btrec_t r(*this, i);
    cvec_t* real_key;

    if(r.key().size() == 0) {
        // null
        cout << "<key = " << r.key() ;
    } else switch(kt) {
    case sortorder::kt_b: {
        cout     << "<key = " << r.key() ;
        } break;
    case sortorder::kt_i8: {
        w_base_t::int8_t value;
        key_type_s k(key_type_s::i, 0, 8);
        W_COERCE(btree_m::_unscramble_key(real_key, r.key(), 1, &k));
        real_key->copy_to(&value, sizeof(value));
        cout     << "<key = " << value;
        }break;
    case sortorder::kt_u8:{
        uint4_t value;
        key_type_s k(key_type_s::u, 0, 8);
        W_COERCE(btree_m::_unscramble_key(real_key, r.key(), 1, &k));
        real_key->copy_to(&value, sizeof(value));
        cout     << "<key = " << value;
        } break;

    case sortorder::kt_i4: {
        int4_t value;
        key_type_s k(key_type_s::i, 0, 4);
        W_COERCE(btree_m::_unscramble_key(real_key, r.key(), 1, &k));
        real_key->copy_to(&value, sizeof(value));
        cout     << "<key = " << value;
        }break;
    case sortorder::kt_u4:{
        uint4_t value;
        key_type_s k(key_type_s::u, 0, 4);
        W_COERCE(btree_m::_unscramble_key(real_key, r.key(), 1, &k));
        real_key->copy_to(&value, sizeof(value));
        cout     << "<key = " << value;
        } break;
    case sortorder::kt_i2: {
        int2_t value;
        key_type_s k(key_type_s::i, 0, 2);
        W_COERCE(btree_m::_unscramble_key(real_key, r.key(), 1, &k));
        real_key->copy_to(&value, sizeof(value));
        cout     << "<key = " << value;
        }break;
    case sortorder::kt_u2: {
        uint2_t value;
        key_type_s k(key_type_s::u, 0, 2);
        W_COERCE(btree_m::_unscramble_key(real_key, r.key(), 1, &k));
        real_key->copy_to(&value, sizeof(value));
        cout     << "<key = " << value;
        } break;
    case sortorder::kt_i1: {
        int1_t value;
        key_type_s k(key_type_s::i, 0, 1);
        W_COERCE(btree_m::_unscramble_key(real_key, r.key(), 1, &k));
        real_key->copy_to(&value, sizeof(value));
        cout     << "<key = " << value;
        }break;
    case sortorder::kt_u1: {
        uint1_t value;
        key_type_s k(key_type_s::u, 0, 1);
        W_COERCE(btree_m::_unscramble_key(real_key, r.key(), 1, &k));
        real_key->copy_to(&value, sizeof(value));
        cout     << "<key = " << value;
        } break;

    case sortorder::kt_f8:{
        f8_t value;
        key_type_s k(key_type_s::f, 0, 8);
        W_COERCE(btree_m::_unscramble_key(real_key, r.key(), 1, &k));
        real_key->copy_to(&value, sizeof(value));
        cout     << "<key = " << value;
        } break;
    case sortorder::kt_f4:{
        f4_t value;
        key_type_s k(key_type_s::f, 0, 4);
        W_COERCE(btree_m::_unscramble_key(real_key, r.key(), 1, &k));
        real_key->copy_to(&value, sizeof(value));
        cout     << "<key = " << value;
        } break;

    default:
        W_FATAL(fcNOTIMPLEMENTED);
        break;
    }

    if ( is_leaf())  {
        if(print_elem) {
	    rid_t rid;
	    r.elem().copy_to(&rid, sizeof(rid_t));
	    cout << ", elen="  << r.elen() << " bytes: " << rid;
        }
    } else {
	cout << ", pid = " << r.child();
    }
    cout << ">" << endl;
    }
    for (i = 0; i < L - hdr.level; i++)  cout << '\t';
    cout << "]" << endl;
}

