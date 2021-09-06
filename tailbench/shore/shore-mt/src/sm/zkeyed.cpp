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

 $Id: zkeyed.cpp,v 1.50 2010/05/26 01:20:49 nhall Exp $

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

/*
 * overall upper limit on #prefix parts:
 */
#define MAXPFX max_num_entries()

/*
 * configured upper-limit on # prefix parts-
 * make this much more than 10, and the cost in cycles 
 * is too high, even though the space savings might
 * be huge. 
 */
#define MAX_PREFIX_LEVEL 10

#define SM_SOURCE
#define ZKEYED_C
#include <sm_int_1.h>
#ifdef __GNUG__
#   pragma implementation
#endif

#include <zkeyed.h>

MAKEPAGECODE(zkeyed_p, page_p)


static int max_prefix_level = MAX_PREFIX_LEVEL;


/*********************************************************************
 *
 *  zkeyed_p::shift(idx, rsib, compressed)
 *
 *  Shift all entries starting at "idx" to first entry of page "rsib".
 *  This is called from btree.cpp, where nothing is known about
 *  key compression, so it's up to this function to maintain the
 *  invariant that the first slot on a page has no compression.
 *
 *********************************************************************/
rc_t
zkeyed_p::shift(slotid_t idx, zkeyed_p* rsib, bool compressed)
{
    FUNC(zkeyed_p::shift);
    w_assert1(idx >= 0 && idx < nrecs());

    int n = nrecs() - idx;

    DBG(<<"zkeyed_p::SHIFT "  
        << " (compressed=" << compressed 
        << ") from page " << pid().page << " idx " << idx
        << " #recs " << n
        );

    int start_simple_move=0;
    rc_t rc;

    if(compressed) {
        // First slot is a special case
        // See if it's got a compressed prefix; if so,
        // materialize the whole thing through zkeyed_p::rec
        // before stuffing it into the rsib.
        //
        // In several cases, though, idx is 0 (we're moving everything)
        // and in that case, we don't have to check anything
        if(idx > 0)
        {
            const char*    junk;
            int            pxl;
            int            pxp;
            int             junklen;
            cvec_t          key;
            W_DO(this->rec(idx, pxl, pxp, key, junk, junklen));
            if(pxl>0) {
                cvec_t    aux;
                aux.put(junk,junklen);
                // It had better fit!
                DBG(<<"Shift : first rec has " << pxp << " prefix parts");
                W_COERCE(rsib->insert(key, aux, 0));
                start_simple_move ++;
            }
        }
    }

    /* 
     * grot performance hack: do in chunks of up to 
     * tmp_chunk_size slots at a time
     */
    const int tmp_chunk_size = 20;    // XXX magic number
    vec_t *tp = new vec_t[tmp_chunk_size];
    if (!tp)
        return RC(fcOUTOFMEMORY);
    w_auto_delete_array_t<vec_t>    ad_tp(tp);

    for (int i = start_simple_move; i < n && (! rc.is_error()); ) {
        int j;

        // NB: this next for-loop increments variable i !!!
        for (j = 0; j < tmp_chunk_size && i < n; j++, i++)  {
            tp[j].set(page_p::tuple_addr(1 + idx + i),
                  page_p::tuple_size(1 + idx + i));
        }

        // i has been incremented j times, hence the
        // subtraction for the 1st arg to insert_expand():
        rc = rsib->insert_expand(1 + i - j, j, tp); // do it & log it
    }
    if (! rc.is_error())  {
        DBG(<<"Removing " << n << " slots starting with " << 1+idx
            << " from page " << pid().page);
        rc = remove_compress(1 + idx, n);
    }
    DBG(<< " page " << pid().page << " has " << nrecs() << " slots");
    DBG(<< " page " << rsib->pid().page << " has " << rsib->nrecs() 
        << " slots");

    return rc.reset();

}


/*********************************************************************
 *
 *  zkeyed_p::shift(idx, idx_dest, rsib, compressed)
 *
 *  Shift all entries starting at "idx" to idx_dest entry of page "rsib".
 *  This is called from btree.cpp, where nothing is known about
 *  key compression, so it's up to this function to maintain the
 *  invariant that the first slot on a page has no compression.
 *
 *********************************************************************/
rc_t
zkeyed_p::shift(slotid_t idx, slotid_t idx_dest, zkeyed_p* rsib, bool compressed)
{
    FUNC(zkeyed_p::shift);
    w_assert1(idx >= 0 && idx < nrecs());

    int n = nrecs() - idx;

    DBG(<<"zkeyed_p::SHIFT "  
        << " (compressed=" << compressed 
        << ") from page " << pid().page << " idx " << idx
        << " #recs " << n
        );

    int start_simple_move=0;
    rc_t rc;
    
    if(compressed) {
        // First slot is a special case
        // See if it's got a compressed prefix; if so,
        // materialize the whole thing through zkeyed_p::rec
        // before stuffing it into the rsib.
        //
        // In several cases, though, idx is 0 (we're moving everything)
        // and in that case, we don't have to check anything
        if(idx > 0)
        {
            const char*    junk;
            int            pxl;
            int            pxp;
            int             junklen;
            cvec_t          key;
            W_DO(this->rec(idx, pxl, pxp, key, junk, junklen));
            if(pxl>0) {
                cvec_t    aux;
                aux.put(junk,junklen);
                // It had better fit!
                DBG(<<"Shift : first rec has " << pxp << " prefix parts");
                W_COERCE(rsib->insert(key, aux, 0));
                start_simple_move ++;
            }
        }
    }
    
    /* 
     * grot performance hack: do in chunks of up to 
     * tmp_chunk_size slots at a time
     */
    const int tmp_chunk_size = 20;    // XXX magic number
    vec_t *tp = new vec_t[tmp_chunk_size];
    if (!tp)
        return RC(fcOUTOFMEMORY);
    w_auto_delete_array_t<vec_t>    ad_tp(tp);

    for (int i = start_simple_move, k = idx_dest; i < n && (! rc.is_error()); ) {
        int j;

        // NB: this next for-loop increments variable i !!!
        for (j = 0; j < tmp_chunk_size && i < n; j++, i++, k++)  {
            tp[j].set(page_p::tuple_addr(1 + idx + i),
                  page_p::tuple_size(1 + idx + i));
        }

        // i has been incremented j times, hence the
        // subtraction for the 1st arg to insert_expand():
        rc = rsib->insert_expand(1 + k - j, j, tp); // do it & log it
    }
    if (! rc.is_error())  {
        DBG(<<"Removing " << n << " slots starting with " << 1+idx
            << " from page " << pid().page);
        rc = remove_compress(1 + idx, n);
    }
    DBG(<< " page " << pid().page << " has " << nrecs() << " slots");
    DBG(<< " page " << rsib->pid().page << " has " << rsib->nrecs() 
        << " slots");

    return rc.reset();

}


/*********************************************************************
 *
 *  zkeyed_p::set_hdr(data)
 *
 *  Set the page header to "data".
 *
 *********************************************************************/
rc_t
zkeyed_p::set_hdr(const cvec_t& data)
{
    W_DO( page_p::overwrite(0, 0, data) );
    return RCOK;
}



/*********************************************************************
 *
 *  zkeyed_p::insert(key, aux, slot, do_it, compress)
 *
 *  Insert a <key, aux> pair into a particular slot of a
 *  keyed page.  These slots start at 0.
 *  if ! do_it, just find out if there's space for the insert
 *
 *********************************************************************/
rc_t
zkeyed_p::insert(
    const cvec_t&    key,
    const cvec_t&    aux,
    slotid_t         idx,
    bool             do_it,
    bool             compress
)
{
    FUNC(zkeyed_p::insert);
    W_IFDEBUG3( W_COERCE(check()) );
    vec_t v;
    DBG(<<"zkeyed_p::INSERT pid " << pid().page  << " idx " << idx
        << " do_it=" << do_it << " compress=" << compress);
#ifdef W_TRACE
    if(compress && do_it) {
        if(_w_debug.flag_on("zkeyed_p::insert",__FILE__)) {
        _w_debug.clog << __LINE__ << " " << __FILE__ 
            << " KEY BEFORE INSERT " 
            << " in page " << pid().page << endl;
        _w_debug.clog << key;
        _w_debug.clog << __LINE__ << " " << __FILE__ 
            << " AUX BEFORE INSERT "  << endl;
        _w_debug.clog << aux;
        _w_debug.clog << flushl;
        }
    }
#endif /* W_TRACE */

    int2_t    plen = 0;    // length of prefix of inserted tuple
    int        thispxp = 0;
    bool    try_compress = compress;

    v.put(&plen, sizeof(plen));
    int2_t klen = key.size();
    v.put(&klen, sizeof(klen));

    if(try_compress && !do_it) {
        /*
         *  If we're only trying to figure out if the
         * thing fits (for the purpose of finding if we need to
         * split a btree page), let's try something quick first: 
         * if we clearly have enough room, just return that indication, 
         * without doing a lot of analysis.  The worst case is that we
         * insert the entire tuple and we expand the next
         * tuple by the size of its element.
         */
        const char*    junk;
        int            pxl;
        int            pxp;
        int            junklen;
        cvec_t        nxtkey;

        if(idx+1 < nrecs()) {
            W_COERCE(this->rec(idx+1, pxl, pxp, nxtkey, junk, junklen));
            v.put(junk,junklen); // expand v by element size
        }
        W_DO(page_p::insert_expand(idx+1, 1, &v, false/*logit*/,false/*doit*/));
        return RCOK;
    }

    /*
     * First: compare with prior key-elem pair, to see
     * how much we can compress this entry, if at all
     *
     * Second: compare with next key-elem pair, to see
     * whether we have to change the next key-elem pair.
     * Insertion of this entry could invalidate the next
     * one.
     *
     * Do all the analysis up front, so that we can
     * return an error if the necessary changes won't
     * fit on this page, and catch that situation before
     * any changes are done (and logged).
     */

    /*
     * First: compare with prior key-elem pair
     */
    bool done = false;
    if( try_compress && idx>0 ) {
    size_t         common_size;

    // Get prior entry, see if the keys have anything
    // in common.
    const char *junk;
    int         junklen;
    cvec_t      prevkey;
    int        prevpxl;
    int        prevpxp;

    w_assert3(idx>0);
    W_DO(this->rec(/*prev*/idx-1,prevpxl, prevpxp, prevkey,junk,junklen));
#ifdef W_TRACE
        if(_w_debug.flag_on("zkeyed_p::insert",__FILE__)) {
        dump(idx-1, __LINE__," PREV BEFORE INSERT ");
        }
#endif

    common_size = key.size(); 

    /*
     * all we care about is what the common size is-
     * we don't really care what the comparison returns.
     * 0, <0, >0 are all legit results of comparison, since
     * it is SOLELY the key that determines what slot
     * we use, but here we are comparing the key-elem pairs
     * In other words, this can legitimately happen:
     * key > prevkey but key.elem < prevkey.prevelem
     */
    if( key.cmp(prevkey, &common_size) == 0) {
        //  common_size is not set if the two are equal
        common_size = key.size();
    }
    DBG(<<"common size " << common_size);

    if(prevpxp < (int)max_prefix_level) {
        /* We can try compressing -- we haven't
         * exceeded the max # of prefix levels
         */
        if((common_size > sizeof(plen) ) &&
        /*
         * This is sort-of arbitrary, but if the
         * total amount you can compress isn't at least
         * 1/8 the size of the entire thing, it's probably
         * not worth the effort.
         */
        (common_size > (key.size()>>3)) ) {
            INC_TSTAT(bt_pcompress);
            w_assert1(idx != 0);

            //Peel off first common_size bytes from the key.
            cvec_t prefix;
            cvec_t rest;
            key.split(common_size, prefix, rest);

            plen = common_size;
            DBG(<<"set prefix len = " << plen);
            DBG(<<"put prefix.rest, size= " << rest.size());
            v.put(rest);
            thispxp = prevpxp + 1;
            done = true;
        }
    }
    } 
    if(done)  {
    // We already put the key in place 
    w_assert3(plen != 0);
    } else  { // !done
    w_assert3(plen == 0);
    v.put(key);
    DBG(<<"put key, size= " << key.size());
    }


    DBG(<<"common prefix len = " << plen);
    DBG(<<"put aux, size= " << aux.size());

    // Put the rest...
    v.put(aux);

    /*
     * Second: compare with next key-elem pair, to see
     * whether we have to rewrite the next key-elem pair.
     * Insertion of this entry could invalidate the next
     * one.
     * So far, we've only computed the vector for this new
     * slot; we haven't inserted it yet, so there's nothing
     * to undo.
     */
    done = false;

    /*
     * Is there a next record? 
     */
    // (idx here is the record number, not the slot number,
    // so we compare with nrecs, not nslots:)
    if(try_compress && (idx+1 < nrecs())) {
        /* 
         * Yes, there is a next record.  Will insertion of
         * this new record invalidate the prefix of the next
         * record?
         */
        size_t       common_size;
        const char *junk;
        int         junklen;
        cvec_t      nextkey;
        int        nextpxl;
        int        nextpxp;

        /*
         * idx now represents "next"  because we 
         * haven't yet done the insert:
         */
        W_DO(this->rec(idx, nextpxl, nextpxp, nextkey,junk,junklen));
        DBG(<<"idx " << idx
            << " nextpxl " << nextpxl
            << " nextpxp " << nextpxp
            << " nextkey.size() " << nextkey.size()
            << " junklen " << junklen
        );
#ifdef W_TRACE
        if(_w_debug.flag_on("zkeyed_p::insert",__FILE__)) {
            dump(idx, __LINE__," NEXT BEFORE INSERT ");
        }
#endif

        if(key.cmp(nextkey, &common_size) == 0) {
            //  common_size is not set if the two are equal
            common_size = key.size();
        }
        bool expand_next = false;
        bool compress_next = false;

        DBG(<<"common size " << common_size);
        if( (thispxp + 1 >= (int)max_prefix_level) 
            || !try_compress) {
            /* 
             * next record, if compressed using this record,
             * would exceed max_prefix_level, and so it must
             * be expanded fully if it's not already.
             * Fake it out by pretending that there
             * is nothing in common with this new record.
             */
            common_size = 0;
            DBG(<<"");
        } 
        if(   (common_size < (nextkey.size()>>3)) ||
                  (common_size <= sizeof(uint2_t))
        ) {
            // Just expand it fully - it's not worth
            // the work
            common_size = 0;
            DBG(<<"");
        }

        if((int)common_size < nextpxl ) {
            /* 
             * we have to fix the next entry: its
             * prefix will be shortened
             */
            expand_next = true;
            DBG(<<"");
        } else if((int)common_size > nextpxl ) {
            /* 
             * We could lengthen the next prefix.
             * But we have to do the lengthening at
             * the same time that we do the insertion
             * of this new record.
             */
            compress_next = true;
            DBG(<<"");
        } else {
            w_assert3((int)common_size == nextpxl );
            /*
             * no change to next entry's prefix
             */
            DBG(<<"");
        }
        DBG(<<"common size " << common_size);

        if(expand_next) {
            /*
             * Do the expansion,
             * then do the insertion, because the expansion
             * doesn't have to be undone if we fail on the
             * insertion.  By letting 'done' remain false,
             * the insertion will be done below...
             */
            W_DO(rewrite(idx, common_size));
        } else if(compress_next) {
            /*
             * Compress next record (lengthen prefix).
             * we'll insert
             * this new item by splitting slot idx+1.
             *  page_p::split_slot(idx+1, off,  vec_t v1, vec_t v2)
             * where off = next prefix len + len of new common part
             * v1 contains: end of this-key-value (part 
             *                   not-common to this, next)
             * v2 contains: updated next.prefixlen, next.keylen 
             *  
             */
            INC_TSTAT(bt_pcompress);

            if( (int)common_size <= plen) {
                /* 
                 * Next rec has less in common with this
                 * than this has in common with prev. We can
                 * simply insert this record, and rewrite next record
                 * for max compression.
                 * BUT we don't want to do it in that order, because
                 * we don't want to deal with running out of space
                 * on the page before compressing next.
                 * Given the relationship of the two prefixes,
                 * we know that next has in common with prev at least
                 * common-size bytes, even if next record isn't compressed
                 * right now, and 
                 * it's safe to rewrite (compress) next before doing
                 * the insert.   We'll cause the insert to be done
                 * below by leaving "done" false.  Just do the rewrite 
                 * here.
                 */
                // idx still represents next rec, since we haven't
                // yet done the insert
                W_DO(rewrite(idx, common_size));
            } else {

                uint2_t  nextklen = nextkey.size();
                uint2_t  thiskeylen = key.size();

                cvec_t v1;
                cvec_t firstpart;

                slot_length_t offset =  (sizeof(uint2_t) + sizeof(uint2_t) +
                                common_size - plen);

                // v is the vector containing thiskey, compressed.
                // Make a copy of it, split into 2 parts: the 
                // part in common with nextkey, and the part that's
                // not in common with nextkey.

                DBG(<<"splitting vector at offset " << offset);
                w_assert3(offset > sizeof(uint2_t)*2);

                v.split(offset, firstpart, v1);
#if W_DEBUG_LEVEL > 2
                {
                    cvec_t _dummy;
                    size_t  len;
                    _dummy.put(firstpart);
                    _dummy.put(v1);
                    w_assert1(_dummy.cmp(v, &len) == 0);
                }
#endif 
                /* firstpart will be discarded because we'll use the
                 * first part of next slot for the contents of "firstpart"
                 */

                cvec_t v2;
                offset =  (sizeof(uint2_t) + sizeof(uint2_t) +
                                common_size - nextpxl);

                uint2_t  nextplen = common_size;
                v2.reset().put(&nextplen, sizeof(nextplen));
                v2.put(&nextklen, sizeof(nextklen));

                /*
                 * Split the next slot into two, using part of the
                 * record for the new slot, retaining part for the next one.
                 * The point at which we split it is "offset".
                 *
                 * Use idx+1 because it takes a slot# , not a record#
                 */
                DBG(<<"splitting slot " << idx+1 << " into two at offset " 
                    << offset << " nextklen=" << nextklen );
                W_DO(page_p::split_slot(idx+1, offset,  v1, v2));
                DBG(<<" split done.");

                vec_t keylen(&thiskeylen, sizeof(thiskeylen));
                /* 
                 * update key len at the same slot because NOW
                 * that slot contains our inserted entry
                 */
                DBG(<<"overwriting slot " << idx+1 
                    << " at offset " << sizeof(uint2_t)
                    << " with vec "<< keylen.size()
                    << " value "<< thiskeylen
                    );
                W_COERCE(page_p::overwrite(idx+1, sizeof(uint2_t), keylen));

                done = true;
            }
        }
    }
    if(!done) {
        // we'll say if you doit, log it
        W_DO( page_p::insert_expand(idx + 1, 1, &v, do_it, do_it) );
    }

    W_IFDEBUG3( W_COERCE(check()) );

    DBG(<<"");
#ifdef W_TRACE
    if(compress && do_it) {
        if(_w_debug.flag_on("zkeyed_p::insert",__FILE__)) {
            if(idx>0) {
                dump(idx-1, __LINE__," PREV AFTER INSERT ");
            }

            w_assert3(idx < nrecs());
            dump(idx, __LINE__," THIS AFTER INSERT ");

            if(idx+1 < nrecs()) {
                dump(idx+1, __LINE__," NEXT AFTER INSERT ");
            }
        }
    }
#endif /* W_TRACE */
    return RCOK;
}

/*********************************************************************
 *
 *  zkeyed_p::rewrite(rec#, new prefix len)
 *
 *  Rewrite the record so that it has the given new prefix compression. 
 *  This cuts and pastes from prior records as needed.
 *  Thus, the prior records must the the correct ones when
 *  this is called.
 *
 *********************************************************************/

rc_t
zkeyed_p::rewrite(
    slotid_t     idx,
    int     prefix_len
)
{
    FUNC(zkeyed_p::rewrite);

    W_IFDEBUG3( W_COERCE(check()) );
    DBG(<<"zkeyed_p::rewrite pid " 
    << pid().page  << " idx " << idx
    << " using pfx len " << prefix_len
    );

    const char *junk;
    int         junklen;
    cvec_t      thiskey;
    int            thispxl;
    int            thispxp;

    if(idx == 0) {
    w_assert3(prefix_len == 0);
    }

    W_DO(this->rec(/*this*/idx, thispxl, thispxp, thiskey,junk,junklen));
    slotid_t slot = idx+1;


#if W_DEBUG_LEVEL > 2
    char *savebuf = new char[smlevel_0::page_sz];
    if (!savebuf) W_FATAL(fcOUTOFMEMORY);
    w_auto_delete_array_t<char> ad_savebuf(savebuf);
    size_t savesize;
    // copy the rec for later comparison purposes 
    savesize = thiskey.copy_to(savebuf, thiskey.size());

    dump(idx, __LINE__," BEFORE REWRITE ");
#endif 
    /* 
     * these are put here only for debugging if the
     * comparison at the end fails
     */
    cvec_t junkvec;
    cvec_t rest;
    cvec_t v;
    /********/

    cvec_t     vpx;
    uint2_t      plen = prefix_len;
    vpx.put(&plen, sizeof(plen));

    if(prefix_len > thispxl) {
    /*
     * remove some of the data and overwrite
     * the prefix length
     */
    W_DO(page_p::cut(slot, sizeof(uint2_t)*2, prefix_len - thispxl));
    W_COERCE(page_p::overwrite(slot , 0, vpx));
    } else if (prefix_len < thispxl) {
    /*
     * insert some of the data and overwrite
     * the prefix length
     */
    DBG(<<"splitting vec at " << prefix_len);
    thiskey.split(prefix_len, junkvec, rest);

    DBG(<<"splitting rest at " << thispxl);
    rest.split(thispxl, v, junkvec);

    w_assert3((int)v.size() == thispxl - prefix_len);

    /* 
     * At this point, we've got to copy out what we're pasting in,
     * so that the paste operation doesn't clobber what we're copying
     */
    w_assert1(v.size() < (smlevel_0::page_sz>>1));


    {
        char *save = new char[v.size()]; //auto-del
        w_auto_delete_array_t<char> autodel(save);
        size_t savesz;
        savesz = v.copy_to(save, v.size());

        v.reset().put(save, savesz);

        DBG(<<"pasting in " << v.size() << " bytes");
        W_DO(page_p::paste(slot, sizeof(uint2_t)*2, v));

        DBG(<<"total slot size " << page_p::tuple_size(slot));
        DBG(<<"overwrite pxl value " << thispxl << " with " << plen);
        W_COERCE(page_p::overwrite(slot , 0, vpx));
    }
    } else {
    /* no change */
    w_assert3(prefix_len == thispxl);
    DBG(<<"rewrite not necessary");
    }

#if W_DEBUG_LEVEL > 2

    dump(idx, __LINE__," AFTER REWRITE ");

    thiskey.reset();
    W_DO(this->rec(/*this*/idx,
    thispxl, thispxp, thiskey,junk,junklen));

    w_assert3(prefix_len == thispxl);
    if(prefix_len == 0) {
        w_assert3(thispxp == 0);
    }
    // compare finished data to pre-rewrite data
    w_assert3(thiskey.cmp(savebuf, savesize)==0);
#endif 

    W_IFDEBUG3( W_COERCE(check()) );
    return RCOK;
}




/*********************************************************************
 *
 *  zkeyed_p::remove(slot)
 *
 *********************************************************************/
rc_t
zkeyed_p::remove(slotid_t idx, bool compress)
{
    FUNC(zkeyed_p::remove);
    W_IFDEBUG3( W_COERCE(check()) );
    /*
     * If the next record has a non-zero prefix, we might have to 
     * expand or shrink it after removing this record.
     *
     * This is not exactly "tight" code, but in the interest
     * of readability, it's left to the compiler to optimize
     * it.
     */
    DBG(<<"zkeyed_p::REMOVE pid " << pid().page  
    << " idx " << idx
    << " compress " << compress
    );

#ifdef W_TRACE
    if(_w_debug.flag_on("zkeyed_p::remove",__FILE__)) {
    dump(idx, __LINE__," THIS BEFORE REMOVE ");
    }
#endif

    slotid_t this_slot   = idx + 1;      // page_p::slot of interest
    slotid_t next_rec    = idx + 1;     // zkeyed_p::rec# of next
   
    bool     expand_next = false;
    bool     compress_next = false;

    bool        try_compress = (compress && (max_prefix_level > 0));
    bool     done = false;
    slotid_t     this_rec   = idx; // zkeyed_p::rec# of interest
    size_t      common_size = 0;

    /*
     * Is there a next entry?
     */
    if(try_compress && (next_rec < nrecs())) {
    /* 
     * Yes, there is a next record.  Will removal of
     * this new record invalidate the prefix of the next
     * record?
     */

    // These slot numbers are confusing, because the
    // page_p slots use a different index from zkeyed_p

    slotid_t prev_rec  = idx-1;            // zkeyed_p::rec# of interest

    const char *junk;
    int         junklen;
    cvec_t      nextkey;
    int        nextpxl;
    int        nextpxp;

    W_DO(this->rec(next_rec, nextpxl, nextpxp, nextkey, junk, junklen));
#ifdef W_TRACE
    if(_w_debug.flag_on("zkeyed_p::remove",__FILE__)) {
        dump(next_rec, __LINE__," NEXT BEFORE REMOVE ");
    }
#endif

    if(nextpxl > 0) {
        /* the next record was compressed */

        if(this_rec == 0) {
         /*
          * Must fully expand next rec
          */
        common_size = 0;
        DBG(<<"");
        } else {
        /* 
         * Look at prev record to re-compute the prefix for next
         */
        cvec_t      prevkey;
        int        prevpxl;
        int        prevpxp;
        W_DO(this->rec(prev_rec, 
            prevpxl, prevpxp, prevkey, junk, junklen));
#ifdef W_TRACE
        if(_w_debug.flag_on("zkeyed_p::remove",__FILE__)) {
            dump(prev_rec, __LINE__," PREV BEFORE REMOVE ");
        }
#endif
        bool    try_compress = max_prefix_level > 0;

        if( (prevpxp + 1 >= (int)max_prefix_level) 
            || !try_compress) {
            common_size = 0;
            /* 
             * next record, if compressed using prev record,
             * would exceed max_prefix_level, and so it must
             * be expanded fully.
             * Fake it out by pretending that there
             * is nothing in common with the prev record.
             */
            DBG(<<"");
        } else if(nextkey.cmp(prevkey, &common_size) == 0) {
            //  common_size is not set if the two are equal
            common_size = prevkey.size();
            DBG(<<"");
        }
        DBG(<<"common size " << common_size);

        if(   (common_size < (nextkey.size()>>3)) ||
                  (common_size <= sizeof(uint2_t))
        ) {
            /*
             * Just expand it fully - it's not worth
             * the work
             */
            common_size = 0;
            DBG(<<"");
        }
        }
        if((int)common_size < nextpxl ) {
        /* 
         * we have to fix the next entry: its
         * prefix will be shortened, and some of
         * the data have to be yanked from this record
         * before we remove this record. We have to
         * do all this in one fell swoop.
         */
        DBG(<<"");
        expand_next = true;
        } else if((int)common_size > nextpxl ) {
        /* 
         * We could lengthen the next prefix.
         */
        DBG(<<"");
        compress_next = true;
        } else {
        /*
         * no change to next entry's prefix
         */
        DBG(<<"");
        w_assert3((int)common_size == nextpxl );
        }
        DBG(<<"common size " << common_size);

        if(expand_next) {
        /*
         * Some of the data (might) have to be yanked from
         * this record before/while we remove this record
         */
        cvec_t      thiskey;
        int        thispxl;
        int        thispxp;
        W_DO(this->rec(this_rec, 
            thispxl, thispxp, thiskey, junk, junklen));
        DBG(<<"idx " << this_rec
            << " thispxl " << thispxl
            << " thispxp " << thispxp
            << " thiskey.size() " << thiskey.size()
            << " junklen " << junklen
        );
        if( ((int)common_size >= thispxl)
            ||
            (nextpxl >= thispxl)
        ) {
            /* 
             * we need to grab some data from this record
             * before the record is removed. We do this
             * in one operation
             */

            DBG(<<"EXPAND next: ");
            uint2_t     nextkeylength = nextkey.size();
            int     off1, off2;

            /*
             * set the offsets into the slot - the 
             * beginning of the stored key is right after
             * the prefix length and key length
             */
            off2 = sizeof(int2_t) + sizeof(int2_t);
            off1 = off2 + (nextpxl - thispxl);
            DBG(<<"merge_slots: " << this_slot << ","
                << off1 << "," << off2 );
            W_DO(page_p::merge_slots(this_slot, off1, off2));

            /*
             * NB: after the merge, this_slot is all that's
             * left.  Next_slot is gone, but its data are
             * in this_slot.
             * update the key length for this_slot:
             */
            vec_t keylen(&nextkeylength, sizeof(nextkeylength));
            // Keylen is located *after* prefix len.
            DBG(<<"over_write: " << this_slot << ","
            << sizeof(uint2_t) << "," << nextkeylength );
            W_DO(page_p::overwrite(this_slot, sizeof(uint2_t), keylen));

            done = true;
            expand_next = false;
        } else {
            /*
             * all three records: next, this, prev
             * are the same in the first common_size bytes,
             * so we don't need to copy any data from this
             * record before we do the expansion.  Next
             * is being expanded fully, most likely, for
             * reasons such as maintaining max# levels.
             *
             * We remove this record, then rewrite next,
             * so that we don't run out of space.
             */
            DBG(<<"EXPAND next: common_size= " << common_size);
        }
        } else if(compress_next) {
        /*
             *  Compress next record (lengthen prefix).
         *  First we remove this record, then we
         *  will rewrite the next record. The rewrite
         *  is really an optimization; it's not a
         *  correctness matter here.
         */
        INC_TSTAT(bt_pcompress);
        DBG(<<"COMPRESS next: common_size= " << common_size);
        }
    }
    } 
    if(!done)  {
    // DO NOT MOVE - this depends
    // on the if(!done) of previous line!
#ifdef W_TRACE
    if(_w_debug.flag_on("zkeyed_p::remove",__FILE__)) {
        if(idx>0) {
        dump(idx-1, __LINE__," PREV BEFORE REMOVE_COMPRESS ");
        }
        dump(idx, __LINE__," THIS BEFORE REMOVE_COMPRESS ");
        if(idx+1 < nrecs()) {
        dump(idx+1, __LINE__," NEXT BEFORE REMOVE_COMPRESS ");
        }
    }
#endif /* W_TRACE */
    W_DO( page_p::remove_compress(this_slot, 1) ); 
    }

    if(compress_next || expand_next) {
    /* NB:
     * next_rec's slot number has changed, 
     * since we've DONE the remove_compress !!!
     */
    next_rec  -= 1;
    w_assert3(next_rec == idx);
    w_assert3(next_rec < nrecs());
    DBG(<<"rewriting next rec with common_size= " <<common_size);
    W_DO(rewrite(next_rec, common_size));

#ifdef W_TRACE
    if(next_rec < nrecs()) {
        if(_w_debug.flag_on("zkeyed_p::remove",__FILE__)) {
        dump(next_rec, __LINE__," NEXT AFTER REWRITE ");
        }
    }
#endif
    }

    W_IFDEBUG3( W_COERCE(check()) );
    return RCOK;
}



/*********************************************************************
 *
 *  zkeyed_p::format(pid, tag, flags, store_flags)
 *
 *  Should never be called (overriden by derived class).
 *
 *********************************************************************/
rc_t
zkeyed_p::format(    
    const lpid_t& /*pid*/, 
    tag_t /*tag*/, 
    uint4_t /*page flags*/,
    store_flag_t  /*store_flags*/
    )
{
    /* 
     *  zkeyed_p is never instantiated individually. it is meant to
     *  be inherited.
     */
    w_assert1(eINTERNAL);
    return RCOK;
}



/*********************************************************************
 *
 *  zkeyed_p::format(pid, tag, flags, store_flags, hdr)
 *
 *  Format a page with header "hdr". 
 *
 *********************************************************************/
rc_t
zkeyed_p::format(
    const lpid_t&     pid, 
    tag_t         tag, 
    uint4_t         page_flags,
    store_flag_t        store_flags,
    const cvec_t&     hdr)
{
    w_assert3(tag == t_btree_p ||
              tag == t_zkeyed_p);

    /* last arg== false -> don't log */
    W_DO( page_p::_format(pid, tag, page_flags, store_flags) );
    // do it, but don't log it
    W_COERCE( page_p::insert_expand(0, 1, &hdr, false/*logit*/) ); 

    /* Now, log as one (combined) record: */
    W_DO(log_page_format(*this, 0, 1, &hdr)); // zkeyed_p btree_p
    return RCOK;
}

/*--------------------------------------------------------------*
 *    zkeyed_p::rec()                        *
 *--------------------------------------------------------------*/
rc_t
zkeyed_p::rec(
    slotid_t         idx, 
    cvec_t&         key,
    const char*&     aux,
    int&         auxlen) const
{
    int pxl, pxp;
    w_rc_t rc = rec(idx, pxl, pxp, key, aux, auxlen);
    w_assert3((smsize_t) pxp <= MAXPFX);
    return rc;
}


struct pxinfo_t {
    const char *pfx; // the string
    int  pxxl;         // length of the prefix for *this*
    int  strl;         // length of this string for this
             // portion of the prefix
    int  use;         // length of this string to be used
};

#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_array_t<pxinfo_t>;
#endif


rc_t
zkeyed_p::rec(
    slotid_t         idx, 
    int&                prefix_len,
    int&                prefix_parts,
    cvec_t&         key,
    const char*&     aux,
    int&         auxlen) const
{
    FUNC(zkeyed_p::rec);
    const char* base = (char*) page_p::tuple_addr(idx + 1);
    const char* p = base;
    prefix_len = 0;
    prefix_parts = 0;

    /* 
     * a record is:
     * -length (int4_t) of key
     * -key
     * -value
     *
     * Do prefix compression by changing this to
     * -length (int2_t) of prefix
     * -length (int2_t) of key
     * -key
     * -value
     */
    int pxl = (int) (* (int2_t*) p);
    p += sizeof(int2_t);

    // First item on the page cannot have a compressed prefix
    if(idx == 0) {
#if W_DEBUG_LEVEL > 2
    if(pxl!=0) {
        DBG( << "PAGE's FIRST TUPLE HAS COMPRESSION!" << pxl);
    }
#endif 
    w_assert1(pxl == 0);
    }

    int l = (int) (* (int2_t*) p);
    p += sizeof(int2_t);

    if(pxl==0) {
    w_assert3( (int)page_p::tuple_size(idx+1) >= l);
    }
    if(pxl>0) {
    pxinfo_t *prefixes = new pxinfo_t[MAXPFX+1]; // auto-del
    w_auto_delete_array_t<pxinfo_t> autodel(prefixes);


    /*  
     *  First, work backward, computing pointers to the prefix
     *  portions and the *their* prefix lengths, until we find
     *  a key with no prefix
     *  When we are done, the structs will mean:
     *    pfx -> ptr to the prefix portion
     *    pxl -> length of this prefix portion (unexpanded)
     *    pxxl -> length of this prefix portion's prefix
     */
    int     _kl = l;
    int     _pxl = pxl;
    const char* _pfx = p;
    int     i=0;
    bool    keep_going = true;

    while(keep_going) {
        DBG(<<"Prefix " << -i
        << " _pxl:" << _pxl 
         << " expanded:" << _kl 
         << " _strl " << _kl - _pxl
         );
        prefixes[i].pxxl = _pxl;
        prefixes[i].strl = _kl - _pxl;
        prefixes[i].use = l - prefix_len - prefixes[i].pxxl;
        prefixes[i].use = (prefixes[i].use <0)? 0: prefixes[i].use;
        prefixes[i].pfx = _pfx;
        DBG(<<"Use " << prefixes[i].use);

        prefix_len += prefixes[i].use;

        keep_going = (prefixes[i].pxxl > 0);

        if(keep_going) {
        i++;
        w_assert1(i <= idx);
        _pfx = (const char*) page_p::tuple_addr((idx-i)+1);
        _pxl = (int) (* (int2_t*) _pfx);
        _pfx += sizeof(int2_t); // skip past prefix-length, to key-length
        _kl = (int) (* (int2_t*) _pfx);
        _pfx += sizeof(int2_t); // again to get past key-length
        }
    }
    prefix_parts = i;
    w_assert3(prefix_len == l);

    prefix_len = 0;
    while(i > 0) {
        if(prefixes[i].use > 0) {
        // Put only the amount needed 
        key.put(prefixes[i].pfx, prefixes[i].use);
        prefix_len += prefixes[i].use;
        }

        i--;
    }
    }
    w_assert3(prefix_len == pxl);

    DBG(<<" total prefix len " << prefix_len);
    DBG(<<" total key size so far " << key.size());

    l -= prefix_len;
    key.put(p, l);
    // DBG(<<" keylen " << l);
    // DBG(<<" total key size so far " << key.size());
    
    p += l;
    aux = p;
    auxlen = page_p::tuple_size(idx + 1) - (p - base);

    if((int) GET_TSTAT(bt_plmax) < prefix_parts)  
    SET_TSTAT(bt_plmax,prefix_parts);

    /*
     * NB: you cannot guarantee that the max prefix level
     * will not be exceeded. Say that your max level is 3,
     * and you insert as follows:
     * 0000001  
     * 0000009  pxl=1
     * 0000008  pxl=1, and 0000009's pxl=2
     * 0000007  pxl=1, and 0000009's pxl=3
     * 0000006  pxl=1, and 0000009's pxl=4
     * 0000005  pxl=1, and 0000009's pxl=5
     * 0000004  pxl=1, and 0000009's pxl=6
     * 0000003  pxl=1, and 0000009's pxl=7
     * 0000002  pxl=1, and 0000009's pxl=8
     *
     * The best we can hope for is that the insertion pattern
     * will include enough randomness to keep this from happening
     * in the worst way.
     *
     * So we remove the following assert:
    // w_assert3( GET_TSTAT(bt_plmax) <= max_prefix_level );
     */

    return RCOK;
}

/*--------------------------------------------------------------*
 *  zkeyed_p::rec_expanded_size()                    *
 *--------------------------------------------------------------*/
int 
zkeyed_p::rec_expanded_size(slotid_t idx) const
{
    const char*    junk;
    int            pxl;
    int            pxp;
    int            junklen;
    cvec_t         key;
    W_COERCE(this->rec(idx, pxl, pxp, key, junk, junklen));
    return     rec_size(idx) + pxl;
}

void
zkeyed_p::dump(slotid_t W_IFTRACE(idx),
           int    W_IFTRACE(line),
           const char *W_IFTRACE(string)) const
{
#ifdef W_TRACE
    const char *junk;
    int         junklen;
    cvec_t      key;
    int            pxl;
    int            pxp;

    w_rc_t rc = this->rec(idx, pxl, pxp, key, junk, junklen);
    if(rc.is_error()) {
        DBG(<<"rc= " << rc);
        return;
    }
    DBG(<<"idx " << idx
        << " pxl " << pxl
        << " pxp " << pxp
        << " key.size() " << key.size()
        << " junklen " << junklen
    );
    if(_w_debug.flag_on("zkeyed_p::remove",__FILE__)) {
    _w_debug.clog << line << " " << __FILE__ 
        << string 
        << " page " << pid().page 
        << " record " <<  idx
         << key
        << endl;
    _w_debug.clog << __LINE__ << " " << __FILE__ 
        << " junklen " <<  junklen
        << flushl;
    }
#endif /* W_TRACE */
}

