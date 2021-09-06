/*<std-header orig-src='shore'>

 $Id: sort_funcs.cpp,v 1.26 2010/05/26 01:20:52 nhall Exp $

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
 * A set of applications functions -- to be moved into the
 * sm library -- to handle new sorting API
 */


#include "shell.h"
#include "sort_funcs.h"


// Turn this to 1 if you want excessive tracing of
// the verify routines
#define TRACE_VERIFY 0


/*
 * Helper function for next test function below
 * Scans index and file. Index is supposed to be key=oid
 * and file contains objects whose last few bytes are oid that
 * matches corresponding index entry.  Index and file should be
 * in same order vis-a-vis the oids.
 */
w_rc_t
compare_index_file(
    stid_t idx,
    stid_t fid, 
    bool did_deep_copy,
    int n
)
{
    DBG(<<"----------------Compare_index_file, idx=" << idx << " file="
            << fid << " deep=" << did_deep_copy);
    char    stringbuffer[MAXBV];
    scan_index_i scani(idx, scan_index_i::gt, cvec_t::neg_inf, 
                  scan_index_i::lt, cvec_t::pos_inf,
                  true // include nulls
                  );
    scan_file_i  scanf(fid, ss_m::t_cc_file);

    bool     feof;
    bool     ieof;
    w_rc_t   rc;
    pin_i*   pin;
    vec_t    key;
    smsize_t klen, elen;
    rid_t    rid;
    int      i=-1; // so the numbers line up with those
    // in verify_index

    while ( !(rc=scani.next(ieof)).is_error() && !ieof ) 
    // while ( !(rc=scanf.next(pin, 0, feof)).is_error() && !feof ) 
    {
        i++;
        //rc = scani.next(ieof);
        rc = scanf.next(pin, 0, feof);
        if(rc.is_error()) {
            DBG(<<"rc=" << rc);
            return RC_AUGMENT(rc);
        }
        w_assert3(!feof);
        klen = MAXBV;


        key.reset().put(stringbuffer, MAXBV);

        vec_t el;
        elen = sizeof(rid);
        el.put(&rid, elen);
        rc = scani.curr(&key, klen, &el, elen);
        if(rc.is_error()) {
            DBG(<<"rc=" << rc);
            return RC_AUGMENT(rc);
        }
		// Adjust key vector's length so we can print it
		// and use the key to count duplicates
        key.reset().put(stringbuffer, klen);

        w_assert3(elen == sizeof(rid));

        DBG(<< i <<": index key length " 
				<< klen << " el length " << elen << " el=rid=" << rid);
        DBG(<< i <<": file next rid is " << pin->rid());

        /* Compare rid with the oid we find in the file record */
        smsize_t offset = pin->body_size() - sizeof(rid_t);
        // DBG(<<"start: " << pin->start_byte() << " length:" << pin->length() << " offset " << offset);
        while(pin->start_byte()+pin->length() <= offset){ 
            rc = pin->next_bytes(feof); 
            if(rc.is_error()) {
                DBG(<<"rc=" << rc);
                return RC_AUGMENT(rc);
            }
            w_assert3(!feof);
        }
        offset -=  pin->start_byte();
        
        int hdrkk=0;
        memcpy(&hdrkk, pin->hdr(), sizeof(hdrkk));

        bool hasnokey=0;
        memcpy(&hasnokey, pin->hdr()+sizeof(hdrkk), sizeof(hasnokey));

        rid_t myrid;
        if(offset + sizeof(rid_t) > pin->length()) {
            smsize_t amt = pin->length() - offset;
            memcpy(&myrid, pin->body() + offset, amt);
            rc = pin->next_bytes(feof); 
            if(rc.is_error()) {
                DBG(<<"rc=" << rc);
                return RC_AUGMENT(rc);
            }
            w_assert3(!feof);
            memcpy((char *)(&myrid)+amt, pin->body(), sizeof(rid_t) - amt);
        } else {
            memcpy(&myrid, pin->body() + offset, sizeof(rid_t));
        }

        DBG(<< i <<": object " << rid 
                << " hdr has value " << hdrkk 
                << " hasnokey=" << hasnokey
                << " body contains rid " << myrid);

        if(!did_deep_copy) {
            // w_assert1(pin->rid() == myrid); // won't b/c we're comparing
            // the output files of the sort, not the input files.
        }
        if(myrid != rid) {
            if(!hasnokey) {
				scan_index_i*    scan2 = new scan_index_i(idx,
                  scan_index_i::eq, key,
                  scan_index_i::eq, key,
                  true /* nullsok */
                  );

				bool i2eof;
				int j=0;
				w_rc_t rc2;
				while ( !(rc2=scan2->next(i2eof)).is_error() && !i2eof ) j++; 
                cerr << "Mismatch !!! "
				<< " File item # " << i
				<<": pin rid " << pin->rid()
                << " hasnokey = " << int(hasnokey)
                << " key " << key
                << " klen " << klen
				<< " duplicates of key = " << j-1
                << endl
                << " hdr contains value " << hdrkk
                << endl
                << " body contains rid " << myrid
                << " while index item contains rid " << rid
                << endl;
            }
        } else {
            DBG(<<"OK for rid " << rid 
                << " key length " << klen);
        }

        if(TRACE_VERIFY) {
            cout 
            << "CIF I:"<<i << "/"<<n
            << " pin->rid() " << pin->rid()
            << " Klen:"<<klen
            << " Erid:"<<rid
            << " H:("<<hdrkk << "," << int(hasnokey) << ") deep="<<int(did_deep_copy)
            << " Brid:" << myrid
            << endl;
        }
    }
    // rc = scani.next(ieof);
    rc = scanf.next(pin, 0, feof);
    w_assert3(ieof && feof);
    if(rc.is_error()) {
        DBG(<<"rc=" << rc);
        return RC_AUGMENT(rc);
    }
    DBG(<<"----------------Compare_index_file, idx=" << idx << " file="
            << fid << " deep=" << did_deep_copy << " DONE; i=" 
            << i  << " n=" << n
            );
    w_assert1(i+1==n);
    return RCOK;
} // compare_index_file

/*
 * Helper function for test function below.
 * Scans index, counts # nulls and # non-nulls in the
 * entire file.
 */
w_rc_t
count_nulls(
    stid_t idx,
    int expected_total,
    int expected_nulls
)
{
    DBG(<<" count nulls ");
    bool     ieof=false;
    w_rc_t    rc;
    vec_t     key, el;
    smsize_t     klen, elen;

    int     nonnulls=0, nulls=0, total=0;
    char    stringbuffer[MAXBV];
    char    stringbuffer2[MAXBV];
    {
    int     i=0;
    /* Scan for non-nulls */
    scan_index_i scani(idx, 
        scan_index_i::gt, cvec_t::neg_inf, 
        scan_index_i::lt, cvec_t::pos_inf,
        false // do not include nulls
          );
    while ( !( rc = scani.next(ieof)).is_error() && !ieof ) {
        i++;
        elen = (klen = MAXBV);
        key.reset().put(stringbuffer, MAXBV);
        el.reset().put(stringbuffer2, MAXBV);

        rc = scani.curr(&key, klen, &el, elen);
        if(rc.is_error()) {
        DBG(<<"rc=" << rc);
        return RC_AUGMENT(rc);
        }
        w_assert3(key.size() != 0);
    } 
    if(rc.is_error()) {
        DBG(<<"rc=" << rc);
        return RC_AUGMENT(rc);
    }
    w_assert3(ieof);
    nonnulls = i;
    }
    {
        int i=0;
        /* Scan for nulls only */
        vec_t    nullvec;
        scan_index_i scani(idx, 
            scan_index_i::eq, nullvec,
            scan_index_i::eq, nullvec,
            true // include nulls
        );
        while ( !( rc = scani.next(ieof)).is_error() && !ieof ) {
            i++;
            elen = (klen = MAXBV);
            key.reset().put(stringbuffer, MAXBV);
            el.reset().put(stringbuffer2, MAXBV);

            rc = scani.curr(&key, klen, &el, elen);
            if(rc.is_error()) {
            DBG(<<"rc=" << rc);
            return RC_AUGMENT(rc);
            }
            w_assert3(key.size() != 0);
        } 
        if(rc.is_error()) {
            DBG(<<"rc=" << rc);
            return RC_AUGMENT(rc);
        }
        w_assert3(ieof);
        nulls=i;
    }
    {
        int i=0;
        /* Scan whole file */
        vec_t    nullvec;
        scan_index_i scani(idx, 
            scan_index_i::gt, cvec_t::neg_inf, 
            scan_index_i::lt, cvec_t::pos_inf,
            true // include nulls
            );
        while ( !( rc = scani.next(ieof)).is_error() && !ieof ) {
            i++;
            elen = (klen = MAXBV);
            key.reset().put(stringbuffer, MAXBV);
            el.reset().put(stringbuffer2, MAXBV);
            rc = scani.curr(&key, klen, &el, elen);
            if(rc.is_error()) {
            DBG(<<"rc=" << rc);
            return RC_AUGMENT(rc);
            }
            w_assert3(key.size() != 0);
        } 
        if(rc.is_error()) {
            DBG(<<"rc=" << rc);
            return RC_AUGMENT(rc);
        }
        w_assert3(ieof);
        total=i;
    }
    DBG(<<"count nulls found " << nulls << " nulls, " << nonnulls << " non-nulls "
            << total << " total ");
    DBG(<<"expected " << expected_nulls << " nulls, " 
            << expected_total << " total ");
    w_assert0(nulls + nonnulls == total);
    w_assert0(nulls == expected_nulls);
    w_assert0(total == expected_total);
    return RCOK;
}

w_rc_t
count_records(
    stid_t fid,
    int expected_total
)
{
    W_DO( sm->force_buffers(false) );

    bool      feof=false;
    w_rc_t    rc;
    int       i=0;
    pin_i     *pin; 
    {
        scan_file_i  scanf(fid, ss_m::t_cc_file);
        while ( !(rc=scanf.next(pin, 0, feof)).is_error() && !feof ) {
            i++;
            DBG(<< "count_records: " << i << " rid " << pin->rid());
        } 
        if(rc.is_error()) {
            DBG(<<"rc=" << rc);
            return RC_AUGMENT(rc);
        }
        w_assert3(feof);
    }
    DBG( <<"--------------- count records " << fid << " found " << i << " OK ");
    if(i != expected_total) {
        cerr <<"i=" << i << " expected " << expected_total << endl ;
        w_assert0(i == expected_total);
    }
    return RCOK;
}

/*
 * Scan the file, deleting corresponding entries from
 * the index.  Probe, Delete the key/elem pr, re-probe,
 * re-insert, re-probe, re-delete, re-probe.
 * This tests insert/remove of null entries, for one thing.
 * The file given should be the original file if it still
 * exists, so that we can avoid deleting in sorted order.
 */
w_rc_t
delete_index_entries(
    stid_t idx,
    stid_t fid,
    smsize_t keyoffset
)
{
    char    _stringbuffer[MAXBV+ALIGNON];
    char    *stringbuffer = (char *)align((u_long)_stringbuffer) ;
    scan_file_i  scanf(fid, ss_m::t_cc_file);

    bool      feof;
    w_rc_t    rc;
    pin_i*    pin;
    vec_t     key, elem;
    smsize_t  klen, elen;
    rid_t     rid;
    int       i=0;

	while ( !(rc=scanf.next(pin, 0, feof)).is_error() && !feof ) {
		i++;

		smsize_t ridoffset = pin->body_size() - sizeof(rid_t);
		klen = ridoffset - keyoffset;
		smsize_t offset = keyoffset;

		DBG(<<" body size " << pin->body_size()
				<< " rid offset " << ridoffset
				<< " key offset " << keyoffset
				<< " key len " << klen
				);

		/* Get key from file record */
		while(pin->start_byte()+pin->length() <= offset){ 
			rc = pin->next_bytes(feof); 
			if(rc.is_error()) {
				DBG(<<"rc=" << rc);
				return RC_AUGMENT(rc);
			}
			w_assert3(!feof);
		}
		DBG(<<"pin->start_byte() " << pin->start_byte());
		offset -=  pin->start_byte();
		// not handling logical case...
		smsize_t amt = pin->length() - offset;
		DBG(<<"memcpy(stringbuffer, body() + " <<offset << ", " << amt <<")");
		memcpy(stringbuffer, pin->body() + offset, amt);

		if(offset + klen > pin->length()) {
			rc = pin->next_bytes(feof); 
			if(rc.is_error()) {
				DBG(<<"rc=" << rc);
				return RC_AUGMENT(rc);
			}
			w_assert3(!feof);
			offset = 0;
			DBG(<<"memcpy(stringbuffer +" << amt
					<< ", body(),  " << klen-amt <<")");
			memcpy(stringbuffer+amt, pin->body(), klen - amt);
		}

		key.reset().put(stringbuffer, klen);
		DBG(<<"key extracted from orig file: klen=" <<klen << " key=" << key);

		offset = ridoffset;

		/* Get oid from file record */
		while(pin->start_byte()+pin->length() <= offset){ 
			rc = pin->next_bytes(feof); 
			if(rc.is_error()) {
			DBG(<<"rc=" << rc);
				return RC_AUGMENT(rc);
			}
			w_assert3(!feof);
		}
		offset -=  pin->start_byte();

		if(offset + sizeof(rid_t) > pin->length()) {
			smsize_t amt = pin->length() - offset;
			DBG(<<"offset=" <<offset << " amt=" << amt);
			memcpy(&rid, pin->body() + offset, amt);
			rc = pin->next_bytes(feof); 
			if(rc.is_error()) {
			DBG(<<"rc=" << rc);
			return RC_AUGMENT(rc);
			}
			w_assert3(!feof);
			offset = 0;
			DBG(<<"offset=" <<offset << " amt=" << amt);
			memcpy((char *)(&rid)+amt, pin->body(), sizeof(rid_t) - amt);
		} else {
			DBG(<<"copy out rid starting from offset " << offset);
			memcpy(&rid, pin->body() + offset, sizeof(rid_t));
		}
		w_assert3(rid == pin->rid());
		DBG(<<"rid extracted from orig file: " <<rid);

#ifdef W_TRACE
	// _w_debug.setflags("sort_funcs.cpp btree.cpp btree_impl.cpp btree_p.cpp");
#endif
		/*
		 * For key,elem pair, 
		 * Probe, delete, probe, insert, probe, delete, probe
		 */
		char *el = stringbuffer+klen;
		elen = MAXBV;
		bool found;
		DBG(<<" START DEBUGGING NEW KEY " << key 
			<< " for rid " << rid);

		DBG(<< " find_assoc " << key );
		// gets *** FIRST *** elem for this key 
		// -- might not match this oid
		rc = sm->find_assoc(idx, key, el, elen, found);
		DBG(<<rc);

		elem.reset().put(el, elen);
		DBG(<<"found=" << found << " elem = " << elem);

		if(!found) {
			  DBG(<<"ERROR: NOT FOUND");
			cerr << "ERROR: Cannot find index entry for " << 
			key << " " << rid <<endl;
		}
		W_DO(rc);

		if(elen != sizeof(rid_t)) {
			cerr << __LINE__ << " ERROR: wrong elem length - expected " <<
			sizeof(rid_t) << " got " <<elen <<endl;
		}
		if(umemcmp(&rid, el, elen)) {
			/*
			cout << "UNSTABLE SORT: rids don't match: expected " 
			<< rid << " found for key " << key
			<<endl;
			*/
		}

		elem.reset().put(&rid, sizeof(rid_t));

		DBG(<<"**** KEY LEN = " << klen << " for rid " <<rid);

		DBG(<< "destroy " << key << ","<<elem);
		rc = sm->destroy_assoc(idx, key, elem);
		DBG(<< rc);
		W_DO(rc);

		/*
			cout << "PRINT INDEX AFTER REMOVAL of key,elem " 
			<< key 
			<< "," << elem
			<< endl;
			W_DO( sm->print_index(idx) );
		*/

		DBG(<< "find " << key);
		elen = MAXBV;
			rc = sm->find_assoc(idx, key, el, elen, found);
		DBG(<< " after-delete find check returns " << rc
			<< " and found=" << found);
		if(found) {
		   // duplicate key - elems had better not match
			if(elen != sizeof(rid_t)) {
			cerr <<  __LINE__ << " ERROR: wrong elem length - expected " <<
				sizeof(rid_t) << " got " <<elen <<endl;
			}
			if(umemcmp(&rid, el, elen)) {
			   DBG(<<" found duplicate, but elems don't match:  OK");
			} else {
			cerr << "ERROR: found deleted key,elem pr: "
				<< key << " " << rid <<endl;
			}
		}
		W_DO(rc);

			elem.reset().put(&rid, sizeof(rid));
		DBG(<< "create " << key << " elem= " << elem << " rid=" << rid);
			W_DO( sm->create_assoc(idx, key, elem) );

		DBG(<<" DONE DEBUGGING THIS KEY " );
#ifdef W_TRACE
	// _w_debug.setflags("sort_funcs.cpp");
#endif

		DBG(<< "find " << key);
		elen = MAXBV;
			W_DO( sm->find_assoc(idx, key, el, elen, found) );
		if(!found) {
			cerr << "ERROR: can't find inserted key,elem pr: "
				<< key << " " << rid <<endl;
		}
		DBG(<< "destroy " << key);
		W_DO( sm->destroy_assoc(idx, key, elem) );
		DBG(<< "find " << key);
		elen = MAXBV;
			W_DO( sm->find_assoc(idx, key, el, elen, found) );
		if(found) {
		   // duplicate key - elems had better not match
			if(elen != sizeof(rid_t)) {
			cerr <<  __LINE__ << " ERROR: wrong elem length - expected " <<
				sizeof(rid_t) << " got " <<elen <<endl;
			}
			if(umemcmp(&rid, el, elen)) {
			   DBG(<<" found duplicate, but elems don't match:  OK");
			} else {
			cerr << "ERROR: found deleted key,elem pr: "
				<< key << " " << rid <<endl;
			}
		}
		// leave it out of the index
		/*
			cout << "PRINT INDEX AFTER REMOVAL of key " << key << endl;
			W_DO( sm->print_index(idx) );
		*/
#ifdef W_TRACE
	// _w_debug.setflags("none");
#endif
	}

    {
    // Index had better be empty now
    scan_index_i*    scanp = new scan_index_i(idx,
                  scan_index_i::gt, cvec_t::neg_inf, 
                  scan_index_i::lt, cvec_t::pos_inf,
                  true /* nullsok */
                  );
        deleter        d4;     // auto_delete for scan_index_i
        d4.set(scanp);
    bool eof=false;
    for (i = 0; (!(rc = scanp->next(eof)).is_error() && !eof) ; i++) ;
    if(i > 0) {
        cerr << " ERROR: INDEX IS NOT EMPTY!  contains " << i 
            << " entries " << endl;
        w_assert3(0);
    }
    }
    if(rc.is_error()) {
    DBG(<<"rc=" << rc);
    return RC_AUGMENT(rc);
    }
    return RCOK;
}

// helper for t_test_bulkld_int, below
void verify_index(int n, stid_t stid, bool nullsok, char *stringbuffer,
    typed_btree_test t, bool reverse, int h, const vec_t &zeroes,
    typed_value &k)
{
    /* verify index entries w/o scanning file -- just pinning the
     * given records and comparing key with record contents */
    scan_index_i* scanp = 0;
    deleter       d4; // auto_delete for scan_index_i
    {
        scanp = new scan_index_i(stid,
              scan_index_i::gt, cvec_t::neg_inf, 
              scan_index_i::lt, cvec_t::pos_inf,
              true /* nullsok */
              );
    }
    DBG(<<"d4.set scanp");
    d4.set(scanp);

    DBG(<<"---------------verify_index: Starting index scan store " << stid );
    bool       eof;
    w_rc_t     rc;
    int        i;
    pin_i      handle;
    vec_t      indexkey;
    vec_t      indexel;
    smsize_t   klen, elen;
    rid_t      index_rid;
    rid_t      body_rid;

    k._u.i8_num = 0; // just for kicks
    for (i = 0; (!(rc = scanp->next(eof)).is_error() && !eof) ; i++)  
    {

        // with index entry #i, populate indexkey, indexel 
        // Set up the indexkey vector so that it points to 
        // one of: stringbuffer or into the keylocation structure
        klen = k._length;
        if(t == test_bv || t == test_b23 || t == test_blarge) {
            indexkey.reset().put(stringbuffer, MAXBV);
            k._u.bv = stringbuffer;
        } else {
            indexkey.reset().put(&k._u, klen);
        }

        {
            elen = sizeof(index_rid);
            indexel.put(&index_rid, elen);
        }
        // populate indexkey, indexel vectors
        W_IGNORE( scanp->curr(&indexkey, klen, &indexel, elen));

        DBG(<< i <<" :----------------");
        DBG(<< i <<": klen=" << klen << " key= " << indexkey);
        DBG(<< i <<": elen=" << elen << " elem(rid)=" << index_rid);

        if(t == test_bv || t == test_b23 || t == test_blarge) {
            w_assert3(k._u.bv == stringbuffer);
            if(nullsok) {
                w_assert1(klen < MAXBV+1);
            } else {
                w_assert1(klen >= 1 && klen < MAXBV+1);
            }
        } else {
            if(nullsok) {
                w_assert1(klen == smsize_t(k._length) ||
                    klen == 0);
            } else {
                w_assert1(klen == smsize_t(k._length));
            }
        }
        w_assert1(elen == smsize_t(sizeof(rid_t)));

        /*
         * Verify key : convert from the typed value or
         * string buffer just populated, into an int.
         */
        int keysource = 0;
        if(klen > 0) {
            convert_back(k, keysource, t, stringbuffer);// sort_funcs3.cpp
            DBG(<< i <<": index scan found key " << keysource << " object is " << index_rid );
            if(reverse) {
                w_assert0(0-keysource == i - h);
            } else {
                switch(t) {
                case test_i1:
                case test_i2:
                case test_i4:
                case test_i8:
                case test_f4:
                case test_f8:
                    // XXX
                    //w_assert1(keysource == i - h);
                    break;

                case test_b1:
                case test_b23:
                case test_blarge:
                case test_bv:
                case test_u1:
                case test_u2:
                case test_u4:
                case test_u8:
                    // because of unsigned-ness,
                    // things get reorded
                    break;
                default:
                    w_assert1(0);
                    break;
                }
            }
        } else {
            DBG(<< i <<": index scan found NO key " );
        }

        /* 
         * Now verify that this is really the key of that object
         */
        smsize_t tail=0;
        {
            tail = sizeof(rid_t);
            // NOTE: here we are explicitly pinning this rid,
            // not getting the handle from a file scan.
            W_IGNORE(handle.pin(index_rid, zeroes.size()));
        }
        smsize_t offset = zeroes.size() - handle.start_byte();
        smsize_t amt = handle.length() - offset;
        smsize_t l = klen;
        if(klen == 0) {
            // we'll end up copying oid into stringbuf, 
            // because for null keys, the oid gets spread across
            // the two pages
            l = tail;
        }

        typed_value bodykey;
        if(klen == 0) {
            memcpy(stringbuffer, handle.body()+offset, amt);
        } else if(t == test_bv || t == test_b23 || t == test_blarge) {
            memcpy(stringbuffer, handle.body()+offset, amt);
            bodykey._u.bv = stringbuffer;
        } else {
            memcpy(&bodykey._u, handle.body()+offset, amt);
        }
        l -= amt;
        bool eof;
        W_IGNORE(handle.next_bytes(eof));

        // b1, u1, i1 all fit on one page
        // except for tail (oid)

        w_assert1(!eof); // at least 2 pgs/object in this test
        if(klen != 0) {
            w_assert1(handle.length() == l + tail);
        }
        if(klen == 0) {
            memcpy(stringbuffer+amt, handle.body(), l);
        } else if(t == test_bv || t == test_b23 || t == test_blarge) {
            // DBG(<<"memcpy amt=" << l << " at offset " << 0);
            memcpy(stringbuffer+amt, handle.body(), l);
            bodykey._u.bv = stringbuffer;
        } else {
            // DBG(<<"memcpy amt=" << l << " at offset " << 0);
            memcpy( (char *)(&bodykey._u)+amt, handle.body(), l);
        }

        int hdrkk;
        memcpy( &hdrkk, handle.hdr(), sizeof(hdrkk));
        bool hasnokey;
        memcpy(&hasnokey, handle.hdr()+sizeof(hdrkk), sizeof(hasnokey));

        DBG(<< i <<": hdr contains int " << hdrkk << " hasnokey " << hasnokey);

        rid_t    body_rid;
        {
            if(klen == 0) {
                memcpy( &body_rid, stringbuffer, tail);
            } else {
                memcpy( &body_rid, handle.body()+l, tail);
            }
            DBG(<< i <<": rid from index is " <<index_rid 
                    << " oid in object is " << body_rid );
            if(body_rid != index_rid) {
                cerr << "Mismatched OIDs " 
                << " index has " << index_rid
                << " body has " << body_rid 
                << " hdr has " << hdrkk 
                <<endl;
            }
        }
        W_IGNORE(handle.next_bytes(eof));
        w_assert1(eof); // no more than 2 pgs/object in this test

        if(klen == 0) {
            DBG(<< i <<": object contains NULL key "); 
            
            if(TRACE_VERIFY) {
                cout
                << "VI I:"<<i << "/"<<n
                << " K:nul"
                << " Erid:"<<index_rid
                << " H:("<<hdrkk << "," << int(hasnokey) << ")"
                << " Bkey: null " 
                << " Brid:" << body_rid 
                << endl;
            }

        } else if(klen>0) {
            int bodyk; 
            convert_back(bodykey, bodyk, t, stringbuffer);//sort_funcs3.cpp
            DBG(<< i <<": object contains key " << bodyk); 

            if(bodyk != keysource) {
                cerr << "Mismatched key, object" <<endl;
                cerr << "...index key is " << keysource <<endl; 
                cerr << "...index rid is " << index_rid << endl;
                cerr << "...key in body is " << bodyk <<endl;
                cerr << "...key value in hdr is " << hdrkk <<endl;
                cerr << "...body rid is " << body_rid <<endl;
            }

            if(TRACE_VERIFY) {
                cout 
                << "VI I:"<<i
                << " K:"<<keysource
                << " Erid:"<<index_rid
                << " H:("<<hdrkk << "," << int(hasnokey) << ")"
                << " Bkey:" << bodyk
                << " Brid:" << body_rid
                << endl;
            }
        }
    }
    DBG(<<"scan .next yields " << rc << " eof=" << eof);
    w_assert1(!rc.is_error());
    // if(!nullsok) w_assert1(i == n);
    if(i != n) {
        // w_assert1(!nullsok);
        // This assert is wrong only if we wanted unique keys
        w_assert1(0);
    }
    DBG(<<"----------------------------- scan of " << stid << " is finished");

    // d4 will delete the scan ptr
}

/*
 * t_test_bulkload_int_btree : a misnomer -- now it tests several key types
 *                 
 * Does several things: 1) creates file of typed large records with key
 *     that crosses page boundary (unless key is of length 1)
 *      and contains its own OID at the end.
 * 2) sorts file, requesting index output
 * 3) uses that to bulk-load an index
 * 4) checks the index - scans it and compares the oid given with
 *     the orig object's key
 * 5) sorts the orig file, requesting new file output, preserve orig file
 * 6) scans index and new file, comparing keys and trailing OIDs
 */
int
t_test_bulkload_int_btree(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    bool    unique_case=false; // NB: if you turn this to true,
	// the test will fail because the TEST can't handle the
	// case where duplicate nulls are eliminated, partly because
	// it doesn't account for *which* null is left after elimination.
	// (ditto for any other dup that gets eliminated)

    if (check(ip, "vid nkeys keytype nullok|notnull", ac, 5))
        return TCL_ERROR;

    int nkeys_arg = 2;
    int vid_arg = 1;
    int type_arg = 3;
    int null_arg = 4;

    bool     nullsok=false;
    if(strcmp(av[null_arg], "nullok")==0) {
        nullsok = true;
    } else if(strcmp(av[null_arg], "notnull")==0) {
        nullsok = false;
    } else {
        cerr << "Bad argument #" << null_arg
        << " to test_bulkload_int_btree: expected nullok | notnull " 
        << " got " << av[null_arg]
        <<endl;
        return TCL_ERROR;
    }

    int runsize = 3; // minimum
    // Let's puff up the objects a bit - make sure 
    // the key crosses a page boundary

    int data_sz = global_sm_config_info.lg_rec_page_space;

    char *garbage = new char[data_sz-1];
    memset(garbage, '0', data_sz -1);
    w_auto_delete_array_t<char> delgarbage(garbage);
    vec_t    zeroes(garbage, data_sz-1);

    int n = atoi(av[nkeys_arg]);


    /* make the values range run from -n/2 to n/2 */
    const bool     reverse = false; /* Put them into the file in reverse order */
    char           stringbuffer[MAXBV];
    typed_value    k;
    vec_t          data;

    const char *test=av[type_arg];
    typed_btree_test t = cvt2type(test);
    /*
    Based on the type argument, we might have to reduce the nkeys.
    I'm not going to try to mess with uniqueness of keys here, b/c
    it's so hard to do the verify.
    Let's just check with a number of keys that covers no more than
    the range of the key type.
    */

    switch(t)
    {
        case test_bv:
            if(n > MAXBV-1) {
                cerr << "Can't do b*1000 test with more than "
                << MAXBV-1 << " records - sorry" << endl;
                return TCL_ERROR;
            }
            break;
        case test_spatial:
        case test_nosuch:
        case test_b23:
        case test_blarge:
            // don't worry about these
            break;

        case test_b1:
        case test_i1:
        case test_u1:
            if(n > int(0x7f)) {
                cout << "Reducing #key values from " << n << " to " << int(0x7f)
                    << " for test with 1-byte key type "
                    << endl;
                n = int(0x7f);
            }
            break;

        case test_i2:
        case test_u2:
            if(n > int(0x7fff)) {
                cout << "Reducing #key values from " << n << " to " << int(0x7fff)
                    << " for test with 1-byte key type "
                    << endl;
                n = int(0x7fff);
            }
            break;

        case test_i4:
        case test_f4:
        case test_u4:
            if(n > int(0x7fffffff)) {
                cout << "Reducing #key values from " << n 
                    << " to " << int(0x7fffffff)
                    << " for test with 1-byte key type "
                    << endl;
                n = int(0x7fffffff);
            }
            break;

        case test_i8:
        case test_f8:
        case test_u8:
            break;
    }
    const          int h = n/2;

    const char*    kd = check_compress_flag(test); 
    CSKF    lfunc;
    generic_CSKF_cookie lfunc_cookie;
    lfunc_cookie.offset = 0; // to deal with assert in getcmpfunc.
    CF        cmpfunc = getcmpfunc(t, lfunc, 
                        key_cookie_t(&lfunc_cookie));

    lfunc_cookie.offset = zeroes.size(); 
    lfunc_cookie.in_hdr = false; // for this test, keys in body
    k._length =  lfunc_cookie.length;

    smsize_t     len_hint = k._length + zeroes.size(); 
    DBG(<<"min_rec_sz/len_hint " << len_hint
            << " k._length " << k._length
            << " zeroes.size() " << zeroes.size());
    // zeroes.size is data_sz -1

    sort_keys_t kl(1); // one key
    {
    // Set attibutes of sort as a whole
    int bad=0;
    // if(kl.set_keep_orig()) { DBG(<<""); bad++; }
    if(kl.set_unique(unique_case)) { DBG(<<""); bad++; }
    if(kl.set_null_unique(unique_case)) { DBG(<<""); bad++; }
    if(kl.set_ascending()) { DBG(<<""); bad++; }

    // Can't use stable for btree bulk-load because
    // we must use rid order for that.
    kl.set_stable(false);

    // no key cookie
    if(kl.set_for_index(lfunc, key_cookie_t(&lfunc_cookie))) { 
        DBG(<<""); bad++; }
    if(bad>0) {
        w_reset_strstream(tclout);
        tclout << smsh_err_name(ss_m::eBADARGUMENT) << ends;
        Tcl_AppendResult(ip, tclout.c_str(), 0);
        w_reset_strstream(tclout);
        return TCL_ERROR;
    }
    }

    if ((t == test_b1)||(t == test_bv)||(t == test_b23)||(t == test_blarge)) {
        kl.set_sortkey_derived(0, 
        testCSKF, 
        (key_cookie_t)zeroes.size(),
        false, // not in hdr: in body
        true, // aligned
        true, // lexico
        cmpfunc
        );
    } else if(nullsok) {
        kl.set_sortkey_derived(0, 
        testCSKF, 
        (key_cookie_t)zeroes.size(),
        false, // not in hdr: in body
        false, // aligned
        false, // lexico
        cmpfunc
        );
    } else {
        kl.set_sortkey_fixed(0,
        // offset, length:
        zeroes.size(), k._length, 
        false, // not in hdr: in body
        false, // aligned
        false, // lexico
        cmpfunc
        );
    }
    {
    deleter        d1; // auto-delete
    deleter        d2; // auto-delete

    stid_t         stid; // phys case
    stid_t         fid;  // phys case
    vid_t          vid;  // phys case
    {
        DO( sm->create_index(atoi(av[vid_arg]), 
        // sm->t_uni_btree, 
        sm->t_btree,
         ss_m::t_load_file, kd, ss_m::t_cc_kvl, stid
         ) );
        DBG(<<"d1.set " << stid); 
        d1.set(stid);
        DO( sm->create_file(atoi(av[vid_arg]), fid, ss_m::t_load_file) );
        DBG(<<"d2.set " << fid); 
        d2.set(fid);
        vid = stid.vol;
    }
    int numnulls=0;
    {
        /* Create the records in the original file 
         * Records look like:
         * hdr:   kk (integer used to generate the key)
         *        bool (true->has no key at all)
         *        (if no key, the sort will not be deterministic where
         *        these records are concerned, so checking the file
         *        needs to know on a per-record basis if there is a key
         *        before it complains)
         * body:  <***arbitrary stuff*****>  
         *        key  (could be length 0)
         *        oid
         */
        int     i;    
        rid_t   rid;
        for (i = 0; i < n; i++)  {
            int kk = i - h;
            if(reverse) kk = 0 - kk;
			// convert kk to typed value in k, based on test-type t
            convert_to(kk, k, t, stringbuffer); // sort_funcs3.cpp

            bool hasnokey=false;

            if(t == test_bv || t == test_b23 || t == test_blarge) {
                w_assert3(k._u.bv == stringbuffer);
            }

            data.reset().put(zeroes);

            DBG(<< i << ": data contains zvec len " << data.size());
            if(nullsok && ((i & 0x1)== 0x1)) {
                // every other one becomes null key
                DBG(<<i << ": NULL key");
                hasnokey=true;
                numnulls++;
            } else if(t == test_bv || t == test_b23 || t == test_blarge) {
                // This doesn't work because "n" is < "nnn" but -1 is > -3.
                // So we use "n<a>" where <a> is the length
                {
                    w_ostrstream s;
                    s << "n" << k._length;
                    strcpy(stringbuffer, s.c_str());
                }

                DBG(<< i << ":... appending string for length " << k._length
                    <<":" <<  k._u.bv
                );
                data.put(k._u.bv, k._length);
            } else {
                DBG(<< i << ":... appending key of length " << k._length);
                data.put(&k._u, k._length);
            }

            vec_t hdr(&kk, sizeof(kk));
            hdr.put(&hasnokey, sizeof(hasnokey));

			DBG(<< i << ":... header contains " << kk
					<< " and hasnokey " << hasnokey);
			DBG(<< i << ":... body contains (before oid is added) " << data);


            {
                vec_t oid(&rid, sizeof(rid));
                DO( sm->create_rec( fid, 
                        hdr,
                        len_hint,
                        data,
                        rid) );
				DBG(<< i << ":... rid " << rid
						<< " hdr (" << kk << ", hasnokey " << hasnokey << ")"
						<< " body size (" 
						<< data.size() << "+" << oid.size() << ")"
						);
                if(TRACE_VERIFY) {
                    cout 
                    <<"created rec I: " << i << " : derived from " << kk 
                    <<" rid is " << rid 
                    <<" hasnokey " << hasnokey 
                    <<" size is " << data.size() 
                        << "+" << oid.size()
                        << endl;
                }
                /* APPEND OID */
                DO( sm->append_rec(rid, oid) );
            }
        }
        DO(count_records(fid, n));
    }

    {   // Sort the file, preparing a file of key/oid 
        // pairs for bulk-ld
        deleter d3;    // auto-delete
        stid_t  ofid;  // physical case

        DO( sm->create_file(vid, ofid, ss_m::t_load_file) );
        DBG(<<"d3.set " << fid);
        d3.set(ofid); // auto-delete

        {
            // This is the new sort API */
            DO( sm->sort_file(
                    fid,  // input file
                    ofid, // output file
                    1,    // #volume ids
                    &vid, // volume id
                    kl,   // key_location_t &
                    len_hint, // len_hint: more than a hint: min rec size
                    runsize,
                    runsize*ss_m::page_sz));
            DBG(<<"------------Sort file of fid=" << fid << " into output file " << ofid << " done");

            DO(count_records(ofid, n));

            if((t == test_bv) && !nullsok) {
                /* 
                 * check file won't work where
                 * keys have to be scrambled
                 */
                int ck = check_file_is_sorted(ofid, kl,  // in sort_funcs3.cpp
                        ((t == test_bv) && !nullsok) // true->do_compare
                        );
                if(ck) {
                    cerr << "check_file_is_sorted failed, reason=" << ck <<endl;
                    return TCL_ERROR;
                    DBG(<<"Check output file done");
                } else {
                    cerr << "check_file_is_sorted OK" <<endl;
                }
            } else {
                int ck = check_file_is_sorted2(ofid, kl, n, numnulls, t);
                w_assert0(ck==0);
            }
        }

        DBG(<<"{------------Bulk load into " << stid << " from " << ofid << " started ...");
        sm_du_stats_t stats;
        DO( sm->bulkld_index(stid, 1, &ofid, stats, false, false) );
        DBG(<<"------------Bulk load into " << stid << " from " << ofid << " DONE }");

        DO(count_nulls(stid, n, numnulls));

		/*
        DBG( << "PRINT INDEX" << stid << " AFTER BULK LOAD" );
        DO( sm->print_index(stid) );
        */

        /* Verify the index by looking at the records it points to an
         * comparing their contents with the key,value pairs
         * but not _scanning_ the file
         */
        verify_index(n, stid, nullsok, stringbuffer, t, reverse, h, zeroes, k);

        //
        // Now compare the index with the file via concurrent scans of both
        { 
                DBG(<<"--------------------------- compare_index_file  #" << n);
                w_rc_t rc = compare_index_file(stid, ofid, false, n);
                DBG(<<"rc=" << rc);
                DO(rc);
        }
        // the temp output file (d3) is deleted here
    }


    // ------------------------------------------------------------------------------
    {   // Re-sort the file, preparing a deep copy of the orig file 
        stid_t  ofid;  // physical case
        deleter d3;    // auto-delete

        DO( sm->create_file(vid, ofid, ss_m::t_load_file) );
        DBG(<<"d3.set " << fid);
        d3.set(ofid); // auto-delete

        if(kl.set_for_file(true/*deep*/, true/*keep*/, 
            false /*carry_obj*/)){ 
            DBG(<<"set_for_file deep, keep, carry_obj returns true"); 
        }
        // TODO: test with carry_obj (not implemented for large objects)
        // TODO: test set_object_marshal

        {
            // This is the new sort API
            DO( sm->sort_file(fid, 
                        ofid,
                        1, &vid,
                        kl,
                        len_hint,
                        runsize,
                        runsize*ss_m::page_sz));
        }
        DBG(<<"--------------Re-sort file " << fid << " into output file " << ofid << " done");

        { /* verify deep copy file */
            DBG(<<" verify deep copy ");
            w_rc_t rc = compare_index_file(stid, ofid, true, n);
            DBG(<<"rc=" << rc);
            DO(rc);

        }
        { /* test insert/remove/probe for nulls */
            // not implemented for logical
            DBG(<<"--------------- Delete index entries idx=" << stid);
            w_rc_t rc = delete_index_entries(stid, 
                fid, // orig file
                zeroes.size());
            DBG(<<"rc=" << rc);
            DO(rc);
        }
    }// the temp output file (d3) is deleted here
    DBG(<<"Leaving scope of d1, d2");
    // d1, d2 will delete the index and original file
    }
    DBG(<<"LEFT scope of d1, d2");
    return TCL_OK;
}
