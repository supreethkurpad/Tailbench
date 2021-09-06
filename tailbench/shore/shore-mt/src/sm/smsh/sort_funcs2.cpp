/*<std-header orig-src='shore'>

 $Id: sort_funcs2.cpp,v 1.25 2010/05/26 01:20:52 nhall Exp $

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

/* RTREE MULTI-KEY/NULL-KEY SORT TESTS */
int    duplicate_values[4] = { -10, 17, 10, 20 };
static const nbox_t duplicate_box(2, duplicate_values);
int     num_duplicate_box=0;

void stop() {
}


/*
 * Scan the file, deleting corresponding entries from
 * the rtree index.  Probe, Delete the key/elem pr, re-probe,
 * re-insert, re-probe, re-delete, re-probe.
 * This tests insert/remove of null entries, for one thing.
 * The file given should be the original file if it still
 * exists, so that we can avoid deleting in sorted order.
 */
w_rc_t
delete_rtree_entries(
    stid_t idx,
    stid_t fid,
    smsize_t keyoffset
)
{
    char    stringbuffer[MAXBV];
    scan_file_i  scanf(fid, ss_m::t_cc_file);

    bool    nullfound=false;
    bool     feof;
    w_rc_t    rc;
    pin_i*    pin;
    nbox_t     key;
    vec_t     elem;
    smsize_t     klen, elen;
    rid_t    rid;
    int     i=0;
    while ( !(rc=scanf.next(pin, 0, feof)).is_error() && !feof ) {
    i++;

    smsize_t ridoffset = pin->body_size() - sizeof(rid_t);
    klen = ridoffset - keyoffset;

    smsize_t offset = keyoffset;

    /* Get key from file record */
    while(pin->start_byte()+pin->length() <= offset){ 
        rc = pin->next_bytes(feof); 
        if(rc.is_error()) {
        DBG(<<"rc=" << rc);
        return RC_AUGMENT(rc);
        }
        w_assert3(!feof);
    }
    offset -=  pin->start_byte();
    // not handling logical case...
        smsize_t amt = pin->length() - offset;
        DBG(<<"offset=" <<offset << " amt=" << amt);
        memcpy(&stringbuffer, pin->body() + offset, amt);

    if(offset + klen > pin->length()) {
        rc = pin->next_bytes(feof); 
        if(rc.is_error()) {
        DBG(<<"rc=" << rc);
        return RC_AUGMENT(rc);
        }
        w_assert3(!feof);
        offset = 0;
        DBG(<<"offset=" <<offset << " amt=" << amt);
        memcpy(stringbuffer+amt, pin->body(), klen - amt);
    }

    key.nullify();
    key.bytes2box(stringbuffer, klen);

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

#ifdef W_TRACE
// _w_debug.setflags("sort_funcs2.cpp btree.cpp btree_impl.cpp btree_p.cpp");
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
    found = false;
        rc = sm->find_md_assoc(idx, key, el, elen, found);
        DBG(<<rc);
    W_DO(rc);

    elem.reset().put(el, elen);
    DBG(<<"found=" << found << " elem = " << elem);

    bool skip=false;
    if(!found) {
        if((key.klen() == 0) && nullfound) {
        // expected to find only one null
        skip = true;
        } else {
        cerr << "ERROR: Cannot find index entry for " << 
            key << " " << rid <<endl;
        stop();
            W_DO(rc);
        }
    }
    if(key.klen() == 0) {
             nullfound = true;
    }
    if(skip) {
        DBG(<<"SKIPPING");
        continue;
    }

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

    DBG(<< "DESTROY " << key << ","<<elem);
    rc = sm->destroy_md_assoc(idx, key, elem);
    DBG(<< rc);
    W_DO(rc);

    
#ifdef W_TRACE
    if(_w_debug.flag_on(("sort_funcs2.cpp"),__FILE__))  {
        cout << "PRINT INDEX AFTER REMOVAL of key,elem " 
        << key 
        << "," << elem
        << endl;
        W_DO( sm->print_md_index(idx) );
    }
#endif
    

    bool wrong_elem = false;

    DBG(<< "FIND AFTER DESTROYED " << key);
    elen = MAXBV;
        rc = sm->find_md_assoc(idx, key, el, elen, found);
    DBG(<< " after-delete find check returns " << rc
        << " and found=" << found);
    if(found) {
       // duplicate key - elems had better not match
        if(elen != sizeof(rid_t)) {
        cerr <<  __LINE__ << " ERROR: wrong elem length - expected " <<
            sizeof(rid_t) << " got " <<elen <<endl;
        }
        if(umemcmp(&rid, el, elen)) {
           wrong_elem = true;
           DBG(<<" found duplicate, but elems don't match:  OK");
        } else {
        cerr << "ERROR: found deleted key,elem pr: "
            << key << " " << rid <<endl;
        }
    }
    W_DO(rc);

    if(!wrong_elem) {
        // REINSERT
        elem.reset().put(&rid, sizeof(rid));
        DBG(<< "REINSERT " << key << " elem= " << elem << " rid=" << rid);
        W_DO( sm->create_md_assoc(idx, key, elem) );

#ifdef W_TRACE
// _w_debug.setflags("sort_funcs2.cpp");
#endif

        // Find it again- after re-insertion, it should be found
        DBG(<< "FIND AFTER REINSERT " << key);
        elen = MAXBV;
        W_DO( sm->find_md_assoc(idx, key, el, elen, found) );
        if(!found) {
        cerr << "ERROR: can't find inserted key,elem pr: "
            << key << " " << rid <<endl;
        }
        DBG(<< "RE-DESTROY " << key);
        W_DO( sm->destroy_md_assoc(idx, key, elem) );
        DBG(<< "FIND AFTER RE-DESTROY " << key);
        elen = MAXBV;
        W_DO( sm->find_md_assoc(idx, key, el, elen, found) );
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
    } else {
        DBG(<< "was wrong elem, so we cannot re-insert -- just go on");
    }
    // leave it out of the index
    
    DBG(<<"DONE WITH KEY " << key);
#ifdef W_TRACE
        // _w_debug.setflags("none");
#endif
    }
    {
    // Index had better be empty now
    scan_rt_i*    scanp = new scan_rt_i(idx,
                  nbox_t::t_overlap,
                  universe,
                  true /* nullsok */
                  );
    bool   eof;
    nbox_t k;
    char * e=0;
    smsize_t elen=0;
    for (i = 0; 
          (!(rc = scanp->next(k,e,elen,eof)).is_error() && !eof) ; i++) ;
    if(i > 0) {
        cerr << " ERROR: RTREE IS NOT EMPTY!  contains " << i 
            << " entries " << endl;
        w_assert3(0);
    }
    delete scanp;
    }
    if(rc.is_error()) {
    DBG(<<"rc=" << rc);
    return RC_AUGMENT(rc);
    }
    return RCOK;
}

w_rc_t
test_scanrt(
    int     n, // expected # items
    nbox_t::sob_cmp_t op,
    const nbox_t&	thekey,
    bool 	nullsok,
    stid_t&	stid,	
    char *	stringbuffer,
    vec_t&	zeroes,
    bool	verbose
)
{
    /* verify */
    scan_rt_i* scanp = 0;
    deleter    d4; // auto_delete for scan_index_i

    {
	scanp = new scan_rt_i(stid,
		      op,
		      thekey,
		      nullsok
		      );
    }
    DBG(<<"d4.set scanp");
    d4.set(scanp);

    DBG(<<"Starting rtree scan with operator " << W_ENUM(op)
	<< " nullsok=" << nullsok);
    w_rc_t 	rc;
    int 	i;
    pin_i	handle;
    rid_t	rid;
    nbox_t 	key;
    smsize_t    elen;
    char *    el = stringbuffer;
    bool     eof;
    {
    elen = sizeof(rid);
    }
    for (i = 0; (!(rc = scanp->next(key, el, elen, eof)).is_error() && !eof) ; i++)  {
    {
        w_assert1(elen == smsize_t(sizeof(rid_t)));
        memcpy(&rid, el, elen);
    }
    DBG(<<"object is " << rid );

    /*
     * Verify key 
     */
    int kk = 0;
    int klen = 0;
    // look at box, compute kk from box
    // assert that side is 10 for each dimension
    if(key.dimension() == 0) {
        // NULL
        DBG(<<"rtree scan found NULL key @i=" << i); 
        klen = 0;
    } else if(key == duplicate_box) {
        DBG(<<"rtree scan found DUPLICATE_BOX key @i=" << i); 
        klen = key.klen();
    } else {
        kk = key.bound(0);
        DBG(<<"rtree scan found key kk=" << kk << " @i=" << i); 
        w_assert3(key.side(1)==10);
        klen = key.klen();
    }

    /* 
     * Now verify that this is really the key of that object
     */
    smsize_t tail=0;
    {
        tail = sizeof(rid_t);
        W_DO(handle.pin(rid, zeroes.size()));
        DBG(<<"Pinning " << rid << " size=" << zeroes.size());
    }
    smsize_t offset = zeroes.size() - handle.start_byte();
    // offset is offset for KEY
    DBG(<<"key offset=" << offset);

    smsize_t amt = handle.length() - offset;
    // amt pinned
    DBG(<<"amt=" << amt);

    smsize_t l = klen; // total key length
    if(klen == 0) {
        // we'll end up copying oid into stringbuf, 
        // because for null keys, the oid gets spread across
        // the two pages
        l = tail;
    }
    DBG(<<"l=" << l);

    typed_value bodykey;
    if(klen == 0) {
        // copy oid into stringbuf.  Amt is amt 
        // left pinned from offset->length
        memcpy(stringbuffer, handle.body()+offset, amt);
    } else {
        // copy key into bodykey.  Amt is amt 
        // left pinned from offset->length
        memcpy(&bodykey._u, handle.body()+offset, amt);
        DBG(<<"copied " << amt << " from body+offset to bodykey._u");
    }
    l -= amt;
    bool eof;
    W_DO(handle.next_bytes(eof));
    w_assert1(!eof); // at least 2 pgs/object in this test
    
    if(klen != 0) {
        // We still have to copy part of key and the entire oid 
        w_assert1(handle.length() == l + tail);
    }
    if(klen == 0) {
        DBG(<<"memcpy amt=" << l << " from 2nd page of body ");
        memcpy(stringbuffer+amt, handle.body(), l);
    } else {
        DBG(<<"memcpy amt=" << l << " from 2nd page of body ");
        memcpy( ((char *)(&bodykey._u))+amt, handle.body(), l);
    }

    {
        rid_t    oid;
        if(klen == 0) {
        // oid is already in stringbuffer
        memcpy( &oid, stringbuffer, tail);
        } else {
        // oid starts at  handle.body() + l;
        memcpy( &oid, handle.body()+l, tail);
        }
        if(oid != rid) {
        DBG(<<"MISMATCH l = " << l << " tail=" << tail);
        { for(unsigned int q=0; q<tail; q++) {
            DBG(<<"handle.body() + " << l << " +" << q << "=" <<
            unsigned( *(unsigned char *)(handle.body()+l+q)));
        } }
        { 
           DBG(<<"handle.hdr_size()== " << handle.hdr_size());
           for(unsigned int q=0; q<handle.hdr_size(); q++) {
            DBG(<<"handle.hdr() + " << q << "=" <<
            unsigned( *(unsigned char *)(handle.hdr()+q)));
        } }
        cerr << "Mismatched OIDs " 
            << " index has " << rid
            << " body has " << oid 
            <<endl;
        stop();
        }
    }
    W_DO(handle.next_bytes(eof));
    w_assert1(eof); // no more than 2 pgs/object in this test

    if(klen == 0) {
        if(verbose) {
        {
            cerr << "NULL key, object id is " << rid << endl;
        }
        }
    } else if(klen>0) {
        int bodyk; 
        convert_back(bodykey, bodyk, test_spatial, stringbuffer);
        DBG(<<"object contains key " << bodyk); 

        if(bodyk == kk) {
        // OK
        if(verbose) {
            {
            cerr << "Matched key, object id is " << rid << endl;
            }
        }
        } else if(bodyk == duplicate_values[0]) {
        // duplicate value
        if(verbose) {
            {
            cerr << "Duplicate key, object id is " << rid << endl;
            }
        }
        } else  {
        cerr << "Mismatched key, object" <<endl;
        cerr << "...index key is " << kk <<endl; 
        {
            cerr << "...object id is " << rid << endl;
        }
        cerr << "...key in body is " << bodyk <<endl;
    stop();
        }
    }
    }
    if(verbose) {
    cout << "*******************************" <<endl;
    cout << " SCANNED " << i << " OK for operator " << W_ENUM(op)
        << " nullsok = " << nullsok
        <<endl;
    switch (op) {
    case nbox_t::t_exact:
        cout << " t_exact" <<endl;
        break;
    case nbox_t::t_overlap:
        cout << " t_overlap" <<endl;
        break;
    case nbox_t::t_cover:
        cout << " t_cover" <<endl;
        break;
    case nbox_t::t_inside:
        cout << " t_inside" <<endl;
        break;
    default:
    case nbox_t::t_bad:
        cout << " t_bad" <<endl;
        break;
    }
    
    cout << " KEY=" << thekey <<endl;
    }

    if(i != n) {
    cerr << "*** ERROR : expected scan to return " << n 
        << " but got " << i
        << endl;
    stop();
    }
    if(verbose) {
    cerr << "*******************************" <<endl;
    }

    DBG(<<"scan .next yields " << rc << " eof=" << eof);
    W_DO( rc );
    DBG(<<"scan is finished, n= " << n);
    // d4 will delete the scan ptr

    return RCOK;
}

/*
 * t_test_bulkload_rtree : tests spatial key types
 *                 
 * Does several things: 1) creates file of typed large records with key
 *     that crosses page boundary (unless key is of length 1)
 *      and contains its own OID at the end.
 * 2) sorts file, requesting index output
 * 3) uses that to bulk-load an r-tree index
 * 4) checks the index - scans it and compares the oid given with
 *     the orig object's key
 * 5) sorts the orig file, requesting new file output, preserve orig file
 * 6) scans index and new file, comparing keys and trailing OIDs
 */
int
t_test_bulkload_rtree(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "vid nkeys nullok|notnull", ac, 4))
    return TCL_ERROR;

    int nkeys_arg = 2;
    int vid_arg = 1;
    int null_arg = 3;

    /* XXX should this be reset by another function that is callable from
       the script level? */
    num_duplicate_box = 0;

    bool     nullsok=false;
    if(strcmp(av[null_arg], "nullok")==0) {
        nullsok = true;
    } else if(strcmp(av[null_arg], "notnull")==0) {
        nullsok = false;
    } else {
        cerr << "Bad argument #" << null_arg
        << " to test_bulkload_rtree: expected nullok | notnull " 
        << " got " << av[null_arg]
        <<endl;
        return TCL_ERROR;
    }

    int runsize = 3; // minimum
    // Let's puff up the objects a bit - make sure 
    // the key crosses a page boundary
    int data_sz = global_sm_config_info.lg_rec_page_space;
    int page_sz = global_sm_config_info.page_size;
    char *garbage = new char[data_sz-1];
    memset(garbage, '0', data_sz -1);
    w_auto_delete_array_t<char> delgarbage(garbage);
    vec_t    zeroes(garbage, data_sz-1);
    vec_t     data;

    int     n = atoi(av[nkeys_arg]);
    /* make the values range run from -n/2 to n/2 */
    const     int h = n/2;
    const bool     reverse = false; /* Put them into the file in reverse order */
    char    stringbuffer[MAXBV];
    typed_value k;
    vec_t     hdr; // keep it null

    typed_btree_test t = test_spatial;

    CSKF    cskfunc;
    generic_CSKF_cookie func_cookie;
    CF        cmpfunc = getcmpfunc(t,cskfunc, key_cookie_t(&func_cookie));

    func_cookie.offset = zeroes.size();
    k._length = func_cookie.length;
    smsize_t     len_hint = k._length + zeroes.size(); 

    sort_keys_t kl(1); // one key
    {
        // Set attibutes of sort as a whole
        int bad=0;
        // if(kl.set_keep_orig()) { DBG(<<""); bad++; }

        // For rtrees, unique *must * be false, as
        // there are no "unique rtrees".
        // Likewise, the hilbert values might not be unique.

        if(kl.set_unique(false)) { DBG(<<""); bad++; }
        if(kl.set_null_unique(false)) { DBG(<<""); bad++; }
        if(kl.set_ascending()) { DBG(<<""); bad++; }
        // no key cookie
        if(kl.set_for_index(cskfunc, key_cookie_t(&func_cookie))) 
            { DBG(<<""); bad++; }
        if(bad>0) {
            w_reset_strstream(tclout);
            tclout << smsh_err_name(ss_m::eBADARGUMENT) << ends;
            Tcl_AppendResult(ip, tclout.c_str(), 0);
            w_reset_strstream(tclout);
            return TCL_ERROR;
        }
        }

    // set up so it's a derived key from
    // the box, produces the output for rtree: <box,oid>

    if(nullsok) {
        kl.set_sortkey_derived(0, 
        onehilbert,
        (key_cookie_t)zeroes.size(),
        false, // not in hdr: in body
        true, // aligned
        false, // lexico
        cmpfunc
        );
    } else {
        kl.set_sortkey_fixed(0, 
        // offset, length:
        zeroes.size(), k._length, 
        false, // not in hdr: in body
        true, // aligned
        false, // lexico
        cmpfunc
        );
    }
    {
    deleter        d1; // auto-delete
    deleter        d2; // auto-delete

	stid_t 	    stid; // phys case
	stid_t 	    fid;  // phys case
	vid_t  	    vid;  // phys case
	{
	    DO( sm->create_md_index(
			atoi(av[vid_arg]), 
			sm->t_rtree,  // non-unique
			ss_m::t_load_file, 
			stid
			) );
	    DBG(<<"d1.set " << stid); 
	    d1.set(stid);
	    DO( sm->create_file(atoi(av[vid_arg]), fid, ss_m::t_load_file) );
	    DBG(<<"d2.set " << fid); 
	    d2.set(fid);
	    vid = stid.vol;
	}
	int 	numnulls = 0;
        universe.nullify();
    int    last=0;
    {
        /* Create the records in the original file */
        int        record_number=1;
        int     i;    
        rid_t       rid;
        for (i = 0; i < n; i++)  {
        int kk = i - h;

        if(reverse) kk = 0 - kk;

        // create a box instead
        int    values[4];
        values[0] = kk;
        values[1] = kk;
        values[2] = kk + last;
        values[3] = kk + 10;
        last = kk;
        nbox_t    box(2, values);
        // HACK: Make a few duplicate values:
        if(i == 2 || i == 4 || i == 6) {
            box = duplicate_box;
            num_duplicate_box++;
        }
        box.canonize();
        universe += box;

        data.reset().put(zeroes);
        DBG(<<"Data contains zvec of len " << data.size());

        if(nullsok && ((i & 0x1)== 0x1)) {
            // every other one becomes null key
            numnulls++;
            DBG(<<" (PLUS NULL KEY)NULL# " << numnulls);
#ifdef W_TRACE
            if(_w_debug.flag_on(("sort_funcs2.cpp"),__FILE__))  {
            cout << 
                record_number << 
                ": CLOBBERING KEY to make it NULL, numnulls=" 
            << numnulls << endl ;
            }
#endif
        } else {
            DBG(<<" PLUS key of length " << box.klen());
            data.put(box.kval(), box.klen());
        }

        // All boxes are of same length.  Override the
        // length given by getcmpfunc.
        func_cookie.length = box.klen();

#ifdef W_TRACE
            if(_w_debug.flag_on(("sort_funcs2.cpp"),__FILE__))  {

            cout << "KEY BOX = " << " hvalue is " <<
                box.hvalue(universe) <<endl;
            box.print(cout, 0/*level*/);
        }
#endif
        {
            vec_t oid(&rid, sizeof(rid));
            DO( sm->create_rec( fid, 
                    hdr,
                    len_hint,
                    data,
                    rid) );
            DBG(<<"created rec: value=" << kk 
            <<" oid is " << rid 
            <<" size is " << data.size() 
                << "+" << oid.size()
            );
            /* APPEND OID */
            DO( sm->append_rec(rid, oid) );
        }
        record_number++;
        }
    }

    {   // Sort the file, preparing a file of key/oid 
        // pairs for bulk-ld
        stid_t  ofid;  // physical case
        deleter d3;    // auto-delete

        {
        DO( sm->create_file(vid, ofid, ss_m::t_load_file
                ) );
        DBG(<<"d3.set " << fid);
        d3.set(ofid); // auto-delete
        }

        {
            // This is the new sort API
            DO( sm->sort_file( fid, ofid,
                1, &vid,
                kl,
                len_hint,
                runsize,
                runsize*page_sz));
        }
        DBG(<<"Sort file done");

#ifdef W_TRACE
        if(_w_debug.flag_on(("sort_funcs2.cpp"),__FILE__) ) {
        // Scan ofid and dump contents
        w_rc_t rc;
        if(0) {
        cout << "SCAN OF SORT's INPUT  FILE" <<endl;
        scan_file_i *scan = new scan_file_i(fid);
        rc = dump_scan(*scan, cout, NULL, true);
        if(rc.is_error()) cout << "returns rc=" << rc <<endl;
        delete scan;
        }
        if(1) {
        cout << "SCAN OF SORT's OUTPUT FILE" <<endl;
        scan_file_i *scan = new scan_file_i(ofid);
        rc = dump_scan(*scan, cout, NULL, true);
        if(rc.is_error()) cout << "returns rc=" << rc <<endl;
        delete scan;
        }
        }
#endif

        sm_du_stats_t stats;
        {
        w_rc_t rc =  sm->bulkld_md_index(stid, 1, &ofid, stats);
        if(rc.is_error()) {
            if(rc.err_num() == ss_m::eDUPLICATE) {
            cerr << "BULK LOAD DONE: unique entries== "
            << stats.rtree.unique_cnt
            << " total entries== " 
            << stats.rtree.entry_cnt
            <<endl;
            }
            DO(rc);
        }
        if(linked.verbose_flag) {
            cout << "BULK LOAD DONE: unique entries== "
            << stats.rtree.unique_cnt 
            << " total entries== " 
            << stats.rtree.entry_cnt
            <<endl;
        }
        }
        DBG(<<"Bulk load done");

#ifdef W_TRACE
        if(_w_debug.flag_on(("sort_funcs2.cpp"),__FILE__))  {
        cout << "PRINT RTREE AFTER BULK LOAD" << endl;
        DO( sm->print_md_index(stid) );
        }
#endif

        // the temp output file (d3) is deleted here
    }

    /*
     * Do a bunch of scans to check the tree
     */
    // scan  == duplicate_box
    // should return "num_duplicate_box" in both cases.
    DO(test_scanrt(
        num_duplicate_box, nbox_t::t_exact, duplicate_box, false,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));
    DO(test_scanrt(
        num_duplicate_box, nbox_t::t_exact, duplicate_box, true,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));

    // scan  == Null
    // should return #nulls or 0, depending on if nulls allowed
    DO(test_scanrt(
        numnulls, nbox_t::t_exact, nbox_t::Null, true,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));
    DO(test_scanrt(
        0, nbox_t::t_exact, nbox_t::Null, false,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));

    // scan: universe overlaps
    // univers overlaps everything - should return all
    // or all-numnulls
    DO(test_scanrt(
        n, nbox_t::t_overlap, universe, true,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));
    DO(test_scanrt(
        (n-numnulls), nbox_t::t_overlap, universe, false,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));

    // scan: Null covers nothing except Null
    // so should return numnulls or 0
    DO(test_scanrt(
        numnulls, nbox_t::t_cover, nbox_t::Null, true,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));
    DO(test_scanrt(
        0, nbox_t::t_cover, nbox_t::Null, false,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));

    // scan: universe covers all
    // should return all or all-numnulls
    DO(test_scanrt(
        (n-numnulls), nbox_t::t_cover, universe, false,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));
    DO(test_scanrt(
        n, nbox_t::t_cover, universe, true,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));

    // scan: Null inside
    // Null is inside everything, including Null
    DO(test_scanrt(
        n, nbox_t::t_inside, nbox_t::Null, true,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));

    // This is rather flaky: if nulls aren't allowed
    // in the scan, then Null can't be compared with
    // anything?? seems the return-nulls should apply
    // ONLY to results, not to keys
    DO(test_scanrt(
        (n-numnulls), nbox_t::t_inside, nbox_t::Null, false,
        stid, 
        stringbuffer, zeroes,
        linked.verbose_flag));



    { /* test insert/remove/probe for nulls */
        // not implemented for logical
        w_rc_t rc = delete_rtree_entries(stid, 
            fid, // orig file
            zeroes.size());
        DBG(<<"rc=" << rc);
        DO(rc);

    }// the temp output file (d3) is deleted here
    DBG(<<"Leaving scope of d1, d2");
    // d1, d2 will delete the index and original file
    }
    DBG(<<"LEFT scope of d1, d2");
    return TCL_OK;
}
