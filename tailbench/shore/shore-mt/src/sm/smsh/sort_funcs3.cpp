/*<std-header orig-src='shore'>

 $Id: sort_funcs3.cpp,v 1.19 2010/05/26 01:20:52 nhall Exp $

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

#include <new>

//NB: ONLY works with true now. Sorry...
// the problem has to do with the way spatial
// is handled the old way vs the new way in smsh 
bool newsort = true ;

const int lower_alpha = 0x61;
const int upper_alpha = 0x7a;


#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_array_t<const char *>;
#endif

nbox_t universe(2); // in sort_funcs3.cpp

w_rc_t 
get_key_info(
    const rid_t&   W_IFDEBUG3(W_IFTRACE(rid)),  // record id
    const object_t&        obj_in,
    key_cookie_t            cookie,  // type info
    factory_t&                ,
    skey_t*                key
)
{
    int                        k = cookie.make_int();
    DBG(<<"get_key_info for key " << k);
    metadata*                meta = (metadata *)obj_in.hdr(0);

    // we shouldn't be called if this is the case
    w_assert1(meta[k].nullable || !meta[k].fixed); 

    DBG(<<"get_key_info for keys " << k << " offset=" 
            << meta[k].offset << " length=" << meta[k].length );

    new(key) skey_t(obj_in, meta[k].offset, meta[k].length, false);
#if W_DEBUG_LEVEL > 2
    if(1) 
    {
#undef DBG
#define DBG(x) cout x << endl;
        DBG(
        << "KEY " << k << " METADATA ARE: " 
        << " offset=" << meta[k].offset
        << " length=" << meta[k].length
        );

        DBG( << rid << " body (" 
            << (obj_in.body_size() - meta[k].offset) 
            << " bytes worth) = " );
        char *object = (char *)obj_in.body(meta[k].offset);

        if(meta[k].length > 0) {
        switch(meta[k].t) {
        case test_bv:
        case test_blarge:
        case test_b23:
        case test_b1: {
                    int  l = meta[k].length;
                    char *p = object;
                    if(l > 1) {
                        if(strlen(p) > 256) {
                            while (*p == 'E') p++; 
                        }
                        if(p-object > 0) {
                            DBG(<<" E(" <<(int)(p-object) <<" times)" << p);
                        } else {
                            while(*p) {
                                w_assert3(*p <= upper_alpha && *p >= lower_alpha);
                                p++;
                            }
                            DBG( << object);
                        }
                    } else {
                        // print as character
                        w_assert3(*p <= upper_alpha && *p >= lower_alpha);
                        DBG( << *object);
                    }
                } 
                break;
        case test_i1: {
                w_base_t::int1_t i;
                memcpy(&i, object, 1);
                DBG( << int(i));
                }; break;
        case test_u1: {
                w_base_t::uint1_t i;
                memcpy(&i, object, 1);
                DBG( << unsigned(i));
                }; break;
        case test_i2: {
                w_base_t::int2_t i;
                memcpy(&i, object, 2);
                DBG( << i);
                }; break;
        case test_u2: {
                w_base_t::uint2_t i;
                memcpy(&i, object, 2);
                DBG( << i);
                }; break;
        case test_i4: {
                w_base_t::int4_t i;
                memcpy(&i, object, 4);
                DBG( << i);
                }; break;
        case test_u4: {
                w_base_t::uint4_t i;
                memcpy(&i, object, 4);
                DBG( << i);
                }; break;
        case test_i8: {
                w_base_t::int8_t i;
                memcpy(&i, object, 8);
                DBG( << i);
                }; break;
        case test_u8: {
                w_base_t::uint8_t i;
                memcpy(&i, object, 8);
                DBG( << i);
                }; break;
        case test_f4: {
                w_base_t::f4_t i;
                memcpy(&i, object, 4);
                DBG( << i);
                }; break;
        case test_f8: {
                w_base_t::f8_t i;
                memcpy(&i, object, 8);
                DBG( << i);
                }; break;
        default:
                w_assert1(0);
                break;
        }
        }
#undef DBG
#define DBG(x) DBG1(x)
    }
#endif
    return RCOK;
}

w_rc_t 
hilbert(
    const rid_t&        W_IFTRACE(rid),        // record id
    const object_t&        obj_in,
    key_cookie_t            cookie,  // type info
    factory_t&                internal,
    skey_t*                key
) 
{
    metadata*                meta = (metadata *)(cookie.make_ptr());
    smsize_t                 length = meta->length;
    smsize_t                 off = meta->offset;

    if(obj_in.body_size() != obj_in.contig_body_size()) {
            /* large-object special case for derive function of rtree test */
            length = obj_in.body_size() - off - sizeof(rid_t);
    }
    smsize_t resultlen = 0;

    int hvalue=0;

    if(length) {
            nbox_t box(2);
            if(length < smsize_t(box.klen()) ) {
                    return RC(ss_m::eBADLENGTH);
            }
            length = smsize_t(box.klen());

            // The data form a box. Materialize it.
            DBG(<<"get box at offset " << off << " with length " << length);

            char *tmp = new char[length];
            if(!tmp) return RC(ss_m::eOUTOFMEMORY);
            vec_t bx(tmp,length);
            rc_t rc = obj_in.copy_out(false/* not in hdr */, off, length, bx);
            if(rc.is_error()) {
                    delete[] tmp;
                    DBG(<<"got error " << rc);
                    return rc;
            }

            box.bytes2box(tmp, length);
            delete[] tmp;

            hvalue = box.hvalue(universe);
            DBG(<<"hilbert: hvalue for rid " << rid << " == " << hvalue
                            << " based on box " << box
                            << " and universe " << universe
               );

            /* allocate space for the result */
            void *c = internal.allocfunc(resultlen);
            memcpy(c, &hvalue, sizeof(hvalue));

            new(key) skey_t(c, 0, sizeof(hvalue), internal);
    } else {
            new(key) skey_t(0, 0, 0, *factory_t::none);
    }

    return RCOK;
}

/*
 * mhilbert: cookie is smsize_t offset into body of the box.
 * box is never in header.  test metadata are in header.
 */

w_rc_t 
mhilbert (
        const rid_t&        rid,  // record id
        const object_t&     obj_in,
        key_cookie_t        cookie,  // type info
        factory_t&          internal,
        skey_t*             key
) 
{
    metadata*                _meta = (metadata*)obj_in.hdr(0);;
    metadata*                meta = &_meta[cookie.make_int()];

    key_cookie_t  arg(meta); // explicit type conversion

    return hilbert(rid, obj_in, arg, internal, key); 
}



w_rc_t 
lonehilbert (
    const rid_t&       ,  // record id
    const object_t&        obj_in,
    key_cookie_t            cookie,  // type info
    factory_t&                internal,
    skey_t*                key
) 
{
    rid_t dummy;
    return onehilbert(dummy, obj_in, cookie, internal, key);
}

w_rc_t 
onehilbert (
    const rid_t&        rid,  // record id
    const object_t&     obj_in,
    key_cookie_t        cookie,  // type info
    factory_t&          internal,
    skey_t*             key
) 
{
    metadata                meta;
    meta.offset = cookie.make_smsize_t();
    meta.length = sizeof(nbox_t);

    key_cookie_t  arg(&meta); // explicit conversion

    return hilbert(rid, obj_in, arg, internal, key);
}


void
make_random_box(char *object, w_base_t::uint4_t W_IFDEBUG1(length))
{
    rand48&  generator(get_generator());
    int array[2 * 2];
    // We only test 2-d boxed
    w_assert1(length == sizeof(array));
    for(int i=0; i<4; i++) {
        int x = w_base_t::int4_t(generator.rand());
        if(x < 0) x = 0 - x;
        x = x % 10000; // keep universe 0->10000
        array[i] = x;
    }
    nbox_t box(2);
    box.bytes2box( (const char *)&array[0], int(sizeof(array)));
    box.canonize();
    universe += box;
    memcpy(object, box.kval(), box.klen());
    DBG(<<"make_random_box returns " << box 
        << "; hvalue=" << box.hvalue(universe)
        << "; universe=" << universe
    );
}


/*
 * NB: caller MUST provide enough room for null terminator
 * at the end.
 * Strings of length 1 are not null-terminated.
 */ 
void
make_random_alpha(char *object, w_base_t::uint4_t length)
{
    FUNC(make_random_alpha);
    const char *o = object;
    // pseudo-randomly choose characters between lower_alpha and upper_alpha, inclusive
    rand48&  generator(get_generator());
    w_base_t::uint1_t j,n; 
    for (unsigned int i=0; i<length; i++) {
        j = 0;
        while(j < lower_alpha || j > upper_alpha) {
            n = w_base_t::uint1_t(generator.rand());
            j = n & 0x7f;
        }
        if((j >= lower_alpha) && (j <= upper_alpha)) {
            *object++ = j;
        } else {
            *object++ = 'x';
        }
    }
    // overwrite last char with 0 for printing purposes, unless
    // length = 1
    if(length > 1) {
        object = const_cast<char *>(o);
        *(object+length-1) = '\0';         
    }
#if W_DEBUG_LEVEL > 2
    if(length == 1) {
        DBG(<<"make_random_alpha " << length << " produces " << *o );
        w_assert3(*o <= upper_alpha && *o >= lower_alpha);
    } else {
        DBG(<<"make_random_alpha " << length << " produces " << o );
        {   const char *p = o;
            while (*p) {
                w_assert3(*p <= upper_alpha && *p >= lower_alpha);
                p++;
            }
        }
    }
#endif 
}

/*
 Scan file and verify that it's sorted --
 this works only for the "index" case 
 and where all keys are fixed and aligned 
 Nulls should work here too

Update: now that we've added the int value (from which the
key is derived) in the record header, we
can use that, too... because that's in a fixed place.
That will be in check_file_is_sorted2, below.
*/
int
check_file_is_sorted( stid_t  fid, sort_keys_t&kl, bool do_compare)
{
#define STRINGMAX ss_m::page_sz
    pin_i*        pin;
    bool          eof;
    rc_t          rc;
    scan_file_i*  scan;

    int nkeys = kl.nkeys();
    int k;

    {
        /* 
         * can do check if all but possibly last key is fixed
         * and last key is fixed in location and/or length
         * (Unfortunately, we have no way to determine that)
         * OR
         * it's for_index thus there is  only one key 
         */
        bool bad = false;
        int non_fixed =0;
        if((kl.lexify_index_key()!=sort_keys_t::noCSKF)  && !kl.is_for_index())   {
            bad = true;
        } else if(!kl.is_for_index()) for(k=0; k<nkeys; k++) {

            if(kl.is_fixed(k) && kl.is_aligned(k)) continue; // ok
            non_fixed++;
            if( k != nkeys-1) {
                // no hope
                bad = true;
                break;
            }
        }
        if(bad) {
            return -1;
        }
    }

    scan = new scan_file_i(fid, ss_m::t_cc_file);

    char *strbuf = new char[STRINGMAX];
    w_auto_delete_array_t<char> autodel(strbuf);

    int   *oldlengths = new int[nkeys];
    typedef const char * charp;
    charp  *oldkeys = new charp[nkeys];
    w_auto_delete_array_t<int> autodel2(oldlengths);
    w_auto_delete_array_t<const char *> autodel3(oldkeys);
    charp *keys = new charp[nkeys];
    int  *lengths = new int[nkeys];
    w_auto_delete_array_t<int> autodel4(lengths);
    w_auto_delete_array_t<const char *> autodel5(keys);

    bool first = true;

    while ( !(rc=scan->next(pin, 0, eof)).is_error() && !eof) {
        DBG(<<" scan record " << pin->rid());

        /* 
         * Locate key(s) in new object
         */
        int myoffset= 0;
        for(k=0; k<nkeys; k++) {
            if(kl.in_hdr(k) || kl.is_for_index()) {
                keys[k] = pin->hdr() + myoffset;
                DBG(<<"key is " << keys[k]);
            } else if (pin->is_small()) {
                keys[k] = pin->body() + kl.offset(k);
                DBG(<<"key is " << keys[k]);
            } else {
                //TODO: can't handle this
                cerr << "can't check large objects yet "<<endl;
                delete scan;
                return 1;
            }
            if ( kl.is_fixed(k) ) {
                DBG(<<"");
                lengths[k] = kl.length(k);
            } else {
                if(kl.in_hdr(k) || kl.is_for_index()) {
                    lengths[k] = pin->hdr_size() ;
                    for(int q=k-1; q>=0; q--) {
                        lengths[k] -= lengths[q];
                    }
                    DBG( << "k=" <<k
                        <<" lengths[k]=" << lengths[k]
                        << " hdr size " << pin->hdr_size()
                        );
                } else {
                    lengths[k] = pin->body_size()-kl.offset(k) ;
                    DBG( << "k=" <<k
                        <<" lengths[k]=" << lengths[k]
                        << " body size " << pin->body_size()
                        );
                }
                if(kl.is_fixed(k) && lengths[k]!=0) {
                    if(lengths[k] < int(kl.length(k))) {
                        DBG(<<" Actual key length too short "
                                << " expected " << int(kl.length(k))
                                << " found " << lengths[k]);
                        w_assert3(0);
                    }
                    lengths[k] = kl.length(k);
                }
            }
            myoffset += lengths[k];
        }

        if(do_compare && !first) {
            int j=0;
            /* compare with last one read */
            /* compare most significant key first */
            /* if uniq cannot be == */
            for(k=0; k<nkeys; k++) {
                if(oldlengths[k] == 0 || lengths[k] == 0) {
                     j = oldlengths[k] - lengths[k];
                } else { 
                    DBG(<<" keycmp oldlen=" << 
                        oldlengths[k] << " newlen= " << lengths[k]);
                    j= (*kl.keycmp(k))(
                        oldlengths[k], oldkeys[k],
                        lengths[k], keys[k]
                        );
                    DBG(<<" keycmp " << " old record with " << pin->rid() 
                        << " yields " << j);
                    // j < 0 iff old < new
                    if(j==0) {
                        continue; // to next key
                    } else {
                        break; // stop here
                    }
                }
            }
            if(kl.is_unique()) {
                w_assert3(j != 0);
                if(j==0) {
                    delete scan;
                    return 2;
                }
            }
            if(kl.is_ascending()) {
                w_assert3(j <= 0);
                if(j>0)  {
                    delete scan;
                    return 3;
                }
            } else {
                w_assert3(j >= 0);
                if(j<0) {
                    delete scan;
                    return 4;
                }
            }
        }

        /* copy out key */
        memset(strbuf, 'Z', STRINGMAX);
        {
            char *dest = strbuf;
            for(k=0; k<nkeys; k++) {
                w_assert1(dest - strbuf < STRINGMAX);
                oldkeys[k] = dest;
                oldlengths[k] = lengths[k];
                memcpy(dest, keys[k], lengths[k]);
                dest += lengths[k];
                DBG(<<"Copied key : length[k]="  << oldlengths[k]);
            }
            if(!do_compare) {
                // TODO: print key, oid instead
            }
        }
        first = false;

    } //scan

    delete scan;
    return 0;
}
// See comments above the above function
// return value 0: ok
// return value > 0: some kind of error
// This checks an output file of sort (whose objects contain rids of 
// other objects), for sortedness, based on the target objects having
// their sort key in their headers, in the form of an integer value.
int
check_file_is_sorted2(stid_t  fid, 
        sort_keys_t&kl, 
        int W_IFTRACE(n), 
        int numnulls,
        typed_btree_test t )
{
    bool use_unsigned(false);
    switch(t)
    {
        case test_blarge:
        case test_b23:
        case test_bv:
        case test_b1:
        case test_u1:
        case test_u2:
        case test_u4:
        case test_u8:
            use_unsigned = true;
            break;
        default:
            break;
    };
#define STRINGMAX ss_m::page_sz
    pin_i*        pin;
    bool          eof;
    rc_t          rc;
    scan_file_i*  scan;

    DBG(<<"check_file_is_sorted2 " << fid 
            << " n " << n
            << " numnulls " << numnulls);

    scan = new scan_file_i(fid, ss_m::t_cc_file);

    int j=0;
    int priorkey=0;
    numnulls++;
    while ( !(rc=scan->next(pin, 0, eof)).is_error() )  {
        if(eof) break;

        j++;
        DBG(<< j << "--- scan record " << j << " rid="  << pin->rid());

        /*
         * Locate rid stored in body
         */
        rid_t  orid;
        memcpy(&orid, pin->body(), sizeof(orid)); 
        DBG(<<"      pin->body for " << pin->rid() << " contains " << orid);

        pin_i handle;
        W_IGNORE(handle.pin(orid, 0));

        /* 
         * Locate keysource in header (this works only for single-key
         * tests and only for those that put the key in the body and
         * put extra info, namely the key's source value in the header.
         */
        int thiskey;
        memcpy(&thiskey, handle.hdr(), sizeof(thiskey)); 
        bool hasnokey;
        memcpy(&hasnokey, handle.hdr()+sizeof(thiskey), sizeof(hasnokey)); 

        DBG(<<"      hdr for " << handle.rid() 
                << " contains value (" << thiskey
                << "," << int(hasnokey) << ")"
                << " j " << j
                << " numnulls " << numnulls
                );

        if(j > numnulls) {
            /* compare with last one read */

            if(kl.is_ascending()) {
                if(priorkey > thiskey)  {
                    bool bad(true);
                    if(use_unsigned) {
                        if(unsigned(priorkey) <= unsigned(thiskey))  {
                            bad = false;
                        }
                    }
                    if(bad) {
                        if(use_unsigned) {
                            DBG(<<"      ascending but priorkey=" << unsigned(priorkey)
                            << " thiskey=" << unsigned(thiskey)
                            << " use_unsigned " << use_unsigned
                            );
                        } else {
                            DBG(<<"      ascending but priorkey=" << priorkey
                            << " thiskey=" << thiskey
                            << " use_unsigned " << use_unsigned
                            );
                        }
                        delete scan;
                        return 3;
                    }
                }
            } else {
                if(priorkey < thiskey)  {
                    bool bad(true);
                    if(use_unsigned) {
                        if(unsigned(priorkey) >= unsigned(thiskey))  {
                            bad = false;
                        }
                    }
                    if(bad) {
                        if(use_unsigned) {
                            DBG(<<"      not ascending but priorkey=" << unsigned(priorkey)
                            << " thiskey=" << unsigned(thiskey)
                            << " use_unsigned " << use_unsigned
                            );
                        } else {
                            DBG(<<"      not ascending but priorkey=" << priorkey
                            << " thiskey=" << thiskey
                            << " use_unsigned " << use_unsigned
                            );
                        }
                        delete scan;
                        return 4;
                    }
                }
            }
        } 
        priorkey = thiskey;

    }
    w_assert1(eof);

    delete scan;
    DBG(<<"---------------check_file_is_sorted2 " << fid << " OK , found " << j);
    return 0;
}


void 
deleter::disarm() 
{
    fid = stid_t::null; 
    scanp=0;
    scanr=0;
}

deleter::~deleter() 
{
    if(fid != stid_t::null) {
        DBG(<<"Destroying file " << fid);
        w_rc_t rc = sm->destroy_file(fid);
        if(rc.is_error() && rc.err_num() == ss_m::eBADSTORETYPE) {
            DBG(<<"Try again: destroying index " << fid);
            rc = sm->destroy_index(fid);
        }
        if(rc.is_error() && rc.err_num() == ss_m::eBADNDXTYPE) {
            DBG(<<"Try again: destroying rtree " << fid);
            rc = sm->destroy_md_index(fid);
        }
        if(rc.is_error()) cerr << "Error trying to destroy file " << rc <<endl;
    }
    if(scanp) {
        DBG(<<"Deleting scan p");
        delete scanp;
    }
    if(scanr) {
        DBG(<<"Deleting scan r");
        delete scanr;
    }
}


/*
 * Test a variety of multi-key sort cases.  We don't
 * guarantee that we get all cases
 * 
 * For each key, specifiy these characteristics 
 * (NOTES: in this simplistic test,
 *  -specifying un-fixed for one key implies unfixed for the succeeding keys
 *  -specifying key type might imply some of the below, e.g., spatial implies
 *        derived
 * )
 *        F (key has fixed len,loc)
 *        V (variable loc or length, implies nothing about nullable or derived)
 *        D (derived key)
 *        N (nullable key, fixed len, loc)
 *        A (is aligned - implies nothing about fixed)
 *         Cannot have V and F.  V is optional, implied if F is not present.
 *        All these are optional, but at least one letter is required to
 *        keep the place of the Tcl, argument so the minimum is either V or F.
 *
 *  Output can be file|index.  If index, what you get is hdr==key,
 *        elem==OID, and the input file is not destroyed.  Keys must
 *        be short, in this case.  It is up to the caller to construct
 *      the key type for the btrees, and any variable-length key
 *         must be last in that case. 
 */

class boxfactory : public factory_t 
{
public:
   void freefunc(const void *p, smsize_t W_IFDEBUG1(s)) {
        nbox_t *b = (nbox_t *)p;
         
        // w_assert1(s == sizeof(nbox_t));
        // No more - it's box.klen()
        w_assert1(s == smsize_t(b->klen()));
        
        delete [] b;
   }
   void* allocfunc(smsize_t number) {
        w_assert1(number == 1); // for now
        return (void *)new nbox_t[number];
   }
   smsize_t  sz() const { return sizeof(nbox_t); }
};

boxfactory box_factory;

w_rc_t 
originalboxCSKF(
    const rid_t&        
#ifdef W_TRACE
    rid
#endif
    ,  // record id
    const object_t&        obj_in,
    key_cookie_t            cookie,  // type info
    factory_t&                ,
    skey_t*                out
)
{
    generic_CSKF_cookie&        K = *(generic_CSKF_cookie*)(cookie.make_ptr());

    smsize_t length = K.length; // 
                        // K.length is sizeof(int) as returned by getcmpfunc,
                        // overridden in sort_funcs2.cpp with box.klen()
    smsize_t offset = K.offset;

    if(offset + length + sizeof(rid_t) > obj_in.body_size()) {
            // null box 
        length = 0;
    }

    if(length>0) {
        void *c = box_factory.allocfunc(1);
        nbox_t* box =  (nbox_t *)c;

        if(obj_in.body_size() == obj_in.contig_body_size()) {
            // small
            box->bytes2box((char *)obj_in.body(offset), length);
        } else {
            // large object - could span pages
            // Grot - extra copies
            char *tmp = new char[length];
            if(!tmp) return RC(ss_m::eOUTOFMEMORY);
            vec_t v(tmp, length);
            w_rc_t rc = obj_in.copy_out(false, offset, length, v);

            if(rc.is_error()) {
                delete[] tmp;
                return rc;
            }
            box->bytes2box(tmp, length);
            delete[] tmp;
        }
        DBG(<<" original box for record " << rid << " is " << *box);
        // NB: bulk-load for rtrees doesn't want the whole
        // box structure, but just the byte string.
        // old:
        // out = new(out) skey_t(c, 0, box_factory.sz(), box_factory);
        // new:
        const void *b = box->kval();
        smsize_t        off = smsize_t((const char *)b - (const char *)c);
        new(out) skey_t(c, off, box->klen(), box_factory);
    } else {
        new(out) skey_t(0, 0, 0, *factory_t::none);

    }
    return RCOK;
}

// marshall used for test_bv and  test_blarge:
w_rc_t 
vblstringCSKF(
    const rid_t&        ,  // record id
    const object_t&     obj_in,
    key_cookie_t        cookie,  // type info
    factory_t&          ,
    skey_t*             out
)
{
    generic_CSKF_cookie&        K = *(generic_CSKF_cookie*)(cookie.make_ptr());

    smsize_t length = 0; // K.length is irrelevant.
                        // We figure length is 
                        // object size - offset - sizeof(rid_t)
    smsize_t offset = K.offset;
    length = obj_in.body_size() - offset - sizeof(rid_t);
    new(out) skey_t(obj_in, offset, length, false);
    return RCOK;
}

CF 
getcmpfunc(typed_btree_test t, 
        CSKF&  get_sort_key_func,
        key_cookie_t lfunc_cookie
) 
{
    generic_CSKF_cookie *g = (generic_CSKF_cookie *) (lfunc_cookie.make_ptr());
    get_sort_key_func  = sort_keys_t::generic_CSKF;
    // g->func is set here
    // g->length is set here
    // g->offset is set by caller.

    switch(t) {
    case test_bv:
    case test_blarge:
        g->func = sort_keys_t::noLEXFUNC; // not needed
        g->length= int(MAXBV); // max
        get_sort_key_func  = vblstringCSKF;
        return sort_keys_t::string_cmp;

    case test_b23:
        g->func = sort_keys_t::noLEXFUNC; // not needed
        g->length= 23;
        return sort_keys_t::string_cmp;

    case test_b1:
        g->func = sort_keys_t::noLEXFUNC; // not needed
        g->length = sizeof(w_base_t::uint1_t);
        return sort_keys_t::string_cmp;

    case test_u1:
        g->func = sort_keys_t::u1_lex;
        g->length = sizeof(w_base_t::uint1_t);
        return  sort_keys_t::uint1_cmp;

    case test_i1: 
        g->func = sort_keys_t::i1_lex;
        g->length = sizeof(w_base_t::int1_t);
        return sort_keys_t::int1_cmp;

    case test_u2:
        g->func = sort_keys_t::u2_lex;
        g->length = sizeof(w_base_t::uint2_t);
        w_assert1(alignon(g->offset,sizeof(w_base_t::uint2_t)) == g->offset);
        return sort_keys_t::uint2_cmp;

    case test_i2: 
        g->func = sort_keys_t::i2_lex;
        g->length = sizeof(w_base_t::int2_t);
        w_assert1(alignon(g->offset,sizeof(w_base_t::int2_t)) == g->offset);
        return sort_keys_t::int2_cmp;

    case test_u4:
        g->func = sort_keys_t::u4_lex;
        g->length = sizeof(w_base_t::uint4_t);
        w_assert1(alignon(g->offset,sizeof(w_base_t::uint4_t)) == g->offset);
        return sort_keys_t::uint4_cmp; 

    case test_spatial: 
        get_sort_key_func  = originalboxCSKF;
        g->func = sort_keys_t::i4_lex;
        g->length = sizeof(w_base_t::int4_t);
        return sort_keys_t::int4_cmp;

    case test_i4: 
        g->func = sort_keys_t::i4_lex;
        g->length = sizeof(w_base_t::int4_t);
        w_assert1(alignon(g->offset,sizeof(w_base_t::int4_t)) == g->offset);
        return sort_keys_t::int4_cmp;

    case test_u8:
        g->func = sort_keys_t::u8_lex;
        g->length = sizeof(w_base_t::uint8_t);
        w_assert1(alignon(g->offset,sizeof(w_base_t::uint8_t)) == g->offset);
        return sort_keys_t::uint8_cmp;

    case test_i8: 
        g->func = sort_keys_t::i8_lex;
        g->length = sizeof(w_base_t::int8_t);
        w_assert1(alignon(g->offset,sizeof(w_base_t::int8_t)) == g->offset);
        return sort_keys_t::int8_cmp;

    case test_f4:
        g->func = sort_keys_t::f4_lex;
        g->length = sizeof(w_base_t::f4_t);
        w_assert1(alignon(g->offset,sizeof(w_base_t::f4_t)) == g->offset);
        return sort_keys_t::f4_cmp;

    case test_f8:
        g->func = sort_keys_t::f8_lex;
        g->length = sizeof(w_base_t::f8_t);
        w_assert1(alignon(g->offset,sizeof(w_base_t::f8_t)) == g->offset);
        return sort_keys_t::f8_cmp;

    default:
        cerr << "switch -- test type not supported : " << (unsigned) t <<endl;
        w_assert3(0);
    } /* switch */
    w_assert1(0);
    return 0;
}

/* XXX the convert functions are sorta hokey, for the string buffer
   is assumed to go with the typed value, and is const in the
   typed_value, but can be modified as the stringbuffer.   See notes
   in shell.h that perhaps the typed_value should be expanded to
   own the stringbuffer associated with it. */

void 
convert_to(int kk, typed_value &k, typed_btree_test t, char *stringbuffer ) 
{
	DBG(<<"convert_to("<<kk<<") t " << t);
    switch(t) {
    case test_bv:
    case test_blarge:
    case test_b23: {
        k._u.bv = stringbuffer;
        w_assert1(kk < MAXBV-1);
        int len = kk; 
        char c = 'p';
        if(kk < 0) {
            len = 0-kk;
            w_assert1(len < MAXBV-1);
            c = 'n';
        }
        if(len == 0) {
            len++;
            c = 'o';
        }
        memset(stringbuffer, c, len);
        stringbuffer[len] = '\0';
        k._length = len+1;
        }
        break;
    case test_b1:
        k._u.b1 = w_base_t::uint1_t(kk);
        break;
    case test_u1:
        k._u.u1_num = w_base_t::uint1_t(kk);
        break;
    case test_i1: 
        k._u.i1_num = w_base_t::int1_t(kk);
        break;
    case test_u2:
        k._u.u2_num = w_base_t::uint2_t(kk);
        break;
    case test_i2: 
        k._u.i2_num = w_base_t::int2_t(kk);
        break;
    case test_u4:
        k._u.u4_num = w_base_t::uint4_t(kk);
        break;
    case test_spatial: 
    case test_i4: 
        k._u.i4_num = w_base_t::int4_t(kk);
		DBG(<<"convert_to("<<kk<<") yields " << k._u.i4_num);
        break;
    case test_u8:
        k._u.u8_num = w_base_t::uint8_t(kk);
        break;
    case test_i8: 
        k._u.i8_num = w_base_t::int8_t(kk);
        break;
    case test_f4:
        k._u.f4_num = w_base_t::f4_t(kk);
        break;
    case test_f8:
        k._u.f8_num = w_base_t::f8_t(kk);
        break;
    default:
        cerr << "switch" <<endl;
        w_assert3(0);
    } /* switch */
}

void 
convert_back(typed_value &k, int& kk, typed_btree_test t, char * W_IFDEBUG1(stringbuffer) ) 
{
    switch(t) {
    case test_b23:
    case test_blarge:
    case test_bv: {
            const char *p = k._u.bv;
            w_assert1(k._u.bv == stringbuffer);
            int n=0;
            while(*p == 'o') { p++; }
            while(*p == 'n') { n--; p++; }
            while(*p == 'p') { n++; p++; }
            // w_assert1(*p == '\0');
            w_assert1(p > k._u.bv);
            kk = n;
            if(n == 0) {
               k._length = 2;
            } else if(n < 0) {
               k._length = (0-n)+1;
            } else {
               k._length = n+1;
            }
        }
        break;
    case test_b1:
        kk = int(k._u.b1);
        break;
    case test_u1:
        kk = int(k._u.u1_num);
        break;
    case test_i1: 
        kk = int(k._u.i1_num);
        break;
    case test_u2:
        kk = int(k._u.u2_num);
        break;
    case test_i2: 
        kk = int(k._u.i2_num);
        break;
    case test_u4:
        kk = int(k._u.u4_num);
        break;
    case test_spatial: 
    case test_i4: 
        kk = int(k._u.i4_num);
        break;
    case test_u8:
        kk = int(k._u.u8_num);
        break;
    case test_i8: 
        kk = int(k._u.i8_num);
        break;
    case test_f4:
        kk = int(k._u.f4_num);
        break;
    case test_f8:
        kk = int(k._u.f8_num);
        break;
    default:
        cerr << "switch" <<endl;
        w_assert3(0);
    } /* switch */
}


w_rc_t 
ltestCSKF(
    const rid_t&        ,  // record id
    const object_t&    obj_in,
    key_cookie_t       k,  // type info
    factory_t&         ,
    skey_t*            key
)
{
    smsize_t z = k.make_smsize_t() ;
    // length is body_size - z - sizeof(rid_t)

    smsize_t length = obj_in.body_size() - z - sizeof(rid_t);
    new(key) skey_t(obj_in, z, length, false);
    return RCOK;
}

w_rc_t 
testCSKF(
    const rid_t&        ,  // record id
    const object_t&     obj_in,
    key_cookie_t        k,  // type info
    factory_t&          ,
    skey_t*             key
)
{
    smsize_t z = k.make_smsize_t();
    // length is body_size - z - sizeof(rid_t)

    smsize_t length = obj_in.body_size() - z - sizeof(rid_t);
    new(key) skey_t(obj_in, z, length, false);
    return RCOK;
}
