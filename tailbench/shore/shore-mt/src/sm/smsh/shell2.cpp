/*<std-header orig-src='shore'>

 $Id: shell2.cpp,v 1.61 2010/05/26 01:20:51 nhall Exp $

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

#include <cctype>
#include "shell.h"

#include <new>

#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_array_t<bool>;
#endif

#define LARGEKEYSTRING (ss_m::page_sz/3)
#define LARGESTRING ss_m::page_sz 
#define STRINGMAX LARGESTRING+1


void
cvt2typed_value(
    // IN:
    typed_btree_test t,
    const char *string,
    // OUT:
    typed_value &_v
)
{
    switch(t) {
    case test_blarge:
    case test_b23:
    case test_bv:
        _v._u.bv = string;
        _v._length = strlen(_v._u.bv);
        break;

    case test_b1:
        _v._u.b1 = *string;
        _v._length = sizeof(char);
        break;

    case test_i1:
        _v._u.i1_num = int1_t(atoi(string));
        _v._length = sizeof(int1_t);
        break;

    case test_i2:
        _v._u.i2_num = int2_t(atoi(string));
        _v._length = sizeof(int2_t);
        break;

    case test_i4:
        _v._u.i4_num = int4_t(atoi(string));
        _v._length = sizeof(int4_t);
        break;

    case test_i8:
        /* XXX need input operators for types */
        _v._u.i8_num = w_base_t::strtoi8(string);
        _v._length = sizeof(w_base_t::int8_t);
        break;

    case test_u1:
        _v._u.u1_num = uint1_t(atoi(string));
        _v._length = sizeof(uint1_t);
        break;

    case test_u2:
        _v._u.u2_num = uint2_t(atoi(string));
        _v._length = sizeof(uint2_t);
        break;

    case test_u4:
        _v._u.u4_num = uint4_t(atoi(string));
        _v._length = sizeof(uint4_t);
        break;

    case test_u8:
        /* XXX need input operators for types */
        _v._u.u8_num = w_base_t::strtou8(string);
        _v._length = sizeof(w_base_t::uint8_t);
        break;

    case test_f4:
        _v._u.f4_num = float(atof(string));
        _v._length = sizeof(f4_t);
        break;

    case test_f8:
        _v._u.f8_num = atof(string);
        _v._length = sizeof(f8_t);
        break;

    default:
        cerr << "Unknown comparison type " << W_ENUM(t) << endl;
        W_FATAL(ss_m::eNOTIMPLEMENTED);
        break;
    }
}

typed_btree_test cvt2type(const char *s); // forward

/* The signed and unsigned values share the same memory range.
   The code used to cast everything ... which meant most of the
   casts were wrong once code started being copied.  This fixes
   the casting problem ... and even sets it up so that different
   memory could be used for each type in the future. */

/* XXX BTW, the prior structure is why macros or templates aren't
   used to generate the comparison functions and the replicated code
   in switch statemnts, etc -- each was hacked differently with the
   typecasts previously.   And some of the hacks were incorrect types! */

/* XXX Note that the values used for the index arrays are
   integer offsets -- which will be a problem if you ever test
   code with >MAXINT integers.  Not something to worry about
   right away.  Just another pecularity. */

int1_t *values_i1  = 0;
uint1_t *values_u1;

int2_t *values_i2  = 0;
uint2_t *values_u2;

int4_t *values_i4  = 0;
uint4_t *values_u4;

w_base_t::int8_t *values_i8  = 0;
w_base_t::uint8_t *values_u8;

f4_t *values_f4  = 0;
f8_t *values_f8  = 0;
char **values_b  = 0;

int u1_cmp(const void *_p, const void *_q) 
{
    uint1_t *p = &values_u1[*(int *)_p];
    uint1_t *q = &values_u1[*(int *)_q];

    if (*p < *q) return -1;
    if (*p > *q) return 1;
    return 0;
}
int u2_cmp(const void *_p, const void *_q) 
{
    uint2_t *p = &values_u2[*(int *)_p];
    uint2_t *q = &values_u2[*(int *)_q];

    if (*p < *q) return -1;
    if (*p > *q) return 1;
    return 0;
}

int u4_cmp(const void *_p, const void *_q) 
{
    uint4_t *p = &values_u4[*(int *)_p];
    uint4_t *q = &values_u4[*(int *)_q];

    DBG(<<"p=" << *(uint4_t *)p
        <<"q=" << *(uint4_t *)q
    );
    if (*p < *q) return -1;
    if (*p > *q) return 1;
    return 0;
}

int u8_cmp(const void *_p, const void *_q) 
{
    w_base_t::uint8_t *p = &values_u8[*(int *)_p];
    w_base_t::uint8_t *q = &values_u8[*(int *)_q];

    DBG(<<"p=" << *(w_base_t::uint8_t *)p
        <<"q=" << *(w_base_t::uint8_t *)q
    );
    if (*p < *q) return -1;
    if (*p > *q) return 1;
    return 0;
}

int i1_cmp(const void *_p, const void *_q) 
{
    int1_t *p = &values_i1[*(int *)_p];
    int1_t *q = &values_i1[*(int *)_q];

    if (*p < *q) return -1;
    if (*p > *q) return 1;
    return 0;
}

int i2_cmp(const void *_p, const void *_q) 
{
    int2_t *p = &values_i2[*(int *)_p];
    int2_t *q = &values_i2[*(int *)_q];

    if (*p < *q) return -1;
    if (*p > *q) return 1;
    return 0;
}

int i4_cmp(const void *_p, const void *_q) 
{
    int4_t *p = &values_i4[*(int *)_p];
    int4_t *q = &values_i4[*(int *)_q];

    DBG(<<"p=" << *(int4_t *)p
        <<"q=" << *(int4_t *)q
    );
    if (*p < *q) return -1;
    if (*p > *q) return 1;
    return 0;
}

int i8_cmp(const void *_p, const void *_q) 
{
    w_base_t::int8_t *p = &values_i8[*(int *)_p];
    w_base_t::int8_t *q = &values_i8[*(int *)_q];

    DBG(<<"p=" << *(w_base_t::int8_t *)p
        <<"q=" << *(w_base_t::int8_t *)q
    );
    if (*p < *q) return -1;
    if (*p > *q) return 1;
    return 0;
}

int f4_cmp(const void *_p, const void *_q) 
{
    f4_t *p = &values_f4[*(int *)_p];
    f4_t *q = &values_f4[*(int *)_q];

    if (*p < *q) return -1;
    if (*p > *q) return 1;
    return 0;
}

int f8_cmp(const void *_p, const void *_q) 
{
    f8_t *p = &values_f8[*(int *)_p];
    f8_t *q = &values_f8[*(int *)_q];

    if (*p < *q) return -1;
    if (*p > *q) return 1;
    return 0;
}
#define LARGEKEYSTRING (ss_m::page_sz/3)
#define LARGESTRING ss_m::page_sz 
#define STRINGMAX LARGESTRING+1

int lex_cmp(const void *_p, const void *_q, int len) 
{
    w_assert3(len >= 0); 

    u_char **p = (u_char **)&values_b[*(int *)_p];
    u_char **q = (u_char **)&values_b[*(int *)_q];

    /* slower, character-at-a-time comparison */
    const u_char *a = *(u_char **)p;
    const u_char *b = *(u_char **)q;

    int l1 = 0, l2 = 0;
    { for(const u_char *pp = a; *pp && (l1 < len); pp++, l1++) ; }
    { for(const u_char *pp = b; *pp && (l2 < len); pp++, l2++) ; }
    len = (int)(l1>l2 ? l1 : l2);

#ifdef W_TRACE
    {
    vec_t tmpa(a, unsigned(l1));
    vec_t tmpb(b, unsigned(l2));
        DBG(<<"lex_cmp(_p = "
        << *(int *)_p << ", _q=" 
        << *(int *)_q << ", len=" << len 
        << ")\na=" << tmpa
        << "\nb=" << tmpb);
    }
#endif
    int diff;
#ifdef W_TRACE
    const u_char *start = a;
#endif
    while (len-- > 0) {
    if ((diff = *a - *b)) {
        DBG(<<"lex_cmp returns " << diff
        << " in char " << int(a - start));
        return diff;
    }
    a++; b++; 
    }
    DBG(<<"lex_cmp returns 0 (SAME)");
    return 0;
}

int lex_cmp1(const void *p, const void *q) 
{
    return lex_cmp(p,q,1);
}
int lex_cmplarge(const void *p, const void *q) 
{
    return lex_cmp(p,q,LARGEKEYSTRING);
}
int lex_cmp23(const void *p, const void *q) 
{
    return lex_cmp(p,q,23);
}
int lex_cmpv(const void *p, const void *q) 
{
    return lex_cmp(p,q,LARGEKEYSTRING);
}

char *make_printable(int i, int limit)
{
    if(i < 0) {
    i = 0-i;
    }
    if(i > limit) {
    i =  i % limit;
    }
    if(i < 1) i=1;

    DBG(<<"chose length " << i);
    char *s = new char[i+1];
    char j = (char)0;
    int  rnd=0;

    rand48&  generator(get_generator());
    s[i] = '\0';
    while(i-- > 0) {
        while(!isalnum(j) 
            // && !ispunct(j)
        ) {
            rnd = (generator.rand() & 0x7f);
            j = (char)rnd;
        }
        s[i] = j; 
    }
    
    for (i=0; i<(int)strlen(s); i++) {
    w_assert3(isalnum(s[i]));
    }

#ifdef W_TRACE
    {
        vec_t tmp(s, strlen(s));
        DBG(<<"make_printable(" << i << "," << limit <<")=" << tmp);
    }
#endif
    w_assert3( (int)strlen(s) <= limit);
    w_assert3( (int)strlen(s) > 0);
    return s;
}

void possible() { }
void special() { }

/*
 * NB: we must define the string like this,
 * because if we use '\0377', the compiler
 * does a conversion to a "legitimate" character,
 * defeating the whole point of using this string.
 */
int _laststring[] = { 0xffffffff, 0x0 };
char *LASTSTRING = (char *)&_laststring;

static int
_t_test_typed_btree(
	Tcl_Interp* ip,
	int vid,
	int n, 
	const char *_keytype,
	const char *ccstring
	)
{
    // args: $volid $nrec $ktype $cc
    ss_m::concurrency_t build_cc = ss_m::t_cc_kvl;
    if(ccstring) {
    build_cc = cvt2concurrency_t(ccstring);
    }
    ss_m::concurrency_t cc = build_cc;

    const char *keytype = check_compress_flag(_keytype);

    w_rc_t rc;
    FUNC(_t_test_typed_btree);
    DBG(<<"test typed btree vid= " << vid
	<< " n=" << n
	<< " _keytype=" << _keytype
	<< " keytype=" << keytype
	);
    char *key_buffer =0;
    {
    int duplicates = 0;
    bool sort_is_stable = true;

    /*
     * NB: before sticking in random values,
     * we sorted -x -> +y; some of the following
     * grunge is just a hold-over from those days, when
     * the test was just for ints, and we wanted to be sure
     * to use negative values.
     */
    assert(n > 0);
    // const int h = n/2;
    const int h = 0;
    int low = 0 - h;
    int high = (n-1) - h;
    int zero = 0;
    int highm1 = (high - 1)>low?(high-1): low;
    int highm2 = (high - 2)>low?(high-2): low;
    int lowp1 = (low + 1)>high?(low+1): high;
    int lowp2 = (low + 2)>high?(low+2): high;

    stid_t stid;

    typed_btree_test t = cvt2type(keytype);
    if(t == test_nosuch) {
        Tcl_AppendResult(ip, "No test for key type: ", keytype, 0);
        return TCL_ERROR;
    }

    /* keys: */
    char i1=0;
    int i4=0;
    w_base_t::int8_t i8 = 0;
    short i2=0;
    float f4=0;
    double f8=0;
    char *b=0;
    vec_t key, el;
    smsize_t klen=0;

    /*
     * 1) Create a set of values to stuff into the index.
     *    Create them in random order.
     * 2) Sort them with qsort, and give them an ordinal value
     *   to stuff into the element portion of the index.
     * Insert them in the index in unsorted order.
     * Scan the index, checking the ordinal numbers.
     *
     * Finally, do all the sordid range scans.
     * 
     */

    int i=0;  // index only
    smsize_t elen = sizeof(i);


    /*
     * create the set of values to use for the given key type
     */
    switch(t) {
        case test_i1:
        case test_u1:
        values_i1 = new int1_t[n];
        values_u1 = (uint1_t *) values_i1;
        break;
        case test_i2:
        case test_u2:
        values_i2 = new int2_t[n];
        values_u2 = (uint2_t *) values_i2;
        break;
        case test_u4:
        case test_i4:
        values_i4 = new int4_t[n];
        values_u4 = (uint4_t *) values_i4;
        break;
        case test_u8:
        case test_i8:
        values_i8 = new w_base_t::int8_t[n];
        values_u8 = (w_base_t::uint8_t *) values_i8;
        break;
        case test_f4:
        values_f4 = new f4_t[n];
        break;
        case test_f8:
        values_f8 = new f8_t[n];
        break;
        case test_b1:
        case test_b23:
        case test_blarge:
        case test_bv:  {
        typedef char * cp;
        values_b = new cp[n];
        key_buffer = new char[LARGEKEYSTRING]; // max
        }
        break;


        case test_spatial:
        default: 
        w_assert1(0);
    }

    /*
     * NB:
     * The first value 
     * we insert will be the zero value for signed keys.
     * For the unsigned keys, it doesn't really matter.
     */
    rand48&  generator(get_generator());

    //create a set of unsorted values: values_*[]
    int j;
    zero = low;
    for (j = low; j <= high; j++)  {

        // TODO : limit this by the range of the
        // domain instead
        i = generator.rand() % 65535;

    #define MAKE_CHAR(i) ((char)(i&0xff))
    #define MAKE_SHORT(i) ((short)i)
    #define MAKE_INT(i) (i)
    #define MAKE_INT8(i) (i)
    #define MAKE_FLOAT(i) ((float)i * 1.0001)
    #define MAKE_DOUBLE(i) (((double)i*100000000) * 1.00001)
    #define MAKE_STRING(i,limit) make_printable(i,limit) 

    #define NORMALIZE(i)  (i-low)

        DBG(<<"j=" << j
            << " normalized=" << NORMALIZE(j)
            << " random i=" << i
        );
            
        w_assert3(NORMALIZE(j) >= 0 && NORMALIZE(j) <n);
        b=0;
        switch (t) {
        case test_u1:
        case test_i1:
            i1 = MAKE_CHAR(i);
            values_i1[NORMALIZE(j)] = i1;
            if (linked.verbose_flag) {
            if(t == test_i1) {
                cerr << values_i1[NORMALIZE(j)] << endl;
            } else {
                cerr << values_u1[NORMALIZE(j)] << endl;
            }
            }
            break;
        case test_u2:
        case test_i2:
            i2 = MAKE_SHORT(j==low?0:i);
            values_i2[NORMALIZE(j)] = i2;
            if (linked.verbose_flag) {
            if(t == test_i2) {
                cerr << values_i2[NORMALIZE(j)] << endl;
            } else {
                cerr << values_u2[NORMALIZE(j)] << endl;
            }
            }
            break;
        case test_u4:
        case test_i4:
            i4 = MAKE_INT(j==low?0:i);
            values_i4[NORMALIZE(j)] = i4;
            if (linked.verbose_flag) {
            if(t == test_i4) {
                cerr << values_i4[NORMALIZE(j)] << endl;
            } else {
                cerr << values_u4[NORMALIZE(j)] << endl;
            }
            }
            break;
        case test_u8:
        case test_i8:
            i8 = MAKE_INT8(j==low?0:i);
            values_i8[NORMALIZE(j)] = i8;
            if (linked.verbose_flag) {
            if(t == test_i8) {
                cerr << values_i8[NORMALIZE(j)] << endl;
            } else {
                cerr << values_u8[NORMALIZE(j)] << endl;
            }
            }
            break;
        case test_f4:
            f4 = (float) MAKE_FLOAT(j==low?0:i);
            values_f4[NORMALIZE(j)] = f4;
            if (linked.verbose_flag) {
            cerr << values_f4[NORMALIZE(j)] << endl;
            }
            break;
        case test_f8:
            f8 = MAKE_DOUBLE(j==low?0:i);
            values_f8[NORMALIZE(j)] = f8;
            if (linked.verbose_flag) {
            cerr << values_f8[NORMALIZE(j)] << endl;
            }
            break;
        case test_b1:
            b = MAKE_STRING(1, 1);
            /*FALLTHROUGH*/
        case test_b23:
            if(!b) b = MAKE_STRING(23, 23);
            /*FALLTHROUGH*/
        case test_blarge:
        case test_bv: {
            if(!b) b = MAKE_STRING(i, LARGEKEYSTRING-1);
            values_b[NORMALIZE(j)] = b;
            if (linked.verbose_flag) {
            cerr << values_b[NORMALIZE(j)] << endl;
            }
            }
            break;

        case test_spatial:
        default: w_assert1(0);
        }
    }
    /*
     * Sort them...
     *
     * After sorting, determine if we have duplicates, and
     * set up the arrays sorted[] and reverse[]:
     *   values_*[i] will be inserted in the order i=0->n
     *   The scan should yield values in the order
     *     values_*[sorted[i]] for i=0->n
     *   value_*[i] should appear in the scan in place reverse[i]
     */
    int* sorted = new int[n];
    w_auto_delete_array_t<int> auto_del_sorted(sorted);

    int* reverse = new int[n];
    w_auto_delete_array_t<int> auto_del_reverse(reverse);

    bool* duplicated = new bool[n]; // if true, sorted[i] is duplicated
        // if value X appears 3 times, the first 2 sorted[j] are
        // marked duplicated, and the last one isn't
    w_auto_delete_array_t<bool> auto_del_dup(duplicated);

    {
        for(j=0; j<n; j++) { 
        sorted[j] = j;
        duplicated[j]=false;
        }
        int (*compar)(const void *, const void *) = 0;

        switch(t) {
        case test_u1:
            compar = u1_cmp;
            break;
        case test_i1:
            compar = i1_cmp;
            break;
        case test_u2:
            compar = u2_cmp;
            break;
        case test_i2:
            compar = i2_cmp;
            break;
        case test_u4:
            compar = u4_cmp;
            break;
        case test_i4:
            compar = i4_cmp;
            break;
        case test_u8:
            compar = u8_cmp;
            break;
        case test_i8:
            compar = i8_cmp;
            break;
        case test_f4:
            compar = f4_cmp;
            break;
        case test_f8:
            compar = f8_cmp;
            break;

        case test_b1:
        case test_b23:
        case test_blarge:
        case test_bv: {
            if(t==test_b1) {
            compar = lex_cmp1;
            } else 
            if(t==test_blarge) {
            compar = lex_cmplarge;
            } else 
            if(t==test_b23) {
            compar = lex_cmp23;
            } else {
            compar = lex_cmpv;
            }
            }
            break;

        case test_spatial:
        default: w_assert1(0);
        }
        qsort(sorted, n, sizeof(int), compar);

        /* 
         * sorted[] has been sorted, values_* remains the same
         */


        /* check sort order : discover if we have duplicates  */
        DBG(<<"skipping 0");
        for(j=1; j<n; j++) {
        int len=0;
        switch(t) {
        case test_u1:
            DBG(<<"j=" << j
            << " values_u1[sorted[j]] = " << (u_char) values_u1[sorted[j]]
            << " values_u1[sorted[j-1]] = " << (u_char) values_u1[sorted[j-1]]
            );
            w_assert1(values_u1[sorted[j]] >= values_u1[sorted[j-1]]);
            if(values_u1[sorted[j]] == values_u1[sorted[j-1]]) {
            duplicated[j-1] = true;
            duplicates++;
            }
            break;

        case test_i1:
            DBG(<<"j=" << j
            << " values_i1[sorted[j]] = " << (u_char) values_i1[sorted[j]]
            << " values_i1[sorted[j-1]] = " << (u_char) values_i1[sorted[j-1]]
            );
            w_assert1(values_i1[sorted[j]] >= values_i1[sorted[j-1]]);
            if(values_i1[sorted[j]] == values_i1[sorted[j-1]]) {
            duplicated[j-1] = true;
            duplicates++;
            }
            break;

        case test_u2:
            DBG(<<"j=" << j
            << " values_u2[sorted[j]] = " << values_u2[sorted[j]]
            << " values_u2[sorted[j-1]] = " << values_u2[sorted[j-1]]
            );
            if (!(values_u2[sorted[j]] >= values_i2[sorted[j-1]])) {
            unsigned short &u1 = values_u2[sorted[j]];
            unsigned short &u2 = values_u2[sorted[j-1]];
            cout << "u1 " << u1 << " u2 " << u2 << endl;
            cout << "ORDER err j " << j << " sorted[j] " << sorted[j]
                << " sorted[j-1] " << sorted[j-1]
                << endl << '\t'
                << " values_u2[s[j]] " << values_u2[sorted[j]] 
                << " SB >= "
                << " values_u2[s[j-1]] " << values_u2[sorted[j-1]] 
                << endl;
            }
            w_assert1(values_u2[sorted[j]] >= values_u2[sorted[j-1]]);
            if (values_u2[sorted[j]] == values_u2[sorted[j-1]]) {
            duplicated[j-1] = true;
            duplicates++;
            }
            break;

        case test_i2:
            DBG(<<"j=" << j
            << " values_i2[sorted[j]] = " << values_i2[sorted[j]]
            << " values_i2[sorted[j-1]] = " << values_i2[sorted[j-1]]
            );
            w_assert1(values_i2[sorted[j]] >= values_i2[sorted[j-1]]);
            if(values_i2[sorted[j]] == values_i2[sorted[j-1]]) {
            duplicated[j-1] = true;
            duplicates++;
            }
            break;

        case test_u4:
            DBG(<<"j=" << j
            << " values_u4[sorted[j]] = " << values_u4[sorted[j]]
            << " values_u4[sorted[j-1]] = " << values_u4[sorted[j-1]]
            );
            w_assert1(values_u4[sorted[j]] >= values_u4[sorted[j-1]]);
            if(values_u4[sorted[j]] == values_u4[sorted[j-1]]) {
            duplicates++;
            duplicated[j-1] = true;
            }
            break;

        case test_i4:
            DBG(<<"j=" << j
            << " values_i4[sorted[j]] = " << values_i4[sorted[j]]
            << " values_i4[sorted[j-1]] = " << values_i4[sorted[j-1]]
            );
            w_assert1(values_i4[sorted[j]] >= values_i4[sorted[j-1]]);
            if(values_i4[sorted[j]] == values_i4[sorted[j-1]]) {
            duplicates++;
            duplicated[j-1] = true;
            }
            break;

        case test_u8:
            DBG(<<"j=" << j
            << " values_u8[sorted[j]] = " << values_u8[sorted[j]]
            << " values_u8[sorted[j-1]] = " << values_u8[sorted[j-1]]
            );
            w_assert1(values_u8[sorted[j]] >= values_u8[sorted[j-1]]);
            if(values_u8[sorted[j]] == values_u8[sorted[j-1]]) {
            duplicates++;
            duplicated[j-1] = true;
            }
            break;

        case test_i8:
            DBG(<<"j=" << j
            << " values_i8[sorted[j]] = " << values_i8[sorted[j]]
            << " values_i8[sorted[j-1]] = " << values_i8[sorted[j-1]]
            );
            w_assert1(values_i8[sorted[j]] >= values_i8[sorted[j-1]]);
            if(values_i8[sorted[j]] == values_i8[sorted[j-1]]) {
            duplicates++;
            duplicated[j-1] = true;
            }
            break;

        case test_f4:
            DBG(<<"j=" << j
            << " values_f4[sorted[j]] = " << values_f4[sorted[j]]
            << " values_f4[sorted[j-1]] = " << values_f4[sorted[j-1]]
            );
            w_assert1(values_f4[sorted[j]] >= values_f4[sorted[j-1]]);
            if(values_f4[sorted[j]] == values_f4[sorted[j-1]]) {
            duplicated[j-1]=true;
            duplicates++;
            }
            break;

        case test_f8:
            DBG(<<"j=" << j
            << " values_f8[sorted[j]] = " << values_f8[sorted[j]]
            << " values_f8[sorted[j-1]] = " << values_f8[sorted[j-1]]
            );
            w_assert1(values_f8[sorted[j]] >= values_f8[sorted[j-1]]);
            if(values_f8[sorted[j]] == values_f8[sorted[j-1]]) {
            duplicates++;
            duplicated[j-1]=true;
            }
            break;

        case test_b1:
            len=1;
        case test_b23:
            if(!len) len=23;
        case test_blarge:
        case test_bv:
            if(!len) len=LARGEKEYSTRING;

#ifdef W_TRACE
            {
            vec_t tmpa(values_b[sorted[j]], 
                strlen(values_b[sorted[j]]));
            vec_t tmpb(values_b[sorted[j-1]], 
                strlen(values_b[sorted[j-1]]));
            DBG(<<"j=" << j
            <<" len=" << len
            << " values_b[sorted[j]] = " 
            << tmpa
            << " values_b[sorted[j-1]] = " 
            << tmpb
            );
            }
#endif
            w_assert1( lex_cmp(&sorted[j], &sorted[j-1], len)>=0);

            if(lex_cmp(&sorted[j],&sorted[j-1], len)==0) {
            duplicates++;
            DBG(<<"duplicated: " << j-1);
            duplicated[j-1]=true;
            } else {
            if(strcmp(values_b[sorted[j-1]],
                values_b[sorted[j]]) == 0) {
                w_assert1("BAD LEX_CMP!");
            }
            }
            break;

        case test_spatial:
        default: w_assert1(0);
        }

        // If we just found duplicate, but the
        // insertion order isn't preserved within
        // the duplicates, we've got an unstable sort
        // That makes this test rather difficult ...

        if(duplicated[j-1] && sorted[j-1] > sorted[j]) {
            sort_is_stable = false;
        }
        }
        DBG(<<"Sort is stable?: " << sort_is_stable);
        while(! sort_is_stable) {
        // Stabilize it - re-order duplicates to make
        // it stable.
        DBG(<<"fixing sorted...");
        sort_is_stable = true;
        for(j=1; j<n; j++) {
            if(duplicated[j-1] && sorted[j-1] > sorted[j]) {
            int temp = sorted[j];
            sorted[j] = sorted[j-1];
            sorted[j-1] = temp;
            }
        }
        }

        DBG(<<"INSERT-ORDER / SORT-ORDER (* if duplicated):");
        for(j=0; j<n; j++) {

        switch(t) {
        case test_u1:
        case test_i1:
            DBG( << j << " : " 
                <<  (unsigned int)((u_char) values_i1[j]) 
                << " /\t"  << sorted[j]
                << " :\t"  << (unsigned int)((u_char)(values_i1[sorted[j]]))
                << (char * )(duplicated[j]?"*":"")
                );
            break;

        case test_u2:
            DBG( << j << " : " <<  values_u2[j] 
                << " /\t"  << sorted[j]
                << " :\t" << values_u2[sorted[j]]
                << (char * )(duplicated[j]?"*":"")
                );
            break;
        case test_i2:
            DBG( << j << " : " <<  values_i2[j] 
                << " /\t"  << sorted[j]
                << " :\t" << values_i2[sorted[j]]
                << (char * )(duplicated[j]?"*":"")
                );
            break;

        case test_u4:
            DBG( << j << " : " <<  values_u4[j] 
                << " /\t"  << sorted[j]
                << " :\t" << values_u4[sorted[j]]
                << (char * )(duplicated[j]?"*":"")
                );
            break;

        case test_i4:
            DBG( << j << " : " <<  values_i4[j] 
                << " /\t"  << sorted[j]
                << " :\t" << values_i4[sorted[j]]
                << (char * )(duplicated[j]?"*":"")
                );
            break;

        case test_u8:
            DBG( << j << " : " <<  values_u8[j] 
                << " /\t"  << sorted[j]
                << " :\t" << values_u8[sorted[j]]
                << (char * )(duplicated[j]?"*":"")
                );
            break;

        case test_i8:
            DBG( << j << " : " <<  values_i8[j] 
                << " /\t"  << sorted[j]
                << " :\t" << values_i8[sorted[j]]
                << (char * )(duplicated[j]?"*":"")
                );
            break;

        case test_f4:
            DBG( << j << " : " <<  values_f4[j] 
                << " /\t"  << sorted[j]
                << " :\t" << values_f4[sorted[j]]
                << (char * )(duplicated[j]?"*":"")
                );
            break;

        case test_f8:
            DBG( << j << " : " <<  values_f8[j] 
                << " /\t"  << sorted[j]
                << " :\t" << values_f8[sorted[j]]
                << (char * )(duplicated[j]?"*":"")
                );
            break;

        case test_b1:
        case test_b23:
        case test_blarge:
        case test_bv:
#ifdef W_TRACE
            {
            vec_t tmpa(values_b[j], strlen(values_b[j]));
            vec_t tmpb(values_b[sorted[j]], 
                strlen(values_b[sorted[j]]));
            DBG( << j << " : " <<  tmpa
                << " /\t"  << sorted[j]
                << " :\t" << tmpb 
                << (char * )(duplicated[j]?"*":"")
                );
            }
            
#endif
            break;

        case test_spatial:
        default: w_assert1(0);
        }
        }
        {   /* 
         * Populate reverse[], now that the results of the
         * sort are stable 
         */
        for(j=0; j<n; j++) {
            reverse[sorted[j]] = j;
        }
        // make sure sorted and reverse are inverses
        for(j=0; j<n; j++) {
            w_assert3(sorted[reverse[j]] == j);
        }
        }
        if (linked.verbose_flag) {
        cerr <<" found " << duplicates << " duplicates in the data set"
            << endl;
        }
        DBG( <<" found " << duplicates << " duplicates in the data set");
    }

    /*
     * Create the index -- unique if no duplicates
     */
    {
        DO( sm->create_index(vid, 
            (duplicates==0)?sm->t_uni_btree:sm->t_btree, 
             ss_m::t_regular, keytype, build_cc,  stid) );
        DBG(<<"created store " << stid);
    }

    /* 
     * Set up vectors for keys, so that we can do the insertions
     */

    /*
     * set up key, elem vectors for insertions
     */
    key.reset();
    switch(t) {
        case test_i1:
        case test_u1:
        key.put(&i1, (klen = sizeof(i1))); 
        break;

        case test_i2:
        case test_u2:
        key.put(&i2, (klen = sizeof(i2))); 
        break;

        case test_u4:
        case test_i4:
        key.put(&i4, (klen = sizeof(i4))); 
        break;

        case test_u8:
        case test_i8:
        key.put(&i8, (klen = sizeof(i8))); 
        break;

        case test_f4:
        key.put(&f4, (klen = sizeof(f4))); 
        break;

        case test_f8:
        key.put(&f8, (klen = sizeof(f8))); 
        break;

        case test_b23:
        case test_blarge:
        case test_b1:
        case test_bv:
        // to be done below -- see "HERE"
        // key.put(b, (klen = strlen(b)));
        break;

        case test_spatial:
        default: w_assert1(0);
    }
    w_assert1(key.size() == klen);

    // element = index of sorted[]
    int elemvalue;
    el.reset();
    el.put((void *)&elemvalue, sizeof(elemvalue));  

    /*
     * Now we can just vary the values and do 
     * repeated insertions with the given key, elem vectors
     */
    j=0;
    for (i = low; i <= high && !rc.is_error(); i++, j++)  {

        w_assert3(NORMALIZE(i) >= 0 && NORMALIZE(i) <n);
        elemvalue = reverse[NORMALIZE(i)];

        if(values_i1) i1 = values_i1[NORMALIZE(i)];
        if(values_i2) i2 = values_i2[NORMALIZE(i)];
        if(values_i4) i4 = values_i4[NORMALIZE(i)];
        if(values_i8) i8 = values_i8[NORMALIZE(i)];
        if(values_f4) f4 = values_f4[NORMALIZE(i)];
        if(values_f8) f8 = values_f8[NORMALIZE(i)];
        if(values_b) {
        b = values_b[NORMALIZE(i)];
        // put into key HERE
        w_assert3(b);
        w_assert3(isalnum(*b));

        key.reset().put(b, (klen = strlen(b)));
        w_assert3(key.size() < LARGEKEYSTRING);
        w_assert3(klen < LARGEKEYSTRING);
        w_assert3(klen == key.size());
        }

        DBG( << "inserting " << keytype <<
        " key[" << i << "] : "  
        // << i1 << "," 
        // << i2 << "," 
        // << i4 << "," 
        // << i8 << "," 
        // << f4 << "," 
        // << f8 << ","
        << b << ";"
        << " elem=" << elemvalue
        << endl
        << " klen " << klen
        << " key.size()=" << key.size()
        << " elen " << elen
        << " el.size()=" << el.size() 
        << endl
        );
#ifdef W_DEBUG
        if(values_b) w_assert3(isalnum(*b));
#endif /* W_DEBUG */
        if (linked.verbose_flag) {
        cerr  << "inserting " << keytype <<
            " key[" << i << "] : "  
            << (u_char)i1 << "," 
            << i2 << "," 
            << i4 << "," 
            << i8 << "," 
            << f4 << "," 
            << f8 << ","
            << b << "; elem=" << elemvalue
            << endl
            << " klen " << klen
            << " key.size()=" << key.size()
            << " elen " << elen
            << " el.size()=" << el.size() << " "
            << endl;
        }
        w_assert3(key.size() > 0);
        {
        rc = sm->create_assoc( stid, key, el);
        }
#if W_DEBUG_LEVEL > 2
        if (rc.reset().is_error()) {
        cerr  << "inserting " << keytype <<
            " key[" << i << "] : "  
            << (u_char)i1 << "," 
            << i2 << "," 
            << i4 << "," 
            << i8 << "," 
            << f4 << "," 
            << f8 << ","
            << b << "; elem=" << elemvalue
            << endl
            << " klen " << klen
            << " key.size()=" << key.size()
            << " elen " << elen
            << " el.size()=" << el.size() << " "
            << endl;
        cerr  << "Dump of index after insert:" << endl;
        DO( sm->print_index(stid) );
        }
#endif /* W_DEBUG */
    }
    if (rc.is_error())  {
        cerr << RC_AUGMENT(rc) <<endl;
        /*
        w_reset_strstream(tclout);  
        tclout << smsh_err_name(rc.err_num()) << ends;
        Tcl_AppendResult(ip, tclout.c_str(), 0);
        w_reset_strstream(tclout);
        */
        goto failure;    
    }

    DBG(<<"Insertions done.");
    if (linked.verbose_flag) {
        DO( sm->print_index(stid) );
    }

    /*
     * Done with insertions; now test the scans.
     * We can re-use key and elem vectors, but
     * we have to set up vectors to define the ranges
     * of the scans.  For that, we use first_*, last_*
     */
    {

        int first=0, last=0; // indexes only

        // for values:
        char first_i1=0, last_i1=0;
        short first_i2=0, last_i2=0;
        int first_i4=0, last_i4=0;
        w_base_t::int8_t first_i8=0, last_i8=0;
        float first_f4=0.0, last_f4=0.0;
        double first_f8=0.0, last_f8=0.0;
        const char *first_b=0, *last_b=0;

        vec_t first_vec;
        vec_t last_vec;

        scan_index_i::cmp_t op1, op2;
        int last_test=15;

        // Set up  the vectors for the range scans:
        {
        unsigned int len=0;
        switch(t) {
        case test_u1:
        case test_i1:
            first_vec.put(&first_i1, sizeof(first_i1)); 
            last_vec.put(&last_i1, sizeof(last_i1)); 
            break;

        case test_u2:
        case test_i2:
            first_vec.put(&first_i2, sizeof(first_i2)); 
            last_vec.put(&last_i2, sizeof(last_i2)); 
            break;
        case test_u4:
        case test_i4:
            first_vec.put(&first_i4, sizeof(first_i4)); 
            last_vec.put(&last_i4, sizeof(last_i4)); 
            break;
        case test_u8:
        case test_i8:
            first_vec.put(&first_i8, sizeof(first_i8)); 
            last_vec.put(&last_i8, sizeof(last_i8)); 
            break;
        case test_f4:
            first_vec.put(&first_f4, sizeof(first_f4)); 
            last_vec.put(&last_f4, sizeof(last_f4)); 
            break;
        case test_f8:
            first_vec.put(&first_f8, sizeof(first_f8)); 
            last_vec.put(&last_f8, sizeof(last_f8)); 
            break;

        case test_b1:
            len = 1;
            w_assert1(strlen(b)==1);
        case test_b23:
            if(t==test_b23) {
            w_assert1(strlen(b)==23);
            }
        case test_blarge:
        case test_bv:
            len = strlen(b);
            w_assert1(len < LARGEKEYSTRING);
            /*
             * NB: first_b, last_b == 0 here: we
             * have to do the vec.put()s after
             * we set the values of first_b, last_b, below
             * at VEC_XXXX
             */
            first_vec.put(first_b, len); 
            last_vec.put(last_b, len); 
            break;

        case test_spatial:
        default: w_assert1(0);
        }
        } // close scope on len

        w_assert3(first_vec.size() == klen);
        w_assert3(last_vec.size() == klen);


        for (int bounds_set = 0; bounds_set <= last_test; bounds_set++) {
        DBG(<<"bounds_set = " << bounds_set);
        switch(bounds_set) {
        case 0:
            first = low;
            last = high;
            break;
        case 1:
            first = low;
            last = low/2;
            break;
        case 2:
            first = low;
            last = lowp1;
            break;
        case 3:
            first = low;
            last = lowp2;
            break;
        case 4:
            first = lowp1;
            last = lowp2;
            break;
        case 5:
            first = low;
            last = zero;
            break;
        case 6:
            // not used
            break;
        case 7:
            first = low;
            last = low;
            break;

        case 8:
            first = high/2;
            last = high;
            break;
        case 9:
            first = highm1;
            last = high;
            break;
        case 10:
            first = highm2;
            last = high;
            break;
        case 11:
            first = high;
            last = high;
            break;
        case 12:
            first = zero;
            last = high;
            break;
        case 13:
            first = zero;
            last = zero;
            break;
        case 14:
            // Special case: have no values for this
            first = low-1;
            last = high;
            break;
        case 15:
            first = low;
            // Special case: have no values for this
            last = high+1;
            break;
        case 16:
            // Special case: have no values for this
            first = low-10;
            // Special case: have no values for this
            last = high+100;
            break;
        default:
            abort();
        }
        /* 
         * what if first or last is a duplicate?
         */
        while(first > low && duplicated[first-1] ) first--;
        while(last < high && duplicated[last]) last++;

        if (linked.verbose_flag) {
            cerr << "SCAN# " <<bounds_set << endl;
        }
        DBG(<< "SCAN# " <<bounds_set );
        for (int scan_type = 0; scan_type <= 8; scan_type++) {
            int expected = 0;
            cc = build_cc;
            /*
             * expected == 0 means we expect exactly
             * 0 results
             * expected < 0 means we can't even
             * check because we haven't got the means
             * (e.g., in the presence of random values, 
             *  duplicates, unsigned values, it's harder).
             */
            {
            // things get a bit difficult if first<low or
            // last>high, or if there are duplicates

            switch(scan_type) {
            case 1:
                op1 = scan_file_i::ge;
                op2 = scan_file_i::le;
                expected = (last - first) + 1;
                if(expected < 0) expected = 0;
                break;
            case 0:
                op1 = scan_file_i::gt;
                op2 = scan_file_i::le;
                expected = last - first;
                if(expected < 0) expected = 0;
                break;
            case 2:
                op1 = scan_file_i::ge;
                op2 = scan_file_i::lt;
                expected = last - first;
                if(expected < 0) expected = 0;
                break;
            case 3:
                op1 = scan_file_i::gt;
                op2 = scan_file_i::lt;
                expected = (last - first) - 1;
                if(expected < 0) expected = 0;
                break;
            case 4:
                op1 = scan_file_i::ge;
                op2 = scan_file_i::eq;
                expected = (last >= first) ? 1 : 0;
                break;
            case 5:
                op1 = scan_file_i::gt;
                op2 = scan_file_i::eq;
                expected = (last > first) ? 1 : 0;
                break;
            case 6:
                op1 = scan_file_i::eq;
                op2 = scan_file_i::le;
                expected = (last >= first) ? 1 : 0;
                break;
            case 7:
                op1 = scan_file_i::eq;
                op2 = scan_file_i::lt;
                expected = (last > first) ? 1 : 0;
                break;
            case 8:
                op1 = scan_file_i::eq;
                op2 = scan_file_i::eq;
                cc = ss_m::t_cc_kvl;
                expected = (last == first) ? 1 : -1;
                // Leave this -1 for the purpose of avoiding
                // a bad scan_index_i at YYYY below 
                break;
            default:
                abort();
            }
            if(expected > 0) {
                switch (op1) {
                case scan_file_i::ge:
                    if((first<low) && op2 != scan_file_i::eq) 
                    expected -= (low-first);
                    break;
                case scan_file_i::gt:
                    break;
                case scan_file_i::eq:
                    if(first<low) expected = 0;
                    break;
                case scan_file_i::le:
                case scan_file_i::lt:
                default:
                    break;
                }
                switch(op2) {
                case scan_file_i::le:
                    if((last>high) && op1 != scan_file_i::eq) 
                        expected -= (last-high);
                    break;
                case scan_file_i::lt:
                    break;
                case scan_file_i::eq:
                    if(last>high) expected = 0;
                    break;
                case scan_file_i::ge:
                case scan_file_i::gt:
                default:
                    break;
                }
            }
            }
            if (linked.verbose_flag) {
            cerr << " scanning " 
                 << cvt2string(op1) <<" " << first 
                 << " " 
                 << cvt2string(op2) <<" " << last <<endl;
            cerr << " expect " << expected << endl;
            }
            DBG( << " scanning " 
             << cvt2string(op1) <<" " << first 
             << " " 
             << cvt2string(op2) <<" " << last
            << " expect " << expected );

            {  
            /* 
             * add or subtract 1 from the *value* associated
             * with first, last, in the cases where first<low
             * and last>high (e.g. "> low-1" && "< high+1" are
             * the criteria)
             */
            int first_adjust = (first < low)? -1 : 0;
            int first_index = (first < low)? low : first;
            int last_adjust = (last > high) ? 1 : 0;
            int last_index = (last > high) ? high : last;

            if(first_adjust == 0) {
            w_assert3(NORMALIZE(first) >= 0 && NORMALIZE(first) <n);
            } 
            if(last_adjust == 0) {
            w_assert3(NORMALIZE(last) >= 0 && NORMALIZE(last) <n);
            }
            switch(t) {
            case test_u1:
            case test_i1:
                first_i1 = first_adjust + 
                values_i1[sorted[NORMALIZE(first_index)]];
                last_i1 = 
                values_i1[sorted[NORMALIZE(last_index)]] 
                + last_adjust;
                if (linked.verbose_flag) {
                cerr << " values range: "  
                     << cvt2string(op1) <<" " 
                     << (u_char) first_i1 << " " 
                     << cvt2string(op2) <<" " 
                     << (u_char) last_i1 <<endl;
                }
                if(first < low && t == test_u1) {
                // can't create a smaller unsigned
                // number than 0
                expected = -2;
                }
                // If our random set covers the entire
                // range of small values, we can't
                // do this test. If it extends either to the 
                // highest small value or to the lowest
                // small value, we can't do this test.
                if(first_i1 >=
                values_i1[sorted[NORMALIZE(first_index)]]) {
                expected = -2;
                }
                if( last_i1 <=
                values_i1[sorted[NORMALIZE(last_index)]] ) {
                expected = -2;
                }
                break;

            case test_u2:
            case test_i2:
                first_i2 = first_adjust + 
                values_i2[sorted[NORMALIZE(first_index)]];
                last_i2 = 
                values_i2[sorted[NORMALIZE(last_index)]]
                + last_adjust;
                if (linked.verbose_flag) {
                cerr << " values range: "  
                     << cvt2string(op1) <<" " 
                     << first_i2 << " " 
                     << cvt2string(op2) <<" " 
                     << last_i2 <<endl;
                }
                if(t == test_u2 && first < low) {
                // can't create a smaller unsigned
                // number than 0
                expected = -2;
                }
                // If our random set covers the entire
                // range of small values, we can't
                // do this test. If it extends either to the 
                // highest small value or to the lowest
                // small value, we can't do this test.
                if(first_i2 >=
                values_i2[sorted[NORMALIZE(first_index)]]) {
                expected = -2;
                }
                if( last_i2 <=
                values_i2[sorted[NORMALIZE(last_index)]] ) {
                expected = -2;
                }
                break;

            case test_u4:
            case test_i4:
                first_i4 = first_adjust +
                values_i4[sorted[NORMALIZE(first_index)]];
                last_i4 = 
                values_i4[sorted[NORMALIZE(last_index)]]
                + last_adjust;
                if (linked.verbose_flag) {
                cerr << " values range: "  
                     << cvt2string(op1) <<" " 
                     << first_i4 << " " 
                     << cvt2string(op2) <<" " 
                     << last_i4 <<endl;
                }
                if(first < low && t == test_u4) {
                // can't create a smaller unsigned
                // number than 0
                expected = -2;
                }
                // If our random set covers the entire
                // range of values, we can't
                // do this test. If it extends either to the 
                // highest small value or to the lowest
                // small value, we can't do this test.
                if(first_i4 >=
                values_i4[sorted[NORMALIZE(first_index)]]) {
                expected = -2;
                }
                if( last_i4 <=
                values_i4[sorted[NORMALIZE(last_index)]] ) {
                expected = -2;
                }
                break;

            case test_u8:
            case test_i8:
                first_i8 = first_adjust +
                values_i8[sorted[NORMALIZE(first_index)]];
                last_i8 = 
                values_i8[sorted[NORMALIZE(last_index)]]
                + last_adjust;
                if (linked.verbose_flag) {
                cerr << " values range: "  
                     << cvt2string(op1) <<" " 
                     << first_i8 << " " 
                     << cvt2string(op2) <<" " 
                     << last_i8 <<endl;
                }
                if(first < low && t == test_u8) {
                // can't create a smaller unsigned
                // number than 0
                expected = -2;
                }
                // If our random set covers the entire
                // range of values, we can't
                // do this test. If it extends either to the 
                // highest small value or to the lowest
                // small value, we can't do this test.
                if(first_i8 >=
                values_i8[sorted[NORMALIZE(first_index)]]) {
                expected = -2;
                }
                if( last_i8 <=
                values_i8[sorted[NORMALIZE(last_index)]] ) {
                expected = -2;
                }
                break;

            case test_f4:
                first_f4 = first_adjust +
                    values_f4[sorted[NORMALIZE(first_index)]];
                last_f4 = values_f4[sorted[NORMALIZE(last_index)]]
                    + last_adjust;
                if (linked.verbose_flag) {
                cerr << " values range: "  
                     << cvt2string(op1) <<" " 
                     << first_f4 << " " 
                     << cvt2string(op2) <<" " 
                     << last_f4 <<endl;
                }
                // If our random set covers the entire
                // range of values, we can't
                // do this test. If it extends either to the 
                // highest value or to the lowest
                // value, we can't do this test.
                if(first_f4 >=
                values_f4[sorted[NORMALIZE(first_index)]]) {
                expected = -2;
                }
                if( last_f4 <=
                values_f4[sorted[NORMALIZE(last_index)]] ) {
                expected = -2;
                }
                break;

            case test_f8:
                first_f8 = first_adjust +
                    values_f8[sorted[NORMALIZE(first_index)]];
                last_f8 = values_f8[sorted[NORMALIZE(last_index)]]
                    + last_adjust;
                if (linked.verbose_flag) {
                cerr << " values range: "  
                     << cvt2string(op1) <<" " 
                     << first_f8 << " " 
                     << cvt2string(op2) <<" " 
                     << last_f8 <<endl;
                }
                // If our random set covers the entire
                // range of values, we can't
                // do this test. If it extends either to the 
                // highest value or to the lowest
                // value, we can't do this test.
                if(first_f8 >=
                values_f8[sorted[NORMALIZE(first_index)]]) {
                expected = -2;
                }
                if( last_f8 <=
                values_f8[sorted[NORMALIZE(last_index)]] ) {
                expected = -2;
                }
                break;

            case test_b1:
            case test_b23:
            case test_blarge:
            case test_bv:
                first_b = first_adjust ? "" :
                    values_b[sorted[NORMALIZE(first)]];
#ifdef W_DEBUG
                w_assert3(first_b);
                if(!first_adjust) w_assert3(isalnum(*first_b) );
#endif /* W_DEBUG */

                last_b = last_adjust? LASTSTRING :
                values_b[sorted[NORMALIZE(last)]];
                w_assert3(last_b);

                if(last > high) {
                if (linked.verbose_flag) {
                    cerr << " values range: "  
                     << cvt2string(op1) <<" " 
                     << first_b << " " 
                     << cvt2string(op2) <<" " 
                     << "<unprintable:last+1>" <<endl;
                }
                DBG(<< " values range: "  
                     << cvt2string(op1) <<" " 
                     << first_b << " " 
                     << cvt2string(op2) <<" " 
                     << "<unprintable:last+1>" );
                } else {
                if (linked.verbose_flag) {
                    cerr << " values range: "  
                     << cvt2string(op1) <<" " 
                     << first_b << " " 
                     << cvt2string(op2) <<" " 
                     << last_b <<endl;
                }
                DBG(<< " values range: "  
                     << cvt2string(op1) <<" " 
                     << first_b << " " 
                     << cvt2string(op2) <<" " 
                     << last_b );
                }
                /*
                 * see comment above, at VEC_XXXX
                 */
                if(first_adjust) {
                w_assert3(strlen(first_b) == 0);
                } else {
                w_assert3(strlen(first_b) > 0);
                }
                first_vec.reset().put(first_b, strlen(first_b)); 

                w_assert3(strlen(last_b) > 0);
                last_vec.reset().put(last_b, strlen(last_b)); 

                if(first < low && t == test_b1) {
                // can't create a smaller unsigned
                // number than 0
                expected = -2;
                }

                // Our random values are restricted to
                // nice printable ascii values, so 
                // we should not be covering the entire
                // range.
                break;

            case test_spatial:
            default: w_assert1(0);

            }
            } // close scope on {first,last}_index, {first,last}_adjust
            /*
             * Vectors have already been set up for the range definition.
             * Start the scan. 
             */

            char scan_space[sizeof(scan_index_i)];
            scan_index_i* scan = 0;
            {
            DBG(<<"stid=" << stid);
            scan = new (scan_space) scan_index_i(stid,
                      op1, first_vec, 
                      op2, last_vec, false, cc);
            }

            if(scan != 0 && scan->error_code().is_error()) {
            if(expected < 0 ) {
                // expected this error
                scan->~scan_index_i();
                continue;
            } else {
                // Bad scan_index_i YYYY
                cerr << " unexpected error: " << 
                scan->error_code() 
                << endl;
                w_assert3(0);
            }
            }

            /* PROLOGUE TO MAIN SCAN LOOP */

            bool eof=false;

            int num_scanned = 0;
            int tmp=first;
            {
            // compute starting place...
            if(first<low) {
                tmp = low;
            }
            if(op1 == scan_file_i::gt ) {
                while(duplicated[NORMALIZE(tmp)]) {
                DBG(<<"incrementing dup starting place " << tmp);
                tmp++;
                if(op2 != scan_file_i::eq ) {
                    expected--;
                }
                }
                DBG(<<"incrementing starting place because GT " << tmp);
                tmp++;
            }
            }

            // special case for strings:
            if(values_b) {
            key.reset().put(key_buffer, LARGEKEYSTRING);
            klen = LARGEKEYSTRING;
            }

            /* MAIN SCAN LOOP */
            for (i = tmp;
            (!(rc = scan->next(eof)).is_error() && !eof) ; 
            i++)  {

            /* clobber value holder */
            elemvalue=-99999;
            // element = elemvalue value, regardless of key type
            el.reset().put(&elemvalue, sizeof(elemvalue));  

            /* clobber key holders */
            i1 = 0; i2 = 0; i4 = 0; i8 = 0; f4 = -0.0; f8 = -0.0; 
            if(key_buffer) {
                memset(key_buffer, '\0', LARGEKEYSTRING);
            }

            smsize_t _klen, _elen;
            DO_GOTO( scan->curr(&key, _klen, &el, _elen));

            if(key_buffer) { w_assert3(_klen <= klen); }
            else { w_assert3(_klen == klen); }

            w_assert3(_elen == elen);

            if (linked.verbose_flag) {
                cerr << "retrieved item #" << i 
                << " element = " << elemvalue
                << " key = " ;

                switch(t) {
                case test_u1:
                case test_i1:
                cerr << (u_char) i1 << endl;
                break;

                case test_u2:
                case test_i2:
                cerr << i2 << endl;
                break;

                case test_u4:
                cerr << (unsigned) i4 << endl;
                break;
                case test_i4:
                cerr << i4 << endl;
                break;

                case test_u8:
                cerr << (w_base_t::uint8_t) i8 << endl;
                break;
                case test_i8:
                cerr << i8 << endl;
                break;

                case test_f4:
                cerr << f4 << endl;
                break;

                case test_f8:
                cerr << f8 << endl;
                break;

                case test_b1:
                case test_b23:
                case test_blarge:
                case test_bv:
                cerr << key_buffer << endl;
                break;

                case test_spatial:
                default: w_assert1(0);
                }
            }
            num_scanned++;

            /* 
             * check elemvalue
             */

            DBG(<<"i " << i << " n " << n
                <<" NORMALIZE(i) " << NORMALIZE(i)
                <<" elemvalue " << elemvalue
                );

    #define DONTCARE -999999
			int what=i;
			if(op2==scan_file_i::eq) {
			    int first_last = (last > high ? high : last) ;
			    while( first_last > low &&
				    duplicated[first_last-1]) {
				w_assert3(first_last >= 1);
				w_assert3(first_last <= high);
				first_last--;
			    }
			    if(what < first_last) what = first_last;
			    i = what;

			} else if(op1==scan_file_i::eq) {
			    // special case for duplicates
			    int last_first = first < low? low : first;
			    while(last_first < high &&
				    duplicated[last_first]) {
				last_first++;
			    }
			    if(what > last_first) what = last_first;

			    i = what;
			} else {
			    if(first >= low && last <= high) {
				w_assert3((NORMALIZE(i) >= 0) 
				    && (NORMALIZE(i) < n));
			    } else {
				if (linked.verbose_flag) {
				cerr<< bounds_set
				<< "POSSIBLE ERROR: Special case not handled," 
				<< " i=" << i
				<< " low=" << low
				<< " high=" << high
				<<endl;
				cerr << "IN SCAN# " <<bounds_set 
				 << " scanning " 
				 << cvt2string(op1) <<" " << first 
				 << " " 
				 << cvt2string(op2) <<" " << last
				 << endl;
				}
				special();
				w_assert1(bounds_set >= 14);
				what = DONTCARE;
			    }
			}
			/*
			 * NB: for the time being, we're assuming
			 * that the btree sort is stable.  If we
			 * don't care if it's stable, do this here:
			 sort_is_stable = false;
			 */
		 sort_is_stable = false;
		// w_assert1(sort_is_stable);
			if(what != DONTCARE && expected != -2) {
			    if (elemvalue != NORMALIZE(what)) {
				if(sort_is_stable) {
		    cerr << "WARNING!****BTREE SORT IS NOT STABLE" <<endl;
				} else {
				    // it's a duplicate
				    // elemvalue must match ONE of the
				    // duplicates
				    int f = what;
				    if(!duplicated[f]) f--;
				    while(duplicated[f]) f--;
				    f++;

				    w_assert1(duplicated[f]);
				    bool found=false;
				    while(duplicated[f]) {
					if(elemvalue == NORMALIZE(f)) {
					    DBG(<<"elemvalue found at " << f);
					    found = true;
					    break;
					}
					f++;
				    }
				    if(elemvalue == NORMALIZE(f)) {
					DBG(<<"elemvalue found at " << f);
					found = true;
				    }
				    // Either we found what
				    // we're looking for, or the
				    // test is possibly bogus because
				    // the range of values.
				    w_assert1(found); 
				}
			    }
			}


			/*
			 * check key value
			 */
			{
			int len=0;
			switch(t) {
			    case test_i1:
			    case test_u1:
			    DBG(<<"i1=" <<(u_char) i1
				<< " expect " << values_i1[sorted[elemvalue]]);
			    w_assert1(i1 == values_i1[sorted[elemvalue]]);
			    break;

			    case test_i2:
			    case test_u2:
			    DBG(<<"i2=" <<i2
				<< " expect " << values_i2[sorted[elemvalue]]);
			    w_assert1(i2 == values_i2[sorted[elemvalue]]);
			    break;

			    case test_u4:
			    DBG(<<"i4=" <<(unsigned) i4
				<< " expect " 
				<< values_u4[sorted[elemvalue]]);
			    w_assert1(i4 == values_i4[sorted[elemvalue]]);
			    break;

			    case test_i4:
			    DBG(<<"i4=" <<i4
				<< " expect " 
				<< values_i4[sorted[elemvalue]]);
			    w_assert1(i4 == values_i4[sorted[elemvalue]]);
			    break;

			    case test_u8:
			    DBG(<<"i8=" << (w_base_t::uint8_t) i8
				<< " expect " 
				<< values_u8[sorted[elemvalue]]);
			    w_assert1(i8 == values_i8[sorted[elemvalue]]);
			    break;

			    case test_i8:
			    DBG(<<"i8=" <<i8
				<< " expect " 
				<< values_i8[sorted[elemvalue]]);
			    w_assert1(i8 == values_i8[sorted[elemvalue]]);
			    break;

			    case test_f4:
			    DBG(<<"f4=" <<f4
				<< " expect " << values_f4[sorted[elemvalue]]);
			    w_assert1(f4 == values_f4[sorted[elemvalue]]);
			    break;

			    case test_f8:
			    DBG(<<"f8=" <<f8
				<< " expect " << values_f8[sorted[elemvalue]]);
			    w_assert1(f8 == values_f8[sorted[elemvalue]]);
			    break;

			    case test_b1:
				len=1;
			    case test_b23:
				if(!len) len=23;
			    case test_blarge:
			    case test_bv:
				if(!len) len=LARGEKEYSTRING;
			    DBG(<<"b=" << key_buffer
				<< " expect " << values_b[sorted[elemvalue]]);
			    break;

			    case test_spatial:
			    default: w_assert1(0);
			}
			} // close scope of len
		    }

		    /*
		     * EPILOGUE TO MAIN SCAN LOOP 
		     * check ending conditions
		     */
		    DBG(<<" rc= " << rc << " eof=" << eof);
		    scan->finish();
		    DO_GOTO( rc );

		    if(expected>0) {
			int boundary ;
			bool do_incr=false;
			if(op1==scan_file_i::eq) {
			    boundary = first;
			    // boundary should be the last of the duplicates
			    do_incr = true;
			} else if(op2==scan_file_i::eq) {
			    boundary = last;
			    // boundary should be the last of the duplicates
			    do_incr = true;
			} else if(op2==scan_file_i::lt) {
			    boundary = (last>high)?high:last-1;
			    // boundary should be last of the duplicates
			    // of the value < value(last).  We find
			    // that by decrementing from last until
			    // we get to the first value < value(last).
			} else {
			    w_assert3(op2==scan_file_i::le);
			    // op2 == scan_file_i::le
			    // boundary should be last of the duplicates
			    // of high/last. 
			    boundary = (last>high)?high:last;
			    do_incr = true;
			}
			if(do_incr) {
			    // We'll reach !duplicated at that last
			    // copy of the value
			    while(duplicated[NORMALIZE(boundary)]) {
				DBG(<<"incrementing dup boundary " 
				    << boundary);
				boundary++;
				expected++;
			    }
			} else {
			    // We'll reach !duplicated at the last
			    // copy of the previous value
			    while(duplicated[NORMALIZE(boundary)]
				    && NORMALIZE(boundary) > 0) {
				DBG(<<"decrementing dup boundary " 
				    << boundary);
				expected--;
				boundary--;
			    }
			}

			// i is incremented..., so it should
			// end up at 1 after the so-far computed boundary
			// so we'll now increment boundary:
			boundary++; 

			DBG(<<"boundary ends up at " << boundary);

			if( i < boundary) {
			    if (linked.verbose_flag) {
			    cerr << "POSSIBLE PREMATURE EOF : eof at " << i
				<< " expected eof at " << boundary << endl;
			    cerr << "IN SCAN# " <<bounds_set 
				 << " scanning " 
				 << cvt2string(op1) <<" " << first 
				 << " " 
				 << cvt2string(op2) <<" " << last
				 << endl;
			    }
			    possible();
			}
			// w_assert1(i == boundary);
		    }

		    if( expected >= 0 && num_scanned != expected ) {
			if (linked.verbose_flag) {
			cerr 
			    << (char *)(duplicates? "POSSIBLE " : "")
			    << "WRONG NUMBER : expected " 
			    << expected
			    << " got " << num_scanned 
			    << " #duplicates= " << duplicates
			    << endl;
			cerr << "IN SCAN# " <<bounds_set 
			     << " scanning " 
			     << cvt2string(op1) <<" " << first 
			     << " " 
			     << cvt2string(op2) <<" " << last
			     << endl;
			}
			possible();
		    }

		    // It's not yet accurate in the presence of duplicates
		    if(expected == 0 || 
			    (duplicates == 0 && expected >= 0)) {
			w_assert1(num_scanned == expected);
		    }

		    scan->~scan_index_i();
		}
	    }
		// DO( sm->print_index(stid) );

	    {
		DBG(<<"destroying store " << stid);
		DO_GOTO( sm->destroy_index(stid) );
	    }
	}
	if(values_i1) { delete[] values_i1; values_i1=0; values_u1=0; }
	if(values_i2) { delete[] values_i2; values_i2=0; values_u2=0; }
	if(values_i4) { delete [] values_i4; values_i4=0; values_u4=0; }
	if(values_i8) { delete [] values_i8; values_i8=0; values_u8=0; }
	if(values_f4) { delete[] values_f4; values_f4=0; }
	if(values_f8) { delete[] values_f8; values_f8=0; }
	if(values_b) { 
	    int k;
	    for(k=0; k < n; k++) {
		if(values_b[k]) delete[] values_b[k];
	    }
	    delete[] values_b; values_b=0; 
	    delete[] key_buffer;
	}
	return TCL_OK;
    }
failure:
    DBG(<<"rc=" <<rc);
    if(values_i1) { delete[] values_i1; values_i1=0; values_u1=0; }
    if(values_i2) { delete[] values_i2; values_i2=0; values_u2=0; }
    if(values_i4) { delete [] values_i4; values_i4=0; values_u4=0; }
    if(values_i8) { delete [] values_i8; values_i8=0; values_u8=0; }
    if(values_f4) { delete[] values_f4; values_f4=0; }
    if(values_f8) { delete[] values_f8; values_f8=0; }
    if(values_b) { 
    int k;
    for(k=0; k < n; k++) {
        if(values_b[k]) delete[] values_b[k];
    }
    delete[] values_b; values_b=0; 
    delete[] key_buffer;
    }
    return TCL_ERROR;
}

int
t_test_int_btree(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "vid nkeys", ac, 3))
    return TCL_ERROR;
    const int n = atoi(av[2]);
    int vid = 0;
    {
	vid = atoi(av[1]);
    }
    int e;
    e= _t_test_typed_btree(ip, vid, 
	    n, "i4", 0);
    // if( e != TCL_OK) { return e; }

    // e= _t_test_typed_btree(ip, vid, lstid, n, "i2", 0);
    // if( e != TCL_OK) { return e; }

    // e= _t_test_typed_btree(ip, vid, lstid, n, "i1", 0);
    if( e != TCL_OK) { return e; }

    return e;
}

int
t_test_typed_btree(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "vid nkeys type cc", ac, 4, 5))
    return TCL_ERROR;
    const int n = atoi(av[2]);
    int vid = 0;
    {
	vid = atoi(av[1]);
    }
    return _t_test_typed_btree(ip, vid, 
        n, av[3], av[4]);
}

int
t_create_typed_hdr_body_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    // Same as create_typed_hdr_rec except that we interpret both
    // the header and the data as typed values 
    // We give a data type for the header and another for the body.
    //
    //                 1 2   3    4        5    6    7
    if (check(ip, "fid hdr type len_hint data type [universe]", ac, 7, 8))
    return TCL_ERROR;

    int fid_arg = 1;
    int hdr_arg = 2;
    int hdr_type_arg = 3;
    int len_hint_arg = 4;
    int body_arg = 5;
    int body_type_arg = 6;
    int universe_arg = 7;
    typed_btree_test t;
    vec_t hdr, data;
    typed_value vh, vd;
    nbox_t box;

    const char *keydescr = check_compress_flag(av[hdr_type_arg]);
    t = cvt2type(keydescr);

    switch(t) {
    case test_spatial:
        box.put(av[hdr_arg]);
        if(newsort) {
            hdr.put(box.kval(), box.klen()); // just the box
        } else {
            nbox_t u(hdr_arg);
            if(ac > universe_arg) {
            u.put(av[universe_arg]);
            int hval = box.hvalue(u);
            hdr.put(&hval, sizeof(hval));
            } else {
                hdr.put(box.kval(), box.klen()); // just the box
            }
        }
        break;

    case test_bv:
        // include null terminator
        hdr.put(av[hdr_arg], strlen(av[hdr_arg])+1);
        break;

    case test_blarge:
        cerr << "Cannot use blarge test for hdrs"<<endl;
        W_FATAL(ss_m::eNOTIMPLEMENTED);
        break;

    default:
    cvt2typed_value(t, av[hdr_arg], vh);
    hdr.put(&vh._u, vh._length);
    break;
    }

    nbox_t box2;

    char *strbuf = new char[STRINGMAX];
    w_auto_delete_array_t<char> autodel(strbuf);

    t = cvt2type(av[body_type_arg]);
    switch(t) {
    case test_spatial: 
    box2.put(av[body_arg]);
        data.put(box2.kval(), box2.klen()); // just the box
        break;

    case test_blarge: {
    // fall through
    size_t s = strlen(av[body_arg]); 
    s++;
    // Copy the key to the END of the string
    // to make sure it ends up not at the
    // very beginning of the record, thereby
    // defeating this test.
        memset(strbuf, 'Z', LARGESTRING);
        memcpy((strbuf+LARGESTRING)-s, av[body_arg], s);
    data.put(strbuf, LARGESTRING);
    }
    break;

    case test_b23:
    case test_bv:
    // include null terminator
    data.put(av[body_arg], strlen(av[body_arg])+1);
    break;

    default:
    cvt2typed_value(t, av[body_arg], vd);
    data.put(&vd._u, vd._length);
    break;
    }
    
    {
	stid_t  stid;
	rid_t   rid;

    w_istrstream anon(av[fid_arg]); 
        anon >> stid;
    DO( sm->create_rec(stid, hdr, 
        objectsize(av[len_hint_arg]), data, rid) );
    w_reset_strstream(tclout);
    tclout << rid << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

int
t_create_typed_hdr_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    // NB: type refers to what goes into the header, NOT the data
    // The data are just taken as a string
    //                 1 2   3        4    5    6
    if (check(ip, "fid hdr len_hint data type [universe]", ac, 6, 7))
    return TCL_ERROR;
    int fid_arg = 1;
    int hdr_arg = 2;
    int len_hint_arg = 3;
    int body_arg = 4;
    int body_type_arg = 5;
    int universe_arg = 6;

    
    vec_t hdr, data;

    // include null terminator
    data.put(av[body_arg], strlen(av[body_arg])+1);

    typed_value vh;
    nbox_t box;
    
    typed_btree_test t = cvt2type(av[body_type_arg]);

    switch(t) {
    case test_spatial: 
    box.put(av[hdr_arg]);
    if(newsort) {
        hdr.put(box.kval(), box.klen()); // just the box
    } else {
        nbox_t u(hdr_arg);
        if(ac > universe_arg) {
        u.put(av[universe_arg]);
        int hval = box.hvalue(u);
        hdr.put(&hval, sizeof(hval));
        } else {
            hdr.put(box.kval(), box.klen()); // just the box
        }
    }
        break;

    case test_bv:
    // include null terminator for printing purposes
    hdr.put(av[hdr_arg], strlen(av[hdr_arg])+1);
    break;

    case test_blarge:
    cerr << "******* SCRIPT ERROR ******" <<endl;
    cerr << "Cannot use blarge test for hdrs"<<endl;
    cerr << "***************************" <<endl;
    cerr << endl;
    cerr << endl;
        W_FATAL(ss_m::eNOTIMPLEMENTED);
    break;

    default:
	cvt2typed_value(t, av[hdr_arg], vh);
	hdr.put(&vh._u, vh._length);
	break;
    }

    {
	stid_t  stid;
	rid_t   rid;

    w_istrstream anon(av[fid_arg]); anon >> stid;
    DO( sm->create_rec(stid, hdr, 
        objectsize(av[len_hint_arg]), data, rid) );
    w_reset_strstream(tclout);
    tclout << rid << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

int
t_get_store_info(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "fid ", ac, 2))
    return TCL_ERROR;

    const char *stidstring = av[1];
    // explicitly conert it here so we can print the result

    sm_store_info_t info(100);
    {
	stid_t  fid;
	w_istrstream anon(stidstring); anon >> fid;
	DO(sm->get_store_info(fid, info));
    }
    w_reset_strstream(tclout);
    tclout 
	<< info.store << " "
	<< cvt_store_t((ss_m::store_t)info.stype) << " "
	<< cvt_ndx_t((ss_m::ndx_t)info.ntype) << " "
	<< cvt_concurrency_t((ss_m::concurrency_t)info.cc) << " "
	<< info.eff << " "
	<< info.large_store << " "
	<< info.root << " "
	<< info.nkc << " "
	<< info.keydescr << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

typed_btree_test 
get_key_type( Tcl_Interp *
	, const char *stidstring )
{
    typed_btree_test tstored;
    w_rc_t rc;
    sm_store_info_t info(100);
    {
    stid_t  fid;
    w_istrstream anon(stidstring); anon >> fid;
    rc = sm->get_store_info(fid, info);
    }
    if (rc.is_error())  {
    cerr << "get_key_type: " << rc << endl;
    return test_nosuch;
    }
    tstored  = cvt2type(info.keydescr);
    return tstored;
}


int 
check_key_type(
    Tcl_Interp *ip,
    typed_btree_test t,
    const char *given,
    const char *stidstring
)
{
    typed_btree_test stored;
    if((stored=get_key_type(ip, stidstring)) != t) {
    Tcl_AppendResult(ip, "Wrong key type: given ",
        given, " index matches typed_btree_test  " , stored , 0);
    return TCL_ERROR;
    }
    return TCL_OK;
}

int
t_scan_sorted_recs(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    //              1    2     3        4     
    if (check(ip, "fid hdrtype datatype [universe]", ac, 4, 5))
    return TCL_ERROR;
    

    int     universe_arg = 4;
    pin_i*      pin;
    bool      eof;
    rc_t        rc;
    scan_file_i* scan;

    typed_btree_test th = cvt2type(av[2]);
    typed_btree_test tb = cvt2type(av[3]);

    {
	stid_t  fid;
	w_istrstream anon(av[1]); anon >> fid;
	scan = new scan_file_i(fid, ss_m::t_cc_file);
    }
    TCL_HANDLE_FSCAN_FAILURE(scan);

    char *strbuf = new char[STRINGMAX];
    w_auto_delete_array_t<char> autodel(strbuf);

    while ( !(rc=scan->next(pin, 0, eof)).is_error() && !eof) {
    DBG(<<" record " << pin->rid());

    w_reset_strstream(tclout);
    switch(th) {
    case  test_bv:
        tclout << "(" << (char*)pin->hdr() << ")";
        break;

    case  test_b1:
        tclout << "(" << *(char*)pin->hdr() << ")";
        break;

    case  test_i1:
        tclout << "(" << (int)(*(int1_t*)pin->hdr()) << ")";
        break;
    case  test_u1:
        tclout << "(" << (unsigned int)(*(uint1_t*)pin->hdr()) << ")";
        break;

    case  test_i2:
        tclout << "(" << *(int2_t*)pin->hdr() << ")";
        break;
    case  test_u2:
        tclout << "(" << *(uint2_t*)pin->hdr() << ")";
        break;

    case  test_i4:
        tclout << "(" << *(int4_t*)pin->hdr() << ")";
        break;
    case  test_u4:
        tclout << "(" << *(uint4_t*)pin->hdr() << ")";
        break;

    case  test_i8:
        tclout << "(" << *(w_base_t::int8_t*)pin->hdr() << ")";
        break;
    case  test_u8:
        tclout << "(" << *(w_base_t::uint8_t*)pin->hdr() << ")";
        break;

    case  test_f8: {
        w_base_t::f8_t tmp;
        memcpy(&tmp, pin->hdr(), sizeof(w_base_t::f8_t));
        tclout << "(" << tmp << ")";
        }
        break;

    case  test_f4:
        tclout << "(" << (f4_t) *(f4_t*)pin->hdr() << ")";
        break;

    case test_spatial:  {
        if(newsort) {
        int hvalue; 

        nbox_t key(2);
        nbox_t u(2);
        u.put(av[universe_arg]);
        key.bytes2box((char*)pin->hdr(), pin->hdr_size());
        hvalue = key.hvalue(u);
        tclout << "(hilbert=" << hvalue << "):" << key ;
                 
        } else {
        int hvalue; 
        w_assert3(pin->hdr_size() == sizeof(hvalue));
        memcpy(&hvalue, pin->hdr(), pin->hdr_size());
        tclout << "(" << hvalue  << ")";
        }
        }
        break;

    default:
        W_FATAL(ss_m::eNOTIMPLEMENTED);
        break;
    }

    // get type of body

    memset(strbuf, 'Z', STRINGMAX);

    if (pin->is_small()) {
        memcpy(strbuf, pin->body(), pin->body_size());
            strbuf[pin->body_size()] = '\0';
    } else if(tb != test_blarge) {
        memcpy(strbuf, pin->body(), 96);
        memcpy(&strbuf[96], "...", 3);
        strbuf[99] = '\0';
    }
    switch(tb) {
    case  test_blarge: {
        int i=0;
        char *p = strbuf;
        while(*p == 'Z') {
            i++;
            p++; 
            if(i>int(pin->body_size())) {
            *p = '\0';
            break;
            }
        }
        w_assert3(pin->body_size() < STRINGMAX );
        strbuf[pin->body_size()]= '\0';

            tclout << "(Z(" << (p-strbuf) << " times) "
            << (char*)p << ")";
        }
        break;

    case  test_b23:
    case  test_bv:
        tclout << "(" << (char*)strbuf << ")";
        break;

    case  test_b1:
        tclout << "(" << *(char*)strbuf << ")";
        break;

    case  test_i1:
        tclout << "(" << (int)(*(uint1_t*)strbuf) << ")";
        break;
    case  test_u1:
        tclout << "(" << (unsigned int)(*(uint1_t*)strbuf) << ")";
        break;

    case  test_i2:
        tclout << "(" << *(int2_t*)strbuf << ")";
        break;
    case  test_u2:
        tclout << "(" << *(uint2_t*)strbuf << ")";
        break;

    case  test_i4:
        tclout << "(" << *(int4_t*)strbuf << ")";
        break;
    case  test_u4:
        tclout << "(" << *(uint4_t*)strbuf << ")";
        break;

    case  test_i8:
        tclout << "(" << *(w_base_t::int8_t*)strbuf << ")";
        break;
    case  test_u8:
        tclout << "(" << *(w_base_t::uint8_t*)strbuf << ")";
        break;

    case  test_f8:
        tclout << "(" << *(double*)strbuf << ")";
        break;

    case  test_f4:
        tclout << "(" << *(float*)strbuf << ")";
        break;

    case test_spatial:
        W_FATAL(ss_m::eNOTIMPLEMENTED);
        break;

    default:
        W_FATAL(ss_m::eNOTIMPLEMENTED);
        break;
    }
    tclout << ends;
    if (linked.verbose_flag) {
        cout << tclout.c_str() <<endl;
    }
    } //scan

    delete scan;

    DO(rc);

    Tcl_AppendElement(ip, TCL_CVBUG "scan success");
    return TCL_OK;
}


int
t_sort_file(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    const int fid_arg =1;
    const int vid_arg =2;
    const int runsize_arg =3;
    const int type_arg =4;
    const int where_arg=5;
    const int distinct_arg =6;
    const int destruct_arg=7;
    const int property_arg=8;
    const int universe_arg=9;
    const int derived_arg=10;
    const char *usage_string = 
    //1  2   3       4    5              6                   7            
    "fid vid runsize type \"body|hdr\"   \"distinct|normal\" \"destruct|keep\" [\"tmp|regular|load_file\" [universe [derived]]]";
//8                       9        10

    if (check(ip, usage_string, ac, destruct_arg+1, property_arg+1, 
    universe_arg+1, derived_arg+1)) 
    return TCL_ERROR;
    
    key_info_t info;

    info.offset = 0;
    info.where = key_info_t::t_body;
    info.universe = 0;    /* XXX reset or something ??? */

    bool unique = false;
    if (strcmp("normal", av[distinct_arg]))  {
    if (strcmp("distinct", av[distinct_arg]))  {
            if(check(ip, usage_string, ac, 0)) return TCL_ERROR;
        } else {
            unique = true;
        }
    }
    
    bool destruct = true; 
    if (strcmp("destruct", av[destruct_arg]))  {
    if (strcmp("keep", av[destruct_arg]))  {
            if(check(ip, usage_string, ac, 0)) return TCL_ERROR;
        } else {
            destruct = false;
        }
    }
    
    if (ac > derived_arg) {
    info.derived = (atoi(av[derived_arg]) != 0);
    } else {
    info.derived = false;
    }

    ss_m::store_property_t property = ss_m::t_regular;
    if (ac > property_arg) {
    property = cvt2store_property(av[property_arg]);
    }

    if (strcmp("hdr", av[where_arg]) == 0)  {
    info.where = key_info_t::t_hdr;
    } else if (strcmp("body", av[where_arg]) ==0)  {
    info.where = key_info_t::t_body;
    } else {
    if(check(ip, usage_string, ac, 0)) return TCL_ERROR;
    }

    typed_btree_test t = cvt2type(av[type_arg]);

    nbox_t bbox(2);
    switch(t) {
    case test_bv:
        // info.type = key_info_t::t_string;
        info.type = sortorder::kt_b;
    info.len = 0;
    break;

    case test_blarge:
        info.type = sortorder::kt_b;
    info.len = LARGESTRING;
    break;

    case test_b23:
        info.type = sortorder::kt_b;
    info.len = 23;
    break;

    case test_b1:
        info.type = sortorder::kt_u1;
    info.len = sizeof(char);
    break;

    case test_u1:
        // info.type = key_info_t::t_char;
        info.type = sortorder::kt_u1;
    info.len = sizeof(uint1_t);
    break;

    case test_u2:
        // info.type = key_info_t::t_char;
        info.type = sortorder::kt_u2;
    info.len = sizeof(uint2_t);
    break;

    case test_u4:
        // info.type = key_info_t::t_char;
        info.type = sortorder::kt_u4;
    info.len = sizeof(uint4_t);
    break;

    case test_u8:
        // info.type = key_info_t::t_char;
        info.type = sortorder::kt_u8;
    info.len = sizeof(w_base_t::uint8_t);
    break;

    default:
    case test_i1:
        // info.type = key_info_t::t_int;
        info.type = sortorder::kt_i1;
    info.len = sizeof(int1_t);
    break;

    case test_i2:
        // info.type = key_info_t::t_int;
        info.type = sortorder::kt_i2;
    info.len = sizeof(int2_t);
    break;

    case test_i4:
        // info.type = key_info_t::t_int;
        info.type = sortorder::kt_i4;
    info.len = sizeof(int4_t);
    break;

    case test_i8:
        // info.type = key_info_t::t_int;
        info.type = sortorder::kt_i8;
    info.len = sizeof(w_base_t::int8_t);
    break;

    case test_f4:
        // info.type = key_info_t::t_float;
        info.type = sortorder::kt_f4;
    info.len = sizeof(f4_t);
    break;

    case test_f8:
        // info.type = key_info_t::t_float;
        info.type = sortorder::kt_f8;
    info.len = sizeof(f8_t);
    break;

    case test_spatial:
        // info.type = key_info_t::t_spatial;
        info.type = sortorder::kt_spatial;

    if(ac > universe_arg) {
        bbox.put(av[universe_arg]);
    }
    info.universe = bbox;
    info.len = bbox.klen();
    break;
    }

    /* XXX Record Alignment not memory alignment.
       The alignon makes the estimate match the actual minimum
       size the record will actually be.   Probably the lower level
       code in the SM should do something equivalent for space
       estimation.   However, this is here. */
    info.est_reclen = MAX(info.len, ALIGNON);

    {
	stid_t in_fid;
	w_istrstream anon(av[fid_arg]); 
		anon >> in_fid;
	stid_t out_fid;

	DO( sm->sort_file(in_fid, 
	      atoi(av[vid_arg]), out_fid, property,
	      info, atoi(av[runsize_arg]), true, unique, destruct,
	      newsort) );
	w_reset_strstream(tclout);
	tclout << out_fid << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

int
t_compare_typed(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    // NB: type refers to what goes into the header, NOT the data
    //                 1   2     3        4    
    if (check(ip, "type datum datum [universe]", ac, 4, 5))
    return TCL_ERROR;

    typed_btree_test t = cvt2type(av[1]);
    typed_value v1, v2;
    vec_t d1, d2;
    int hval1, hval2;
    

    if(t == test_spatial) {
    nbox_t box1(av[2]);
    nbox_t box2(av[3]);
    if (ac == 5) {
        nbox_t u(av[4]);
        hval1 = box1.hvalue(u);
    cerr << __LINE__ << " " << "put hvalue " << hval1 <<endl;
            d1.put(&hval1, sizeof(hval1));

        hval2 = box2.hvalue(u);
    cerr << __LINE__ << " " << "put hvalue " << hval2 <<endl;
            d2.put(&hval2, sizeof(hval2));
    } else {
            d1.put(box1.kval(), box1.klen());
            d2.put(box2.kval(), box2.klen());
        }
    } else {
    cvt2typed_value(t, av[2], v1);
    d1.put(&v1._u, v1._length);

    cvt2typed_value(t, av[3], v2);
    d2.put(&v2._u, v2._length);
    }

    w_reset_strstream(tclout);
    tclout << (int) d1.cmp(d2) << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

int
t_find_assoc_typed(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid key type", ac, 4))
    return TCL_ERROR;
    typed_btree_test t = cvt2type(av[3]);

    if(check_key_type(ip, t, av[3], av[1]) == TCL_ERROR) return TCL_ERROR;

    typed_value v1;
    vec_t key;

    if(t == test_spatial) {
        // use md_index functions
        W_FATAL(ss_m::eNOTIMPLEMENTED);
    } else if(t == test_bv) {
        // include null terminator for printing purposes
        key.put(av[2], strlen(av[2])+1);
    } else {
        cvt2typed_value(t, av[2], v1);
        key.put(&v1._u, v1._length);
    }

    bool found;
    
#define ELSIZE 4044
    char *el = new char[ELSIZE];
    smsize_t elen = ELSIZE-1;

    w_rc_t ___rc;

    {
        stid_t stid;
    w_istrstream anon(av[1]); anon >> stid;
    ___rc = sm->find_assoc(stid, key, el, elen, found);
    }
    if (___rc.is_error())  {
    w_reset_strstream(tclout);
    tclout << ___rc << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    delete[] el;
    return TCL_ERROR;
    }

    el[elen] = '\0';
    if (found) {
    Tcl_AppendElement(ip, el);
    delete[] el;
    return TCL_OK;
    } 

    Tcl_AppendElement(ip, TCL_CVBUG "not found");
    delete[] el;
    return TCL_ERROR;
}

static const struct name2type_t {
    const char*         name;
    typed_btree_test     type;
} name2type[] = {
    { "i1",    test_i1 },
    { "I1",    test_i1 },
    { "u1",    test_u1 },
    { "U1",    test_u1 },

    { "i2",    test_i2 },
    { "I2",    test_i2 },
    { "u2",    test_u2 },
    { "U2",    test_u2 },

    { "i4", test_i4 },
    { "I4", test_i4 },
    { "u4", test_u4 },
    { "U4", test_u4 },

    { "i8", test_i8 },
    { "I8", test_i8 },
    { "u8", test_u8 },
    { "U8", test_u8 },

    { "f4",    test_f4 },
    { "F4",    test_f4 },
    { "f8",    test_f8 },
    { "F8",    test_f8 },

    { "b1",    test_b1 },
    { "B1",    test_b1 },
    { "b23",test_b23 },
    { "B23",test_b23 },

    { "blarge",test_blarge },
    { "Blarge",test_blarge },


    { "b*1000",test_bv },
    { "B*1000",test_bv },

/* for rtree smsh scripts */
    { "spatial",  test_spatial },

    { 0,    test_nosuch }
};

typed_btree_test
cvt2type(const char *s)
{
    for (const name2type_t* p = name2type; p->name; p++)  {
    if (streq(s, p->name))  return p->type;
    }
    return test_nosuch;
}

const char *
cvtFROMtype( typed_btree_test t)
{
    for (const name2type_t* p = name2type; p->name; p++)  {
		if (p->type == t)  return p->name;
    }
    return "Unknown";
}
