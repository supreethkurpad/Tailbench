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

 $Id: lexify.cpp,v 1.35.2.5 2010/02/05 20:39:45 nhall Exp $

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

/* Routines for translating integers and floating point numbers
 * into a form that allows lexicographic
 * comparison in an architecturally-neutral form.
 *
 * Original work for IEEE double-precision values by
 *     Marvin Solomon (solomon@cs.wisc.edu) Feb, 1997.
 * Extended for integer and IEEE single-precision values by
 *     Nancy Hall Feb, 1997.
 *
 */

#undef BIGFLOAT
#undef BIGFLOAT_VERIFY

#undef BIGDOUBLE
#undef BIGDOUBLE_VERIFY

#ifndef __GNUC__
#define __attribute__(x)
#endif

/* Compile-time options:
 *  BIGDOUBLE declares that doubles are stored "big-endian" -- that is,
 *    byte 0 contains the sign and high-order part of exponent, etc., and
 *    the the mantissa is stored with decreasingly significant parts
 *    at increasing addresses.
 *  BIGLONG declares that longs are big-endian -- that is,
 *    most-significant-byte first.
 * These options need only be specified to get more efficient code tailored
 * to a particular platform.  If they are specified in error, the code will
 * not work.  If they are not specified, the code should work fine on all
 * platforms, albeit sub-optimally.
 *
 * So far as I know, every BIGLONG platform is also BIGDOUBLE and vice versa,
 * but they are logically independent properties.  For example, little-endian
 * platforms (longs stored least-significan-byte first) use a variety of
 * byte-orderings for doubles.  Thus it would perhaps be worthwhile to
 * have a few alternatives to BIGDOUBLE.
 *
 * Most of this code should work for any floating-point representation I know
 * of, although it has only been tested for IEEE-488.  The assumptions on which
 * it depends are
 *    size = 8 bytes
 *    sign/magnitude representation of negative value, with the sign bit
 *        (0=pos, 1=neg) as the high bit.
 *    positive doubles compare lexicographically (bitwise).
 * The magic constant in byteorder, however, is specific to the IEEE format.
 *
 */

#define SM_SOURCE
#define BTREE_C

#ifdef __GNUG__
#   pragma implementation "lexify.h"
#endif

#include "sm_int_0.h"
#include "lexify.h"

/* XXX alignment is currently a mess.   This just puts the controls
 * for everything into one place so it can be tweaked easily.  
 *
 * The code aligned doubles on sparc to their natural size, but allowed
 * x4 alignment on other architectures -- which is actually wrong since
 * it slows down doubles on those platforms.
 *
 * [iu]8 was aligned to 4 bytes, when really it should be aligned to 8
 * bytes due to it's natural size.
 *
 * All this has an interaction with alignment of things in pages too,
 * which is another bizarre interaction I don't even what to get into
 * now.
 *
 * If page/record alignment is changed to 8 bytes, strict alignment can
 * be used for all architectures.
 */

/* XXX random 4 byte alignments on i386 screw this up */
#if (!defined(I386) && (ALIGNON == 0x8)) || defined(ARCH_LP64)
#define    STRICT_INT8_ALIGNMENT
#define    STRICT_F8_ALIGNMENT
#elif defined(Sparc) || defined(Snake)
/* This is a bogus mixed-mode alignment which should go?? */
#define    STRICT_F8_ALIGNMENT
#endif

#ifdef STRICT_INT8_ALIGNMENT
#define    _ALIGN_IU8    0x8
#else
#define    _ALIGN_IU8    0x4
#endif

#ifdef STRICT_F8_ALIGNMENT
#define    _ALIGN_F8    0x8
#else
#define    _ALIGN_F8    0x4
#endif

#define    ALIGN_MASK_F8    (_ALIGN_F8-1)
#define    ALIGN_MASK_F4    (0x4-1)

#define    ALIGN_MASK_IU8    (_ALIGN_IU8-1)
#define    ALIGN_MASK_IU4    (0x4-1)
#define    ALIGN_MASK_IU2    (0x2-1)


#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_array_t<sortorder::uint1_t>;
#endif

sortorder SortOrder;

NORET
sortorder::sortorder()
{
    Ibyteorder(I8perm, 8);
    Ibyteorder(I4perm, 4);
    Ibyteorder(I2perm, 2);
    Ibyteorder(I1perm, 1);

    Fbyteorder(Fperm);
    Dbyteorder(Dperm);
}

NORET
sortorder::~sortorder()
{
}

/* Discover the byte order of the current machine and store in permutation
 * (which should be an array of length 8) a permutation for converting to/from
 * "standard" (big-endian) order.
 *
 * The idea behind this code is nice, but to fully take advantage
 * of it, a magic number needs to be constructed for each size
 * of integer that will generate a perumtation.  The reason for
 * this is machines with "twisted" byte orders, where the
 * byte order of larger integers does not match the byte order
 * of smaller integers.  If that is the case, this style
 * of approach is really nice.  Short of that, the big/little
 * byte order convention suffices -- Bolo
 */

void 
sortorder::Ibyteorder(int permutation[4]) 
{
    /* The following magic constant has the representation
     * 0x3f404142 on a BIGLONG machine.
     */
    int magic = 0x3f404142;
    u_char *p = (u_char *)&magic;
    int i;
    for (i=0;i<4;i++)
        permutation[i] = p[i] - 0x3f;
#ifdef BIGLONG
    /* verify that the BIGLONG assertion is correct */
    for (i=0;i<4;i++) w_assert1(permutation[i] == i);

   w_assert3(w_base_t::is_big_endian()); 
#else
#if W_DEBUG_LEVEL > 2
    // Make sure lexify agrees with w_base_t
    if(permutation[1] == 1) {
       w_assert3(w_base_t::is_big_endian()); 
    } else {
       w_assert3(w_base_t::is_little_endian()); 
    }
#endif 
#endif
}

/*
 * Generate a reordering permuatation for an integer of length size
 * to convert from big endian to small endian and back again.
 *
 * This will not work correctly on systems with "twisted" byte
 * orders where different types are in a different byte order.
 * On the other hand, such a system breaks numerous other assumptions
 * throughout the  system!
 */
void sortorder::Ibyteorder(int *permutation, int size) 
{
#if W_DEBUG_LEVEL > 2
    /*
     * XXX Paranoia, overly so.  w_base_t should be paranoid.
     * Verify that our concept of byte order matches the base class
     */
    int2_t    magic = 0x1234;
    bool    my_big_endian;

    my_big_endian = ((uint1_t *)&magic)[0] == 0x12;
        
    if (my_big_endian) {
        w_assert3(w_base_t::is_big_endian()); 
    }
    else {
        w_assert3(w_base_t::is_little_endian()); 
    }
#endif

#ifdef BIGLONG
#error    "BIGLONG not supported"
#endif
    
    int    i;
    if (w_base_t::is_big_endian())
        for  (i = 0; i < size; i++)
            permutation[i] = i;
    else
        for (i = 0; i < size; i++)
            permutation[i] = size - i - 1;
}

void 
sortorder::Fbyteorder(int permutation[4]) 
{
    /* The following magic constant has the representation
     * 0x3f404142 on a BIGFLOAT machine.
     */
    f4_t f = 0.7509957552f;
    u_char *p = (u_char *)&f;
    for (int i=0;i<4;i++)
        permutation[i] = p[i] - 0x3f;
#ifdef BIGFLOAT_VERIFY
    /* verify that the BIGFLOAT assertion is correct */
    for (int i=0;i<4;i++)
        w_assert1(permutation[i] == i);
#endif
}

void 
sortorder::Dbyteorder(int permutation[8]) 
{
    /* The following magic constant has the representation
     * 0x3f40414243444546 on a BIGDOUBLE machine.
     */
    f8_t d = 0.00049606070982314491;
    u_char *p = (u_char *)&d;
    int i;
    for (i=0;i<8;i++)
        permutation[i] = p[i] - 0x3f;
#ifdef BIGDOUBLE_VERIFY
    /* verify that the BIGDOUBLE assertion is correct */
    for (i=0;i<8;i++)
        w_assert1(permutation[i] == i);
#endif
}

/* Translate (double, float, int, unsigned int, short, unsigned short,
 *  char, unsigned char)
 * to an (8,4,2,1)-byte string such that
 * lexicographic comparison of the strings will give the same result
 * as numeric comparison of the corresponding numbers.
 * The permutation perm is the result of Dbyteorder() or Ibyteorder() above.
 */

void 
sortorder::int_lexify(
    const void *d, 
    bool        is_signed, 
    int         len, 
    void *      res, 
    int         perm[]
) 
{
#ifdef BIGLONG
    switch(len) {
    case 8:
        // NB: can't count on alignment for this, so
        // do it the simple way
        for (int i=0;i<8;i++)
            ((int1_t *)res)[i] = ((int1_t *)&d)[perm[i]];
        break;
    case 4:
        ((int4_t *)res)[0] = ((int4_t *)d)[0];
        break;
    case 2:
        ((int2_t *)res)[0] = ((int2_t *)d)[0];
        break;
    case 1:
        ((int1_t *)res)[0] = ((int1_t *)d)[0];
        break;
    }
#else
    /* reorder bytes to big-endian */
    for (int i=0;i<len;i++) {
        ((uint1_t *)res)[i] = ((uint1_t *)d)[perm[i]];
    }
#endif
    
    if(is_signed) {
        /* correct the sign */
        uint1_t x = ((uint1_t *)res)[0];
        ((uint1_t *)res)[0] = (x ^ 0x80);
    }
}

void 
sortorder::int_unlexify(
    const void *str, 
    bool is_signed, 
    int len, 
    void *res, 
    int perm[]
) 
{

#ifdef BIGLONG
    switch(len) {
    case 8:
    ((w_base_t::int8_t *)res)[0] = ((int8_t *)str)[0] ^ 0x8000000000000000;
    break;
    case 4:
    ((w_base_t::int4_t *)res)[0] = ((int4_t *)str)[0] ^ 0x80000000;
    break;
    case 2:
    ((w_base_t::int2_t *)res)[0] = ((int2_t *)str)[0] ^ 0x8000;
    break;
    case 1:
    ((w_base_t::int1_t *)res)[0] = ((int1_t *)str)[0] ^ 0x80;
    break;
    }
#else

    uint1_t cp[24];
    w_assert1(len < int(sizeof(cp)));

    memcpy(cp, str, len);
    uint1_t x = cp[0];
    if(is_signed) {
    /* correct the sign */
    x ^= 0x80;
    }
    cp[0] = x;
    /* reorder bytes to big-endian */
    int i;
    for (i=0;i<len;i++)
        ((uint1_t *)res)[i] = cp[perm[i]];
#endif
}

void 
sortorder::float_lexify(
        f4_t d, 
        void *res, 
        int 
#ifndef BIGFLOAT
        perm
#endif
        []) 
{

#ifdef BIGFLOAT
    ((int4_t *)res)[0] = ((int4_t *)&d)[0];
#else
    /* reorder bytes to big-endian */
    for (int i=0;i<4;i++)
        ((int1_t *)res)[i] = ((int1_t *)&d)[perm[i]];
#endif
    
    /* correct the sign */
    if (*(int1_t *)res & 0x80) {
        /* negative -- flip all bits */
        ((uint4_t *)res)[0] ^= 0xffffffff;
    }
    else {
        /* positive -- flip only the sign bit */
        *(int1_t *)res ^= 0x80;
    }
}

void
sortorder::float_unlexify(
    const void *str, 
    int 
#ifndef BIGFLOAT
    perm
#endif
    [], 
    f4_t *result
) 
{
    FUNC(sortorder::float_unlexify);
    f4_t res = *(f4_t *)str;
    DBG(<<"float_unlexify converting " << res);

#ifdef BIGFLOAT
    /* correct the sign */
    uint4_t bits = 0;
    if (*(uint1_t *)str & 0x80) {
        bits = 0x80000000;
    } else {
        bits = 0xffffffff;
    }

    /* fast inline version of memcpy(&res, str, 4) */
    *((int4_t *)&res) = *((int4_t *)str) ^ bits;
#else
    uint1_t cp[sizeof(float)];

    memcpy(cp, str, sizeof(float));

    /* correct the sign -- set the bits for
     * the first byte
     */
    uint1_t bits = 0;
    if (cp[0] & 0x80) {
        bits = 0x80;
    } else {
        bits = 0xff;
    }
    DBG(<<"sortorder::float_unlexify: bits=" << bits);

    cp[0] ^= bits;
    if(bits == 0xff) for (int i=1;i<4;i++) { cp[i] ^= bits; }

    /* reorder bytes from big-endian */
    DBG(<<"sortorder::float_unlexify: bits=" << bits);
    for (int i=0;i<4;i++) {
        ((int1_t *)&res)[i] = cp[perm[i]];
    }
#endif
    DBG(<<"float_unlexify result " << res);
    *result = res;
}

void 
sortorder::dbl_lexify(f8_t d, 
        void *res, 
        int 
#ifndef BIGDOUBLE
        perm
#endif
        []
        ) 
{

#ifdef BIGDOUBLE
    /* fast inline version of memcpy(res, &d, 8) */
    ((int4_t *)res)[0] = ((int4_t *)&d)[0];
    ((int4_t *)res)[1] = ((int4_t *)&d)[1];
#else
    /* reorder bytes to big-endian */
    for (int i=0;i<8;i++)
        ((int1_t *)res)[i] = ((int1_t *)&d)[perm[i]];
#endif
    
    /* correct the sign */
    if (*(int1_t *)res & 0x80) {
        /* negative -- flip all bits */
        ((uint4_t *)res)[0] ^= 0xffffffff;
        ((uint4_t *)res)[1] ^= 0xffffffff;
    } else {
        /* positive -- flip only the sign bit */
        *(int1_t *)res ^= 0x80;
    }
}

void
sortorder::dbl_unlexify(
    const void *str, 
    int 
#ifndef BIGDOUBLE
    perm
#endif
    [], 
    f8_t *result
) 
{
    FUNC(dbl_unlexify);
    int i;
    f8_t res;

#ifdef BIGDOUBLE
    uint4_t bits = 0;
    if (*(uint1_t *)str & 0x80) {
        bits = 0x80000000;
    } else {
        bits = 0xffffffff;
    }
    /* fast inline version of memcpy(&res, str, 8) */
    ((int4_t *)&res)[0] = ((int4_t *)str)[0] ^ bits;
    if(bits== 0x80000000) bits = 0x0;
    ((int4_t *)&res)[1] = ((int4_t *)str)[1] ^ bits;
#else
    uint1_t cp[sizeof(double)];

    // Can't count on solaris memcpy working here if the address
    // isn't 8-byte aligned.
    /*
     * NB: It's not the memcpy, it's the too-smart-for-its-
     * britches gcc that bypasses memcpy if it thinks it
     * can do better -- unfortunately, sometimes it doesn't
     * really know what the alignment is...
     */
    // memcpy(cp, str, sizeof(double));
    for(i=0; i<8; i++) ((char *)cp)[i] = ((char *)str)[i];

    /* correct the sign */
    uint1_t bits = 0;
    if (cp[0] & 0x80) {
        bits = 0x80;
    } else {
        bits = 0xff;
    }
    DBG(<<"sortorder::float_unlexify: bits=" << bits);
    cp[0] ^=  bits;
    if(bits == 0xff) for (i=1;i<8;i++) { cp[i] ^= bits; }
    DBG(<<"sortorder::float_unlexify: bits=" << bits);

    /* reorder bytes from big-endian */
    for (i=0;i<8;i++)
        ((int1_t *)&res)[i] = cp[perm[i]]; 
#endif
    
    // don't do anything that requires alignment
    // *result = res;
    // memcpy((char *)result, (char *)&res, sizeof(f8_t));
    // brain-damaged memcpy(?) on solaris appears to do
    // double assignment, so we do a byte copy to avoid
    // alignment problems here:
    /*
     * NB: It's not the memcpy, it's the too-smart-for-its-
     * britches gcc that bypasses memcpy if it thinks it
     * can do better -- unfortunately, sometimes it doesn't
     * really know what the alignment is...
     */
    /* XXX align tools */
    switch((ptrdiff_t)result & 0x7) {
    case 0x00:
        *result = res;
        break;

    case 0x04:
        for(i=0; i<2; i++) 
        ((unsigned int *)result)[i] = ((unsigned int *)&res)[i];
        break;

    default:
        for(i=0; i<8; i++) 
        ((char *)result)[i] = ((char *)&res)[i];
        break;
    }
}


bool 
sortorder::lexify(
    const key_type_s  *kp,
    const void *d, 
    void *res
) 
{
    FUNC(lexify);
    keytype k = convert(kp);
    DBG(<<" k=" << int(k));
    switch(k) {
    case kt_nosuch:
    case kt_spatial:
        return false;

    case kt_i1:
        int_lexify(d, true, 1, res, I1perm);
        break;

    case kt_i2:
        int_lexify(d, true, 2,  res, I2perm);
        break;

    case kt_i4:
        int_lexify(d, true, 4, res, I4perm);
        break;

    case kt_i8:
        int_lexify(d, true, 8, res, I8perm);
        break;

    case kt_u1:
        int_lexify(d, false, 1, res, I1perm);
        break;

    case kt_u2:
        int_lexify(d, false, 2, res, I2perm);
        break;

    case kt_u4:
        int_lexify(d, false, 4, res, I4perm);
        break;

    case kt_u8:
        int_lexify(d, false, 8, res, I8perm);
        break;

    case kt_f4:
        DBG(<<"");
        float_lexify(*(f4_t *)d, res, Fperm);
        break;

        /* XXX shouldn't need copy with strict alignment */
    case kt_f8: {
        double dbl;
        memcpy(&dbl, d, sizeof(f8_t));
        dbl_lexify(dbl, res, Dperm);
        }
        break;

    case kt_b:
        if(kp->variable) {
        return false;
        }
        memcpy(res, d, kp->length);
        break;
    }
    return true;
}

bool 
sortorder::unlexify(
    const key_type_s  *kp,
    const void *str, 
    void *res
) 
{
    FUNC(unlexify);
    keytype k = convert(kp);
    DBG(<<" k=" << int(k));
    switch(k) {
    case kt_nosuch:
    case kt_spatial:
         return false;
         break;

    case kt_i1:
        int_unlexify(str,  true, 1, res, I1perm);
        break;

    case kt_i2:
        /* XXX why aren't the alignment tools used for all of these? */
        w_assert3(((ptrdiff_t)res & ALIGN_MASK_IU2) == 0x0);
        int_unlexify(str, true, 2,  res, I2perm);
        break;

    case kt_i4:
        w_assert3(((ptrdiff_t)res & ALIGN_MASK_IU4) == 0x0);
        int_unlexify(str, true, 4, res, I4perm);
        break;

    case kt_i8:
        w_assert3(((ptrdiff_t)res & ALIGN_MASK_IU8) == 0x0);
        int_unlexify(str, true, 8, res, I8perm);
        break;

    case kt_u1:
        int_unlexify(str, false, 1, res, I1perm);
        break;

    case kt_u2:
        w_assert3(((ptrdiff_t)res & ALIGN_MASK_IU2) == 0x0);
        int_unlexify(str, false, 2, res, I2perm);
        break;

    case kt_u4:
        w_assert3(((ptrdiff_t)res & ALIGN_MASK_IU4) == 0x0);
        int_unlexify(str, false, 4, res, I4perm);
        break;

    case kt_u8:
        w_assert3(((ptrdiff_t)res & ALIGN_MASK_IU8) == 0x0);
        int_unlexify(str, false, 8, res, I8perm);
        break;

    case kt_f4:
        // should be at least 4-byte aligned
        w_assert3(((ptrdiff_t)res & ALIGN_MASK_F4) == 0x0);
        float_unlexify(str, Fperm, (f4_t *)res);
        break;

    case kt_f8:
        // should be at least 4-byte aligned
        // architectures' alignment requirements
        // for doubles might differ.
#ifdef BOLO_DEBUG
        if (((ptrdiff_t)res & ALIGN_MASK_F8))
        cerr << "f8 unaligned " << res << endl;
#endif
        w_assert3(((ptrdiff_t)res & ALIGN_MASK_F8) == 0x0);
        dbl_unlexify(str, Dperm, (f8_t *)res);
        break;

    case kt_b:
        if(! kp->variable) {
             memcpy(res, str, kp->length);
        } else {
        return false;
        }
        break;
    }
    return true;
}

sortorder::keytype
sortorder::convert(const key_type_s *kp) 
{
    DBG(<<"convert type " << kp->type
    << " length " << kp->length);
    keytype result = kt_nosuch;

    switch (kp->type)  {
    case key_type_s::i:
    case key_type_s::I:
    case key_type_s::u:
    case key_type_s::U:
        w_assert3(kp->length == 1 || kp->length == 2 
        || kp->length == 4 
        || kp->length == 8 
        );
        w_assert3(! kp->variable);
        switch(kp->length) {
        case 1:
        result = (kp->type==key_type_s::i
                || kp->type==key_type_s::I)?
                kt_i1 : kt_u1;
        break;
        case 2:
        result = (kp->type==key_type_s::i ||
              kp->type==key_type_s::I)?kt_i2:kt_u2;
        break;
        case 4:
        result = (kp->type==key_type_s::i ||
              kp->type==key_type_s::I)?kt_i4:kt_u4;
        break;
        case 8:
        result = (kp->type==key_type_s::i ||
              kp->type==key_type_s::I)?kt_i8:kt_u8;
        break;
        default:
        break;
    }
        break;

    case key_type_s::f:
    case key_type_s::F:
        w_assert3(kp->length == 4 || kp->length == 8); 
        w_assert3(! kp->variable);
        switch(kp->length) {
        case 4:
        result = kt_f4;
        break;
        case 8:
        result = kt_f8;
        break;
        default:
        break;
    }
        break;

    case key_type_s::B: // unsigned byte compare
    case key_type_s::b: // unsigned byte compare
        // may be  kp->variable
    result = kt_b;
        break;

    default:
        W_FATAL(eNOTIMPLEMENTED);
    }
    DBG(<<"convert returns " << int(result));
    return result;
}

