/*<std-header orig-src='shore' incl-file-exclusion='LEXIFY_H'>

 $Id: lexify.h,v 1.17 2010/06/08 22:28:55 nhall Exp $

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

#ifndef LEXIFY_H
#define LEXIFY_H

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

#ifdef __GNUG__
#   pragma interface 
#endif

namespace ssm_sort {
    class sort_keys_t; // forward
}

/**\brief Class containing basic types used by sort 
 * and by B+-Tree key-management. 
 */
class sortorder : private smlevel_0 {
    friend class ssm_sort::sort_keys_t;
    typedef w_base_t::f4_t f4_t;
    typedef w_base_t::f8_t f8_t;

    typedef w_base_t::uint1_t uint1_t;
    typedef w_base_t::uint2_t uint2_t;
    typedef w_base_t::uint4_t uint4_t;
    typedef w_base_t::uint8_t uint8_t;

    typedef w_base_t::int1_t int1_t;
    typedef w_base_t::int2_t int2_t;
    typedef w_base_t::int4_t int4_t;
    typedef w_base_t::int8_t int8_t;
public:
    /**\enum keytype 
     *\brief Enumerator that describes the basic fixed-length and 
     * variable-length
     * (sub-)key types that are used in B+-Trees and 
     * can be used in sorting.
     * \details
     * - kt_i1 signed 1-byte integer values
     * - kt_u1 unsigned 1-byte integer values
     * - kt_i2 signed 2-byte integer values 
     * - kt_u2 unsigned 2-byte integer values 
     * - kt_i4 signed 4-byte integer values 
     * - kt_u4 unsigned 4-byte integer values 
     * - kt_f4 IEEE single precision floating point values 
     * - kt_f8 IEEE double precision floating point values 
     * - kt_b unsigned byte string 
     */
    enum keytype {
        kt_nosuch,
        /* signed, unsigned 1-byte integer values */
        kt_i1, kt_u1, 
        /* signed, unsigned 2-byte integer values */
        kt_i2, kt_u2, 
        /* signed, unsigned 4-byte integer values */
        kt_i4, kt_u4, 
        /* signed, unsigned 64-bit integer values */
        kt_i8, kt_u8,
        /* IEEE single, double precision floating point values */
        kt_f4, kt_f8, 
        /* unsigned byte string */
        kt_b,
        /* not used here */
        kt_spatial 
    };

    NORET sortorder();
    NORET ~sortorder();

    /**\brief Convert to lexicographic form.
     * @param[in] kp    Describes the type and length
     *                  of the string to be converted.
     * @param[in] d     The string to be converted.
     * @param[out] res  The result.
     *
     * Invokes a predefined LEXFUNC for the know keytype.
     * Returns true if it worked, false otherwise.
     *
     */
    bool lexify(const key_type_s *kp, const void *d, void *res); 

    /**\brief Convert from lexicographic form.
     * @param[in] kp    Describes the type and length
     *                  of the string to be converted.
     * @param[in] str     The string in lexicographic form to be converted.
     * @param[out] res  The result.
     *
     * Invokes a predefined inverse LEXFUNC for the known keytype.
     * Returns true if it worked, false otherwise
     */
    bool unlexify(const key_type_s *kp, const void *str, void *res) ;

private:
    void int_lexify( const void *d, bool is_signed, int len, 
        void *res, int perm[]);
    void int_unlexify( const void *str, bool is_signed, int len, 
        void *res, int perm[]);

public: 
    /**\cond skip */
    void float_lexify(w_base_t::f4_t d, void *res, int perm[]) ;
    void float_unlexify( const void *str, int perm[], w_base_t::f4_t *result); 
    void dbl_lexify(w_base_t::f8_t d, void *res, int perm[]) ;
    void dbl_unlexify( const void *str, int perm[], w_base_t::f8_t *result);

    void Ibyteorder(int permutation[8]) ;
    void Ibyteorder(int *permutation, int size);
    void Fbyteorder(int permutation[4]) ;
    void Dbyteorder(int permutation[8]) ;

    /**\endcond skip */
    /**\brief Convert from a key_type_s to a keytype enumerator value */
    static keytype convert(const key_type_s *k);

private:
    int I1perm[1];
    int I2perm[2];
    int I4perm[4];
    int I8perm[8];
    int Fperm[4];
    int Dperm[8];
};

extern class sortorder SortOrder;

/*<std-footer incl-file-exclusion='LEXIFY_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
