/*<std-header orig-src='shore' incl-file-exclusion='NBOX_H'>

 $Id: nbox.h,v 1.19 2010/05/26 01:20:21 nhall Exp $

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

#ifndef NBOX_H
#define NBOX_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <w_base.h>
#include <iosfwd>

#ifdef __GNUG__
#pragma interface
#endif

/**\brief Spatial object class: n-dimensional box 
 * \details
* An n-dimensional box is represented as: 
* -xlow, ylow,  (lower left point)
* -xhigh, yhigh. (higher right point).
* Only integer coordinates are supported for the points.
* This is used in the API for R*-trees.
*/ 
class nbox_t {
    friend ostream& operator<<(ostream& os, const nbox_t& box);

public:
    typedef w_base_t::int4_t int4_t;

    enum { max_dimension = 4 }; 
    static const int4_t    max_int4;
    static const int4_t    min_int4;

    /**\brief Special marker for queries.
    * This is not a box with area 0, but rather, a
    * non-box i.e., not a legit value
    */
    static nbox_t&     Null;

public:
    /**\brief Enum to describe comparisons. */ 
    enum sob_cmp_t { t_exact = 1, t_overlap, t_cover, t_inside, t_bad };

    bool is_Null() const { return (dim == 0)?true:false; }

protected:
    int4_t    array[2*max_dimension];    // boundary points
    int       dim;              // dimension
private:
    fill4    filler;         // 8 byte alignment
    int4_t* box() { return array; }    // reveal internal storage
public:
    /** Create a box.
     * @param[in] dimension  Number of dimensions for the "box".
     */
    nbox_t(int dimension = max_dimension);
    /** Create a box.
     * @param[in] dimension  Number of dimensions for the "box".
     * @param[in] box   Contains the dimensions.
     */
    nbox_t(int dimension, int box[]);
    /** Create a box.
     * @param[in] nbox  Source for copy constructor.
     */
    nbox_t(const nbox_t& nbox);
    /** Create a box.
     * @param[in] s     String: used only for Tcl-based tester.
     * @param[in] len   Length of s; used for Tcl-based tester.
     */
    nbox_t(const char* s, int len);    // for conversion from tuple key 

    virtual ~nbox_t() {}

    /// return given dimension
    int dimension() const     { return dim; }
    /// return highest value for \a nth dimension
    int bound(int n) const     { return array[n]; }
    /// return length of \a nth side
    int side(int n) const      { return array[n+dim]-array[n]; }
    /// return middle of \a nth side
    int center(int n) const { return (array[n+dim]-array[n])/2+array[n]; }

    bool    empty() const;    // test if box is empty
    void    squared();    // make the box squared
    void    nullify();    // make the box empty (poor name)

    /**\brief Make canonical.
     * \details
     * First point low in all dimensions, 2nd high in all dimensions
     */
    void    canonize(); 

    /**\brief Return Hilbert value
     */
    int hvalue(const nbox_t& universe, int level=0) const; // hilbert value
    /**\brief Comparison function for Hilbert values.
     * \details
     * For use with bulk load.
     */
    int hcmp(const nbox_t& other, const nbox_t& universe, 
            int level=0) const; // hilbert value comparison

    void print(ostream &, int level) const;
    void draw(int level, ostream &DrawFile, const nbox_t& CoverAll) const;

    /**\brief Area of a box
     * Return value:
     *    >0 : valid box
     *    =0 : a point
     *    <0 : empty box 
     */
    double area() const;

    /**\brief Margin of a Rectangle
    */
    int margin();

    /*!
    * Some binary operations:
    *    ^: intersection  ->  box
    *    +: bounding box  ->  box (result of addition)
    *    +=: enlarge by adding the new box  
    *    ==: exact match  ->  boolean
    *    /: containment   ->  boolean
    *    ||: overlap     ->  boolean
    *    >: bigger (comapre low values) -> boolean
    *    <: smaller (comapre low values) -> boolean
    *    *: square of distance between centers of two boxes 
    */
    nbox_t operator^(const nbox_t& other) const;
    nbox_t operator+(const nbox_t& other) const;

    nbox_t& operator+=(const nbox_t& other);
    nbox_t& operator=(const nbox_t& other);
    bool operator==(const nbox_t& other) const;
    bool operator/(const nbox_t& other) const;
    bool contains(const nbox_t& other) const { return *this / other; }
    bool operator||(const nbox_t& other) const;
    bool operator>(const nbox_t& other) const;
    bool operator<(const nbox_t& other) const;
    double operator*(const nbox_t& other) const;

    /**\brief For Tcl/smsh use only */
    nbox_t(const char* s);        // for conversion from ascii for tcl
    /**\brief For Tcl/smsh use only */
    operator char*();    // conversion to ascii for tcl
    /**\brief For Tcl/smsh use only */
    void put(const char*);  // conversion from ascii for tcl

    /**\brief conversion from key to box */
    void  bytes2box(const char* key, int klen);
    /**\brief conversion from box to key */
    const void* kval() const { return (void *) array; }
    /**\brief conversion from box to key */
    int   klen() const { return 2*sizeof(int)*dim; }

};

inline nbox_t& nbox_t::operator=(const nbox_t &r)
{
    if (this != &r) {
        int i;
        dim = r.dim;
        for (i = 0; i < dim*2; i++)
            array[i] = r.array[i];
    }
    return *this;
}

/*<std-footer incl-file-exclusion='NBOX_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
