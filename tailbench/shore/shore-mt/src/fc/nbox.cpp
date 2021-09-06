/*<std-header orig-src='shore'>

 $Id: nbox.cpp,v 1.24.2.6 2010/03/19 22:17:16 nhall Exp $

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
#define NBOX_C
#ifdef __GNUC__
#   pragma implementation
#endif

#include <w.h>
#include <nbox.h>

#include <cstdlib>
#include <cmath>

#include <iostream>
#include <w_strstream.h>
#include <cstdio>

#ifndef MIN
#define MIN(x,y) ((x) < (y) ? (x) : (y))
#endif

#ifndef MAX
#define MAX(x,y) ((x) > (y) ? (x) : (y))
#endif

#ifndef ABS
#define ABS(x) ((x) >= 0 ? (x) : -(x))
#endif

/* "register" declarations limit the optimizers of compilers */
#ifdef WANT_REGISTER
#define        REGISTER        register
#else
#define        REGISTER
#endif

// special ref to nbox for rtree queries
nbox_t                __Null__(0); // dimension 0
nbox_t&                nbox_t::Null = __Null__;

const nbox_t::int4_t        nbox_t::max_int4 = w_base_t::int4_max;
const nbox_t::int4_t        nbox_t::min_int4 = w_base_t::int4_min;

nbox_t::nbox_t(int dimension)
: dim(dimension)
{
        w_assert9(dimension <= max_dimension);
#ifdef ZERO_INIT
        memset(array, '\0', sizeof(array));
#endif
        nullify();
}

nbox_t::nbox_t(const nbox_t& r)
{
        *this = r;
}

nbox_t::nbox_t(int dimension, int box[])
: dim(dimension)
{
    w_assert9(dimension <= max_dimension);
#ifdef ZERO_INIT
        memset(array, '\0', sizeof(array));
#endif
    for (int i=0; i<dim; i++) {
        array[i] = box[i];
        array[i+dim] = box[i+dim];
    }
}

nbox_t::nbox_t(const char* s, int len)
: dim(len / (2 * sizeof(int)))
{
    w_assert9(dim <= max_dimension);
#ifdef ZERO_INIT
    memset(array, '\0', sizeof(array));
#endif
    memcpy((void*) array, (void*) s, len);
}

//
// for tcl test only: representation for a box is like "2.0.0.1.1."
//         (x0=0, y0=0, x1=1, y1=1)
//
nbox_t::nbox_t(const char* s)
{
#ifdef ZERO_INIT
    memset(array, '\0', sizeof(array));
#endif
    if (s==NULL) {
        dim = max_dimension;
        nullify();
        return;
    }

    put(s);
}

bool nbox_t::empty() const
{
    return (area() < 0.0);
}

void nbox_t::print(ostream &o, int level) const
{
    REGISTER int i, j;
    for (j = 0; j < 5 - level; j++) o << "\t";
    o << "---------- :\n";

    if(dim > 0) {
        /* A box is 2 points, each in <dim> dimensions */

        for (j = 0; j < 5 - level; j++) o << "\t";
        o << array[0] ;
        for (i=1; i<dim; i++) {
            o << "," << array[i] ;
        }
        o << endl;

        for (j = 0; j < 5 - level; j++) o << "\t";
        o << array[0+dim] ;
        for (i=1; i<dim; i++) {
            o << "," << array[i+dim] ;
        }
        o << endl;
    }
    if(dim == 0) { o << "<NULL>" <<endl; }
}

//
// for draw gremlin picture only
//

void nbox_t::draw(int level, ostream &DrawFile, const nbox_t& CoverAll) const
{
    /*static int seed = 0;*/ //dsm
    int thick;
    double x1, y1, x2, y2;
    double ScreenSize, WorldSize;
    double ratio, adjust;
    double adj,base;

    if (empty()) return;

    base = 2147483647.0;                /* XXX magic number int::max */
    ScreenSize = 500.0;
    WorldSize = (double)MAX(CoverAll.array[2]-CoverAll.array[0],
                        CoverAll.array[3]-CoverAll.array[1]);

    switch (level-1) {
        case 0: thick = 5; break;
        case 1: thick = 1; break;
        case 2: thick = 4; break;
        case 3: thick = 2; break;
        case 4: thick = 6; break;
        default: thick = 3; break;
    }

    adjust = 0.0;
    if (thick != 5) {
        //srand(seed);
        adj = rand();
        adjust = (level-1) * 5.0 + (adj/base) * 5.0 ;
    }

    ratio = ScreenSize / WorldSize;

    if (thick!=5) {
        x1 = -1.0*ratio* ABS(array[0])*0.05;
        x2 = ratio*ABS(array[2])*0.05;
        y1 = -1.0*ratio*ABS(array[1])*0.05;
        y2 = ratio*ABS(array[3])*0.05;
    } else {
        x1 = x2 = y1 = y2 = 0.0;
    }

    x1 = 32.0 + (array[0] - CoverAll.array[0]) * ratio - adjust;
    x2 = 32.0 + (array[2] - CoverAll.array[0]) * ratio + adjust;
    y1 = -64.0 + (array[1] - CoverAll.array[1]) * ratio - adjust;
    y2 = -64.0 + (array[3] - CoverAll.array[1]) * ratio + adjust;

        W_FORM(DrawFile)("VECTOR\n");
        W_FORM(DrawFile)("%3.2f %3.2f\n",x1,y1);
        W_FORM(DrawFile)("%3.2f %3.2f\n",x2,y1);
        W_FORM(DrawFile)("%3.2f %3.2f\n",x2,y2);
        W_FORM(DrawFile)("%3.2f %3.2f\n",x1,y2);
        W_FORM(DrawFile)("%3.2f %3.2f\n",x1,y1);
        W_FORM(DrawFile)("*\n");
        W_FORM(DrawFile)("%1d 0\n",thick);
        W_FORM(DrawFile)("0\n");
}

bool nbox_t::operator==(const nbox_t& other) const
{
    REGISTER int i;

    w_assert9(dim == other.dim);

    for (i=0; i<dim; i++) {
        if (array[i] != other.array[i] || array[i+dim] != other.array[i+dim])
            return false;
    }
    return true;
}

ostream& operator<<(ostream& os, const nbox_t& box)
{
   REGISTER int i;

    os << "[";
    if(box.dim > 0) {
        /* A box is 2 points, each in <dim> dimensions */

        os << "<"<< box.array[0] ;
        for (i=1; i<box.dim; i++) {
            os << "," << box.array[i] ;
        }
        os <<  ">,<" ;

        os << box.array[0+box.dim] ;
        for (i=1; i<box.dim; i++) {
            os << "," << box.array[i+box.dim] ;
        }
        os << ">";
    }
    os << "] " << endl;

   return os;
}

double nbox_t::area() const
{
    if(is_Null()) return -1.0; 

    REGISTER int i;
    int point = true;
    int s;
    double product = 1.0;
    for (i=0; i<dim; i++) {
        if ((s=side(i)) < 0) return -1.0;
        if ((product *= (double)s) < 0) return (4.0*max_int4*max_int4);
        point = (point && !s);
    }
    if (point) return 1.0;
    else return product;
}

int nbox_t::margin()
{
    REGISTER int i, sum = 0;
    for (i=0; i<dim; i++) {
        sum += side(i);
    }
    return sum;
}

//
// if two boxes are not intersected, then result will be an empty box
// invalid to intersect Null with anything
//
nbox_t nbox_t::operator^(const nbox_t& other) const
{
    REGISTER int i;
    w_assert1(dim == other.dim);
    int box[2*max_dimension];

    for (i=0; i<dim; i++) {
        box[i] = MAX(array[i], other.array[i]);
        box[i+dim] = MIN(array[i+dim], other.array[i+dim]);
    }
    return nbox_t(dim, box);
}

nbox_t nbox_t::operator+(const nbox_t& other) const
{
    REGISTER int i;
    w_assert1(dim == other.dim);
    int box[2*max_dimension];

    for (i=0; i<dim; i++) {
        box[i] = MIN(array[i], other.array[i]);
        box[i+dim] = MAX(array[i+dim], other.array[i+dim]);
    }
    return nbox_t(dim, box);
}

nbox_t& nbox_t::operator+=(const nbox_t& other)
{
    REGISTER int i;
    w_assert1(dim == other.dim);
    for (i=0; i<dim; i++) {
        array[i] = MIN(array[i], other.array[i]);
        array[i+dim] = MAX(array[i+dim], other.array[i+dim]);
    }
    return *this;
}

bool nbox_t::operator||(const nbox_t& other) const
{
    REGISTER int i;

    w_assert1(dim == other.dim);

    for (i=0; i<dim; i++)
    {
       // Check for overlap along dimension i
       if ((array[i] > other.array[i+dim] || array[i+dim] < other.array[i]))
          return false;
    }
    return true;
}

bool nbox_t::operator/(const nbox_t& other) const
{
    REGISTER int i;

    w_assert9(dim == other.dim);

    for (i=0; i<dim; i++) 
        if (array[i] > other.array[i]) return false;

    for (i=dim; i<2*dim; i++)
        if (array[i] < other.array[i]) return false;

    return true;
}

bool nbox_t::operator>(const nbox_t& other) const
{
    REGISTER int i;

    w_assert1(dim == other.dim);
    for (i=0; i<dim*2; i++) {
        if (array[i] > other.array[i]) return true;
        else if (array[i] < other.array[i]) return false;
    }
    return false;
}

bool nbox_t::operator<(const nbox_t& other) const
{
    REGISTER int i;

    w_assert1(dim == other.dim);
    for (i=0; i<dim*2; i++) {
        if (array[i] < other.array[i]) return true;
        else if (array[i] > other.array[i]) return false;
    }
    return false;
}

double nbox_t::operator*(const nbox_t& other) const
{
    REGISTER int i;
    w_assert1(dim == other.dim);
    double square = 0.0;
    for (i=0; i<dim; i++) {
        int diff = array[i]+array[i+dim]-other.array[i]-other.array[i+dim];
        square += (diff*diff/4.0);
    }
    return square;
}

//
// for tcl only
//
// XXtghread problems
//
nbox_t::operator char*()
{
        static char s[40];        /* XXX sharing problem */
        w_ostrstream ss(s, sizeof(s));

        W_FORM(ss)("%d.%ld.%ld.%ld.%ld", dim, 
                array[0], array[1], array[2], array[3]);
        ss << ends;

        return s;
}

void nbox_t::bytes2box(const char* key, int klen)
{
    dim = klen / (2*sizeof(int));
    memcpy((void*) array, (void*) key, klen);
}

//
// for tcl test only: representation for a box is like "2.0.0.1.1."
//         (x0=0, y0=0, x1=1, y1=1)
//
void nbox_t::put(const char* s)
{
    int MAYBE_UNUSED n;
    n = sscanf(C_STRING_BUG s, "%d.%d.%d.%d.%d", &dim,
                &array[0], &array[1], &array[2], &array[3]);
    w_assert1(n==5 && dim == 2);
    for (int i=2*dim; i<2*max_dimension; i++) {
        array[i] = 0;
    }
}

//
// modify the current box to be squared (for 2 dimension only)
//
void nbox_t::squared()
{
    int x_side = side(0);
    int y_side = side(1);

    if (x_side > y_side) {
        array[1] -= (x_side-y_side)/2;
        array[3] += (x_side-y_side)/2;
    } else {
        array[0] -= (y_side-x_side)/2;
        array[2] += (y_side-x_side)/2;
    }
}

void nbox_t::nullify()
{
    for (int i=0; i<dim; i++) {
        array[i] = max_int4;
        array[i+dim] = min_int4;
    }
}

//
// tables for 2 dimensional hilbert value calculation
//
static const int rotation_table[4] = { 3, 0, 0, 1};
static const int sense_table[4] = {-1, 1, 1, -1};
static const int quad_table[4][2][2] = { {{0,1}, {3,2}}, {{1,2}, {0,3}},
                {{2,3}, {1,0}}, {{3,0}, {2,1}} };

/* should be cygnus win32 environment problem */
#ifdef WINNT
/* math.h has a #define-d log2 */
#undef log2
#endif

static int log2(int value)
{
    int ground = 1;
    REGISTER int i = 0;
    while (( ground = (ground<<1)) <= value) i++;
    return i;
}

static int power(int base, int index)
{
    REGISTER int val = 1;
    for (int i=0; i<index; i++) val *= base;
    return val;
}

//
// hilbert value for 2 dimensional spatial object only
//
int nbox_t::hvalue(const nbox_t& universe, int level) const
{
    int min_side = MIN(universe.side(0), universe.side(1));
    if (level == 0)
        level = MIN(14, log2(min_side) );

    int x_low = universe.bound(0), y_low = universe.bound(1);
    int ret_val = 0;
    int x = center(0), y = center(1);

    int count = 0, rotation = 0, sense = 1;

    REGISTER int i,j;
    for ( i=(universe.side(0)+1)/2, j=(universe.side(1)+1)/2;
            i>0 && j>0 && level>count; i=i/2, j=j/2) {
        count++;
        int x_val = (x - x_low) < i ? 0 : 1;
        int y_val = (y - y_low) < j ? 0 : 1;
        int quad = quad_table[rotation][x_val][y_val];
        x_low += (i*x_val);
        y_low += (j*y_val);
        ret_val += ((((sense==-1)?(3-quad):quad)-1)*power(4,level-count));
        rotation = (rotation + rotation_table[quad]) % 4;

        sense *= sense_table[quad];
    }

    return ret_val;
}

int nbox_t::hcmp(const nbox_t& other, const nbox_t& universe, int level) const
{
    int min_side = MIN(universe.side(0), universe.side(1));
    if (level == 0)
        level = MIN(14, log2(min_side) );

    int x_low = universe.bound(0), y_low = universe.bound(1);
    int val1, val2;
    int x1 = center(0), y1 = center(1),
        x2 = other.center(0), y2 = other.center(1);
    int count = 0, rotation = 0, sense = 1;

    REGISTER int i,j;
    for ( i=(universe.side(0)+1)/2, j=(universe.side(1)+1)/2;
            i>0 && j>0 && level>count; i=i/2, j=j/2) {
        count++;
        int x_val = (x2 - x_low) < i ? 0 : 1;
        int y_val = (y2 - y_low) < j ? 0 : 1;
        int quad = quad_table[rotation][x_val][y_val];
        val2 = (sense==-1)?(3-quad):quad;
        x_val = (x1 - x_low) < i ? 0 : 1;
        y_val = (y1 - y_low) < j ? 0 : 1;
        quad = quad_table[rotation][x_val][y_val];
        val1 = (sense==-1)?(3-quad):quad;
        if (val1!=val2) return (val1-val2);

        rotation = (rotation + rotation_table[quad]) % 4;
        sense *= sense_table[quad];
        x_low += (i*x_val);
        y_low += (j*y_val);
    }

    return 0;
}

void    
nbox_t::canonize()
{
    int x;
    for (int i=0; i<dim; i++) {
        if(array[i] > array[i+dim]) {
            // swap
            x = array[i];
            array[i] = array[i+dim];
            array[i+dim] = x;
        }
    }
}
