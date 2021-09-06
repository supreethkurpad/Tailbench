#ifndef TEST_STAT_STRUCT_GEN_H
#define TEST_STAT_STRUCT_GEN_H

/* DO NOT EDIT --- GENERATED from test_stat.dat by stats.pl
           on Wed Sep  1 18:45:08 2021

<std-header orig-src='shore' genfile='true'>

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


 w_base_t::base_stat_t b;
 w_base_t::base_float_t f;
 w_base_t::base_stat_t i;
 w_base_t::base_stat_t j;
 w_base_t::base_stat_t u;
 w_base_t::base_float_t k;
 w_base_t::base_stat_t l;
 w_base_t::base_stat_t v;
 w_base_t::base_stat_t x;
 w_base_t::base_float_t d;
 w_base_t::base_float_t sum;
public: 
friend ostream &
    operator<<(ostream &o,const test_stat &t);
public: 
friend test_stat &
    operator+=(test_stat &s,const test_stat &t);
public: 
friend test_stat &
    operator-=(test_stat &s,const test_stat &t);
static const char    *stat_names[];
static const char    *stat_types;
#define W_test_stat  3 + 2

#endif /* TEST_STAT_STRUCT_GEN_H */
