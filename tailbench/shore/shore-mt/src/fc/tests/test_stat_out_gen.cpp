#ifndef TEST_STAT_OUT_GEN_CPP
#define TEST_STAT_OUT_GEN_CPP

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


ostream &
operator<<(ostream &o,const test_stat &t)
{
	o << setw(W_test_stat) << "b " << t.b << endl;
	o << setw(W_test_stat) << "f " << t.f << endl;
	o << setw(W_test_stat) << "i " << t.i << endl;
	o << setw(W_test_stat) << "j " << t.j << endl;
	o << setw(W_test_stat) << "u " << t.u << endl;
	o << setw(W_test_stat) << "k " << t.k << endl;
	o << setw(W_test_stat) << "l " << t.l << endl;
	o << setw(W_test_stat) << "v " << t.v << endl;
	o << setw(W_test_stat) << "x " << t.x << endl;
	o << setw(W_test_stat) << "d " << t.d << endl;
	o << setw(W_test_stat) << "sum " << t.sum << endl;
	return o;
}

#endif /* TEST_STAT_OUT_GEN_CPP */
