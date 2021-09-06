#ifndef STHREAD_STATS_OUT_GEN_CPP
#define STHREAD_STATS_OUT_GEN_CPP

/* DO NOT EDIT --- GENERATED from sthread_stats.dat by stats.pl
           on Wed Sep  1 18:45:09 2021

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
operator<<(ostream &o,const sthread_stats &t)
{
	o << setw(W_sthread_stats) << "num_io " << t.num_io << endl;
	o << setw(W_sthread_stats) << "read " << t.read << endl;
	o << setw(W_sthread_stats) << "write " << t.write << endl;
	o << setw(W_sthread_stats) << "sync " << t.sync << endl;
	o << setw(W_sthread_stats) << "truncate " << t.truncate << endl;
	o << setw(W_sthread_stats) << "writev " << t.writev << endl;
	o << setw(W_sthread_stats) << "readv " << t.readv << endl;
	return o;
}

#endif /* STHREAD_STATS_OUT_GEN_CPP */
