/*<std-header orig-src='shore'>

 $Id: test_stat.cpp,v 1.1.2.4 2010/03/19 22:17:53 nhall Exp $

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
#include "w_stat.h"
#if defined(TEST1) || defined(TEST2)
#include "w_statistics.h"
#endif
#include "test_stat.h"
#include "test_stat_out_gen.cpp"

#include <iostream>

/* the code is here: */

#if defined(TEST1) || defined(TEST2)
// define the output operator
#include "test_stat_stat_gen.cpp"
#endif

// the strings:
const char *test_stat ::stat_names[] = {
#include "test_stat_msg_gen.h"
	NULL
};


void
test_stat::dec() 
{
    i--;
    j--;
    k-=1.0;
    v --;
    compute();
}
void
test_stat::inc() 
{
    i++;
    j++;
    k+=1.0;
    v ++;
    compute();
}

