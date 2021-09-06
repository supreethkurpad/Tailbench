/*<std-header orig-src='shore'>

 $Id: errcodes.cpp,v 1.13.2.4 2010/03/19 22:19:21 nhall Exp $

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

#include <w.h>
#include <option.h>

#include <iostream>

int main()
{
    cout << "ERROR CODES:\n"; 
    (void) w_error_t::print(cout);

    return 0;
}
option_group_t t(2); // causes error codes for options to
    // be included.

#ifdef __GNUC__
typedef w_auto_delete_array_t<char> gcc_kludge_1;
typedef w_list_i<option_t, unsafe_list_dummy_lock_t>             gcc_kludge_0;

#endif /* __GNUC__*/

