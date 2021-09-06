/*<std-header orig-src='shore'>

 $Id: lsns.cpp,v 1.1.2.2 2009/10/30 23:50:53 nhall Exp $

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

#include <w_stream.h>
#include <cstddef>
#include <w.h>
#include <lsn.h>

int main()
{
    for(int i=-2; i < 4; i++)
    {
        cout  << endl;
        for(int j=-2; j<4; j++)
        {
            lsn_t a(i,j);    

            cout 
                << " i=" << i
                << " j=" << j
                << "  " 
                << " valid=" << (const char *)(a.valid()?"true":"false")
                << " file=" << a.file()
                << " rba=" << a.rba()
                << " mask=" << a.mask()
                << endl;
        }
    }
}
