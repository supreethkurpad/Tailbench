/*<std-header orig-src='shore'>

 $Id: ss.cpp,v 1.1.2.4 2009/10/30 23:49:04 nhall Exp $

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

/*  -- do not edit anything above this line --   </std-header>*/

// test w_ostrstream_buf, w_ostrstream

#include "w_strstream.h"
#include <iostream>
#include <cstdlib>

void testit(bool terminate, w_ostrstream &s,
    int argc, char **argv)
{
    for (int i = 2; i < argc; i++) {
        if (i>2) s << ' ' ;
        s << argv[i];
    }

    if (terminate)
        s << ends;

    cout << "c_str @ " << (void*) s.c_str() << endl;
    const char *t = s.c_str();
    cout << "strlen = " << strlen(t) << endl;
    cout << "buf '" << t << "'" << endl;
}

int main(int argc, char **argv)
{
    int            len = 30;
    if (argc > 1)
        len = atoi(argv[1]);

    cout << "len = " << len  ;
    cout << "; args = " ;
    for(int i=2; i<argc; i++)
    {
        cout << argv[i];
    }
    cout << endl;
    bool terminate = false;
    if (len < 0) {
        terminate = true;
        len = -len;
    }
    w_ostrstream_buf    s(len);
    w_ostrstream        s2;

	testit(terminate, s, argc, argv);
	testit(terminate, s2, argc, argv);

    return 0;
}
