/*<std-header orig-src='shore'>

 $Id: stats2.cpp,v 1.1.2.5 2010/03/19 22:17:53 nhall Exp $

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
#include <w_base.h>
#include <w_debug.h>

#include "w_stat.h"
#undef TEST1
#undef TEST2
#if defined(TEST1) || defined(TEST2)
#include "w_statistics.h"
#endif
#include "test_stat.h"

#include <iostream>

void statstest();

int main()
{

    DBG(<<"app_test: main");
    statstest();
    return 0;
}

// make it easy to change LINENO to __LINE__
// but have __LINE__ disabled for now because it makes it easy
// to get false failures in testall
#define LINENO " "
void
statstest()
{
    /*********w_statistics_t ***********/

    // DEAD class w_statistics_t ST; // generic stats class
    class test_stat TSTA; // my test class that uses stats
    class test_stat TSTB; // my test class that uses stats
    w_rc_t    e;
    TSTA.compute();
    TSTB.dec();

    // DEAD ST << TSTA;
    // DEAD ST << TSTA;
    // DEAD ST << TSTA;
	cout << TSTA;
	cout << TSTB;

#ifdef TEST1
    // Various ways the contents of ST can be printed:
    {
        cout << "ST.printf():" << endl;
        ST.printf();
        cout << endl;

        cout << "cout << ST:" << endl;
        cout << ST;
        cout << endl;

        /*  
         * Format some output my own way.
         * This is a way to do it by calling
         * on the w_statistics_t structure to provide
         * the string name.
         */
        cout << "My own pretty formatting for module : " 
            << ST.module(TEST_i) << endl;
        W_FORM2(cout,("\t%-30.30s %10.10d", 
                ST.string(TEST_i), ST.int_val(TEST_i)) );
            cout << endl;
        W_FORM2(cout,("\t%-30.30s %10.10u", 
                ST.string(TEST_j), ST.uint_val(TEST_j)) );
            cout << endl;
        W_FORM2(cout,("\t%-30.30s %10.6f", 
                ST.string(TEST_k), ST.float_val(TEST_k)) );
            cout << endl;
        W_FORM2(cout,("\t%-30.30s %10.10u", 
                ST.string(TEST_v), ST.ulong_val(TEST_v)) );
            cout << endl;
        cout << endl;

        /*
         * Error cases:
         */
        cout << "Expect some unknowns-- these are error cases:" <<endl;
        cout <<  ST.typechar(TEST_i) << ST.typestring(TEST_STATMIN) << endl;
        cout     // <<  ST.typechar(-1,1) 
            << ST.typestring(-1) 
                << ST.string(-1,-1)<<  ST.float_val(-1,-1)
        << endl;
    }
#endif
#ifdef TEST2
    {
        {
            const w_statistics_t &CUR = ST;

            cout << LINENO << endl;
            TSTA.inc();
            cout << LINENO<< " :******* TSTA.inc(): " << endl;
            CUR.printf();
            cout << endl;
        }
    }
#endif
    /********** end w_statistics_t tests ********/
}

