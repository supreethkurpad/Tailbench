/*<std-header orig-src='shore'>

 $Id: rc.cpp,v 1.26 2010/06/08 22:27:15 nhall Exp $

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


w_rc_t testing2()
{

    w_rc_t rc = RC(fcOS);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);
    RC_AUGMENT(rc);

    // This constitutes a check on THIS rc
    if (rc.is_error())  {; } 

    return rc;
}
w_rc_t testing1()
{
	return testing2();
}

w_rc_t testing()
{

    w_rc_t rc = RC(fcOS);
    RC_AUGMENT(rc);
    RC_PUSH(rc, fcINTERNAL);
    RC_AUGMENT(rc);
    RC_PUSH(rc, fcFULL);
    RC_AUGMENT(rc);
    RC_PUSH(rc, fcEMPTY);
    RC_AUGMENT(rc);
    RC_PUSH(rc, fcNOTFOUND);
    RC_AUGMENT(rc);

    // This constitutes a check on THIS rc
    if (rc.is_error())  {; } 

    return rc;
}

w_rc_t testing_ok()
{
    return RCOK;
}

int main()
{
	// Turn on checking but turn off W_FATAL response.
	w_rc_t::set_return_check(true, false);


    cout << "Expect two 'error not checked' messages" << endl;
    {
        w_rc_t rc;
		rc = testing();
    }

    {
        testing_ok();
    }

    if(testing_ok().is_error()) {
        cout << "FAILURE: This should never happen!" << endl;
    }

    cout << "Expect 3 forms of the string of errors" << endl;
	{
			w_rc_t rc = testing();
		{
			//////////////////////////////////////////////////// 
			// this gets you to the integer values, one at a time
			//////////////////////////////////////////////////// 
			cout << "*************************************" << endl;
			w_rc_i iter(rc);
			cout << endl << "1 : List of error numbers:" << endl;
			for(w_base_t::int4_t e = iter.next_errnum();
				e!=0; e = iter.next_errnum()) {
			cout << "error = " << e << endl;
			}
			cout << "End list of error numbers:" << endl;
		}
		{
			//////////////////////////////////////////////////// 
			// this gets you to the w_error_t structures, one
			// at a time.  If you print each one, though, you
			// get it PLUS everything attached to it
			//////////////////////////////////////////////////// 
			w_rc_i iter(rc);
			cout << "*************************************" << endl;
			cout << endl << "2 : List of w_error_t:" << endl;
			for(const w_error_t *e = iter.next();
				e; 
				e = iter.next()) {
			cout << "error = " << *e << endl;
			}
			cout << "End list of w_error_t:" << endl;
		}
		{
			cout << "*************************************" << endl;
			cout << endl << "3 : print the rc:" << endl;
			cout << "error = " << rc << endl;
			cout << "End print the rc:" << endl;
		}
	}

	{
		w_rc_t rc = testing1();
		w_assert1(rc.is_error());
		cout << " ORIG:" << rc << endl;
		w_rc_i it(rc);

        w_rc_t rc2(rc);
		cout << " COPY CONSTRUCTOR: " << rc2 << endl;

        rc2 = rc;
		w_assert1(rc2.is_error());
		cout << " COPY OPERATOR: " << rc2 << endl;

	}
    return 0;
}

