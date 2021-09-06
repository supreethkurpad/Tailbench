/*<std-header orig-src='shore'>

 $Id: startstop.cpp,v 1.1 2010/05/26 01:21:21 nhall Exp $

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

// This include brings in all header files needed for writing a VAs 
//
#include "sm_vas.h"

/* This is about the most minimal storage manager program there is.
 * This starts up and shuts down a storage manager. 
 */


/* smthread-based class for all sm-related work */
class smthread_user_t : public smthread_t {
	int        argc;
	char       **argv;
public:
	int        retval;

	smthread_user_t(int ac, char **av) 
			: smthread_t(t_regular, "smthread_user_t"),
			argc(ac), argv(av), retval(0) { }
	~smthread_user_t()  {}
	void run() {
		cout << "Starting SSM and performing recovery ..." << endl;
		ss_m* ssm = new ss_m();
		if (!ssm) {
			cerr << "Error: Out of memory for ss_m" << endl;
			retval = 1;
			return;
		}
		/* DO WORK: 
		 * Any other work with the storage manager should be
		 * done here or in child threads forked by this one,
		 * and passed this ss_m as an argument, then
		 * AWAIT CHILDREN (via join)
		 */

		delete ssm;
		ssm = NULL;
	}
};


int
main(int argc, char* argv[])
{
	/* the storage manager requires that certain options are set.
	 * Get them from the file named EXAMPLE_SHORECONFIG
	 */
    option_group_t options(3);// three levels.
    W_COERCE(options.add_class_level("startstop"));	
    W_COERCE(options.add_class_level("server"));	// server or client
    W_COERCE(options.add_class_level(argv[0]));	// program name
	// Here add your options to this group.

    W_COERCE(ss_m::setup_options(&options));
	// adds the storage manager options to this group 
	
	{
		/* scan the file to get values for the options */
		/* Note that we could scan the command line also or instead: you
		 * can find an example of this is src/sm/tests/options.cpp
		 */
		w_ostrstream      err_stream;
		const char* opt_file = "EXAMPLE_SHORECONFIG"; 	// option config file
		option_file_scan_t opt_scan(opt_file, &options);

		// scan the file and override any current option settings
		// options names must be spelled correctly
		w_rc_t rc = opt_scan.scan(true /*override*/, err_stream, true);
		if (rc.is_error()) {
			cerr << "Error in reading option file: " << opt_file << endl;
			cerr << "\t" << err_stream.c_str() << endl;
			return 1;
		}
    }
	{
		/* check that all required options have been set */
		w_ostrstream      err_stream;
		w_rc_t rc = options.check_required(&err_stream);
        if (rc.is_error()) {
            cerr << "These required options are not set:" << endl;
            cerr << err_stream.c_str() << endl;
			return 1;
        }
    }

	/* GO */
    smthread_user_t *smtu = new smthread_user_t(argc, argv);
    if (!smtu)
            W_FATAL(fcOUTOFMEMORY);

    w_rc_t e = smtu->fork();
    if(e.is_error()) {
        cerr << "error forking thread: " << e <<endl;
        return 1;
    }
    e = smtu->join();
    if(e.is_error()) {
        cerr << "error forking thread: " << e <<endl;
        return 1;
    }
    int        rv = smtu->retval;
    delete smtu;
    return rv;
}

