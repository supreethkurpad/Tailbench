/*<std-header orig-src='shore'>

 $Id: init_config_options.cpp,v 1.3 2010/06/08 22:28:15 nhall Exp $

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

/**\anchor init_config_options_example */
/*
 * This file implements configuration option processing for
 * both the client and the server.
 */

#include <w_stream.h>
#include <cstring>

// since this file only deals with the SSM option package,
// rather than including sm_vas.h, just include what's needed for
// options:

#include "w.h"
#include "option.h"
#include <w_strstream.h>

/*
 * init_config_options intializes configuration options 
 *
 * The options parameter is the option group holding all the options.
 * It is assumed that all SSM options have been added if called
 * by the server.
 *
 * The prog_type parameter is should be either "client" or "server".
 *
 * The argc and argv parameters should be argc and argv from main().
 * Recognized options will be located in argv and removed.  argc
 * is changed to reflect the removal.
 *
 */

w_rc_t
init_config_options(option_group_t& options,
            const char* prog_type,
            int& argc, char** argv)
{

    w_rc_t rc;    // return code

    // set prog_name to the file name of the program without the path
    char* prog_name = strrchr(argv[0], '/');
    if (prog_name == NULL) {
        prog_name = argv[0];
    } else {
        prog_name += 1; /* skip the '/' */
        if (prog_name[0] == '\0')  {
            prog_name = argv[0];
        }
    }
 
    W_DO(options.add_class_level("example")); 
    W_DO(options.add_class_level(prog_type));    // server or client
    W_DO(options.add_class_level(prog_name));    // program name

    // read the example config file to set options
    {
        w_ostrstream      err_stream;
        const char* opt_file = "EXAMPLE_SHORECONFIG";     // option config file
        option_file_scan_t opt_scan(opt_file, &options);

        // scan the file and override any current option settings
        // options names must be spelled correctly
        rc = opt_scan.scan(true /*override*/, err_stream, true);
        if (rc.is_error()) {
            cerr << "Error in reading option file: " << opt_file << endl;
            cerr << "\t" << err_stream.c_str() << endl;
            return rc;
        }
    }

    // parse argv for options
    if (!rc.is_error()) {
        // parse command line
        w_ostrstream      err_stream;
        rc = options.parse_command_line((const char **)argv, 
                argc, 2, &err_stream);
        err_stream << ends;
        if (rc.is_error()) {
            cerr << "Error on Command line " << endl;
            cerr << "\t" << w_error_t::error_string(rc.err_num()) << endl;
            cerr << "\t" << err_stream.c_str() << endl;
        return rc;
        }
    }
 
    // check required options
    {
        w_ostrstream      err_stream;
        rc = options.check_required(&err_stream);
        if (rc.is_error()) {
            cerr << "These required options are not set:" << endl;
            cerr << err_stream.c_str() << endl;
            return rc;
        }
    } 

    return RCOK;
}
