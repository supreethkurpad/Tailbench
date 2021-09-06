/*<std-header orig-src='shore'>

 $Id: smsh.cpp,v 1.1.2.15 2010/03/19 22:20:31 nhall Exp $

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

/* 
 * tclTest.c --
 *
 *        Test driver for TCL.
 *
 * Copyright 1987-1991 Regents of the University of California
 * All rights reserved.
 *
 * Permission to use, copy, modify, and distribute this
 * software and its documentation for any purpose and without
 * fee is hereby granted, provided that the above copyright
 * notice appears in all copies.  The University of California
 * makes no representations about the suitability of this
 * software for any purpose.  It is provided "as is" without
 * express or implied warranty.
 */

#include <cstdlib>
#include <climits>
#include "w_stream.h"
#include <cstring>
#include "w_debug.h"
#include "shell.h"

#ifndef __GNU_LIBRARY__
#define __GNU_LIBRARY__
#endif
#include "w_getopt.h"
// DEAD #include <unix_stats.h>

/* tcl */
#ifdef EXTERN
#undef EXTERN
#endif
#include <tcl.h>
#include "tcl_workaround.h"
#include "tcl_thread.h"
#include "smsh.h"
#include <sthread_stats.h>

const char* tcl_init_cmd = 0; // DEAD?

bool instrument = true;
bool verbose = false;
bool verbose2 = false;
bool force_compress = false;
bool log_warn_callback = false;
const char* f_arg = NULL;
const char *argv0 = NULL;
bool interactive = false;

// NOTE: NOT TLS but these are read-only 
struct linked_vars linked;

// Error codes for smsh
enum {        SSH_MIN_ERROR = 50000,
        SSH_OS,
        SSH_FAILURE,
        SSH_COMMAND_LINE,
        SSH_MAX_ERROR};

static const w_error_info_t smsh_error_list[]=
{
    {SSH_OS,        "operating system error"},
    {SSH_FAILURE,"general smsh program failure"},
    {SSH_COMMAND_LINE,"problem with command line"},
};

/*
 * Function print_usage print smsh usage information to
 * err_stream.  The long_form parameter indicates whether
 * a longer and more detailed description should be printed.
 * The options parameter is the list of all options used
 * by smsh and the layers is calls.
 */
void print_usage(ostream& err_stream, const char* prog_name,
                 bool long_form, option_group_t& options)
{
    if (!long_form) {
        err_stream << "usage: " << prog_name
             << " [-h] [-v] [-O] [-C] [-s] [-L] [-V] [-f script_file]" 
             << " [-t assignment]"
             << endl;
        options.print_usage(long_form, err_stream);
    } else {
        err_stream << "usage: " << prog_name << endl;
        err_stream << "switches:\n"
            << "\t-h: print help message and exit\n"
            << "\t-v: verbose printing of all option values\n"
            << "\t-V: verbose printing of test results\n"
            << "\t-O: use old sort implementation\n"
            << "\t-C: compress btrees\n"
            << "\t-s: print stats at exit\n"
            << "\t-L: test log warning callback\n"
            << "\t-f script_file: specify script to run\n"
        << "options:" << endl;
        options.print_usage(true, err_stream);
    }
    return;
}


// DEAD unix_stats U;
#ifndef SOLARIS2
// DEAD unix_stats C(RUSAGE_CHILDREN);
#endif

int main(int argc, const char** argv)
{
    argv0 = argv[0];

#if 0        /* Can't use with tools that want smsh output */
#if defined(W_TRACE)
    char *c = getenv("DEBUG_FLAGS");
    if(c!=NULL) cout << "DEBUG_FLAGS =|" << c << "|" << endl;
    else cout << "DEBUG_FLAGS is not set. " <<endl;
    c = getenv("DEBUG_FILE");
    if(c!=NULL) cout << "DEBUG_FILE =|" << c << "|" << endl;
    else cout << "DEBUG_FILE is not set. " <<endl;

    const char *cc = _w_fname_debug__;
    if(cc!=NULL) cout << "_w_fname_debug__ =|" << cc << "|" << endl;
    else cout << "_w_fname_debug__ is not set. " <<endl;
#else
    cout << "Debugging not configured." << endl;
#endif
#endif

    bool print_stats = false;


    // DEAD U.start();
#ifndef SOLARIS2
    // DEAD C.start();
#endif

    // Set up smsh related error codes
    if (! (w_error_t::insert(
                "ss_m shell",
                smsh_error_list, SSH_MAX_ERROR - SSH_MIN_ERROR - 1))) {
        abort();
    }


    /*
     * The following section of code sets up all the various options
     * for the program.  The following steps are performed:
        - determine the name of the program
        - setup an option group for the program
        - initialize the ssm options
        - scan default option configuration files ($HOME/.shoreconfig .shoreconfig)
        - process any options found on the command line
        - use getopt() to process smsh specific flags on the command line
        - check that all required options are set before initializing sm
     */         

    // set prog_name to the file name of the program
    const char* prog_name = strrchr(argv[0], '/');
    if (prog_name == NULL) {
            prog_name = argv[0];
    } else {
            prog_name += 1; /* skip the '/' */
            if (prog_name[0] == '\0')  {
                    prog_name = argv[0];
            }
    }

    /*
     * Set up and option group (list of options) for use by
     * all layers of the system.  Level "smsh" indicates
     * that the program is a a part to the smsh test suite.
     * Level "server" indicates
     * the type of program (the smsh server program).  The third
     * level is the program name itself.
     */
    option_group_t options(3);
    W_COERCE(options.add_class_level("smsh"));
    W_COERCE(options.add_class_level("server"));
    W_COERCE(options.add_class_level(prog_name));

    /*
     * Set up and smsh option for the name of the tcl library directory
     * and the name of the .smshrc file.
     */
    option_t* smsh_libdir;
    option_t* smsh_smshrc;
    W_COERCE(options.add_option("smsh_libdir", "directory name", NULL,
                "directory for smsh tcl libraries",
                true, option_t::set_value_charstr, smsh_libdir));
    W_COERCE(options.add_option("smsh_smshrc", "rc file name", ".smshrc",
                "full path name of the .smshrc file",
                false, option_t::set_value_charstr, smsh_smshrc));

    // have the sm add its options to the group
    W_COERCE(ss_m::setup_options(&options));


    /*
     * Scan the default configuration files: $HOME/.shoreconfig, .shoreconfig.  Note
     * That OS errors are ignored since it is not an error
     * for this file to not be found.
     */
    rc_t        rc;
    {
    char                opt_file[ss_m::max_devname+1];
    for(int file_num = 0; file_num < 2 && !rc.is_error(); file_num++) {
        // scan default option files
        w_ostrstream        err_stream;
        const char*        config = ".shoreconfig";
        if (file_num == 0) {
            if (!getenv("HOME")) {
                // ignore it ...
                // cerr << "Error: environment variable $HOME is not set" << endl;
                // rc = RC(SSH_FAILURE);
                break;
            }
            if (sizeof(opt_file) <= strlen(getenv("HOME")) + strlen("/") + strlen(config) + 1) {
                cerr << "Error: environment variable $HOME is too long" << endl;
                rc = RC(SSH_FAILURE);
                break;
            }
            strcpy(opt_file, getenv("HOME"));
            strcat(opt_file, "/");
            strcat(opt_file, config);
        } else {
            w_assert3(file_num == 1);
            strcpy(opt_file, "./");
            strcat(opt_file, config);
        }
        {
            option_file_scan_t opt_scan(opt_file, &options);
            rc = opt_scan.scan(true, err_stream);
            err_stream << ends;
            if (rc.is_error()) {
                // ignore OS error messages
                if (rc.err_num() == fcOS) {
                    rc = RCOK;
                } else {
                    // this error message is kind of gross but is
                    // sufficient for now
                    cerr << "Error in reading option file: " << opt_file << endl;
                    //cerr << "\t" << w_error_t::error_string(rc.err_num()) << endl;
                    cerr << "\t" << err_stream.c_str() << endl;
                }
            }
        }
    }
    }

    /* 
     * Assuming there has been no error so far, the command line
     * is processed for any options in the option group "options".
     */
    if (!rc.is_error()) {
        // parse command line
        w_ostrstream        err_stream;
        rc = options.parse_command_line(argv, argc, 2, &err_stream);
        err_stream << ends;
        if (rc.is_error()) {
            cerr << "Error on command line " << endl;
            cerr << "\t" << w_error_t::error_string(rc.err_num()) << endl;
            cerr << "\t" << err_stream.c_str() << endl;
            print_usage(cerr, prog_name, false, options);
        }
    } 

    /* 
     * Assuming there has been no error so far, the command line
     * is processed for any smsh specific flags.
     */
    int option;
    //if (!rc) 
    {  // do even if error so that smsh -h can be recognized
        bool verbose_opt = false; // print verbose option values
        while ((option = getopt(argc, (char * const*) argv, "Cf:hLOsTvV")) != -1) {
            switch (option) {
            case 'T':
                extern bool logtrace;
                logtrace = true;
                break;
            case 'O':
                    // Force use of old sort
                cout << "Force use of old sort implementation." <<endl;
                newsort = false;
                break;

            case 'C':
                // force compression of btrees
                force_compress = true;
                break;

            case 's':
                print_stats = true;
                break;

            case 'f':
                f_arg = optarg;
                break;

            case 'L':
                // use log warning callback
                log_warn_callback = true;
                break;

            case 'h':
                // print a help message describing options and flags
                print_usage(cerr, prog_name, true, options);
                // free rc structure to avoid complaints on exit
                W_IGNORE(rc);
                goto done;
                break;
            case 'v':
                verbose_opt = true;
                break;
            case 'V':
                verbose = true;
                break;
            default:
                cerr << "unknown flag: " << option << endl;
                rc = RC(SSH_COMMAND_LINE);
            }
        }

        if (verbose_opt) {
            options.print_values(false, cerr);
        }
    }

    /*
     * Assuming no error so far, check that all required options
     * in option_group_t options are set.  
     */
    if (!rc.is_error()) {
        // check required options
        w_ostrstream        err_stream;
        rc = options.check_required(&err_stream);
        err_stream << ends;
        if (rc.is_error()) {
            cerr << "These required options are not set:" << endl;
            cerr << err_stream.c_str() << endl;
            print_usage(cerr, prog_name, false, options);
        }
    } 


    /* 
     * If there have been any problems so far, then exit
     */
    if (rc.is_error()) {
        // free the rc error structure to avoid complaints on exit
        W_IGNORE(rc);
        goto errordone;
    }

    /*
     * At this point, all options and flags have been properly
     * set.  What follows is initialization for the rest of
     * the program.  The ssm will be started by a tcl_thread.
     */

#if 0
    char* merged_args = Tcl_Merge(argc, (char **) argv);
    if(!merged_args) {
        cerr << "Tcl_Merge failed." <<endl; exit(1); 
        goto errordone;
    }
#endif

    // setup table of sm commands - doesn't involve the Tcl_Interp
    dispatch_init();

    // set up the linked variables
    // either these should be read-only or
    // they need to be made thread-safe.  We can assume for smsh they
    // are for all purposes read-only, since only the mama thread sets
    // them in the scripts.
    linked.sm_page_sz = ss_m::page_sz;
    linked.sm_max_exts = ss_m::max_exts;
    linked.sm_max_vols = ss_m::max_vols;
    linked.sm_max_servers = ss_m::max_servers;
    linked.sm_max_keycomp = ss_m::max_keycomp;
    linked.sm_max_dir_cache = ss_m::max_dir_cache;
    linked.sm_max_rec_len = ss_m::max_rec_len;
    linked.sm_srvid_map_sz = ss_m::srvid_map_sz;
    linked.verbose_flag = verbose?1:0;
    linked.verbose2_flag = verbose2?1:0;
    linked.instrument_flag = instrument?1:0;
    linked.compress_flag = force_compress?1:0;
    linked.log_warn_callback_flag = log_warn_callback?1:0;

    {
        int tty = isatty(0);
        interactive = tty && f_arg;
    }

    // Create the main tcl_thread
    {
        tcl_thread_t* tcl_thread = NULL;
        bool ok = true;

        if(ok) {
            if (f_arg) {
                TCL_AV char* av[2];
                av[0] = TCL_AV1 "source";
                av[1] = f_arg;
                // smsh -f <file>
#if 0
            cerr << __func__ << " " << __LINE__ << " " << __FILE__
                << " libdir " << smsh_libdir->value()
                << " msshrc " << smsh_smshrc->value()
                << endl;
#endif
                tcl_thread = new tcl_thread_t(2, av, 
                                smsh_libdir->value(),
                                smsh_smshrc->value()
                                );
            } else {
                // interactive
                /*
                cerr << __func__ << " " << __LINE__ << " " << __FILE__
                << " INTERACTIVE libdir " << smsh_libdir->value()
                << " msshrc " << smsh_smshrc->value()
                << endl;
                */
                tcl_thread = new tcl_thread_t(0, 0,
                                smsh_libdir->value(),
                                smsh_smshrc->value()
                                );
            }
            assert(tcl_thread);
#if 0
            fprintf(stderr, 
                "Forking main (interactive=%s) tcl thread @ 0x%p id %d ... \n",
                (const char *)(f_arg ? "false" : "true"),
                ((void*)tcl_thread), tcl_thread->id);
#endif

            W_COERCE( tcl_thread->fork() );
            W_COERCE( tcl_thread->join() );
#if 0
            fprintf(stderr, 
                "Joined main (interactive=%s) tcl thread @ 0x%p id %d ... \n",
                (const char *)(f_arg ? "false" : "true"),
                ((void*)tcl_thread), tcl_thread->id);
#endif

            delete tcl_thread;
        }
    }


    // Shutdown TCL and have it deallocate resources still held!
    Tcl_Finalize();

    // DEAD U.stop(1); // 1 iteration
#ifndef SOLARIS2
    // DEAD C.stop(1); // 1 iteration
#endif

    if(print_stats) 
    {
        cout << "Thread stats" <<endl;
        sthread_t::dump_stats(cout);
        cout << endl;

        // DEAD cout << "Unix stats for parent:" <<endl;
        // DEAD cout << U << endl << endl;

#ifndef SOLARIS2
        // DEAD cout << "Unix stats for children:" <<endl;
        // DEAD cout << C << endl << endl;
#endif
    }
    cout << flush;

done:
    clean_up_shell();
    fprintf(stderr, "%d tcl threads ran\n", num_tcl_threads_ttl);
    return 0;

errordone:
    clean_up_shell();
    return 1;
}

