/*<std-header orig-src='shore' incl-file-exclusion='SSH_H'>

 $Id: smsh.h,v 1.2 2010/05/26 01:20:52 nhall Exp $

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

#ifndef SSH_H
#define SSH_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SSH_ERROR(err) \
    {                                                           \
        cerr << "ssh error (" << err << ") at file " << __FILE__\
             << ", line " << __LINE__ << endl;                  \
        W_FATAL(fcINTERNAL);                                    \
    }

#undef DO

#define DO(x)                                                   \
do  {                                                           \
        int err = x;                                            \
        if (err)  { SSH_ERROR(err); }                           \
} while (0)

#define COMM_ERROR(err) \
        cerr << "communication error: code " << (err) \
             << ", file " << __FILE__ << ", line " << __LINE__

#define COMM_FATAL(err) { COMM_ERROR(err); W_FATAL(fcINTERNAL); }

//
// Start and stop client communication listening thread
//
extern rc_t stop_comm();
extern rc_t start_comm();

struct linked_vars {
    w_base_t::uint4_t  sm_page_sz;
    w_base_t::uint4_t  sm_max_exts;
    w_base_t::uint4_t  sm_max_vols;
    w_base_t::uint4_t  sm_max_servers;
    w_base_t::uint4_t  sm_max_keycomp;
    w_base_t::uint4_t  sm_max_dir_cache;
    w_base_t::uint4_t  sm_max_rec_len;
    w_base_t::uint4_t  sm_srvid_map_sz;
    w_base_t::uint4_t  verbose_flag;
    w_base_t::uint4_t  verbose2_flag;
    w_base_t::uint4_t  compress_flag;
    w_base_t::uint4_t  log_warn_callback_flag;
    w_base_t::uint4_t  instrument_flag;
} ;
extern linked_vars linked;

extern bool force_compress;
extern bool log_warn_callback;
extern w_rc_t out_of_log_space (xct_i*, xct_t*&, ss_m::fileoff_t, 
		                         ss_m::fileoff_t, const char *logdir);
extern w_rc_t get_archived_log_file (const char *, ss_m::partition_number_t);
extern void dispatch_init();

/*<std-footer incl-file-exclusion='SSH_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
