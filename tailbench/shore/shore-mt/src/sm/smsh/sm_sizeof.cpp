/*<std-header orig-src='shore'>

 $Id: sm_sizeof.cpp,v 1.10.2.6 2010/03/19 22:20:31 nhall Exp $

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

#include "shell.h"
#include <iostream>

#include "page_s.h"
#include "lock_x.h"
#if 0
#define SM_SOURCE
#include "sm_int_2.h"
#include "logrec.h"
#include "logdef_gen.cpp"
#endif
#if 0
/* XXX should break btree overhead out into its own _s include file */
#include "page.h"
#include "btree_p.h"
#endif


#define    TYPE_INFO(type)    { #type , sizeof(type) }
#define    SIZE_INFO(size) { #size, size }

struct smsh_type_info_t {
    const char    *type;
    size_t        size;
};

static const smsh_type_info_t    smsh_type_info[] = {
    TYPE_INFO(rid_t),
    TYPE_INFO(stid_t),
    TYPE_INFO(sdesc_t),
    TYPE_INFO(vec_t),
    TYPE_INFO(cvec_t),
    TYPE_INFO(vid_t),
    TYPE_INFO(lvid_t),
    TYPE_INFO(smthread_t),
    TYPE_INFO(sthread_t),
//    TYPE_INFO(store_property_t),
    TYPE_INFO(key_info_t),
    TYPE_INFO(sort_keys_t),
    TYPE_INFO(smsize_t),
//    TYPE_INFO(smksize_t),
    TYPE_INFO(sm_du_stats_t),
    TYPE_INFO(SmVolumeMetaStats),
    TYPE_INFO(SmFileMetaStats),
    TYPE_INFO(page_s),
    TYPE_INFO(page_s::space_t),
    SIZE_INFO(page_s::hdr_sz),
    SIZE_INFO(page_s::data_sz),
    SIZE_INFO(page_s::max_slot),
    { "page_s::offsetof(lsn1)", w_offsetof(page_s, lsn1) }, 
    { "page_s::offsetof(pid)", w_offsetof(page_s, pid) }, 
    { "page_s::offsetof(next)", w_offsetof(page_s, next) }, 
    { "page_s::offsetof(prev)", w_offsetof(page_s, prev) }, 
    { "page_s::offsetof(space)", w_offsetof(page_s, space) }, 
    // { "page_s::offsetof(space._tid)", w_offsetof(page_s, space._tid) }, 
    // { "page_s::offsetof(space._nfree)", w_offsetof(page_s, space._nfree) }, 
    // { "page_s::offsetof(space._nrsvd)", w_offsetof(page_s, space._nrsvd) }, 
    // { "page_s::offsetof(space._xct_rsvd)", w_offsetof(page_s, space._xct_rsvd) }, 
    { "page_s::offsetof(end)", w_offsetof(page_s, end) }, 
    { "page_s::offsetof(nslots)", w_offsetof(page_s, nslots) }, 
    { "page_s::offsetof(nvacant)", w_offsetof(page_s, nvacant) }, 
    { "page_s::offsetof(tag)", w_offsetof(page_s, tag) }, 
    { "page_s::offsetof(store_flags)", w_offsetof(page_s, _private_store_flags) }, 
    { "page_s::offsetof(page_flags)", w_offsetof(page_s, page_flags) }, 
    { "page_s::offsetof(data)", w_offsetof(page_s, data) }, 
    { "page_s::offsetof(reserved_slot)", w_offsetof(page_s, reserved_slot) }, 
    { "page_s::offsetof(slot)", w_offsetof(page_s, slot) }, 
    { "page_s::offsetof(lsn2)", w_offsetof(page_s, lsn2) }, 
    TYPE_INFO(rectag_t),
    TYPE_INFO(record_t),
    { "record_t::offsetof(info)", w_offsetof(record_t, info) },
//    TYPE_INFO(btrec_t),
//    TYPE_INFO(file_p),
//    TYPE_INFO(btree_p),
//    TYPE_INFO(rtree_p),    
    TYPE_INFO(snum_t),
    TYPE_INFO(shpid_t),
    TYPE_INFO(extid_t),
    TYPE_INFO(shrid_t),
    TYPE_INFO(tid_t),
    TYPE_INFO(lpid_t),
    TYPE_INFO(pin_i),
//    TYPE_INFO(extlink_t),
//    TYPE_INFO(histoid_t),
//    TYPE_INFO(histoid_update_t),
//    TYPE_INFO(lg_tag_chunks_h),
//    TYPE_INFO(lid_entry_t),
#if !defined(FORCE_EGCS) && !defined(GCC_BROKEN_WARNINGS) && !defined(GCC_VER_3_WARNINGS)
    /* XXX this works on sun solaris with gcc-2.7.2.3 and gcc-2.95.3,
       but not on other platforms and or ocmpilers. */
    // something goes weird and types aren't declared w/ those compilers?
    TYPE_INFO(lock_request_t),
    TYPE_INFO(xct_lock_info_t),
#endif
//    TYPE_INFO(log_buf),
//    TYPE_INFO(logrec_t),
//    TYPE_INFO(pginfo_t),
//    TYPE_INFO(store_histo_t),
//    TYPE_INFO(Pmap),
    TYPE_INFO(sm_stats_info_t),
    TYPE_INFO(sm_config_info_t),
//    TYPE_INFO(bf_core_m),
    TYPE_INFO(file_pg_stats_t),
    TYPE_INFO(lgdata_pg_stats_t),
    TYPE_INFO(lgindex_pg_stats_t),
    TYPE_INFO(file_stats_t),
//    TYPE_INFO(xct_state_t),
    TYPE_INFO(lsn_t),
    TYPE_INFO(scan_index_i),
    TYPE_INFO(scan_file_i),
    TYPE_INFO(append_file_i),
    TYPE_INFO(scan_rt_i)
//    TYPE_INFO(xct_t),
//    TYPE_INFO(xct_impl)
#ifdef USE_2PC
    ,TYPE_INFO(sm2pc_stats_t)
    ,TYPE_INFO(DeadlockMsg)
#endif
    ,TYPE_INFO(off_t)
};

#ifndef numberof
#define    numberof(x)    (sizeof(x)/sizeof(x[0]))
#endif
    
/* XXX print sizes of requested SM structures */
int    t_sm_sizeof(Tcl_Interp *, int argc, TCL_AV char **argv)
{
    cout << "t_sizeof" << endl;
    if (argc < 1)
        return TCL_ERROR;

    if (argc == 1) {
        for (unsigned j = 0; j < numberof(smsh_type_info); j++) {
            const smsh_type_info_t &t = smsh_type_info[j];

            cout << "\tsizeof(" << t.type << ") => "
                 << t.size << endl;
        }

        return TCL_OK;
    }

    for (int i = 1; i < argc; i++) {
        const char *arg = argv[i];
        const size_t    arg_len = strlen(arg);

        for (unsigned j = 0; j < numberof(smsh_type_info); j++) {
            const smsh_type_info_t &t = smsh_type_info[j];

            if (strncmp(arg, t.type, arg_len) == 0)
                cout << "\tsizeof(" << t.type << ") => "
                     << t.size << endl;
        }
    }

    return TCL_OK;
}

