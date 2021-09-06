/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore' incl-file-exclusion='SM_BASE_H'>

 $Id: sm_base.h,v 1.149 2010/06/15 17:30:07 nhall Exp $

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

#ifndef SM_BASE_H
#define SM_BASE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/**\file sm_base.h
 * \ingroup Macros
 */

#ifdef __GNUG__
#pragma interface
#endif

#include <climits>
#ifndef OPTION_H
#include "option.h"
#endif
#ifndef __opt_error_def_gen_h__
#include "opt_error_def_gen.h"
#endif

#include <vector>

class ErrLog;
class sm_stats_info_t;
class xct_t;
class xct_i;

class device_m;
class io_m;
class bf_m;
class comm_m;
class log_m;
class lock_m;

class tid_t;
class option_t;

class stid_t;
class rid_t;

#ifndef        SM_EXTENTSIZE
#define        SM_EXTENTSIZE        8
#endif
#ifndef        SM_LOG_PARTITIONS
#define        SM_LOG_PARTITIONS        8
#endif

typedef   w_rc_t        rc_t;


/* This structure collects the depth on construction
 * and checks that it matches the depth on destruction; this
 * is to ensure that we haven't forgotten to release
 * an anchor somewhere.
 * It's been extended to check the # times
 * we have acquired the 1thread_log_mutex. 
 *
 * We're defining the CHECK_NESTING_VARIABLES macro b/c
 * this work is spread out and we want to have 1 place to
 * determine whether it's turned on or off; don't want to 
 * make the mistake of changing the debug level (on which
 * it depends) in only one of several places.
 *
 * NOTE: this doesn't work in a multi-threaded xct context.
 * That's b/c the check is too late -- once the count goes
 * to zero, another thread can change it and throw off all the
 * counts. To be sure, we'd have to use a TLS copy as well
 * as the common copy of these counts.
 */
#if W_DEBUG_LEVEL > 0
#define CHECK_NESTING_VARIABLES 1
#else
#define CHECK_NESTING_VARIABLES 0
#endif
struct check_compensated_op_nesting {
#if CHECK_NESTING_VARIABLES
    xct_t* _xd;
    int _depth;
    int _depth_of_acquires;
    int _line;
    const char *const _file;
    // static methods are so we can avoid having to
    // include xct.h here.
    static int compensated_op_depth(xct_t* xd, int dflt);
    static int acquire_1thread_log_depth(xct_t* xd, int dflt);

    check_compensated_op_nesting(xct_t* xd, int line, const char *const file)
    : _xd(xd), 
    _depth(_xd? compensated_op_depth(_xd, 0) : 0), 
    _depth_of_acquires(_xd? acquire_1thread_log_depth(_xd, 0) : 0), 
    _line(line),
    _file(file)
    {
    }

    ~check_compensated_op_nesting() {
        if(_xd) {
            if( _depth != compensated_op_depth(_xd, _depth) ) {
                fprintf(stderr, 
                    "th.%d check_compensated_op_nesting(%d,%s) depth was %d is %d\n",
                    sthread_t::me()->id,
                    _line, _file, _depth, compensated_op_depth(_xd, _depth));
            }

            if(_depth_of_acquires != acquire_1thread_log_depth(_xd, _depth)) {
                fprintf(stderr, 
                "th.%d check_acquire_1thread_log_depth (%d,%s) depth was %d is %d\n",
                    sthread_t::me()->id,
                    _line, _file, _depth_of_acquires, 
                    acquire_1thread_log_depth(_xd, _depth));
            }

            w_assert0(_depth == compensated_op_depth(_xd, _depth));
            w_assert0(_depth_of_acquires == acquire_1thread_log_depth(_xd, _depth));
        }
    }
#else
    check_compensated_op_nesting(xct_t*, int, const char *const) { }
#endif
};



/**\cond skip */

/**\brief Encapsulates a few types used in the API */
class smlevel_0 : public w_base_t {
public:
    enum { eNOERROR = 0, eFAILURE = -1 };
    enum { 
        page_sz = SM_PAGESIZE,        // page size (SM_PAGESIZE is set by makemake)
        ext_sz = SM_EXTENTSIZE,        // extent size
        max_exts = max_int4,        // max no. extents, must fit extnum_t
#if defined(_POSIX_PATH_MAX)
        max_devname = _POSIX_PATH_MAX,        // max length of unix path name
    // BEWARE: this might be larger than you want.  Array sizes depend on it.
    // The default might be small enough, e.g., 256; getconf() yields the upper
    // bound on this value.
#elif defined(MAXPATHLEN)
        max_devname = MAXPATHLEN,
#else
        max_devname = 1024,        
#endif
        max_vols = 20,                // max mounted volumes
        max_xct_thread = 20,        // max threads in a xct
        max_servers = 15,       // max servers to be connected with
        max_keycomp = 20,        // max key component (for btree)
        max_openlog = SM_LOG_PARTITIONS,        // max # log partitions
        max_dir_cache = max_vols * 10,

        /* XXX I want to propogate sthread_t::iovec_max here, but
           it doesn't work because of sm_app.h not including
           the thread package. */
        max_many_pages = 8,

        srvid_map_sz = (max_servers - 1) / 8 + 1,
        ext_map_sz_in_bytes = ((ext_sz + 7) / 8),

        dummy = 0
    };

    enum {
        max_rec_len = max_uint4
    };

    typedef sthread_base_t::fileoff_t fileoff_t;
    /*
     * Sizes-in-Kbytes for for things like volumes and devices.
     * A KB is assumes to be 1024 bytes.
     * Note: a different type was used for added type checking.
     */
    typedef sthread_t::fileoff_t smksize_t;
    typedef w_base_t::base_stat_t base_stat_t; 

    /*
     * rather than automatically aborting the transaction, when the
     * _log_warn_percent is exceeded, this callback is made, with a
     * pointer to the xct that did the writing, and with the
     * expectation that the result will be one of:
     * - return value == RCOK --> proceed
     * - return value == eUSERABORT --> victim to abort is given in the argument
     *
     * The server has the responsibility for choosing a victim and 
     * for aborting the victim transaction. 
     *
     */

    /**\brief Log space warning callback function type.  
     *
     * For more details of how this is used, see the constructor ss_m::ss_m().
     *
     * Storage manager methods check the available log space. 
     * If the log is in danger of filling to the point that it will be
     * impossible to abort a transaction, a
     * callback is made to the server.  The callback function is of this type.
     * The danger point is a threshold determined by the option sm_log_warn. 
     *
     * The callback
     * function is meant to choose a victim xct and 
     * tell if the xct should be
     * aborted by returning RC(eUSERABORT).  
     *
     * Any other RC value is returned to the server through the call stack.
     *
     * The arguments:
     * @param[in] iter    Pointer to an iterator over all xcts.
     * @param[out] victim    Victim will be returned here. This is an in/out
     * paramter and is initially populated with the transaction that is
     * attached to the running thread.
     * @param[in] curr    Bytes of log consumed by active transactions.
     * @param[in] thresh   Threshhold just exceeded. 
     * @param[in] logfile   Character string name of oldest file to archive.
     *                     
     *  This function must be careful not to return the same victim more
     *  than once, even though the callback may be called many 
     *  times before the victim is completely aborted.
     *
     *  When this function has archived the given log file, it needs
     *  to notify the storage manager of that fact by calling
     *  ss_m::log_file_was_archived(logfile)
     */
    typedef w_rc_t (*LOG_WARN_CALLBACK_FUNC) (
            xct_i*      iter,     
            xct_t *&    victim, 
            fileoff_t   curr, 
            fileoff_t   thresh, 
            const char *logfile
        );
    /**\brief Callback function type for restoring an archived log file.
     *
     * @param[in] fname   Original file name (with path).
     * @param[in] needed   Partition number of the file needed.
     *
     *  An alternative to aborting a transaction (when the log fills)
     *  is to archive log files.
     *  The server can use the log directory name to locate these files,
     *  and may use the iterator and the static methods of xct_t to 
     *  determine which log file(s) to archive.
     *
     *  Archiving and removing the older log files will work only if
     *  the server also provides a LOG_ARCHIVED_CALLBACK_FUNCTION 
     *  to restore the
     *  archived log files when the storage manager needs them for
     *  rollback.
     *  This is the function type used for that purpose.
     *
     *  The function must locate the archived log file containing for the
     *  partition number \a num, which was a suffix of the original log file's
     *  name.
     *  The log file must be restored with its original name.  
     */
    typedef    w_base_t::uint4_t partition_number_t; 
    typedef w_rc_t (*LOG_ARCHIVED_CALLBACK_FUNC) (
            const char *fname,
            partition_number_t num
        );

    typedef w_rc_t (*RELOCATE_RECORD_CALLBACK_FUNC) (
	   vector<rid_t>&    old_rids, 
           vector<rid_t>&    new_rids
       );

/**\cond skip */
    enum switch_t {
        ON = 1,
        OFF = 0
    };
/**\endcond skip */

    /**\brief Comparison types used in scan_index_i
     * \enum cmp_t
     * Shorthand for CompareOp.
     */
    enum cmp_t { bad_cmp_t=badOp, eq=eqOp,
                 gt=gtOp, ge=geOp, lt=ltOp, le=leOp };


    /* used by lock escalation routines */
    enum escalation_options {
        dontEscalate        = max_int4_minus1,
        dontEscalateDontPassOn,
        dontModifyThreshold        = -1
    };

    /**\brief Types of stores.
     * \enum store_t
     */
    enum store_t { 
        t_bad_store_t, 
        /// a b-tree or r-tree index
        t_index, 
        /// a file of records
        t_file, 
        /// t_lgrec is used for storing large record pages 
        /// and is always associated with some t_file store
        t_lgrec 
    };
    
    // types of indexes

    /**\brief Index types */
    enum ndx_t { 
        t_bad_ndx_t,             // illegal value
        t_btree,                 // B+tree with duplicates
        t_uni_btree,             // Unique-key btree
        t_rtree,                 // R*tree
	t_mrbtree,       // Multi-rooted B+tree with regular heap files   
	t_uni_mrbtree,          
	t_mrbtree_l,          // Multi-rooted B+tree where a heap file is pointed by only one leaf page 
	t_uni_mrbtree_l,               
	t_mrbtree_p,     // Multi-rooted B+tree where a heap file belongs to only one partition
	t_uni_mrbtree_p
    };

    /**\enum concurrency_t 
     * \brief 
     * Lock granularities 
     * \details
     * - t_cc_bad Illegal
     * - t_cc_none No locking
     * - t_cc_record Record-level locking for files & records
     * - t_cc_page Page-level locking for files & records 
     * - t_cc_file File-level locking for files & records 
     * - t_cc_vol Volume-level locking for files and indexes 
     * - t_cc_kvl Key-value locking for B+-Tree indexes
     * - t_cc_im Aries IM locking for B+-Tree indexes : experimental
     * - t_cc_modkvl Modified key-value locking: experimental
     * - t_cc_append Used internally \todo true?
     */
    enum concurrency_t {
        t_cc_bad,                // this is an illegal value
        t_cc_none,                // no locking
        t_cc_record,                // record-level
        t_cc_page,                // page-level
        t_cc_file,                // file-level
        t_cc_vol,
        t_cc_kvl,                // key-value
        t_cc_im,                 // ARIES IM, not supported yet
        t_cc_modkvl,                 // modified ARIES KVL, for paradise use
        t_cc_append                 // append-only with scan_file_i
    };

/**\cond skip */

    /* 
     * smlevel_0::operating_mode is always set to 
     * ONE of these, but the function in_recovery() tests for
     * any of them, so we'll give them bit-mask values
     */
    enum operating_mode_t {
        t_not_started = 0, 
        t_in_analysis = 0x1,
        t_in_redo = 0x2,
        t_in_undo = 0x4,
        t_forward_processing = 0x8
    };

    static concurrency_t cc_alg;        // concurrency control algorithm
    static bool          cc_adaptive;        // is PS-AA (adaptive) algorithm used?

#include "e_error_enum_gen.h"

    static const w_error_info_t error_info[];
    static void init_errorcodes();

    static void  add_to_global_stats(const sm_stats_info_t &from);
    static void  add_from_global_stats(sm_stats_info_t &to);

    static device_m* dev;
    static io_m* io;
    static bf_m* bf;
    static lock_m* lm;

    static log_m* log;
    static tid_t* redo_tid;

    static LOG_WARN_CALLBACK_FUNC log_warn_callback;
    static LOG_ARCHIVED_CALLBACK_FUNC log_archived_callback;
    static fileoff_t              log_warn_trigger; 
    static int                    log_warn_exceed_percent; 
    
    static int    dcommit_timeout; // to convey option to coordinator,
                                   // if it is created by VAS

    static ErrLog* errlog;

    static bool        shutdown_clean;
    static bool        shutting_down;
    static bool        logging_enabled;
    static bool        do_prefetch;

    static operating_mode_t operating_mode;
    static bool in_recovery() { 
        return ((operating_mode & 
                (t_in_redo | t_in_undo | t_in_analysis)) !=0); }
    static bool in_recovery_analysis() { 
        return ((operating_mode & t_in_analysis) !=0); }
    static bool in_recovery_undo() { 
        return ((operating_mode & t_in_undo ) !=0); }
    static bool in_recovery_redo() { 
        return ((operating_mode & t_in_redo ) !=0); }

    // these variable are the default values for lock escalation counts
    static w_base_t::int4_t defaultLockEscalateToPageThreshold;
    static w_base_t::int4_t defaultLockEscalateToStoreThreshold;
    static w_base_t::int4_t defaultLockEscalateToVolumeThreshold;

    // These variables control the size of the log.
    static fileoff_t max_logsz; // max log file size

    // This variable controls checkpoint frequency.
    // Checkpoints are taken every chkpt_displacement bytes
    // written to the log.
    static fileoff_t chkpt_displacement;

    // The volume_format_version is used to test compatability
    // of software with a volume.  Whenever a change is made
    // to the SM software that makes it incompatible with
    // previouly formatted volumes, this volume number should
    // be incremented.  The value is set in sm.cpp.
    static w_base_t::uint4_t volume_format_version;

    // This is a zeroed page for use wherever initialized memory
    // is needed.
    static char zero_page[page_sz];

    // option for controlling background buffer flush thread
    static option_t* _backgroundflush;


    /*
     * Pre-defined store IDs -- see also vol.h
     * 0 -- is reserved for the extent map and the store map
     * 1 -- directory (see dir.cpp)
     * 2 -- root index (see sm.cpp)
     */
    enum {
        store_id_extentmap = 0,
        store_id_directory = 1,
        store_id_root_index = 2 
        // The store numbers for the lid indexes (if
        // the volume has logical ids) are kept in the
        // volume's root index, constants for them aren't needed.
        //
    };

    enum {
            eINTERNAL = fcINTERNAL,
            eOS = fcOS,
            eOUTOFMEMORY = fcOUTOFMEMORY,
            eNOTFOUND = fcNOTFOUND,
            eNOTIMPLEMENTED = fcNOTIMPLEMENTED
    };

    enum store_flag_t {
        // NB: this had better match sm_store_property_t (sm_int_3.h) !!!
        // or at least be convted properly every time we come through the API
        st_bad            = 0x0,
        st_regular        = 0x01, // fully logged
        st_tmp            = 0x02, // space logging only, 
                                  // file destroy on dismount/restart
        st_load_file      = 0x04, // not stored in the stnode_t, 
                            // only passed down to
                            // io_m and then converted to tmp and added to the
                            // list of load files for the xct.
                            // no longer needed
        st_insert_file     = 0x08,        // stored in stnode, but not on page.
                            // new pages are saved as tmp, old pages as regular.
        st_empty           = 0x100 // store might be empty - used ONLY
                            // as a function argument, NOT stored
                            // persistently.  Nevertheless, it's
                            // defined here to be sure that if other
                            // store flags are added, this doesn't
                            // conflict with them.
    };

    /* 
     * for use by set_store_deleting_log; 
     * type of operation to perform on the stnode 
     */
    enum store_operation_t {
            t_delete_store, 
            t_create_store, 
            t_set_deleting, 
            t_set_store_flags, 
            t_set_first_ext};

    enum store_deleting_t  {
            t_not_deleting_store, 
            t_deleting_store, 
            t_store_freeing_exts, 
            t_unknown_deleting};
/**\endcond  skip */

#if SM_PLP_TRACING
    static uint     _ptrace_level;
    static mcs_lock _ptrace_lock;
    static ofstream _ptrace_out;
    enum plp_tracing_level_t { 
        PLP_TRACE_NONE  = 0x0, 
        PLP_TRACE_PAGE  = 0x01,
        PLP_TRACE_CS    = 0x02
    };
#endif

};

/**\cond  skip */
ostream&
operator<<(ostream& o, smlevel_0::store_flag_t flag);

ostream&
operator<<(ostream& o, const smlevel_0::store_operation_t op);

ostream&
operator<<(ostream& o, const smlevel_0::store_deleting_t value);

/**\endcond  skip */

/*<std-footer incl-file-exclusion='SM_BASE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
