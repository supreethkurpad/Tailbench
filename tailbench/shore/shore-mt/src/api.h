// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='STHREAD_H'>

 $Id: api.h,v 1.3 2010/06/15 17:30:20 nhall Exp $

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

/* 
 * This file contains doxygen documentation only 
 * Its purpose is to determine the layout of the SSMAPI
 * page, which is the starting point for the on-line 
 * storage manager documentation.
 */

/**\defgroup SSMAPI SHORE Storage Manager Application Programming Interface (SSM API)
 *
 * Most of the SHORE Storage Manager functionality is presented in 
 * two C++ classes, ss_m and smthread_t.
 * The ss_m is the storage manager, an instance of which must be 
 * constructed before any storage manager methods may be used.
 * The construction of the single instance performs recovery.
 *
 * All storage manager methods must be called in the context of a
 * storage manager thread, smthread_t.  This means they must be called
 * (directly or indirectly) by the run() method of a class derived from
 * smthread_t.
 * See: smthread_t, \ref SSMINIT.
 *
 * The storage manager is paramaterized with options and their associated
 * values, some of which have defaults and others of which must be given
 * values by the server. An options-processing package is provided for this
 * purpose.
 *
 * See
 *   - \ref SSMOPT for an inventory of the storage manager's options, 
 *   - \ref OPTIONS for a discussion of code to initialize the options, 
 *   - \ref SSMINIT for a discussion of how to initialize and start up
 *   a storage manager,
 *   - \ref startstop.cpp for an example of the minimal required use of
 *   options in a server, and
 *   - the example consisting of \ref create_rec.cpp and \ref init_config_options.cpp for a more complete example
 *
 */

/**\defgroup IDIOMS Programming Idioms
 * \ingroup SSMAPI
 */

/**\defgroup MACROS Significant C Preprocessor Macros 
 * \ingroup SSMAPI
 */

/**\defgroup IDS Identifiers
 * \ingroup SSMAPI
 *
 * Identifiers for persistent storage entities are used throughout
 * the storage manager API. This page collects them for convenience of
 * reference.  
 */

/**\defgroup SSMINIT Starting Up, Shutting Down, Thread Context
 * \ingroup SSMAPI
 *
 * \section SSMSTART Starting a Storage Manager
 * Starting the Storage Manager consists in 2 major things:
 * - Initializing the options the storage manager expects to be set.
 *   See
 *   - \ref OPTIONS for a discussion of code to initialize the options
 *   - \ref SSMOPT for an inventory of the storage manager's options.
 * - Constructing an instance of the class ss_m.
 *   The constructor ss_m::ss_m performs recovery, and when 
 *   it returns to the caller, the caller may begin 
 *   using the storage manager.  
 *
 * No more than one instance may exist at any time.
 *
 * Storage manager functions must be called in the context of
 * a run() method of an smthread_t. 
 *
 * See \ref SSMVAS for an example of how this is done.
 *
 * See also \ref SSMLOGSPACEHANDLING and \ref LOGSPACE for discussions 
 * relating to the constructor and its arguments.
 *
 * \section SSMSHUTDOWN Shutting Down a Storage Manager
 * Shutting down the storage manager consists of deleting the instance
 * of ss_m created above.
 *
 * The storage manager normally shuts down gracefully; if you want
 * to force an unclean shutdown (for testing purposes), you can do so.
 * See ss_m::set_shutdown_flag.
 *
 * \section SSMLOGSPACEHANDLING Handling Log Space
 * The storage manager contains a primitive mechanism for responding
 * to potential inability to rollback or recover due to lack of log
 * space.
 * When it detects a potential problem, it can issue a callback to the
 * server, which can then deal with the situation as it sees fit.
 * The steps that are necessary are:
 * - The server constructs the storage manager ( ss_m::ss_m() ) with two callback function
 *   pointers,
 *   the first of type \ref ss_m::LOG_WARN_CALLBACK_FUNC, and 
 *   the second of type \ref ss_m::LOG_ARCHIVED_CALLBACK_FUNC.
 * - The server is run with a value given to the sm_log_warn option,
 *   which determines the threshold at which the storage manager will
 *   invoke *LOG_WARN_CALLBACK_FUNC.  This is a percentage of the
 *   total log space in use by active transactions.
 *   This condition is checked when any thread calls a storage  manager
 *   method that acts on behalf of a transaction.
 * - When the server calls the given LOG_WARN_CALLBACK_FUNC, that function
 *   is given these arguments:
 *    - iter    Pointer to an iterator over all xcts.
 *    - victim    Victim will be returned here.
 *    - curr    Bytes of log consumed by active transactions.
 *    - thresh   Threshhold just exceeded. 
 *    - logfile   Character string name of oldest file to archive.
 *
 *    The initial value of the victim parameter is the transaction that
 *    is attached to the running thread.  The callback function might choose
 *    a different victim and this in/out parameter is used to convey its choice.
 *
 *    The callback function can use the iterator to iterate over all
 *    the transactions in the system. The iterator owns the transaction-list
 *    mutex, and if this function is not using that mutex, or if it
 *    invokes other static methods on xct_t, it must release the mutex by
 *    calling iter->never_mind().
 *
 *    The curr parameter indicates whte bytes of log consumed by the
 *    active transactions and the thresh parameter indicates the threshold
 *    that was just exceeded.
 *
 *    The logfile parameter is the name (including path) of the log file
 *    that contains the oldest log record (minimum lsn) needed to
 *    roll back any of the active transactions, so it is the first
 *    log file candidate for archiving.
 *
 *    If the server's policy is to abort a victim, it needs only set
 *    the victim parameter and return eUSERABORT.  The storage manager
 *    will then abort that transaction, and the storage manager
 *    method that was called by the victim will return to the running
 *    thread with eUSERABORT.
 *
 *    If the server's policy is not to abort a victim, it can use
 *    xct_t::log_warn_disable() to prevent the callback function
 *    from being called with this same transaction as soon as
 *    it re-enters the storage manager.
 *
 *    If the policy is to archive the indicated log file, and an abort
 *    of some long-running transaction ensues, that log file might be
 *    needed again, in which case, a failure to open that log file will
 *    result in a call to the second callback function, indicated by the
 *    LOG_ARCHIVED_CALLBACK_FUNC pointer.  If this function returns \ref RCOK,
 *    the log manager will re-try opening the file before it chokes.
 *
 *    This is only a stub of an experimental handling of the problem.
 *    It does not yet provide any means of resetting the counters that
 *    cause the tripping of the LOG_WARN_CALLBACK_FUNC.
 *    Nor does it handle the problem well in the face of true physical
 *    media limits.  For example, if, in recovery undo, it needs to
 *    restore archived log files, there is no automatic means of 
 *    setting aside the tail-of-log files to make room for the older
 *    log files; and similarly, when undo is finished, it assumes that
 *    the already-opened log files are still around.
 *    If a callback function renames or unlinks a log file, because the
 *    log might have the files opened, the rename/unlink will not
 *    effect a removal of these files until the log is finished with them.
 *    Thus, these hooks are just a start in dealing with the problem.
 *    The system must be stopped and more disks added to enable the
 *    log size to increase, or a fully-designed log-archiving feature
 *    needs to be added.
 *    Nor is this well-tested.
 *
 *    The example \ref log_exceed.cpp is a primitive
 *    example using these callbacks. That example shows how you must
 *    compile the module that uses the API for xct_t.
 *
 */


/**\defgroup OPTIONS Run-Time Options 
 * \ingroup SSMAPI
 */

/**\defgroup SSMOPT List of Run-Time Options
 * \ingroup OPTIONS
 */

 /**\defgroup SSMSTG Storage Structures
  *
  * The modules below describe the storage manager's storage structures. 
  * In summary, 
  * - devices contain
  *   - volumes, which contain
  *     - stores, upon which are built 
  *       - files of records,
  *       - conventional indexes (B+-trees), and
  *       - spatial indexes (R*-trees)
  *
  *
 * \ingroup SSMAPI
 */

 /**\defgroup SSMVOL Devices and Volumes
 * \ingroup SSMSTG
 */

/**\defgroup SSMSTORE Stores
 * \ingroup SSMSTG
 */

/**\defgroup SSMFILE Files of Records
 * \ingroup SSMSTG
 */

/**\defgroup SSMPIN Pinning Records
 * \ingroup SSMFILE
 */

/**\defgroup SSMBTREE B+-Tree Indexes
 * \ingroup SSMSTG
 */

/**\defgroup SSMRTREE R*-Tree Indexes
 * \ingroup SSMSTG
 */

/**\defgroup SSMSCAN Scanning
 * \ingroup SSMSTG
 */

/**\defgroup SSMSCANF Scanning Files
 * \ingroup SSMSCAN
 * To iterate over the records in a file, 
 * construct an instance of the class scan_file_i, q.v..
 * That page contains examples. 
 */

/**\defgroup SSMSCANI Scanning B+-Tree Indexes
 * \ingroup SSMSCAN
 * To iterate over the {key,value} pairs in an index, 
 * construct an instance of the class scan_index_i, q.v.
 * That page contains examples. 
 */

/**\defgroup SSMSCANRT Scanning R*-Tree Indexes
 * \ingroup SSMSCAN
 * To iterate over the {key,value} pairs in a spatial index, 
 * construct an instance of the class scan_rt_i, q.v.
 * That page contains examples. 
 *
 */

 /**\defgroup SSMBULKLD Bulk-Loading Indexes 
 * \ingroup SSMSTG
 *
 * Bulk-loading indexes consists of the following steps:
 * - create the source of the datas for the bulk-load, which can be
 *   - one or more file(s) of records, or
 *   - a sort_stream_i
 * - call a bulk-loading method in ss_m
 *
 * To avoid excessive logging of files that do not need to persist after
 * the bulk-load is done, use the sm_store_property_t property
 * t_load_file for the source files.
 */

 /**\defgroup SSMSORT Sorting 
 * \ingroup SSMSTG
 */
 /**\example sort_stream.cpp */

/**\defgroup SSMXCT  Transactions, Locking and Logging
 * \ingroup SSMAPI
 */

/**\defgroup SSMLOCK Locking 
 * \ingroup SSMXCT
 */

/**\defgroup SSMSP  Partial Rollback: Savepoints
 * \ingroup SSMXCT
 */

/**\defgroup SSMQK  Early Lock Release: Quarks
 * \ingroup SSMXCT
 */

/**\defgroup SSM2PC  Distributed Transactions: Two-Phase Commit
 * \ingroup SSMXCT
 */
/**\defgroup SSMMULTIXCT Multi-threaded Transactions
 * \ingroup SSMXCT
 */

/**\defgroup LOGSPACE Running Out of Log Space  
 *   \ingroup SSMXCT
 */

/**\defgroup LSNS How Log Sequence Numbers are Used
 * \ingroup SSMXCT
 */

/**\defgroup SSMSTATS Storage Manager Statistics
 * \ingroup SSMAPI
 *
 * The storage manager contains functions to gather statistics that
 * it collects. These are mostly counters and are described here.
 *
 * Volumes can be analyzed to gather usage statistics.  
 * See ss_m::get_du_statistics and ss_m::get_volume_meta_stats.
 *
 * Bulk-loading indexes gathers statistics about the bulk-load activity.
 * See ss_m::bulkld_index and ss_m::bulkld_md_index.
 *
 * \note A Perl script facilitates modifying the statistics gathered by
 * generating much of the supporting code, including
 * structure definitions and output operators.  
 * The server-writer can generate her own sets of statistics using
 * the same Perl tool.
 * See \ref STATS for
 * more information about how these statistics sets are built.
 *
 */

/**\defgroup SSMVTABLE Virtual Tables
 * \ingroup SSMAPI
 * \details
 *
 * Virtual tables are string representations of internal
 * storage manager tables.
 * These tables are experimental. If the tables get to be very
 * large, they might fail.
 * - lock table (see ss_m::lock_collect)
 *   Columns are:
 *   - mode
 *   - duration
 *   - number of children 
 *   - id  of owning transaction
 *   - status (granted, waiting)
 * - transaction table (see ss_m::xct_collect)
 *   Columns are:
 *   - number of threads attached
 *   - global transaction id
 *   - transaction id
 *   - transaction state (in integer form)
 *   - coordinator  
 *   - forced-readonly (Boolean)
 * - threads table (see ss_m::thread_collect)
 *   Columns are:
 *   - sthread ID
 *   - sthread status
 *   - number of I/Os issued
 *   - number of reads issued
 *   - number of writes issued
 *   - number of syncs issued
 *   - number of truncates issued
 *   - number of writev issued
 *   - number of readv issued
 *   - smthread name
 *   - smthread thread type (integer)
 *   - smthread pin count
 *   - is in storage manager
 *   - transaction ID of any attached transaction
*/
 /**\example vtable_example.cpp */

/**\defgroup MISC Miscellaneous
 * \ingroup SSMAPI
 */
/**\defgroup SSMSYNC Synchronization, Mutual Exclusion, Deadlocks
 * \ingroup MISC
 *
 * Within the storage manager are a variety of primitives that provide for
 * ACID properties of transactions and for correct behavior of concurrent
 * threads. These include:
 * - read-write locking primitives for concurrent threads  (occ_rwlock,
 *   mcs_rwlock)
 * - mutexes (pthread_mutex_t, queue_based_lock_t)
 * - condition variables (pthread_cond_t)
 * - latches (latch_t)
 * - database locks 
 *
 * The storage manager uses database locks to provide concurrency control
 * among transactions; 
 * latches are used for syncronize concurrent threads' accesses to pages in the 
 * buffer pool.  The storage manager's threads use carefully-designed
 * orderings of the entities they "lock" with synchronization primitives
 * to avoid any sort of deadlock.  All synchronization primitives
 * except data base locks are meant to be held for short durations; they
 * are not even held for the duration of a disk write, for example. 
 *
 * Deadlock detection is done only for database locks.
 * Latches are covered by locks, which is
 * to say that locks are acquired before latches are requested, so that
 * deadlock detection in the lock manager is generally sufficient to prevent
 * deadlocks among concurrent threads in a properly-written server.
 *
 * Care must be taken, when writing server code, to avoid deadlocks of
 * other sorts such as latch-mutex, or latch-latch deadlocks.
 * For example, multiple threads may cooperate on behalf of the same
 * transaction; if they are trying to pin records without a well-designed
 * ordering protocol, they may deadlock with one thread holding page
 * A pinned (latched) and waiting to pin (latch) B, while the other holds
 * B pinned and waits for a pin of A.
 */

/**\defgroup SSMAPIDEBUG Debugging the Storage Manager
 * \ingroup SSMDEBUG
 *
 * The storage manager contains a few methods that are useful for
 * debugging purposes. Some of these should be used for not other
 * purpose, as they are not thread-safe, or might be very expensive.
 */
/**\defgroup TLS Thread-Local Variables
 * \ingroup MISC
 */
/**\defgroup UNUSED Unused code 
 * \ingroup MISC
 */


/**\defgroup OPT Configuring and Building the Storage Manager
 * \to any other build options from shore.def or elsewhere? (api.h)
 *
 */

/**\defgroup IMPLGRP Implementation Notes
 * See \ref IMPLNOTES "this page" for some implementation details.
 */

/**\defgroup REFSGRP References
 * See \ref REFERENCES "this page" for references to selected papers 
 * from which ideas are used in the Shore Storage Manager. 
 */
