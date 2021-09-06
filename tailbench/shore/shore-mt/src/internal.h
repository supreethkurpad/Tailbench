// -*- mode:c++; c-basic-offset:4 -*-
/*<std-header orig-src='shore' incl-file-exclusion='STHREAD_H'>

 $Id: internal.h,v 1.3 2010/06/15 17:30:20 nhall Exp $

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

/* This file contains doxygen documentation only */

/**\page IMPLNOTES Implementation Notes
 *
 * \section MODULES Storage Manager Modules
 * The storage manager code contains the following modules (with related C++ classes):
 *
 * - \ref SSMAPI (ss_m) 
 *   Most of the programming interface to the storage manager is encapsulated
 *   in the ss_m class. 
 * - \ref IO_M (io_m),  \ref VOL_M (vol_m) and \ref DIR_M (dir_m)
 *   These managers handle volumes, page allocation and stores, which are the
 *   structures underlying files of records, B+-Tree indexes, and
 *   spatial indexes (R*-Trees).
 * - \ref FILE_M (file_m), \ref BTREE_M (btree_m), and \ref RTREE_M (rtree_m)
 *   handle the storage structures available to servers.
 * - \ref LOCK_M (lock_m) 
 *   The lock manager is quasi-stand-alone.
 * - \ref XCT_M (xct_t) and * \ref LOG_M (log_m) handle transactions,
 *   logging, and recovery.
 * - \ref BF_M (bf_m)
 *   The buffer manager works closely with \ref XCT_M and \ref LOG_M.
 *
 * \section IO_M I/O Manager
 * The I/O manager was, in the early days of SHORE, expected to
 * have more responsibility than it now has; now it is little more
 * than a wrapper for the \ref VOL_M.  
 *
 * \section VOL_M Volume Manager
 * The volume manager handles formatting of volumes,
 * allocation and deallocation of pages and extents in stores.
 * Within a page, allocation of space is up to the manager of the
 * storage structure (btree, rtree, or file).
 *
 * Pages are reserved for a store in units of ss_m::ext_sz, 
 * a compile-time constant that indicates the size of an extent.
 * An extent is a set of contiguous pages.  Extents are represented 
 * by persistent data structures, extnode_t, which are 
 * linked together to form the entire structure of a store.  
 * A stnode_t holds metadata for a store and  sits at the 
 * head of the chain of extents that forms the store, and 
 * the extents in the store list are marked as having an owner, 
 * which is the store id of the store to which they belong.  
 * A store id is a number of type snum_t, and an extent id is 
 * a number of type extnum_t.  
 * Scanning the pages in a store can be accomplished by scanning the 
 * list of extnode_t.   
 * Each extent has a number, and the pages in the extent are arithmetically 
 * derived from the extent number; likewise, from any page, 
 * its extent can be computed.  Free extents are not linked together; 
 * they simply have no owner (signified by an  extnode_t::owner == 0).
 *
 * \subsection PAGES Page Types
 * Pages in a volume come in a variety of page types.
 * Each page type is a C++ class that derives from the base class
 * page_p.  Page_p implements functionality that is common to all
 * (or most) page types. The types are as follows:
 *
 * - extlink_p : extent-link pages, used by vol_m
 * - stnode_p  : store-node pages, used by vol_m
 * - file_p    : slotted pages of file-of-record, used by file_m
 * - lgdata_p  : pages of large records, used by file_m
 * - lgindex_p : pages of large records, used by file_m
 * - keyed_p   : slotted pages of indexes, used by btree_m
 * - zkeyed_p  : slotted pages of indexes, used by btree_m
 * - rtree_p   : slotted pages of spatial indexes, used by rtree_m
 *
 * Issues specific to the page types will be dealt with in the descriptions of the modules.
 *
 * \subsection RSVD_MODE Space Reservation on a Page
 *
 * All pages are \e slotted (those that don't need the slot structure
 * may use only one slot) and have the following layout:
 * - header, including
 *   - lsn_t log sequence number of last page update
 *   - page id
 *   - links to next and previous pages (used by some storage structures)
 *   - page tag (indicates type of page)
 *   - space management metadata (space_t)
 *   - store flags (logging level metadata)
 * - slots (grow down)
 * - slot table array of pointers to the slots (grows up)
 * - footer (copy of log sequence number of last page update)
 * 
 * Different storage structures offer different opportunities for fine-grained 
 * locking and need different means of allocation space within a page.
 * Special care is taken to reserve space on a page when slots 
 * are freed (records are deleted) so that rollback can restore 
 * the space on the page.  
 * Page types that use this space reservation have 
 * \code page_p::rsvd_mode() == true \endcode.
 *
 * In the case of B-trees, this is not used because
 * undo and redo are handled logically -- entries 
 * can be re-inserted in a different page.  But in the case of files, 
 * records are identified by physical ID, which includes page and slot number,
 * so records must be reinserted just where they first appeared. 
 *
 * Holes in a page are coalesced (moved to the end of the page) as needed, 
 * when the total free space on the page satisfies a need but the 
 * contiguous free space does not.  Hence, a record truncation followed 
 * by an append to the same record does not necessarily cause the 
 * shifting of other records on the same page.
 *
 * A count of free bytes is maintained for all pages. More space-allocation
 * metadata is maintained for rsvd_mode() pages.
 *
 * When a transaction release a slot on a page with rsvd_mode(), the slot 
 * remains
 * "reserved" for use by the same transaction. That slot is not free to
 * be allocated by another transaction until the releasing transaction 
 * commits.  This is because if the transaction aborts, the slot must
 * be restored with the same slot number.  Not only must the slot number be
 * preserved, but the number of bytes consumed by that slot must remain 
 * available lest the transaction abort.
 * The storage manager keeps track of the youngest active transaction
 * that is freeing space (i.e., "reserving" it) on the page 
 * and the number of bytes freed ("reserved")
 * by the youngest transaction.  
 * When the youngest transaction to reserve space on the page becomes
 * older than the oldest active transaction in the system, the reserved
 * space becomes free. This check for freeing up the reserved space happens
 * whenever a transaction tries to allocate space on the page.
 *
 * During rollback, a transaction can use \e any amount of
 * reserved space, but during forward processing, it can only use space
 * it reserved, and that is known only if the transaction in question is
 * the youngest transaction described in the above paragraph.
 *
 * The changes to space-reservation metadata (space_t) are not logged.
 * The actions that result in updates to this metadata are logged (as
 * page mark and page reclaim).
 *
 *
 * \subsection ALLOCEXT Allocation of Extents and Pages
 * Extents are linked through extlink_t to form a linked list, which
 * composes the allocated extents of a store. The volume manager 
 * handles allocating extents to stores.  Deallocation
 * of extents is shared by the volume manager and the the lock manager
 * (see the discussion in lockid_t::set_ext_has_page_alloc).
 *
 * The volume manager keeps a cache of {store, extent} pairs, to find
 * extents already allocated to a store that contain free pages. This
 * cache is consulted before new extents are allocated to a store.
 * Since after restart the cache is necessarily empty, it is primed when
 * first needed for the purpose of allocating anything for the store. 
 *
 * Priming the store is an expensive operation.
 * It is not done on each volume mount, because volumes are mounted and
 * dismounted several times during recovery, and priming on each
 * mount would be prohibitive.
 *
 * \section FILE_M File Manager
 * A file is a group of variable-sized records (or objects).  
 * A record is the smallest persistent datum that has identity 
 * A record may also have a "user header", whose contents are
 * for use by the server.  
 * As records vary in size, so their storage representation varies. 
 * The storage manager changes the storage representation as needed.
 * A file comprises two stores.  
 * One store is allocated for slotted (small-record) pages, called file_p
 * pages.  
 * One store is allocated for large records, and contains lgdata_p and
 * lgindex_p pages.
 * Small records are those whose size is less than or equal to
 * sm_config_info_t.max_small_rec.  A record larger than this
 * has a slot on a small-record page, which slot contains metadata
 * refering to pages in the large-record store.
 * The scan order of a file is the physical order of the records
 * in the small-record store.
 *
 * Every record, large or small, has the following metadata in the
 * record's slot on the file_p page; these data are held in a rectag_t
 * structure:
 * \code
struct rectag_t {
    uint2_t   hdr_len;  // length of user header, may be zero
    uint2_t   flags;    // enum recflags_t: indicates internal implementation
    smsize_t  body_len; // true length of the record 
};
\endcode
  * The flags have have these values:
    - t_small   : a small record, entirely contained on the file_p
    - t_large_0 : a large record, the slot on the file_p contains the
	              user header, while the body is a list
	              of chunks (pointers to contiguous lgdata_p pages)
    - t_large_1 : a large record, the slot on the file_p contains the
	              user header, while the body is a reference to a single
	              lgindex_p page, which is the root of a 1-level index of
				  lgdata_p pages.
    - t_large_2 : like t_large_1 but the index may be two levels deep. This
	              has not been implemented.
 *
 * Internally (inside the storage manager), the class record_t is a
 * handle on the record's tag and is the class through which the
 * rectag_t is manipulated.
 *
 * A record is exposed to the server through a set of ss_m methods (create_rec,
 * append_rec, etc), and through the pin_i class.
 *
 * All updates to records are accomplished by copying out part or all of
 * the record from the buffer pool to the server's address space, performing
 * updates there, and handing the new data to the storage manager. 
 * User (server) data are not updated directly in the buffer pool.
 *
 * The server may cause the file_p and at most one large data page to
 * be pinned for a given record through the pin_i class; the server must
 * take care not to create latch-latch deadlocks by holding a record pinned
 * while attempting to pin another.  An ordering protocol among the pages
 * pinned must be observed to avoid such deadlocks.
 *
 * \note The system only detects lock-lock deadlocks.  Deadlocks involving
 * mutexes or latches or other blocking mechanisms will cause the server to
 * hang.
 *
 * \subsection HISTO Finding a Page with Space
 *
 * When a record is created, the file manager tries to use an already-allocated
 * page that has space for the record (based on the length hint and
 * the data given in the create_rec call). The file manager caches information
 * about page utilization for pages in each store.  The page utilization
 * data take the form of a histoid_t, which contains a heap
 * and a histogram.  The heap keeps track of the amount of
 * free space in (recently-used) pages in the heap, and it is 
 * searchable so that it can
 * return the page with the smallest free space that is larger than a
 * given value.
 * The histogram has a small number of buckets, each of which counts
 * the number of pages in the file containing free space between 
 * the bucket min and the bucket max.
 *
 * Three policies used can be used in combination to search for space.
 * - t_cache : look in the heap for a page with space. 
 * - t_compact : if the histograms say there are any pages with 
 *   sufficient space somewhere in the file, 
 *   do a linear search of the file for such a page.
 * - t_append : append the new record to the file
 *
 * Using append_file_t to create records means only t_append is used, 
 * ensuring that the record will always be appended to the file.
 * ss_m::create_rec uses t_cache | t_compact | t_append.
 *
 * \bug GNATS 33 t_compact is turned off.  It is expensive and has not been
 * tested lately, thus there is presently no policy that will keep
 * files compact.  If files become too sparse, the server must reorganize
 * the database.
 *
 * Once the file manager has located a page with sufficient space to
 * create the record, the I/O and volume managers worry about 
 * \ref RSVD_MODE.
 *
 * \subsection HISTO Allocating a New File Page
 *
 * If the file manager cannot find an already-allocated page with
 * sufficient space, or if it is appending a record to a file and
 * needs to allocate a new page, it first descends to the I/O manager
 * to find an extent with a free page (or the last extent in the file) (
 * \ref ALLOCEXT).
 *
 * Once it has found an extent with a free page, it allocates a page in
 * that extent.  
 *
 * IX-locks are acquired on file pages for the purpose of fine-grained locking.
 * There is no file structure superimposed on the store, with which
 * to link the file pages together, so as soon as an extent is allocated
 * to a store, it is visible to any transaction; in particular, it is
 * visible between the time the page is allocated by the I/O manager and
 * the time the file manager formats that page for use.
 * For this reason, the allocation of a file page must be a top-level action
 * so that
 * undo of the allocation is logical: it must check for the file page
 * still being empty before the page can be freed.
 * If another transaction slipped in an allocated a record on the same page,
 * the page can't be freed on undo of the page allocation.
 *
 * Protocol for allocating a file page:
 * - Enter I/O manager (mutex).
 * - Start top-level action (grab log anchor).
 * - IX-lock (long duration) the page.
 * - Allocate the page.
 * - End top-level action (grab log anchor) to compensate around
 *    the changes to the extent map.
 * - Log the file-page allocation.
 *
 * To free a page, the transaction must acquire an EX lock on the page;
 * this prevents the page from being used by any other transaction until
 * the freeing transaction commits.  If the EX lock cannot be acquired,
 * the page is in use and will not be freed (e.g., the other transaction
 * could abort and re-insert something on the page).
 *
 * \section BTREE_M B+-Tree Manager
 *
 * The values associated with the keys are opaque to the storage
 * manager, except when IM (Index Management locking protocol) is used, 
 * in which case the value is
 * treated as a record ID, but no integrity checks are done.  
 * It is the responsibility of the server to see that the value is
 * legitimate in this case.
 *
 * B-trees can be bulk-loaded from files of sorted key-value pairs,
 * as long as the keys are in \ref LEXICOFORMAT "lexicographic form". 
 *
 * The implementation of B-trees is straight from the Mohan ARIES/IM
 * and ARIES/KVL papers. See \ref MOH1, which covers both topics.
 *
 * Those two papers give a thorough explanation of the arcane algorithms,
 * including logging considerations.  
 * Anyone considering changing the B-tree code is strongly encouraged 
 * to read these papers carefully.  
 * Some of the performance tricks described in these papers are 
 * not implemented here.  
 * For example, the ARIES/IM paper describes performance of logical 
 * undo of insert operations if and only if physical undo 
 * is not possible.  
 * The storage manager always undoes inserts logically.
 *
 * \todo internal.h Doc Ryan's change in btree_impl.cpp search for Ordinarily; add stats and smsh tests for this case.
 *
 * \section RTREE_M R*-Tree Manager
 *
 * The spatial indexes in the storage manager are R*-trees, a variant
 * of R-trees that perform frequent restructuring to yield higher
 * performance than normal R-trees.  The entire index is locked.
 * See \ref BKSS.
 *
 * \section DIR_M Directory Manager
 * All storage structures created by a server
 * have entries in a B+-Tree index called the 
 * \e store \e directory or just \e directory.
 * This index is not exposed to the server.
 *
 * The storage manager maintains  some transient and some persistent data
 * for each store.  The directory's key is the store ID, and the value it
 * returns from a lookup is a
 * sdesc_t ("store descriptor") structure, which
 * contains both the persistent and transient information. 
 *
 * The persistent information is in a sinfo_s structure; the 
 * transient information is resident only in the cache of sdesc_t 
 * structures that the directory manager
 * maintains.
 *
 * The metadata include:
 * - what kind of storage structure uses this store  (btree, rtree, file)
 * - if a B-tree, is it unique and what kind of locking protocol does it use?
 * - what stores compose this storage structure (e.g., if file, what is the
 *   large-object store and what is the small-record store?)
 * - what is the root page of the structure (if an index)
 * - what is the key type if this is an index
 *
 * \section LOCK_M Lock Manager
 *
 * The lock manager understands the folling kind of locks
 * - volume
 * - extent
 * - store
 * - page
 * - kvl
 * - record
 * - user1
 * - user2
 * - user3
 * - user4
 *
 * Lock requests are issued with a lock ID (lockid_t), which
 * encodes the identity of the entity being locked, the kind of
 * lock, and, by inference,  a lock hierarchy for a subset of the
 * kinds of locks above.
 * The lock manager does not insist that lock identifiers 
 * refer to any existing object.  
 *
 * The lock manager enforces two lock hierarchies:
 * - Volume - store - page - record
 * - Volume - store - key-value
 *
 * Note that the following lock kinds are not in any hierarchy:
 * -extent
 * -user1, user2, user3, user4
 *
 * Other than the way the lock identifiers are inspected for the purpose 
 * of enforcing the hierarchy, lock identifiers are considered opaque 
 * data by the lock manager.  
 *
 * The lockid_t structure can be constructed from the IDs of the
 * various entities in (and out of ) the hierarchy; see lockid_t and
 * the example lockid_test.cpp.
 *
 * The lock manager escalates up the hierarchy by default.  
 * The escalation thresholds are based on run-time options.  
 * They can be controlled (set, disabled) on a per-object level.  
 * For example, escalation to the store level can be disabled when 
 * increased concurrency is desired.  
 * Escalation can also be controlled on a per-transaction or per-server basis.
 *
 * Locks are acquired by storage manager operations as appropriate to the
 * use of the data (read/write). (Update locks are not acquired by the
 * storage manager.)
 *
 * The storage manager's API allows explicit acquisition 
 * of locks by a server.  
 * Freeing locks is automatic at transaction commit and rollback.  
 * There is limited support for freeing locks in the middle of 
 * a transaction:
 * - locks of duration less than t_long can be unlocked with unlock(), and
 * - quarks (sm_quark_t) simplify acquiring and freeing locks mid-transaction:
 *
 * A quark is a marker in the list of locks held by a transaction.  
 * When the quark is destroyed, all locks acquired since the 
 * creation of the quark are freed.  Quarks cannot be used while more than
 * one thread is attached to the transaction, although the storage 
 * manager does not strictly enforce this (due to the cost).
 * When a quark is in use for a transaction, the locks acquired
 * will be of short duration, the assumption being that the quark
 * will be closed before commit-time.  
 *
 * Extent locks are an exception; they must be held long-term for
 * page allocation and deallocation to work, so even in the context
 * of an open quark, extent locks will be held until end-of-transaction.
 *
 * The lock manager uses a hash table whose size is determined by
 * a configuration option.  
 * The hash function used by the lock manager is known not 
 * to distribute locks evenly among buckets.
 * This is partly due to the nature of lock IDs.
 *
 * To avoid expensive lock manager queries, each transaction 
 * keeps a cache of the last \<N\> locks acquired (the number
 * \<N\> is a compile-time constant).
 * This close association between the transaction manager and
 * the lock manager is encapsulated in several classes in the file lock_x.
 *
 * \subsection DLD Deadlock Detection
 * The lock manager uses a statistical deadlock-detection scheme
 * known as "Dreadlocks" [KH1].
 * Each storage manager thread (smthread_t) has a unique fingerprint, which is
 * a set of bits; the deadlock detector ORs together the bits of the
 * elements in a waits-for-dependency-list; each thread, when 
 * blocking, holds a  digest (the ORed bitmap).  
 * It is therefore cheap for a thread to detect a cycle when it needs to 
 * block awaiting a lock: look at the holders
 * of the lock and if it finds itself in any of their digests, a
 * cycle will result.
 * This works well when the total number of threads relative to the bitmap
 * size is such that it is possible to assign a unique bitmap to each
 * thread.   
 * If you cannot do so, you will have false-positive deadlocks
 * "detected".
 * The storage manager counts, in its statistics, the number of times
 * it could not assign a unique fingerprint to a thread.  
 * If you notice excessive transaction-aborts due to false-positive
 * deadlocks,
 * you can compile the storage manager to use a larger
 * number bits in the 
 * \code sm_thread_map_t \endcode
 * found in 
 * \code smthread.h \endcode.
 *
 * \section XCT_M Transaction Manager
 * When a transaction commits, the stores that are marked for deletion
 * are deleted, and the stores that were given sm_store_property_t t_load_file or t_insert_file are turned into t_regular stores.
 * Because these are logged actions, and they occur if and only if the 
 * transaction commits, the storage manager guarantees that the ending
 * of the transaction and re-marking and deletion of stores is atomic.
 * This is accomplished by putting the transaction into a state
 * xct_freeing_space, and writing a log record to that effect.
 * The space is freed, the stores are converted, and a final log record is written before the transaction is truly ended.
 * In the vent of a carash while a transaction is freeing space, 
 * recovery searches all the 
 * store metadata for stores marked for deleteion
 * and deletes those that would otherwise have been missed in redo.
 *
 * \section LOG_M Log Manager
 *
 * \subsection LOG_M_USAGE How the Server Uses the Log Manager
 *
 * Log records for redoable-undoable operations contain both the 
 * redo- and undo- data, hence an operation never causes two 
 * different log records to be written for redo and for undo.  
 * This, too, controls logging overhead.  
 *
 * The protocol for applying an operation to an object is as follows:
 * - Lock the object.
 * - Fix the page(s) affected in exclusive mode.
 * - Apply the operation.
 * - Write the log record(s) for the operation.
 * - Unfix the page(s).
 *
 * The protocol for writing log records is as follows:
 * - Grab the transaction's log buffer in which the last log record is to be 
 *   cached by calling xct_t::get_logbuf()
 * - Write the log record into the buffer (the idiom is to construct it
 *      there using C++ placement-new).
 * - Release the buffer with xct_t::give_logbuf(),
 *    passing in as an argument the fixed page that was affected
 *    by the update being logged.  This does several things: 
 *    - writes the transaction ID, previous LSN for this transaction 
 *      into the log record
 *    - inserts the record into the log and remembers this record's LSN
 *    - marks the given page dirty.
 *
 * Between the time the xct log buffer is grabbed and the time it is
 * released, the buffer is held exclusively by the one thread that
 * grabbed it, and updates to the xct log buffer can be made freely.
 * (Note that this per-transaction log buffer is unrelated to the log buffer
 * internal to the log manager.)
 *
 * The above protocol is enforced by the storage manager in helper
 * functions that create log records; these functions are generated
 * by Perl scripts from the source file logdef.dat.  (See \ref LOGDEF.)
 *
 *\subsection LOGRECS Log Record Types
 * \anchor LOGDEF
 * The input to the above-mentioned Perl script is the source of all
 * log record types.  Each log record type is listed in the  file
 * \code logdef.dat \endcode
 * which is fairly self-explanatory, reproduced here:
 * \include logdef.dat
 *
 * The bodies of the methods of the class \<log-rec-name\>_log
 * are hand-written and reside in \code logrec.cpp \endcode.
 *
 * Adding a new log record type consists in adding a line to
 * \code logdef.dat, \endcode
 * adding method definitions to 
 * \code logrec.cpp, \endcode
 * and adding the calls to the free function log_<log-rec-name\>(args)
 * in the storage manager.
 * The base class for every log record is logrec_t, which is worth study
 * but is not documented here.
 *
 * Some logging records are \e compensated, meaning that the 
 * log records are skipped during rollback. 
 * Compensations may be needed because some operation simply cannot
 * be undone.  The protocol for compensating actions is as follows:
 * - Fix the needed pages.
 * - Grab an \e anchor in the log.  
 *   This is an LSN for the last log record written for this transaction.
 * - Update the pages and log the updates as usual.
 * - Write a compensation log record (or piggy-back the compensation on
 *   the last-written log record for this transaction to reduce 
 *   logging overhead) and free the anchor.
 *
 * \note Grabbing an anchor prevents all other threads in a multi-threaded
 * transaction from gaining access to the transaction manager.  Be careful
 * with this, as it can cause mutex-latch deadlocks where multi-threaded
 * transactions are concerned.  In other words, two threads cannot concurrently
 * update in the same transaction.
 *
 * In some cases, the following protocol is used to avoid excessive
 * logging by general update functions that, if logging were turned
 * on, would generate log records of their own.
 * - Fix the pages needed in exclusive mode.
 * - Turn off logging for the transaction.
 * - Perform the updates by calling some general functions.  If an error occurs, undo the updates explicitly.
 * - Turn on logging for the transaction.
 * - Log the operation.  If an error occurs, undo the updates with logging turned off..
 * - Unfix the pages.
 *
 * The mechanism for turning off logging for a transaction is to
 * construct an instance of xct_log_switch_t.
 *
 * When the instance is destroyed, the original logging state
 * is restored.  The switch applies only to the transaction that is 
 * attached to the thread at the time the switch instance is constructed, 
 * and it prevents other threads of the transaction from using 
 * the log (or doing much else in the transaction manager) 
 * while the switch exists.
 *
 * \subsection LOG_M_INTERNAL Log Manager Internals
 *
 * The log is a collection of files, all in the same directory, whose
 * path is determined by a run-time option.
 * Each file in the directory is called a "log file" and represents a
 * "partition" of the log. The log is partitioned into files to make it 
 * possible to archive portions of the log to free up disk space.
 * A log file has the name \e log.\<n\> where \<n\> is a positive integer.
 * The log file name indicates the set of logical sequence numbers (lsn_t)
 * of log records (logrec_t) that are contained in the file.  An
 * lsn_t has a \e high part and a \e low part, and the
 * \e high part (a.k.a., \e file part) is the \<n\> in the log file name.
 *
 * The user-settable run-time option sm_logsize indicates the maximum 
 * number of KB that may be opened at once; this, in turn, determines the
 * size of a partition file, since the number of partition files is
 * a compile-time constant.
 * The storage manager computes partition sizes based on the user-provided
 * log size, such that partitions sizes are a convenient multiple of blocks
 * (more about which, below).
 *
 * A new partition is opened when the tail of the log approaches the end
 * of a partition, that is, when the next insertion into the log
 * is at an offset larger than the maximum partition size.  (There is a
 * fudge factor of BLOCK_SIZE in here for convenience in implementation.)
 * 
 * The \e low part of an lsn_t represents the byte-offset into the log file
 * at which the log record with that lsn_t sits.
 *
 * Thus, the total file size of a log file \e log.\<n\>
 * is the size of all log records in the file, 
 * and the lsn_t of each log record in the file is
 * lsn_t(\<n\>, <byte-offset>) of the log record within the file.
 *
 * The log is, conceptually, a forever-expanding set of files. The log 
 * manager will open at most PARTITION_COUNT log files at any one time.
 * - PARTITION_COUNT = smlevel_0::max_openlog
 * - smlevel_0::max_openlog (sm_base.h) = SM_LOG_PARTITIONS
 * - SM_LOG_PARTITIONS a compile-time constant (which can be overridden in 
 *   config/shore.def).
 *
 * The log is considered to have run out of space if logging requires that
 * more than smlevel_0::max_openlog partitions are needed.
 * Partitions are needed only as long as they contain log records 
 * needed for recovery, which means:
 * - log records for pages not yet made durable (min recovery lsn)
 * - log records for uncommitted transactions (min xct lsn)
 * - log records belonging to the last complete checkpoint
 *
 * Afer a checkpoint is taken and its log records are durable,
 * the storage manager tries to scavenge all partitions that do not
 * contain necessary log records.  The buffer manager provides the
 * min recovery lsn; the transaction manager provides the min xct lsn,
 * and the log manager keeps track of the location of the last 
 * completed checkpoint in its master_lsn.  Thus the minimum of the
 * 
 * \e file part of the minmum of these lsns indicates the lowest partition 
 * that cannot be scavenged; all the rest are removed.
 *
 * When the log is in danger of runing out of space 
 * (because there are long-running  transactions, for example) 
 * the server may be called via the
 * LOG_WARN_CALLBACK_FUNC argument to ss_m::ss_m.  This callback may
 * abort a transaction to free up log space, but the act of aborting
 * consumes log space. It may also archive a log file and remove it.
 * If the server provided a
 * LOG_ARCHIVED_CALLBACK_FUNC argument to ss_m::ss_m, this callback
 * can be used to retrieve archived log files when needed for
 * rollback.
 * \warning This functionality is not complete and has not been
 * well-tested.
 *
 * Log files (partitions) are written in fixed-sized blocks.  The log
 * manager pads writes, if necessary, to make them BLOCK_SIZE. 
 * - BLOCK_SIZE = 8192, a compile-time constant.
 *
 * A skip_log record indicates the logical end of a partition.
 * The log manager ensures that the last log record in a file 
 * is always a skip_log record. 
 *
 * Log files (partitions) are composed of segments. A segment is
 * an integral number of blocks.
 * - SEGMENT_SIZE = 128*BLOCK_SIZE, a compile-time constant.
 *
 * The smallest partition is one segment plus one block, 
 * but may be many segments plus one block. The last block enables
 * the log manager to write the skip_log record to indicate the
 * end of the file.
 *
 * The partition size is determined by the storage manager run-time option,
 * sm_logsize, which determines how much log can be open at any time,
 * i.e., the combined sizes of the PARTITION_COUNT partitions.
 *
 * The maximum size of a log record (logrec_t) is 3 storage manager pages.
 * A page happens to match the block size but the two compile-time
 * constants are not inter-dependent. 
 * A segment is substantially larger than a block, so it can hold at least
 * several maximum-sized log records, preferably many.
 * 
 * Inserting a log record consists of copying it into the log manager's
 * log buffer (1 segment in size).  The buffer wraps so long as there
 * is room in the partition.  Meanwhile, a log-flush daemon thread
 * writes out unflushed portions of the log buffer. 
 * The log daemon can lag behind insertions, so each insertion checks for
 * space in the log buffer before it performs the insert. If there isn't
 * enough space, it waits until the log flush daemon has made room.
 *
 * When insertion of a log record would wrap around the buffer and the
 * partition has no room for more segments, a new partition is opened,
 * and the entire newly-inserted log record will go into that new partition.
 * Meanwhile, the log-flush daemon will see that the rest of the log
 * buffer is written to the old partition, and the next time the
 * log flush daemon performs a flush, it will be flushing to the
 * new partition.
 *
 * The bookkeeping of the log buffer's free and used space is handled
 * by the notion of \e epochs.
 * An epoch keeps track of the start and end of the unflushed portion
 * of the segment (log buffer).  Thus, an epoch refers to only one
 * segment (logically, log buffer copy within a partition).
 * When an insertion fills the log buffer and causes it to wrap, a new
 * epoch is created for the portion of the log buffer representing
 * the new segment, and the old epoch keeps track of the portion of the 
 * log buffer representing the old segment.  The inserted log record
 * usually spans the two segements, as the segments are written contiguously
 * to the same log file (partition).
 *
 * When an insertion causes a wrap and there is no more room in the
 * partition to hold the new segment, a new 
 * epoch is created for the portion of the log buffer representing
 * the new segment, and the old epoch keeps track of the portion of the 
 * log buffer representing the old segment, as before.  
 * Now, however, the inserted log record is inserted, in its entirety,
 * in the new segment.  Thus, no log record spans partitions.
 *
 * Meanwhile, the log-flush buffer knows about the possible existence of
 * two epochs.  When an old epoch is valid, it flushes that epoch.
 * When a new epoch is also valid, it flushes that new one as well.
 * If the two epochs have the same target partition, the two flushes are
 * done with a single write.
 *
 * The act of flushing an epoch to a partition consists in a single
 * write of a size that is an even multiple of BLOCK_SIZE.  The
 * flush appends a skip_log record, and zeroes as needed, to round out the
 * size of the write.  Writes re-write portions of the log already
 * written, in order to overwrite the skip_log record at the tail of the
 * log (and put a new one at the new tail).
 *
 *
 *\subsection RECOV Recovery
 * The storage manager performs ARIES-style logging and recovery.
 * This means the logging and recovery system has these characteristics:
 * - uses write-ahead logging (WAL)
 * - repeats history on restart before doing any rollback 
 * - all updates are logged, including those performed during rollback
 * - compensation records are used in the log to bound the amount
 *   of logging done for rollback 
 *   and guarantee progress in the case of repeated 
 *   failures and restarts.
 *
 * Each time a storage manager (ss_m class) is constructed, the logs
 * are inspected, the last checkpoint is located, and its lsn is
 * remembered as the master_lsn, then recovery is performed.
 * Recovery consists of three phases: analysis, redo and undo.
 *
 *\subsubsection RECOVANAL Analysis
 * This pass analyzes the log starting at the master_lsn, and
 *   reading log records written thereafter.  Reading the log records for the
 *   last completed checkpoint, it reconstructs the transaction table, the
 *   buffer-pool's dirty page table, and mounts the devices and
 *   volumes that were mounted at the time of the checkpoint.
 *   From the dirty page table, it determines the \e redo_lsn, 
 *   the lowest recovery lsn of the dirty pages, which is 
 *   where the next phase of recovery must begin.
 *
 *\subsubsection RECOVREDO Redo
 * This pass starts reading the log at the redo_lsn, and, for each
 *   log record thereafter, decides whether that log record's 
 *   work needs to be redone.  The general protocol is:
 *   - if the log record is not redoable, it is ignored
 *   - if the log record is redoable and contains a page ID, the
 *   page is inspected and its lsn is compared to that of the log
 *   record. If the page lsn is later than the log record's sequence number,
 *   the page does not need to be updated per this log record, and the
 *   action is not redone.
 *
 *\subsubsection RECOVUNDO Undo
 *  After redo,  the state of the database matches that at the time 
 *  of the crash.  Now the storage manager rolls back the transactions that 
 *  remain active.  
 *  Care is taken to undo the log records in reverse chronological order, 
 *  rather than allowing several transactions to roll back 
 *  at their own paces.  This is necessary because some operations 
 *  use page-fixing for concurrency-control (pages are protected 
 *  only with latches if there is no page lock in
 *  the lock hierarchy -- this occurs when 
 *  logical logging and high-concurrency locking are used, 
 *  in the B-trees, for example.  A crash in the middle of 
 * a compensated action such as a page split must result in 
 * the split being undone before any other operations on the 
 * tree are undone.). 
 *
 * After the storage manager has recovered, control returns from its
 * constructor method to the caller (the server).
 * There might be transactions left in prepared state.  
 * The server is now free to resolve these transactions by 
 * communicating with its coordinator. 
 *
 *\subsection LSNS Log Sequence Numbers
 *
 * Write-ahead logging requires a close interaction between the
 * log manager and the buffer manager: before a page can be flushed
 * from the buffer pool, the log might have to be flushed.
 *
 * This also requires a close interaction between the transaction
 * manager and the log manager.
 * 
 * All three managers understand a log sequence number (lsn_t).
 * Log sequence numbers serve to identify and locate log records
 * in the log, to timestamp pages, identify timestamp the last
 * update performed by a transaction, and the last log record written
 * by a transaction.  Since every update is logged, every update
 * can be identified by a log sequence number.  Each page bears
 * the log sequence number of the last update that affected that
 * page.
 *
 * A page cannot be written to disk until  the log record with that
 * page's lsn has been written to the log (and is on stable storage).
 * A log sequence number is a 64-bit structure,  with part identifying
 * a log partition (file) number and the rest identifying an offset within the file. 
 *
 * \subsection LOGPART Log Partitions
 *
 * The log is partitioned to simplify archiving to tape (not implemented)
 * The log comprises 8 partitions, where each partition's
 * size is limited to approximately 1/8 the maximum log size given
 * in the run-time configuration option sm_logsize.
 * A partition resides in a file named \e log.\<n\>, where \e n
 * is the partition number.
 * The configuration option sm_logdir names a directory 
 * (which must exist before the storage manager is started) 
 * in which the storage manager may create and destroy log files.
 *
 *  The storage manger may have at most 8 active partitions at any one time.  
 *  An active partition is one that is needed because it 
 *  contains log records for running transactions.  Such partitions 
 *  could (if it were supported) be streamed to tape and their disk 
 *  space reclaimed.  Space is reclaimed when the oldest transaction 
 *  ends and the new oldest transaction's first log record is 
 *  in a newer partition than that in which the old oldest 
 *  transaction's first log record resided.  
 *  Until tape archiving is implemented, the storage 
 *  manager issues an error (eOUTOFLOGSPACE) 
 *  if it consumes sufficient log space to be unable to 
 *  abort running transactions and perform all resulting necessary logging 
 *  within the 8 partitions available. 
 * \note Determining the point at which there is insufficient space to
 * abort all running transactions is a heuristic matter and it
 * is not reliable}.  
 *
 * Log records are buffered by the log manager until forced to stable 
 * storage to reduce I/O costs.  
 * The log manager keeps a buffer of a size that is determined by 
 * a run-time configuration option.  
 * The buffer is flushed to stable storage when necessary.  
 * The last log in the buffer is always a skip log record, 
 * which indicates the end of the log partition.
 *
 * Ultimately, archiving to tape is necessary.  The storage manager
 * does not perform write-aside or any other work in support of
 * long-running transactions.
 *
 * The checkpoint manager chkpt_m sleeps until kicked into action
 * by the log manager, and when it is kicked, it takes a checkpoint, 
 * then sleeps again.  Taking a checkpoint amounts to these steps:
 * - Write a chkpt_begin log record.
 * - Write a series of log records recording the mounted devices and volumes..
 * - Write a series of log records recording the mounted devices.
 * - Write a series of log records recording the buffer pool's dirty pages.
 *    For each dirty page in the buffer pool, the page id and its recovery lsn 
 *    is logged.  
 *    \anchor RECLSN
 *    A page's  recovery lsn is metadata stored in the buffer 
 *    manager's control block, but is not written on the page. 
 *    It represents an lsn prior to or equal to the log's current lsn at 
 *    the time the page was first marked dirty.  Hence, it
 *    is less than or equal to the LSN of the log record for the first
 *    update to that page after the page was read into the buffer
 *    pool (and remained there until this checkpoint).  The minimum
 *    of all the  recovery lsn written in this checkpoint 
 *    will be a starting point for crash-recovery, if this is 
 *    the last checkpoint completed before a crash.
 * - Write a series of log records recording the states of the known 
 *    transactions, including the prepared transactions.  
 * - Write a chkpt_end log record.
 * - Tell the log manage where this checkpoint is: the lsn of the chkpt_begin
 *   log record becomes the new master_lsn of the log. The master_lsn is
 *   written in a special place in the log so that it can always be 
 *   discovered on restart.
 *
 *   These checkpoint log records may interleave with other log records, making
 *   the checkpoint "fuzzy"; this way the world doesn't have to grind to
 *   a halt while a checkpoint is taken, but there are a few operations that
 *   must be serialized with all or portions of a checkpoint. Those operations
 *   use mutex locks to synchronize.  Synchronization of operations is
 *   as follows:
 *   - Checkpoints cannot happen simultaneously - they are serialized with
 *   respect to each other.
 *   - A checkpoint and the following are serialized:
 *      - mount or dismount a volume
 *      - prepare a transaction
 *      - commit or abort a transaction (a certain portion of this must
 *        wait until a checkpoint is not happening)
 *      - heriocs to cope with shortage of log space
 *   - The portion of a checkpoint that logs the transaction table is
 *     serialized with the following:
 *      - operations that can run only with one thread attached to
 *        a transaction (including the code that enforces this)
 *      - transaction begin, end
 *      - determining the number of active transactions
 *      - constructing a virtual table from the transaction table
 *
 * \section BF_M Buffer Manager
 * The buffer manager is the means by which all other modules (except
 * the log manager) read and write pages.  
 * A page is read by calling bf_m::fix.
 * If the page requested cannot be found in the buffer pool, 
 * the requesting thread reads the page and blocks waiting for the read to complete.
 *
 * All frames in the buffer pool are the same size, and they cannot be coalesced, 
 * so the buffer manager manages a set of pages of fixed size.
 *
 * \subsection BFHASHTAB Hash Table
 * The buffer manager maintains a hash table mapping page IDs to
 * buffer control blocks.  A control block points to its frame, and
 * from a frame one can arithmetically locate its control block (in
 * bf_m::get_cb(const page_s *)).
 * The hash table for the buffer pool uses cuckoo hashing 
 * (see \ref P1) with multiple hash functions and multiple slots per bucket.  
 * These are compile-time constants and can be modified (bf_htab.h).
 *
 * Cuckoo hashing is subject to cycles, in which making room on one 
 * table bucket A would require moving something else into A.
 * Using at least two slots per bucket reduces the chance of a cycle.
 *
 * The implementation contains a limit on the number of times it looks for
 * an empty slot or moves that it has to perform to make room.  It does
 * If cycles are present, the limit will be hit, but hitting the limit
 * does not necessarily indicate a cycle.  If the limit is hit,
 * the insert will fail.
 * The "normal" solution in this case is to rebuild the table with
 * different hash functions. The storage manager does not handle this case.
 *
 * \bug GNATS 35 The buffer manager hash table implementation contains a race.
 * While a thread performs a hash-table
 * lookup, an item could move from one bucket to another (but not
 * from one slot to another within a bucket).
 * The implementation contains a temporary work-around for
 * this, until the problem is more gracefully fixed: if lookup fails to
 * find the target of the lookup, it performs an expensive lookup and
 * the statistics record these as bf_harsh_lookups. This is expensive.
 *
 * \subsection REPLACEMENT Page Replacement
 * When a page is fixed, the buffer manager looks for a free buffer-pool frame,
 * and if one is not available, it has to choose a victim to replace. 
 * It uses a clock-based algorithm to determine where in the buffer pool
 * to start looking for an unlatched frame:
 * On the first pass of the buffer pool it considers only clean frames. 
 * On the second pass it will consider dirty pages,
 * and on the third or subsequent pass it will consider any frame.
 *
 * The buffer manager forks background threads to flush dirty pages. 
 * The buffer manager makes an attempt to avoid hot pages and to minimize 
 * the cost of I/O by sorting and coalescing requests for contiguous pages. 
 * Statistics kept by the buffer manager tell the number of resulting write 
 * requests of each size.
 *
 * There is one bf_cleaner_t thread for each volume, and it flushes pages for that
 * volume; this is done so that it can combine contiguous pages into
 * single write requests to minimize I/O.  Each bf_cleaner_t is a master thread with
 * multiple page-writer slave threads.  The number of slave threads per master
 * thread is controlled by a run-time option.
 * The master thread can be disabled (thereby disabling all background
 * flushing of dirty pages) with a run-time option. 
 *
 * The buffer manager writes dirty pages even if the transaction
 * that dirtied the page is still active (steal policy). Pages
 * stay in the buffer pool as long as they are needed, except when
 * chosen as a victim for replacement (no force policy).
 *
 * The replacement algorithm is clock-based (it sweeps the buffer
 * pool, noting and clearing reference counts). This is a cheap
 * way to achieve something close to LRU; it avoids much of the
 * overhead and mutex bottlenecks associated with LRU.
 *
 * The buffer manager maintains a hash table that maps page IDs to buffer 
 * frame  control blocks (bfcb_t), which in turn point to frames
 * in the buffer pool.  The bfcb_t keeps track of the page in the frame, 
 * the page ID of the previously-held page, 
 * and whether it is in transit, the dirty/clean state of the page, 
 * the number of page fixes (pins) held on the page (i.e., reference counts), 
 * the \ref RECLSN "recovery lsn" of the page, etc.  
 * The control block also contains a latch.  A page, when fixed,
 * is always fixed in a latch mode, either LATCH_SH or LATCH_EX.
 *
 * Page fixes are expensive (in CPU time, even if the page is resident).
 *
 * Each page type defines a set of fix methods that are virtual in 
 * the base class for all pages: The rest of the storage manager 
 * interacts with the buffer manager primarily through these methods 
 * of the page classes.  
 * The macros MAKEPAGECODE are used for each page subtype; they 
 * define all the fix methods on the page in such a way that bf_m::fix() 
 * is properly called in each case. 
 *
 * A page frame may be latched for a page without the page being read from disk; this
 * is done when a page is about to be formatted. 
 *
 * The buffer manager is responsible for maintaining WAL; this means it may not
 * flush to disk dirty pages whose log records have not reached stable storage yet.
 * Temporary pages (see sm_store_property_t) do not get logged, so they do not
 * have page lsns to assist in determining their clean/dirty status, and since pages
 * may change from temporary (unlogged) to logged, they require special handling, described
 * below.
 *
 * When a page is unfixed, sometimes it has been updated and must be marked dirty.
 * The protocol used in the storage manager is as follows:
 *
 * - Fixing with latch mode EX signals intent to dirty the page. If the page
 *   is not already dirty, the buffer control block for the page is given a
 *   recovery lsn of the page's lsn. This means that any dirtying of the page
 *   will be done with a log record whose lsn is larger than this recovery lsn.
 *   Fixing with EX mode of an already-dirty page does not change 
 *   the recovery lsn  for the page.
 *
 * - Clean pages have a recovery lsn of lsn_t::null.
 *
 * - A thread updates a page in the buffer pool only when it has the
 *   page EX-fixed(latched).
 *
 * - After the update to the page, the thread writes a log record to 
 *   record the update.  The log functions (generated by Perl) 
 *   determine if a log record should be written (not if a tmp 
 *   page, or if logging turned off, for example),
 *   and if not, they call page.set_dirty() so that any subsequent
 *   unfix notices that the page is dirty.
 *   If the log record is written, the modified page is unfixed with
 *   unfix_dirty() (in xct_impl::give_logbuf).
 *
 * - Before unfixing a page, if it was written, it must be marked dirty first
 *   with 
 *   - set_dirty followed by unfix, or
 *   - unfix_dirty (which is set_dirty + unfix).
 *
 * - Before unfixing a page, if it was NOT written, unfix it with bf_m::unfix
 *   so its recovery lsn gets cleared.  This happens only if this is the
 *   last thread to unfix the page.  The page could have multiple fixers 
 *   (latch holders) only if it were fixed in SH mode.  If fixed (latched)
 *   in EX mode,  this will be the only thread to hold the latch and the
 *   unfix will clear the recovery lsn.
 *
 *  It is possible that a page is fixed in EX mode, marked dirty but never
 *  updated after all,  then unfixed.  The buffer manager attempts to recognize
 *  this situation and clean the control block "dirty" bit and recovery lsn.
 *
 * Things get a little complicated where the buffer-manager's 
 * page-writer threads are
 * concerned.  The  page-writer threads acquire a share latches and copy
 * dirty pages; this being faster than holding the latch for the duration of the
 * write to disk
 * When the write is finished,  the page-writer re-latches the page with the
 * intention of marking it clean if no intervening updates have occurred. This
 * means changing the \e dirty bit and updating the recovery lsn in the buffer 
 * control block. The difficulty lies in determining if the page is indeed clean,
 * that is, matches the latest durable copy.
 * In the absence of unlogged (t_temporary) pages, this would not be terribly
 * difficult but would still have to cope with the case that the page was
 * (updated and) written by another thread between the copy and the re-fix.
 * It might have been cleaned, or that other thread might be operating in
 * lock-step with this thread.
 * The conservative handling would be not to change the recovery lsn in the
 * control block if the page's lsn is changed, however this has 
 * serious consequences
 * for hot pages: their recovery lsns might never be moved toward the tail of
 * the log (the recovery lsns remain artificially low) and 
 * thus the hot pages can prevent scavenging of log partitions. If log
 * partitions cannot be scavenged, the server runs out of log space.
 * For this reason, the buffer manager goes to some lengths to update the
 * recovery lsn if at all possible.
 * To further complicate matters, the page could have changed stores, 
 * and thus its page type or store (logging) property could differ.
 * The details of this problem are handled in a function called determine_rec_lsn().
 *
 * \subsection PAGEWRITERMUTEX Page Writer Mutexes
 *
 * The buffer manager keeps a set of \e N mutexes to sychronizing the various
 * threads that can write pages to disk.  Each of these mutexes covers a
 * run of pages of size smlevel_0::max_many_pages. N is substantially smaller
 * than the number of "runs" in the buffer pool (size of 
 * the buffer pool/max_many_pages), so each of the N mutexes actually covers
 * several runs:  
 * \code
 * page-writer-mutex = page / max_many_pages % N
 * \endcode
 *
 * \subsection BFSCAN Foreground Page Writes and Discarding Pages
 * Pages can be written to disk by "foreground" threads under several
 * circumstances.
 * All foreground page-writing goes through the method bf_m::_scan.
 * This is called for:
 * - discarding all pages from the buffer pool (bf_m::_discard_all)
 * - discarding all pages belonging to a given store from the buffer pool 
 *   (bf_m::_discard_store), e.g., when a store is destroyed.
 * - discarding all pages belonging to a given volume from the buffer pool 
 *   (bf_m::_discard_volume), e.g., when a volume is destroyed.
 * - forcing all pages to disk (bf_m::_force_all) with or without invalidating
 *   their frames, e.g., during clean shutdown.
 * - forcing all pages of a store to disk (bf_m::_force_store) with 
 *   or without invalidating
 *   their frames, e.g., when changing a store's property from unlogged to
 *   logged.
 * - forcing all pages of a volume to disk (bf_m::_force_store) with 
 *   without invalidating the frames, e.g., when dismounting a volume.
 * - forcing all pages whose recovery lsn is less than or equal to a given
 *   lsn_t, e.g.,  for a clean shutdown, after restart.
 */
