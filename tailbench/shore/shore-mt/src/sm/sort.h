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

/*<std-header orig-src='shore' incl-file-exclusion='SORT_H'>

 $Id: sort.h,v 1.30 2010/05/26 01:20:45 nhall Exp $

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

#ifndef SORT_H
#define SORT_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

// NOTE: you cannot run all the smsh scripts w/o INSTRUMENT_SORT
// turned on.
// We define a method that will allow us to find out
// if INSTRUMENT_SORT is defined; this way the smsh scripts can
// avoid croaking by running things that rely on this being defined.
// It can't be inlined. It's called only by smsh. 
#undef  INSTRUMENT_SORT
extern "C" bool sort_is_instrumentd(); // sort.cpp

#include <lexify.h>
#include <sort_s.h>

#ifdef __GNUG__
#pragma interface
#endif


struct sort_desc_t;
class run_scan_t;

/**\addtogroup SSMSORT
 *
 * The storage manager provides two tools for sorting,
 * the class sort_stream_i and the method ss_m::sort_file.
 *
 * The class sort_stream_i implements
 * sorting of \<key,elem\> pairs with a simple interface.
 * It is an adjunct of bulk-loading indexes. One inserts
 * \<key,elem\> pairs into the stream, and then iterates over
 * the stream with its sort_stream_i::get_next method to 
 * retrieve the pairs in order.  The sort_stream_i uses temporary
 * files as necessary to store the data stream.
 *
 * The storage manager function ss_m::sort_file implements
 * a polyphase merge-sort.
 *
 * Both methods offer a variety of run-time options. 
 * The options are specified with different APIs:
 * - The sort_stream_i uses \ref ssm_sort::key_info_t to describe keys 
 *   and \ref ssm_sort::sort_parm_t to describe behavior options.
 * - The ss_m::sort_file uses \ref ssm_sort::sort_keys_t to describe keys 
 *   and behavior options.
 *
 * \note Historical note: The original implementation of the sort_file
 * method used the same APIs (for specifying sort behavior and describing
 * keys) as sort_stream_i, but that implementation for sort_file was
 * inadequate for the needs of the projects using the storage manager
 * at the time (viz user-defined key types, 
 * marshalling and unmarshalling, etc.). 
 * The new implementation is more general, but the sort_stream_i
 * has not been rewritten to use the new options- and key- descriptor classes.
 *
 * See sort_stream_i for details and \ref sort_stream.cpp for an example
 * of its use.
 * The balance of this section describes the use of ss_m::sort_file and
 * its application to bulk-loading.
 *
 * Sort_file supports: 
 * - sorting on one or more keys
 * - key types may be 
 *    - fundamental (predefined) or user-defined
 *    - derived or embedded
 * - records being sorted may require marshalling when read from disk 
 *   to memory and unmarshalling when written back to disk
 * - the result of the sort can be 
 *   - a sorted copy of the input file
 *   - a file ready for bulk-loading an index to the original file
 *
 * For all this generality, the sort uses \ref SORTCALLBACK "callbacks" 
 * to the server for such things as 
 * - key comparisons
 * - key derivation
 * - object marshalling and unmarshalling
 * - producing the final form of keys for output, when the sort key
 * is not the key to be loaded into the index (for example consider R-trees:
 * a polygon is the index key, but the records are sorted on 
 * the polygon's Hilbert value)
 *
 * \section SORTRUN Runs 
 *
 * The sort takes as many runs as required to read and sort the
 * entire input file, while limiting the amount of memory used.
 * The run size is given as an argument to sort_file.
 * 
 * The sort uses temporary files when the input file contains more records
 * than can fit in one run (determined by the run size). 
 * These temporary files may be spread across 
 * multiple volumes, which is useful if the
 * volumes reside on different spindles.
 *
 * If the entire input file fits into the buffer pool pages of one run,
 * much of the complexity of the sort is eliminated, because the copying
 * of objects and metadata to scratch files is unnecessary, but for the
 * general (polyphase) case, behavioral options describe how the
 * writing to scratch files is handled, and these options depend on
 * the kind of output desired (see \ref SORTOUTPUT).
 *
 *
 * \section SORTCALLBACK Callbacks 
 *
 * The sort has to call back to the server for 
 *
 * - marshalling a record (ssm_sort::MOF):  
 *   When a record is first encountered in the 
 *   input file, it may be marshalled to produce an in-memory version of the
 *   entire object (presumably byte-swapped, pointer-swizzled, etc.). This
 *   copy of the record (in the form of an object_t,
 *   which is a handle for the record or in-memory representation)
 *   is passed to the next callback function (ssm_sort::CSKF).
 *   If a marshal function is supplied it is called at least once on
 *   every record.
 *
 * - creating or locating a key (ssm_sort::CSKF): 
 *   This callback locates or derives a key for the object.
 *   Its result is an skey_t, which is a handle for the derived or
 *   embedded key.  This function might have to allocate heap space for
 *   the derived keys, in which case sort_file has to deallocate such
 *   space, so this callback takes a factory_t for heap management.
 *
 * - comparing two keys (ssm_sort::CF): The server provides this function to compare
 *   two byte streams of some given length each.
 *
 * - unmarshalling an object (ssm_sort::UMOF): 
 *   Before the object is written to the output
 *   file after the last run, it may be unmarshalled to produce an on-disk
 *   version of the object.  The unmarshal function need not be called
 *   on every record. When the output is to be a bulk-loadable file
 *   for indexes, unmarshal is not needed. When the output is a copy
 *   of the input file, it is sometimes needed:
 *   - Case 1, Large object, not deep copy (large-object store will be
 *     transferred to the output file so carrying the object has no 
 *     effect here) :  No need to unmarshal; 
 *     the large object
 *     was marshaled in the first place only for the purpose of locating
 *     or deriving a sort key.  
 *   - Case 2, Large object with deep copy or small object 
 *     - Case 2a, Carrying object : Call unmarshal function (UMOF), write
 *       its result to the output file.
 *     - Case 2b, Not carrying object : No need to unmarshal; pin the original
 *       object and copy it to the output file.
 *
 * \section SORTOUTPUT Behavior and Results of Sort 
 *
 * Behaviors that can be controlled are the following; these
 * behaviors are determined by the contents of the sort_keys_t
 * argument to sort_file:
 * - The kind of output the sort produces, which can be
 *   - a sorted copy of the input file (ssm_sort::sort_keys_t::set_for_file), or
 *   - a file suitable for bulk-loading an index (sort_keys_t::set_for_index), 
 *   the format of which is:
 *     - header contains key in lexicographic format
 *     - body contains element
 * - whether the original file is to be retained or deleted by the sort
 *   (sort_keys_t::set_keep_orig), with variations on these choices
 * - how the key is to be located or derived (sort_keys_t::set_sortkey_fixed,
     sort_keys_t::set_sortkey_derived, and other attributes of
     sort_keys_t)
 * - whether the sort is to be stable (sort_keys_t::set_stable)
 * - whether the sort is ascending or descending (sort_keys_t::set_ascending)
 * - whether the sort is to eliminate duplicate nulls (sort_keys_t::set_null_unique)
 * - whether the sort is to eliminate all duplicates (sort_keys_t::set_unique)
 * - whether the sort is to call user-defined marshal and unmarshal
 *   functions to get/put the resulting data from/to disk
 *
 * Many behavioral options depend on other options, as discussed below.
 *
 * \subsection SORTOUTPUTFILE Result is Sorted Copy of Input File
 *
 * Use ssm_sort::sort_keys_t::set_for_file to create a sorted copy of the input file.
 *
 * Applicable key description data:
 *   - number of keys : >= 1
 *   - key access (for each key):
 *      - in header or body : a key or its source data 
 *          may reside in either header or body of the original record
 *      - at fixed location (ssm_sort::sort_keys_t::set_sortkey_fixed): 
 *          In all records this key is at the same
 *            location in the record (header or body).  User may not provide 
 *            a ssm_sort::CSKF for the key (it will not be used, so it is rejected).
 *          - aligned : By indicating that a key is adequately aligned in
 *          the original record for its key-comparison function (ssm_sort::CF) to 
 *          operate on it, the sort function can avoid the work 
 *          of copying to an aligned and contiguous buffer before calling the 
 *          comparison function (ssm_sort::CF).  (The sort function determines if the
 *          key resides entirely on one page; the user does not have to
 *          indicate this.)  If the key is in lexicographic format in the
 *          original record (after marshalling, if marshalling is used),
 *          alignment is immaterial.
 *      - derived (ssm_sort::sort_keys_t::set_sortkey_derived) : User must provide a ssm_sort::CSKF callback function to derive the
 *            key. The source data may be in
 *            the header or body of the record. The ssm_sort::CSKF "knows" the
 *            key's offset from the header or body, or receives that
 *            information from an argument (key_cookie_t) passed to the
 *            callback function.
 *   - lexicographic format:  ssm_sort::sort_keys_t::is_lexico indicates that
 *          the key is in \ref LEXICOFORMAT "lexigographic format"
 *          in the original record, so it can be compared in 
 *          segments (if, say, it is spread
 *          across page boundaries) and its alignment is immaterial.
 *
 * Applicable sort behavior options:
 * - marshalling : A callback marshal function (MOF) will be called
 *           to reformat records from which keys are created. This will
 *           be done before the ssm_sort::CSKF is called.
 * - ascending : sort in ascending or descending order
 * - stable : optionally ensure stable sort
 * - unique : eliminate records with duplicate keys
 * - null_unique : eliminate records with duplicate null keys
 * - keep_orig : optionally delete the original file 
 * - carry_obj : optionally duplicate the entire object in the runs
 * - deep_copy : optionally copy the large objects
 *
 * \subsection SORTOUTPUTINDEX Result is Input to Bulk-Load
 *
 * Applicable key description data:
 *   - number of sort keys : only one-key sort is supported
 *   - index key derivation: the index key may differ from the sort key,
 *     in which case the user provides a callback (ssm_sort::CSKF) function
*       to produce the index key. It must be produced in 
 *          \ref LEXICOFORMAT "lexigographic format".
 *   - sort key access:
 *      - in header or body : a key or its source data 
 *          may reside in either header or body of the original record
 *      - at fixed location (ssm_sort::sort_keys_t::set_sortkey_fixed): 
 *          In all records the key is at the same
 *            location in the record (header or body).  User may not provide 
 *            a ssm_sort::CSKF for the key (it will not be used, so it is rejected).
 *          - aligned : By indicating that a key is adequately aligned in
 *          the original record for its key-comparison function (ssm_sort::CF) to 
 *          operate on it, the sort function can avoid the work 
 *          of copying to an aligned and contiguous buffer before calling the 
 *          comparison function (ssm_sort::CF).  (The sort function determines if the
 *          key resides entirely on one page; the user does not have to
 *          indicate this.)  If the key is in lexicographic format in the
 *          original record (after marshalling, if marshalling is used),
 *          alignment is immaterial.
 *      - derived (ssm_sort::sort_keys_t::set_sortkey_derived) : User must provide a ssm_sort::CSKF callback function to derive the
 *            key. The source data may be in
 *            the header or body of the record. The ssm_sort::CSKF "knows" the
 *            key's offset from the header or body, or receives that
 *            information from an argument (key_cookie_t) passed to the
 *            callback function.
 *       - lexicographic format: if 
 *          the key is in \ref LEXICOFORMAT "lexigographic format"
 *          in the original record, it can be compared in 
 *          segments (if, say, it is spread
 *          across page boundaries) and its alignment is immaterial.
 *          \b NOTE If it is \b not in 
 *          lexicographic format, a ssm_sort::CSKF callback
 *          must put it in lexicographic format for the purpose of
 *          loading into a B+-Tree (should the sort key be used as the
 *          index key), so the sort key must therefore be
 *          derived in this case.
 *
 * Applicable sort behavior options:
 * - marshalling : use callback marshal function (MOF) to reformat
 *                 records from which keys are created
 * - ascending : sort in ascending or descending order
 * - stable : may not be set because it might violate RID order  in the
 *        event of duplicates (the elements for the pairs with the same
 *        key are sorted on the element in the B+-Tree).  If stability
 *        is needed, the key should be derived from two keys, or should
 *        be a concatenation of multiple keys (either suitable colocated
          in the object or derived) that would ensure the stability.
 * - unique : eliminate records with duplicate keys
 * - null_unique : eliminate records with duplicate null keys
 * - keep_orig : n/a
 * - carry_obj : n/a
 * - deep_copy : n/a
 *
 * \section SORTKEYS Keys 
 *
 * Sort keys may be located in the input records or derived from 
 * the input records.
 * Index keys that are different from the sort key are derived.
 *
 * When the output is to be a sorted copy of the input file, the
 * keys do not appear in the output file except inasmuch as they
 * are embedded in the original input records.
 * The sort keys in this case may be derived from the record contents, 
 * in which case they truly do not appear in the output.
 * Multiple keys may be used for the sort, and they may be a combination
 * of fixed-location keys and derived keys. See \ref SORTOUTPUTFILE.
 *
 * When the output is to be a bulk-loadable file, its records takes the form
 * header = key, body = element, and sort-file gives the caller no control
 * over the elements' contents: they are the record-IDs of the original
 * records.  If something other than the record-ID is desired for bulk-loading
 * an index, the output can be made to be a sorted copy of the
 * original file, with a suitable unmarshal (UMOF) applied.
 *
 * Only one sort key can be used in this case, but the index key can differ
 * differ from the sort key. See \ref SORTOUTPUTINDEX.
 *
 */

//
// chunk list for large object buffer
//
/**\cond skip */
struct s_chunk  {

    char* data;
    s_chunk* next;

    NORET s_chunk() { data = 0; next = 0; };
    NORET s_chunk(w_base_t::uint4_t size, s_chunk* tail) { 
              data = new char[size];
              next = tail;
        };
    NORET ~s_chunk() { delete [] data; };
};

class chunk_mgr_t {

private:
    s_chunk* head;

    void _free_all() { 
          s_chunk* curr;
          while (head) {
        curr = head;
        head = head->next;
        delete curr;
          }
      };

public:

    NORET chunk_mgr_t() { head = 0; };
    NORET ~chunk_mgr_t() { _free_all(); };

    void  reset() { _free_all(); head = 0; };

    void* alloc(w_base_t::uint4_t size) {
          s_chunk* curr = new s_chunk(size, head);
          head = curr;
           return (void*) curr->data;
      };
};
/**\endcond skip */

#       define PROTOTYPE(_parms) _parms

#ifndef PFCDEFINED

#define PFCDEFINED
typedef int  (*PFC) PROTOTYPE((w_base_t::uint4_t kLen1, 
        const void* kval1, w_base_t::uint4_t kLen2, const void* kval2));

#endif

/**\class sort_stream_i
 * \brief Sorting tool.
 * \details
 * \ingroup SSMSORT
 * Used for bulk-loading indexes but may be used independently.
 * After creating an instance of sort_stream_i, you can keep putting
 * \<key,element\> pairs into the stream, which will save the
 * records in a temporary persistent store, sort them, and then
 * return them in sorted order like an iterator over the temporary
 * store.  The temporary store is destroyed upon completion (destruction).
 *
 * Before you can begin inserting pairs into the stream, you must initialize the
 * stream with information about the key on which to sort (where it is to 
 * be found, what its length and type are, etc.).
 *
 * See example \ref sort_stream.cpp
 */
class sort_stream_i : public smlevel_top, public xct_dependent_t 
{
  typedef ssm_sort::key_info_t key_info_t;
  typedef ssm_sort::sort_parm_t sort_parm_t;
  typedef ssm_sort::sort_keys_t sort_keys_t;
  friend class ss_m;

  public:

    /**\brief Constructor.  If you use this constructor you must use init().
    */
    NORET    sort_stream_i();
    /**\brief Constructor that calls init().
     *
     * @param[in] k See key_info_t, which describes the keys used to
     * sort the data to be inserted.  
     * @param[in] s See ssm_sort::sort_parm_t, which describes behavior of the sort.
     * @param[in] est_rec_sz  Estimated record size; allows the sort to
     * estimate how many items will fit in a page.
     */
    NORET    sort_stream_i(const key_info_t& k,
                const sort_parm_t& s, uint est_rec_sz=0);

    NORET    ~sort_stream_i();

    /**\brief Return a key-comparison function for the given
     * pre-defined key type.
     * @param[in] type See key_info_t. 
     * @param[in] up If true, the function returns will sort in
     * ascending order, if false, in descending order.
     */
    static PFC  get_cmp_func(key_info_t::key_type_t type, bool up);

    /**\brief Initialize the stream with necessary metadata.
     *
     * @param[in] k See key_info_t, which describes the keys used to
     * sort the data to be inserted.  
     * @param[in] s See ssm_sort::sort_parm_t, which describes behavior of the sort.
     * @param[in] est_rec_sz  Estimated record size; allows the sort to
     * estimate how many items will fit in a page.
     */
    void    init(const key_info_t& k, const sort_parm_t& s, uint est_rec_sz=0);

    /// Release resources, render unusable.  (Called by destructor if apropos.) 
    void    finish();

    /**\brief Insert a key,elem pair into the stream.
     * @param[in] key  Key of key,elem pair.
     * @param[in] elem Element of key,elem pair.
     *
     * \note Must be invoked in a transaction, although this is not
     * enforced gracefully.
     */
    rc_t    put(const cvec_t& key, const cvec_t& elem);

    /**\brief Fetch the next key,elem pair from the stream (in sorted order).
     * @param[out] key  Key of key,elem pair.
     * @param[out] elem Element of key,elem pair.
     * @param[out] eof Set to true if no more pairs in the stream.
     *
     * \note Must be invoked in a transaction, although this is not
     * enforced gracefully.
     */
    rc_t    get_next(vec_t& key, vec_t& elem, bool& eof) ;

    /// Returns true until the first put() call.
    bool    is_empty() const { return empty; }

    /// Sort happens at first get_next call, returns false until then.
    bool    is_sorted() const { return sorted; }

  private:
    void    set_file_sort() { _file_sort = true; _once = false; }
    void    set_file_sort_once(sm_store_property_t prop) 
                {
                    _file_sort = true; _once = true; 
                    _property = prop; 
                }
    rc_t    file_put(const cvec_t& key, const void* rec, uint rlen,
                uint hlen, const rectag_t* tag);

    rc_t    file_get_next(vec_t& key, vec_t& elem, 
                w_base_t::uint4_t& blen, bool& eof) ;

    rc_t    flush_run();        // sort and flush one run

    rc_t    flush_one_rec(const record_t *rec, rid_t& rid,
                const stid_t& out_fid, file_p& last_page,
                bool to_final_file);

    rc_t    remove_duplicates();    // remove duplicates for unique sort
    rc_t    merge(bool skip_last_pass);

    void    xct_state_changed( xct_state_t old_state, xct_state_t new_state);

    key_info_t        ki;        // key info
    sort_parm_t       sp;        // sort parameters
    sort_desc_t*      sd;        // sort descriptor

    bool              sorted;        // sorted flag
    bool              eof;        // eof flag
    bool              empty;        // empty flag
    const record_t*   old_rec;    // used for sort unique
  
    bool              _file_sort;    // true if sorting a file

    int2_t*           heap;           // heap array
    int               heap_size;     // heap size
    run_scan_t*       sc;           // scan descriptor array    
    w_base_t::uint4_t num_runs;      // # of runs for each merge
    int               r;           // run index

    chunk_mgr_t       buf_space;    // in-memory storage

    // below vars used for speeding up sort if whole file fits in memory
    bool              _once;        // one pass write to result file
    sm_store_property_t _property;    // property for the result file
};

class file_p;

//
// run scans
//
/**\cond skip */
class run_scan_t {
    typedef ssm_sort::key_info_t key_info_t;
    typedef ssm_sort::sort_parm_t sort_parm_t;

    lpid_t pid;         // current page id
    file_p* fp;         // page buffer (at most fix two pages for unique sort)
    int2_t   i;           // toggle between two pages
    int2_t   slot;        // slot for current record
    record_t* cur_rec;  // pointer to current record
    bool eof;         // end of run
    key_info_t kinfo;   // key description (location, len, type)
    int2_t   toggle_base; // default = 1, unique sort = 2
    bool   single;    // only one page
    bool   _unique;    // unique sort

public:
    PFC cmp;

    NORET run_scan_t();
    NORET ~run_scan_t();

    rc_t init(rid_t& begin, PFC c, const key_info_t& k, bool unique);
    rc_t current(const record_t*& rec);
    rc_t next(bool& eof);

    bool is_eof()   { return eof; }

    friend bool operator>(run_scan_t& s1, run_scan_t& s2);
    friend bool operator<(run_scan_t& s1, run_scan_t& s2);

    const lpid_t& page() const { return pid; }
};
/**\endcond skip */


/*<std-footer incl-file-exclusion='SORT_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
