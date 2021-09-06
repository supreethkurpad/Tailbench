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

/*<std-header orig-src='shore' incl-file-exclusion='SORT_S_H'>

 $Id: sort_s.h,v 1.32 2010/05/26 01:20:45 nhall Exp $

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

#ifndef SORT_S_H
#define SORT_S_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

// forward
class file_p;
class record_t;

#define OLDSORT_COMPATIBILITY

/**\brief A namespace to contain certain types used for ss_m::sort_file. */
namespace ssm_sort {

/**\brief Descriptor for sort key, used with sort_stream_i
 *
 * This structure was used in the original implementation
 * of sort_file (now deprecated).  Unfortunately, the
 * API for sort_stream_i has not been updated to use the
 * alternative key descriptor structure, sort_keys_t.
 */
struct key_info_t {
    typedef sortorder::keytype key_type_t;

    /**\cond skip */
    /// for backward compatibility only: should use keytype henceforth
    enum dummy_t {  t_char=sortorder::kt_u1,
                    t_int=sortorder::kt_i4,
                    t_float=sortorder::kt_f4,
                    t_string=sortorder::kt_b, 
                    t_spatial=sortorder::kt_spatial};
    /**\endcond skip */

    key_type_t  type;       // key type
    /// For sorting on spatial keys
    nbox_t      universe;   
    /**\brief Is a derived key. 
     * If true, the key must be the only item in rec
     *  header, and the header will not be copied to
     *  the result record (allows the user to store derived
     *  key in the header temporarily, for sorting purposes).
     */
    bool        derived;    

    /**\enum where_t
     **\brief Describes where the key is found: in a record header or body.
     * For file sort. 
     *
     * - t_hdr  : key is in the record header
     * - t_body  : key is in the record body
     */
    enum where_t { t_hdr = 0, t_body };
    /**\brief Where the key resides: header or body */
    where_t              where;      
    /**\brief Offset from beginning of header or body where the key starts */
    w_base_t::uint4_t    offset;     
    /**\brief Length of key */
    w_base_t::uint4_t    len;        
    /**\brief Estimated record length. A hint. */
    w_base_t::uint4_t    est_reclen; 
    
    key_info_t() {
      type = sortorder::kt_i4;
      where = t_body;
      derived = false;
      offset = 0;
      len = sizeof(int);
      est_reclen = 0;
    }
};

/**\brief Behavioral options for sort_stream_i.
 *
 * This structure was used in the original implementation
 * of sort_file (now deprecated).  Unfortunately, the
 * API for sort_stream_i has not been updated to use the
 * alternative behavioral options structure, sort_keys_t.
 */
struct sort_parm_t {
    /// Number of pages for each run.
    uint2_t run_size;        
    /// Volume on which scratch files will reside.
    vid_t   vol;        
    /// True -> duplicates will be removed. 
    bool   unique;        
    /// Sort in ascending order?
    bool   ascending;        
    /// Destroy the input file when done?
    bool   destructive;    

    /// Logging level of scratch files
    smlevel_3::sm_store_property_t property; 

    sort_parm_t() : run_size(10), unique(false), ascending(true),
            destructive(false), 
            property(smlevel_3::t_regular) {}
};


/*
 * For new sort:
 */

/**\brief Input, output argument to CSKF, MOF, UMOF callbacks.
 * \ingroup SSMSORT
 * \details
 * Since some of the callbacks need variously an integer or a
 * pointer, this class
 * puns an integer and a void *.
 *
 * See ss_m::sort_file
 */
class key_cookie_t {
    void *   c; // datum stored as a void*, punned by
                // the extraction methods
public:
    explicit key_cookie_t () : c(NULL) { }

    /// Create a cookie from an integer.
    explicit key_cookie_t (int i) {
        union { int _i; void *v; } _pun = {i};
        c = _pun.v;
    }
    /// Create a cookie from a pointer.
    explicit key_cookie_t (void *v):c(v) { }

    /// Extract integer value.
    int make_int() const { return operator int(); }

    /// Extract smsize_t value.
    int make_smsize_t() const { return operator smsize_t(); }

    /// Extract ptr value.
    void *make_ptr() const { return c; }

    static key_cookie_t   null; // newsort.cpp

private:
    operator int () const { 
         union { void *v; int _i; } _pun = {c};
             return _pun._i;
    }

    operator smsize_t () const { 
         union { void *v; int _i; } _pun = {c};
         smsize_t t =  _pun._i & 0xffffffff;
#ifdef ARCH_LP64
         w_assert1((_pun._i & 0xffffffff00000000) == 0);
#endif
         return t;
    }
};

/**\brief A memory allocator (abstract base class) used by ss_m::sort_file and its adjuncts.
 * \ingroup SSMSORT
 * \details
 * The ssm_sort::CSKF, ssm_sort::MOF and ssm_sort::UMOF callbacks  
 * may need to allocate memory, in which case they 
 * should use the factory passed in as an argument; this
 * allows the storage manager to free the memory using the same
 * allocator.
 *
 * These allocators are used with object_t (record handles) and 
 * skey_t (key handles).
 *
 * The default allocators used by ss_m::sort_file are 
 * - factory_t::none : does not allocate or free, used for statically-
 *   allocated space.
 * - factory_t::cpp_vector : a factory that allocates byte-strings that
 *   start on a double-aligned boundary and frees them. 
 *
 *  Users must write their own factories that inherit from
 *  factory_t.
 */
class factory_t 
{
   /* users are meant to write their own factories 
    * that inherit from this
    */
public:
   factory_t();
   virtual    NORET ~factory_t();

   virtual    void* allocfunc(smsize_t)=0;
   virtual    void freefunc(const void *, smsize_t)=0;

   // none: causes no delete - used for statically allocated space
   static factory_t*    none;

   // cpp_vector - simply calls delete[] 
   static factory_t*    cpp_vector;

   void freefunc(vec_t&v) {
    for(int i=v.count()-1; i>=0; i--) {
        DBG(<<"freefuncVEC(ptr=" << (void*)v.ptr(i) << " len=" << v.len(i));
        freefunc((void *)v.ptr(i), v.len(i));
    }
   }
};

/* XXX move virtual functions to a .cpp */
inline factory_t::factory_t() {}
inline NORET factory_t::~factory_t() {}

/**\brief Descriptor for fixed-location keys
 * \ingroup SSMSORT
 * \details
 * A sort_keys_t contains an instance of this for each key.
 */
class key_location_t 
{
public:
    /// Key is in record header
    bool         _in_hdr;
    /// Offset from start of header/body of start of key
    smsize_t     _off;
    /// Length of key
    smsize_t     _length;

    key_location_t() : _in_hdr(false), _off(0), _length(0)  {}

    key_location_t(const key_location_t &old) : 
        _in_hdr(old._in_hdr), 
        _off(old._off), _length(old._length) {}

    /// Key is in record header
    bool is_in_hdr() const { return _in_hdr; }
};

class skey_t; // forward

/**\brief Handle on a record in the buffer pool or in scratch memory.
 * \ingroup SSMSORT
 * \details
 * The ss_m::sort_file function calls a variety of user-defined 
 * callback functions to process records in the buffer pool
 * or in-memory copies of records and to produce more of the same. 
 *
 * This class provides a generic handle to reference the records or copies of 
 * records, since the callbacks need only to
 * - know the header length
 * - find the data at an offset in the header
 * - know the body length
 * - find the data at an offset in the body
 * - copy a generic handle
 * - copy out a portion of the header or body to a buffer
 * - create a new generic handle from a pair of buffer
 *
 * An object_t created from buffers must also know how the buffers were
 * allocated so it can free the buffers to the same heap, so it keeps
 * a reference to the factory_t used by the callback function
 * to create the buffers. 
 */
class object_t : public smlevel_top 
{
    friend class skey_t;
    static const object_t& none;
protected:
    void        _construct(file_p&, slotid_t);
    void        _construct(
              const void *hdr, smsize_t hdrlen, factory_t *,
              const void *body, smsize_t bodylen, factory_t *);

    void      _replace(const object_t&); // copy over
    NORET     object_t();
public:
    NORET     object_t(const object_t&o) 
               :
               _valid(false),
               _in_bp(false),
               _rec(0),
               _fp(0),
               _hdrfact(factory_t::none),
               _hdrlen(0),
               _hdrbuf(0),
               _bodyfact(factory_t::none),
               _bodylen(0),
               _bodybuf(0)
            { _replace(o); }

    NORET     object_t(
              const void *hdr, 
              smsize_t hdrlen, 
              factory_t& hf,
              const void *body, 
              smsize_t bodylen, 
              factory_t& bf) 
               :
               _valid(false),
               _in_bp(false),
               _rec(0),
               _fp(0),
               _hdrfact(&hf),
               _hdrlen(hdrlen),
               _hdrbuf(hdr),
               _bodyfact(&bf),
               _bodylen(bodylen),
               _bodybuf(body)
               { }

    NORET     ~object_t();

    bool        is_valid() const  { return _valid; }
    bool        is_in_buffer_pool() const { return is_valid() && _in_bp; }
    smsize_t    hdr_size() const { return _hdrlen; }
    smsize_t    body_size() const { return _bodylen; }

    const void *hdr(smsize_t offset) const; 
    const void *body(smsize_t offset) const;
    void        freespace();
    void        assert_nobuffers() const;
    smsize_t    contig_body_size() const; // pinned amt

    w_rc_t      copy_out(
                    bool in_hdr, 
                    smsize_t offset, 
                    smsize_t length, 
                    vec_t&dest) const;


private:      // data
    bool        _valid;
    bool        _in_bp; // in buffer pool or in memory

    // for in_buffer_pool:
    record_t*    _rec;
    file_p*      _fp;

    // for in_memory:
    factory_t*    _hdrfact;
    smsize_t      _hdrlen;
    const void*   _hdrbuf;

    factory_t*    _bodyfact;
    smsize_t      _bodylen;
    const void*   _bodybuf;

protected:
    int     _save_pin_count;
        // for callback
    void      _callback_prologue() const {
#        if W_DEBUG_LEVEL > 2
            /*
             * leaving SM
             */
            // escape const-ness of the method
            int *save_pin_count = (int *)&_save_pin_count;
            *save_pin_count = me()->pin_count();
            w_assert3(_save_pin_count == 0);
            me()->check_pin_count(0);
            me()->in_sm(false);
#        endif 
        }
    void      _callback_epilogue() const {
#        if W_DEBUG_LEVEL > 2
            /*
             * re-entering SM
             */
            me()->check_actual_pin_count(_save_pin_count);
            me()->in_sm(true);
#        endif 
        }
    void    _invalidate() {
               _valid=false;
               _in_bp=false;
               _rec = 0;
               _fp=0;
               _hdrfact=factory_t::none;
               _hdrlen=0;
               _hdrbuf=0;
               _bodyfact=factory_t::none;
               _bodylen=0;
               _bodybuf=0;
               _save_pin_count=0;
            }
};

class run_mgr; // forward

/**\brief The result of a CSKF function.  
 * \ingroup SSMSORT
 * \details
 * This is a handle on a key.  
 * The key might reside in scratch memory or in the buffer pool.
 * This class provides a generic API for using such a key.
 *
 * The attributes are:
 * - pointer to start of key (method ptr())
 * - length of key (method size())
 * - length of contiguous portion of key (method contig_length())
 * - location (in buffer pool or scratch memory) (method is_in_obj())
 * - sub-location (in buffer pool: in record header or body) (method is_in_hdr())
 * - the heap allocator (factory_t) used to allocate buffers if this
 *   refers to a key in scratch memory.
 *
 *   Generally only the constructor is used by a CSKF callback function,
 *   and the idiom used (indeed, the only way it can 
 *   be populated) is placement new as follows:
 *   \code
using namespace ssm_sort;
w_rc_t 
exampleCSKF(
    const rid_t&         rid,
    const object_t&      obj,
    key_cookie_t         ,  
    factory_t&           f,
    skey_t*              out
)
{
    {
    // this shows how to create an skey_t from the entire header of the
    // incoming object_t, whether the object is in the buffer pool or
    // in scratch memory:
    smsize_t length = obj.hdr_size();
    smsize_t offset = 0;
    bool     in_hdr = true;
    new(out) skey_t(obj, offset, length, in_hdr);
    }

    {
    // this shows how to create an skey_t from the last 3 bytes of the
    // body of the incoming object_t, whether the 
    // object is in the buffer pool or in scratch memory:
    smsize_t length = 3;
    smsize_t offset = obj.body_size() - length;
    bool     in_hdr = false;
    new(out) skey_t(obj, offset, length, in_hdr);
    }

    {
    // this shows how to create an skey_t derived from the record id
    int val = rid.pid.page;
    smsize_t length = sizeof(val);
    smsize_t offset = 0;
    void *buf = f.allocfunc(length);
    memcpy(&buf, &val, sizeof(val));
    new(out) skey_t(buf, offset, length, f);
    }

    return RCOK;
}
\endcode
 */
class skey_t 
{
    // To let run_mgr construct empty skey_t
    friend class ssm_sort::run_mgr;
public:
    /**\brief Construct from a key in the buffer pool
     * @param[in]  o  structure describing the key location
     * @param[in]  offset  offset from record header or body where key starts
     * @param[in]  len  length of entire key
     * @param[in]  in_hdr  true if key is in record header, false if in body
     */
    NORET skey_t(
        const object_t& o, 
        smsize_t offset,
        smsize_t len, 
        bool in_hdr) 

        : _valid(true), _in_obj(true),
        _length(len),
        _obj(&o), _offset(offset),
        _in_hdr(in_hdr), 
        _fact(factory_t::none), _buf(0)
        {
        }
    /**\brief Construct from a key in scratch memory
     * @param[in]  buf  scratch memory buffer
     * @param[in]  offset  offset from buf where key starts
     * @param[in]  len  length of entire key
     * @param[in]  f  factory used to allocate the buffer
     */
    NORET skey_t(
        void *buf, 
        smsize_t offset,
        smsize_t len,  // KEY len, NOT NECESSARILY BUF len
        factory_t& f
        ) 
        : _valid(true), _in_obj(false),
        _length(len),
        _obj(&object_t::none),
        _offset(offset),
        _in_hdr(false), 
        _fact(&f), _buf(buf)
        {
        }

    NORET   ~skey_t() {}

    /// Length of entire key
    smsize_t  size() const     { return _length; }

    /// True if this structure is populated (points to a key)
    bool      is_valid() const { return _valid; }

    /// True if key is in the buffer pool
    bool      is_in_obj() const { return is_valid() && _in_obj; }

    /**\brief True unless this key is for an object other than \a o
     *
     * Used for assertions in sort code.
     */
    bool      consistent_with_object(const object_t&o ) const { 
                                 return ((_obj == &o) || !_in_obj); }

    /// True if key is in buffer pool and is in record header
    bool      is_in_hdr() const { return is_in_obj() && _in_hdr; }

    /// Pointer into byte at \a offset in key
    const void *ptr(smsize_t offset) const;  // key

    /// Using its factory, free space allocated for the in-scratch-memory buffer
    void      freespace();

    /// Asserts that no heap memory is held by this or its object_t.
    void      assert_nobuffers()const;

    /// Pinned amount of the key
    smsize_t  contig_length() const; 

    /// Copies key into vector (which must have pre-allocated space)
    w_rc_t    copy_out(vec_t&dest) const;

private:
    bool        _valid; 
    bool        _in_obj; // else in mem
    smsize_t    _length;
protected:
    // for in_obj case;
    const object_t*    _obj;     
    smsize_t    _offset; // into buf or object of start of key
private:
    bool        _in_hdr;
    // for !in_obj but valid
    factory_t*    _fact;
    const void*    _buf;   // buf to be deallocated -- key
                // might not start at offset 0
protected:
    void      _invalidate();
    NORET     skey_t() : 
                _valid(false), _in_obj(false), _length(0),
                _obj(&object_t::none),
                _offset(0), _in_hdr(false),
                _fact(factory_t::none), _buf(0)
                { }
    void    _construct(
               const object_t *o, smsize_t off, smsize_t l, bool h) {
                _valid = true;
                _in_obj = true;
                _obj = o;
                _offset = off;
                _length = l;
                _in_hdr = h;
                _fact = factory_t::none;
                }
    void      _construct(
            const void *buf, smsize_t off, smsize_t l, factory_t* f) {
                _valid = true;
                _obj = 0;
                _in_obj = false;
                _offset = off;
                _length = l;
                _fact = f;
                _buf = buf;
                }
    void    _replace(const skey_t&); // copy over
    void    _replace_relative_to_obj(const object_t &, const skey_t&); // copy over
}; // skey_t


 /** 
 **\typedef  CSKF
 * \ingroup SSMSORT
 *\brief Create-Sort-Key Function.
 * @param[in] rid   Record ID of the record containing the key.
 * @param[in] in_obj  Handle (object_t) on the record whose key we need. 
 * @param[in] cookie  Describes the location and type of key in the source
 * record.
 * @param[in] f  Class for managing allocated space.
 * @param[out] out   Result is written to this object_t.  
 * \details
 *
 * This type of callback function fills in the skey_t \a out for
 *  a key in an object (record).  It is called only when the object 
 *  is first encountered in its run in a sort.
 *
 *  A set of predefined callbacks are provided, q.v.:
 *  - sort_keys_t::noCSKF  : vacuous - does nothing
 *  - sort_keys_t::generic_CSKF :  copies or lexifies a key based on its
 *      key_cookie_t argument.
 *
 *  See generic_CSKF_cookie for an example of a simple user-defined CSKF.
 *
 *  The skey_t \a out is pre-allocated and must be populated by 
 *  this callback function.
 *  The factory_t \a f may be used for this allocation, or a user-defined
 *  factory_t may be used instead. Whatever factory is used to allocate
 *  the buffer to hold a key, that same factory must be placed in
 *  the skey_t via the placement-new constructor call. Examples are
 *  given in the description for skey_t.
 *
 *  This callback must not free any space for \a in_obj.
 */  
typedef w_rc_t (*CSKF)(
    const rid_t&        rid,
    const object_t&     in_obj,
    key_cookie_t        cookie,  // type info
    factory_t&          f,
    skey_t*             out
);

/**\typedef MOF
 * \ingroup SSMSORT
 *\brief  Marshal Object Function 
 * @param[in] rid  Record id of the source record to marshal
 * @param[in] obj_in Handle on the source record (object_t)
 * @param[in] cookie Describes location and type of key in the source record
 * @param[out] obj_out Result is to be written to this object_t.  
 *\details
 * The \a obj_out is already allocated; this function must populate it.
 * The \a obj_out has no buffers or factory associated with it. The
 * MOF must populate the \a obj_out from its own factory (it may use
 * factory_t::cpp_vector, which uses the global heap, or it may 
 * use the factory from the \a obj_in, but ONLY if that factory is
 * \b not factory_t::none.
 * The \a obj_out
 * object carries its factory along with it throughout the sort process.
 *
 * This callback must not free any space for \a obj_in.
 *
 *  This type of callback is used 
 *  - when a record is first encountered in the input file
 *  - if sort_keys_t::carry_obj() is true,
 *  it's also called when the object is read back
 *  from a temporary file.
 *
 *  A predefined callback is provided: 
 *  - ssm_sort::sort_keys_t::noMOF  : vacuous - does nothing
 *
 *  The sort code checks for 
 *  \code
 sort_keys_t::marshal_func() == sort_keys_t::noMOF
 \endcode
 * in which case, it does not call a marshal function at all. (This
 * is less work than allocating the output object_t to call a
 * vacuous function.)
 */
typedef w_rc_t (*MOF)(
    const rid_t&       rid,  // record id
    const object_t&    obj_in,
    key_cookie_t       cookie,  // type info
                // func must allocate obj_out,
    object_t*         obj_out     // SM allocates, MOF initializes, SM
                // frees its buffers
    );

/**\typedef UMOF
 * \ingroup SSMSORT
 * \brief Un-marshal Object Function 
 * @param[in] rid  Record id of the source record to marshal
 * @param[in] obj_in Handle on the source record (object_t)
 * @param[in] cookie Describes location and type of key in the destination 
 * record
 * @param[out] obj_out Result is to be written to this object_t.  
 *
 * \details
 *
 * The \a obj_out is already allocated; this function must populate it.
 * The \a obj_out has no buffers or factory associated with it. The
 * MOF must populate the \a obj_out from its own factory (it may use
 * factory_t::cpp_vector, which uses the global heap, or it may 
 * use the factory from the \a obj_in, but ONLY if that factory is
 * \b not factory_t::none.
 * The \a obj_out
 * object carries its factory along with it throughout the sort process.
 *
 * This callback must not free any space for \a obj_in.
 *
 *  This type of callback is used 
 *  - when an object is written to a temp file
 *  - when the final object is written to the result file (if the
 *  output is a copy of the input file).
 *
 *  A predefined callback is provided: 
 *  - ssm_sort::sort_keys_t::noUMOF  : vacuous - does nothing
 *
 *  The sort code checks for 
 *  \code
 sort_keys_t::unmarshal_func() == sort_keys_t::noUMOF
 \endcode
 * in which case, it does not call a marshal function at all. (This
 * is less work than allocating the output object_t to call a
 * vacuous function.)
 *
 */
typedef w_rc_t (*UMOF)(
    const rid_t&       rid,  // orig record id of object in buffer
    const object_t&    obj_in,
    key_cookie_t       cookie,  // type info
    object_t*        obj_out // SM allocates, UMOF initializes,
            // SM will copy to disk, free mem
    );

/**\typedef CF
 * \ingroup SSMSORT
 * \brief key Comparison Function
 * @param[in] length1  Length of first key in bytes
 * @param[in] key1    Pointer to start of first key
 * @param[in] length2 Length of second key in bytes
 * @param[in] key2    Pointer to start of second key
 * \retval int Negative if key1 < key2, 0 if equal, Positive if key1 > key2
 * \details
 * Used on every key comparison.  
 * \note It is up to the server to determine if the keys are properly
 * aligned for comparison as fundamental types. Alignment is determined by
 * the combined behavior of the CSKF and MOF callbacks, as well as of
 * the location of the keys in the original records.
 */
typedef int (*CF) (
    w_base_t::uint4_t length1, 
    const void *      key1, 
    w_base_t::uint4_t length2, 
    const void *      key2
);

/**\typedef LEXFUNC
 * \ingroup SSMSORT
 * \brief Lexify key function
 * @param[in] source    Pointer to start of key
 * @param[in] len    Length of key
 * @param[out] sink    Pointer to output buffer (preallocated, of length len)
 * \details
 *
 * This type of function is called by the generic_CSKF. It may be useful
 * for user-defined CKSF callbacks.  Its purpose is to reformat keys 
 * into lexicographic form so that simple string-type (byte-by-byte)
 * comparisons yield the same results as typed comparisons would.
 * An alternative to lexifying keys and using string comparisons is to
 * use typed comparisons, however, that has its disadvantages, particularly
 * for keys that might be spread across page boundaries or are not aligned.
 */
typedef w_rc_t (*LEXFUNC) (const void *source, smsize_t len, void *sink);

/**\brief A cookie passed to generic_CSKF callback must point to one of these.
 * \ingroup SSMSORT
 * \details
 * This struct may be used by user-defined CSKF callback functions.
 * For example:
    \code
    w_rc_t 
    example_stringCSKF(
        const rid_t&        ,  // not used
        const object_t&     obj_in,
        key_cookie_t        cookie,  // generic_CSKF_cookie
        factory_t&          , // not used
        skey_t*             out
    )
    {
        // Cast the cookie to a pointer to a generic_CSKF_cookie.
        generic_CSKF_cookie&  K = *(generic_CSKF_cookie*)(cookie.make_ptr());
        bool is_in_header = false;

        // Populate the output : point to key in body of record/object 
        new(out) skey_t(obj_in, K.offset, K.length, is_in_header);
        return RCOK;
    }
    \endcode
 */
struct generic_CSKF_cookie 
{
    /// value == noLEXFUNC means don't lexify
    LEXFUNC    func; 
    /// True if the key is in the record header, false if in body.
    bool       in_hdr;
    /// Offset from start of record header or body.
    smsize_t   offset;
    /// Key length.
    smsize_t   length;
};

/**\brief Parameter to control behavior of sort_file. 
 * \ingroup SSMSORT
 * \details
 * This class determines how the sort will behave, and it holds descriptions
 * of the keys to be used for a sort.  
 * For details of the effect on a sort, see \ref SORTOUTPUT.
 * For details about sort keys, see \ref SORTKEYS.
 *
 * He who creates this data structure determines
 * what is "most significant" key, next-most significant key, etc.
 * Key 0 gets compared first, 1 next, and so on.
 *
 * For related types, see the ssm_sort namespace.
 */
class sort_keys_t 
{
public:
    typedef ssm_sort::LEXFUNC LEXFUNC;
 
    /**\brief Lexify callback function that does a simple memory copy 
     * @param[in] source    Pointer to start of key
     * @param[in] len    Length of key
     * @param[out] sink    Pointer to output buffer
     * 
     * \details
     * Does no reformatting; simply copies from source to sink.
     */
    static w_rc_t noLEXFUNC (const void *source, smsize_t len, void *sink);

    /**\brief Vacuous callback function, does nothing.
     * @param[in] rid   Ignored.
     * @param[in] obj  Ignored.
     * @param[in] cookie  Ignored.
     * @param[in] f  Ignored.
     * @param[out] out   Ignored.
     * \details
     * This function should never be used.  It is a default value.
     * The sort_file checks for
     * \code sort_keys_t::lexify() == sort_keys_t::noCSKF 
     * \endcode
     * and if so, bypasses any code connected with key creation, 
     * using the object_t it would have passed in to this function
     * as if it were the output of this function.
     * This comparison and bypass is faster than executing the
     * prologue and epilogue code to acquire space and release it,
     * needed when a CSKF is called.
     */
    static w_rc_t noCSKF(
        const rid_t&       rid,
        const object_t&    obj,
        key_cookie_t       cookie,  // type info
        factory_t&         f,
        skey_t*            out
    );

    /**\brief Either copies or lexifies a key.
     * @param[in] rid   Record ID of the record containing the key.
     * @param[in] in_obj  This refers to the record containing the key.
     * @param[in] cookie  Must be a pointer to a generic_CSKF_cookie,
     *                 which tells it which \ref LEXFUNC function to call 
     *                 (noLEXFUNC indicates straight copy),
     *                 and also tells it the length and location (offset) 
     *                 of the key.
     * @param[in] f    A heap manager for allocating space.
     * @param[out] out   Result is written here.
     *
     * \details
     * One normally expects the user to provide the entire
     * function for this, but we have this generic version just
     * for simplifying the handling of basic types for backward
     * compatibility.
     */
    static w_rc_t generic_CSKF(
        const rid_t&     rid,
        const object_t&  in_obj,
        key_cookie_t     cookie,  // type info
        factory_t&       f,
        skey_t*          out
    );

    /**\brief Vacuous Marshal Object Function
     * \details
     * This function is never called; rather, the
     * sort code checks for 
     * \code sort_keys_t::marshal_func() ==sort_keys_t::noMOF 
     * \endcode and if so, bypasses any code specific to
     * marshalling. This is done because the preparatory work
     * for calling a marshal function includes allocating space for
     * the results, and it is cheaper to bypass it altogether.
     */
    static w_rc_t noMOF (
        const rid_t&     ,  
        const object_t&  ,
        key_cookie_t     ,  
        object_t*    
    );

    /**\brief Vacuous Unmarshal Object Function
     * \details
     * This function is never called; rather, the
     * sort code checks for 
     * \code sort_keys_t::unmarshal_func() == sort_keys_t::noMOF 
     * \endcode and if so, bypasses any code specific to
     * unmarshalling. This is done because the preparatory work
     * for calling an unmarshal function includes allocating space for
     * the results, and it is cheaper to bypass it altogether.
     */
    static w_rc_t noUMOF (
        const rid_t&     ,  
        const object_t&  ,
        key_cookie_t     ,  
        object_t*    
    );

    /*
     * Default comparision functions for in-buffer-pool
     * comparisons.  No byte-swapping is done; alignment
     * requirements must already be met before calling these.
     */
    static int string_cmp(w_base_t::uint4_t , const void* , 
            w_base_t::uint4_t , const void*);
    static int uint8_cmp(w_base_t::uint4_t , const void* , 
            w_base_t::uint4_t , const void* );
    static int int8_cmp(w_base_t::uint4_t , const void* , 
            w_base_t::uint4_t , const void* );
    static int uint4_cmp(w_base_t::uint4_t , const void* , 
            w_base_t::uint4_t , const void* );
    static int int4_cmp(w_base_t::uint4_t , const void* , 
            w_base_t::uint4_t , const void* );
    static int uint2_cmp(w_base_t::uint4_t , const void* , 
            w_base_t::uint4_t , const void* );
    static int int2_cmp(w_base_t::uint4_t , const void* , 
            w_base_t::uint4_t , const void* );
    static int uint1_cmp(w_base_t::uint4_t , const void* , 
            w_base_t::uint4_t , const void* );
    static int int1_cmp(w_base_t::uint4_t , const void* , 
            w_base_t::uint4_t , const void* );
    static int f4_cmp(w_base_t::uint4_t , const void* , 
            w_base_t::uint4_t , const void* );
    static int f8_cmp(w_base_t::uint4_t , const void* , 
            w_base_t::uint4_t , const void* );

public:
    //@{
    /** @name LEXFUNCs
     * LEXFUNC (q.v.) functions for fundamental types.
     */
    static w_rc_t f8_lex(const void *source, smsize_t len, void *sink);
    static w_rc_t f4_lex(const void *source, smsize_t len, void *sink);
    static w_rc_t u8_lex(const void *source, smsize_t len, void *sink);
    static w_rc_t i8_lex(const void *source, smsize_t len, void *sink);
    static w_rc_t u4_lex(const void *source, smsize_t len, void *sink);
    static w_rc_t i4_lex(const void *source, smsize_t len, void *sink);
    static w_rc_t u2_lex(const void *source, smsize_t len, void *sink);
    static w_rc_t i2_lex(const void *source, smsize_t len, void *sink);
    static w_rc_t u1_lex(const void *source, smsize_t len, void *sink);
    static w_rc_t i1_lex(const void *source, smsize_t len, void *sink);
    //@}


private:

     /* Metadata about a key -- info about the key
     *     as it appears in all objects, not per-object
     *      key info.
     */
    class key_meta_t {
        public:
        /* 
         * callbacks:
         */ 
        CF              _cb_keycmp;  // comparison
        CSKF            _cb_keyinfo; // get location/copy/derived
        key_cookie_t    _cookie;
        w_base_t::int1_t _mask;
        fill1            _dummy1;
        fill2            _dummy2;
        key_meta_t() : _cb_keycmp(0), _cb_keyinfo(0), 
            _cookie(0),
            _mask(t_none) {}
        key_meta_t(const key_meta_t &old) : 
            _cb_keycmp(old._cb_keycmp), 
            _cb_keyinfo(old._cb_keyinfo),
            _cookie(old._cookie),
            _mask(old._mask) {}
    };
    int        _nkeys;        // constructor 
    int        _spaces;    // _grow
    key_meta_t*     _meta;
    key_location_t* _locs;
    bool    _stable;    // set_stable, is_stable
    bool    _for_index;    // set_for_index, is_for_index
    bool    _remove_dups;    // set_unique
    bool    _remove_dup_nulls; // set_null_unique
    bool    _ascending;    // ascending, set_ascending
    bool    _deep_copy;    // set_for_index, set_for_file
    bool    _keep_orig_file;// set_for_index, set_for_file, set_keep_orig
    bool    _carry_obj;    // set_for_file, carry_obj, set_carry_obj

    CSKF    _cb_lexify_index_key;    // set_for_index, index key lexify
    key_cookie_t _cb_lexify_index_key_cookie;    // set_for_index

    MOF        _cb_marshal;    // set_object_marshal
    UMOF    _cb_unmarshal;    // set_object_marshal
    key_cookie_t _cb_marshal_cookie;    // set_object_marshal

    void _grow(int i);

    void _prep(int key);
    void _set_loc(int key, smsize_t off, smsize_t len) {
        key_location_t &t = _locs[key];
        t._off = off;
        t._length = len;
    }
    void _set_mask(int key,
        bool fixed, 
        bool aligned, 
        bool lexico, 
        bool in_header,
        CSKF gfunc,
        CF   cfunc,
        key_cookie_t cookie
    ) {
        key_meta_t &t = _meta[key];
        if(aligned) t._mask |= t_aligned;
        else t._mask &= ~t_aligned;
        if(fixed) t._mask |= t_fixed;
        else t._mask &= ~t_fixed;
        if(lexico) t._mask |= t_lexico;
        else t._mask &= ~t_lexico;
        if(in_header) t._mask |= sort_keys_t::t_hdr;
        else t._mask &= ~sort_keys_t::t_hdr;
        t._cb_keycmp = cfunc;
        t._cb_keyinfo = gfunc;
        w_assert3(cfunc);
        if( ! fixed) {
            w_assert3(gfunc);
        }
        t._cookie = cookie;
    }

    void _copy(const sort_keys_t &old);

    /*
     * mask:
     * t_fixed: key location is fixed for all records: no need
     *    to call CSKF for each record 
     * t_aligned: key location adequately aligned (relative
     *    to the beginning of the record, which is 4-byte aligned) for
     *    in-buffer-pool comparisons with the given comparison
     *    function.  Copy to a contiguous buffer is unnecessary iff
     *    the entire key happens to be on one page (which is always
     *    the case with small records). 
     * t_lexico: Key is already in lexicographic order and
     *    can be spread across pages, and the comparison 
     *    function (usually string-compare or byte-compare) 
     *    can be called on successive segments of the key -- 
     *    copying they key to a contiguous buffer is unnecessary.
     *    Implies key is adequately aligned, but does not imply t_fixed.
     *
     *  t_hdr: key is at offset in hdr rather than in body 
     */

    enum mask { t_none = 0x0, 
        t_fixed = 0x1, 
        t_aligned =0x2, 
        t_lexico=0x4,
        t_hdr = 0x40  // key found in hdr
        };

public:
    /// Create a structure that's ready to be populated with \a nkeys keys.
    NORET sort_keys_t(int nkeys);

    NORET ~sort_keys_t() {
        delete[] _meta;
        delete[] _locs;
    }

    /// Copy operator.
    NORET sort_keys_t(const sort_keys_t &old);

    /// Return number of keys.
    int     nkeys() const { return _nkeys; }

    /**\brief Output is to be suitable for index bulk-load, index key is sort
     * key.
     *
     * Call this if you want the output file to be written with objects 
     * of the form 
     * * hdr == key, body==rid
     * and the input file not to be destroyed.
     * This file is suitable for bulk-loading an index, and
     * the index key is the sort key.
     *
     * You \b must provide conversion functions for the sort key
     * to be converted to a lexicographic format string if it is not
     * already in such format in the original record, if the
     * index key (being the sort key) is to be used in a B+-Tree.
     *
     * Only one sort key is supported when sorting for index bulk-load, but
     * the key may be derived, and so the CSKF callback can combine
     * multiple keys, and lexifying them ensures that they can be
     * sorted as one.  This is not entirely sufficient to cover all
     * cases of multiple keys, but it will do for many cases, particularly
     * where the sub-keys are of fixed length.
     */
    int  set_for_index() {
            _keep_orig_file = true;
            _deep_copy = false;
            _for_index = true; 
            if(_nkeys > 1) {
                return 1; // BAD 
                // we only support single-key btrees
            }
            _cb_lexify_index_key = sort_keys_t::noCSKF;
            _cb_lexify_index_key_cookie = key_cookie_t(0);
            return 0;
        }

    /**\brief Output is to be suitable for index bulk-load, index key is
     * different from the sort key.
     * @param[in] lfunc Key creation/location function for the index key.
     * @param[in] ck Datum for \a lfunc
     * \details
     *
     * Only one sort key can be used.
     *
     * Call this if you want the output file
     * to be written with objects of the form 
     * hdr == key, body==rid
     * and the input file not to be destroyed,
     * and you wish the index key to be different from the sort key.
     *
     * The \a lfunc argument must produce an index key 
     * in \ref LEXICOFORMAT "lexicographic format" if the index is to
     * be a B+-Tree.  This function is called when the record is
     * first encountered (reading the input file), since the record is
     * already pinned to gather a sort key.
     *
     * Only one sort key is supported when bulk-loading for indexes, but
     * the key may be derived, and so the CSKF callback can combine
     * multiple keys, and lexifying them ensures that they can be
     * sorted as one.  This is not entirely sufficient to cover all
     * cases of multiple keys, but it will do for many cases, particularly
     * where the sub-keys are of fixed length.
     */
    int   set_for_index(CSKF lfunc, key_cookie_t ck) {
            set_for_index();
            if(!lfunc) lfunc = sort_keys_t::noCSKF;
            _cb_lexify_index_key = lfunc;
            _cb_lexify_index_key_cookie = ck;
            return 0;
        }

    /// Return true if set_for_index() was called.
    bool    is_for_index() const { return _for_index; }

    /// Return true if set_for_file() was called.
    bool    is_for_file() const { return !_for_index; }

    /// Return true if set_stable() 
    bool    is_stable() const { return _stable; }

    /// Ensure stable sort.  Cannot be used with set_for_index.
    void    set_stable(bool val) { _stable = val; }

    /// Return the function that creates or locates and/or lexifies the key.
    CSKF    lexify_index_key() const { return _cb_lexify_index_key; }

    /// Pointer to input datum for the associated lexify() CSKF. 
    key_cookie_t     lexify_index_key_cookie() const { return _cb_lexify_index_key_cookie; }

    /**\brief Output is to be a the input records, sorted 
     * @param[in] deepcopy Use true if you want a deep copy.
     * @param[in] keeporig Use true if you want to retain the input file.
     * @param[in] carry_obj Use true if you want to carry along the entire
     *                      objects through the scratch files and to the
     *                      output file. Used only for is_for_file().
     *
     * Call this if you want the output file
     * to contain copies of the input file records, undulterated,
     * but in sorted order.
     *
     * Multiple keys are supported.  Use of a CSKF is not needed if
     * the keys are embedded in the records, suitably aligned, and do
     * not cross page boundaries (string comparisons excepted, of course,
     * as string-comparison methods can be called repeatedly on successive
     * corresponding portions of string keys).
     */
    int     set_for_file(bool deepcopy, bool keeporig, bool carry_obj) {
            _for_index = false;
            (void) set_carry_obj(carry_obj);
            (void) set_deep_copy(deepcopy);
            return set_keep_orig(keeporig);
        }

    /**\brief Ensure marshaling and unmarshaling of the objects.
     * @param[in] marshal MOF to be used when reading records from disk.
     * @param[in] unmarshal UMOF to be used to write records to disk,
     * @param[in] c Arguemtn to \a marshal and \a unmarshal
     *
     * Call this if the objects in the file need to be byte-swapped
     * or otherwise marshaled before use, and if they need to be
     * unmarshaled before the output file is written.
     * This may be used with set_for_index or set_for_file.
     */
    int     set_object_marshal( MOF marshal, UMOF unmarshal, key_cookie_t c) {
            _cb_marshal = marshal;
            _cb_unmarshal = unmarshal;
            _cb_marshal_cookie = c;
            return 0;
        }

    /**\brief Return the marshal function or noMOF if none was given */
    MOF     marshal_func() const { return _cb_marshal; }
    /**\brief Return the unmarshal function or noUMOF if none was given */
    UMOF    unmarshal_func() const { return _cb_unmarshal; }

    /**\brief Pointer to datum for marshal and unmarshal functions */
    key_cookie_t    marshal_cookie() const { return _cb_marshal_cookie; }

    /**\brief True if sort will be in ascending order */
    bool    is_ascending() const { return _ascending; } 

    /**\brief Ensure that sort will be in ascending order */
    int     set_ascending( bool value = true) {  _ascending = value; return 0; }

    /**\brief True if duplicate keys and their records will be removed.
     *
     * When duplicates are encountered, they are sorted by record-id,
     * and the larger of the two (per umemcmp)  is removed.
     * */
    bool    is_unique() const { return _remove_dups; } 

    /**\brief Ensure that duplicate keys and their records will be removed 
     * */
    int     set_unique( bool value = true) {  _remove_dups = value; return 0; }

    /**\brief True if duplicate null keys and their records will be removed 
     * 
     * When duplicates are encountered, they are sorted by record-id,
     * and the larger of the two (per umemcmp) is removed.
     * */
    bool    null_unique() const { return _remove_dup_nulls; } 

    /**\brief Ensure that duplicate null keys and their records will be removed
     */
    int     set_null_unique( bool value = true) {  _remove_dup_nulls = value; 
            return 0; }

    /**\brief True if the sort will copy the entire objects through each
     * phase.
     * \details
     * Used when is_for_file only.
     */
    bool    carry_obj() const {  return _carry_obj; }

    /**\brief Control whether the sort will copy the entire objects through each
     * phase.
     * @param[in] value   If true, ensure keep_orig().
     * \details
     * Used when is_for_file only.
     * This is useful if the keys are fixed and consume most of the
     * original objects, in which case there is no need for the
     * sort code to duplicate the key as well as the object 
     * in the temporary output files, or to re-pin the original
     * records to copy them to the output file.
     */
    int     set_carry_obj(bool value = true) {  
        return _carry_obj = value; 
        return 0;
        }

    /**\brief True if the sort will copy the entire objects to the
     * result file.
     */
    bool    deep_copy() const {  return _deep_copy; }
    /**\brief Control whether the sort will copy the entire objects to the
     * result file.
     * @param[in] value   If true, ensure deep_copy().
     * \details
     * Used when is_for_file only.
     * When large objects appear in the input file and the input (original)
     * file is not to be kept, sort can copy only the metadata for the
     * large objects and reassign the large-object store to the result file.
     * This eliminates a lot of object creation and logging.
     */
    int     set_deep_copy(bool value = true ) {  
        _deep_copy = value; 
        return 0; 
        }

    /**\brief Return true if the sort will not destroy the input file.
     * \details
     * Used when is_for_file only.
     * This is turned on automatically when set_for_index.
     */
    bool    keep_orig() const {  return _keep_orig_file; }
    /**\brief Control whether the sort will not destroy the input file.
     * @param[in] value   If true, ensure keep_orig().
     * \details
     * Used when is_for_file only.
     * This is turned on automatically when set_for_index.
     */
    int     set_keep_orig(bool value = true ) {  
        if(value && !_deep_copy) return 1;
        _keep_orig_file = value;
        return 0;
        }

    /**\brief Set attributes of key.
     * @param[in] keyindex The ordinal number of the index whose 
     *                      attributes are to be set
     * @param[in] off Offset from beginning 
     *               of record header or body where the key is to be found
     * @param[in] len Length of key in recordd
     * @param[in] in_header True indicates that the key is 
     *                 to be found in the record header rather than in the body.
     * @param[in] aligned True indicates that the key, as found in the record,
     *                  is suitably aligned for key comparisons with the
     *                  CF (key-comparison function) to be used. False means
     *                  that sort has to make an aligned copy before
     *                  doing a key comparison.
     * @param[in] lexico True indicates that the key, as found in the record,
     *                  is already in \ref LEXICOFORMAT "lexicographic format"
     * @param[in] cfunc Key comparison function to use on this key.
     *
     * \retval 0 if OK, 1 if error.
     * \details
     * You must call this or set_sortkey_derived for each of the keys.
     *
     * There must be implicit agreement between what the \a cfunc
     * expects and the arguments \a aligned and \a lexico.
     */
    int set_sortkey_fixed(
        int keyindex,  // Key number origin 0
        smsize_t off,  // offset from beginning of hdr or body
        smsize_t len,  // length
        bool in_header,// header or body
        bool aligned,  // is aligned
        bool lexico,   // needs lexifying
        CF   cfunc     // comparison func
        );

    /**\brief Set attributes of key.
     * @param[in] keyindex The ordinal number of the index whose 
     *                      attributes are to be set
     * @param[in] gfunc Key-creation (lexify) function.
     * @param[in] cookie Datum for \a gfunc.
     * @param[in] in_header True indicates that the key is 
     *                 to be found in the record header rather than in the body.
     * @param[in] aligned True indicates that the key, as found in the 
     *                  result of \a gfunc,
     *                  is suitably aligned for key comparisons with the
     *                  CF (key-comparison function) to be used. False means
     *                  that sort has to make an aligned copy before
     *                  doing a key comparison.
     * @param[in] lexico True indicates that the key, as found in the result
     *                  of \a gfunc,
     *                  is in \ref LEXICOFORMAT "lexicographic format".
     * @param[in] cfunc Key comparison function to use on this key.
     *
     * \retval 0 if OK, 1 if error.
     * \details
     * You must call this or set_sortkey_fixed for each of the keys.
     *
     * There must be implicit agreement between what the \a cfunc
     * expects and the arguments \a aligned and \a lexico.
     */
    int set_sortkey_derived(
        int keyindex, 
        CSKF gfunc,
        key_cookie_t cookie,
        bool in_header,
        bool aligned, 
        bool lexico, 
        CF   cfunc
        );

    /**\brief Return the key-location information for a given fixed-location key.
     * @param[in] i The ordinal number of the index of interest.
     * \details
     * Only for fixed-location keys.
     */
    key_location_t&  get_location(int i) { return _locs[i]; }

    /**\brief Return the offset for a given fixed-location key.
     * @param[in] i The ordinal number of the index of interest.
     * \details
     * Only for fixed-location keys.
     */
    smsize_t offset(int i) const {
        return _locs[i]._off;
    }
    /**\brief Return the offset for a given fixed-location key.
     * @param[in] i The ordinal number of the index of interest.
     * \details
     * Only for fixed-location keys.
     */
    smsize_t length(int i) const {
        return _locs[i]._length;
    }
    /**\brief Return the CSKF function for a given derived key.
     * @param[in] i The ordinal number of the index of interest.
     * \details
     * Only for derived keys.
     */
    CSKF keycreate(int i) const {
        return _meta[i]._cb_keyinfo;
    }

    /**\brief Return the argument to the CSKF function for a given derived key.
     * @param[in] i The ordinal number of the index of interest.
     * \details
     * Only for derived keys.
     */
    key_cookie_t cookie(int i) const {
        return _meta[i]._cookie;
    }

    /**\brief Return the key-comparison function for a given key.
     * @param[in] i The ordinal number of the index of interest.
     * \details
     * For fixed-location and derived keys.
     */
    CF keycmp(int i) const {
        return _meta[i]._cb_keycmp;
    }

    /// Return true if key \a i is in lexicographic format in the input record
    bool     is_lexico(int i) const {
        return (_meta[i]._mask & t_lexico)?true:false;
    }
    /// Return true if key \a i is in a fixed location in all input records
    bool     is_fixed(int i) const {
        return (_meta[i]._mask & t_fixed)?true:false;
    }
    /// Return true if key \a i is in suitably aligned in the input record for the key-comparison function
    bool     is_aligned(int i) const {
        return (_meta[i]._mask & t_aligned)?true:false;
    }
    /// True if the key or the source of a derived key is to be found in the record header
    bool     in_hdr(int i) const {
        return (_meta[i]._mask & t_hdr)?true:false;
    }
};

} // end namespace ssm_sort

inline void 
ssm_sort::sort_keys_t::_grow(int i) 
{
    // realloc it to accommodate i more entries
    {
    key_location_t* tmp = new key_location_t[_spaces + i];
    if(!tmp) W_FATAL(fcOUTOFMEMORY);
    if(_locs) {
        memcpy(tmp, _locs, nkeys() * sizeof(key_location_t));
        delete[] _locs;
    }
    _locs = tmp;
    }
    {
    key_meta_t* tmp = new key_meta_t[_spaces + i];
    if(!tmp) W_FATAL(fcOUTOFMEMORY);
    if(_meta) {
        memcpy(tmp, _meta, nkeys() * sizeof(key_meta_t));
        delete[] _meta;
    }
    _meta = tmp;
    }
    _spaces += i;
    // don't change nkeys
}
inline NORET 
ssm_sort::sort_keys_t::sort_keys_t(int nkeys):
    _nkeys(0),
    _spaces(0),
    _meta(0),
    _locs(0),
    _stable(false), _for_index(false), 
    _remove_dups(false), _remove_dup_nulls(false),
    _ascending(true), _deep_copy(false), _keep_orig_file(false),
    _carry_obj(false),
    _cb_lexify_index_key(sort_keys_t::noCSKF),
    _cb_lexify_index_key_cookie(0),
    _cb_marshal(sort_keys_t::noMOF),
    _cb_unmarshal(sort_keys_t::noUMOF),
    _cb_marshal_cookie(0)
{ 
    _grow(nkeys);
}

inline void 
ssm_sort::sort_keys_t::_prep(int key) 
{
    if(key >= _spaces) {
    _grow(5);
    }
    if(key >= _nkeys) {
    // NB: hazard if not all of these filled in!
    _nkeys = key+1;
    }
}

inline void 
ssm_sort::sort_keys_t::_copy(const sort_keys_t &old) 
{
    _prep(old.nkeys()-1);
    int i;
    for(i=0; i< old.nkeys(); i++) {
    _locs[i] = old._locs[i];
    _meta[i] = old._meta[i];
    }
    w_assert3(nkeys() == old.nkeys());
    w_assert3(_spaces >= old._spaces);
    for(i=0; i< old.nkeys(); i++) {
    w_assert3(_meta[i]._mask == old._meta[i]._mask);
    }
}

inline NORET 
ssm_sort::sort_keys_t::sort_keys_t(const sort_keys_t &old) : 
    _nkeys(0), _spaces(0), 
    _meta(0), _locs(0) 
{
    _copy(old);
}

inline int 
ssm_sort::sort_keys_t::set_sortkey_fixed(
    int key, 
    smsize_t off, 
    smsize_t len,
    bool in_header,
    bool aligned, 
    bool lexico,
    CF   cfunc
) 
{
    if(is_for_index() && key > 0) {
        return 1;
    }
    _prep(key);
    _set_mask(key,  true, aligned, lexico, 
                in_header, noCSKF, cfunc, key_cookie_t::null);
    _set_loc(key,  off, len);
    return 0;
}

inline int 
ssm_sort::sort_keys_t::set_sortkey_derived(
    int key, 
    CSKF gfunc,
    key_cookie_t cookie,
    bool in_header,
    bool aligned, 
    bool lexico, 
    CF   cfunc
)
{
    if(is_for_index() && key > 0) {
        return 1;
    }
    _prep(key);
    _set_mask(key, false, aligned, lexico, 
        in_header,
        gfunc, cfunc, 
        cookie);
    return 0;
}

/*<std-footer incl-file-exclusion='SORT_S_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
