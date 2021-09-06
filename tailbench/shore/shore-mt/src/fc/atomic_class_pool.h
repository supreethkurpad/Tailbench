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

#ifndef __ATOMIC_TRASH_STACK
#define __ATOMIC_TRASH_STACK

#include <atomic_templates.h>

// for placement new support, which users need
#include <new>
#include <cassert>
#include <stdlib.h>
#include "atomic_container.h"


/** \brief A thread-safe memory pool based on the atomic container, used by
 * atomic_class_pool.
 *
 * Creates a new atomic_container with \e seed pre-allocated
 * untyped items of size \e nbytes each.
 * It is important to have a non-zero seed value so that 
 * atomic_container versioning works correctly.
 *
 * Maintains a global freelist of fixed-sized memory chunks to recycle
 * and provides a drop-in replacement for malloc() and free()
 *
 */
struct atomic_preallocated_pool : protected atomic_container 
{
    atomic_preallocated_pool(uint nbytes, long seed=128)
        : atomic_container(-sizeof(ptr)), _nbytes(nbytes+sizeof(ptr))
    {
        // start with a non-empty pool so threads don't race at the beginning
        ptr* head = NULL;
        for(int i=0; i < seed; i++) {
            vpn u = {alloc()};
            u.p->next = head;
            head = u.p;
        }
        for(int i=0; i < seed; i++) {
            ptr* p = head;
            head = head->next;
            dealloc(p);
        }
    }
    void* alloc() {
        void* val = pop();
        if(val) return val;
        
        vpn u = { malloc(_nbytes) };
        if(!u.v) u.v = null();
        return prepare(u);
    }
    void dealloc(void* val) { push(val); }
    
    ~atomic_preallocated_pool() {
        vpn val;
        while( (val.v=pop()) ) {
            val.n += _offset; // back up to the real start of the pointer
            free(val.v);
        }
    }
    
    uint const _nbytes;
};

// forward decls...
template<class T>
struct atomic_class_pool;
template<class T>
void* operator new(size_t nbytes, atomic_class_pool<T>& pool);
template<class T>
inline void operator delete(void* ptr, atomic_class_pool<T>& pool);

/** \brief A thread-safe memory pool for typed objects, based on atomic_preallocated_pool.
 *
 * Provides a replacement for new/delete on the specific class. Note
 * that there's actually no way to prevent the user from allocating
 * whatever they want, but they will be unable to destroy anything but
 * the specified class (and its subclasses).
 *
 * \code
 * Example:
 *
 *        class foo { };
 *        atomic_class_pool<foo> pool;
 *        foo* f = new(pool) foo;
 *        pool.destroy(f);
 * \endcode
 */
template<class T>
struct atomic_class_pool : protected atomic_preallocated_pool {

    /** \brief Create a pool for class T.
     *
     * By default the pool will hand out sizeof(T) bytes at a time; if
     * T is a base class and this pool is to be used with subclasses,
     * nbytes must be set at least as large as the largest
     * class. Oversized allocations will assert().
     */
    atomic_class_pool(long nbytes=sizeof(T), long seed=128)
        : atomic_preallocated_pool(nbytes, seed)
    {
    }

    /** \brief Destroys an object (by calling its destructor) and returns its
     * memory to the pool.
     *
     * Undefined behavior results if the object did not come from this
     * pool.
     */
    void destroy(T* tptr) {
        // avoid pointer aliasing problems with the optimizer
        union { T* t; void* v; } u = {tptr};

        // destruct the object and deallocate its memory
        u.t->~T();
        dealloc(u.v);
    }

    /** \brief Return the object size given to the constructor.
     */
    uint nbytes() { return _nbytes; }
    
    // these guys need to access the underlying preallocated stack
    friend void* operator new<>(size_t, atomic_class_pool<T> &);
    friend void operator delete<>(void*, atomic_class_pool<T> &);
};

/** \brief WARNING: When finished, call pool.destroy(t) instead of delete.
 *
 * NOTE: use placement-style new with the pool. 
 * \code 
 * usage: T* t = new(pool) T(...)
 * \endcode
 */

template<class T>
inline void* operator new(size_t nbytes, atomic_class_pool<T>& pool) {
    assert(pool.nbytes() >= nbytes);
    return pool.alloc();
}

/** Called automatically by the compiler if T's constructor throws
 * (otherwise memory would leak).
 *
 *
 * Unfortunately, there is no "delete(pool)" syntax in C++ so the user
 * must still call pool.destroy()
 */
template<class T>
inline void operator delete(void* ptr, atomic_class_pool<T>& pool) {
    pool.dealloc(ptr);
}

#endif
