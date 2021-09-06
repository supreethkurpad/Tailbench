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

/* This file is home to all the polling functions we *do not* want inlined */
#include "w_defines.h"
#include <w.h>
#include "atomic_templates.h"

#include "mcs_lock.h"
#include "sthread.h"
#include "srwlock.h"

void mcs_lock::spin_on_waiting(qnode* me) {
    while(me->vthis()->_waiting) ;
}

mcs_lock::qnode* mcs_lock::spin_on_next(qnode* me) {
    qnode* next;
    while(!(next=me->vthis()->_next)) ;
    return next;
}


void mcs_rwlock::_spin_on_writer() 
{
    while(has_writer()) ;
    // callers do membar_enter
}

void mcs_rwlock::_spin_on_readers() 
{
    while(has_reader()) ;
    // callers do membar_enter
}

void occ_rwlock::acquire_read()
{
    int count = atomic_add_32_nv(&_active_count, READER);
    while(count & WRITER) {
        // block
        count = atomic_add_32_nv(&_active_count, -READER);
        {
            CRITICAL_SECTION(cs, _read_write_mutex);
            
            // nasty race: we could have fooled a writer into sleeping...
            if(count == WRITER)
                DO_PTHREAD(pthread_cond_signal(&_write_cond));
            
            while(*&_active_count & WRITER) {
                DO_PTHREAD(pthread_cond_wait(&_read_cond, &_read_write_mutex));
            }
        }
        count = atomic_add_32_nv(&_active_count, READER);
    }
    membar_enter();
}

void occ_rwlock::acquire_write()
{
    // only one writer allowed in at a time...
    CRITICAL_SECTION(cs, _read_write_mutex);    
    while(*&_active_count & WRITER) {
        DO_PTHREAD(pthread_cond_wait(&_read_cond, &_read_write_mutex));
    }
    
    // any lurking writers are waiting on the cond var
    int count = atomic_add_32_nv(&_active_count, WRITER);
    w_assert1(count & WRITER);

    // drain readers
    while(count != WRITER) {
        DO_PTHREAD(pthread_cond_wait(&_write_cond, &_read_write_mutex));
        count = *&_active_count;
    }
}

void tatas_lock::spin() { while(*&(_holder.handle)) ; }
