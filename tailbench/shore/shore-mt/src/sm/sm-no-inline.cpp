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

#define SM_SOURCE
#define LOG_CORE_C
#include "sm_int_1.h"
#include "log.h"
#include "log_core.h"

void log_core::_spin_on_epoch(long old_end) {
    while(*&_cur_epoch.vthis()->end + *&_cur_epoch.vthis()->base != old_end) ;
}    

long log_core::_spin_on_count(long volatile* count, long bound) {
    long old_count;
    while( (old_count=*count) >= bound) ;
    membar_enter();
    return old_count;
}

