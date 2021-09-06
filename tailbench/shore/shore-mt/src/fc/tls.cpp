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

// -*- mode:c++; c-basic-offset:4 -*-
#include "tls.h"
using namespace tls_tricks;

#include <vector>
#include <utility>

#define STR(x) STR2(x)
#define STR2(x) #x

#define ATTEMPT(action) do {                    \
    int err = action;                    \
    if(err != 0) {                        \
        std::printf("%s failed with error %d\n", #action, err);    \
        std::exit(-1);                        \
    }                            \
    } while(0)

typedef std::vector<std::pair<void(*)(), void(*)()> > tls_function_list;

static tls_function_list* registered_tls(NULL);
static bool tls_manager_initialized = false;
  
void tls_manager::global_init() {
    if(tls_manager_initialized)
         return;
    tls_manager_initialized = true;
    registered_tls = new tls_function_list;
}
void tls_manager::global_fini() {
    if(tls_manager_initialized) {
       delete registered_tls;
       registered_tls = NULL;
       tls_manager_initialized = false;
  }
}

void tls_manager::register_tls(void (*init)(), void (*fini)()) {
  /* init the tls for the current (main) thread, then add it to the
     pile for others which follow
   */
  (*init)();
  registered_tls->push_back(std::make_pair(init, fini));
}
/**\var static __thread bool tls_manager::_thread_initialized;
 * \ingroup TLS
 */
__thread bool tls_manager::_thread_initialized(false);

// called from sthread.cpp just inside sthread_t::_start()
void tls_manager::thread_init() {
  if(_thread_initialized)
    return;
  _thread_initialized = true;
  for(tls_function_list::iterator it=registered_tls->begin(); it != registered_tls->end(); ++it)
    (*it->first)();
}

// called from sthread.cpp at end of sthread_t::_start()
void tls_manager::thread_fini() {
  if(!_thread_initialized)
    return;
  _thread_initialized = false;
  for(tls_function_list::iterator it=registered_tls->begin(); it != registered_tls->end(); ++it)
    (*it->second)();
}
