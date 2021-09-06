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

/*<std-header orig-src='shore' incl-file-exclusion='BTREE_LATCH_MANAGER_H'>

 $Id: btree_latch_manager.cpp,v 1.2 2010/05/26 01:20:12 nhall Exp $

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

#include "btree_latch_manager.h"
#include <vector>
#include "cpu_info.h"

// This business is to allow us to switch from one kind of
// lock to another with more ease.
#if defined(USE_OCC_LOCK_HERE)
#define OCCWRITE .write_lock()
#define OCCREAD .read_lock()
#else
#define OCCWRITE 
#define OCCREAD 
#endif

void btree_latch_manager::shutdown()
{
    CRITICAL_SECTION(cs, _latch_lock OCCWRITE);
    for(long i=0; i < _socket_count; i++) {
	CRITICAL_SECTION(cs, _socket_latch_locks[i] OCCWRITE);
	_socket_latches[i].clear();
    }
    for(btree_latch_map::iterator it=_latches.begin(); it != _latches.end(); ++it) 
    {
        w_assert1(it->second->mode() == LATCH_NL);
        delete it->second;
    }
    _latches.clear();
}

btree_latch_manager::btree_latch_manager()
    : _socket_count(cpu_info::socket_count())
    , _socket_latches(new btree_latch_map[_socket_count])
    , _socket_latch_locks(new Lock[_socket_count])
{
}

latch_t &btree_latch_manager::find_latch(lpid_t const &root, bool bIgnoreLatches) 
{
    long socket = cpu_info::socket_self();
    latch_t *latch=NULL;
    {
	CRITICAL_SECTION(cs, _socket_latch_locks[socket] OCCREAD);
        btree_latch_map::iterator pos=_socket_latches[socket].find(root);
        if(pos != _socket_latches[socket].end()) {
            lpid_t MAYBE_UNUSED xxx = pos->first;
            w_assert1(root==xxx);
            latch=pos->second;
            return *latch; 
        }
    }
    
    // not there... need to add a new entry (but verify first!)
    CRITICAL_SECTION(cs, _latch_lock OCCWRITE);
    CRITICAL_SECTION(socket_cs, _socket_latch_locks[socket] OCCWRITE);
    if( (latch=_socket_latches[socket][root]) )   {
        return *latch; // somebody else beat us to it
    }

    // not in the master directory either?
    if( ! ( latch=_latches[root]) ) {
	latch = new latch_t;
	_latches[root] = latch;
    }
    _socket_latches[socket][root] = latch;
    return *latch;
}

// do the work -- assumes I already have the _latch_lock in write mode
void btree_latch_manager::_destroy_latches(lpid_t const &root) 
{
    for(long i=0; i < _socket_count; i++) {
	CRITICAL_SECTION(cs, _socket_latch_locks[i] OCCWRITE);
	_socket_latches[i].erase(root);
    }
    btree_latch_map::iterator pos=_latches.find(root);
    if(pos != _latches.end())
    {
        w_assert1(root == pos->first);
        latch_t *latch = pos->second;
        _latches.erase(pos);
        delete latch;
    }
    // not there... someone beat us to it.
}

void btree_latch_manager::destroy_latches(lpid_t const &root) 
{
    CRITICAL_SECTION(cs, _latch_lock OCCWRITE);
    _destroy_latches(root);
}

// This is called when the volume is dismounted
void btree_latch_manager::destroy_latches(stid_t const &store) 
{
    unsigned int i=0;
    std::vector<lpid_t> _tmp;
    _tmp.reserve(30); // probably more than sufficient most of the time

    CRITICAL_SECTION(cs, _latch_lock OCCWRITE);
    // collect all the root page ids for the index
    {
        for(
            btree_latch_map::iterator pos=_latches.begin();
            pos != _latches.end();
            pos++
        )
        {
            lpid_t root = pos->first;
            if(root._stid == store)  { 
                _tmp.push_back(root);
            }
        }
    }
    // Must free the read lock before acquiring the write lock
    // in the destroy_latches() below
    for(i=0; i < _tmp.size(); i++)
    {
        _destroy_latches(_tmp[i]);
    }
}

btree_latch_manager::~btree_latch_manager() 
{
    shutdown();
    delete [] _socket_latches;
    delete [] _socket_latch_locks;
}
