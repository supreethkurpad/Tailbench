/*<std-header orig-src='shore' incl-file-exclusion='BASICS_H'>

 $Id: basics.h,v 1.71 2010/05/26 01:20:11 nhall Exp $

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

#ifndef BASICS_H
#define BASICS_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#ifndef W_BASE_H
#include <w_base.h>
#endif

typedef w_base_t::int1_t      int1_t;
typedef w_base_t::int2_t      int2_t;
typedef w_base_t::int4_t      int4_t;
typedef w_base_t::uint1_t     uint1_t;
typedef w_base_t::uint2_t     uint2_t;
typedef w_base_t::uint4_t     uint4_t;

/* sizes-in-bytes for all persistent data in the SM. */
typedef uint4_t               smsize_t;

/* For types of store, volumes, see stid_t.h and vid_t.h */

typedef w_base_t::uint4_t    shpid_t; 

#ifndef SM_SOURCE
// This is for servers. SM_SOURCE is defined in the SM sources.
namespace locking {
	typedef w_base_t::lock_duration_t  lock_duration_t; 
	typedef w_base_t::lock_mode_t  lock_mode_t; 
	static const lock_mode_t NL = w_base_t::NL;
	static const lock_mode_t IS = w_base_t::IS;
	static const lock_mode_t IX = w_base_t::IX;
	static const lock_mode_t SH = w_base_t::SH;
	static const lock_mode_t SIX = w_base_t::SIX;
	static const lock_mode_t UD = w_base_t::UD;
	static const lock_mode_t EX = w_base_t::EX;
};
using namespace locking;

namespace two_phase_commit {
	typedef w_base_t::vote_t  vote_t; 
};
using namespace two_phase_commit;
#endif


/* Type of a record# on a page  in SM (sans page,store,volume info) */
typedef w_base_t::int2_t slotid_t;  

/* XXX duplicates w_base types. */
const int2_t    max_int2 = 0x7fff;         /*  (1 << 15) - 1;     */
const int2_t    min_int2 = (short)0x8000;     /* -(1 << 15);        */
const int4_t    max_int4 = 0x7fffffff;         /*  (1 << 31) - 1;  */
const int4_t    max_int4_minus1 = max_int4 -1;
const int4_t    min_int4 = 0x80000000;         /* -(1 << 31);        */

const uint2_t    max_uint2 = 0xffff;
const uint2_t    min_uint2 = 0;
const uint4_t    max_uint4 = 0xffffffff;
const uint4_t    min_uint4 = 0;



/*
 * Safe Integer conversion (ie. casting) function
 */
inline int u4i(uint4_t x) {w_assert1(x<=(unsigned)max_int4); return int(x); }

// inline unsigned int  uToi(int4_t x) {assert(x>=0); return (uint) x; }



inline bool is_aligned(smsize_t sz)
{
    return w_base_t::is_aligned(sz);
}

inline bool is_aligned(const void* p)
{
    return w_base_t::is_aligned(p);
}

/*<std-footer incl-file-exclusion='BASICS_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
