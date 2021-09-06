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

/*<std-header orig-src='shore'>

 $Id: sm_s.cpp,v 1.30 2010/06/08 22:28:56 nhall Exp $

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

#define SM_SOURCE
#ifdef __GNUG__
#pragma implementation "sm_s.h"
#endif

#include <sm_int_0.h>

#include <w_strstream.h>
#include <stdio.h>

const rid_t rid_t::null;
const lpid_t lpid_t::bof;
const lpid_t lpid_t::eof;
const lpid_t lpid_t::null;

// debug pretty-print of an lsn -- used in debugger
char const* 
db_pretty_print(lsn_t const* lsn, int /*i=0*/, char const* /* s=0 */) 
{
  char *tmp = (char *) ::valloc(100);
  snprintf(tmp, sizeof(tmp), "%d.%lld", lsn->hi(), (long long int)(lsn->lo()));
  return tmp;
}

#include <atomic_templates.h>

/* This function makes an atomic copy of the lsn passed to it.

   To prove correctness, we need to show that all possible
   interleavings result in us returning a valid lsn. Note that we
   always access hi before lo.
   There are four possible results: correct (.), conflict (!), or incorrect (x)

   Read:        H        L        H
   ==================================================================
   Write:
    .    HL
    !            HL
    *                    HL
    .                            HL
    .    H        L                
    *            H        L        
    *                    H        L
    .    H                L        
    *            H                L
    x    H                        L
                                
                                
   If we only write each value once there is one corner case that
   sneaks through. So, we need to add a step in the protocol: the
   writer will always set H to a sentinel value before changing L,
   then set H to its new proper value afterward.
   
   Read            H        L        H    
   ==================================================================
   Write:
    .    HLH
    s    HL        H                
    s    HL                H        
    s    HL                        H
    s    H        LH                
    s    H        L        H        
    s    H        L                H
    s    H                LH        
    s    H                L        H
    s    H                        LH
    !            HLH                
    !            HL        H        
    !            HL                H
    !            H        LH        
    !            H        L        H
    !            H                LH
    !                    HLH        
    !                    HL        H
    !                    H        LH
    .                            HLH
                        
 */



bool lpid_t::valid() const
{
#if W_DEBUG_LEVEL > 2
    // try to stomp out uses of this function
    if(_stid.vol.vol && ! page) w_assert3(0);
#endif 
    return _stid.vol.vol != 0;
}

/*********************************************************************
 *
 * key_type_s: used in btree manager interface and stored
 * in store descriptors
 *
 ********************************************************************/

w_rc_t key_type_s::parse_key_type(
    const char*     s,
    uint4_t&        count,
    key_type_s      kc[])
{
    w_istrstream is(s);

    uint j;
	for (j = 0; j < count; j++)  {
		kc[j].variable = 0;
		if (! (is >> kc[j].type))  {
			if (is.eof())  break;
			return RC(smlevel_0::eBADKEYTYPESTR);
		}
		if (is.peek() == '*')  {
			// Variable-length keys look like:
			// ... b*<max>

			if (! (is >> kc[j].variable))
			return RC(smlevel_0::eBADKEYTYPESTR);
		}
		if (! (is >> kc[j].length))
			return RC(smlevel_0::eBADKEYTYPESTR);
		if (kc[j].length > key_type_s::max_len)
			return RC(smlevel_0::eBADKEYTYPESTR);

		switch (kc[j].type)  {

		case key_type_s::I: // signed compressed
		case key_type_s::U: // unsigned compressed
			kc[j].compressed = true;
			// drop down
		case key_type_s::i:
		case key_type_s::u:
			if ( (kc[j].length != 1 
			&& kc[j].length != 2 
			&& kc[j].length != 4 
			&& kc[j].length != 8 )
			|| kc[j].variable)
			return RC(smlevel_0::eBADKEYTYPESTR);
			break;

		case key_type_s::F: // float compressed
			kc[j].compressed = true;
			// drop down
		case key_type_s::f:
			if ((kc[j].length != 4 && kc[j].length != 8) 
										   || kc[j].variable)
			return RC(smlevel_0::eBADKEYTYPESTR);
			break;

		case key_type_s::B: // uninterpreted bytes, compressed
			kc[j].compressed = true;
			// drop down
		case key_type_s::b: // uninterpreted bytes
			w_assert3(kc[j].length > 0);
			break;

		default:
			return RC(smlevel_0::eBADKEYTYPESTR);
			}
	}
    count = j;

    for (j = 0; j < count; j++)  {
		if (kc[j].variable && j != count - 1)
			return RC(smlevel_0::eBADKEYTYPESTR);
	}
    
    return RCOK;
}


w_rc_t key_type_s::get_key_type(
    char*         s,    // out
    int            buflen, // in
    uint4_t         count,  // in
    const key_type_s*    kc)   // in
{
    w_ostrstream o(s, buflen);

    uint j;
    char c;
    for (j = 0; j < count; j++)  {
    if(o.fail()) break;

    switch (kc[j].type)  {
    case key_type_s::I: // compressed int
        c =  'I';
        break;
    case key_type_s::i:
        c =  'i';
        break;
    case key_type_s::U: // compressed unsigned int
        c = 'U';
        break;
    case key_type_s::u:
        c = 'u';
        break;
    case key_type_s::F: // compressed float
        c =  'F';
        break;
    case key_type_s::f:
        c =  'f';
        break;
    case key_type_s::B: // uninterpreted bytes, compressed
        c =  'B';
        break;
    case key_type_s::b: // uninterpreted bytes
        c =  'b';
        break;
    default:
        return RC(smlevel_0::eBADKEYTYPESTR);
        }
    o << c;
    if(kc[j].variable) o << '*';
    o << kc[j].length;
    }
    o << ends;
    if(o.fail()) return RC(smlevel_0::eRECWONTFIT);
    return RCOK;
}

