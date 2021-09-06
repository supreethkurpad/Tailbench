/*<std-header orig-src='shore'>

 $Id: bitvector.cpp,v 1.4 2010/06/15 17:24:29 nhall Exp $

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

#include <w_stream.h>
#include <cstddef>
#include <w.h>
#include <w_bitvector.h>


int main()
{
	w_bitvector_t<256> v, w, tmp;

	w_assert1(v.is_empty());
	w_assert1(w.is_empty());

	// Word 0 : 0->63
	v.set_bit(0);
	w_assert1(v.is_set(0));
	w_assert1(v.is_empty()==false);
	w_assert1(v.num_bits_set()==1);

	v.set_bit(1);
	w_assert1(v.is_set(1));
	w_assert1(v.num_bits_set()==2);

	v.set_bit(35);
	w_assert1(v.is_set(35));
	w_assert1(v.num_bits_set()==3);
	v.clear_bit(35);
	w_assert1(v.is_set(35)==false);
	w_assert1(v.num_bits_set()==2);
	v.set_bit(35);
	w_assert1(v.is_set(35));
	w_assert1(v.num_bits_set()==3);

	// Word 1 : 64->127
	v.set_bit(72);
	w_assert1(v.is_set(72));
	w_assert1(v.num_bits_set()==4);

	// Word 2 : 128 -> 191
	v.set_bit(172);
	w_assert1(v.is_set(172));
	w_assert1(v.num_bits_set()==5);

	// Word 3 : 192 -> 255
	v.set_bit(200);
	w_assert1(v.is_set(200));
	w_assert1(v.num_bits_set()==6);

	v.set_bit(255);
	w_assert1(v.is_set(255));
	w_assert1(v.num_bits_set()==7);

	w_assert1(v.is_empty()==false);

    int i;
    for (i = 0; i < v.num_bits(); i++)  {
		w.set_bit(i);
		w_assert1(w.num_bits_set()==i+1);
    }
	w_assert1(w.is_empty()==false);
	w_assert1(w.is_full()==true);
#if defined(ARCH_LP64)
	w_assert1(w.num_words()==4);
#else
	w_assert1(w.num_words()==8);
#endif

	w.copy(v);
	w_assert1(w.num_bits_set()==7);
	w_assert1(w.is_empty()==false);
	w_assert1(w.is_full()==false);
    for (i = 0; i < v.num_words(); i++)  {
		w_assert1(w.get_bit(i)==v.get_bit(i));
	}

	w.clear();
	w_assert1(w.is_empty()==true);
	w_assert1(w.is_full()==false);
    for (i = 0; i < v.num_words(); i++)  {
		w_assert1(w.get_bit(i)==0);
	}

	w.set_bit(0);
	int n=w.words_overlap(tmp, v);
#if defined(ARCH_LP64)
	w_assert1(n==4); // bit 0 is in both
#else
	w_assert1(n==8); // bit 0 is in both
#endif

	w.set_bit(5);
	n=w.words_overlap(tmp, v);

#if defined(ARCH_LP64)
	w_assert1(n==3); // bit 5 is not in both
#else
	w_assert1(n==7); // bit 5 is not in both
#endif
	w.clear_bit(5);
	n=w.words_overlap(tmp, v);

#if defined(ARCH_LP64)
	w_assert1(n==4); // back to former state
#else
	w_assert1(n==8); // back to former state
#endif

	w.set_bit(200);
	n=w.words_overlap(tmp, v);
#if defined(ARCH_LP64)
	w_assert1(n==4); 
#else
	w_assert1(n==8); 
#endif

	w.set_bit(255);
	n=w.words_overlap(tmp, v);
#if defined(ARCH_LP64)
	w_assert1(n==4); 
#else
	w_assert1(n==8); 
#endif

	w.set_bit(72);
	n=w.words_overlap(tmp, v);
#if defined(ARCH_LP64)
	w_assert1(n==4); 
#else
	w_assert1(n==8); 
#endif

	// Is all of w found in v?
	w_assert1(w.overlap(tmp,v) == true);
	// Is all of v found in w?
	w_assert1(v.overlap(tmp,w) == false);

	w.set_bit(172);
	w_assert1(w.overlap(tmp,v) == true);
	w_assert1(v.overlap(tmp,w) == false);
	w.set_bit(35);
	w_assert1(w.overlap(tmp,v) == true);
	w_assert1(v.overlap(tmp,w) == false);

	w.set_bit(1);
	n=w.words_overlap(tmp, v);
	w_assert1(w.overlap(tmp,v) == true);
	w_assert1(v.overlap(tmp,w) == true);

    return 0;
}

