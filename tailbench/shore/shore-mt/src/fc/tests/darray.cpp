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

#include <shore-config.h>
#include <w_base.h>
#include <dynarray.h>
#include <errno.h>
#include <sys/mman.h>
#include <algorithm>
#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio>

// no system I know of *requires* larger pages than this
static size_t const MM_PAGE_SIZE = 8192;
// most systems can't handle bigger than this, and we need a sanity check
static size_t const MM_MAX_CAPACITY = MM_PAGE_SIZE*1024*1024*1024;

#include <unistd.h>
#include <cstdio>

static int foocount = 0;
struct foo {
    int id;
    foo() : id(++foocount) { std::fprintf(stdout, "foo#%d\n", id); }
    ~foo() { std::fprintf(stdout, "~foo#%d\n", id); }
};

template struct dynvector<foo>;

int main() {
    {
	dynarray mm;
	w_base_t::int8_t err;

	// NOTE: the casts to size_t enable us to build
	// on -m32, but this isn't tested there yet.
	err = mm.init(size_t(5l*1024*1024*1024));
	// char const* base = mm;
	// std::fprintf(stdout, "&mm[0] = %p\n", base);
	err = mm.resize(10000);
	err = mm.resize(100000);
	err = mm.resize(1000000);
	err = mm.fini();
    }
    {
	// test alignment
	dynarray mm;
	int err;
	
	err = mm.init(size_t(5l*1024*1024*1024), size_t(1024*1024*1024));
    }

    {
	int err;
	dynvector<foo> dv;
	err = dv.init(100000);
	std::fprintf(stdout, "size:%llu  capacity:%llu  limit:%llu\n", 
	(unsigned long long) dv.size(), (unsigned long long) dv.capacity(), (unsigned long long) dv.limit());
	err = dv.resize(4);
	std::fprintf(stdout, "size:%llu  capacity:%llu  limit:%llu\n", 
	(unsigned long long) dv.size(), (unsigned long long) dv.capacity(), (unsigned long long) dv.limit());
	err = dv.resize(10);
	std::fprintf(stdout, "size:%llu  capacity:%llu  limit:%llu\n", 
	(unsigned long long) dv.size(), (unsigned long long) dv.capacity(), (unsigned long long) dv.limit());
	foo f;
	err = dv.push_back(f);
	err = dv.push_back(f);
	err = dv.push_back(f);
	std::fprintf(stdout, "size:%llu  capacity:%llu  limit:%llu\n", 
	(unsigned long long) dv.size(), (unsigned long long) dv.capacity(), (unsigned long long) dv.limit());
	err = dv.resize(16);
	std::fprintf(stdout, "size:%llu  capacity:%llu  limit:%llu\n", 
	(unsigned long long) dv.size(), (unsigned long long) dv.capacity(), (unsigned long long) dv.limit());
	err = dv.fini();
    }

    return 0;
}
