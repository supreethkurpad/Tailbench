/*<std-header orig-src='shore'>

 $Id: mmap.cpp,v 1.1.2.3 2010/01/28 04:53:42 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository


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

#include <w.h>
#include <sthread.h>

// Returns NULL in case of error
char * trymap(
    char *buf,
    size_t size
) 
{
#ifdef HAVE_HUGETLBFS
    w_rc_t	e;
    e = sthread_t::set_hugetlbfs_path(HUGETLBFS_PATH);
    if (e.is_error()) W_COERCE(e);
#endif
    w_rc_t rc = sthread_t::set_bufsize(size, buf, false);
    if(rc.is_error()) {
        cerr << "Test failed with size " << size 
        << " : rc= "  << rc
        << endl;
        w_assert1(!rc.is_error());
        return NULL;
    }
    return buf;
}

void test_write(char *b, size_t s)
{
     // Make sure we can write each address
     // Since it's SM_PAGESIZE-aligned, we should be able
     // to write as integers
     int numpages = s/SM_PAGESIZE;
     int intsperpage = SM_PAGESIZE/sizeof(int);
     for (int p=0; p< numpages; p++)
     {
	 for (int i=0; i< intsperpage; i++)
	 {
	 	int  off = p*SM_PAGESIZE + i*sizeof(int);
		int *target = (int *)(b+off);
		*target = 0;
	 }
     }
     cerr << "test_write size= " << s << " success" << endl;
}

int test(size_t input_size)
{

    cout << "input size " << int(input_size) << endl;
    char *b1 = trymap(0, input_size);
    if(b1) { 
        // success
        test_write(b1, input_size);
        int err = sthread_t::do_unmap();
        w_assert0(!err);
        return 0;
    }
    else
    {
      // failure
      return 1;
    }
}


#define num 7
int main()
{
    static size_t sizes[num] =  
    {
#define MB 1024*1024
    256*MB,
    512*MB,
    11*MB,
    1*MB,
        // 1000*MB, probably too big
        700*MB,
        1,
    25
    };

    int e=0;

    for(int i=0; i < num; i++)
    {
       cout << "-------------------------------------" << sizes[i] << endl;
       e += test(sizes[i]);
    }
    return e;
}

