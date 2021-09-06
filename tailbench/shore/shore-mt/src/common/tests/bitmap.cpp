/*<std-header orig-src='shore'>

 $Id: bitmap.cpp,v 1.11.2.3 2009/10/30 23:50:52 nhall Exp $

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

#include <w.h>
#include <w_base.h>
#include <w_getopt.h>
#include <basics.h>
#include <bitmap.h>

#include <iostream>

int main(int argc, char **argv)
{
    int    numBits = 71;
    int    numBytes;
    int    i;
    int    errors = 0;
    int    iterations = 0;
    bool    random_tests = false;
    bool    stateless_tests = false;
    int    c;

    while ((c = getopt(argc, argv, "n:ri:")) != EOF) {
        switch (c) {
        case 'n':
            /* number of bits */
            numBits = atoi(optarg);
            break;
        case 'r':
            /* tests using random bit numbers;
               otherwise tests test all valid
               bit numbers in order */
            random_tests = true;
            break;
        case 'i':
            /* iterations for each test */
            iterations = atoi(optarg);
            break;
        default:
            errors++;
            break;
        }
    }
    if (errors) {
        W_FORM2(cerr,("usage: %s [-n bits] [-r] [-i iterations]\n",
                 argv[0]));
        return 1;
    }
    if (!iterations)
        iterations = random_tests ? 10000 : numBits;

    // tests must leave no state if probes are random, or will
    // repeat through the sample space
    stateless_tests = (random_tests || iterations > numBits);

    numBytes = (numBits - 1) / 8 + 1;

    u_char    *map = new u_char[numBytes];
    if (!map)
        W_FATAL(fcOUTOFMEMORY);

    W_FORM2(cout,("test bitmap of %d bits, stored in %d bytes (%d bits).\n",
             numBits, numBytes, numBytes*8));
    
    for (i = 0; i < numBytes; i++)
        map[i] = rand();

    cout << "Clear Map:    ";
    errors = 0;
    bm_zero(map, numBits);
    for (i = 0; i < numBits; i++)  {
        if (bm_is_set(map, i))  {
            cout << i << " ";
            errors++;
            break;
        }
    }
    cout << (errors ? "failed" : "passed") << endl << flush;
    
    cout << "Set Map:      ";
    errors = 0;
    bm_fill(map, numBits);
    for (i = 0; i < numBits; i++)  {
        if ( ! bm_is_set(map, i))  {
            cout << i << " ";
            errors++;
            break;
        }
    }
    cout << (errors ? "failed" : "passed") << endl << flush;
    
    cout << "Set:          ";
    errors = 0;
    bm_zero(map, numBits);
    for (i = 0; i < iterations; i++)  {
        int    bit = (random_tests ? rand() : i) % numBits;
        bm_set(map, bit);
        if ( ! bm_is_set(map, bit) )  {
            cout << bit << " ";
            errors++;
            break;
        }
    }
    cout << (errors ? "failed" : "passed") << endl << flush;
    
    cout << "Clear:        ";
    errors = 0;
    bm_fill(map, numBits);
    for (i = 0; i < iterations; i++)  {
        int    bit = (random_tests ? rand() : i) % numBits;
        bm_clr(map, bit);
        if (bm_is_set(map, bit))  {
            cout << bit << " ";
            errors++;
            break;
        }
    }
    cout << (errors ? "failed" : "passed") << endl << flush;
    
    cout << "First Set:    ";
    errors = 0;
    bm_zero(map, numBits);
    for (i = 0; i < iterations; i++)  {
        int    bit = (random_tests ? rand() : i) % numBits;
        bm_set(map, bit);
        
        if (bm_first_set(map, numBits, 0) != bit)  {
            cout << bit << " ";
            errors++;
            break;
        }
        
        if (bm_first_set(map, numBits, bit) != bit)  {
            cout << bit << " ";
            errors++;
            break;
        }

        bm_clr(map, bit);
    }
    cout << (errors ? "failed" : "passed") << endl << flush;
    
    cout << "First Clear:  ";
    errors = 0;
    bm_fill(map, numBits);
    for (i = 0; i < iterations; i++)  {
        int    bit = (random_tests ? rand() : i) % numBits;
        bm_clr(map, bit);
        if (bm_first_clr(map, numBits, 0) != bit)  {
            cout << bit << " ";
            errors++;
            break;
        }
        
        if (bm_first_clr(map, numBits, bit) != bit)  {
            cout << bit << " ";
            errors++;
            break;
        }
        bm_set(map, bit);
    }
    cout << (errors ? "failed" : "passed") << endl << flush;

    cout << "Last Set:     ";
    errors = 0;
    bm_zero(map, numBits);
    for (i = 0; i < iterations; i++)  {
        int    bit = (random_tests ? rand() : i) % numBits;
        bm_set(map, bit);
        
        if (bm_last_set(map, numBits, numBits-1) != bit)  {
            cout << bit << " ";
            errors++;
            break;
        }
        
        if (bm_last_set(map, numBits, bit) != bit)  {
            cout << bit << " ";
            errors++;
            break;
        }
        if (stateless_tests)
            bm_clr(map, bit);
    }
    cout << (errors ? "failed" : "passed") << endl << flush;
    
    cout << "Last Clear:   ";
    errors = 0;
    bm_fill(map, numBits);
    for (i = 0; i < iterations; i++)  {
        int    bit = (random_tests ? rand() : i) % numBits;
        bm_clr(map, bit);
        if (bm_last_clr(map, numBits, numBits-1) != bit)  {
            cout << bit << " ";
            errors++;
            break;
        }
        
        if (bm_last_clr(map, numBits, bit) != bit)  {
            cout << bit << " ";
            errors++;
            break;
        }
        if (stateless_tests)
            bm_set(map, bit);
    }
    cout << (errors ? "failed" : "passed") << endl << flush;

    cout << "Num Set:      ";
    errors = 0;
    bm_zero(map, numBits);
    if (bm_num_set(map, numBits) != 0) {
        cout << "all ";
        errors++;
    }
    for (i = 0; i < numBits; i++)  {
        bm_set(map, i);
        
        if (bm_num_set(map, numBits) != i+1)  {
            cout << i << " ";
            errors++;
            break;
        }
    }
    cout << (errors ? "failed" : "passed") << endl << flush;

    cout << "Num Clear:    ";
    errors = 0;
    bm_fill(map, numBits);
    if (bm_num_clr(map, numBits) != 0) {
        cout << "all ";
        errors++;
    }
    for (i = 0; i < numBits; i++)  {
        bm_clr(map, i);
        
        if (bm_num_clr(map, numBits) != i+1)  {
            cout << i << " ";
            errors++;
            break;
        }
    }
    cout << (errors ? "failed" : "passed") << endl << flush;
    
    delete [] map;
    
    cout << flush;
    return 0;
}

