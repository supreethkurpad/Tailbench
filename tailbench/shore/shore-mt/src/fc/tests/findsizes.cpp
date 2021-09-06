/*<std-header orig-src='shore'>

 $Id: findsizes.cpp,v 1.1.2.2 2009/10/30 23:49:03 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99,2006-09 Computer Sciences Department, University of
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

#include "w.h"
#include "w_findprime.h"
#include "w_getopt.h"
#include <iostream>

bool printprime(false);
bool printpower(false);
bool printrequest(false);
typedef w_base_t::int8_t i8;
i8   base;

int        parse_args(int argc, char **argv)
{
    int        errors = 0;
    int        c;

    while ((c = getopt(argc, argv, "ptr")) != EOF) {
        switch (c) {
        case 'p':
                printprime=true;
                    break;
        case 't':
                printpower=true;
                    break;
        case 'r':
                printrequest=true;
                    break;
        default:
                errors++;
                break;
        }
    }
    if (errors) {
            cerr 
                    << "usage: "
                    << argv[0]
                    << " <0xnnn|0nnn|nnn> [p|t|r] "
                    << endl;
    }
    base = strtoul(argv[optind], 0,0);

    return errors ? -1 : optind;
}

bool debug(false);

int 
main(int argc, char *argv[])
{

    int e = parse_args(argc, argv);
    if(e<0) exit(e);

    // find a power of 2 that would be close to the base
    i8 pwr = 0x400; // start with 1024
    int diff=2<<30;
    for(int i=0; i < 32; i++)
    {
        pwr<<=1;
        if(pwr > base) {
            break;
        } else {
            diff = base - pwr;
        }
    }
    if( (pwr - base) > diff) {
        // use previous pwr
        pwr >>=1;
    } 
    i8 p2 = w_findprime(pwr);

    if(debug) {
        cout 
        << "base  " << base 
        << " closest power of 2 " << pwr 
        << " findprime -> " << p2
        << endl;
    } 
    if(printpower) cout  << pwr << endl;
    if(printprime) cout  << p2 << endl;

    // i8 p1 = w_findprime(base);

    // Work backward from p1 to bpool size to request.
    i8 nbufpages = ((p2 * 9)-8)/16  ;
    if(debug) {
        cout  << "nbufpages =" << nbufpages << endl;
    }

#define page_sz 8192ull

    i8 size = ((page_sz * (nbufpages - 1)) +1)/1024 ;
    if(debug)
    {
        cout  << "request bpool size =" << size << endl;
    } 
    if(printrequest) cout  << size << endl;


    return 0;
}
