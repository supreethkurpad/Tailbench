/*<std-header orig-src='shore'>

 $Id: findprime.cpp,v 1.1.2.2 2009/10/30 23:49:03 nhall Exp $

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

union pun
{
    long i;
    unsigned long ui;
} u;


int        parse_args(int argc, char **argv)
{
    int        errors = 0;
    int        c;

    while ((c = getopt(argc, argv, "d:D:o:O:x:X:i:I:n:N:")) != EOF) {
        switch (c) {
        case 'O':
        case 'o':
        case 'X':
        case 'x':
        case 'D':
        case 'd':
        case 'N':
        case 'n':
        case 'I':
        case 'i':
                u.ui = strtoul(optarg, 0,0);
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
                    << " -[Oo] <octal number>  |"
                    << " -[Xx] <hex number>  |"
                    << " -[IiDdNn] <decimal number>"
                    << endl;
    }

    return errors ? -1 : optind;
}

int 
main(int argc, char *argv[])
{

    int e = parse_args(argc, argv);
    if(e<0) exit(e);

    w_base_t::int8_t P = w_findprime(u.ui);

    cout 
        << P << endl;
    return 0;
}
