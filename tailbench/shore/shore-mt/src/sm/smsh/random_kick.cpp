/*<std-header orig-src='shore'>

 $Id: random_kick.cpp,v 1.11.2.5 2010/03/19 22:20:30 nhall Exp $

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

#include <os_types.h>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <signal.h>


#if 0
// declared in stdlib.h
extern "C" {
    long random();
    void srandom(unsigned int);
    char *initstate(unsigned, char*, size_t);
    char *setstate(char *);
};
#endif

unsigned int rseed=1;
unsigned char rstate[32]= {
    0x76, 0x4, 0x24, 0x2c,
    0x03, 0xab, 0x38, 0xd0,
    0xab, 0xed, 0xf1, 0x23,
    0x03, 0x00, 0x08, 0xd0,
    0x76, 0x40, 0x24, 0x2c,
    0x03, 0xab, 0x38, 0xd0,
    0xab, 0xed, 0xf1, 0x23,
    0x01, 0x00, 0x38, 0xd0
};

int
dorandom(long mod) 
{
    long res = random();

    if(mod==0) {
    (void) setstate((char *)rstate);
    srandom(rseed);
    } else if(mod>0) {
    res %= mod;
    }
    return (int) res;
}

extern "C" void
die(
    int 
)
{
    cout << flush;
    exit(158);
}

int
main(
    int ac,
    char *av[]
) 
{
    if (ac != 6) {
    cerr << "Usage: "
    << av[0] 
    << " <sig> <pid> <max0> <max1> <max2> " 
    << endl;
    return 1;
    }

    signal(SIGUSR1, SIG_IGN);
    signal(SIGUSR2, die);

    int pid, sig;
    int max0 = atoi(av[3]);
    int max1 = atoi(av[4]);
    int max2 = atoi(av[5]);
    sig = atoi(av[1]);
    pid = atoi(av[2]);

    int category=0, t=0;
    while(1) {
    pid_t pp = getppid();
    if(pp == 1) { 
        cerr << "random_kick: parent went away -- exiting " << endl;
        return 1;
    }
      
    category = dorandom(3);

    switch(category) {
    case 0:
         t=dorandom(max0);
         break;
    case 1:
         t=dorandom(max1);
         break;
    case 2:
         t=dorandom(max2);
         break;
    }
    cout << "random_kick: pid " << getpid() << " sleep... " << t << endl;
    if(t>0) {
        sleep(t);
    }
    cout << "random_kick: kill " << pid << "," << sig << endl;
    kill(pid, sig);
    }
    return 0;
}

