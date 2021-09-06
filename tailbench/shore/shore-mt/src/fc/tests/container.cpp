/*<std-header orig-src='shore'>

 $Id: container.cpp,v 1.1.2.4 2009/10/30 23:49:02 nhall Exp $

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
#include "atomic_container.h"
#include "w_getopt.h"
#include <iostream>

int tries(0);

typedef unsigned long u_type;

int        parse_args(int argc, char **argv)
{
    int        errors = 0;
    int        c;

    while ((c = getopt(argc, argv, "n:S:")) != EOF) {
        switch (c) {
        case 'n':
                tries = atoi(optarg);
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
                    << " [-n tries]"
                    << endl;
    }

    return errors ? -1 : optind;
}

struct  THING {
// offset 0
        THING *_me;
// offset 8/4
        char  dummy[43]; // odd number
// offset 52/48
        int    _i;
// offset 56/??
        THING * _next;
// offset 64/??
        char  dummy2[10]; // odd number again
// offset 64/??
        char  dummy3;    // not aligned 
        int  dummy4;    // aligned 
public:
        THING(int i) : _i(i) { _me = this; }

        int check(int i)  const {  
                if(i != _i) {
                    cout << "int: expected " << ::dec << i << " found " << _i << endl;
                    return 1;
                }
                if(_me != this) {
                    union {  const THING *p; u_type x; }u; u.p  =  this;
                    union {  const THING *p; u_type x; }m; m.p= _me;

                    cout << "ptr: expected " << ::hex << u.x
                      << " found " << m.x
                      << ::dec
                      << endl;
                    return 1; 

                 }
                 return 0; 
            }
        int check()  const {  
                if(_me != this) {
                    union {  const THING *p; u_type x; }u; u.p  =  this;
                    union {  const THING *p; u_type x; }m; m.p= _me;

                    cout << "ptr: expected " << ::hex << u.x
                      << " found " << m.x
                      << ::dec
                      << endl;

                    return 1; 
                 }
                 return 0; 
        }
        int  i() const { return _i; }
};
atomic_container C(w_offsetof(THING, _next));

int* pushed, *popped;

int push1(int i)
{
    THING *v;
    v = new THING(i);
    w_assert1(v != NULL);
    w_assert1(v->check(i) == 0);
    union {
        void *v;
        u_type u;
    } pun = {v};

cout << " pushing " << ::hex << pun.u << ::dec << endl;
    C.push(v);
    pushed[i]++;
    return 0;
}

int pop1(int i)
{
    THING *v;
    v = (THING *)C.pop();
    union {
        void *v;
        u_type u;
    } pun = {v};

cout << " popping " << ::hex << pun.u << ::dec << endl;
    w_assert1(v);
    if(v->check(i)) {
                delete v;
            return 1;
    }  else {
            popped[v->i()]--;
    }
        delete v;
    return 0;
}
int pop1()
{
    THING *v;
// The atomic container should do a slow pop (i.e., pop from the
// backup list) if need be, but should return NULL if  empty
    v = (THING *)C.pop();
    union {
        void *v;
        u_type u;
    } pun = {v};
cout << " popping " << ::hex << pun.u << ::dec << endl;

    if(!v) return -1;
    if(v->check()) {
                delete v;
       return 1;
    }  else {
                popped[v->i()]--;
    }
        delete v;
    return 0;
}

// push/pop pairs
int test1(int tries)
{
    int e(0);
    for(int i=0; i < tries; i++)
    {
        e += push1(i);
        e += pop1(i);
    }
    cout << "test 1 e=" << e << endl;
    return e;
}

// (push2, /pop1) then pop all
int test2(int tries)
{
    int e(0);

    for(int i=0; i < tries; i++)
    {
        e += push1(i*2);
        e += push1(i*3);
        e += pop1();
    }
    cout << "test 2 e= " << e << endl;
    return e;
}

// pop all.
int  test3(int /*tries*/)
{
    int e(0);
    int f(0);

    // pop1() returns -1 if empty
    // The atomic container should do a slow pop (i.e., pop from the
    // backup list) if need be, but should return NULL if  empty
    while( (f = pop1()) >=0 ) 
    {
            e += f;
    }
    cout << "test 3 e=" << e << endl;
    return e;
}

int 
main(int argc, char *argv[])
{
    int e = parse_args(argc, argv);
    if(e<0) exit(e);

    cout << "OFFSET IS " << C.offset() << endl;
    cout 
        << "w_offsetof(THING, _me) " 
        << w_offsetof(THING, _me)  << endl;
    cout 
        << "w_offsetof(THING, dummy) " 
        << w_offsetof(THING, dummy)  << endl;
    cout 
        << "w_offsetof(THING, _i) " 
        << w_offsetof(THING, _i)  << endl;
    cout 
        << "w_offsetof(THING, _next) " 
        << w_offsetof(THING, _next)  << endl;
    cout 
        << "w_offsetof(THING, dummy2) " 
        << w_offsetof(THING, dummy2)  << endl;
    cout 
        << "w_offsetof(THING, dummy3) " 
        << w_offsetof(THING, dummy3)  << endl;
    cout 
        << "w_offsetof(THING, dummy4) " 
        << w_offsetof(THING, dummy4)  << endl;
    cout << "SIZEOF THING  IS " << sizeof(THING) << endl;

    cout << "tries " << tries << endl;

    pushed = new int[tries*3];
    popped = new int[tries*3];
    for(int i=0; i < tries*3; i++) pushed[i] = popped[i] = 0;
    e = test1(tries);
    e += test2(tries);
    e += test3(tries);

    w_assert1(e==0) ;
    delete[] pushed;
    delete[] popped;
}
