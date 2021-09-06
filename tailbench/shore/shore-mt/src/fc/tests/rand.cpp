/*<std-header orig-src='shore'>

 $Id: rand.cpp,v 1.1.2.4 2009/10/30 23:49:04 nhall Exp $

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

#include "w.h"
#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include "rand48.h"

// WORDS_BIGENDIAN is determined by autoconf -- see configure.ac
typedef struct
{
#ifdef WORDS_BIGENDIAN
// big-endian: vax, sparc
    unsigned int sign:1;
    unsigned int exponent:11;
    unsigned int mant1:20;
    unsigned int mant2:32;
#else
// We don't deal with little-endian machines whose float order is big-endian
// little-endian : intel, powerpc, etc

    unsigned int mant2:32;
    unsigned int mant1:20;
    unsigned int exponent:11;
    unsigned int sign:1;
#endif
} w_ieee_double;  

typedef struct
{
    unsigned int sign:1;
    unsigned int exponent:11;
    unsigned int mant1:20;
    unsigned int mant2:32;
} w_ieee_double_big;  
typedef struct
{
    unsigned int mant2:32;
    unsigned int mant1:20;
    unsigned int exponent:11;
    unsigned int sign:1;
} w_ieee_double_little;  

typedef unsigned char UC;
typedef UC ary[8];


typedef union {
    double  d;
    signed48_t s;
    long long l;
    w_ieee_double ieee;
    w_ieee_double_big    big;
    w_ieee_double_little lit;
    ary c;
}PUN;
static void p(const char *str,  const PUN &u);
static void p(const char *str,  const unsigned48_t &u);
static void p(const char *str,  const signed48_t &u);
static void p(const char *str,  const double &u);

struct  randorig : rand48 {
   randorig()  { seed(RAND48_INITIAL_SEED); }
   randorig(const rand48 &other) { _state = other._state; }
   bool operator == (const randorig & other)const 
                {  return _state == other._state; }
   void dump( const char *str, bool debug)const ;
};

void randorig::dump( const char *str, bool debug) const 
{
    union {
        double  d;
        long long l;
        w_ieee_double ieee;
    } u;
    u.l = _state;

    cout << str 
         << " hex: " << ::hex << u.l << ::dec;

    if(debug)
    {
        cout 
             << " double: " << setprecision(21) << u.d 
         << " sign: " << ::hex << u.ieee.sign << ::dec
         << " exp: " << ::hex << u.ieee.exponent << ::dec
         << " mant1: " << ::hex << u.ieee.mant1 << ::dec
         << " mant2: " << ::hex << u.ieee.mant2 << ::dec;
    }
    cout << endl;


    long long tmp = _state;
    const unsigned long long mplier = 0x5deece66dull;
    tmp *= mplier;
    cout << " _update preview: "
         << " state * 0x5deece66dull: " << ::hex << tmp << ::dec
         ;

    tmp += 0xb;
    cout 
         << " state + 0xb: " << ::hex << tmp << ::dec
         ;
        tmp = _mask(tmp);
    cout 
         << " _mask(state): " << ::hex << tmp << ::dec
         ;
    cout 
        << endl;
}

// randalt: basically the same as the library rand class,
// but this gives us a change to override the generators for
// testing purposes
struct  randalt : randorig {
        randalt() { }
        randalt(const rand48 &other) : randorig(other) { }
    double drand() ;
};

double randalt::drand() 
{
    union {
       double        d;
       w_ieee_double ieee;
    } u;

if(0) {
    
    unsigned long long save_state = _state;

    unsigned48_t tmp = _update();
    cout << endl;

    u.ieee.sign = 0x0;
    u.ieee.exponent = 0x0;
    u.ieee.mant1 = 0x0;
    u.ieee.mant2 = 0x0;

    p("ALT clear 0x0", u.d);
    u.ieee.exponent = 0x3ff;
    p("ALT set exponent 0x3ff", u.d);
    cout << endl;
    p("ALT _update()", tmp);

    tmp <<= 4;
    p("ALT _update()<<4", tmp);
    cout << endl;

    unsigned48_t tmp3 = tmp & 0xfffff00000000ull;
    p("ALT high part of _update()<<4", tmp3);

    tmp3 >>= 32;
    p("ALT high part shifted right", tmp3);

    u.ieee.mant1 = (tmp & 0xfffff00000000ull)>>32;
    p("ALT set mant1 to high part of _update()<<4", u.d);

    tmp3 = tmp & 0x0000ffffffffull;
    p("ALT low part of _update()<<4", tmp3);

    u.ieee.mant2 = tmp & 0xffffffffull;
    p("ALT set mant2 to low part of _update()<<4", u.d);
    _state = save_state;
}
    u.ieee.sign = 0x0;
    u.ieee.exponent = 0x3ff;
    unsigned48_t tmp = _update();
    tmp <<= 4;
    unsigned48_t tmp3 = tmp & 0xfffff00000000ull;
    tmp3 >>= 32;
    u.ieee.mant1 = (tmp & 0xfffff00000000ull)>>32;
    tmp3 = tmp & 0x0000ffffffffull;
    u.ieee.mant2 = tmp & 0xffffffffull;

    return u.d-1.0;
}

rand48  tls_rng  = RAND48_INITIALIZER;
randorig orig;
randalt alt;

#include <w_getopt.h>
#include <iostream>

int tries(0);
bool verbose(false);
bool debug(false);
bool shift(false);
bool use_drand(false);
bool use_randn(false);
bool use_alt(false);
unsigned long long seed = RAND48_INITIALIZER;
int    parse_args(int argc, char **argv)
{
    int    errors = 0;
    int    c;

    while ((c = getopt(argc, argv, "adDnt:sS:v")) != EOF) {
    switch (c) {
    case 'a':
            use_alt = true;
        break;
    case 'n':
            use_randn = true;
        break;
    case 'D':
            debug = true;
        break;
    case 'd':
            use_drand = true;
        break;
    case 'v':
        verbose = true;
        break;
    case 't':
        // tries = atoi(optarg);
        tries = atol(optarg);
        break;
    case 's':
        shift = true;
        break;
    case 'S':
        seed = (unsigned long long)atol(optarg);;
                orig.seed(seed);
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
            << " [-t tries]"
            << " [-S seed]"
            << " [-v (verbose: default=false)]"
            << " [-D (debug: default=false)]"
            << " [-s (print shift info: default=false)]"
            << " [-d (also use drand: default=false)]"
            << " [-n (also use nrand: default=false)]"
            << " [-a (also use alternative impl: default=false)]"
            << endl;
    }

    return errors ? -1 : optind;
}
void p(const char *str,  const PUN &u)
{
    cout << str 
         << " double: " << setprecision(21) << u.d 
         << " hex: " << ::hex << u.l << ::dec
         << " sign: " << ::hex << u.ieee.sign << ::dec
         << " exp: " << ::hex << u.ieee.exponent << ::dec
         << " mant1: " << ::hex << u.ieee.mant1 << ::dec
         << " mant2: " << ::hex << u.ieee.mant2 << ::dec;
    cout << " -> " ;

     for (int i=0;i < 8; i++) {
        cout << ::hex << " " << unsigned(u.c[i]) << ::dec;
     }
    cout << endl;
}
void p(const char *str, const double &d)
{
    PUN u;
    u.d = d;
    p(str, u);
}
void p(const char *str,  const unsigned48_t &g)
{
    PUN u;
    u.s = g;
    p(str, u);
}
void p(const char *str,  const signed48_t &g)
{
    PUN u;
    u.s = g;
    p(str, u);
}


int 
main(int argc, char *argv[])
{
if(verbose) {
#ifdef WORDS_BIGENDIAN
    cout << "BIG ENDIAN" << endl;
#else
    cout << "LITTLE ENDIAN" << endl;
#endif
    PUN u;
    u.big.sign = 0x1; // 1 bit
    u.big.exponent = 0x123; // 11 bits
    u.big.mant1 = 0xaaaaa; // 20 bits
    u.big.mant2 = 0xbbccddee; // 32 bits
    p("set u.big   ", u.s);
    u.lit.sign = 0x1; // 1 bit
    u.lit.exponent = 0x123; // 11 bits
    u.lit.mant1 = 0xaaaaa; // 20 bits
    u.lit.mant2 = 0xbbccddee; // 32 bits
    p("set u.little", u.s);
}

    int e = parse_args(argc, argv);
    if(e<0) exit(e);
    e = 0;

    alt._state = orig._state;

    // test the input and output functions (not operators)
    {
        unsigned48_t X = alt._state;
        const char *file = "./rand48-test";
        ofstream O(file, ios::out);
        out (O,X); 
        ifstream I(file);
        in (I, X);
        if(X != alt._state) {
            cerr << "Look at " <<  file << " : state should be " << alt._state
                << " but was input as " << X
                << endl;
        }
    }

    for(int i=0; i < tries; i++)
    {
        cout 
        << "--------------------------------------------------------- "
        << endl
        << ::dec << (i+1) << ": " ;

        if(verbose || debug) orig.dump("ORIG", debug);
        if((verbose || debug)  && use_alt) alt.dump("ALT ", debug);

        if( orig == alt ) 
        {
            if(verbose) cout << __LINE__ << " ---------  MATCH " << endl;
        }
        else
        {
            if(verbose) cout << __LINE__ << " ---------  MISMATCH " << endl;
            if(verbose) orig.dump("ORIG", debug);
            if(verbose && use_alt) alt.dump("ALT ", debug);
        }
        if(use_drand)
        {
        double d=orig.drand();
        double f=alt.drand();
        if(verbose && use_alt) 
        {
            if( orig == alt ) 
            {
            cout << __LINE__ << " ---------  MATCH " << endl;
            }
            else
            {
            cout << __LINE__ << " ---------  MISMATCH " << endl;
            orig.dump("ORIG", debug);
            alt.dump("ALT ", debug);
            }
        } else {
            if(verbose) orig.dump("ORIG", debug);
        }

        cout << setprecision(21) 
            << " drand: orig=" << d ;

        if(use_alt) 
        {
            cout << " alt=" << f ;

            if(d != f) {
            cout << " ************ERR" << endl ;
            e++;
            p(" orig.drand:", d);
            p("  alt.drand:", f);
            }
        }
        cout << endl;
        } // use_drand

        if(1) { // use rand()
        signed48_t g= orig.rand();
        signed48_t h = alt.rand();
        cout << " rand: orig=" << ::hex << g ;
        if(shift) {
            cout << " shift " << ::hex << (g<<32);
        }

        if(use_alt) {
            cout << " alt=" << h << ::dec ;
            if(g != h) {
                cout << " ************ERR" ;
                e++;
                p(" orig.drand:", g);
                p("  alt.drand:", h);
            }
        }
        cout << endl;
        } // rand

        if(use_randn) {
        signed48_t g= orig.randn(23*tries);
        signed48_t h = alt.randn(23*tries);
        cout 
            << " randn(" << 23*tries << "): orig=" << g ;
        if(use_alt) {
            cout << " alt=" << h ;
            if(g != h) {
                cout << " ************ERR" ;
                e++;
                p(" orig.drand:", g);
                p("  alt.drand:", h);
            }
        }
        cout << endl;
        } // use_randn
    }
    w_assert1(e==0) ;
    return e;
}
