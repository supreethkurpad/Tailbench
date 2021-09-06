/*<std-header orig-src='shore'>

 $Id: cuckoo.cpp,v 1.1.2.4 2010/03/19 22:17:53 nhall Exp $

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
// definition of sthread_t needed before including w_hashing.h
struct sthread_t
{
    static w_base_t::uint8_t rand();
};
typedef unsigned short uint2_t;

#include "w_hashing.h"
#include "w_findprime.h"
#include "w_getopt.h"
#include "rand48.h"
#include <iostream>

// MUST be initialized before the hashes below.
static __thread rand48 tls_rng = RAND48_INITIALIZER;

w_base_t::uint8_t sthread_t::rand() { return tls_rng.rand(); }

int tries(50);
enum {H0, H1, H2, H3, H4, H5, H6, H7, H8, H9, H10, HASH_COUNT};
w_hashing::hash2        _hash2[HASH_COUNT];
int hash_count(H2);

bool debug(false);
bool prnt(false);
w_base_t::int8_t     _size(1024);
w_base_t::int8_t     _prime_replacement(1024);
bool use_prime(true);

int        parse_args(int argc, char **argv)
{
    int        errors = 0;
    int        c;

    while ((c = getopt(argc, argv, "dn:ps:h:x:P:")) != EOF) {
        switch (c) {
        case 'h':
                hash_count = w_base_t::int8_t(atoi(optarg));
                break;
        case 's':
                _size = w_base_t::int8_t(atoi(optarg));
                break;
        case 'p':
                    prnt=true;
                break;
        case 'd':
                    debug=true;
                break;
        case 'P':
                _prime_replacement = w_base_t::int8_t(atoi(optarg));
                use_prime = false;
                break;
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
                    << " [-n <#tries>]"
                    << " [-p (print: default=false)]"
                    << " [-d (debug: default=false)]"
                    << " [-s <buffer pool size: default=1024>]"
                    << " [-h <#hash functions: default=2, max=10>]"
                    << " [-x <#hash functions or-ed: 1 or 2; default=2 ]"
                    << " [-P <hash table size (replacement for prime)> ]"
                    << endl;
    }

    return errors ? -1 : optind;
}

struct bfpid_t
{
    uint2_t _vol;
    uint   _store;
    uint   page;

    bfpid_t (uint2_t v, uint s, uint p) :
                _vol(v), _store(s), page(p) {}

    uint2_t vol() const { return _vol; }
    uint store() const { return _store; }
};
ostream &
operator << (ostream &o, const bfpid_t &p)
{
            o << p._vol << "." << p._store << "." << p.page ;
            return o;
}

unsigned 
hash(int h, bfpid_t const &pid) 
{
    if(debug) cout << "h=" << h << " pid=" << pid << endl;
    w_assert1(h >= 0 && h < HASH_COUNT);
    unsigned x = pid.vol();
    if(debug) cout << " x= " << x << endl;
    x ^= pid.page; // XOR doesn't do much, since
                   // most of the time the volume is the same for all pages
    if(debug) cout << " x= " << x << endl;
    w_assert1(h < HASH_COUNT);

    unsigned retval = 
		_hash2[h](x);

    if(debug) cout << " retval= " << retval << endl;
    retval %= unsigned(_size);
    if(debug) cout << " retval= " << retval << endl;
    return retval;
}

#define DUMP2(i) \
    if(prnt) cout << endl \
        << "\t" \
        << ::hex \
            << _hash2[i].a.a \
            << "=_hash2["<<::dec<<i<<::hex<<"].a.a" \
        << endl \
        << "\t" \
        << _hash2[i].a.b  \
            << "=_hash2["<<::dec<<i<<::hex<<"].a.b" \
        << endl \
        << "\t" \
        << _hash2[i].b.a \
            << "=_hash2["<<::dec<<i<<::hex<<"].b.a" \
        << endl \
        << "\t" \
        << _hash2[i].b.b  \
            << "=_hash2["<<::dec<<i<<::hex<<"].b.b" \
        << endl

void dump()
{
    if(prnt)
    {
        cout << "HASH 1  tables"  << endl;
    }
    if(prnt)
    {
        cout << "HASH 2  tables"  << endl;
    }
    for(int i=0; i < HASH_COUNT; i++) {
        DUMP2(i);
    }
}
    

int 
main(int argc, char *argv[])
{
    int e = parse_args(argc, argv);
    if(e<0) exit(e);

    cout << "tries=" << tries << endl;

    dump();

#define page_sz 8192ull

    w_base_t::int8_t nbufpages=(_size * 1024 - 1)/page_sz + 1; // sm.cpp passes to
            // bf_m constructor
    w_base_t::int8_t nbuckets=(16*nbufpages+8)/9; // bf_core_m constructor

    w_base_t::int8_t _prime = w_findprime(w_base_t::int8_t(nbuckets));
    if(!use_prime) { // override
        _prime = _prime_replacement;
    }

    cout << "For requested bp size "<< _size << ": "
    << endl
    << "\t nbufpages=" << nbufpages << " of page size " << page_sz << ","
    << endl
    << "\t nbuckets (from bf_core_m constructor)="  << nbuckets << "," 
    << endl;
    if(use_prime) {
        cout << "\t use prime " ;
    }
    cout 
        << " _size=" << _prime 
    << endl;

    _size = _prime;

    int *buckets = new int[_size];
    for(int i=0; i < _size; i++)
    { buckets[i]=0; }

    for(int i=0; i < tries; i++)
    {
#define START 0
        int j= START + i;

        bfpid_t p(1,5,j);
        for (int k=0; k < hash_count; k++)
        {
            unsigned h=::hash(k,p);
            if(prnt) {
                cout << ::hex << "0x" << h  << ::dec
                <<  "  \t "  << p
                <<  " hash # " << k ;
                if(buckets[int(h)] > 0) cout << " XXXXXXXXXXXXXXXXXXXX ";
                cout << endl;
            }

            buckets[int(h)]++;
        }
                    
    }

    int worst=0;
    int ttl=0;
    for(int i=0; i < _size; i++)
    {
        int collisions=buckets[i]-1;
        if(collisions > 0) {
            ttl += collisions;
            if(collisions > worst) worst=collisions;
        }
    }
    cout << "SUMMARY: buckets: " << _size
        << endl
        << " tries: " << tries << "(x " << hash_count
        << " hashes=" << tries*hash_count << ");  "
        << endl
        << " collisions: " << ttl
        << " ( " << 100*float(ttl)/float(tries*hash_count) << " %) "
        << endl
        << " worst case (max bkt len): " << worst
        << endl;

    delete[] buckets;

}
