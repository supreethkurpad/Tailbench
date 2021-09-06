/*<std-header orig-src='shore'>

 $Id: htab.cpp,v 1.3 2010/06/08 22:28:15 nhall Exp $

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
#define SM_SOURCE
#define HTAB_UNIT_TEST_C

#include "w.h"
typedef w_rc_t rc_t;
typedef unsigned short uint2_t;

#include "sm_int_4.h"
#include "bf_core.h"
#include "w_getopt.h"
#include "rand48.h"

rand48  tls_rng  = RAND48_INITIALIZER;
bool    debug(false);
bool    Random(false);
bool    Random_uniq(false);
int     tries(10);
signed48_t pagebound(1000);
uint2_t vol(1);
uint    storenum(5);
uint    bufkb(1024);
uint4_t    npgwriters(1);

uint4_t  nbufpages = 0;

void usage(int W_UNUSED(argc), char *const *argv)
{
    // while ((option = getopt(argc, argv, "b:dn:p:Rs:v:w:")) != -1) {
    cerr << "usage:  " << argv[0]
    << " -b <bufpoolsz in KB> : default=" <<  int(bufkb)
    << endl
    << " -w <#pagewriters> : default="  << int(npgwriters)
    << endl
    << " -v <volume#> : default= "  << int(vol)
    << endl
    << " -s <store#> :  default="  << int(storenum)
    << endl
    << " -R (means use random page#s :  default=" 
        << (const char *)(Random?"true":"false")
    << endl
    << " -r (means use unique random page#s :  default=" 
        << (const char *)(Random_uniq?"true":"false")
    << endl
    << " -p <page# limit> : default="  << long(pagebound)
    << endl
    << " -n <# tries> : default="  << tries
    << endl
    << " -d (means debug : default="  
        << (const char *)(debug?"true":"false")
    << endl;
}

class bfcb_t; // forward

// This has to derive from smthread_t because the buffer-pool has to
// be used in an smthread_t::run() context, in order that its statistics
// can be gathered in a per-thread structure.  If we don't do it this way,
// we'll croak in INC_TSTAT deep in the buffer pool.
class htab_tester : public smthread_t
{
    typedef enum { Zero, Init, Inserted, Evicted, Returned, Removed } Status;
    // Zero: not used yet
    // Init : initialized with a pid
    // Inserted : Inserted into the htab
    // Evicted: we noticed it's not in the ht any more - 
    //    bf_core::replacement evicted it
    // Returned: got moved by an insert (we don't get told about every move)  
    // Removed: we removed it
    bf_m *     _bfm;
    int        _tries;
    signed48_t _pagebound;
    uint2_t    _vol;
    uint       _store;
    bf_core_m *core;

protected:
    struct pidinfo 
    {
        lpid_t pid; // index i -> pid
        lpid_t returned; // return value from insert
        Status status;
        int    count; // # times random gave us this pid
        int    count_removes; // # times returned/evicted/removed
        int    inserts;
        int    evicts;

        pidinfo() : status(Zero),count(0),count_removes(0),
        inserts(0), evicts(0) {}
        friend ostream & operator<< (ostream &o, const struct pidinfo &info);
    };
    friend ostream & operator<< (ostream &o, const struct pidinfo &info);
private:

    lpid_t  *_i2pid; // indexed by i 
    pidinfo *_pid2info; // indexed by pid

    bf_core_m::Tstats S;

public:
    htab_tester(int i, signed48_t pb, uint2_t v, uint    s) : 
         _bfm(NULL),
         _tries(i),
         _pagebound(pb),
         _vol(v), _store(s)
    {
        {
            long  space_needed = bf_m::mem_needed(nbufpages);
            /*
             * Allocate the buffer-pool memory
             */ 
            char    *shmbase;
            w_rc_t    e;
#ifdef HAVE_HUGETLBFS
            // fprintf(stderr, "setting path to  %s\n", _hugetlbfs_path->value());
             e = smthread_t::set_hugetlbfs_path(HUGETLBFS_PATH);
#endif
            e = smthread_t::set_bufsize(space_needed, shmbase);

            if (e.is_error()) {
                W_COERCE(e);
            }
            w_assert1(is_aligned(shmbase));
            bf_m *_bfm = new bf_m(nbufpages, shmbase, npgwriters);

            if (! _bfm) {
                W_FATAL(fcOUTOFMEMORY);
            }
        }

        _pid2info = new pidinfo[int(pb)];
        _i2pid = new lpid_t[i];
        core = _bfm->_core;
        memset(&S, '\0', sizeof(S));
    }
    ~htab_tester() 
    {

    }

    void run();
    void run_inserts();
    void run_lookups();
    void run_removes();
    void cleanup();
    pidinfo & pid2info(const lpid_t &p) { return _pid2info[p.page]; }
    pidinfo &i2info(int i) { return pid2info(i2pid(i)); }
    lpid_t &i2pid(int i) { return _i2pid[i]; }

    bool was_returned(lpid_t &p) 
    {
        for(int i=0; i < _tries; i++)
        {
            if(_pid2info[i].returned == p) return true;
        }
        return false;
    }
	// non-const b/c it updates the stats
    void printstats(ostream &o, bool final=false);
    void print_bf_fill(ostream &o) const;

};

void
htab_tester::print_bf_fill(ostream &o) const
{
    int frames, slots;
    htab_count(core, frames, slots);

    o << "BPool HTable stats part2: #buffers "  << frames 
        << " #slots " << slots 
    << "  " <<  (slots*100.0)/(float)frames 
        << "% full "  
    << endl;
}

void
htab_tester::printstats(ostream &o, bool final) 
{
    if(final) o << "FINAL ";

    o << "NEW htab stats:" << endl;

#define D(x) if(S.bf_htab_##x > 0) o << S.bf_htab_##x << " " << #x << endl;
    if(!final)
    {
        D(insertions);
        D(ensures);
        D(cuckolds);
        D(slow_inserts);
        D(slots_tried);
        D(probes);
        D(harsh_probes);
        D(probe_empty);
        D(hash_collisions);
        D(harsh_lookups);
        D(lookups);
        D(lookups_failed);
        D(removes);
        D(max_limit);
        D(limit_exceeds);
    }
#undef D
#define D(x) if(S.x > 0) o << #x << " " << S.x << endl;
    else
    {
        _bfm->htab_stats(S);
        S.compute();

        D(bf_htab_insertions);
        // D(bf_htab_slots_tried);
        D(bf_htab_slow_inserts);
        D(bf_htab_probe_empty);
        // D(bf_htab_hash_collisions); (depends on hash funcs, optimiz level)
        D(bf_htab_harsh_lookups);
        D(bf_htab_lookups);
        D(bf_htab_lookups_failed);
        D(bf_htab_removes);
    }
#undef D

    print_bf_fill(o);
}

void htab_tester::cleanup()
{
    run_removes();
    delete[] _pid2info;
    delete[] _i2pid;
    // delete core;
    delete   _bfm;
    _bfm=0;
}

void htab_tester::run()
{
    signed48_t    pgnum(0); 

    // Create the set of page ids
    // Either sequential or random.
    for(int i=0; i < _tries; i++)
    {
        pidinfo &info = i2info(i);
        w_assert0(info.status == Zero);
    }
    for(int i=0; i < _tries; i++)
    {
        if(Random) {
            // If Random_uniq, we have to look through all
            // the already-created pids and if this pgnum is
            // already there, we have to jettison it and
            // try another.
            pgnum = tls_rng.randn(_pagebound);

            if(Random_uniq)  {
                // give it at most _pagebound tries
                int j= int(_pagebound);
                while (j-- > 0) 
                {
                    pidinfo &info = _pid2info[pgnum];
                    // Is it already in use?
                    if(info.status == Zero) break;
                    // yes -> try again
                    pgnum = tls_rng.randn(_pagebound);
                }
                if(j == 0) {
                    cerr << " Could not create unique random set " << endl;
                    exit (-1);
                }
#if 0
            } else {
                pidinfo &info = _pid2info[pgnum];
                if(info.status != Zero)  
                if(debug) {
                // merely report duplicate
                cout << " random produced duplicate " 
                    << info.pid
                    << endl;
                }
#endif
            }
        } else {
            // sequential
            pgnum = i % _pagebound; 
        }
        // Create a pid based on pgnum and store it

#define START 0
    // Well, there IS a page 0...
        pgnum += START;

        lpid_t p(_vol, _store, pgnum);
        pidinfo &info = pid2info(p);

#if 0
        if(info.status != Zero) if(debug) 
        {
            // duplicate
            cout << __LINE__ 
            << " detected duplicate " << p
            << " at index " << i 
            // << " info: " << info
            << endl;
            w_assert1(info.status == Init);
            w_assert1(info.pid == p);
            w_assert1(info.count > 0);
        }
#endif
        _i2pid[i] = p;
        info.pid = p;
        info.count ++;
        // info.returned is null pid
        info.status = Init;
        info.inserts = info.evicts = 0;
#if 0
        if(debug) cout << p << endl;
#endif
    }

    // let's verify that we have no dups if we don't want dups
    if(debug || Random_uniq) for(int i=0; i < _tries; i++)
    {
        pidinfo &info=_pid2info[_i2pid[i].page];
        if(Random_uniq) w_assert0(int(info.pid.page) == i || info.pid.page == 0);
#if 0
        if(info.count > 1) {
            cout << info.pid << " duplicated; count= "
            << info.count << endl;
        }
#endif
    }
    if(Random_uniq) {
        cout << "verified no dups" << endl;
    }

    // do the test
    run_inserts();
    run_lookups();
    // Don't remove: just see how the hash table used the
    // available buffers and entries.
    // run_removes();

    {
        int evicted(0), returned(0), inserted(0), inited(0), removed(0);
        for(int i=0; i < _tries; i++)
        {
            pidinfo &info = i2info(i);
            if(info.status == Init) inited++;
            if(info.status == Inserted) inserted++;
            if(info.status == Returned) returned++;
            if(info.status == Evicted) evicted++;
            if(info.status == Removed) removed++;
        }
        int unaccounted = _tries - (inited + inserted+ returned + evicted
                + removed);
        cout << "Remaining Init " << inited
            << " Inserted " << inserted
            << " Evicted " << evicted
            << " Returned " << returned
            << " Removed " << removed
            << " Unaccounted " << unaccounted
            << endl;

    }
    printstats(cout, true);
    cleanup();
}

void htab_tester::run_inserts()
{
    for(int i=0; i < _tries; i++)
    {
        pidinfo &info = i2info(i);

        lpid_t pid = info.pid;
        if(info.status == Inserted)  
        if(debug) {
            cout 
            << "i=" << i
            << " pid=" << pid
            << " ALREADY INSERTED " 
                << endl;
            // This COULD trigger an assert in bf_core.cpp
            cout << " #inserts " << info.inserts
            << " #evicts " << info.evicts
            << " count " << info.count
            << endl;
        }

        int slots(0);
        int frames(0); 
        if(debug) 
        {
            htab_count(core, frames, slots);
            cout << "before htab_insert:\t frames =" << frames 
            << " slots in use " << slots <<endl;;
        }

        bfcb_t *p = htab_insert(core, pid, S);
            
        if(p) 
        {
            if(debug) {
                cout << "Possible move: : pid= "
                << pid
                << " returned pid "
                << p->pid()
                << endl; 

                printstats(cout); 
            }

            info.returned = p->pid();

            pidinfo &info_returned = pid2info(p->pid());
            info_returned.status = Returned;
            info_returned.evicts++ ;

            // make sure the pin count is zero so that
            // the sm has a possibility of evicting it later.
            p->zero_pin_cnt();
        }
        info.status = Inserted;
        info.inserts++;
        if(debug) {
            if(i % 10) cout << "." ;
            else cout << i << endl;
        }
        bfcb_t *p2 = htab_lookup(core, pid, S);
        if(!p2) {
            cerr << " Cannot find just-inserted page " << pid << endl;
            printstats(cerr);
            w_assert1(0);
        } else {
            // correct the pin-count, since the lookup incremented it...
            // This is to avoid an assert at shut-down
            // and to give the old bf htab half a chance to evict a
            // page.
           p2->zero_pin_cnt();
        }
        w_assert1(p2->pid() == pid);

        if(debug) {
            int slots2;
            htab_count(core, frames, slots2);
            if(slots2 <  slots) 
            {
                cout << "htab_insert reduced # entries in use:\t frames =" 
                << frames << " slots in use " << slots2 <<endl;
                // w_assert1(0);
            }
        }

    }
    if(debug) {
        cout <<endl <<  " after insertions : " << endl; printstats(cout);
    }
}

void htab_tester::run_lookups()
{
    for(int i=0; i < _tries; i++)
    {
        pidinfo &info = i2info(i);
        lpid_t pid=info.pid;
        bfcb_t *p = htab_lookup(core, pid, S);
        if(!p) {
            if(info.status != Returned) 
            {
                // NOTE: the hash table at this writing gives
                // no indication when it cannot insert because
                // it ran out of room.   The assumption is that
                // there is room for everything because in the
                // sm the number of page buffers in the pool limits
                // the number of things you would have inserted 
                // at any time.
                // What happens here is that the bf_core::replacement()
                // pitches out what it finds that has pin_cnt of 0,
                // which is the case for every one of these we are inserting.
                if(debug) {
                    cout << pid << " was evicted; "
                    << " status = " << info.status
                    << " was_returned = " << was_returned(pid)
                    << endl;
                }
                info.status = Evicted; // without explanation 
                info.evicts++; // without explanation 
            }
        }
    }
    if(debug) {
    cout <<endl <<  " after lookups : " << endl; printstats(cout);
    }
}

void htab_tester::run_removes()
{
    for(int i=0; i < _tries; i++)
    {
        pidinfo &info = i2info(i);
        lpid_t pid=info.pid;
        if(info.count_removes == 0)
        {
            // Don't try to remove it twice
            /*bool b =*/ htab_remove(core, pid, S);
            info.status = Removed;
            // note: returns false if pin count was zero, which
            // it will be for this test.
            info.count_removes++;
        }
    }
    if(debug) {
        cout << endl << " after removes : " << endl; printstats(cout);
        cout << endl;
    }
}

#include <pthread.h>
int
main (int argc, char *const argv[])
{
     
    const int page_sz = SM_PAGESIZE;

    char option;
    while ((option = getopt(argc, argv, "b:dFn:p:rRs:Tv:w:")) != -1) {
    switch (option) {
    case 'r' :
        Random_uniq = true;
        Random = true;
        break;
    case 'R' :
        Random = true;
        break;
    case 'd' :
        debug = true;
        break;
    case 'b' :
            bufkb = strtol(optarg, 0, 0);
            break;
    case 'w' :
            npgwriters = strtol(optarg, 0, 0);
        break;
    case 'v':
            vol = atoi(optarg);
        break;
    case 's':
            storenum = atoi(optarg);
        break;
    case 'p':
            pagebound = strtol(optarg, 0, 0);
        break;
    case 'n':
            tries = atoi(optarg);
        break;
    default:
        usage(argc, argv);
        return 1;
        break;
    }
    }

    if(Random_uniq && (tries > pagebound)) {
        // NOTE: we now have to do this because we index
        // the info structures on the page #
        cerr << "For " << tries 
            << " page bound (-p) is too low: "
            << int(pagebound) << ". Using " << tries << endl;
        pagebound = tries;
    }
    
    nbufpages = (bufkb * 1024 - 1) / page_sz + 1;
    if (nbufpages < 10)  {
        cerr << error_prio << "ERROR: buffer size ("
             << bufkb
             << "-KB) is too small" << flushl;
        cerr << error_prio << "       at least " << 10 * page_sz / 1024
             << "-KB is needed" << flushl;
        W_FATAL(fcOUTOFMEMORY);
    }
#if DEAD
    long  space_needed = bf_m::mem_needed(nbufpages);
    /*
     * Allocate the buffer-pool memory
     */ 
    char    *shmbase;
    w_rc_t    e;
#ifdef HAVE_HUGETLBFS
    // fprintf(stderr, "setting path to  %s\n", _hugetlbfs_path->value());
     e = smthread_t::set_hugetlbfs_path(HUGETLBFS_PATH);
#endif

    e = smthread_t::set_bufsize(space_needed, shmbase);
    if (e.is_error()) {
        W_COERCE(e);
    }
    w_assert1(is_aligned(shmbase));

    // cout <<"SHM at address " << W_ADDR(shmbase) << endl;

    bf_m *bf = new bf_m(nbufpages, shmbase, npgwriters);

    if (! bf) {
        W_FATAL(fcOUTOFMEMORY);
    }
    // cout <<"bfm at address " << W_ADDR(bf) 
    // << " nbufpages " << nbufpages
    // << " npagewriters " << npgwriters
    // << endl;
#endif

    latch_t::on_thread_init(me());
    {
        cout <<"creating tests with " 
             << tries << " tries, "
             << pagebound << " upper bound on pages, "
        << " volume " << vol 
        << ", store " << storenum 
        << "."
        << endl;
        htab_tester anon(tries, pagebound, vol, storenum);
        anon.fork();
        anon.join();
    }
	// cerr << endl << flushl;
    latch_t::on_thread_destroy(me());
    return 0;
}
