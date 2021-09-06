/*<std-header orig-src='shore' incl-file-exclusion='TCL_THREAD_H'>

 $Id: tcl_thread.h,v 1.35.2.10 2010/03/19 22:20:31 nhall Exp $

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

#ifndef TCL_THREAD_H
#define TCL_THREAD_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#ifdef __GNUG__
#pragma interface
#endif

#include "tcl_workaround.h"

extern "C" int num_tcl_threads_running();
extern bool interactive; 
extern const char *argv0;  // the REAL argv[0]
extern bool debug; 
extern Tcl_Interp* global_ip; 
class ss_m;
extern ss_m* sm;
extern int t_co_retire(Tcl_Interp* , int , TCL_AV char*[]);

class barrier_t; // forward, in tcl_thread.cpp
class tcl_thread_t : public smthread_t  {
public:
    NORET                        tcl_thread_t(
        int                         ac, 
        TCL_AV char*                av[], 
        const char *                libdirpath,
        const char *                rcfilename
        );
    NORET                        tcl_thread_t(
        int                          ac, 
        TCL_AV char*                 av[], 
        Tcl_DString*                 parent_vars,
        Tcl_DString*                 parent_procs
        );
    NORET                        ~tcl_thread_t();

    Tcl_Interp*                  get_ip() { return ip; }

    static w_list_t<tcl_thread_t, queue_based_block_lock_t> threadslist;
    w_link_t                     link;

#define USE_BARRIER 1
#if USE_BARRIER
    pthread_barrier_t            mybarrier; // of 2
#else
    volatile bool                isWaiting;
    volatile bool                canProceed;
    volatile bool                hasExited;
    pthread_cond_t               quiesced; // paired with lock
    pthread_cond_t               proceed; // paired with lock
    pthread_mutex_t              lock; // paired with quiesced, proceed
#endif


    static void                 sync_other(unsigned long id);
    static void                 sync_others(int n, unsigned long id[]);
    static void                 sync_threads(int n, tcl_thread_t *[]);
    static void                 join(unsigned long id);
    w_rc_t                      join() { return this->smthread_t::join(); }

    void                         sync();
    void                         named_sync(barrier_t *b);

    // This is a pointer to a global variable that all
    // threads can use to pass info between them.
    // It is linked to a TCL variable via the
    // link_to_inter_thread_comm_buffer command.
    static char*                inter_thread_comm_buffer;

protected:
    virtual void                 run();

protected:
    const char *                lib_dir;
    const char *                rcfile;
    char*                       args;
    int                         ac;
    Tcl_Interp*                 ip;
    bool                        is_main_thread;
    Tcl_DString *               parent_vars;
    Tcl_DString *               parent_procs;
    static tcl_thread_t        *find(unsigned long id);
public:
    static queue_based_block_lock_t      thread_mutex;


    /* these are protected by thread_mutex, which also protects the
     * list of tcl_thread_t  threadslist
     */
    static int                  thread_count;
    static int                  thread_forked;
    static int                  thread_joined;
public:
    static bool                 allow_remote_command;

private:
    tcl_thread_t(const tcl_thread_t&);
    tcl_thread_t& operator=(const tcl_thread_t&);
    void                        set_args(TCL_AV char*    av[]);

    void                        initialize();
    void                        uninitialize();
};

extern void grab_procs(Tcl_Interp* pip, Tcl_DString *buf);
extern void grab_vars(Tcl_Interp* pip, Tcl_DString *buf);

// for gcc template instantiation
typedef w_list_i<tcl_thread_t, queue_based_block_lock_t>             tcl_thread_t_list_i;

extern int num_tcl_threads_ttl;

/*<std-footer incl-file-exclusion='TCL_THREAD_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
