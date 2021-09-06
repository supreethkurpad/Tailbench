/*<std-header orig-src='shore'>

 $Id: tcl_thread.cpp,v 1.142 2010/06/15 17:30:09 nhall Exp $

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

#define TCL_THREAD_C

#ifdef __GNUG__
#   pragma implementation
#endif


extern "C" void dumpthreads();


#include <strings.h>
#include "shell.h"
#include <crash.h>

bool debug(false);

#ifdef EXPLICIT_TEMPLATE
template class w_list_i<tcl_thread_t, queue_based_block_lock_t>;
template class w_list_t<tcl_thread_t, queue_based_block_lock_t>;
#endif

// an unlikely naturally occuring error string which causes the interpreter loop to
// exit cleanly.
static const char *TCL_EXIT_ERROR_STRING = "!@#EXIT#@!";

const char* Logical_id_flag_tcl = "Use_logical_id";
Tcl_Interp* global_ip (NULL); // for debugging

int tcl_thread_t::thread_count = 0;
int tcl_thread_t::thread_forked = 0;
int tcl_thread_t::thread_joined = 0;
char* tcl_thread_t::inter_thread_comm_buffer = 0;

ss_m* sm = 0;

extern "C" int vtable_dispatch(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[]);
extern "C" int sm_dispatch(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[]);
extern "C" int st_dispatch(ClientData, Tcl_Interp *, int, TCL_AV char **);

extern const char* tcl_init_cmd;
static int    num_tcl_threads(0);
/*static*/ int    num_tcl_threads_ttl(0); // now  extern
extern "C" int num_tcl_threads_running() { return num_tcl_threads; }

// protects threadslist
queue_based_block_lock_t tcl_thread_t::thread_mutex;

w_list_t<tcl_thread_t, queue_based_block_lock_t> 
    tcl_thread_t::threadslist(W_LIST_ARG(tcl_thread_t, link), &tcl_thread_t::thread_mutex);

class xct_i;

// For debugging
extern "C" void tcl_assert_failed();
void tcl_assert_failed() {}

extern "C" int
t_debugflags(ClientData, Tcl_Interp* ip, int ac, TCL_AV char** W_IFTRACE(av))
{
    if (ac != 2 && ac != 1) {
    Tcl_AppendResult(ip, "usage: debugflags [arg]", 0);
    return TCL_ERROR;
    }
#ifdef W_TRACE
    char *f;
    f = getenv("DEBUG_FILE");
    if(ac>1) {
        if(strcmp(av[1],"off")==0) {
            av[1] = (char *)"";
        }
         _w_debug.setflags(av[1]);
    } else {
        Tcl_AppendResult(ip,  _w_debug.flags(), 0);
    }
    if(f) {
        Tcl_AppendResult(ip,  "NB: written to file: ",f,".", 0);
    }
#endif
    return TCL_OK;
}
extern "C" int
t_assert(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (ac != 2) {
        Tcl_AppendResult(ip, "usage: assert [arg]", 0);
        return TCL_ERROR;
    }
    int boo = 0;
    int    res = Tcl_ExprBoolean(ip, av[1], &boo);
    if(res == TCL_OK && boo==0) {
        // assertion failure
        // A place to put a gdb breakpoint!!!
        tcl_assert_failed();
    }
    Tcl_AppendResult(ip, tcl_form_flag(boo), 0);
    cout << flush;
    return res;
}


extern "C" int
t_timeofday(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* /*av*/[])
{
    if (ac > 1) {
        Tcl_AppendResult(ip, "usage: timeofday", 0);
        return TCL_ERROR;
    }

    stime_t now = stime_t::now();

    w_ostrstream_buf otmp(100);        // XXX magic number
    /* XXX should use stime_t operator<<, but format differs */
    W_FORM2(otmp, ("%d %d", now.secs(), now.usecs()));
    otmp << ends;
    Tcl_AppendResult(ip, otmp.c_str(), 0);

    return TCL_OK;
}


#if !defined(USE_SSMTEST) 
#define av
#endif
extern "C" int
t_debuginfo(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
#if !defined(USE_SSMTEST) 
#undef av
#endif
{
    if (ac != 4) {
        Tcl_AppendResult(ip, "usage: debuginfo category v1 v2", 0);
        return TCL_ERROR;
    }
#if defined(USE_SSMTEST) 
    const char *v1 = av[2];
    int v2 = atoi(av[3]);
    debuginfo_enum d = debug_delay;
    /*
     * categories: delay, crash, abort, yield
     * v1  is string
     * v2  is int
     *
     * Same effect as setting environment variables 
     * (e.g.)
     * CRASHTEST <-- v1
     * CRASHTESTVAL <-- v2
     */
    if(strcmp(av[1], "delay")==0) {
        // v1: where, v2: time
        d = debug_delay;
    } else if(strcmp(av[1], "crash")==0) {
        // v1: where, v2: nth-time-through
        d = debug_crash;
    } else if(strcmp(av[1], "none")==0) {
        v1 = "none";
        v2 = 0;
        setdebuginfo(debug_delay, v1, v2);
        d = debug_crash;
    }
    setdebuginfo(d, v1, v2);
    return TCL_OK;
#else
    Tcl_AppendResult(ip, "USE_SSMTEST not configured", 0);
    return TCL_ERROR;
#endif
}

extern "C" int
t_write_random(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    rand48&  generator(get_generator());
    if (ac != 2) {
        Tcl_AppendResult(ip, "usage: write_random filename", 0);
        return TCL_ERROR;
    }
    if(ac == 2) {
        ofstream f(av[1], ios::out);
        if(!f) {
            cerr << "Cannot write to file " << av[1] << endl;
        }
        else
        {
            unsigned48_t  seed = generator._state;
            out(f,seed);
        }
        f.close();
    }
    return TCL_OK;
}

extern "C" int
t_read_random(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (ac != 2) {
        Tcl_AppendResult(ip, "usage: read_random filename", 0);
        return TCL_ERROR;
    }
    if(ac == 2) {
        rand48&  generator(get_generator());
        ifstream f(av[1]);
        if(!f) {
            cerr << "Cannot read file " << av[1] << endl;
        }
        else 
        {
            in(f,generator._state);
        }
        f.close();
    }
    return TCL_OK;
}

extern "C" int
t_random(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (ac > 2) {
        Tcl_AppendResult(ip, "usage: random [modulus]", 0);
        return TCL_ERROR;
    }
    int mod=0;
    if(ac == 2) {
        mod = atoi(av[1]);
    } else {
        mod = -1;
    }
    rand48&  generator(get_generator());
    unsigned int res = generator.rand(); // return only unsigned
    if(mod==0) {
        /* initialize to a given, known, state */
        generator.seed(0);
    } else if(mod>0) {
        res %= mod;
    }
    {
        w_ostrstream_buf otmp(40);        // XXX magic number
        W_FORM2(otmp, ("%#d",res));
        otmp << ends;
        Tcl_AppendResult(ip, otmp.c_str(), 0);
    }

    return TCL_OK;
}

extern "C" int
t_fork_thread(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{

    if (ac < 3)  {
        Tcl_AppendResult(ip, "usage: ", av[0], " proc args", 0);
        return TCL_ERROR;
    }

    char* old_result = strdup(Tcl_GetStringResult(ip));
    w_assert1(old_result);

    tcl_thread_t* p = 0;
    // 
    // When we create a thread from a script (t_fork_thread),
    // we MUST have at least argument.
    //

    w_assert1(ac - 1 > 0);
    {
        Tcl_DString* procs = new Tcl_DString;
        Tcl_DString* vars = new Tcl_DString;
        grab_procs(ip, procs);
        grab_vars(ip,  vars);
        p = new tcl_thread_t(ac - 1, av + 1, vars, procs);
    }

    // NOTE: new thread deletes the NS

    if (!p) {
        Tcl_AppendResult(ip, "cannot create thread", 0);
        return TCL_ERROR;
    }

    rc_t e = p->fork();
    if (e.is_error()) {
        delete p;
        Tcl_AppendResult(ip, "cannnot start thread", 0);
        return TCL_ERROR;
    }
    {
        CRITICAL_SECTION(cs, tcl_thread_t::thread_mutex);
        tcl_thread_t::thread_forked++;
    }

    /* XXX ownership of old_result relinquished, but should really
       allocate old_result with Tcl_Alloc() to be exacting. */
    // Tcl_SetResult(ip, old_result, TCL_DYNAMIC);
    Tcl_SetResult(ip, old_result, TCL_VOLATILE);
    free (old_result);

    w_ostrstream_buf s(20);        // XXX magic number
    s << p->id << ends;
    Tcl_AppendResult(ip, s.c_str(), 0);
    
    return TCL_OK;
}

/*
 * Old sync commands:
 * sync [comment] (that way when one of these is waiting, you can tell
 *                 which sync command it's waiting on)
 * sync_thread t1 t2 t3 ...
 *                in sequence, it issues syncs with these threads.
 *                No comment.
 *                Order is important, and makes it hard to get this to work
 *                in a true concurrent environment.
 *
 * New sync commands:
 *
 * Named barriers: these allow us to synchronize N threads, N>2.  Described 
 *               below Unnamed barriers.
 *
 * Unnamed barriers are necessarily for 2 threads.
 *               Unnamed barriers have the same syntax as the original
 *               sync/sync_thread commands and are a re-implementation 
 *               of those commands.   
 *
 *               Each thread has an implicit unnamed barrier for itself.
 *
 *               That's what the old command "sync" is waiting on now. 
 *               The old command "sync_thread t1" now syncs on that thread's
 *               implicit unnamed barrier, and
 *               sync_thread t1 t2 t3 does those syncs in order.
 *               So it's not very useful for more than 2 threads unless you
 *               can guarantee that the threads won't be waiting on 
 *               each other.
 *
 *               Commands: sync, sync_thread
 *
 * Named barriers: these allow us to synchronize N threads, N>2, such that
 *               none proceeds until all have synced to that barrier.
 *               Named barriers must be defined before use, and must be
 *               undefined to free up their resources.
 *
 *               Commands:
 *
 *                define_named_sync <name> N
 *                named_sync <name> [comment for debugging]
 *                undef_named_sync <name> [silent]
 *
 * NOTE: scripts/vol.init defines 10 named sync points, 1,2,...,10
 * for the associated number of threads.
 *
 * When the main tcl thread goes away, it removes all 
 * named barriers, so the undef isn't required, but it can be
 * used before operations like define_named_sync for idempotence.
 */

class barrier_t {
private: 
    pthread_barrier_t *_bar;
    char *_name;
public:
    w_link_t _link;
    bool  has_name(const char *nm) const  
    {
        return strcmp(nm, _name) == 0;
    }
    void wait();
    void detach() { 
        //fprintf(stderr, "removing barrier %s\n", _name);
        _link.detach(); 
    } // used inside a crit sect
    NORET barrier_t(const char *nm, int n) ;
    NORET ~barrier_t() ;
};
queue_based_lock_t barrier_mutex;
w_list_t<barrier_t, queue_based_lock_t> barriers(
            W_LIST_ARG(barrier_t, _link), &barrier_mutex);

NORET barrier_t::barrier_t(const char *nm, int n) : 
    _bar(NULL), _name(NULL)
{ 
    _name = strdup(nm);
    _bar = new pthread_barrier_t;
    DO_PTHREAD(pthread_barrier_init(_bar, 0, n));
    CRITICAL_SECTION(cs, &barrier_mutex);
    barriers.push(this);
    //fprintf(stderr, "created barrier %s\n", _name);
}
NORET barrier_t::~barrier_t() 
{ 
    ::free(_name);
    _name = NULL;
    {
        CRITICAL_SECTION(cs, &barrier_mutex);
        detach();
    }
    DO_PTHREAD(pthread_barrier_destroy(_bar));
    delete _bar;
    _bar = NULL;
}
void barrier_t::wait() 
{
    DO_PTHREAD_BARRIER(pthread_barrier_wait(_bar));
}

void remove_all_barriers() {
	barrier_t *b = NULL;
	do {
		// We should be safe because it is only
		// at the end of the main thread that we
		// do this.
		{
			CRITICAL_SECTION(cs, &barrier_mutex);
			w_list_i<barrier_t, queue_based_lock_t> iter(barriers);
			b = iter.next();
		}
		if(b) {
			delete b;
		}
	} while(b);
}

barrier_t *find_barrier(const char *name)
{
    CRITICAL_SECTION(cs, &barrier_mutex);
    w_list_i<barrier_t, queue_based_lock_t> iter(barriers);
    barrier_t *b = NULL;
    while( (b = iter.next() ) ) {
        if(b->has_name(name)) return b;
    }
    return NULL;
}

extern "C" int
t_define_named_sync(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "name number-of-threads", ac, 3, 3))
        return TCL_ERROR;

    int n = atoi(av[2]);
    barrier_t *b = NULL;
    if(n > 0) {
        b = new barrier_t(av[1], n);
    }
    if(!b) {
        Tcl_AppendResult(ip, "Could not created barrier ", av[1],
                " for ", av[2], " threads.", 0);
        return TCL_ERROR;
    }
    return TCL_OK;
}

extern "C" int
t_undef_named_sync(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "name [silent] ", ac, 2, 3))
        return TCL_ERROR;

    bool silent = false;
    if(ac > 2) {
        if(strcmp(av[2], "silent")==0) silent=true;
    }
    barrier_t *b = find_barrier(av[1]);
    if(!b && !silent) {
        Tcl_AppendResult(ip, "Could not find barrier ", av[1], 0);
        return TCL_ERROR;
    }
    if(b) {
        delete b;
    }
    return TCL_OK;
}

extern "C" int
t_named_sync(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "name [comment for debugging]", ac, 2, 3))
        return TCL_ERROR;

    const char *comment = "";
    if (ac > 2)  {
        comment = av[2];
    }
    // to shut the compiler up:
    if(comment && 0) fprintf(stderr,"shut the compiler up\n") ;


    barrier_t *b = find_barrier(av[1]);
    if(!b) {
        Tcl_AppendResult(ip, "Could not find barrier ", av[1], 0);
        return TCL_ERROR;
    }

    // ((tcl_thread_t *)sthread_t::me())->named_sync(b);
    b->wait();

    return TCL_OK;
}

extern "C" int
t_sync(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "[comment for debugging]", ac, 1, 2))
        return TCL_ERROR;

    const char *comment = "";
    if (ac > 1)  {
        comment = av[1];
    }
    // to shut the compiler up:
    if(comment && 0) fprintf(stderr,"shut the compiler up\n") ;

    ((tcl_thread_t *)sthread_t::me())->sync();

    return TCL_OK;
}

extern "C" int
t_sync_thread(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (ac == 1)  {
        Tcl_AppendResult(ip, "usage: ", av[0],
             " thread_id1 thread_id2 ...", 0);
        return TCL_ERROR;
    }

    u_long *ids = new u_long[ac-1];
    for (int i = 1; i < ac; i++)  {
        ids[i-1] = strtol(av[i], 0, 10);
    }
    tcl_thread_t::sync_others(ac-1, ids);
    delete [] ids;

    return TCL_OK;
}

extern "C" int
t_join_thread(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (ac == 1)  {
        Tcl_AppendResult(ip, "usage: ", av[0],
             " thread_id1 thread_id2 ...", 0);
        return TCL_ERROR;
    }

    for (int i = 1; i < ac; i++)  {
        tcl_thread_t::join(strtol(av[i], 0, 10));
        {
            CRITICAL_SECTION(cs, tcl_thread_t::thread_mutex);
            tcl_thread_t::thread_joined++;
        }
    }

    return TCL_OK;
}

extern "C" int
t_yield(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (ac != 1)  {
        Tcl_AppendResult(ip, "usage: ", av[0], 0);
        return TCL_ERROR;
    }

    me()->yield();

    return TCL_OK;
}

extern "C" int
t_link_to_inter_thread_comm_buffer(ClientData, Tcl_Interp* ip,
                   int ac, TCL_AV char* av[])
{
    if (ac != 2)  {
        Tcl_AppendResult(ip, "usage: ", av[0], "variable", 0);
        return TCL_ERROR;
    }

    return Tcl_LinkVar(ip, av[1], (char*)&tcl_thread_t::inter_thread_comm_buffer, TCL_LINK_STRING);

    //return TCL_OK;
}

extern "C" int
t_exit(ClientData, Tcl_Interp *ip, int ac, TCL_AV char* av[])
{
    int e = (ac == 2 ? atoi(av[1]) : 0);
    cout << flush;

    fprintf(stderr, 
        "********************************************************\n");
    fprintf(stderr, 
        "Exiting with %d via smsh exit command! Expect assertion failure.\n",
        e);
    fprintf(stderr, 
        "********************************************************\n");

    if (e == 0)  {
        Tcl_SetResult(ip, TCL_SETRES TCL_EXIT_ERROR_STRING, TCL_STATIC);
        return TCL_ERROR;  // interpreter loop will catch this and exit
    }  else  {
        _exit(e);
    }
    return TCL_ERROR;
}


/*
 * This is a hacked version of the tcl7.4 'time' command.
 * It uses the shore-native 'stime' support to print
 * "friendly" times, instead of
 *      16636737373 microseconds per iteration
 */

extern "C" int 
t_time(ClientData, Tcl_Interp *interp,int argc, TCL_AV char **argv)
{
    int count, i, result;
    stime_t start, stop;

    if (argc == 2) {
        count = 1;
    } else if (argc == 3) {
        if (Tcl_GetInt(interp, argv[2], &count) != TCL_OK) {
            return TCL_ERROR;
        }
    } else {
        Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
        " command ?count?\"", (char *) NULL);
        return TCL_ERROR;
    }
    start = stime_t::now();

    for (i = count ; i > 0; i--) {
        result = Tcl_Eval(interp, TCL_CVBUG argv[1]);
        if (result != TCL_OK) {
            if (result == TCL_ERROR) {
                w_ostrstream_buf msgstr(60);    // XXX magic number
                W_FORM2(msgstr, ("\n    (\"time\" body line %d)",
                         interp->errorLine));
                msgstr << ends;
                Tcl_AddErrorInfo(interp, TCL_CVBUG msgstr.c_str());
            }
            return result;
        }
    }

    stop = stime_t::now();

    Tcl_ResetResult(interp);    /* The tcl version does this. ??? */

    w_ostrstream_buf s(128);        // XXX magic number

    if (count > 0) {
        sinterval_t timePer(stop - start);
        s << timePer << " seconds";

        if (count > 1) {
            sinterval_t timeEach(timePer / count);;
            s << " (" << timeEach << " seconds per iteration)";
        }
    } else
        s << "0 iterations";
    s << ends;

    Tcl_AppendResult(interp, s.c_str(), 0);

    return TCL_OK;
}

extern "C" int
t_pecho(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    for (int i = 1; i < ac; i++) {
        cout << ((i > 1) ? " " : "") << av[i];
#ifdef PURIFY
        if(purify_is_running()) {
            if(i>1) purify_printf(" ");
            purify_printf(av[i]);
        }
#endif
        Tcl_AppendResult(ip, (i > 1) ? " " : "", av[i], 0);
    }
#ifdef PURIFY
    if(purify_is_running()) { purify_printf("\n"); }
#endif
    cout << endl << flush;

    return TCL_OK;
}


extern "C" int
t_echo(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    for (int i = 1; i < ac; i++) {
        cout << ((i > 1) ? " " : "") << av[i];
        Tcl_AppendResult(ip, (i > 1) ? " " : "", av[i], 0);
    }
    cout << endl << flush;

    return TCL_OK;
}



/*
 * This is from Henry Spencer's Portable string library, which
 * is in the public domain.  Bolo changed it to have a per-thread
 * context, so safe_strtok() can be used safely in a multi-threaded
 * environment.
 *
 * To use safe_strtok(), set with *scanpoint = NULL when starting off.
 * After that, it will be maintained normally.
 *
 * Get next token from string s (NULL on 2nd, 3rd, etc. calls),
 * where tokens are nonempty strings separated by runs of
 * chars from delim.  Writes NULs into s to end tokens.  delim need not
 * remain constant from call to call.
 *
 * XXX this may be something to insert into the fc or common
 * directories, as it is a problem wherever threads use strtok.
 */

static char *safe_strtok(char *s, const char *delim, char *&scanpoint)
{
    char *scan;
    char *tok;
    const char *dscan;

    if (s == NULL && scanpoint == NULL)
        return(NULL);
    if (s != NULL)
        scan = s;
    else
        scan = scanpoint;

    /*
     * Scan leading delimiters.
     */
    for (; *scan != '\0'; scan++) {
        for (dscan = delim; *dscan != '\0'; dscan++)
            if (*scan == *dscan)
                break;
        if (*dscan == '\0')
            break;
    }
    if (*scan == '\0') {
        scanpoint = NULL;
        return(NULL);
    }

    tok = scan;

    /*
     * Scan token.
     */
    for (; *scan != '\0'; scan++) {
        for (dscan = delim; *dscan != '\0';)    /* ++ moved down. */
            if (*scan == *dscan++) {
                scanpoint = scan+1;
                *scan = '\0';
                return(tok);
            }
    }

    /*
     * Reached end of string.
     */
    scanpoint = NULL;
    return(tok);
}


void grab_vars(Tcl_Interp* pip, Tcl_DString *vars)
{
    // assertions: will call stackoverflowed() if !ok and will return false
    w_assert1(sthread_t::me()->isStackFrameOK(0));
    int err = Tcl_Eval(pip, TCL_CVBUG "info vars");
    if (err != TCL_OK)  {
        w_assert1(0);
    } else {
        if(debug) {
            cerr 
            << __LINE__
            << "{ info vars returns " <<  Tcl_GetStringResult(pip) 
            << "}" << endl << endl;
        }
    }

    char    *result = strdup(Tcl_GetStringResult(pip));
    w_assert1(result);
    if(debug) { cerr <<  __LINE__ << "{ grab_vars  returns " << result 
        << " }" << endl << endl; }

    Tcl_DStringInit(vars);

    // char    *last = result + strlen(result);
    char    *context = 0;
    char    *p = safe_strtok(result, " ", context);

    static const char *array_set_cmd = "array set ";
    static const char *set_cmd = "set ";
    static const char *blank = " ";
    static const char *empty = "\"\"";
    static const char *startlist = "{";
    static const char *endlist = "}";
    static const char *nl = "\n";

    while (p)  {
        TCL_GETX char* v = Tcl_GetVar(pip, p, TCL_GLOBAL_ONLY);

        if (v)  {
            Tcl_DStringAppend(vars, set_cmd, strlen(set_cmd));
            Tcl_DStringAppend(vars, p, strlen(p));
            Tcl_DStringAppend(vars, blank, 1);
            Tcl_DStringAppend(vars, startlist, 1);
            Tcl_DStringAppend(vars, blank, 1);

            if(debug) {
                cerr << __LINE__ 
                << " p " << p
                << " v " << v
                << endl
                << " v len " << strlen(v) << endl;
            }

            if(strlen(v) == 0) {
                Tcl_DStringAppend(vars, empty, strlen(empty));
            }
            else {
                Tcl_DStringAppend(vars, v, strlen(v));
            }
            Tcl_DStringAppend(vars, blank, 1);
            Tcl_DStringAppend(vars, endlist, 1);
            Tcl_DStringAppend(vars, nl, 1);

            // Tcl_SetVar(ip, p, v, TCL_GLOBAL_ONLY);
        } else 
        if (strcmp(p,"env")) {
            // skip the environment b/c this could be problematic
            // for non-main threads -- I seem to choke here.

            Tcl_VarEval(pip, "array get ", p, 0);
            char *sr = strdup(Tcl_GetStringResult(pip));
            Tcl_DStringAppend(vars, array_set_cmd, strlen(array_set_cmd));
            Tcl_DStringAppend(vars, blank, 1);
            Tcl_DStringAppend(vars, p, strlen(p));
            Tcl_DStringAppend(vars, blank, 1);
            Tcl_DStringAppend(vars, startlist, 1);
            Tcl_DStringAppend(vars, blank, 1);
            if(debug) {
                cerr << __LINE__ 
                << " p " << p
                << " sr " << sr
                << endl
                << " sr len " 
                << strlen(sr) 
                << endl;
            }
            Tcl_DStringAppend(vars, sr, strlen(sr));
            Tcl_DStringAppend(vars, blank, 1);
            Tcl_DStringAppend(vars, endlist, 1);
            Tcl_DStringAppend(vars, nl, 1);
            free(sr);
        }
        p = safe_strtok(0, " ", context);
    }

    if(debug) { cerr <<  __LINE__ << " { grab_vars returns " 
        << Tcl_DStringValue(vars) << "}" << endl << endl; }
    free(result);
    // assertions: will call stackoverflowed() if !ok and will return false
    w_assert1(sthread_t::me()->isStackFrameOK(0));
}

void grab_procs(Tcl_Interp* pip, Tcl_DString *buf)
{
    // assertions: will call stackoverflowed() if !ok and will return false
    w_assert1(sthread_t::me()->isStackFrameOK(0));
    Tcl_DStringInit(buf);
    
    int err = Tcl_Eval(pip, TCL_CVBUG "info procs");
    if (err != TCL_OK)  {
        w_assert1(0);
    }

    char* procs = strdup(Tcl_GetStringResult(pip));

    if(debug) { cerr << "{grab_procs " << procs  << "}" << endl << endl; }
    if (*procs)  {
        char *context = 0;
        w_assert1(procs);

        for (char* p = safe_strtok(procs, " ", context);
                 p;   p = safe_strtok(0, " ", context))  
        {
            char line[1000];

            Tcl_DStringAppend(buf, "proc ", -1);
            Tcl_DStringAppend(buf, p, -1);
            {
                w_ostrstream s(line, sizeof(line));
                s << "info args " << p << ends;
                Tcl_Eval(pip, TCL_CVBUG s.c_str());
                Tcl_DStringAppend(buf, " { ", -1);
                Tcl_DStringAppend(buf, Tcl_GetStringResult(pip), -1);
                Tcl_DStringAppend(buf, " } ", -1);
            }
            {
                w_ostrstream s(line, sizeof(line));
                s << "info body " << p << ends;
                Tcl_Eval(pip, TCL_CVBUG s.c_str());
                Tcl_DStringAppend(buf, " { ", -1);
                Tcl_DStringAppend(buf, Tcl_GetStringResult(pip), -1);
                p = Tcl_DStringAppend(buf, " } \n", -1);
                assert(p);
                assert(Tcl_CommandComplete(p));
                
                assert(err == TCL_OK);
            }
        }
    }
    free(procs);
    if(debug) { cerr << "grab_procs returns " << Tcl_DStringValue(buf) << endl << endl; }
    Tcl_ResetResult(pip);
    // assertions: will call stackoverflowed() if !ok and will return false
    w_assert1(sthread_t::me()->isStackFrameOK(0));
}

static void create_stdcmd(Tcl_Interp* ip)
{
    Tcl_CreateCommand(ip, TCL_CVBUG "debugflags", t_debugflags, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "__assert", t_assert, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "timeofday", t_timeofday, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "random", t_random, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "read_random", t_read_random, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "write_random", t_write_random, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "debuginfo", t_debuginfo, 0, 0);

    Tcl_CreateCommand(ip, TCL_CVBUG "fork_thread", t_fork_thread, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "join_thread", t_join_thread, 0, 0);
    // original sync points: unnamed. Now these are 2-thread barriers
    Tcl_CreateCommand(ip, TCL_CVBUG "sync", t_sync, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "sync_thread", t_sync_thread, 0, 0);
    // New sync points: named. Now these are N-thread barriers and have to be declared
    // and given the number N.
    Tcl_CreateCommand(ip, TCL_CVBUG "define_named_sync", t_define_named_sync, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "undef_named_sync", t_undef_named_sync, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "named_sync", t_named_sync, 0, 0);
    // alias for named_sync:
    Tcl_CreateCommand(ip, TCL_CVBUG "named_sync_thread", t_named_sync, 0, 0);

    Tcl_CreateCommand(ip, TCL_CVBUG "yield", t_yield, 0, 0);

    Tcl_CreateCommand(ip, TCL_CVBUG "link_to_inter_thread_comm_buffer", t_link_to_inter_thread_comm_buffer, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "echo", t_echo, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "pecho", t_pecho, 0, 0); // echo also to purify logfile

    // Tcl_CreateCommand(ip, TCL_CVBUG "verbose", t_verbose, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "exit", t_exit, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "time", t_time, 0, 0);
}

class  __shared {
public:
    char                line[1000];
    int                 stdin_hdl;
    int                 line_number;

    __shared( int fd) : stdin_hdl(fd)
        { line[0] = '\0';  line_number=0;}

    w_rc_t    read();
};

w_rc_t    __shared::read()
{
    cin.getline(line, sizeof(line) - 2);
    line_number++;
    return RCOK;
}

static void process_stdin(Tcl_Interp* ip)
{
    FUNC(process_stdin);

    int partial = 0;
    Tcl_DString buf;
    Tcl_DStringInit(&buf);
    const int tty = isatty(0);
    __shared shared(0);

    while (1) {
        cin.clear();
        if (tty) {
            TCL_GETX char* prompt = 
                Tcl_GetVar(ip, TCL_CVBUG (partial ? "tcl_prompt2" :
                           "tcl_prompt1"), TCL_GLOBAL_ONLY);
            if (! prompt) {
                if (! partial)  cout << "% " << flush;
            } else {
                if (Tcl_Eval(ip, TCL_CVBUG prompt) != TCL_OK)  {
                    cerr << Tcl_GetStringResult(ip) << endl;
                    Tcl_AddErrorInfo(ip,
                         TCL_CVBUG "\n    (script that generates prompt)");
                    if (! partial) cout << "% " << flush;
                } else {
                    ::fflush(stdout);
                }
            }

            // wait for stdin to have data
            W_COERCE(shared.read());
            DBG(<< "stdin is ready");
        }
        else {
            // cin.getline(shared.line, sizeof(shared.line) - 2);
			shared.read();
        }

        shared.line[sizeof(shared.line)-2] = '\0';
        strcat(shared.line, "\n");
        if ( !cin) {
            if (! partial)  {
                DBGTHRD("break: !cin && !partial" << shared.line);
                break;
            }
            shared.line[0] = '\0';
        }
        char* cmd = Tcl_DStringAppend(&buf, shared.line, -1);
        DBGTHRD("process_stdin: line is " << shared.line
            << "calling CommandComplete for " << cmd);
        if (shared.line[0] && ! Tcl_CommandComplete(cmd))  {
            DBG(<< "is partial");
            partial = 1;
            continue;
        }

        DBGTHRD("process_stdin:  complete: record and eval:" << cmd);
        partial = 0;
        int result = Tcl_RecordAndEval(ip, cmd, 0);
        DBGTHRD("Tcl_RecordAndEval returns:" << result);
        Tcl_DStringFree(&buf);
        if (result == TCL_OK)  {
            DBGTHRD("TCL_OK:" << Tcl_GetStringResult(ip));
            TCL_GETX char *r = Tcl_GetStringResult(ip);
            if (*r) {
            cout << r<< endl;
            }
        } else {
            TCL_GETX char *r = Tcl_GetStringResult(ip);
            if (result == TCL_ERROR && !strcmp(r, TCL_EXIT_ERROR_STRING))  {
            DBGTHRD("TCL_ERROR:" << r);
            break;
            }  else  {
                cerr << "process_stdin(): Error";
                if (result != TCL_ERROR) cerr << " " << result;
                if (*r) { cerr << ": " << r; }
                cerr << endl;
            }
        }
    }
}

void
copy_interp(Tcl_Interp *ip, Tcl_DString *vars, Tcl_DString *procs)
{
    if(tcl_init_cmd) {
        int result = Tcl_Eval(ip, TCL_CVBUG tcl_init_cmd);

        if (result != TCL_OK)  {
            cerr << "tcl_thread_t(): Error evaluating command:"
                << tcl_init_cmd <<endl ;
            if (result != TCL_ERROR) cerr << "     " << result;
            TCL_GETX char *r = Tcl_GetStringResult(ip);
            if (*r)  cerr << ": " << r;
            cerr << endl;
            w_assert3(0);;
        }
    }

    
    /* These three are done separately because 
     * they are done at a different time from create_stdcmd() 
     * in the initial (main() ) case
     */
    Tcl_CreateCommand(ip, TCL_CVBUG "vtable", vtable_dispatch, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "sm", sm_dispatch, 0, 0);
    Tcl_CreateCommand(ip, TCL_CVBUG "st", st_dispatch, 0, 0);

    create_stdcmd(ip); // creates more commands

    if(vars) {
        // add the variables
        int err = Tcl_Eval(ip, Tcl_DStringValue(vars));
        if (err != TCL_OK)  {
            cerr << __LINE__
                << " { set vars returns " <<  Tcl_GetStringResult(ip) 
                << "}" << endl << endl;
        }
    }
    if(procs) {
        // add the procs
        int err = Tcl_Eval(ip, Tcl_DStringValue(procs));
        if (err != TCL_OK)  {
            cerr << __LINE__
                << " { set vars returns " <<  Tcl_GetStringResult(ip) 
                << "}" << endl << endl;
        }
    }
    Tcl_ResetResult(ip);

    if(debug) {
        int err = Tcl_Eval(ip, TCL_CVBUG "info vars");
        if (err != TCL_OK)  {
            w_assert1(0);
        } else {
            cerr 
                << __LINE__
                << " { info vars returns " <<  Tcl_GetStringResult(ip) 
                << "}" << endl << endl;
        }
    }
}

void tcl_thread_t::run()
{
    {
    CRITICAL_SECTION(cs, thread_mutex);

    initialize();
    copy_interp(ip, parent_vars, parent_procs);
    if(debug) {
        int err = Tcl_Eval(ip, TCL_CVBUG "info vars");
        if (err != TCL_OK)  {
            w_assert1(0);
        } else {
            if(debug) {
                cerr << __LINE__ <<  " thread: " << name()
                << "{ info vars returns " <<  Tcl_GetStringResult(ip) 
                << "}" << endl << endl;
            }
        }
    }
    if(is_main_thread) 
    {
        w_assert1(parent_vars ==NULL);
        w_assert1(parent_procs ==NULL);
        // Only the main thread can start an sm and it
        // must be the first and last tcl_thread around.
        // So hanging onto this thread mutex should
        // not be an issue; there should be no
        // other tcl threads around until we are procesing
        // stdin (process_stdin(), below).
        w_assert0(thread_count == 1);
        if (thread_count == 1)  
        {
            assert(sm == 0);
            // Try without callback function.
            if(log_warn_callback) {
               sm = new ss_m(out_of_log_space, get_archived_log_file);
            } else {
               sm = new ss_m();
            }
            if (! sm)  {
                cerr << __FILE__ << ':' << __LINE__
                 << " out of memory" << endl;
                cs.exit();
                W_FATAL(fcOUTOFMEMORY);
            }
            W_COERCE( sm->config_info(global_sm_config_info));
        } else { 
            // There should be exactly ONE tcl_thread_t constructed this way
            w_assert1(0); 
        }
    } 
    else 
    {
        w_assert1(parent_vars != NULL);
        w_assert1(parent_procs != NULL);
        // copy vars and procs from parent to ip.  Now, we can't do this by 
        // invoking anything on the parent ip, so we had to
        // carry all this info around until now.
        Tcl_DStringFree(parent_vars);
        Tcl_DStringFree(parent_procs);
        delete parent_vars;
        delete parent_procs;
    }
    } // end critical section

    if (args) 
    {
        // This could be the main thread, in the case
        // of smsh -f <file>, in which case the main thread
        // has the args "source <file>..."
        // OR
        // this could be a thread created by t_fork_thread,
        // in which case it MUST have a procedure for its first argument


        int result = Tcl_Eval(ip, args);
        if (result != TCL_OK)  {
            cerr << "Error in tcl thread at " <<  __LINE__  
            << " in " << __FILE__
            << "\n args: " << args
            << "\n result: " << Tcl_GetStringResult(ip) << endl;
            cerr << "command: " << args <<endl;

            SSH_CHECK_STACK;

            if(debug) {
                int err = Tcl_Eval(ip, TCL_CVBUG "info vars");
                if (err != TCL_OK)  {
                    w_assert1(0);
                } else {
                    if(debug) {
                        cerr << __LINE__
                        << "{ info vars returns " <<  Tcl_GetStringResult(ip) 
                        << "}" << endl << endl;
                    }
                }
            }
        }
    } 
    else 
    {
        // Only the main thread can read from stdin
        w_assert1(is_main_thread);
        process_stdin(ip);
    }

    if (xct() != NULL) 
    {
        cerr << "Dying thread is running a transaction: aborting ..." << endl;
        w_rc_t rc = sm->abort_xct();
        if(rc.is_error()) {
            cerr << "Cannot abort tx : " << rc << endl;
            if(rc.err_num() == ss_m::eTWOTHREAD)  {
                me()->detach_xct(xct());
            }
        } else {
            cerr << "Transaction abort complete" << endl;
        }
    }
    
#if USE_BARRIER
#else
    {
    // post this in case someone is waiting for us when we die.  That will
    // unblock them, and we will be dead if they wait for us again.
        CRITICAL_SECTION(cslock, lock);
        hasExited = true;
        DO_PTHREAD(pthread_cond_signal(&quiesced));
    }
#endif

    if(is_main_thread) {
        CRITICAL_SECTION(cs, thread_mutex);
        // We can't get more than one main thread.
        // But a main thread can get here while others
        // are running if we didn't wait/join all the
        // forked threads.
        if (thread_count == 1)  {
            //COERCE(sm->dismount_all());
            delete sm;
            sm = 0;
        } else {
            fprintf(stderr, " AAAK thread count %d forked %d joined %d\n", 
                    thread_count, thread_forked, thread_joined);
            w_assert1(thread_count == 1);
            w_assert1(thread_forked == thread_joined);
        }
    }
    if(is_main_thread) {
        remove_all_barriers();
    }

    // Now, delete the interpreter here, since we cannot
    // do so in the calling thread ; we must do it in the
    // thread that created the ip.
    uninitialize();
}

void tcl_thread_t::join(unsigned long id)
{
    tcl_thread_t *r = find(id);
    if (r)  {
        w_rc_t rc = r->join();
        if(rc.is_error()) {
            w_reset_strstream(tclout); 
            tclout << rc << endl;
            fprintf(stderr, "\nJoin not allowed because: %s\nFix the script!\n", 
                    tclout.c_str());
         }
        delete r;
    }
}

void tcl_thread_t::sync_others(int n, unsigned long id[])
{
    tcl_thread_t **others = new tcl_thread_t *[n];
    for(int i=0; i < n; i++) {
        others[i] = find(id[i]);
    }
    sync_threads(n, others);
    delete[] others;
}

void tcl_thread_t::sync_threads(int n, tcl_thread_t *t[])
{
#if USE_BARRIER
#else
    bool *acquired = new bool[n];
#endif

    for(int i=0; i < n; i++) {
        tcl_thread_t *r = t[i];
#if USE_BARRIER
#else
        acquired[i] = false;
#endif
        if(r) {
#if USE_BARRIER
            DO_PTHREAD_BARRIER(pthread_barrier_wait(&r->mybarrier));
#else
            CRITICAL_SECTION(cslock, r->lock);
            if (r->status() != r->t_defunct)  {
                acquired[i]=true;
                while (!(r->isWaiting || r->hasExited)) {
                    DO_PTHREAD(pthread_cond_wait(&r->quiesced, &r->lock));
                }
                r->isWaiting = false;
            }
#endif
        }
    }
#if USE_BARRIER
#else
    for(int i=0; i < n; i++) {
        tcl_thread_t *r = t[i];
        if(r) {
            CRITICAL_SECTION(cslock, r->lock);
            if (r->status() != r->t_defunct)  {
                r->canProceed = true;
                int res = pthread_cond_signal(&r->proceed);
                if(res) {
                    W_FATAL_MSG(fcINTERNAL,  
                            << "Unexpected result from pthread_cond_signal "
                            << res);
                }
                DO_PTHREAD(pthread_mutex_unlock(&r->lock));
            } else if(acquired[i]) {
            }
            DO_PTHREAD( pthread_mutex_unlock(&r->lock) );
        }
    }
#endif
    for(int i=0; i < n; i++) t[i] = NULL;
}

void tcl_thread_t::sync_other(unsigned long id)
{
    tcl_thread_t *r = find(id);
    if(r) {
#if USE_BARRIER
        DO_PTHREAD_BARRIER(pthread_barrier_wait(&r->mybarrier));
#else
        CRITICAL_SECTION(cs, r->lock);
        if (r->status() != r->t_defunct)  {
            while (!(r->isWaiting || r->hasExited)) {
                DO_PTHREAD(pthread_cond_wait(&r->quiesced, &r->lock));
            }
            r->isWaiting = false;
            r->canProceed = true;
            DO_PTHREAD(pthread_cond_signal(&r->proceed));
        }
#endif
    }
}

void tcl_thread_t::sync()
{
#if USE_BARRIER
    DO_PTHREAD_BARRIER(pthread_barrier_wait(&mybarrier));
#else
    CRITICAL_SECTION(cs, lock);
    isWaiting = true;
    DO_PTHREAD( pthread_cond_signal(&quiesced) );
    while (!canProceed) {
        DO_PTHREAD(pthread_cond_wait(&proceed, &lock));
    }
    canProceed = false;
#endif
}

void
tcl_thread_t::set_args(
   TCL_AV char*    av[]
)
{
    // Find out how much space we need to allocate to hold the args
    unsigned int len;
    int i;
    for (i = 0, len = 0; i < ac; i++)
        len += strlen(av[i]) + 1;



    // args is thread datum holding the arguments to this
    // thread's creating function (e.g. t_fork_thread cmd)
    if (len) {
        if(args) { ::free(args); args=NULL; }
        // Must use malloc because we use strcat on this...
        args = (char *)::malloc(len+10);
        w_assert1(args);

        args[0] = '\0';
        for (i = 0; i < ac; i++) {
            strcat(args, av[i]);
            strcat(args, " ");
        }
        /*
        cerr << __LINE__ 
            << " ac " << ac
            << " len " << strlen(args) << " len  " << len 
            << " args=" << args
            << endl;
        */
        w_assert1(strlen(args) <= len);
    }
}

// Constructor used in smsh.cpp
tcl_thread_t::tcl_thread_t(
       int             _ac, 
       TCL_AV char*    av[],
       const  char*    _libdir,
       const  char*    _rcfile 
) : smthread_t(t_regular, 
    "main-tcl_thread", WAIT_FOREVER, sthread_t::default_stack * 2),
#if USE_BARRIER
#else
      isWaiting(false),
      canProceed(false),
      hasExited(false),
#endif
      lib_dir(_libdir),
      rcfile(_rcfile),
      args(0), // set by set_args
      ac(_ac),
      ip(0),
      // This is a hack, but it says that if we are creating this
      // thread from smsh.cpp, we are creating the "main tcl thread",
      // the one that reads from stdin.
      // All other tcl threads are created by the main thread (they cannot
      // themselves fork threads, although this might be permitted later)
      // via the fork_thread command (t_fork_thread)
      is_main_thread(true),
      parent_vars(0),
      parent_procs(0)
{
    atomic_inc(num_tcl_threads);
    atomic_inc(num_tcl_threads_ttl);

/*
cerr << __func__ << " " << __LINE__ << " " << __FILE__
                << " lib_dir " << (char *)(lib_dir ? lib_dir : "NULL")
                << " rcfile " << (char *)(rcfile ? rcfile : "NULL")
                << endl << flushl;
*/
    /* give thread a unique name, not just "tcl_thread" */
    {
        w_ostrstream_buf    o(40);        // XXX magic number
        W_FORM2(o,("tcl_thread(%d)-MAIN", id));
        o << ends;
        rename(o.c_str());
    }
    set_args(av);
    w_assert1(ip==0); // can't create an interpreter until run() 
    { 
        CRITICAL_SECTION (cs, thread_mutex);
        // This is a non-main thread.  One that does
        // not read from stdin.
        ++thread_count;
        w_assert0(thread_count == 1);
        threadslist.push(this);
    }

#if USE_BARRIER
    DO_PTHREAD(pthread_barrier_init(&mybarrier, NULL, 2));
#else
    DO_PTHREAD(pthread_cond_init(&quiesced, NULL));
    DO_PTHREAD(pthread_cond_init(&proceed, NULL));
    DO_PTHREAD(pthread_mutex_init(&lock, NULL));
#endif
}

/* The increased stack size is because TCL is stack hungry */
tcl_thread_t::tcl_thread_t(
       int             _ac, 
       TCL_AV char*    av[],
       Tcl_DString*    _parent_vars, // procs
       Tcl_DString*    _parent_procs // procs
) : smthread_t(t_regular, 
    "tcl_thread", WAIT_FOREVER, sthread_t::default_stack * 2),
#if USE_BARRIER
#else
      isWaiting(false),
      canProceed(false),
      hasExited(false),
#endif
      lib_dir(0),
      rcfile(0),
      args(0), // set by set_args
      ac(_ac),
      ip(0),
      // This is a hack, but it says that if we are creating this
      // thread from smsh.cpp, we are creating the "main tcl thread",
      // the one that reads from stdin.
      // All other threads are created by the main thread (they cannot
      // themselves fork threads, although this might be permitted later)
      // via the fork_thread command (t_fork_thread)
      is_main_thread(false),
      parent_vars(_parent_vars),
      parent_procs(_parent_procs)
{
    atomic_inc(num_tcl_threads);
    atomic_inc(num_tcl_threads_ttl);
    /* give thread a unique name, not just "tcl_thread" */
    {
        w_ostrstream_buf    o(40);        // XXX magic number
        W_FORM2(o,("tcl_thread(%d)-forked", id));
        o << ends;
        rename(o.c_str());
    }
    set_args(av);
    w_assert1(ip==0);

    { 
        CRITICAL_SECTION (cs, thread_mutex);
        // This is a non-main thread.  One that does
        // not read from stdin.
        ++thread_count;
        w_assert0(thread_count > 1);
        threadslist.push(this);
    }


#if USE_BARRIER
    DO_PTHREAD(pthread_barrier_init(&mybarrier, NULL, 2));
#else
    DO_PTHREAD(pthread_cond_init(&quiesced, NULL));
    DO_PTHREAD(pthread_cond_init(&proceed, NULL));
    DO_PTHREAD(pthread_mutex_init(&lock, NULL));
#endif
}

tcl_thread_t::~tcl_thread_t()
{
    atomic_dec(num_tcl_threads);

    w_assert1(ip==NULL);

    // arguments to the t_fork_thread that created this thread
    if(args) {
        ::free(args);
        args=0;
    }

    { 
        CRITICAL_SECTION (cs, thread_mutex);
        --thread_count ;
        w_assert0(thread_count >= 0);
        if(is_main_thread)  w_assert0(thread_count == 0);
        link.detach();
    }
#if USE_BARRIER
    DO_PTHREAD(pthread_barrier_destroy(&mybarrier));
#else
    DO_PTHREAD(pthread_cond_destroy(&quiesced));
    DO_PTHREAD(pthread_cond_destroy(&proceed));
    DO_PTHREAD(pthread_mutex_destroy(&lock));
#endif
}

extern bool instrument; // smsh.cpp
extern bool verbose; // smsh.cpp
extern bool verbose2; // smsh.cpp
extern bool force_compress; // smsh.cpp
extern bool log_warn_callback; // smsh.cpp 

// Invoked for all threads, but it
// continues ONLY for the main/global ip
void tcl_thread_t::uninitialize()
{
    if (ip) {
        Tcl_DeleteInterp(ip);
        ip = 0;
    }
    // In case Tcl has some cleaning up to do
    clean_up_shell();
}

void tcl_thread_t::initialize(
    // TCL_AV char* av[]
)
{

    // Create tcl interpreter; assign to global_ip only 
    // for debugger
    global_ip = 
        ip = Tcl_CreateInterp();

    if (!ip)
        W_FATAL(fcOUTOFMEMORY);

    if(!lib_dir) return;
    // Only main thread has a lib_dir

    // default is to not use logical IDs
    Tcl_SetVar(ip, TCL_CVBUG Logical_id_flag_tcl, 
            TCL_CVBUG tcl_form_flag(0), TCL_GLOBAL_ONLY);

    w_ostrstream_buf s(ss_m::max_devname + 1);
    s << lib_dir << "/smsh.tcl" << ends;
    if (Tcl_EvalFile(ip, TCL_CVBUG s.c_str()) != TCL_OK)  {
        cerr << __FILE__ << ':' << __LINE__ << ':'
         << " error in \"" << s.c_str() << "\" script" << endl;
        cerr << Tcl_GetStringResult(ip) << endl;
        W_FATAL(fcINTERNAL);
    }

    create_stdcmd(ip);

    {
        // read .smshrc
        Tcl_DString buf;
        char* rcfilename = Tcl_TildeSubst(ip, rcfile, &buf);
        if (rcfilename)  {
            FILE* f;
            f = fopen(rcfilename, "r");
            if (f) {
                if (Tcl_EvalFile(ip, rcfilename) != TCL_OK) {
                    cerr << Tcl_GetStringResult(ip)<< endl;
                    w_assert1(0);
                    return;
                }
                fclose(f);
            } else {
                cerr << "WARNING: could not open rc file: " 
                    << rcfilename << endl;
            }
        }
        Tcl_DStringFree(&buf);
    }

    Tcl_SetVar(ip, TCL_CVBUG "log_warn_callback_flag",
        TCL_SETV2 tcl_form_flag(log_warn_callback), TCL_GLOBAL_ONLY);
#if 1

    Tcl_SetVar(ip, TCL_CVBUG "compress_flag",
        TCL_SETV2 tcl_form_flag(force_compress), TCL_GLOBAL_ONLY);

    Tcl_SetVar(ip, TCL_CVBUG "instrument_flag",
           TCL_SETV2 tcl_form_flag(instrument), TCL_GLOBAL_ONLY);

    Tcl_SetVar(ip, TCL_CVBUG "verbose_flag",
           TCL_SETV2 tcl_form_flag(verbose), TCL_GLOBAL_ONLY);
    Tcl_SetVar(ip, TCL_CVBUG "verbose2_flag",
           TCL_SETV2 tcl_form_flag(verbose2), TCL_GLOBAL_ONLY);
#endif


    Tcl_SetVar(ip, TCL_CVBUG "argv", args, TCL_GLOBAL_ONLY);
    {
        w_ostrstream_buf otmp(100);        // XXX magic number
        otmp << ac << ends;
        Tcl_SetVar(ip, TCL_CVBUG "argc", otmp.c_str(), TCL_GLOBAL_ONLY);
    }

#if 0
    {
        const char *f_arg = (ac > 0)?  av[1] : argv0;
        Tcl_SetVar(ip, TCL_CVBUG "argv0", TCL_CVBUG f_arg, TCL_GLOBAL_ONLY);
    }
#endif

    Tcl_SetVar(ip, TCL_CVBUG "tcl_interactive",
           TCL_CVBUG (tcl_form_flag(interactive)), TCL_GLOBAL_ONLY);

#if 0
    if(debug)
    {
        Tcl_DString* procs = new Tcl_DString;
        Tcl_DString* vars = new Tcl_DString;
        grab_vars(ip,  vars);
        grab_procs(ip, procs);
        Tcl_DStringFree(procs);
        Tcl_DStringFree(vars);
        delete procs;
        delete vars;
        std::cerr << "                INITIALIZE CHECK OK" << endl;
        Tcl_Eval(ip, TCL_CVBUG "interp recursionlimit");
        std::cerr << "                recursionlimit "
                << Tcl_GetStringResult(ip)
            << endl;
    }
#endif
    {
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "sm_page_sz", (char*)&linked.sm_page_sz, TCL_LINK_INT);
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "sm_max_exts", (char*)&linked.sm_max_exts, TCL_LINK_INT);
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "sm_max_vols", (char*)&linked.sm_max_vols, TCL_LINK_INT);
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "sm_max_servers", (char*)&linked.sm_max_servers, TCL_LINK_INT);
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "sm_max_keycomp", (char*)&linked.sm_max_keycomp, TCL_LINK_INT);
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "sm_max_dir_cache", (char*)&linked.sm_max_dir_cache, TCL_LINK_INT);
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "sm_max_rec_len", (char*)&linked.sm_max_rec_len, TCL_LINK_INT);
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "sm_srvid_map_sz", (char*)&linked.sm_srvid_map_sz, TCL_LINK_INT);

    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "instrument_flag", (char*)&linked.instrument_flag, TCL_LINK_INT);
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "verbose_flag", (char*)&linked.verbose_flag, TCL_LINK_INT);
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "verbose2_flag", (char*)&linked.verbose2_flag, TCL_LINK_INT);
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "compress_flag", (char*)&linked.compress_flag, TCL_LINK_INT);
    (void) Tcl_LinkVar(ip, 
            TCL_CVBUG "log_warn_callback_flag", 
            (char*)&linked.log_warn_callback_flag, TCL_LINK_INT);
    /*
    if (Tcl_AppInit(ip) != TCL_OK)  {
    cerr << "Tcl_AppInit failed: " << Tcl_GetStringResult(ip)
         << endl;
    }
    */
    }
}

tcl_thread_t    *tcl_thread_t::find(unsigned long id)
{
    CRITICAL_SECTION(cs, thread_mutex);
    w_list_i<tcl_thread_t, queue_based_block_lock_t>    i(threadslist);
    tcl_thread_t        *r;
    while ((r = i.next()) && (r->id != id))
        ;
    return r;
}
