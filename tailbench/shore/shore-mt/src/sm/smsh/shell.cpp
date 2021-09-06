/*<std-header orig-src='shore'>

 $Id: shell.cpp,v 1.335 2010/05/26 01:20:51 nhall Exp $

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

#define SHELL_C


#include "shell.h"

#include <cstdio>
#include <fstream>
#include <w_strstream.h>
// include strings.h for strcasecmp and its ilk
#include <strings.h>
#include <ctype.h>

#ifdef __GNUG__
#define        IOS_FAIL(stream)        stream.setstate(ios::failbit)
#else
#define        IOS_FAIL(stream)        stream.setstate(ios_base::failbit)
#endif


// NOTE: now each thread has its own generator; this might not be what
// we want, but for now such is the case, until we have a MT-safe/reentrant
// generator.   
__thread rand48*       generatorp(NULL);
__thread char *        resultcharbufp(NULL);
__thread vec_t *       resultvecp(NULL);
__thread sort_stream_i* sortcontainerp(NULL);
/* XXX magic number */
__thread char   outbuf[(ss_m::page_sz * 4) > (1024 * 16) ? 
            (ss_m::page_sz * 4) : (1024 * 16) ];
__thread w_ostrstream *tcloutp(NULL);

rand48 &get_generator() {
     if (generatorp == NULL) {
         generatorp = new rand48;
     }
     return *generatorp;
}

char *get_resultcharbuf() {
     if (resultcharbufp == NULL) {
         resultcharbufp = new char[100];
     }
     return resultcharbufp;
}

vec_t &get_resultvec() {
     if (resultvecp == NULL) {
         resultvecp = new vec_t;
     }
     return *resultvecp;
}

sort_stream_i *get_sort_container() {
     return sortcontainerp;
}

sort_stream_i *replace_sort_container( 
         sort_stream_i *replacement ) {
     if (sortcontainerp != NULL) {
                 delete sortcontainerp;
     }
         sortcontainerp = replacement;
         return sortcontainerp;
}

w_ostrstream &get_tclout() {
     if (tcloutp == NULL) {
         tcloutp = new w_ostrstream(outbuf, sizeof(outbuf));
     }
     return *tcloutp;
}

void clean_up_shell()
{
// TLS
        if(generatorp != NULL) delete generatorp;
        generatorp = NULL;

// TLS
        if(resultcharbufp != NULL) delete [] resultcharbufp;
        resultcharbufp = NULL;

// TLS
        if(resultvecp != NULL) delete resultvecp;
        resultvecp = NULL;

// TLS
        if(sortcontainerp != NULL) delete sortcontainerp;
        sortcontainerp = NULL;

// TLS
        if(tcloutp != NULL) delete tcloutp;
        tcloutp = NULL;
}

/* ************************** __thread-local stuff *******************/
#if defined(W_DEBUG) || defined(SSH_DUMPRC)
// Set to true if you want every occurence of rc_t error
// to get output to stderr
/* XXX it would be nice if this was controllable from smsh for non-debug
   sessions. */
#ifdef SSH_DUMPRC
#define DEFAULT_DUMPRC        true
#else
#define        DEFAULT_DUMPRC        false
#endif
bool dumprc =  DEFAULT_DUMPRC;
#endif

const char*
cvt_store_t(ss_m::store_t n)
{
    switch(n) {
    case ss_m::t_bad_store_t:
        return "t_bad_store_t";
    case ss_m::t_index:
        return "t_index";
    case ss_m::t_file:
        return "t_file";
    case ss_m::t_lgrec:
        return "t_lgrec";
    }
    return "unknown";
}
const char*
cvt_ndx_t(ss_m::ndx_t n)
{
    switch(n) {
    case ss_m::t_btree:
        return "t_btree";
    case ss_m::t_uni_btree:
        return "t_uni_btree";
    case ss_m::t_rtree:
        return "t_rtree";
    case ss_m::t_bad_ndx_t:
        return "t_bad_ndx_t";
    }
    return "unknown";
}
const char*
cvt_concurrency_t( ss_m::concurrency_t cc)
{
    switch(cc) {
    case ss_m::t_cc_none:
        return "t_cc_none";
    case ss_m::t_cc_record:
        return "t_cc_record";
    case ss_m::t_cc_page:
        return "t_cc_page";
    case ss_m::t_cc_file:
        return "t_cc_file";
    case ss_m::t_cc_vol:
        return "t_cc_vol";
    case ss_m::t_cc_kvl:
        return "t_cc_kvl";
    case ss_m::t_cc_modkvl:
        return "t_cc_modkvl";
    case ss_m::t_cc_im:
        return "t_cc_im";
    case ss_m::t_cc_append:
        return "t_cc_append";
    case ss_m::t_cc_bad: 
        return "t_cc_bad";
    }
    return "unknown";
}

const char *check_compress_flag(const char *kd)
{
    if(!force_compress) {
        return kd;
    }
        char *_result = get_resultcharbuf();
    switch(kd[0]) {
        case 'b':
                _result[0] = 'B';
                break;
        case 'i':
                _result[0] = 'I';
                break;
        case 'u':
                _result[0] = 'U';
                break;
        case 'f':
                _result[0] = 'F';
                break;
        default:
                _result[0] = kd[0];
                break;
    }
    memcpy(&_result[1], &kd[1], strlen(kd));
    return _result;
}

// Used only when we're doing physical-id sm calls
vid_t 
make_vid_from_lvid(const char* lv)
{
    // see if the lvid string represents a long lvid)
    if (!strchr(lv, '.')) {
        vid_t vid;
        w_istrstream anon(lv);
        anon >> vid;

        return vid;
    }
    uint2_t vid;
    w_istrstream anon(lv); anon >> vid;
    std::cerr << "Trying to create vid from " << lv 
    << " result=" << vid
    << std::endl;
    return vid_t(vid);
}

int        tcl_scan_bool(const char *rep, bool &result)
{
        if (strcasecmp(rep, "true") == 0) {
                result = true;
                return TCL_OK;
        }

        if (strcasecmp(rep, "false") == 0) {
                result = false;
                return TCL_OK;
        }

        return TCL_ERROR;
}


int        tcl_scan_flag(const char *s, bool &result)
{
        if (strcasecmp(s, "on") == 0 || strcmp(s, "1") == 0
            || strcasecmp(s, "yes") == 0) {
                result = true;
                return TCL_OK;
        }

        if (strcasecmp(s, "off") == 0 || strcmp(s, "0") == 0
                 || strcasecmp(s, "no") == 0) {
                 result = false;
                 return TCL_OK;
        }


        return TCL_ERROR;
}


ss_m::ndx_t cvt2ndx_t(const char *s); //forward ref

void
cvt2lockid_t(const char *str, lockid_t &n)
{
    stid_t stid;
    lpid_t pid;
    rid_t rid;
    kvl_t kvl;
    vid_t vid;
    extid_t extid;
    lockid_t::user1_t u1;
    lockid_t::user2_t u2;
    lockid_t::user3_t u3;
    lockid_t::user4_t u4;

    while(*str  && isspace(*str)) str++;

    char      dummychar;
    int len = strlen(str);

    w_istrstream ss(str, len);

    /* This switch conversion is used, because the previous,
       "try everything" was causing problems with I/O streams.
       It's better anyway, because it is deterministic instead
       of priority ordered. */

    switch (str[0]) {
    case ' ':
    case '\t':
    case '\n':
            ss >> dummychar;
            break;
    case 's':
            ss >> stid;
            break;
    case 'x':
            ss >> extid;
            break;
    case 'r':
            ss >> rid;
            break;
    case 'p':
            ss >> pid;
            break;
    case 'k':
            ss >> kvl;
            break;
    case 'u':
            if (len < 2)  {
                    IOS_FAIL(ss);
                break;
            }
            switch (str[1])  {
                case '1':
                    ss >> u1;
                    break;
                case '2':
                    ss >> u2;
                    break;
                case '3':
                    ss >> u3;
                    break;
                case '4':
                    ss >> u4;
                    break;
                default:
                    IOS_FAIL(ss);
                    break;
            }
            break;
    default:
            /* volume id's are just XXX.YYY.  They should be changed  */
            ss >> vid;
            break;
    }

    if (!ss) {
            cerr << "cvt2lockid_t: bad lockid -- " << str << endl;
            abort();
            return; 
    }

    switch (str[0]) {
    case 's':
            n = lockid_t(stid);
            break;
    case 'r':
            n = lockid_t(rid);
            break;
    case 'p':
            n = lockid_t(pid);
            break;
    case 'x':
            n = lockid_t(extid);
            break;
    case 'k':
            n = lockid_t(kvl);
            break;
    case 'u':
            switch (str[1])  {
                case '1':
                    n = lockid_t(u1);
                    break;
                case '2':
                    n = lockid_t(u2);
                    break;
                case '3':
                    n = lockid_t(u3);
                    break;
                case '4':
                    n = lockid_t(u4);
                    break;
            }
            break;
    default:
            n = lockid_t(vid);
            break;
    }
    
    return;
}

bool 
use_logical_id(Tcl_Interp*)
{
    return false;
}

/*
 * A note on the pointers passed to the TCL code.
 *
 * The c/c++ code generates "textual tokens" which refer to pointers.
 * The actual representation of these tokens is the numeric value
 * of the pointer.  When the TCL code wants to refer to the object,
 * it returns that token (aka pointer) to the c/c++ code where it
 * is converted back to a pointer of the proper type.
 *
 * Marshalling the pointers out as some sort of type-safe representation
 * woul be a cool idea, and would hopefully reduce bugs.  However, the tcl
 * code  assumes that it can perform numeric comparisons of pointer values,
 * and that it can generate its own pointer values.  In particular, it looks
 * for and generates a '0' representation for null pointers to check
 * for the existence of returned objects, AND to indicate it has nothing
 * when using the c/c++ primitives.
 *
 * This scheme works OK, but problems arose with Visual C++ 7.0, which
 * uses 0-padded hexadecimal output WITHOUT a base indicator for
 * output pointer values.  Depending upon the value of the pointer,
 * and what hexadecimal digits it contains, it can be interpreted by
 * strtol() as either hex, decimal, or octal.  Since the converted objects
 * are pointers, this is bad.
 *
 * You may ask "Why is this not simply a operator<<(stream,const type *)
 *  instead of this name?  The reason is that this is used for a specific
 *  reason -- to marshall pointers to a textual representation to be 
 *  returned to TCL land.  By calling this named function sematics are
 *  guaranteed.  Otherwise, there is no guarantee of what is happening
 *  to the output.
 */

ostream &format_ptr(ostream &o, const void *ptr)
{
    // ensure a numeric value for 0 -- some use '(nil)'
    // XXX could force base to match other pointers -- 0x0
    if (!ptr)
            return o << "0";
            
    return o << ptr;
}

/*
 * Output a format with a NULL tag for null pointers instead of 0.
 * In TCL-land this would indicate that it is a placeholder and
 * will not be used.
 */

ostream &format_ptr_null(ostream &o, const void *ptr)
{
        if (!ptr)
                return o << "NULL";
        return format_ptr(o, ptr);
}



static int read_ptr(Tcl_Interp *, const char *s, void *&ptr)
{
    char             *t;
    long long        l = -1;
    bool             ok = true;


    // skip leading white space
    while(isspace(*s)) s++; 

    l = strtoll(s, &t, 16);

    // skip trailing white space
    while(isspace(*t)) t++; 

    // if it didn't scan completely something is wrong
    ok = !(s == t || *t != 0);

    // try it again with base 10 just in case.
    if (!ok)
            l = strtoll(s, &t, 10);

    // if it didn't scan completely something is wrong
    ok = !(s == t || *t != 0);

    ptr = (void *) (ok ? l : 0);

    return ok ? 0 : -1;
}

#if 1 
/* This is templated to allow for possible labelling of pointers by
   type in the future.  It also takes care of casting errors in some
   compilers. */

#define        READ_PTR(TYPE)        \
        int read_ptr(Tcl_Interp *ip, const char *s, TYPE *&ptr)        \
        {                                                        \
                return read_ptr(ip, s, (void *&) ptr);                \
        }

READ_PTR(xct_t)
READ_PTR(sm_quark_t)
READ_PTR(pin_i)
READ_PTR(scan_index_i)
READ_PTR(scan_file_i)
READ_PTR(append_file_i)
#endif

vec_t &
parse_vec(const char *c, int len) 
{
    vec_t &junk(get_resultvec());

    smsize_t         i;

    junk.reset();
    if(::strncmp(c, "zvec", 4)==0) {
        c+=4;
        i = objectsize(c);
        const zvec_t i_zvec_tmp(i);
        junk.set(i_zvec_tmp);
        // DBG(<<"returns zvec_t("<<i<<")");
            return junk;
    }
    // DBG(<<"returns normal vec_t()");
    junk.put(c, len);
    return junk;
}

static int
t_checkpoint(Tcl_Interp* ip, int ac, TCL_AV char* /*av*/[])
{
    if (check(ip, "", ac, 1))  return TCL_ERROR;
    DO(sm->checkpoint());
    return TCL_OK;
}

static int
t_sleep(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "milliseconds", ac, 2))  return TCL_ERROR;
    int timeout = atoi(av[1]);
    me()->sleep(timeout);
    return TCL_OK;
}


static int
t_lock_timeout(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "milliseconds", ac, 2)) return TCL_ERROR;
    int timeout = atoi(av[1]);
    me()->lock_timeout(timeout);
    return TCL_OK;
}

static int
t_begin_xct(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "[lock timeout in ms]", ac, 1, 2)) 
        return TCL_ERROR;
    int timeout=0;
    if (ac == 2) timeout = atoi(av[1]);

    /* Instrument all our xcts */
    sm_stats_info_t *s = new sm_stats_info_t;
    memset(s, '\0', sizeof(sm_stats_info_t));
    if(timeout > 0) {
        me()->lock_timeout(timeout);
        DO( sm->begin_xct(s, WAIT_SPECIFIED_BY_THREAD) );
    } else
    {
        DO( sm->begin_xct(s) );
    }
    return TCL_OK;
}

static int
t_commit_xct(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "[lazy]", ac, 1, 2)) return TCL_ERROR;
    bool lazy = false;
    if (ac == 2) lazy = (strcmp(av[1], "lazy") == 0);
    sm_stats_info_t *s=0;
    DO( sm->commit_xct(s, lazy) );
    /*
     * print stats
     */
    if(linked.instrument_flag && s) {
        w_reset_strstream(tclout);
        tclout << *s << ends;
    }
    delete s;
    return TCL_OK;
}

static  int
t_chain_xct(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "[lazy]", ac, 1, 2))  return TCL_ERROR;
    bool lazy = false;
    if (ac == 2) lazy = (strcmp(av[1], "lazy") == 0);
    /*
     * For chain, we have to hand in a new struct,
     * and we get back the old one.
     */
    sm_stats_info_t *s = new sm_stats_info_t;
    memset(s, '\0', sizeof(sm_stats_info_t));
    DO( sm->chain_xct(s, lazy) );
    /*
     * print stats
     */
    if(linked.instrument_flag && s) {
        w_reset_strstream(tclout);
        tclout << *s << ends;
    }
    delete s;
    return TCL_OK;
}

static int
t_set_coordinator(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{

    if (check(ip, "handle", ac, 2)) return TCL_ERROR;
    server_handle_t        h(av[1]);
    DO( sm->set_coordinator(h));
    return TCL_OK;
}

static int
t_enter2pc(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "gtid_t", ac, 2)) return TCL_ERROR;
    gtid_t        g(av[1]);
    DO( sm->enter_2pc(g));
    return TCL_OK;
}

static int
t_recover2pc(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "gtid_t", ac, 2)) return TCL_ERROR;

    gtid_t        g(av[1]);
    tid_t                 t;
    DO( sm->recover_2pc(g, true, t));
    w_reset_strstream(tclout);
    tclout << t << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_num_active(Tcl_Interp* ip, int ac, TCL_AV char* [])
{
    if (check(ip, "", ac, 1)) return TCL_ERROR;

    int num = ss_m::num_active_xcts();
    w_reset_strstream(tclout);
    tclout << num << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_num_prepared(Tcl_Interp* ip, int ac, TCL_AV char*[])
{
    if (check(ip, "", ac, 1)) return TCL_ERROR;

    int numu=0, num=0;
    DO( sm->query_prepared_xct(num));
    num += numu ;
    w_reset_strstream(tclout);
    tclout << num << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_prepare_xct(Tcl_Interp* ip, int ac, TCL_AV char* [])
{
    if (check(ip, "", ac, 1)) return TCL_ERROR;
    sm_stats_info_t *s=0;
    vote_t        v;
    DO( sm->prepare_xct(s, v));
    /*
     * print stats
     */
    if(linked.instrument_flag && s) {
        w_reset_strstream(tclout);
        tclout << *s << ends;
    }
    delete s;

    w_reset_strstream(tclout);
    tclout << W_ENUM(v) << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

int
t_abort_xct(Tcl_Interp* ip, int ac, TCL_AV char* [])
{
    if (check(ip, "", ac, 1)) return TCL_ERROR;
    sm_stats_info_t *s=0;
    DO( sm->abort_xct(s));
    /*
     * print stats
     */
    if(linked.instrument_flag && s) {
        w_reset_strstream(tclout);
        tclout << *s << ends;
    }
    delete s;
    return TCL_OK;
}

static int
t_save_work(Tcl_Interp* ip, int ac, TCL_AV char* [])
{
    if (check(ip, "", ac, 1))  return TCL_ERROR;
    sm_save_point_t sp;
    DO( sm->save_work(sp) );
    w_reset_strstream(tclout);
    tclout << sp << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_rollback_work(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "savepoint", ac, 2))  return TCL_ERROR;
    sm_save_point_t sp;

    w_istrstream anon(av[1]); anon >> sp;
    DO( sm->rollback_work(sp));

    return TCL_OK;
}

static int
t_open_quark(Tcl_Interp* ip, int ac, TCL_AV char* [])
{
    if (check(ip, "", ac, 1))  return TCL_ERROR;

    sm_quark_t *quark= new sm_quark_t;
    DO(quark->open());
    w_reset_strstream(tclout);
    format_ptr(tclout, quark) << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_close_quark(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "close_quark quark", ac, 2, 2))  return TCL_ERROR;

    sm_quark_t *quark = 0;
    if (read_ptr(ip, av[1], quark)) return TCL_ERROR;

    DO(quark->close());
    delete quark;
    return TCL_OK;
}

static int
t_xct(Tcl_Interp* ip, int ac, TCL_AV char* [])
{
    if (check(ip, "", ac, 1))  return TCL_ERROR;

    w_reset_strstream(tclout);
    format_ptr(tclout, me()->xct()) << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    // fprintf(stderr, "%d %p me()->xct()returns %p\n", me()->id, me(), me()->xct());
    // w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_attach_xct(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "xct", ac, 2))
        return TCL_ERROR;

    xct_t *x = 0;
    if (read_ptr(ip, av[1], x)) {
        // fprintf(stderr, "%d %p t_ATTACH failed %s\n", me()->id, me(), av[1]);
        return TCL_ERROR;
    }

    // fprintf(stderr, "%d %p t_ATTACHing %p\n", me()->id, me(), x);
    me()->attach_xct(x);
    // fprintf(stderr, "%d %p t_ATTACH_xct returns %p\n", me()->id, me(), me()->xct());
    return TCL_OK;
}

static int
t_detach_xct(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "xct", ac, 2))
        return TCL_ERROR;
    
    xct_t *x = 0;
    if (read_ptr(ip, av[1], x)) return TCL_ERROR;

    // fprintf(stderr, "%d %p t_DETACHing %p\n", me()->id, me(), x);
    me()->detach_xct(x);
    // fprintf(stderr, "t_DETACH_xct me()->xct() returns %p\n", me()->xct() );
    return TCL_OK;
}

static int
t_state_xct(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "xct", ac, 2))
        return TCL_ERROR;

    xct_t *x = 0;
    if (read_ptr(ip, av[1], x)) return TCL_ERROR;

    ss_m::xct_state_t t = sm->state_xct(x);

    w_reset_strstream(tclout);
    switch(t) {
        case ss_m::xct_stale:
                tclout << "xct_stale" << ends;
                break;
        case ss_m::xct_active:
                tclout << "xct_active" << ends;
                break;
        case ss_m::xct_prepared:
                tclout << "xct_prepared" << ends;
                break;
        case ss_m::xct_aborting:
                tclout << "xct_aborting" << ends;
                break;
        case ss_m::xct_chaining:
                tclout << "xct_chaining" << ends;
                break;
        case ss_m::xct_committing: 
                tclout << "xct_committing" << ends;
                break;
        case ss_m::xct_freeing_space: 
                tclout << "xct_freeing_space" << ends;
                break;
        case ss_m::xct_ended:
                tclout << "xct_ended" << ends;
                break;
    };
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_tid_to_xct(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "xct", ac, 2))
        return TCL_ERROR;

        tid_t t;
        w_istrstream anon(av[1]); anon >> t;
    
    xct_t* x = sm->tid_to_xct(t);
    w_reset_strstream(tclout);
    format_ptr(tclout, x) << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}
static int
t_xct_to_tid(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "xct", ac, 2))
        return TCL_ERROR;

    xct_t *x = 0;
    if (read_ptr(ip, av[1], x)) return TCL_ERROR;

    if(x) {
        tid_t t = sm->xct_to_tid(x);
        w_reset_strstream(tclout);
        tclout << t << ends;
        Tcl_AppendResult(ip, tclout.c_str(), 0);
        return TCL_OK;
    } else {
        Tcl_AppendResult(ip, "Cannot take xct_to_tid of null xct", 0);
        return TCL_ERROR;
    }
}

extern "C" void print_all_latches();
static int
t_dump_latches(Tcl_Interp* ip, int ac, TCL_AV char*[])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;
    
    print_all_latches();
    return TCL_OK;
}

static int
t_dump_xcts(Tcl_Interp* ip, int ac, TCL_AV char*[])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;
    
    DO( sm->dump_xcts(cout) );
    return TCL_OK;
}

extern "C" void clear_all_fingerprints();
static int
t_reinit_fingerprints(Tcl_Interp* ip, int ac, TCL_AV char*[])
{
    if (check(ip, "", ac, 1)) return TCL_ERROR;
    clear_all_fingerprints();
    return TCL_OK;
}
static int
t_force_buffers(Tcl_Interp* ip, int ac, TCL_AV char*[])
{
    if (check(ip, "[flush]", ac, 1,2)) return TCL_ERROR;
    bool flush = false;
    if (ac == 2) {
        flush = true;
    }
    DO( sm->force_buffers(flush) );
    return TCL_OK;
}
 

static int
t_force_vol_hdr_buffers(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "lvid", ac, 2)) return TCL_ERROR;
    // keep compiler quiet about unused parameters
    if (av) {}

    vid_t vid;
    w_istrstream anon(av[1]); anon >> vid;
    DO( sm->force_vol_hdr_buffers(vid) );
    return TCL_OK;
}
 

static int
t_snapshot_buffers(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;
    // keep compiler quiet about unused parameters
    if (av) {}

    u_int ndirty, nclean, nfree, nfixed;
    DO( sm->snapshot_buffers(ndirty, nclean, nfree, nfixed) );

    w_reset_strstream(tclout);
    tclout << "ndirty " << ndirty 
      << "  nclean " << nclean
      << "  nfree " << nfree
      << "  nfixed " << nfixed
      << ends;
    
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_mem_stats(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "[reset]", ac, 1,2))
        return TCL_ERROR;

    bool reset = false;
    if (ac == 2) {
        if (strcmp(av[1], "reset") == 0) {
            reset = true;
        }
    }

#if DEAD
    w_reset_strstream(tclout);
    {
        size_t amt=0, num=0, hiwat;
        w_shore_alloc_stats(amt, num, hiwat); // TODO: some replacement?
        tclout << amt << " bytes allocated in main heap in " 
                << num
                << " calls, high water mark= " << hiwat
                << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
#endif
    return TCL_OK;
}

static int
t_gather_xct_stats(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "[reset]", ac, 1,2))
        return TCL_ERROR;

    bool reset = false;
    if (ac == 2) {
        if (strcmp(av[1], "reset") == 0) {
            reset = true;
        }
    }
    {
        sm_stats_info_t* stats = new sm_stats_info_t;
        w_auto_delete_t<sm_stats_info_t>         autodel(stats);

        sm_stats_info_t& internal = *stats; 
        // internal gets initalized by gather_xct_stats
        DO( sm->gather_xct_stats(internal, reset));

        w_reset_strstream(tclout);
        tclout << internal << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_gather_stats(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "[reset]", ac, 1,2))
        return TCL_ERROR;

    bool reset = false;
    if (ac == 2) {
        if (strcmp(av[1], "reset") == 0) {
            reset = true;
        }
    }

    w_reset_strstream(tclout);
    {
        static sm_stats_info_t last_time;
        static sm_stats_info_t curr;

        sm_stats_info_t* stats = new sm_stats_info_t;
        w_auto_delete_t<sm_stats_info_t>         autodel(stats);
        sm_stats_info_t& diff = *stats; 

        // curr gets initalized by gather_stats
        DO( sm->gather_stats(curr));

        diff = curr;
        // last_time starts out all zeroes.
        diff -= last_time;
        if(reset) {
            // Save values at the last reset.
            last_time = curr;
        }
        tclout << diff << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

sm_config_info_t global_sm_config_info;

static int
t_config_info(Tcl_Interp* ip, int ac, TCL_AV char*[])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;


    DO( sm->config_info(global_sm_config_info));

    w_reset_strstream(tclout);
    tclout << global_sm_config_info << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_set_disk_delay(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "milli_sec", ac, 2))
        return TCL_ERROR;

    uint4_t msec = atoi(av[1]);
    DO( sm->set_disk_delay(msec));

    return TCL_OK;
}

static int
t_start_log_corruption(Tcl_Interp* ip, int ac, TCL_AV char* [])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;

    DO( sm->start_log_corruption());

    return TCL_OK;
}

static int
t_sync_log(Tcl_Interp* ip, int ac, TCL_AV char* [])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;

    DO( sm->sync_log());

    return TCL_OK;
}

extern "C" void check_disk(const vid_t &vid);
static int
t_check_volume(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "vid", ac, 2))
        return TCL_ERROR;
    {
        vid_t   vid;

        w_istrstream anon(av[1]); anon >> vid;
        w_ostrstream junk;
        junk << "vid = " << vid << ends;
        // fprintf(stderr, "calling check_disk vid=%s, results in %s\n", 
          //       av[1], junk.c_str());

        check_disk(vid);
        w_reset_strstream(tclout);
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    return TCL_OK;
}

static int
t_vol_root_index(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "vid", ac, 2))
        return TCL_ERROR;
    {
        vid_t   vid;
        stid_t iid;

        w_istrstream anon(av[1]); anon >> vid;
        DO( sm->vol_root_index(vid, iid));
        w_reset_strstream(tclout);
        tclout << iid << ends;

    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    return TCL_OK;
}

static int
t_get_volume_meta_stats(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "vid [t_cc_none|t_cc_volume]", ac, 2, 3))  {
        return TCL_ERROR;
    }


    vid_t vid;
    w_istrstream anon(av[1]); anon >> vid;

    ss_m::concurrency_t cc = ss_m::t_cc_none;
    if (ac > 2)  {
        cc = cvt2concurrency_t(av[2]);
    }

    SmVolumeMetaStats volumeStats;
    DO( sm->get_volume_meta_stats(vid, volumeStats, cc) );

    w_reset_strstream(tclout);
    tclout 
        << "total_pages " << volumeStats.numPages << " "
        << "systm_pages " << volumeStats.numSystemPages << " "
        << "pages_rsvd_for_stores " << volumeStats.numReservedPages << " "
        << "pages_alloc_to_stores " << volumeStats.numAllocPages << " "
        << "max_stores " << volumeStats.numStores << " "
        << "stores_allocated " << volumeStats.numAllocStores 
        << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);

    return TCL_OK;
}


#ifdef EXPLICIT_TEMPLATE
template class w_auto_delete_t<SmFileMetaStats>;
template class w_auto_delete_t<sm_stats_info_t>;
#endif
static int
t_get_file_meta_stats(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    const char errString[] = "vid storeNumList [batch|dont_batch] [t_cc_none|t_cc_file]";

    if (check(ip, errString, ac, 3, 4, 5))  {
        return TCL_ERROR;
    }


    vid_t vid;
    w_istrstream anon(av[1]); anon >> vid;

    int                        numFiles;
    TCL_SLIST        char        **listElements;
    if (Tcl_SplitList(ip, av[2], &numFiles, &listElements) != TCL_OK)  {
        return TCL_ERROR;
    }

    SmFileMetaStats* fileStats = new SmFileMetaStats[numFiles];
    w_auto_delete_t<SmFileMetaStats> deleteOnExit(fileStats);

    for (int i = 0; i < numFiles; ++i)  {
        int snum;
        if (Tcl_GetInt(ip, listElements[i], &snum) != TCL_OK)  {
            free(listElements);
            return TCL_ERROR;
        }
        fileStats[i].smallSnum = (snum_t)snum;
    }
    free(listElements);

    bool batch = false;
    if (ac > 3)  {
        if (!strcmp(av[3], "batch"))  {
            batch = true;
        }  else if (!strcmp(av[3], "dont_batch"))  {
            Tcl_AppendResult(ip, errString, 0);
            return TCL_ERROR;
        }
    }

    ss_m::concurrency_t cc = ss_m::t_cc_none;
    if (ac > 4)  {
        cc = cvt2concurrency_t(av[4]);
    }

    DO( sm->get_file_meta_stats(vid, numFiles, fileStats, batch, cc) );

    for (int j = 0; j < numFiles; ++j)  {
        w_reset_strstream(tclout);
        tclout << fileStats[j].smallSnum << " "
                        << fileStats[j].largeSnum << " {"
                        << fileStats[j].small.numReservedPages << " "
                        << fileStats[j].small.numAllocPages << "} {"
                        << fileStats[j].large.numReservedPages << " "
                        << fileStats[j].large.numAllocPages << "}"
                        << ends;

        Tcl_AppendElement(ip, TCL_CVBUG tclout.c_str());
        w_reset_strstream(tclout);
    }

    return TCL_OK;
}

static int
t_dump_buffers(Tcl_Interp* ip, int ac, TCL_AV char*[])
{
    if (check(ip, "", ac, 1)) return TCL_ERROR;
    DO(sm->dump_buffers(cout));
    return TCL_OK;
}

static int
t_get_volume_quota(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "vid", ac, 2))  return TCL_ERROR;
    smlevel_0::smksize_t capacity, used;
    lvid_t lvid;
    w_istrstream anon(av[1]); anon >> lvid;
    DO( sm->get_volume_quota(lvid, capacity, used) );

    w_reset_strstream(tclout);
    tclout << capacity << " " << used << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    return TCL_OK;
}

static int
t_get_device_quota(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "device", ac, 2))  return TCL_ERROR;
    smlevel_0::smksize_t capacity, used;
    DO( sm->get_device_quota(av[1], capacity, used) );
    w_reset_strstream(tclout);
    tclout << capacity << " " << used << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    return TCL_OK;
}

static int
t_format_dev(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "device size_in_KB 'force/noforce'", ac, 4)) 
        return TCL_ERROR;
    bool force = false;
    if (av[3] && streq(av[3], "force")) force = true;
    DO( sm->format_dev(av[1], objectsize(av[2]),force));
    return TCL_OK;
}

static int
t_mount_dev(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    // return volume count
    if (check(ip, "device [local_lvid_for_vid]", ac, 2, 3))
        return TCL_ERROR;

    u_int volume_count=0;
    devid_t devid;

    if (ac == 3) {
        DO( sm->mount_dev(av[1], volume_count, devid, make_vid_from_lvid(av[2])));
    } else 
    {
        DO( sm->mount_dev(av[1], volume_count, devid));
    }
    w_reset_strstream(tclout);
    tclout << volume_count << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_dismount_dev(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "device", ac, 2))
        return TCL_ERROR;
    // keep compiler quiet about unused parameters
    if (av) {}
    
    DO( sm->dismount_dev(av[1]));
    return TCL_OK;
}

static int
t_dismount_all(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;
    // keep compiler quiet about unused parameters
    if (av) {}

    DO( sm->dismount_all());
    return TCL_OK;
}

static int
t_list_devices(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;
    // keep compiler quiet about unused parameters
    if (av) {}

    u_int count;
    const char** dev_list; 
    devid_t* devid_list; 
    DO( sm->list_devices(dev_list, devid_list, count));

    // return list of volumes
    for (u_int i = 0; i < count; i++) {
        w_reset_strstream(tclout);
        tclout << dev_list[i] << " " << devid_list[i] << ends;
        Tcl_AppendElement(ip, TCL_CVBUG tclout.c_str());
        w_reset_strstream(tclout);
    }
    if (count > 0) {
        delete [] dev_list;
        delete [] devid_list;
    }
    return TCL_OK;
}

static int
t_list_volumes(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "device", ac, 2))
        return TCL_ERROR;
    u_int count;
    lvid_t* lvid_list; 
    DO( sm->list_volumes(av[1], lvid_list, count));

    // return list of volumes
    for (u_int i = 0; i < count; i++) {
        w_reset_strstream(tclout);
        tclout << lvid_list[i] << ends;
        Tcl_AppendElement(ip, TCL_CVBUG tclout.c_str());
        w_reset_strstream(tclout);
    }
    if (count > 0) delete [] lvid_list;
    return TCL_OK;
}


static int
t_create_vol(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "device lvid quota_in_KB ['skip_raw_init']", ac, 4, 5)) 
        return TCL_ERROR;

    lvid_t lvid;
    w_istrstream anon(av[2]); anon >> lvid;
    smsize_t quota = objectsize(av[3]);

    bool skip_raw_init = false;
    if (av[4] && streq(av[4], "skip_raw_init")) skip_raw_init = true;

    {
        DO( sm->create_vol(av[1], lvid, quota, skip_raw_init
            , make_vid_from_lvid(av[2])
            ));
    }
    return TCL_OK;
}

static int
t_destroy_vol(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "lvid", ac, 2)) 
        return TCL_ERROR;

    lvid_t lvid;
    w_istrstream anon(av[1]); anon >> lvid;
    DO( sm->destroy_vol(lvid));
    return TCL_OK;
}


static int
t_has_logical_id_index(Tcl_Interp* ip, int ac, TCL_AV char* 
        [])
{
    if (check(ip, "lvid", ac, 2)) return TCL_ERROR;
    Tcl_AppendResult(ip, tcl_form_bool(false), 0);
    return TCL_OK;
}

static int
t_check_volume_page_types(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "vid", ac, 2))  return TCL_ERROR;
    DBG(<< av[0] << " " << av[1]);

    vid_t vid;
    w_istrstream v(av[1], strlen(av[1])); 
    v  >> vid;
    DO( sm->check_volume_page_types(vid) );
    return TCL_OK;
}

static int
t_get_du_statistics(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    // borthakur : June, 1994
    if (check(ip, "vid|stid [pretty] [noaudit]", ac, 2, 3, 4))  return TCL_ERROR;
    sm_du_stats_t stats;

    DBG(<< av[0] << " " << av[1]);

    stats.clear();

    bool pretty = false;
    bool audit = true;
    if (ac == 4) {
        if (strcmp(av[3], "noaudit") == 0) {
            audit = false;
        }
        if (strcmp(av[3], "pretty") == 0) {
            pretty = true;
        }
    }
    if (ac >= 3) {
        if (strcmp(av[2], "pretty") == 0) {
            pretty = true;
        }
        if (strcmp(av[2], "noaudit") == 0) {
            audit = false;
        }
    }
    if( ac < 2) {
        Tcl_AppendResult(ip, "need storage id or volume id", 0);
        return TCL_ERROR;
    }

    TCL_AV char* str = av[1];
    int len = strlen(av[1]);

    {
        stid_t stid;
        vid_t vid;

        w_istrstream s(str, len); s  >> stid;
        w_istrstream v(str, len); v  >> vid;

        if (s) {
            DO( sm->get_du_statistics(stid, stats, audit) );
        } else if (v) {
            DO( sm->get_du_statistics(vid, stats, audit) );
        } else {
            cerr << "bad ID for " << av[0] << " -- " << str << endl;
            return TCL_ERROR;
        }
    }
    if (pretty) stats.print(cout, "du:");

    w_reset_strstream(tclout);
    tclout <<  stats << ends; 
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);

    return TCL_OK;
}


#ifndef PURIFY
#define av /*av not used*/
#endif
static int
t_purify_print_string(Tcl_Interp* ip, int ac, TCL_AV char* av[])
#undef av
{
    if (check(ip, "string", ac, 2))  {
        return TCL_ERROR;
    }

#ifdef PURIFY
    purify_printf("%s\n", av[1]);
#endif

    return TCL_OK;
}

static int
t_set_debug(Tcl_Interp* ip, int ac, TCL_AV char** W_IFTRACE(av))
{
    if (check(ip, "[flag_string]", ac, 1, 2)) return TCL_ERROR;
  
#ifdef W_TRACE
    //
    // If "" is used, debug prints are turned off
    // Always returns the previous setting
    //
    Tcl_AppendResult(ip,  _w_debug.flags(), 0);
    if(ac>1) {
         _w_debug.setflags(av[1]);
    }
#endif
    return TCL_OK;
}

static int
t_set_store_property(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "fid property", ac, 3))  
        return TCL_ERROR;

    ss_m::store_property_t property = cvt2store_property(av[2]);

    {
        stid_t fid;
        w_istrstream anon(av[1]); anon >> fid;
        DO( sm->set_store_property(fid, property) );
    }

    return TCL_OK;
}

static int
t_get_store_property(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "fid", ac, 2))  
        return TCL_ERROR;

    ss_m::store_property_t property;

    {
        stid_t fid;
        w_istrstream anon(av[1]); anon >> fid;
        DO( sm->get_store_property(fid, property) );
    }

    w_reset_strstream(tclout);
    tclout << property << ends;

    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_set_lock_level(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "file|page|record", ac, 2))  
        return TCL_ERROR;
    ss_m::concurrency_t cc = cvt2concurrency_t(av[1]);
    sm->set_xct_lock_level(cc);
    return TCL_OK;
}


static int
t_get_lock_level(Tcl_Interp* ip, int ac, TCL_AV char* [])
{
    if (check(ip, "", ac, 1))  
        return TCL_ERROR;
    ss_m::concurrency_t cc = ss_m::t_cc_bad;
    cc = sm->xct_lock_level();

    w_reset_strstream(tclout);
    tclout << cvt_concurrency_t(cc) << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_create_index(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
     //            1   2       3                                        4           5
     //            1: volid
     //                2: unique or not
     //                        3: file type (regular, load_file, etc)
     //                                        4: key descr, e.g. b*100
     //                                            5: concurrency
     //                                            6: small (optional)
    if (check(ip, "vid ndxtype [\"tmp|regular|load_file|insert_file\"] [keydescr] [t_cc_none|t_cc_kvl|t_cc_modkvl|t_cc_im] [small]", ac, 3, 4, 5,  6, 7))
        return TCL_ERROR;

    const char *keydescr="b*1000";

    ss_m::store_property_t property = ss_m::t_regular;
    if (ac > 3) {
        property = cvt2store_property(av[3]);
    }
    if (ac > 4) {
        if(cvt2type((keydescr = av[4])) == test_nosuch) {
            w_reset_strstream(tclout);
            // Fake it
            tclout << "E_WRONGKEYTYPE" << ends;
            Tcl_AppendResult(ip, tclout.c_str(), 0);
            w_reset_strstream(tclout);
            return TCL_ERROR;
            /*
             * The reason that we allow this to go on is 
             * that we have some tests that explicitly use
             * combined key types, e.g. i2i2, and the
             * tests merely check the conversion to and from
             * the key type strings
             */
        }
    }
    ss_m::concurrency_t cc = ss_m::t_cc_kvl;
    bool small = false;
    if (ac > 5) {
        cc = cvt2concurrency_t(av[5]);
        if(cc == ss_m::t_cc_bad) {
            if(::strcmp(av[5],"small")==0) {
                cc = ss_m::t_cc_none;
                small = true;
            }
        }
    }
    if (ac > 6) {
        if(::strcmp(av[6],"small")!=0) {
            Tcl_AppendResult(ip, 
               "create_index: 5th argument must be \"small\" or empty", 0);
            return TCL_ERROR;
        } else {
            small = true;
        }
    }
    if(small) {
        Tcl_AppendResult(ip, 
       "create_index: argument \"small\" is only for the logical id API - not supported", 
       0);
        return TCL_ERROR;

    }
    const char *kd = check_compress_flag(keydescr);
    ss_m::ndx_t ndx = cvt2ndx_t(av[2]);

    {
        stid_t stid;
        vid_t  volid = vid_t(atoi(av[1]));
        DO( sm->create_index( volid,
                             ndx, property, kd, cc,
                             stid));
        w_reset_strstream(tclout);
        tclout << stid << ends;
    }

    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_destroy_md_index(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "fid", ac, 2))
        return TCL_ERROR;
    
    {
        stid_t fid;
        w_istrstream anon(av[1]); anon >> fid;
        DO( sm->destroy_md_index(fid) );
    }

    return TCL_OK;
}
static int
t_destroy_index(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "fid", ac, 2))
        return TCL_ERROR;
    
    {
        stid_t fid;
        w_istrstream anon(av[1]); anon >> fid;
        DO( sm->destroy_index(fid) );
    }

    return TCL_OK;
}

//
// prepare for bulk load:
//  read the input file and put recs into a sort stream
//
static rc_t 
prepare_for_blkld(sort_stream_i& s_stream, 
                  Tcl_Interp* ip,
                  TCL_AV char *fid,
                  const char* type, 
                  const char* universe=NULL)
{
    key_info_t info;
    sort_parm_t sp;

    info.where = key_info_t::t_hdr;

    typed_btree_test t = cvt2type(type);

    switch(t) {
    case  test_bv:
        //info.type = key_info_t::t_string;
        info.type = sortorder::kt_b;
            info.len = 0;
        break;

    case  test_b1:
        // info.type = key_info_t::t_char;
        info.type = sortorder::kt_u1;
            info.len = sizeof(char);
        break;

    case test_i1:
        // new
        info.type = sortorder::kt_i1;
            info.len = sizeof(uint1_t);
        break;

    case test_i2:
        // new
        info.type = sortorder::kt_i2;
            info.len = sizeof(uint2_t);
        break;

    case test_i4:
        // info.type = key_info_t::t_int;
        info.type = sortorder::kt_i4;
            info.len = sizeof(uint4_t);
        break;

    case test_i8:
        // NB: Tcl doesn't know about 64-bit arithmetic
        // info.type = key_info_t::t_int;
        info.type = sortorder::kt_i8;
            info.len = sizeof(w_base_t::int8_t);
        break;

    case test_u1:
        // new
        info.type = sortorder::kt_u1;
            info.len = sizeof(uint1_t);
        break;

    case test_u2:
        // new
        info.type = sortorder::kt_u2;
            info.len = sizeof(uint2_t);
        break;

    case test_u4:
        // new
        info.type = sortorder::kt_u4;
            info.len = sizeof(uint4_t);
        break;

    case test_u8:
        // NB: Tcl doesn't handle 64-bit arithmetic
        // new
        info.type = sortorder::kt_u8;
            info.len = sizeof(w_base_t::uint8_t);
        break;

    case test_f4:
        // info.type = key_info_t::t_float;
        info.type = sortorder::kt_f4;
            info.len = sizeof(f4_t);
        break;

    case test_f8:
        // new test
        info.type = sortorder::kt_f8;
            info.len = sizeof(f8_t);
        break;

    case test_spatial:
        {
            // info.type = key_info_t::t_spatial;
            info.type = sortorder::kt_spatial;
            if (universe==NULL) {
                if (check(ip, "stid src type [universe]", 1, 0))
                    return RC(fcINTERNAL);
            }
            nbox_t univ(universe);
            info.universe = univ;
            info.len = univ.klen();
        }
        break;

    default:
        W_FATAL(fcNOTIMPLEMENTED);
        break;
    }
    
    sp.run_size = 3;
    sp.destructive = true;
    scan_file_i* f_scan = 0;

    {
        stid_t stid;
        w_istrstream anon(fid); anon >> stid;
        sp.vol = stid.vol;
        f_scan = new scan_file_i(stid, ss_m::t_cc_file);
    }

    HANDLE_FSCAN_FAILURE(f_scan);

    /*
     * scan the input file, put into a sort stream
     */
    s_stream.init(info, sp);
    pin_i* pin;
    bool eof = false;
    while (!eof) {
        W_DO(f_scan->next(pin, 0, eof));
        if (!eof && pin->pinned()) {
            vec_t key(pin->hdr(), pin->hdr_size()),
                  el(pin->body(), pin->body_size());
                
if(t == test_spatial) {
        nbox_t k;
        k.bytes2box((char *)pin->hdr(), pin->hdr_size());

        DBG(<<"hdr size is " << pin->hdr_size());
        int dim = pin->hdr_size() / (2 * sizeof(int));
        const char *cp = pin->hdr();
        int tmp;
        for(int j=0; j<dim; j++) {
            memcpy(&tmp, cp, sizeof(int));
            cp += sizeof(int);
            DBG(<<"int[" << j << "]= " << tmp);
        }
        DBG(<<"key is " << k);
        DBG(<<"body is " << (const char *)(pin->body()));
}
            W_DO(s_stream.put(key, el));
        }
    }

    delete f_scan;
    
    return RCOK;
}

static int
t_blkld_ndx(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    // arguments: 0         1    2     3
    // arguments: blkld_ndx stid nsrcs srcs [src*] [type [universe]]
    int first_src = 3;
    int stid_arg = 1;
    if (ac < 4) {
        Tcl_AppendResult(ip, "wrong # args; should be \"", 
                        "stid nsrcs src [src*] [type [universe]]"
                         "\"", 0);
        return TCL_ERROR;
    }
    /* set up array of srcs */
    int nsrcs = atoi(av[2]);
    if(nsrcs < 1 || nsrcs > 10) {
        Tcl_AppendResult(ip, "Expected 1 ... 10 (arbitrary) sources", 0);
        return TCL_ERROR;
    }
    /* nsrcs == ac -1(blkld_ndx) 
                 -1(stid) -1(nsrcs) possibly -2 for type and universe */
    if(nsrcs < ac-5) {
        Tcl_AppendResult(ip, "Too many arguments provided for "
           "number of sources given (2nd argument)", 0);
        return TCL_ERROR;
    }
    if(nsrcs > ac-3) {
        Tcl_AppendResult(ip, "Not enough sources arguments provided for "
           "number of sources given (2nd argument)", 0);
        return TCL_ERROR;
    }
    int        excess_arguments = ac - 3 - nsrcs;
    const char *typearg = 0, *universearg = 0;
    if(excess_arguments == 2) {
        typearg = av[ac-2];
        universearg = av[ac-1];
    } else if(excess_arguments == 1) {
        typearg = av[ac-1];
    } else if(excess_arguments != 0) {
        w_assert1(0);
    }

    sm_du_stats_t stats;

    if (excess_arguments) {
        // using sorted stream
            sort_stream_i s_stream;

        if(nsrcs!=1) {
            Tcl_AppendResult(ip, "Bulk load with sort_stream "
               " takes only one source file.", 0);
        }
            if (excess_arguments==1) {
            DBG(<<"prepare_for_blkld 1 " );
            DO( prepare_for_blkld(s_stream, ip, av[first_src], typearg) );
            } else {
            DBG(<<"prepare_for_blkld 2 " );
            DO( prepare_for_blkld(s_stream, ip, av[first_src], typearg, universearg) );
        }

        {
            stid_t stid;
            w_istrstream anon(av[stid_arg]); 
            anon >> stid;
            DBG(<<"bulkld_index 4 " );
            DO( sm->bulkld_index(stid, s_stream, stats) ); 
            DBG(<<"bulkld_index 4 finished " );
        }
    } else {
        // input file[s] already sorted
        {
            stid_t stid;
            stid_t* srcs = new stid_t[nsrcs];
            w_istrstream anon(av[stid_arg]); 
            anon >> stid;
            for(int i=0; i<nsrcs; i++) {
               w_istrstream anon2(av[first_src+i]); 
               anon2 >> srcs[i];
            }
            DBG(<<"bulkld_index 6 " );
            w_rc_t rc = sm->bulkld_index(stid, nsrcs, srcs, stats); 
            delete[] srcs;
            DO(rc);
        }
    } 
    DBG(<<"bulkld_index ends OK " );

    {
        w_reset_strstream(tclout);
        tclout << stats.btree << ends;
        Tcl_AppendResult(ip, tclout.c_str(), 0);
        w_reset_strstream(tclout);
    }
    
    return TCL_OK;

}

static int
t_blkld_md_ndx(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid src hf he universe", ac, 6)) 
        return TCL_ERROR;

    nbox_t univ(av[5]);
    
    sm_du_stats_t stats;

    sort_stream_i s_stream;

    const char* type = "spatial";

    DO( prepare_for_blkld(s_stream, ip, av[2], type, av[5]) );

    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        DO( sm->bulkld_md_index(stid, s_stream, stats,
                  // hff,      hef,         universe
                  atoi(av[3]), atoi(av[4]), &univ) );
    }

    {
        w_reset_strstream(tclout);
        tclout << stats.rtree << ends;
        Tcl_AppendResult(ip, tclout.c_str(), 0);
        w_reset_strstream(tclout);
    }
    
    return TCL_OK;
}

static int
t_print_index(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid", ac, 2))
        return TCL_ERROR;

    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        DO( sm->print_index(stid) );
    }

    return TCL_OK;
}

static int
t_print_md_index(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid", ac, 2))
        return TCL_ERROR;

    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        DO( sm->print_md_index(stid) );
    }

    return TCL_OK;
}

#include <crash.h>


static int
t_crash(Tcl_Interp* ip, int ac, TCL_AV char ** W_IFTRACE(av))
{

    if (check(ip, "str cmd", ac, 3))
        return TCL_ERROR;

    DBG(<<"crash " << av[1] << " " << av[2] );

    fprintf(stderr, "Tcl crash command : _exit(1)\n");
    cout << flush;
    _exit(1);
    // no need --
    return TCL_OK;
}

/*
 * callback function for running out of log space
 */
class xct_i; class xct_t;

static int
t_restart(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "clean:bool", ac, 1, 2))
        return TCL_ERROR;

    bool clean = false;
    if(ac==2) {
            /* XXX error ignored for now */
            tcl_scan_bool(av[1], clean);
    }
    sm->set_shutdown_flag(clean);
    delete sm;

    /* 
     * callback function for out-of-log-space
     */

    // Try without callback function.
    if(log_warn_callback) {
       sm = new ss_m(out_of_log_space, get_archived_log_file);
    } else {
       sm = new ss_m();
    }
    w_assert1(sm);
    
    return TCL_OK;
}


static int
t_create_file(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "vid  [\"tmp|regular|load_file|insert_file\"] [cluster-page]",
              ac, 2, 3, 4)) 
        return TCL_ERROR;
    
    ss_m::store_property_t property = ss_m::t_regular;
  
    shpid_t        cluster_page=0;
    if (ac > 3) {
       cluster_page = atoi(av[3]);
    }
    if (ac > 2) {
        property = cvt2store_property(av[2]);
    }

    {
        stid_t stid;
        int volumeid = atoi(av[1]);
        DO( sm->create_file(vid_t(volumeid), stid, property,
                cluster_page) );

        w_reset_strstream(tclout);
        tclout << stid << ends;
    }
    
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_destroy_file(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "fid", ac, 2))
        return TCL_ERROR;
    
    {
        stid_t fid;
        w_istrstream anon(av[1]); anon >> fid;
        DO( sm->destroy_file(fid) );
    }

    return TCL_OK;
}

static int
t_create_scan(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    FUNC(t_create_scan);
    if (check(ip, "stid cmp1 bound1 cmp2 bound2 [concurrency_t] [keydescr]", ac, 6, 8))
        return TCL_ERROR;
    
    vec_t t1, t2;
    vec_t* bound1 = 0;
    vec_t* bound2 = 0;
    scan_index_i::cmp_t c1, c2;
    

    bool convert=false;
    int b1, b2; // used if keytype is int or unsigned
    w_base_t::int8_t bl1, bl2; // used if keytype is 64 bits

    c1 = cvt2cmp_t(av[2]);
    c2 = cvt2cmp_t(av[4]);
    if (streq(av[3], "pos_inf"))  {
        bound1 = &vec_t::pos_inf;
    } else if (streq(av[3], "neg_inf")) {
        bound1 = &vec_t::neg_inf;
    } else {
        convert = true;
        (bound1 = &t1)->put(av[3], strlen(av[3]));
    }
    
    if (streq(av[5], "pos_inf")) {
        bound2 = &vec_t::pos_inf;
    } else if (streq(av[5], "neg_inf")) {
        bound2 = &vec_t::neg_inf;
    } else {
        (bound2 = &t2)->put(av[5], strlen(av[5]));
        convert = true;
    }

    if (convert && (ac > 7)) {
        const char *keydescr = (const char *)av[7];

        enum typed_btree_test t = cvt2type(keydescr);
        if (t == test_nosuch) {
            Tcl_AppendResult(ip, 
               "create_scan: 7th argument must start with [bBiIuUfF]", 0);
            return TCL_ERROR;
                
        }
        switch(t) {
            case test_i8:
            case test_u8: {
                    /* XXX this breaks with error checking strtoq,
                       need to use strtoq, strtouq.  Or add
                       w_base_t::ato8() that mimics atoi()
                       semantics. */
                    bl1 = w_base_t::strtoi8(av[3]); //bound1 
                    t1.reset();
                    t1.put(&bl1, sizeof(w_base_t::int8_t));
                    // DBG(<<"bound1 = " << bl1);

                    bl2 = w_base_t::strtoi8(av[5]); //bound2
                    t2.reset();
                    t2.put(&bl2, sizeof(w_base_t::int8_t));
                    // DBG(<<"bound2 = " << bl2);
                }
                break;
            case test_i4:
            case test_u4: {
                    b1 = atoi(av[3]); //bound1 
                    t1.reset();
                    t1.put(&b1, sizeof(int));
                    DBG(<<"bound1 = " << b1);

                    b2 = atoi(av[5]); // bound2
                    t2.reset();
                    t2.put(&b2, sizeof(int));
                    DBG(<<"bound2 = " << b2);
                }
                break;
            case test_bv:
            case test_b1:
            case test_b23:
                break;
            default:
                Tcl_AppendResult(ip, 
               "smsh.create_scan unsupported for given keytype:  ",
                   keydescr, 0);
            return TCL_ERROR;
        }
    }

    ss_m::concurrency_t cc = ss_m::t_cc_kvl;
    if (ac == 7) {
        cc = cvt2concurrency_t(av[6]);
    }
    DBG(<<"c1 = " << W_ENUM(c1));
    DBG(<<"c2 = " << W_ENUM(c2));
    DBG(<<"cc = " << W_ENUM(cc));

    scan_index_i* s;
    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        s = new scan_index_i(stid, c1, *bound1, c2, *bound2, false, cc);
    }
    if (!s) {
        cerr << "Out of memory: file " << __FILE__ 
             << " line " << __LINE__ << endl;
        W_FATAL(fcOUTOFMEMORY);
    }
    
    w_reset_strstream(tclout);
    format_ptr(tclout, s) << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_scan_next(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "scanid [keydescr]", ac, 2, 3))
        return TCL_ERROR;

    typed_btree_test  t = test_bv; // default
    if (ac > 2) {
        t = cvt2type(av[2]);
    } 

    scan_index_i *s = 0;
    if (read_ptr(ip, av[1], s)) return TCL_ERROR;

    smsize_t klen, elen;
    klen = elen = 0;
    bool eof;
    DO( s->next(eof) );
    if (eof) {
        Tcl_AppendElement(ip, TCL_CVBUG "eof");
    } else {

        DO( s->curr(0, klen, 0, elen) );
        w_assert1(klen + elen + 2 <= ss_m::page_sz);

        vec_t key(outbuf, klen);
        vec_t el(outbuf + klen + 1, elen);
        DO( s->curr(&key, klen, &el, elen) );

        typed_value v;

        // For the time being, we assume that value/elem is string
        outbuf[klen] = outbuf[klen + 1 + elen] = '\0';

        bool done = false;
        switch(t) {
        case test_bv:
            outbuf[klen] = outbuf[klen + 1 + elen] = '\0';
            Tcl_AppendResult(ip, outbuf, " ", outbuf + klen + 1, 0);
            done = true;
            break;
        case test_b1:
            char c[2];
            c[0] = outbuf[0];
            c[1] = 0;
            w_assert3(klen == 1);
            done = true;
            Tcl_AppendResult(ip, c, " ", outbuf + klen + 1, 0);
            break;

        case test_i1:
        case test_u1:
        case test_i2:
        case test_u2:
        case test_i4:
        case test_u4:
        case test_i8:
        case test_u8:
        case test_f4:
        case test_f8:
            memcpy(&v._u, outbuf, klen);
            v._length = klen;
            break;
        case test_spatial:
        default:
            W_FATAL(fcNOTIMPLEMENTED);
        }
        {
                w_ostrstream_buf tmp(100);        // XXX magic number

                switch(t) {
                case test_bv:
                case test_b1:
                    break;

                /* Casts force integer instead of character output */
                case test_i1:
                    tmp << (int)v._u.i1_num;
                    break;
                case test_u1:
                    tmp << (unsigned)v._u.u1_num;
                    break;

                case test_i2:
                    tmp << v._u.i2_num;
                    break;
                case test_u2:
                    tmp << v._u.u2_num;
                    break;

                case test_i4:
                    tmp << v._u.i4_num;
                    break;
                case test_u4:
                    tmp << v._u.u4_num;
                    break;

                case test_i8:
                    tmp << v._u.i8_num;
                    break;
                case test_u8:
                    tmp << v._u.u8_num;
                    break;

                case test_f4:
                    tmp << v._u.f4_num;
                    break;
                case test_f8:
                    tmp << v._u.f8_num;
                    break;

                case test_spatial:
                default:
                    W_FATAL(fcNOTIMPLEMENTED);
                }
                if(!done) {
                    tmp << ends;
                    Tcl_AppendResult(ip, tmp.c_str(), " ", outbuf + klen + 1, 0);
                }
        }
    }
    
    return TCL_OK;
}

static int
t_destroy_scan(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    // last optional argument is comment, for debugging
    if (check(ip, "scanid", ac, 2, 3))
        return TCL_ERROR;

    scan_index_i        *s(NULL);
    if (read_ptr(ip, av[1], s)) return TCL_ERROR;

    delete s;
    
    return TCL_OK;
}

static int
t_create_multi_recs(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "fid hdr len_hint data count", ac, 6))
        return TCL_ERROR;

    vec_t hdr, data;
    hdr.set(parse_vec(av[2], strlen(av[2])));
    data.set(parse_vec(av[4], strlen(av[4])));

    w_reset_strstream(tclout);

    register int i;
    int count = atoi(av[5]); //count -- can't be > signed#

    {
        stid_t  stid;
        rid_t   rid;

        w_istrstream anon(av[1]); anon >> stid;

        stime_t start(stime_t::now());
        for (i=0; i<count; i++)
                DO( sm->create_rec(stid, hdr, objectsize(av[3]), data, rid) );
        sinterval_t delta(stime_t::now() - start);
        cerr << "t_create_multi_recs: Write clock time = " 
                << delta << " seconds" << endl;
    }
    tclout << "success" << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_multi_file_append(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "scan hdr len_hint data count", ac, 6))
        return TCL_ERROR;

    append_file_i *s = 0;
    if (read_ptr(ip, av[1], s)) return TCL_ERROR;

    smsize_t len_hint = objectsize(av[3]);

    vec_t hdr, data;
    hdr.set(parse_vec(av[2], strlen(av[2])));
    data.set(parse_vec(av[4], strlen(av[4])));

    w_reset_strstream(tclout);

    register int i;
    int count = atoi(av[5]); // count - can't be > signed int

    {
        rid_t   rid;
        stime_t start(stime_t::now());
        for (i=0; i<count; i++)
                DO( s->create_rec(hdr, len_hint, data, rid) );
        sinterval_t delta(stime_t::now() - start);
        if (linked.verbose_flag)  {
            cerr << "t_multi_file_append: Write clock time = " 
                    << delta << " seconds" << endl;
        }
    }
    tclout << "success" << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_multi_file_update(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "scan objsize ", ac, 3))
        return TCL_ERROR;

    scan_file_i        *s = 0;
    if (read_ptr(ip, av[1], s)) return TCL_ERROR;
    scan_file_i &scan = *s;

    smsize_t osize = objectsize(av[2]);

    w_reset_strstream(tclout);
    {
        char* d= new char[osize];
        vec_t data(d, osize);
        // random uninit data ok


        w_rc_t rc;
        bool eof=false;
        pin_i*  pin = 0;

        while ( !(rc=scan.next(pin, 0, eof)).is_error() && !eof) {
            w_assert3(pin);
            DO( pin->update_rec(0, data) );
        }
        delete[] d;
    }

    tclout << "success" << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_create_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "fid hdr len_hint data", ac, 5)) 
        return TCL_ERROR;
   
    vec_t hdr, data;
    hdr.set(parse_vec(av[2], strlen(av[2])));
    data.set(parse_vec(av[4], strlen(av[4])));

    {
        stid_t  stid;
        rid_t   rid;
        
        w_istrstream anon(av[1]); anon >> stid;
        DBG(<< av[0] << " " << av[1] << " " << av[2] << " " << av[3] << " ");
        DO( sm->create_rec(stid, hdr, objectsize(av[3]), data, rid) );
        w_reset_strstream(tclout);
        tclout << rid << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_destroy_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "rid", ac, 2)) 
        return TCL_ERROR;
    

    {
        rid_t   rid;

        DBG(<< av[0] << " " << av[1]);
        w_istrstream anon(av[1]); anon >> rid;
        DO( sm->destroy_rec(rid) );
    }

    Tcl_AppendElement(ip, TCL_CVBUG "update success");
    return TCL_OK;
}

static int
t_read_rec_1(Tcl_Interp* ip, int ac, TCL_AV char* av[], bool dump_body_too)
{
    if (check(ip, "rid start length [num_pins]", ac, 4, 5)) 
        return TCL_ERROR;
    
    smsize_t   start  = objectsize(av[2]);
    smsize_t   length = objectsize(av[3]);
    smsize_t   num_pins;
    if (ac == 5) {
        num_pins = numobjects(av[4]);                
    } else {
        num_pins = 1;
    }
    pin_i   handle;

    w_reset_strstream(tclout);

    {
        rid_t   rid;
        w_istrstream anon(av[1]); anon >> rid;

        for (smsize_t i=1; i<num_pins; i++) {
            W_IGNORE(handle.pin(rid, start));
            handle.unpin();
        }
        DO(handle.pin(rid, start)); 
        tclout << "rid=" << rid << " " << ends;
    }
    /* result: 
       rid=<rid> size(h.b)=<sizes> hdr=<hdr>
       \nBytes:x-y: <body>
    */

    if (length == 0) {
        length = handle.body_size();
    }
    tclout << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);

    w_reset_strstream(tclout);
    tclout << " size(h.b)=" << handle.hdr_size() << "." 
                          << handle.body_size() << " " << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);

    w_reset_strstream(tclout);
    dump_pin_hdr(tclout, handle);
    tclout << " " << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);

    if(dump_body_too) {
        w_reset_strstream(tclout);
        dump_pin_body(tclout, handle, start, length, ip);
        tclout << " " << ends;
        Tcl_AppendResult(ip, tclout.c_str(), 0);
        w_reset_strstream(tclout);
    }

    return TCL_OK;
}
static int
t_read_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    return t_read_rec_1(ip, ac, av, true);
}
static int
t_read_hdr(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    return t_read_rec_1(ip, ac, av, false);
}

static int
t_print_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "rid start length", ac, 4)) 
        return TCL_ERROR;
    
    smsize_t   start  = objectsize(av[2]);
    smsize_t   length = objectsize(av[3]);
    pin_i   handle;

    {
        rid_t   rid;
        w_istrstream anon(av[1]); anon >> rid;

        DO(handle.pin(rid, start)); 
        if (linked.verbose_flag)  {
            cout << "rid=" << rid << " "; 
        }
    }

    if (length == 0) {
        length = handle.body_size();
    }

    if (linked.verbose_flag) {
        dump_pin_hdr(cout, handle);
    }
    w_reset_strstream(tclout);
    dump_pin_body(tclout, handle, start, length, 0);
    if (linked.verbose_flag) {
        tclout << ends;
        cout << tclout.c_str();
    }

    return TCL_OK;
}

static int
t_read_rec_body(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "rid [start [length]]", ac, 2, 3, 4)) 
        return TCL_ERROR;

    smsize_t start = (ac >= 3) ? objectsize(av[2]) : 0;
    smsize_t length = (ac >= 4) ? objectsize(av[3]) : 0;
    pin_i handle;

    {
        rid_t   rid;
        w_istrstream anon(av[1]); anon >> rid;

        DO(handle.pin(rid, start));
    }

    if (length == 0) {
        length = handle.body_size();
    }

    smsize_t offset = (start - handle.start_byte());
    smsize_t start_pos = start;
    smsize_t amount;

    Tcl_ResetResult(ip);

    bool eor = false;  // check end of record
    while (length > 0)
    {
        amount = MIN(length, handle.length() - offset);

        sprintf(outbuf, "%.*s", (int)amount, handle.body() + offset);
        Tcl_AppendResult(ip, outbuf, 0);

        length -= amount;
        start_pos += amount;
        offset = 0;

        DO(handle.next_bytes(eor));
        if (eor) {
            break;                // from while loop
        }
    }  // end while
    return TCL_OK;
}

static int
t_update_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "rid start data", ac, 4)) 
        return TCL_ERROR;
    
    smsize_t   start;
    vec_t   data;

    data.set(parse_vec(av[3], strlen(av[3])));
    start = objectsize(av[2]);    

    {
        rid_t   rid;

        w_istrstream anon(av[1]); anon >> rid;
        DO( sm->update_rec(rid, start, data) );
    }

    Tcl_AppendElement(ip, TCL_CVBUG "update success");
    return TCL_OK;
}

static int
t_update_rec_hdr(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "rid start hdr", ac, 4)) 
        return TCL_ERROR;
    
    smsize_t   start;
    vec_t   hdr;

    hdr.set(parse_vec(av[3], strlen(av[3])));
    start = objectsize(av[2]);    

    {
        rid_t   rid;

        w_istrstream anon(av[1]); anon >> rid;
        DO( sm->update_rec_hdr(rid, start, hdr) );
    }

    Tcl_AppendElement(ip, TCL_CVBUG "update hdr success");
    return TCL_OK;
}

static int
t_append_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "rid data", ac, 3)) 
        return TCL_ERROR;
    
    vec_t   data;
    data.set(parse_vec(av[2], strlen(av[2])));

    {
        rid_t   rid;
        w_istrstream anon(av[1]); anon >> rid;
        DO( sm->append_rec(rid, data) );
    }

    Tcl_AppendElement(ip, TCL_CVBUG "append success");
    return TCL_OK;
}

static int
t_truncate_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "rid amount", ac, 3)) 
        return TCL_ERROR;
 
    smsize_t amount = objectsize(av[2]);
     
    {
        rid_t   rid;
        w_istrstream anon(av[1]); anon >> rid;
        DO( sm->truncate_rec(rid, amount) );
    }

    Tcl_AppendElement(ip, TCL_CVBUG "append success");
    return TCL_OK;
}

void
dump_pin_hdr( 
        ostream &out,
        pin_i &handle
) 
{
    int j;
    char *outbuf = new char[handle.hdr_size()+1];

    // DETECT ZVECS
    // Assume the header is reasonably small

    memcpy(outbuf, handle.hdr(), handle.hdr_size());
    outbuf[handle.hdr_size()] = 0;
    
    for (j=handle.hdr_size()-1; j>=0; j--) {
            if(outbuf[j]!= '\0') break;
    }
    if(j== -1) {
            out << " hdr=zvec" << handle.hdr_size() ;
    } else {
            out << " hdr=" << outbuf ;
    }
    delete[] outbuf;
}

/* XXX it takes an strstream, but that is really tclout under the ocvers */

void
dump_pin_body( 
    w_ostrstream &out, 
    pin_i &handle, 
    smsize_t start,
    smsize_t length,
    Tcl_Interp * ip
)
{
    const        int buf_len = ss_m::page_sz + 1;

    char *buf = new char[buf_len];
    if (!buf)
            W_FATAL(fcOUTOFMEMORY);
    DBG(<<"buf & " << &buf );
            

    // BODY is not going to be small
    // write to ostream, and if
    // ip is defined, flush to ip every
    // so - often
    if(start == (smsize_t)-1) {
        start = 0;
    }
    if(length == 0) {
        length = handle.body_size();
    }

    smsize_t i = start;
    smsize_t offset = start-handle.start_byte();
    bool eor = false;  // check end of record
    while (i < length && !eor) {
        smsize_t handle_len = MIN(handle.length(), length);
        //s << "Bytes:" << i << "-" << i+handle.length()-offset 
        //  << ": " << handle.body()[0] << "..." << ends;
        int width = handle_len-offset;

        // w_reset_strstream(out);

        if (true || linked.verbose_flag) {
            out << "\nBytes:" << i << "-" << i+handle_len-offset << ": " ;
            if(ip) {
                out << ends;
                Tcl_AppendResult(ip, out.c_str(), 0);
                w_reset_strstream(out);
            }

            // DETECT ZVECS
            {
                int j;

                /* Zero the buffer beforehand.  The sprintf will output *at
                   most* 'width' characters, and if the printed string is
                   shorter, the loop examines unset memory.

                   XXX this seems overly complex, how about checking width
                   for non-zero bytes instead of the run-around with
                   sprintf?
                 */

                w_assert1(width+1 < buf_len);
                DBG(<<"width " << width );
                memset(buf, '\0', width+1);
                sprintf(buf, "%.*s", width, handle.body()+offset);
                DBG(<<"buf " << buf );
                for (j=width-1; j>=0; j--) {
                    if(buf[j]!= '\0')  {
                        DBG(<<"byte # " << j << 
                        " is non-zero: " << (unsigned char) buf[j] );
                        break;
                    }
                }
                if(j== -1) {
                    out << "zvec" << width;
                } else {
                    sprintf(buf, "%.*s", width, handle.body()+offset);
                    out << buf;
                    if(ip) {
                        out << ends;
                        Tcl_AppendResult(ip, buf, 0);
                        DBG(<<"buf " << buf );
                        w_reset_strstream(out);
                    }
                }
            }
        }

        i += handle_len-offset;
        offset = 0;
        if (i < length) {
            if(handle.next_bytes(eor).is_error()) {
                cerr << "error in next_bytes" << endl;
                delete[] buf;
                return;
            }
        }
    } /* while loop */
    delete[] buf;
}


w_rc_t
dump_scan(scan_file_i &scan, ostream &out, DSCB callback, bool hex) 
{
    w_rc_t  rc;
    bool    eof=false;
    pin_i*  pin = 0;
    const int buf_unit = 1024;        /* must be a power of 2 */
    char    *buf = 0;
    int     buf_len = 0;

    DBG(<< " &scan "<< u_long( (void*)&scan) );

    while ( !(rc=scan.next(pin, 0, eof)).is_error() && !eof) 
    {
         DBG(
                 << " pin " << u_long( (void *)pin)
                 << " eof " << eof  );
        if(callback) {
            (*callback)(*pin);
        } else 
        if (linked.verbose_flag) {
            out << pin->rid();
            if(pin->hdr_size() > 0) {
                if(pin->hdr_size() == sizeof(unsigned int)) {
                    out << " hdr: " ;
                    // HACK:
                    unsigned int m;
                    memcpy(&m, pin->hdr(), sizeof(unsigned int));
                    out << m;
                } else if(hex) {
                   out << " hdr: " ;
                   for(unsigned int qq = 0; qq < pin->hdr_size(); qq++) {
                       const char *xxx = pin->hdr() + qq;
                       cout << int( (*xxx) ) << " ";
                   }
                } else {
                   out << " hdr: " ;
                    // print as char string
                    out << pin->hdr();
                }
            }
            out << "(size=" << pin->hdr_size() << ")" ;
            out << " body: " ;
        }
        vec_t w;
        bool eof = false;

        if(!callback) 
        for(unsigned int j = 0; j < pin->body_size();) {
            if(pin->body_size() > 0) {
                w.reset();

                /* Lazy buffer allocation, in buf_unit chunks.
                   There must be space for the nul string terminator */
                if ((int)pin->length() >= buf_len) {
                        /* allocate the buffer in buf_unit increments */
                        w_assert3(buf_len <= 8192);
                        if (buf)
                                delete[] buf;
                        /* need a generic alignment macro (alignto) */
                        buf_len = (pin->length() + 1 + buf_unit-1) &
                                ~(buf_unit - 1);
                        buf = new char [buf_len];
                        if (!buf)
                                W_FATAL(fcOUTOFMEMORY);
                }

                w.put(pin->body(), pin->length());
                w.copy_to(buf, buf_len);
                buf[pin->length()] = '\0';
            if (linked.verbose_flag) {
#ifdef W_DEBUG
                    if(hex) {
                       for(unsigned int qq = 0; qq < pin->length(); qq++) {
                           cout << int(buf[qq]) << " ";
                       }
                    } else{
                       out << buf ;
                    }
#endif /* W_DEBUG */
                 out << "(length=" << pin->length() << ")" 
                                << "(size=" << pin->body_size() << ")" 
                                << endl;
            }
                rc = pin->next_bytes(eof);
                if(rc.is_error()) break;
                j += pin->length();
            } else {
            // length 0
                if (linked.verbose_flag) {
                     out << "(length=" << pin->length() << ")" 
                                    << "(size=" << pin->body_size() << ")" 
                                    << endl;
                }
            }
            if(eof || rc.is_error()) break;
        }
        if (linked.verbose_flag) {
            out << endl;
        }
    }
    if(pin) pin->unpin();
    if (buf)
            delete[] buf;
    return rc;
}


static int
t_scan_recs(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "fid [start_rid]", ac, 2, 3)) 
        return TCL_ERROR;
    
    rc_t    rc;
    scan_file_i *scan;

    {
        rid_t   rid;
        stid_t  fid;

        w_istrstream anon(av[1]); anon >> fid;
        if (ac == 3) {
            w_istrstream anon2(av[2]); anon2 >> rid;
            scan = new scan_file_i(fid, rid);
        } else {
            scan = new scan_file_i(fid);
            DBG( << " scan "<< u_long(scan) );
        }
                DBG(<< " scan "<< u_long(scan) <<  fid);
    }

        /*
    if(scan->error_code().is_error()) { 
        w_rc_t rc = RC(scan->error_code().err_num());
        // cerr << rc << endl;
        bool t1 = rc.is_error();
        bool t2 = scan->error_code().is_error();
                // Something weird was happening here for a while.
                // the line scan->error_code().is_error() returns true
                // while the rc.is_error() is false and
                // scan->_error_occurred.is_error() returns false.
                // maybe the copy operator has something to do with it,
                // or it's a compiler problem(?)
        cerr << "scan->error_code().is_error() " << t2 << endl;
        cerr << "scan->error_code().is_error() " 
                     << scan->error_code().is_error() << endl;
        cerr << "rc.is_error() " << t1 << endl;
                // OK now it gets handled by the macro below.
    }
        */
    TCL_HANDLE_FSCAN_FAILURE(scan);
    DBG(<< " scan "<< u_long(scan) );
    w_assert1(scan != NULL);
    rc = dump_scan(*scan, cout);
    delete scan;

    DO( rc );

    Tcl_AppendElement(ip, TCL_CVBUG "scan success");
    return TCL_OK;
}

static int
t_scan_rids(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "fid [start_rid]", ac, 2, 3))
        return TCL_ERROR;
  
    pin_i*      pin;
    bool      eof;
    rc_t        rc;
    scan_file_i *scan = NULL;

    Tcl_ResetResult(ip);
    {
        rid_t   rid;
        stid_t  fid;

        w_istrstream anon(av[1]); anon >> fid;

        if (ac == 3) {
            w_istrstream anon2(av[2]); anon2 >> rid;
            scan = new scan_file_i(fid, rid);
        } else {
            scan = new scan_file_i(fid);
        }
        TCL_HANDLE_FSCAN_FAILURE(scan);

        while ( !(rc = scan->next(pin, 0, eof)).is_error() && !eof ) {
            w_reset_strstream(tclout);
            tclout << pin->rid() << ends;
            Tcl_AppendResult(ip, tclout.c_str(), " ", 0);
            w_reset_strstream(tclout);
        }
    }

    if (scan) {
        delete scan;
        scan = NULL;
    }

    DO( rc);

    return TCL_OK;
}

static int
t_pin_create(Tcl_Interp* ip, int ac, TCL_AV char*[])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;

    pin_i* p = new pin_i;;
    if (!p) {
        cerr << "Out of memory: file " << __FILE__ 
             << " line " << __LINE__ << endl;
        W_FATAL(fcOUTOFMEMORY);
    }
    
    w_reset_strstream(tclout);
    format_ptr(tclout, p) << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_pin_destroy(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin", ac, 2))
        return TCL_ERROR;

    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    if (p) delete p;

    return TCL_OK;
}

static int
t_pin_pin(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin rec_id start [SH/EX]", ac, 4,5))
        return TCL_ERROR;

    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    smsize_t start = objectsize(av[3]);
    lock_mode_t lmode = w_base_t::SH;
 
    if (ac == 5) {
        if (strcmp(av[4], "EX") == 0) {
            lmode = w_base_t::EX;
        }
    }

    {
        rid_t   rid;
        w_istrstream anon2(av[2]); anon2 >> rid;

        DO(p->pin(rid, start, lmode)); 
    }
    return TCL_OK;
}

static int
t_pin_unpin(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin", ac, 2))
        return TCL_ERROR;

    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    p->unpin();

    return TCL_OK;
}

static int
t_pin_repin(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin [SH/EX]", ac, 2,3))
        return TCL_ERROR;
    
    lock_mode_t lmode = w_base_t::SH;
    if (ac == 3) {
        if (strcmp(av[2], "EX") == 0) {
            lmode = w_base_t::EX;
        }
    }

    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    DO(p->repin(lmode));

    return TCL_OK;
}

static int
t_pin_body(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin", ac, 2))
        return TCL_ERROR;
    
    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    memcpy(outbuf, p->body(), p->length());
    outbuf[p->length()] = '\0';

    Tcl_AppendResult(ip, outbuf, 0);
    return TCL_OK;
}


static int
t_pin_next_bytes(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin", ac, 2))
        return TCL_ERROR;

    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    bool eof;
    DO(p->next_bytes(eof));

    Tcl_AppendResult(ip, tcl_form_flag(eof), 0);

    return TCL_OK;
}

static int
t_page_containing(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin offset", ac, 3))
        return TCL_ERROR;
    
    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    smsize_t offset = strtol(av[2], 0, 0);
    smsize_t byte_in_page;
    lpid_t page = p->page_containing(offset, byte_in_page);

    w_reset_strstream(tclout);
    tclout << page <<ends;

    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);

    return TCL_OK;
}

static int
t_pin_hdr(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin", ac, 2))
        return TCL_ERROR;
    
    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    memcpy(outbuf, p->hdr(), p->hdr_size());
    outbuf[p->hdr_size()] = '\0';

    Tcl_AppendResult(ip, outbuf, 0);

    return TCL_OK;
}

static int
t_pin_pinned(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin", ac, 2))
        return TCL_ERROR;
    
    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    Tcl_AppendResult(ip, tcl_form_flag(p->pinned()), 0);

    return TCL_OK;
}

static int
t_pin_large_impl(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin", ac, 2))
        return TCL_ERROR;

    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    int j= p->large_impl();
    w_reset_strstream(tclout);
    tclout << j <<ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);

    return TCL_OK;
}

static int
t_pin_is_small(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin", ac, 2))
        return TCL_ERROR;
    
    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    Tcl_AppendResult(ip, tcl_form_flag(p->is_small()), 0);

    return TCL_OK;
}

static int
t_pin_rid(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin", ac, 2))
        return TCL_ERROR;

    // A great example of why a hash table of pointers "handed out"
    // would be great to validate pointers coming back in.

    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    w_reset_strstream(tclout);
    {
        tclout << p->rid() << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);

    return TCL_OK;
}

static int
t_pin_append_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin data", ac, 3))
        return TCL_ERROR;
    
    vec_t   data;
    data.set(parse_vec(av[2], strlen(av[2])));

    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    DO( p->append_rec(data) );

    return TCL_OK;
}

static int
t_pin_update_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin start data", ac, 4))
        return TCL_ERROR;
   
    smsize_t start = objectsize(av[2]); 
    vec_t   data;
    data.set(parse_vec(av[3], strlen(av[3])));

    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    DO( p->update_rec(start, data) );

    return TCL_OK;
}

static int
t_pin_update_rec_hdr(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin start data", ac, 4))
        return TCL_ERROR;
   
    smsize_t start = objectsize(av[2]); 
    vec_t   data;
    data.set(parse_vec(av[3], strlen(av[3])));

    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    DO( p->update_rec_hdr(start, data) );

    return TCL_OK;
}

static int
t_pin_truncate_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "pin amount", ac, 3))
        return TCL_ERROR;
   
    smsize_t amount = objectsize(av[2]); 

    pin_i *p = 0;
    if (read_ptr(ip, av[1], p)) return TCL_ERROR;

    DO( p->truncate_rec(amount) );

    return TCL_OK;
}

static int
t_scan_file_create(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    
    ss_m::concurrency_t cc;
    cc = cvt2concurrency_t(av[2]);

    scan_file_i* s = NULL;

    {
        if (check(ip, "fileid concurrency_t [prefetch(Boolean) [start_rid]]", 
                ac, 3,4,5))
             return TCL_ERROR;

        stid_t   fid;
        w_istrstream anon(av[1]); anon >> fid;

        if (ac >= 4) {
            rid_t   rid;
            bool prefetch=false;
            w_istrstream anon3(av[3]); anon3 >> prefetch;

            if (ac > 4) {
                w_istrstream anon2(av[2]); anon2 >> rid;
            }
            s = new scan_file_i(fid, rid, cc, prefetch);
        } else {
            if(cc ==  ss_m::t_cc_append) {
                s = new append_file_i(fid);
            } else {
                s = new scan_file_i(fid, cc);
            }
        }
        if (!s) {
            cerr << "Out of memory: file " << __FILE__ 
                 << " line " << __LINE__ << endl;
            W_FATAL(fcOUTOFMEMORY);
        }
        if(s->error_code().is_error()) {
            w_rc_t rc = RC(s->error_code().err_num());
            cerr << rc << endl;
        }
        TCL_HANDLE_FSCAN_FAILURE(s);
    }
    
    w_reset_strstream(tclout);
    format_ptr(tclout, s) << ends;

    Tcl_AppendResult(ip, tclout.c_str(), 0);

    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_scan_file_destroy(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "scan", ac, 2))
        return TCL_ERROR;

    scan_file_i *s = 0;
    if (read_ptr(ip, av[1], s)) return TCL_ERROR;

    if (s) delete s;

    return TCL_OK;
}


static int
t_scan_file_cursor(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "scan ", ac, 2))
        return TCL_ERROR;
   
    scan_file_i *s = 0;
    if (read_ptr(ip, av[1], s)) return TCL_ERROR;

    pin_i* p;
    bool eof=false;
    s->cursor(p,  eof); // void func
    if (eof) p = NULL;

    w_reset_strstream(tclout);
    format_ptr_null(tclout, p) << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);

    return TCL_OK;
}

static int
t_scan_file_next(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "scan start", ac, 3))
        return TCL_ERROR;
   
    smsize_t start = objectsize(av[2]);
    scan_file_i *s = 0;
    if (read_ptr(ip, av[1], s)) return TCL_ERROR;

    pin_i* p;
    bool eof=false;
    DO(s->next(p, start, eof));
    if (eof) p = NULL;

    w_reset_strstream(tclout);
    format_ptr_null(tclout, p) << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);

    return TCL_OK;
}


static int
t_scan_file_next_page(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "scan start", ac, 3))
        return TCL_ERROR;
   
    smsize_t start = objectsize(av[2]);
    scan_file_i *s = 0;
    if (read_ptr(ip, av[1], s)) return TCL_ERROR;

    pin_i* p;
    bool eof;
    DO(s->next_page(p, start, eof));
    if (eof) p = NULL;

    w_reset_strstream(tclout);
    format_ptr_null(tclout, p) << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);

    return TCL_OK;
}


static int
t_scan_file_finish(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "scan", ac, 2))
        return TCL_ERROR;
   
    scan_file_i *s = 0;
    if (read_ptr(ip, av[1], s)) return TCL_ERROR;

    s->finish();
    return TCL_OK;
}

static int
t_scan_file_append(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "scan hdr len_hint data", ac, 5)) 
        return TCL_ERROR;

    append_file_i *s = 0;
    if (read_ptr(ip, av[1], s)) return TCL_ERROR;
   
    vec_t hdr, data;
    hdr.set(parse_vec(av[2], strlen(av[2])));
    data.set(parse_vec(av[4], strlen(av[4])));

    rid_t    rid;

    w_reset_strstream(tclout);

    {
        DO( s->create_rec(hdr, objectsize(av[3]), data, rid) );
        tclout << rid << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_create_assoc(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid key el [keydescr] [concurrency]", ac, 4, 5, 6)) 
                 // 1    2  3   4         5
        return TCL_ERROR;
    
    vec_t key, el;
    int i; // used iff keytype is int/unsigned
    unsigned int u; // used iff keytype is int/unsigned

    w_base_t::int8_t il; // used iff keytype is int8_t 
    w_base_t::uint8_t ul; // used iff keytype is uint8_t
    
    key.put(av[2], strlen(av[2]));
    el.put(av[3], strlen(av[3]));

    ss_m::concurrency_t cc = ss_m::t_cc_kvl;
    rid_t rid;
    if (ac == 6) {
        cc = cvt2concurrency_t(av[5]);
        if(cc == ss_m::t_cc_im ) {
            // Interpret the element as a rid

            {
                w_istrstream anon2(av[3]); anon2 >> rid;
            }
            el.reset().put(&rid, sizeof(rid));
        } 
    }

    if (ac > 4) {
        const char *keydescr = (const char *)av[4];

        enum typed_btree_test t = cvt2type(keydescr);
        if (t == test_nosuch) {
            Tcl_AppendResult(ip, 
               "create_assoc: 4th argument must start with [bBiIuUfF]", 0);
            return TCL_ERROR;
                
        }
        switch(t) {
            case test_i4: {
                    i = strtol(av[2], 0, 0);
                    key.reset();
                    key.put(&i, sizeof(int));
                } break;

            case test_u4: {
                    u = strtoul(av[2], 0, 0);
                    key.reset();
                    key.put(&u, sizeof(u));
                }
                break;

            case test_i8: {
                    il = w_base_t::strtoi8(av[2]); 
                    key.reset();
                    key.put(&i, sizeof(w_base_t::int8_t));
                } break;
            case test_u8: {
                    ul = w_base_t::strtou8(av[2]);
                    key.reset();
                    key.put(&u, sizeof(w_base_t::uint8_t));
                }
                break;

            case test_bv:
            case test_b1:
            case test_b23:
                break;

            default:
                Tcl_AppendResult(ip, 
               "smsh.create_assoc function unsupported for given keytype:  ",
                   keydescr, 0);
            return TCL_ERROR;
        }
    }

    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        DO( sm->create_assoc(stid, key, el) );
    }        
    return TCL_OK;
}

static int
t_destroy_assoc(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid key el [keytype]", ac, 4, 5)) 
        return TCL_ERROR;
    
    vec_t key, el;
    
    key.put(av[2], strlen(av[2]));
    el.put(av[3], strlen(av[3]));

    int i;
    unsigned int u;

    if (ac > 4) {
        const char *keydescr = (const char *)av[4];

        // KLUDGE -- restriction for now
        // only because it's better than no check at all
        if(keydescr[0] != 'b' 
                && keydescr[0] != 'i'
                && keydescr[0] != 'u'
        ) {
            Tcl_AppendResult(ip, 
               "create_index: 4th argument must start with b,i or u", 0);
            return TCL_ERROR;
                
        }
        if( keydescr[0] == 'i' ) {
            i = atoi(av[2]);
            key.reset();
            key.put(&i, sizeof(int));
        } else if( keydescr[0] == 'u') {
            u = strtoul(av[2], 0, 0);
            key.reset();
            key.put(&u, sizeof(u));
        }
    }

    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        DO( sm->destroy_assoc(stid, key, el) );
    }
    
    return TCL_OK;
}
static int
t_destroy_all_assoc(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid key", ac, 3)) 
        return TCL_ERROR;
    
    vec_t key;
    
    key.put(av[2], strlen(av[2]));

    int num_removed;
    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        DO( sm->destroy_all_assoc(stid, key, num_removed) );
    }
    
    w_reset_strstream(tclout);
    tclout << num_removed << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_find_assoc(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid key [keytype]", ac, 3, 4))
        return TCL_ERROR;
    
    vec_t key;
    bool found;
    key.put(av[2], strlen(av[2]));

    int i;
    unsigned int u;
    
    if (ac > 3) {
        const char *keydescr = (const char *)av[3];

        // KLUDGE -- restriction for now
        // only because it's better than no check at all
        if(keydescr[0] != 'b' 
                && keydescr[0] != 'i'
                && keydescr[0] != 'u'
        ) {
            Tcl_AppendResult(ip, 
               "create_index: 4th argument must start with b,i or u", 0);
            return TCL_ERROR;
                
        }
        if( keydescr[0] == 'i' ) {
            i = atoi(av[2]);
            key.reset();
            key.put(&i, sizeof(int));
        } else if( keydescr[0] == 'u') {
            u = strtoul(av[2], 0, 0);
            key.reset();
            key.put(&u, sizeof(u));
        }
    }
#define ELSIZE 4044
    char *el = new char[ELSIZE];
    smsize_t elen = ELSIZE-1;

    w_rc_t ___rc;

    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        ___rc = sm->find_assoc(stid, key, el, elen, found);
    }
    if (___rc.is_error())  {
        w_reset_strstream(tclout);
        tclout << ___rc << ends;
        Tcl_AppendResult(ip, tclout.c_str(), 0);
        w_reset_strstream(tclout);
        delete[] el;
        return TCL_ERROR;
    }

    el[elen] = '\0';
    if (found) {
        Tcl_AppendElement(ip, TCL_CVBUG el);
        delete[] el;
        return TCL_OK;
    } 

    Tcl_AppendElement(ip, TCL_CVBUG "not found");
    delete[] el;
    return TCL_ERROR;
}

static int
t_create_md_index(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "vid dim ndxtype [\"tmp|regular|load_file|insert_file\"] ", 
                ac, 4, 5))
        return TCL_ERROR;

    int2_t dim = atoi(av[2]);

    ss_m::store_property_t property = ss_m::t_regular;
    if (ac > 4) {
        property = cvt2store_property(av[4]);
    }
    {
        stid_t stid;
        DO( sm->create_md_index(atoi(av[1]), cvt2ndx_t(av[3]),
                                property, stid, dim) );
        w_reset_strstream(tclout);
        tclout << stid << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_create_md_assoc(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid box el", ac, 4))
        return TCL_ERROR;

    vec_t el;
    nbox_t key(av[2]);

    el.put(av[3], strlen(av[3]) + 1);
    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        DO( sm->create_md_assoc(stid, key, el) );
    }

    return TCL_OK;
}


static int
t_find_md_assoc(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid box", ac, 3))
        return TCL_ERROR;

    nbox_t key(av[2]);
    bool found;

#define ELEN_ 80
    char el[ELEN_];
    smsize_t elen = sizeof(char) * ELEN_;

    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        DO( sm->find_md_assoc(stid, key, el, elen, found) );
    }
    if (found) {
      el[elen] = 0;        // null terminate the string
      Tcl_AppendElement(ip, TCL_CVBUG el);
    }
    return TCL_OK;
}


static int
t_destroy_md_assoc(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid box el", ac, 4))
        return TCL_ERROR;

    vec_t el;
    nbox_t key(av[2]);

    el.put(av[3], strlen(av[3]) + 1);
    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        DO( sm->destroy_md_assoc(stid, key, el) );
    }

    return TCL_OK;
}

static void boxGen(nbox_t* rect[], int number, const nbox_t& universe)
{
    register int i;
    int box[4];
    int length, width;

        rand48&  generator(get_generator());

    for (i = 0; i < number; i++) {
        // generate the bounding box
        length = width = 20;
        if (length > 10*width || width > 10*length)
             width = length = (width + length)/2;
        box[0] = ((int) (generator.drand() * (universe.side(0) - length)) 
                  + universe.side(0)) % universe.side(0) 
                + universe.bound(0);
        box[1] = ((int) (generator.drand() * (universe.side(1) - width)) 
                  + universe.side(1)) % universe.side(1)
                + universe.bound(1);
        box[2] = box[0] + length;
        box[3] = box[1] + width;
        rect[i] = new nbox_t(2, box); 
        }
}


/* Tool to hand out boxes in a consistent fashion so various resuls
   are repeatble.   The minder also takes care of deallocating resoursces
   on exit, fixing some memory leaks. */

struct BoxMinder {
        enum { box_count = 50 };

        bool        have_boxes;
        int        next;

        nbox_t        *box[box_count];

        BoxMinder()
        : have_boxes(false),
          next(-1)
        {
                for (unsigned i = 0; i < box_count; i++)
                        box[i] = 0;
        }

        ~BoxMinder()
        {
                for (unsigned i = 0; i < box_count; i++) {
                        if (box[i])
                                delete box[i];
                        box[i] = 0;
                }
                have_boxes = false;
                next = -1;
        }

        nbox_t        *next_box(const nbox_t &universe)
        {
                if (++next >= box_count) {
                        for (unsigned i = 0; i < box_count; i++)
                                delete box[i];
                        next = -1;
                        have_boxes = false;
                }

                if (!have_boxes) {
                        boxGen(box, box_count, universe);
                        next = 0;
                        have_boxes = true;
                }

                return box[next];
        }

};

static BoxMinder        boxes;

/*
 XXX why are 'flag' and 'i' static?  It breaks multiple threads.
 It's that way so jiebing can have repeatable box generation.
 In the future, the generator should be fixed.  There is also
 a problem with deleteing  possibly in-use boxes.
 */
static int
t_next_box(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if(num_tcl_threads_running() > 1) {
        fprintf(stderr, "t_next_box is not thread-safe\n");
        w_assert1(0);

        Tcl_AppendResult(ip, "t_next_box is not thread-safe");
        return TCL_ERROR;
    }

    if (check(ip, "universe", ac, 2))
        return TCL_ERROR;

    nbox_t        universe(av[1]);
    nbox_t        *box = 0;

    box = boxes.next_box(universe);

    Tcl_AppendElement(ip, TCL_CVBUG (*box));

    return TCL_OK;
}


static int
t_draw_rtree(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid", ac, 3))
        return TCL_ERROR;

    ofstream DrawFile(av[2]);
    if (!DrawFile) {
            w_rc_t        e = RC(fcOS);
        cerr << "Can't open draw file: " << av[2] << ":" << endl
                << e << endl;

        return TCL_ERROR;        /* XXX or should it be some other error? */
    }

    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        DO( sm->draw_rtree(stid, DrawFile) );
    }
    return TCL_OK;
}

static int
t_rtree_stats(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid", ac, 2))
        return TCL_ERROR;
    uint2_t level = 5;
    uint2_t ovp[5];
    rtree_stats_t stats;

    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;
        DO( sm->rtree_stats(stid, stats, level, ovp) );
    }

    if (linked.verbose_flag)  {
        cout << "rtree stat: " << endl;
        cout << stats << endl;
    }

    if (linked.verbose_flag)  {
        cout << "overlapping stats: ";
        for (uint i=0; i<stats.level_cnt; i++) {
            cout << " (" << i << ") " << ovp[i];
        }
        cout << endl;
    }
 
    return TCL_OK;
}

static int
t_rtree_scan(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid cond box [concurrency_t]", ac, 4, 5))
        return TCL_ERROR;

    nbox_t box(av[3]);
    nbox_t::sob_cmp_t cond = cvt2sob_cmp_t(av[2]);
    scan_rt_i* s = NULL;

    ss_m::concurrency_t cc = ss_m::t_cc_page;
    if (ac == 5) {
        cc = cvt2concurrency_t(av[4]);
    }

    {
        stid_t stid;
        w_istrstream anon(av[1]); anon >> stid;

        s = new scan_rt_i(stid, cond, box, cc);
    }
    if (!s) {
        cerr << "Out of memory: file " << __FILE__
             << " line " << __LINE__ << endl;
        W_FATAL(fcOUTOFMEMORY);
    }

    bool eof = false;
    char tmp[100];
    smsize_t elen = 100;
    nbox_t ret(box);

#ifdef SSH_VERBOSE
    if (linked.verbose_flag)  {
        cout << "------- start of query ----------\n";
        cout << "matching rects of " << av[2] 
             << " query for rect (" << av[3] << ") are: \n";
    }
#endif

    if ( s->next(ret, tmp, elen, eof).is_error() ) {
        delete s; return TCL_ERROR;
        }
    while (!eof) {
        if (linked.verbose_flag)  {
            ret.print(cout, 4);
#ifdef SSH_VERBOSE
            tmp[elen] = 0;
            cout << "\telem " << tmp << endl;
#endif
        }
        elen = 100;
        if ( s->next(ret, tmp, elen, eof).is_error() ) {
            delete s; return TCL_ERROR;
            }
        }

#ifdef SSH_VERBOSE
    if (linked.verbose_flag)  {
        cout << "-------- end of query -----------\n";
    }
#endif

    delete s;
    return TCL_OK;
}


static int
t_begin_sort_stream(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{

    const int vid_arg=1;
    const int runsize_arg=2;
    const int type_arg=3;
    const int rlen_arg=4;
    const int distinct_arg=5;
    const int updown_arg=6;
    const int universe_arg=7;
    const int derived_arg=8;
    const char *usage_string = 
    //1  2       3    4           5                 6              7        8      
    "vid runsize type rlen \"distinct/duplicate\" \"up/down\" [universe [derived] ]";

    if (check(ip, usage_string, ac, updown_arg+1, universe_arg+1, derived_arg+1))
        return TCL_ERROR;

    key_info_t info;
    sort_parm_t sp;
    
    info.offset = 0;
    info.where = key_info_t::t_hdr;

    switch(cvt2type(av[type_arg])) {
        case test_bv:
            // info.type = key_info_t::t_string;
            info.type = sortorder::kt_b;
            info.len = 0;
            break;
        case test_b1:
            // info.type = key_info_t::t_char;
            info.type = sortorder::kt_u1;
            info.len = sizeof(char);
            break;
        case test_i1:
            // new test
            info.type = sortorder::kt_i1;
            info.len = sizeof(int1_t);
            break;
        case test_i2:
            // new test
            info.type = sortorder::kt_i2;
            info.len = sizeof(int2_t);
            break;
        case test_i4:
            // info.type = key_info_t::t_int;
            info.type = sortorder::kt_i4;
            info.len = sizeof(int4_t);
            break;
        case test_i8:
            // NB: Tcl doesn't handle 64-bit arithmetic
            // info.type = key_info_t::t_int;
            info.type = sortorder::kt_i8;
            info.len = sizeof(w_base_t::int8_t);
            break;
        case test_u1:
            // new test
            info.type = sortorder::kt_u1;
            info.len = sizeof(uint1_t);
            break;
        case test_u2:
            // new test
            info.type = sortorder::kt_u2;
            info.len = sizeof(uint2_t);
            break;
        case test_u4:
            // new test
            info.type = sortorder::kt_u4;
            info.len = sizeof(uint4_t);
            break;
        case test_u8:
            // new test
            info.type = sortorder::kt_u8;
            info.len = sizeof(w_base_t::uint8_t);
            break;
        case test_f4:
            // info.type = key_info_t::t_float;
            info.type = sortorder::kt_f4;
            info.len = sizeof(f4_t);
            break;
        case test_f8:
            // new test
            info.type = sortorder::kt_f8;
            info.len = sizeof(f8_t);
            break;
        case test_spatial:
            // info.type = key_info_t::t_spatial;
            info.type = sortorder::kt_spatial;
            info.len = 0; // to be set from universe
            if (ac<=8) {
                if (check(ip, usage_string, 9, 10)) return TCL_ERROR;
            }
            break;
        default:
            W_FATAL(fcNOTIMPLEMENTED);
            break;
    }

    sp.run_size = atoi(av[runsize_arg]);
    sp.vol = atoi(av[vid_arg]);

    if (strcmp("normal", av[distinct_arg]))  {
        sp.unique = !strcmp("distinct", av[distinct_arg]);
        if (!sp.unique) {
            if (check(ip, usage_string, ac, 0)) return TCL_ERROR;
        }
    }

    if (strcmp("up", av[updown_arg]))  {
        sp.ascending = (strcmp("down", av[updown_arg]) != 0);
        if (sp.ascending) {
            if (check(ip, usage_string, ac, 0)) return TCL_ERROR;
        }
    }

    if (ac>universe_arg) {
        nbox_t univ(av[universe_arg]);
        info.universe = univ;
        info.len = univ.klen();
    }

    if (ac>derived_arg) {
        info.derived = (atoi(av[derived_arg]) != 0);
    }

    sort_stream_i* res = replace_sort_container( 
                    new sort_stream_i(info, sp, atoi(av[rlen_arg])));
    
    w_assert0(res);
    return TCL_OK;
}
    
static int
t_sort_stream_put(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    // Puts typed key in header, string data in body
    if (check(ip, "type key data", ac, 4))
        return TCL_ERROR;

    sort_stream_i* sort_container(get_sort_container());

    if (!sort_container)
        return TCL_ERROR;

    vec_t key, data;

    typed_value v;
    cvt2typed_value( cvt2type(av[1]), av[2], v);

    key.put(&v._u, v._length);

    data.put(av[3], strlen(av[3]));

    DO( sort_container->put(key, data) );
    return TCL_OK;
}

static int
t_sort_stream_get(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "next type", ac, 3))
        return TCL_ERROR;

    if (strcmp(av[1],"next")) {
      if (check(ip, "next", ac, 0))
        return TCL_ERROR;
    }

    vec_t key, data;
    bool eof = true;
    sort_stream_i* sort_container(get_sort_container());

    if (sort_container) {
        DO( sort_container->get_next(key, data, eof) );
    }

    if (eof) {
        Tcl_AppendElement(ip, TCL_CVBUG "eof");
                (void) replace_sort_container(NULL);
    } else {
        typed_value v;

        typed_btree_test t = cvt2type(av[2]);
        w_reset_strstream(tclout);

        switch(t) {
        case test_bv:{
                char* buf= new char [key.size()];
                key.copy_to(buf, key.size());

                tclout <<  buf;
                delete[] buf;
            }
            break;

        case test_spatial:
            W_FATAL(fcNOTIMPLEMENTED);
            break;

        default:
            key.copy_to((void*)&v._u, key.size());
            v._length = key.size();
            break;
        }
        switch ( t ) {
        case  test_bv:
            break;

        case  test_b1:
            tclout <<  v._u.b1;
            break;

        case  test_i1:
            tclout << (int)v._u.i1_num;
            break;

        case  test_u1:
            tclout << (unsigned int)v._u.u1_num ;
            break;

        case  test_i2:
            tclout << v._u.i2_num ;
            break;

        case  test_u2:
            tclout << v._u.u2_num ;
            break;

        case  test_i4:
            tclout << v._u.i4_num ;
            break;
        case  test_u4:
            tclout << v._u.u4_num ;
            break;

        case  test_i8:
            tclout << v._u.i8_num ;
            break;
        case  test_u8:
            tclout << v._u.u8_num ;
            break;
        case  test_f8:
            tclout << v._u.f8_num ;
            break;

        case  test_f4:
            tclout << v._u.f4_num ;
            break;

        case test_spatial:
        default:
            W_FATAL(fcNOTIMPLEMENTED);
            break;
        }
        {
            char* buf = new char[data.size()+1];
            data.copy_to(buf, data.size());
            buf[data.size()] = '\0';

            tclout << " "  << (char *)&buf[0] ;
                delete[] buf;
        }
        tclout << ends;
            Tcl_AppendResult(ip, tclout.c_str(), 0);
        w_reset_strstream(tclout);
    }

    return TCL_OK;
}

static int
t_sort_stream_end(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
        (void) replace_sort_container(NULL);

    // keep compiler quiet about unused parameters
    if (ip || ac || av) {}
    return TCL_OK;
}



static int
t_link_to_remote_id(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "local_volume remote_id", ac, 3))
        return TCL_ERROR;

    {
        cout << "WARNING: link_to_remote_id not supported: no logical IDs" << endl;
        w_reset_strstream(tclout);
        tclout << av[2] << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_convert_to_local_id(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "remote_id", ac, 2))
        return TCL_ERROR;

    {
        cout << "WARNING: convert_to_local_id not supported: no logical IDs"
             << endl;
        w_reset_strstream(tclout);
        tclout << av[1] << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_lfid_of_lrid(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "lrid", ac, 2))
        return TCL_ERROR;

    {
        cout << "WARNING: t_lfid_of_lrid not supported: logical IDs" << endl;
        w_reset_strstream(tclout);
        tclout << av[1] << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}




static int
t_lock(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "entity mode [duration [timeout]]", ac, 3, 4, 5) )
        return TCL_ERROR;

    rc_t rc; // return code
    lock_mode_t mode = cvt2lock_mode(av[2]);

    {
        lockid_t n;
        cvt2lockid_t(av[1], n);
        
        if (ac == 3)  {
            DO( sm->lock(n, mode) );
        } else {
            lock_duration_t duration = cvt2lock_duration(av[3]);
            if (ac == 4) {
                DO( sm->lock(n, mode, duration) );
            } else {
                long timeout = atoi(av[4]);
                DO( sm->lock(n, mode, duration, timeout) );
            }
        }
    }
    return TCL_OK;
}

static int
t_dont_escalate(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "entity [\"dontPassOnToDescendants\"]", ac, 2, 3))
        return TCL_ERROR;
    
    bool        passOnToDescendants = true;
    rc_t        rc;

    if (ac == 3)  {
        if (strcmp(av[2], "dontPassOnToDescendants"))  {
            Tcl_AppendResult(ip, "last parameter must be \"dontPassOnToDescendants\", not ", av[2], 0);
            return TCL_ERROR;
        }  else  {
            passOnToDescendants = false;
        }
    }

    {
        lockid_t n;
        cvt2lockid_t(av[1], n);
        DO( sm->dont_escalate(n, passOnToDescendants) );
    }

    return TCL_OK;
}

static int
t_get_escalation_thresholds(Tcl_Interp* ip, int ac, TCL_AV char* [])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;

    int4_t        toPage = 0;
    int4_t        toStore = 0;
    int4_t        toVolume = 0;

    DO( sm->get_escalation_thresholds(toPage, toStore, toVolume) );

    if (toPage == smlevel_0::dontEscalate)
        toPage = 0;
    if (toStore == smlevel_0::dontEscalate)
        toStore = 0;
    if (toVolume == smlevel_0::dontEscalate)
        toVolume = 0;

    w_reset_strstream(tclout);
    tclout << toPage << " " << toStore << " " << toVolume << ends;
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int4_t
parse_escalation_thresholds(const char *s)
{
    int                result = atoi(s);

    if (result == 0)
        result = smlevel_0::dontEscalate;
    else if (result < 0)
        result = smlevel_0::dontModifyThreshold;
    
    return result;
}

static int
t_set_escalation_thresholds(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    int                        listArgc;
    TCL_SLIST char        **listArgv;
    static const char                *errString = 
    "{toPage, toStore, toVolume} (0 =don't escalate, <0 =don't modify)";

    if (check(ip, errString, ac, 2))
        return TCL_ERROR;
    
    if (Tcl_SplitList(ip, av[1], &listArgc, &listArgv) != TCL_OK)
        return TCL_ERROR;
    
    if (listArgc != 3)  {
        Tcl_SetResult(ip, (char *) errString, TCL_STATIC);
        Tcl_Free( (char *) listArgv);
        return TCL_ERROR;
    }

    int4_t        toPage = parse_escalation_thresholds(listArgv[0]);
    int4_t        toStore = parse_escalation_thresholds(listArgv[1]);
    int4_t        toVolume = parse_escalation_thresholds(listArgv[2]);

    Tcl_Free( (char *) listArgv);

    DO( sm->set_escalation_thresholds(toPage, toStore, toVolume) );

    return TCL_OK;
}


static int
t_lock_many(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "num_requests entity mode [duration [timeout]]", 
              ac, 4, 5, 6) )
        return TCL_ERROR;

    rc_t rc; // return code
    smsize_t num_requests = numobjects(av[1]);
    smsize_t i;
    lock_mode_t mode = cvt2lock_mode(av[3]);

    {
        lockid_t n;
        cvt2lockid_t(av[2], n);
        
        if (ac == 4)  {
            for (i = 0; i < num_requests; i++)
                DO( sm->lock(n, mode) );
        } else {
            lock_duration_t duration = cvt2lock_duration(av[4]);
            if (ac == 5) {
                for (i = 0; i < num_requests; i++)
                    DO( sm->lock(n, mode, duration) );
            } else {
                long timeout = atoi(av[5]);
                for (i = 0; i < num_requests; i++)
                    DO( sm->lock(n, mode, duration, timeout) );
            }
        }
    }
    return TCL_OK;
}

static int
t_unlock(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "entity", ac, 2) )
        return TCL_ERROR;

    rc_t rc; // return code

    {
        lockid_t n;
        cvt2lockid_t(av[1], n);
        DO( sm->unlock(n) );
    }
    return TCL_OK;
}

extern "C" void dumpthreads();

static int
t_dump_threads(Tcl_Interp* ip, int ac, TCL_AV char*[])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;

    dumpthreads();

    return TCL_OK;
}

static int
t_dump_histo(Tcl_Interp *ip, int ac, TCL_AV char *[])
{
        if (check(ip, "", ac, 1))
                return TCL_ERROR;

        DO(sm->dump_histo(cout, false));

        return TCL_OK;
}


static int
t_dump_locks(Tcl_Interp* ip, int ac, TCL_AV char*[])
{
    if (check(ip, "", ac, 1))
        return TCL_ERROR;

    DO( sm->dump_locks(cout) );
    return TCL_OK;
}

static int
t_query_lock(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "entity", ac, 2) )
        return TCL_ERROR;

    rc_t rc; // return code
    lock_mode_t m = w_base_t::NL;

    {
        lockid_t n;
        cvt2lockid_t(av[1], n);
        DO( sm->query_lock(n, m) );
    }

    Tcl_AppendResult(ip, lock_base_t::mode_str[m], 0);
    return TCL_OK;
}

static int
t_set_lock_cache_enable(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "enable/disable)", ac, 2) )
        return TCL_ERROR;

    bool enable;
    if (strcmp(av[1], "enable") == 0) {
        enable = true;
    } else if (strcmp(av[1], "disable") == 0) {
        enable = false;
    } else {
        Tcl_AppendResult(ip, "wrong first arg, should be enable/disable", 0);
        return TCL_ERROR;
    }

    DO(sm->set_lock_cache_enable(enable));
    return TCL_OK;
}

static int
t_lock_cache_enabled(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "", ac, 1) )
        return TCL_ERROR;
    // keep compiler quiet about unused parameters
    if (av) {}

    bool enabled;
    DO(sm->lock_cache_enabled(enabled));
    Tcl_AppendResult(ip, tcl_form_bool(enabled), 0);

    return TCL_OK;
}


static int
t_create_many_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "num_recs fid hdr len_hint chunkdata chunkcount", ac, 7)) 
        return TCL_ERROR;
   
    smsize_t num_recs = numobjects(av[1]);
    smsize_t chunk_count = objectsize(av[6]);
    smsize_t len_hint = objectsize(av[4]);
    vec_t hdr, data;
    hdr.put(av[3], strlen(av[3]));
    data.put(av[5], strlen(av[5]));

    
    {
        stid_t  stid;
        rid_t   rid;
        
        w_istrstream anon2(av[2]); anon2 >> stid;
        cout << "creating " << num_recs << " records in " << stid
             << " with hdr_len= " << hdr.size() << " chunk_len= "
             << data.size() << " in " << chunk_count << " chunks."
             << endl;

        for (uint i = 0; i < num_recs; i++) {
            DO( sm->create_rec(stid, hdr, len_hint, data, rid) );
            for (uint j = 1; j < chunk_count; j++) {
                DO( sm->append_rec(rid, data));
            }
        }
        w_reset_strstream(tclout);
        tclout << rid << ends;
    }
    Tcl_AppendResult(ip, tclout.c_str(), 0);
    w_reset_strstream(tclout);
    return TCL_OK;
}

static int
t_update_rec_many(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "num_updates rid start data", ac, 5)) 
        return TCL_ERROR;
    
    smsize_t   start;
    vec_t   data;
    smsize_t            num_updates = numobjects(av[1]);

    data.set(parse_vec(av[4], strlen(av[4])));
    start = objectsize(av[3]);    

    {
        rid_t   rid;

        w_istrstream anon2(av[2]); anon2 >> rid;
        for (smsize_t i = 0; i < num_updates; i++) {
            DO( sm->update_rec(rid, start, data) );
        }
    }

    Tcl_AppendElement(ip, TCL_CVBUG "update success");
    return TCL_OK;
}


static int
t_lock_record_blind(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "num", ac, 2))
        return TCL_ERROR;

    const int page_capacity = 20;
    rid_t rid;
    rid.pid._stid.vol = 999;
    uint4_t n;
    w_istrstream anon(av[1]); anon >> n;

    stime_t start(stime_t::now());
    
    for (uint i = 0; i < n; i += page_capacity)  {
        rid.pid.page = i+1;
        uint j = page_capacity;
        if (i + j >= n) j = n - i; 
        while (j--) {
            rid.slot = j;
            DO( sm->lock(rid, w_base_t::EX) );
        }
    }

    sinterval_t delta(stime_t::now() - start);
    cerr << "Time to get " << n << " record locks: "
            << delta << " seconds" << endl;

    return TCL_OK;
}


static int
t_testing(Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    if (check(ip, "stid key times", ac, 4))
        return TCL_ERROR;

    stid_t stid;
    w_istrstream anon(av[1]); anon >> stid;

    //align the key
    char* key_align = strdup(av[2]);
    w_assert1(key_align);

    vec_t key;
    key.set(key_align, strlen(key_align));

    int times = atoi(av[3]);

    char el[80];
    smsize_t elen = sizeof(el) - 1;
    
    bool found;
    for (int i = 0; i < times; i++)  {
        DO( sm->find_assoc(stid, key, el, elen, found) );
    }

    free(key_align);
    el[elen] = '\0';

    if (found)  {
        Tcl_AppendElement(ip, TCL_CVBUG el);
        return TCL_OK;
    }

    Tcl_AppendElement(ip, TCL_CVBUG "not found");
    return TCL_ERROR;
}

#include <vtable.h>
#include <sthread_vtable_enum.h>
#include <sm_vtable_enum.h>
static  void
__printit(const char *what, vtable_t  &x) {

    // Too much to fit in tclout
    if (linked.verbose_flag)  {
        /* method 1: */
        cout << what <<
            " METHOD 1 - individually: n=" << x.quant() <<endl;
        for(int i=0; i< x.quant(); i++) {
            vtable_row_t & row(x[i]);
            cout << "------------- first=0, last=" << row.quant() 
                << " (printing only non-null strings whose value != \"0\") "
                <<endl;
            for(int j=0; j < row.quant(); j++) {
                if(strlen(row[j]) > 0) {
                    if(strcmp(row[j], "0") != 0) {
                    cout << i << "." << j << ":" << row[j]<< "|" << endl;
                    }
                }
            }
        }
        cout << endl;
    }

    /* method 2: */
    // cout << what << " METHOD 2 - group" <<endl;
    // x.operator<<(cout);
}

static int
t_vt_thread(Tcl_Interp* ip, int , TCL_AV char* [])
{
    vtable_t  x;
    DO(ss_m::thread_collect(x));
    __printit("threads", x);
    // Tcl_AppendResult(ip, tclout.c_str(), 0);
    return TCL_OK;
}

static int
t_vt_stats(Tcl_Interp* ip, int , TCL_AV char* [])
{
    Tcl_AppendResult(ip, "vtable stats: not yet implemented", 0);
    return TCL_ERROR;
}

static int
t_vt_xct(Tcl_Interp* ip, int , TCL_AV char* [])
{
    vtable_t  x;
    DO(ss_m::xct_collect(x));

    __printit("transactions/xct", x);
    // Tcl_AppendResult(ip, tclout.c_str(), 0);
    return TCL_OK;
}

static int
t_vt_bpool(Tcl_Interp* ip, int , TCL_AV char* [])
{
    vtable_t  x;
    DO(ss_m::bp_collect(x));

    __printit("buffer pool", x);
    // Tcl_AppendResult(ip, tclout.c_str(), 0);
    return TCL_OK;
}

static int
t_vt_lock(Tcl_Interp* ip, int , TCL_AV char* [])
{
    vtable_t  x;
    DO(ss_m::lock_collect(x));

    __printit("locks", x);
    // Tcl_AppendResult(ip, tclout.c_str(), 0);
    return TCL_OK;
}

static int
t_st_stats(Tcl_Interp *, int argc, TCL_AV char **)
{
        if (argc != 1)
                        return TCL_ERROR;

        sthread_t::dump_stats(cout);
        return TCL_OK;
}

static int
t_st_check_stack(Tcl_Interp *, int argc, TCL_AV char **)
{
    if (argc != 1)
        return TCL_ERROR;

    check_sp(__FILE__, __LINE__);
    return TCL_OK;
}

static int
t_st_rc(Tcl_Interp *, int argc, TCL_AV char **argv)
{
    if (argc < 2)
                return TCL_ERROR;

    /* XXXX should this output to normal TCL output ??? */
    for (int i = 1; i < argc; i++) {
        w_rc_t        e = w_rc_t("error", 0, atoi(argv[i]));
        cout << "RC(" << argv[i] << "):" <<
                        endl << e << endl;
    }

    return TCL_OK;
}

// XXX in its own file since it includes almost the whole world to
// grab data structures to size.
extern int t_sm_sizeof(Tcl_Interp *, int argc, TCL_AV char **argv);


struct cmd_t {
    const char* name;
    smproc_t* func;
};


/* these are not marked const because they do get sorted in dispatch_init(),
 * which is called from main; otherwise, they are essentially static const
 */
static cmd_t cmd[] = {
    { "testing", t_testing },
    { "sleep", t_sleep },
    { "test_int_btree", t_test_int_btree },
    { "test_typed_btree", t_test_typed_btree },
    { "test_bulkload_int_btree", t_test_bulkload_int_btree },
    { "test_bulkload_rtree", t_test_bulkload_rtree },

    { "checkpoint", t_checkpoint },

    // thread/xct related
    { "begin_xct", t_begin_xct },
    { "abort_xct", t_abort_xct },
    { "commit_xct", t_commit_xct },
    { "chain_xct", t_chain_xct },
    { "save_work", t_save_work },
    { "rollback_work", t_rollback_work },
    { "open_quark", t_open_quark },
    { "close_quark", t_close_quark },
    { "xct", t_xct },  // get current transaction
    { "attach_xct", t_attach_xct},
    { "detach_xct", t_detach_xct},
    { "state_xct", t_state_xct},
    { "dump_xcts", t_dump_xcts},
    { "dump_latches", t_dump_latches},
    { "xct_to_tid", t_xct_to_tid},
    { "tid_to_xct", t_tid_to_xct},
    { "prepare_xct", t_prepare_xct},
    { "enter2pc", t_enter2pc},
    { "set_coordinator", t_set_coordinator}, // extern 2pc
    { "recover2pc", t_recover2pc},
    { "num_prepared", t_num_prepared},
    { "num_active", t_num_active},
    { "lock_timeout", t_lock_timeout},

    //
    // basic sm stuff
    //
    { "reinit_fingerprints", t_reinit_fingerprints },
    { "force_buffers", t_force_buffers },
    { "force_vol_hdr_buffers", t_force_vol_hdr_buffers },
    { "gather_stats", t_gather_stats },
    { "gather_xct_stats", t_gather_xct_stats },
    { "mem_stats", t_mem_stats },
    { "snapshot_buffers", t_snapshot_buffers },
    { "config_info", t_config_info },
    { "set_disk_delay", t_set_disk_delay },
    { "start_log_corruption", t_start_log_corruption },
    { "sync_log", t_sync_log },
    { "dump_buffers", t_dump_buffers },
    { "set_store_property", t_set_store_property },
    { "get_store_property", t_get_store_property },
    { "get_lock_level", t_get_lock_level },
    { "set_lock_level", t_set_lock_level },

    // Device and Volume
    { "format_dev", t_format_dev },
    { "mount_dev", t_mount_dev },
    { "unmount_dev", t_dismount_dev },
    { "unmount_all", t_dismount_all },
    { "dismount_dev", t_dismount_dev },
    { "dismount_all", t_dismount_all },
    { "list_devices", t_list_devices },
    { "list_volumes", t_list_volumes },
    { "create_vol", t_create_vol },
    { "destroy_vol", t_destroy_vol },
    { "has_logical_id_index", t_has_logical_id_index },
    { "get_volume_quota", t_get_volume_quota },
    { "get_device_quota", t_get_device_quota },
    { "vol_root_index", t_vol_root_index },
    { "get_volume_meta_stats", t_get_volume_meta_stats},
    { "get_file_meta_stats", t_get_file_meta_stats},
    { "check_volume", t_check_volume }, // prints stuff

    { "get_du_statistics", t_get_du_statistics },   // DU DF
    { "check_volume_page_types", t_check_volume_page_types },   
    //
    // Debugging
    //
    { "set_debug", t_set_debug },
    { "purify_print_string", t_purify_print_string },
    //
    //        Indices
    //
    { "create_index", t_create_index },
    { "destroy_index", t_destroy_index },
    { "destroy_md_index", t_destroy_md_index },
    { "blkld_ndx", t_blkld_ndx },
    { "create_assoc", t_create_assoc },
    { "destroy_all_assoc", t_destroy_all_assoc },
    { "destroy_assoc", t_destroy_assoc },
    { "find_assoc", t_find_assoc },
    { "find_assoc_typed", t_find_assoc_typed },
    { "print_index", t_print_index },

    // R-Trees
    { "create_md_index", t_create_md_index },
    { "create_md_assoc", t_create_md_assoc },
    { "find_md_assoc", t_find_md_assoc },
    { "destroy_md_assoc", t_destroy_md_assoc },
    { "next_box", t_next_box },
    { "draw_rtree", t_draw_rtree },
    { "rtree_stats", t_rtree_stats },
    { "rtree_query", t_rtree_scan },
    { "blkld_md_ndx", t_blkld_md_ndx },
    { "sort_file", t_sort_file },
    { "compare_typed", t_compare_typed },
    { "create_typed_hdr_body_rec", t_create_typed_hdr_body_rec },
    { "create_typed_hdr_rec", t_create_typed_hdr_rec },
    { "get_store_info", t_get_store_info },
    { "scan_sorted_recs", t_scan_sorted_recs },
    { "print_md_index", t_print_md_index },

    { "begin_sort_stream", t_begin_sort_stream },
    { "sort_stream_put", t_sort_stream_put },
    { "sort_stream_get", t_sort_stream_get },
    { "sort_stream_end", t_sort_stream_end },

    //
    //  Scans
    //
    { "create_scan", t_create_scan },
    { "scan_next", t_scan_next },
    { "destroy_scan", t_destroy_scan },
    //
    // Files, records
    //
    { "create_file", t_create_file },
    { "destroy_file", t_destroy_file },
    { "create_rec", t_create_rec },
    { "create_multi_recs", t_create_multi_recs },
    { "multi_file_append", t_multi_file_append },
    { "multi_file_update", t_multi_file_update },
    { "destroy_rec", t_destroy_rec },
    { "read_rec", t_read_rec },
    { "read_hdr", t_read_hdr },
    { "print_rec", t_print_rec },
    { "read_rec_body", t_read_rec_body },
    { "update_rec", t_update_rec },
    { "update_rec_hdr", t_update_rec_hdr },
    { "append_rec", t_append_rec },
    { "truncate_rec", t_truncate_rec },
    { "scan_recs", t_scan_recs },
    { "scan_rids", t_scan_rids },

    // Pinning
    { "pin_create", t_pin_create },
    { "pin_destroy", t_pin_destroy },
    { "pin_pin", t_pin_pin },
    { "pin_unpin", t_pin_unpin },
    { "pin_repin", t_pin_repin },
    { "pin_body", t_pin_body },
    { "pin_next_bytes", t_pin_next_bytes },
    { "pin_hdr", t_pin_hdr },
    { "pin_page_containing", t_page_containing },
    { "pin_pinned", t_pin_pinned },
    { "pin_is_small", t_pin_is_small },
    { "pin_large_impl", t_pin_large_impl },
    { "pin_rid", t_pin_rid },
    { "pin_append_rec", t_pin_append_rec },
    { "pin_update_rec", t_pin_update_rec },
    { "pin_update_rec_hdr", t_pin_update_rec_hdr },
    { "pin_truncate_rec", t_pin_truncate_rec },

    // Scanning files
    { "scan_file_create", t_scan_file_create },
    { "scan_file_destroy", t_scan_file_destroy },
    { "scan_file_next", t_scan_file_next },
    { "scan_file_cursor", t_scan_file_cursor },
    { "scan_file_next_page", t_scan_file_next_page },
    { "scan_file_finish", t_scan_file_finish },
    { "scan_file_append", t_scan_file_append },

    // Logical ID related
    { "link_to_remote_id", t_link_to_remote_id },
    { "convert_to_local_id", t_convert_to_local_id },
    { "lfid_of_lrid", t_lfid_of_lrid },

    // Lock
    { "lock", t_lock },
    { "lock_many", t_lock_many },
    { "unlock", t_unlock },
    { "dump_locks", t_dump_locks },
    { "query_lock", t_query_lock },
    { "set_lock_cache_enable", t_set_lock_cache_enable },
    { "lock_cache_enabled", t_lock_cache_enabled },

    { "dont_escalate", t_dont_escalate },
    { "get_escalation_thresholds", t_get_escalation_thresholds },
    { "set_escalation_thresholds", t_set_escalation_thresholds },

    { "restart", t_restart },
    // crash name "command"
    { "crash", t_crash },

    { "multikey_sort_file", t_multikey_sort_file },

    { "dump_threads", t_dump_threads },
    { "dump_histo", t_dump_histo },

    { "sizeof", t_sm_sizeof },

    //
    // Performance tests
    //   Name format:
    //     OP_many_rec = perform operation on many records
    //     OP_rec_many = perform operation on 1 record many times
    //
    { "create_many_rec", t_create_many_rec },
    { "update_rec_many", t_update_rec_many },
    { "lock_record_blind", t_lock_record_blind },

    //{ 0, 0}
};

static cmd_t vtable_cmd[] = {
    { "thread", t_vt_thread },
    { "lock", t_vt_lock },
    { "bp", t_vt_bpool },
    { "xct", t_vt_xct },
    { "stat", t_vt_stats },
    { "stats", t_vt_stats },

    //{ 0, 0}
};


static cmd_t        sthread_cmd[] = {
        { "checkstack", t_st_check_stack },
        { "stats", t_st_stats },
        { "rc", t_st_rc },
//        { "events", t_st_events },
//        { "threads", t_st_threads }
};


static int cmd_cmp(const void* c1, const void* c2) 
{
    cmd_t& cmd1 = *(cmd_t*) c1;
    cmd_t& cmd2 = *(cmd_t*) c2;
    return strcmp(cmd1.name, cmd2.name);
}

#ifndef numberof
#define        numberof(x)        (sizeof(x) / sizeof(x[0]))
#endif

void dispatch_init() 
{
    qsort( (char*)sthread_cmd, numberof(sthread_cmd), sizeof(cmd_t), cmd_cmp);
    qsort( (char*)vtable_cmd, numberof(vtable_cmd), sizeof(cmd_t), cmd_cmp);
    qsort( (char*)cmd, numberof(cmd), sizeof(cmd_t), cmd_cmp);
}


int
_dispatch(const cmd_t *_cmd,
          size_t sz,
          const char *module,
          Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    int ret = TCL_OK;
    cmd_t key;

    w_assert3(ac >= 2);        
    key.name = av[1];
    cmd_t* found = (cmd_t*) bsearch((char*)&key, 
                                    (char*)_cmd, 
                                    sz/sizeof(cmd_t), 
                                    sizeof(cmd_t), cmd_cmp);
    if (found) {
        SSH_CHECK_STACK;
        ret = found->func(ip, ac - 1, av + 1);
        SSH_CHECK_STACK;
        return ret;
    }

    w_reset_strstream(tclout);
    tclout << __FILE__ << ':' << __LINE__  << ends;
    Tcl_AppendResult(ip, tclout.c_str(), " unknown ", module, "  routine \"",
                     av[1], "\"", 0);
    w_reset_strstream(tclout);
    return TCL_ERROR;
}

extern "C" int st_dispatch(ClientData, Tcl_Interp *tcl, int argc, TCL_AV char **argv)
{
    return _dispatch(sthread_cmd, sizeof(sthread_cmd), "st",
             tcl, argc, argv);
}

extern "C" int
vtable_dispatch(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    return _dispatch( vtable_cmd,  sizeof(vtable_cmd), "vtable", ip, ac, av);
}

extern "C" int
sm_dispatch(ClientData, Tcl_Interp* ip, int ac, TCL_AV char* av[])
{
    return _dispatch( cmd,  sizeof(cmd), "sm", ip, ac, av);
}


void check_sp(const char *file, int line)
{
    if( smthread_t::me()->isStackOK(file, line) == false) 
    {
        W_FATAL(fcINTERNAL);
    }
    return;
}

smsize_t   
objectsize(const char *str)
{
    return  strtoul(str,0,0);
}

smsize_t   
numobjects(const char *str)
{
    return  strtoul(str,0,0);
}

static const struct nda_t {
    const char*         name;
ss_m::ndx_t         type;
} nda[] = {
    { "btree",        ss_m::t_btree },
    { "uni_btree",         ss_m::t_uni_btree },
    { "rtree",        ss_m::t_rtree },
    { 0,                ss_m::t_bad_ndx_t }
};

ss_m::ndx_t 
cvt2ndx_t(const char *s)
{
    for (const nda_t* p = nda; p->name; p++)  {
        if (streq(s, p->name))  return p->type;
    }
    return sm->t_bad_ndx_t;
}

