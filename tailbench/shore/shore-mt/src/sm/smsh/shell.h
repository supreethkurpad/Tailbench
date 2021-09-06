/*<std-header orig-src='shore' incl-file-exclusion='SHELL_H'>

 $Id: shell.h,v 1.50 2010/05/26 01:20:51 nhall Exp $

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

#ifndef SHELL_H
#define SHELL_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 * Everything common to shell.cpp, shell2.cpp, etc
 */


#include <sm_vas.h>
#include "smsh.h"
// DEAD #include "w_random.h"
#include "rand48.h"
#include <cstring>
#include "smsh_error.h"
#undef EXTERN
#include <tcl.h>
#include "tcl_workaround.h"
#include "tcl_thread.h"

#ifdef PURIFY
#include <purify.h>
#endif

using namespace ssm_sort;
extern bool newsort;
extern sm_config_info_t global_sm_config_info;
extern __thread rand48 generator;

extern void check_sp(const char *, int);
#define    SSH_CHECK_STACK    check_sp(__FILE__, __LINE__)

#define SSH_VERBOSE 

extern ss_m* sm;

typedef int smproc_t(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
const int MAXRECLEN = 1000;

typedef w_base_t::uint1_t  uint1_t;
typedef w_base_t::uint2_t  uint2_t;
typedef w_base_t::uint4_t  uint4_t;
// NB: can't do this because it's defined in sys include files:
// typedef w_base_t::uint8_t  uint8_t;
typedef w_base_t::int1_t  int1_t;
typedef w_base_t::int2_t  int2_t;
typedef w_base_t::int4_t  int4_t;
// NB: can't do this because it's defined in sys include files:
// typedef w_base_t::int8_t  int8_t;
typedef w_base_t::f4_t  f4_t;
typedef w_base_t::f8_t  f8_t;

/* XXX  The typed value doesn't know its own type, and it can
   point to memory that it doesn't control.  Perhaps it should
   be more self-contained.   However it works as is. */

struct typed_value {
    int _length;
    union {
    const char    *bv;
    char    b1;
    w_base_t::uint8_t u8_num;
    w_base_t::int8_t  i8_num;
    uint4_t u4_num;
    int4_t  i4_num;
    uint2_t u2_num;
    int2_t  i2_num;
    uint1_t u1_num;
    int1_t  i1_num;
    f4_t    f4_num;
    f8_t    f8_num;
    char    polygon[sizeof(nbox_t)]; // for rtree test
    } _u; 
};

extern w_ostrstream &get_tclout(); /* shell.cpp */

#define tclout get_tclout()

#if defined(W_DEBUG) || defined(SSH_DUMPRC)
extern bool dumprc; // in shell.cpp
#define DUMPRC(a) if(dumprc) { cerr << a << endl; } else 
#else
#define DUMPRC(a)
#endif

#undef DO
#define DO(x)                            \
{                                \
    w_rc_t ___rc = x;                        \
    if (___rc.is_error())  {                        \
        w_reset_strstream(tclout);                              \
        DUMPRC(___rc);                                          \
        tclout << smsh_err_name(___rc.err_num()) << ends;    \
        Tcl_AppendResult(ip, tclout.c_str(), 0);        \
        w_reset_strstream(tclout);                              \
        return TCL_ERROR;                    \
    }                                \
}

#undef DO_GOTO
#define DO_GOTO(x)                          \
{                                \
    w_rc_t ___rc = x;                        \
    if (___rc.is_error())  {                        \
    w_reset_strstream(tclout);                              \
    tclout << smsh_err_name(___rc.err_num()) << ends;    \
    Tcl_AppendResult(ip, tclout.c_str(), 0);        \
    w_reset_strstream(tclout);                              \
    goto failure;                        \
    }                                \
}

#define TCL_HANDLE_FSCAN_FAILURE(f_scan)             \
    if((f_scan==0) || (f_scan->error_code().is_error())) {     \
       w_rc_t err;                         \
       if(f_scan != 0) {                      \
        err = RC(f_scan->error_code().err_num());          \
        delete f_scan; f_scan =0;                  \
    } else {                         \
        err = RC(fcOUTOFMEMORY);                 \
    }                             \
    DO(err);                        \
    }

#define HANDLE_FSCAN_FAILURE(f_scan)                \
    if((f_scan==0) || (f_scan->error_code().is_error())) {    \
       w_rc_t err;                         \
       if(f_scan) {                         \
        err = RC(f_scan->error_code().err_num());          \
        delete f_scan; f_scan =0;                  \
        return err;                     \
    } else {                         \
        return RC(fcOUTOFMEMORY);                 \
    }                             \
    }

extern "C"  void clean_up_shell();
extern rand48 &get_generator(); /* shell.cpp */
    
extern smsize_t   objectsize(const char *);
extern smsize_t   numobjects(const char *);

extern void   dump_pin_hdr(ostream &out, pin_i &handle); 
extern void   dump_pin_body(w_ostrstream &out, pin_i &handle,
    smsize_t start, smsize_t length, Tcl_Interp *ip); 

typedef void (*DSCB) (pin_i &);
extern w_rc_t dump_scan(scan_file_i &scan, ostream &out, 
		DSCB callback = NULL,
		bool hex=false); 
extern vec_t & parse_vec(const char *c, int len) ;
extern vid_t make_vid_from_lvid(const char* lv);
extern ss_m::ndx_t cvt2ndx_t(const char *s);
extern lockid_t cvt2lockid_t(const char *str);
extern bool use_logical_id(Tcl_Interp* ip);
extern const char * check_compress_flag(const char *);


inline const char *tcl_form_flag(int flag)
{
    return flag ? "1" : "0";
}

extern int tcl_scan_flag(const char *s, bool &flag);

inline const char *tcl_form_bool(bool flag)
{
    return flag ? "TRUE" : "FALSE";
}

extern int tcl_scan_bool(const char *rep, bool &result);


inline int
streq(const char* s1, const char* s2)
{
    return !strcmp(s1, s2);
}

//ss_m::ndx_t cvt2ndx_t(const char* s)

#ifdef SSH_VERBOSE
inline const char *
cvt2string(scan_index_i::cmp_t i)
{
    switch(i) {
    case scan_index_i::gt: return ">";
    case scan_index_i::ge: return ">=";
    case scan_index_i::lt: return "<";
    case scan_index_i::le: return "<=";
    case scan_index_i::eq: return "==";
    default: return "BAD";
    }
    return "BAD";
}
#endif
inline scan_index_i::cmp_t 
cvt2cmp_t(const char *s)
{
    if (streq(s, ">"))  return scan_index_i::gt;
    if (streq(s, ">=")) return scan_index_i::ge;
    if (streq(s, "<"))  return scan_index_i::lt;
    if (streq(s, "<=")) return scan_index_i::le;
    if (streq(s, "==")) return scan_index_i::eq;
    return scan_index_i::bad_cmp_t;
}

inline lock_mode_t
cvt2lock_mode(const char *s)
{
    for (int i = lock_base_t::MIN_MODE; i <= lock_base_t::MAX_MODE; i++)  {
    if (strcmp(s, lock_base_t::mode_str[i]) == 0)
        return (lock_mode_t) i;
    }
    cerr << "cvt2lock_mode: bad lock mode" << endl;
    W_FATAL(ss_m::eINTERNAL);
    return w_base_t::IS;
}

inline lock_duration_t 
cvt2lock_duration(const char *s)
{
    for (int i = 0; i < w_base_t::t_num_durations; i++) {
    if (strcmp(s, lock_base_t::duration_str[i]) == 0)
        return (lock_duration_t) i;
    }

    cerr << "cvt2lock_duration: bad lock duration" << endl;
    W_FATAL(ss_m::eINTERNAL);
    return w_base_t::t_long;
}

inline ss_m::sm_store_property_t
cvt2store_property(const char *s)
{
    ss_m::sm_store_property_t prop = ss_m::t_regular;
    if (strcmp(s, "tmp") == 0)  {
    prop = ss_m::t_temporary;
    } else if (strcmp(s, "regular") == 0)  {
    prop = ss_m::t_regular;
    } else if (strcmp(s, "load_file") == 0) {
    prop = ss_m::t_load_file;
    } else if (strcmp(s, "insert_file") == 0) {
    prop = ss_m::t_insert_file;
    } else {
    cerr << "bad store property: " << s << endl;
    W_FATAL(ss_m::eINTERNAL);
    }
    return prop;
}

inline nbox_t::sob_cmp_t 
cvt2sob_cmp_t(const char *s)
{
    if (streq(s, "||"))  return nbox_t::t_overlap;
    if (streq(s, "/")) return nbox_t::t_cover;
    if (streq(s, "=="))  return nbox_t::t_exact;
    if (streq(s, "<<")) return nbox_t::t_inside;
    return nbox_t::t_bad;
}

inline ss_m::concurrency_t 
cvt2concurrency_t(const char *s)
{
    if (streq(s, "t_cc_none"))  return ss_m::t_cc_none;
    if (streq(s, "t_cc_record"))  return ss_m::t_cc_record;
    if (streq(s, "t_cc_page"))  return ss_m::t_cc_page;
    if (streq(s, "t_cc_file"))  return ss_m::t_cc_file;
    if (streq(s, "t_cc_vol"))  return ss_m::t_cc_vol;
    if (streq(s, "t_cc_kvl"))  return ss_m::t_cc_kvl;
    if (streq(s, "t_cc_modkvl"))  return ss_m::t_cc_modkvl;
    if (streq(s, "t_cc_im"))  return ss_m::t_cc_im;
    if (streq(s, "t_cc_append"))  return ss_m::t_cc_append;
    return ss_m::t_cc_bad;
}

inline int
check(Tcl_Interp* ip, const char* s, int ac, int n1, int n2 = 0, int n3 = 0,
      int n4 = 0, int n5 = 0)
{
    SSH_CHECK_STACK;
    if (ac != n1 && ac != n2 && ac != n3 && ac != n4 && ac != n5) {
        if (s[0])  {
            Tcl_AppendResult(ip, "wrong # args; should be \"", s,
                     "\"", 0);
        } else {
            Tcl_AppendResult(ip, "wrong # args, none expected", 0);
        }
        return -1;
    }
    return 0;
}

enum typed_btree_test {
    test_nosuch, 

	// not lexified, typed compare:
    test_i1, test_i2, test_i4, test_i8,
    test_u1, test_u2, test_u4, test_u8,
    test_f4, test_f8, 

    // selected byte lengths v=variable
    test_b1, test_b23, test_bv, test_blarge,
    test_spatial
};

// Can't use extern "C" because CC finds it not matching the
// type smproc_t
// extern "C" {
    int t_multikey_sort_file(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    int t_test_bulkload_int_btree(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    int t_test_bulkload_rtree(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    int t_test_int_btree(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    int t_test_typed_btree(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    int t_sort_file(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    int t_create_typed_hdr_body_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    int t_create_typed_hdr_rec(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    int t_scan_sorted_recs(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    int t_compare_typed(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    int t_find_assoc_typed(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    int t_get_store_info(Tcl_Interp* ip, int ac, TCL_AV char* av[]);
    typed_btree_test cvt2type(const char *s);
    const char * cvtFROMtype(typed_btree_test t);
    void cvt2typed_value( typed_btree_test t, 
                const char *string, typed_value &);
    int check_key_type(Tcl_Interp *ip, typed_btree_test t, 
    const char *given, const char *stidstring);
    typed_btree_test get_key_type(Tcl_Interp *ip,  const char *stidstring );
    const char* cvt_concurrency_t( ss_m::concurrency_t cc);
    const char* cvt_ndx_t( ss_m::ndx_t cc);
    const char* cvt_store_t(ss_m::store_t n);

// }

/*<std-footer incl-file-exclusion='SHELL_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
