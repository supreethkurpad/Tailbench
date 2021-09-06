/*<std-header orig-src='shore' incl-file-exclusion='CRASH_H'>

 $Id: crash.h,v 1.19.2.7 2010/03/19 22:20:23 nhall Exp $

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

#ifndef CRASH_H
#define CRASH_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

extern bool logtrace;
#if W_DEBUG_LEVEL >= 0
#define LOGTRACE(arg) \
if(logtrace) {\
    w_ostrstream s; \
    s <<" th."<<me()->id << " " << "tid." << xct()->tid() << " "  arg ; \
    fprintf(stderr, "%s\n", s.c_str()); \
}

#define LOGTRACE1(arg) \
if(logtrace) {\
    w_ostrstream s; \
    s <<" th."<<me()->id << " "  arg ; \
    fprintf(stderr, "%s\n", s.c_str()); \
}

#else
#define LOGTRACE(arg) 
#define LOGTRACE1(arg) 
#endif


enum debuginfo_enum { debug_none, debug_delay, debug_crash, debug_abort, debug_yield };

extern w_rc_t ssmtest(log_m *, const char *c, const char *file, int line) ;
extern void setdebuginfo(debuginfo_enum, const char *, int );

#if defined(USE_SSMTEST)

#   define SSMTEST(x) W_DO(ssmtest(smlevel_0::log,x,__FILE__,__LINE__))
#   define VOIDSSMTEST(x) W_IGNORE(ssmtest(smlevel_0::log,x,__FILE__,__LINE__))

#else 

#   define SSMTEST(x) 
#   define VOIDSSMTEST(x)

#endif 

/*<std-footer incl-file-exclusion='CRASH_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
