/*<std-header orig-src='shore' incl-file-exclusion='W_RUSAGE_H'>

 $Id: w_rusage.h,v 1.12.2.5 2010/03/19 22:17:20 nhall Exp $

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

#ifndef W_RUSAGE_H
#define W_RUSAGE_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <sys/time.h>

#ifdef Linux
    extern "C" {
#endif
#include <sys/resource.h>
#ifdef Linux
    }
#endif

/**\cond skip */
/* XXX
 * 
 * This needs to be fixed.  The system should not be dependent
 * upon unix rusage facilities.  It should adapt to whatever is 
 * available (if anything)  on the host system.
 */

/* XXX using kernel include file to do this is bogus.   It would be better
   to say "enough of the actual defines are defined", rather than determining
   whether the kernel include file has been sucked in.  Heck, we might
   not be including the right file! */

/* _SYS_RESOURCE_H_ for *BSD unix boxes */
/* _SYS_RESOURCE_INCLUDED for HPUX */
#if !defined(_SYS_RESOURCE_H) && !defined(_SYS_RESOURCE_H_) && !defined(_SYS_RESOURCE_INCLUDED)
#define    _SYS_RESOURCE_H

/*
 * Process priority specifications
 */

#define    PRIO_PROCESS    0
#define    PRIO_PGRP    1
#define    PRIO_USER    2


/*
 * Resource limits
 */

#define    RLIMIT_CPU    0        /* cpu time in milliseconds */
#define    RLIMIT_FSIZE    1        /* maximum file size */
#define    RLIMIT_DATA    2        /* data size */
#define    RLIMIT_STACK    3        /* stack size */
#define    RLIMIT_CORE    4        /* core file size */
#define    RLIMIT_NOFILE    5        /* file descriptors */
#define    RLIMIT_VMEM    6        /* maximum mapped memory */
#define    RLIMIT_AS    RLIMIT_VMEM

#define    RLIM_NLIMITS    7        /* number of resource limits */

#define    RLIM_INFINITY    0x7fffffff

typedef unsigned long rlim_t;

struct rlimit {
    rlim_t    rlim_cur;        /* current limit */
    rlim_t    rlim_max;        /* maximum value for rlim_cur */
};

#define    RUSAGE_SELF    0
#define    RUSAGE_CHILDREN    -1

struct    rusage {
    struct timeval ru_utime;    /* user time used */
    struct timeval ru_stime;    /* system time used */
    long    ru_maxrss;        /* XXX: 0 */
    long    ru_ixrss;        /* XXX: 0 */
    long    ru_idrss;        /* XXX: sum of rm_asrss */
    long    ru_isrss;        /* XXX: 0 */
    long    ru_minflt;        /* any page faults not requiring I/O */
    long    ru_majflt;        /* any page faults requiring I/O */
    long    ru_nswap;        /* swaps */
    long    ru_inblock;        /* block input operations */
    long    ru_oublock;        /* block output operations */
    long    ru_msgsnd;        /* messages sent */
    long    ru_msgrcv;        /* messages received */
    long    ru_nsignals;        /* signals received */
    long    ru_nvcsw;        /* voluntary context switches */
    long    ru_nivcsw;        /* involuntary " */
};
#endif    /* _SYS_RESOURCE_H */

#ifndef Linux
#ifndef getrusage
    extern "C" int getrusage(int x , struct rusage* use);
#endif /* getrusage */
#endif /* Linux  */


/**\endcond skip */

/*<std-footer incl-file-exclusion='W_RUSAGE_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
