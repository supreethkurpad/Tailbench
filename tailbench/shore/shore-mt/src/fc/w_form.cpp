/*<std-header orig-src='shore'>

 $Id: w_form.cpp,v 1.9.2.4 2010/03/19 22:17:19 nhall Exp $

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


/*
 *   This software is Copyright 1989, 1991, 1992, 1993, 1994, 1998 by:
 *
 *    Josef Burger    <bolo@cs.wisc.edu>
 *
 *   All Rights Reserved.
 *
 *   This software may be freely used as long as credit is given
 *   to the author and this copyright is maintained.
 */

/*
 * An implementation of the 'form' function, an IO stream helper
 * for c++ that is equivalent to sprintf for stdio.
 *
 * This is also an improvement over most c++ library implementations
 * in that it allows the buffers for multiple formats to exist 
 * simultaneously, instead of always over-writing one static buffer.
 * This also provides some sanity in threaded environments; more than
 * one thread can form() at once.
 *
 * Configuration options:
 *    BUF_SLOTS    Number of outstanding formatted output buffers.
 *    MAX_BUFFER    Maximum length of each formatted output buffer.
 *
 *    FORM_PRE_ALLOCATE
 *            if defined, all buffer space that form() uses is
 *            statically allocated.  When this is not defined,
 *            a single static buffer exists to provide formatting
 *            in case memory allocatin fails.
 *    HAVE_VSNPRINTF    If defined uses the 'n' format of vsprintf which
 *            prevents the buffer from being written past its end.
 */

#include <cstdarg>
#include <cstdio>


#define    BUF_SLOTS    16
#define    MAX_BUFFER    1024

#ifdef FORM_PRE_ALLOCATE
static char    buffers[BUF_SLOTS][MAX_BUFFER];
#else
static char    *buffers[BUF_SLOTS];
static char    default_buffer[MAX_BUFFER];
#endif
static unsigned    slot = 0;

/**\cond skip */
struct form_cleanup
{
   ~form_cleanup() {
      for(int i=0; i < BUF_SLOTS; i++)
      {
         delete[] buffers[i];
         buffers[i]=NULL;
      }
   }
} __cleanup__;
/**\endcond skip */


/* XXX Yes, this is not thread safe at the moment. */

const char *form(const char *format, ...)
{
    va_list        ap;
    char        *buffer;
    unsigned    me;
    
    /* XXX Buffer allocation is the only thing that needs to
       be protected to be trivially thread safe. */
    me = slot;
    slot = (slot + 1) % BUF_SLOTS;

#ifndef FORM_PRE_ALLOCATE
    if (!buffers[me]) {
        buffers[me] = new char[MAX_BUFFER];
        /* backup strategy ... use one static buffer to guarantee
           output in case everything goes to hell. */
        if (!buffers[me])
            buffers[me] = default_buffer;
    }
#endif
    buffer = buffers[me];

    va_start(ap, format);
#ifdef HAVE_VSNPRINTF
    vsnprintf(buffer, MAX_BUFFER, format, ap);
#else
#ifdef HAVE_VPRINTF
    vsprintf(buffer, format, ap);
#else
#error need vsprintf
#endif

#endif
    va_end(ap);

    buffer[MAX_BUFFER-1] = '\0';    /* Paranoia */

    return buffer;
}
