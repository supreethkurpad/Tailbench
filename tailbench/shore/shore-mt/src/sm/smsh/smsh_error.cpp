/*<std-header orig-src='shore'>

 $Id: smsh_error.cpp,v 1.1.2.5 2010/03/19 22:20:31 nhall Exp $

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

#include <w.h>
#include "e_error_def_gen.h"
#include "st_error_def_gen.h"
#include "opt_error_def_gen.h"
#include "fc_error_def_gen.h"

#include "e_einfo_bakw_gen.h"
#include "opt_einfo_bakw_gen.h"
#include "st_einfo_bakw_gen.h"
#include "fc_einfo_bakw_gen.h"
#include <w_debug.h>
#include "smsh_error.h"

const char *
smsh_err_msg(const char *str)
{
    FUNC(smsh_err_msg);

    return w_error_t::error_string(smsh_err_code(str));
}

unsigned int
smsh_err_code(const char *x)
{
    FUNC(smsh_err_code);
    w_error_info_t  *v;
    int             j;


#define LOOK(a,b,c) \
    v = (a);\
    j = (b);\
    while( (v != 0) && j++ <= (c) ) {\
        if(strcmp(v->errstr,x)==0) { \
            return  v->err_num;\
        }\
        v++;\
    }
    LOOK(e_error_info_bakw,E_ERRMIN,E_ERRMAX);
    LOOK(st_error_info_bakw,ST_ERRMIN,ST_ERRMAX);
    LOOK(opt_error_info_bakw,OPT_ERRMIN,OPT_ERRMAX);
    LOOK(fc_error_info_bakw,FC_ERRMIN,FC_ERRMAX);

#undef LOOK
    return fcNOSUCHERROR;
}

// returns error name given error code
// return false if the error code is not in SVAS_* or OS_*
const char *
smsh_err_name(unsigned int x)
{
    FUNC(smsh_err_name);

    DBG(<<"smsh_err_name for " << x);

    w_error_info_t  *v;
    int j;

#define LOOK(a,b,c) \
    v = (a);\
    j = (b);\
    while( (v != 0) && j++ <= (c) ) {\
        if(x == v->err_num) {\
            return  v->errstr;\
        }\
        v++;\
    }
    
    LOOK(e_error_info_bakw,E_ERRMIN,E_ERRMAX);
    LOOK(st_error_info_bakw,ST_ERRMIN,ST_ERRMAX);
    LOOK(opt_error_info_bakw,OPT_ERRMIN,OPT_ERRMAX);
    LOOK(fc_error_info_bakw,FC_ERRMIN,FC_ERRMAX);

#undef LOOK

    return "bad error value";
}

