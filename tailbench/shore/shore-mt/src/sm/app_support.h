/*<std-header orig-src='shore' incl-file-exclusion='APP_SUPPORT_H'>

 $Id: app_support.h,v 1.14 2010/06/08 22:28:55 nhall Exp $

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

#ifndef APP_SUPPORT_H
#define APP_SUPPORT_H

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

/*
 * This file contains support for application programs to access
 * file page structures.
 *
 * It is used to generate common/sm_app.h.
 * 
 * Users should see sm/file_s.h for definitions of record_t.
 */

class shore_file_page_t : public page_s {

public:
    int slot_count() const { return nslots; }

    record_t* rec_addr(int idx) const {
	return ((idx > 0 && idx < nslots && slot(idx).offset >=0) ? 
		(record_t*) (data() + slot(idx).offset) : 
        0);
    }
};

/*
 * compile time constants also available from ss_m::config_info()
 *
 * The correctness of these constants is checked in ss_m::ss_m().
 */
class ssm_constants {
public: 
    enum {
    // See comments in sm.cpp where we compute max_small_rec for
    // the config_info.
    /* old:
    max_small_rec = align(page_s::data_sz - sizeof(file_p_hdr_t) -
            sizeof(page_s::slot_t) - sizeof(rectag_t))
                - align(1),
    */
    max_small_rec = page_s::data_sz - sizeof(file_p_hdr_t) -
            sizeof(page_s::slot_t) - sizeof(rectag_t),
    lg_rec_page_space = page_s::data_sz
    };
};

/*<std-footer incl-file-exclusion='APP_SUPPORT_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
