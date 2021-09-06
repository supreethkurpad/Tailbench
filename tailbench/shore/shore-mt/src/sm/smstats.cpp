/*<std-header orig-src='shore'>

 $Id: smstats.cpp,v 1.16 2010/05/26 01:20:43 nhall Exp $

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

#include "sm_int_0.h"
#include "bf.h"
// smstats_info_t is the collected stats from various
// sm parts.  Each part is separately-generate from .dat files.
#include "smstats.h"
#include "sm_stats_t_inc_gen.cpp"
#include "sm_stats_t_dec_gen.cpp"
#include "sm_stats_t_out_gen.cpp"
#include "bf_htab_stats_t_inc_gen.cpp"
#include "bf_htab_stats_t_dec_gen.cpp"
#include "bf_htab_stats_t_out_gen.cpp"

// the strings:
const char *sm_stats_t ::stat_names[] = {
#include "bf_htab_stats_t_msg_gen.h"
#include "sm_stats_t_msg_gen.h"
   ""
};

void bf_htab_stats_t::compute()
{
    // Because the hash table collects some of its own statistics,
    // we have to gather them from the htab here.
    smlevel_0::bf->htab_stats(*this);
    {
        w_base_t::base_float_t *avg = &bf_htab_insert_avg_tries;
		w_base_t::base_stat_t *a = &bf_htab_insertions;
		w_base_t::base_stat_t *b = &bf_htab_slots_tried;
        if(*a > 0) {
           *avg = *b /(w_base_t::base_float_t) (*a);
        } else {
           *avg = 0.0;
        }
    }
    {
        w_base_t::base_float_t *avg = &bf_htab_lookup_avg_probes;
		w_base_t::base_stat_t *a = &bf_htab_lookups;
		w_base_t::base_stat_t *b = &bf_htab_probes;
        if(*a > 0) {
           *avg = *b /(w_base_t::base_float_t) (*a);
        } else {
           *avg = 0.0;
        }
    }
}

void sm_stats_t::compute()
{
    if(need_vol_lock_r > 0) {
        double x = double(await_vol_lock_r);
        x *= 100;
        x /= double(need_vol_lock_r);
        await_vol_lock_r_pct = w_base_t::base_stat_t(x);
    }

    if(need_vol_lock_w > 0) {
        double y = double(await_vol_lock_w);
        y *= 100;
        y /= double(need_vol_lock_w);
        await_vol_lock_w_pct = w_base_t::base_stat_t(y);
    } 

}

sm_stats_info_t &operator+=(sm_stats_info_t &s, const sm_stats_info_t &t)
{
    s.sm += t.sm;
    return s;
}

sm_stats_info_t &operator-=(sm_stats_info_t &s, const sm_stats_info_t &t)
{
    s.sm -= t.sm;
    return s;
}


sm_stats_info_t &operator-=(sm_stats_info_t &s, const sm_stats_info_t &t);

/*
 * One static stats structure for collecting
 * statistics that might otherwise be lost:
 */
namespace local_ns {
    sm_stats_info_t _global_stats_;
    static queue_based_block_lock_t _global_stats_mutex;
};
void
smlevel_0::add_to_global_stats(const sm_stats_info_t &from)
{
    CRITICAL_SECTION(cs, local_ns::_global_stats_mutex);
    local_ns::_global_stats_ += from;
}
void
smlevel_0::add_from_global_stats(sm_stats_info_t &to)
{
    CRITICAL_SECTION(cs, local_ns::_global_stats_mutex);
    to += local_ns::_global_stats_;
}
