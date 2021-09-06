#ifndef BF_HTAB_STATS_T_DEC_GEN_CPP
#define BF_HTAB_STATS_T_DEC_GEN_CPP

/* DO NOT EDIT --- GENERATED from bf_htab_stats.dat by stats.pl
           on Wed Sep  1 18:45:13 2021

<std-header orig-src='shore' genfile='true'>

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


bf_htab_stats_t &
operator-=(bf_htab_stats_t &s,const bf_htab_stats_t &t)
{
	s.bf_htab_insertions -= t.bf_htab_insertions;
	s.bf_htab_slow_inserts -= t.bf_htab_slow_inserts;
	s.bf_htab_slots_tried -= t.bf_htab_slots_tried;
	s.bf_htab_ensures -= t.bf_htab_ensures;
	s.bf_htab_cuckolds -= t.bf_htab_cuckolds;
	s.bf_htab_lookups -= t.bf_htab_lookups;
	s.bf_htab_harsh_lookups -= t.bf_htab_harsh_lookups;
	s.bf_htab_lookups_failed -= t.bf_htab_lookups_failed;
	s.bf_htab_probes -= t.bf_htab_probes;
	s.bf_htab_harsh_probes -= t.bf_htab_harsh_probes;
	s.bf_htab_probe_empty -= t.bf_htab_probe_empty;
	s.bf_htab_hash_collisions -= t.bf_htab_hash_collisions;
	s.bf_htab_removes -= t.bf_htab_removes;
	s.bf_htab_limit_exceeds -= t.bf_htab_limit_exceeds;
	s.bf_htab_max_limit -= t.bf_htab_max_limit;
	s.bf_htab_insert_avg_tries -= t.bf_htab_insert_avg_tries;
	s.bf_htab_lookup_avg_probes -= t.bf_htab_lookup_avg_probes;
	s.bf_htab_bucket_size -= t.bf_htab_bucket_size;
	s.bf_htab_table_size -= t.bf_htab_table_size;
	s.bf_htab_entries -= t.bf_htab_entries;
	s.bf_htab_buckets -= t.bf_htab_buckets;
	s.bf_htab_slot_count -= t.bf_htab_slot_count;
	return s;
}

#endif /* BF_HTAB_STATS_T_DEC_GEN_CPP */
