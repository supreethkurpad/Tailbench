#ifndef BF_HTAB_STATS_T_COLLECT_GEN_CPP
#define BF_HTAB_STATS_T_COLLECT_GEN_CPP

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
/*  -- do not edit anything above this line --   </std-header>*/


	t.set_base(VT_bf_htab_insertions, TMP_GET_STAT(bf_htab_insertions));
	t.set_base(VT_bf_htab_slow_inserts, TMP_GET_STAT(bf_htab_slow_inserts));
	t.set_base(VT_bf_htab_slots_tried, TMP_GET_STAT(bf_htab_slots_tried));
	t.set_base(VT_bf_htab_ensures, TMP_GET_STAT(bf_htab_ensures));
	t.set_base(VT_bf_htab_cuckolds, TMP_GET_STAT(bf_htab_cuckolds));
	t.set_base(VT_bf_htab_lookups, TMP_GET_STAT(bf_htab_lookups));
	t.set_base(VT_bf_htab_harsh_lookups, TMP_GET_STAT(bf_htab_harsh_lookups));
	t.set_base(VT_bf_htab_lookups_failed, TMP_GET_STAT(bf_htab_lookups_failed));
	t.set_base(VT_bf_htab_probes, TMP_GET_STAT(bf_htab_probes));
	t.set_base(VT_bf_htab_harsh_probes, TMP_GET_STAT(bf_htab_harsh_probes));
	t.set_base(VT_bf_htab_probe_empty, TMP_GET_STAT(bf_htab_probe_empty));
	t.set_base(VT_bf_htab_hash_collisions, TMP_GET_STAT(bf_htab_hash_collisions));
	t.set_base(VT_bf_htab_removes, TMP_GET_STAT(bf_htab_removes));
	t.set_base(VT_bf_htab_limit_exceeds, TMP_GET_STAT(bf_htab_limit_exceeds));
	t.set_base(VT_bf_htab_max_limit, TMP_GET_STAT(bf_htab_max_limit));
	t.set_base(VT_bf_htab_insert_avg_tries, TMP_GET_STAT(bf_htab_insert_avg_tries));
	t.set_base(VT_bf_htab_lookup_avg_probes, TMP_GET_STAT(bf_htab_lookup_avg_probes));
	t.set_base(VT_bf_htab_bucket_size, TMP_GET_STAT(bf_htab_bucket_size));
	t.set_base(VT_bf_htab_table_size, TMP_GET_STAT(bf_htab_table_size));
	t.set_base(VT_bf_htab_entries, TMP_GET_STAT(bf_htab_entries));
	t.set_base(VT_bf_htab_buckets, TMP_GET_STAT(bf_htab_buckets));
	t.set_base(VT_bf_htab_slot_count, TMP_GET_STAT(bf_htab_slot_count));

#endif /* BF_HTAB_STATS_T_COLLECT_GEN_CPP */
