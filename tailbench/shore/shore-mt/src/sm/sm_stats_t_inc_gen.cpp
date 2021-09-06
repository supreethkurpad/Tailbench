#ifndef SM_STATS_T_INC_GEN_CPP
#define SM_STATS_T_INC_GEN_CPP

/* DO NOT EDIT --- GENERATED from sm_stats.dat by stats.pl
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


sm_stats_t &
operator+=(sm_stats_t &s,const sm_stats_t &t)
{
	s.bf_one_page_write += t.bf_one_page_write;
	s.bf_two_page_write += t.bf_two_page_write;
	s.bf_three_page_write += t.bf_three_page_write;
	s.bf_four_page_write += t.bf_four_page_write;
	s.bf_five_page_write += t.bf_five_page_write;
	s.bf_six_page_write += t.bf_six_page_write;
	s.bf_seven_page_write += t.bf_seven_page_write;
	s.bf_eight_page_write += t.bf_eight_page_write;
	s.bf_more_page_write += t.bf_more_page_write;
	s.bf_cleaner_sweeps += t.bf_cleaner_sweeps;
	s.bf_cleaner_signalled += t.bf_cleaner_signalled;
	s.bf_already_evicted += t.bf_already_evicted;
	s.bf_dirty_page_cleaned += t.bf_dirty_page_cleaned;
	s.bf_flushed_OHD_page += t.bf_flushed_OHD_page;
	s.bf_kick_full += t.bf_kick_full;
	s.bf_kick_replacement += t.bf_kick_replacement;
	s.bf_kick_threshold += t.bf_kick_threshold;
	s.bf_sweep_page_hot_skipped += t.bf_sweep_page_hot_skipped;
	s.bf_discarded_hot += t.bf_discarded_hot;
	s.bf_log_flush_all += t.bf_log_flush_all;
	s.bf_log_flush_lsn += t.bf_log_flush_lsn;
	s.bf_write_out += t.bf_write_out;
	s.bf_sleep_await_clean += t.bf_sleep_await_clean;
	s.bf_fg_scan_cnt += t.bf_fg_scan_cnt;
	s.bf_core_unpin_cleaned += t.bf_core_unpin_cleaned;
	s.bf_unfix_cleaned += t.bf_unfix_cleaned;
	s.bf_look_cnt += t.bf_look_cnt;
	s.bf_hit_cnt += t.bf_hit_cnt;
	s.bf_grab_latch_failed += t.bf_grab_latch_failed;
	s.bf_replace_out += t.bf_replace_out;
	s.bf_replaced_dirty += t.bf_replaced_dirty;
	s.bf_replaced_clean += t.bf_replaced_clean;
	s.bf_no_transit_bucket += t.bf_no_transit_bucket;
	s.bf_prefetch_requests += t.bf_prefetch_requests;
	s.bf_prefetches += t.bf_prefetches;
	s.bf_upgrade_latch_race += t.bf_upgrade_latch_race;
	s.bf_upgrade_latch_changed += t.bf_upgrade_latch_changed;
	s.restart_repair_rec_lsn += t.restart_repair_rec_lsn;
	s.vol_reads += t.vol_reads;
	s.vol_writes += t.vol_writes;
	s.vol_blks_written += t.vol_blks_written;
	s.vol_alloc_exts += t.vol_alloc_exts;
	s.vol_free_exts += t.vol_free_exts;
	s.io_m_linear_searches += t.io_m_linear_searches;
	s.io_m_linear_search_extents += t.io_m_linear_search_extents;
	s.vol_cache_primes += t.vol_cache_primes;
	s.vol_cache_clears += t.vol_cache_clears;
	s.vol_last_extent_search += t.vol_last_extent_search;
	s.vol_last_extent_search_cost += t.vol_last_extent_search_cost;
	s.vol_last_page_cache_update += t.vol_last_page_cache_update;
	s.vol_last_page_cache_find += t.vol_last_page_cache_find;
	s.vol_last_page_cache_find_hit += t.vol_last_page_cache_find_hit;
	s.vol_histo_ext_cache_update += t.vol_histo_ext_cache_update;
	s.vol_histo_ext_cache_find += t.vol_histo_ext_cache_find;
	s.vol_histo_ext_cache_find_hit += t.vol_histo_ext_cache_find_hit;
	s.vol_resv_cache_insert += t.vol_resv_cache_insert;
	s.vol_resv_cache_erase += t.vol_resv_cache_erase;
	s.vol_resv_cache_hit += t.vol_resv_cache_hit;
	s.vol_resv_cache_fail += t.vol_resv_cache_fail;
	s.vol_lock_noalloc += t.vol_lock_noalloc;
	s.log_dup_sync_cnt += t.log_dup_sync_cnt;
	s.log_sync_cnt += t.log_sync_cnt;
	s.log_fsync_cnt += t.log_fsync_cnt;
	s.log_chkpt_cnt += t.log_chkpt_cnt;
	s.log_chkpt_wake += t.log_chkpt_wake;
	s.log_fetches += t.log_fetches;
	s.log_inserts += t.log_inserts;
	s.log_full += t.log_full;
	s.log_full_old_xct += t.log_full_old_xct;
	s.log_full_old_page += t.log_full_old_page;
	s.log_full_wait += t.log_full_wait;
	s.log_full_force += t.log_full_force;
	s.log_full_giveup += t.log_full_giveup;
	s.log_file_wrap += t.log_file_wrap;
	s.log_bytes_generated += t.log_bytes_generated;
	s.lock_deadlock_cnt += t.lock_deadlock_cnt;
	s.lock_false_deadlock_cnt += t.lock_false_deadlock_cnt;
	s.lock_dld_call_cnt += t.lock_dld_call_cnt;
	s.lock_dld_first_call_cnt += t.lock_dld_first_call_cnt;
	s.lock_dld_false_victim_cnt += t.lock_dld_false_victim_cnt;
	s.lock_dld_victim_self_cnt += t.lock_dld_victim_self_cnt;
	s.lock_dld_victim_other_cnt += t.lock_dld_victim_other_cnt;
	s.nonunique_fingerprints += t.nonunique_fingerprints;
	s.unique_fingerprints += t.unique_fingerprints;
	s.rec_pin_cnt += t.rec_pin_cnt;
	s.rec_unpin_cnt += t.rec_unpin_cnt;
	s.rec_repin_cvt += t.rec_repin_cvt;
	s.fm_pagecache_hit += t.fm_pagecache_hit;
	s.fm_page_nolatch += t.fm_page_nolatch;
	s.fm_page_moved += t.fm_page_moved;
	s.fm_page_invalid += t.fm_page_invalid;
	s.fm_page_nolock += t.fm_page_nolock;
	s.fm_alloc_page_reject += t.fm_alloc_page_reject;
	s.fm_page_full += t.fm_page_full;
	s.fm_error_not_handled += t.fm_error_not_handled;
	s.fm_ok += t.fm_ok;
	s.fm_histogram_hit += t.fm_histogram_hit;
	s.fm_search_pages += t.fm_search_pages;
	s.fm_search_failed += t.fm_search_failed;
	s.fm_search_hit += t.fm_search_hit;
	s.fm_lastpid_cached += t.fm_lastpid_cached;
	s.fm_lastpid_hit += t.fm_lastpid_hit;
	s.fm_alloc_pg += t.fm_alloc_pg;
	s.fm_ext_touch += t.fm_ext_touch;
	s.fm_ext_touch_nop += t.fm_ext_touch_nop;
	s.fm_nospace += t.fm_nospace;
	s.fm_cache += t.fm_cache;
	s.fm_compact += t.fm_compact;
	s.fm_append += t.fm_append;
	s.fm_appendonly += t.fm_appendonly;
	s.bt_find_cnt += t.bt_find_cnt;
	s.bt_insert_cnt += t.bt_insert_cnt;
	s.bt_remove_cnt += t.bt_remove_cnt;
	s.bt_traverse_cnt += t.bt_traverse_cnt;
	s.bt_partial_traverse_cnt += t.bt_partial_traverse_cnt;
	s.bt_restart_traverse_cnt += t.bt_restart_traverse_cnt;
	s.bt_posc += t.bt_posc;
	s.bt_scan_cnt += t.bt_scan_cnt;
	s.bt_splits += t.bt_splits;
	s.bt_leaf_splits += t.bt_leaf_splits;
	s.bt_cuts += t.bt_cuts;
	s.bt_grows += t.bt_grows;
	s.bt_shrinks += t.bt_shrinks;
	s.bt_links += t.bt_links;
	s.bt_upgrade_fail_retry += t.bt_upgrade_fail_retry;
	s.bt_clr_smo_traverse += t.bt_clr_smo_traverse;
	s.bt_pcompress += t.bt_pcompress;
	s.bt_plmax += t.bt_plmax;
	s.bt_update_cnt += t.bt_update_cnt;
	s.sort_keycmp_cnt += t.sort_keycmp_cnt;
	s.sort_lexindx_cnt += t.sort_lexindx_cnt;
	s.sort_getinfo_cnt += t.sort_getinfo_cnt;
	s.sort_mof_cnt += t.sort_mof_cnt;
	s.sort_umof_cnt += t.sort_umof_cnt;
	s.sort_memcpy_cnt += t.sort_memcpy_cnt;
	s.sort_memcpy_bytes += t.sort_memcpy_bytes;
	s.sort_keycpy_cnt += t.sort_keycpy_cnt;
	s.sort_mallocs += t.sort_mallocs;
	s.sort_malloc_bytes += t.sort_malloc_bytes;
	s.sort_malloc_hiwat += t.sort_malloc_hiwat;
	s.sort_malloc_max += t.sort_malloc_max;
	s.sort_malloc_curr += t.sort_malloc_curr;
	s.sort_tmpfile_cnt += t.sort_tmpfile_cnt;
	s.sort_tmpfile_bytes += t.sort_tmpfile_bytes;
	s.sort_duplicates += t.sort_duplicates;
	s.sort_page_fixes += t.sort_page_fixes;
	s.sort_page_fixes_2 += t.sort_page_fixes_2;
	s.sort_lg_page_fixes += t.sort_lg_page_fixes;
	s.sort_rec_pins += t.sort_rec_pins;
	s.sort_files_created += t.sort_files_created;
	s.sort_recs_created += t.sort_recs_created;
	s.sort_rec_bytes += t.sort_rec_bytes;
	s.sort_runs += t.sort_runs;
	s.sort_run_size += t.sort_run_size;
	s.sort_phases += t.sort_phases;
	s.sort_ntapes += t.sort_ntapes;
	s.page_fix_cnt += t.page_fix_cnt;
	s.page_refix_cnt += t.page_refix_cnt;
	s.page_unfix_cnt += t.page_unfix_cnt;
	s.page_alloc_cnt += t.page_alloc_cnt;
	s.page_dealloc_cnt += t.page_dealloc_cnt;
	s.page_btree_alloc += t.page_btree_alloc;
	s.page_file_alloc += t.page_file_alloc;
	s.page_file_mrbt_alloc += t.page_file_mrbt_alloc;
	s.page_btree_dealloc += t.page_btree_dealloc;
	s.page_file_dealloc += t.page_file_dealloc;
	s.page_file_mrbt_dealloc += t.page_file_mrbt_dealloc;
	s.ext_lookup_hits += t.ext_lookup_hits;
	s.ext_lookup_misses += t.ext_lookup_misses;
	s.alloc_page_in_ext += t.alloc_page_in_ext;
	s.extent_lsearch += t.extent_lsearch;
	s.begin_xct_cnt += t.begin_xct_cnt;
	s.commit_xct_cnt += t.commit_xct_cnt;
	s.abort_xct_cnt += t.abort_xct_cnt;
	s.log_warn_abort_cnt += t.log_warn_abort_cnt;
	s.prepare_xct_cnt += t.prepare_xct_cnt;
	s.rollback_savept_cnt += t.rollback_savept_cnt;
	s.mpl_attach_cnt += t.mpl_attach_cnt;
	s.anchors += t.anchors;
	s.compensate_in_log += t.compensate_in_log;
	s.compensate_in_xct += t.compensate_in_xct;
	s.compensate_records += t.compensate_records;
	s.compensate_skipped += t.compensate_skipped;
	s.log_switches += t.log_switches;
	s.await_1thread_log += t.await_1thread_log;
	s.acquire_1thread_log += t.acquire_1thread_log;
	s.get_logbuf += t.get_logbuf;
	s.await_1thread_xct += t.await_1thread_xct;
	s.await_vol_monitor += t.await_vol_monitor;
	s.need_vol_lock_r += t.need_vol_lock_r;
	s.need_vol_lock_w += t.need_vol_lock_w;
	s.await_vol_lock_r += t.await_vol_lock_r;
	s.await_vol_lock_w += t.await_vol_lock_w;
	s.await_vol_lock_r_pct += t.await_vol_lock_r_pct;
	s.await_vol_lock_w_pct += t.await_vol_lock_w_pct;
	s.s_prepared += t.s_prepared;
	s.lock_query_cnt += t.lock_query_cnt;
	s.unlock_request_cnt += t.unlock_request_cnt;
	s.lock_request_cnt += t.lock_request_cnt;
	s.lock_acquire_cnt += t.lock_acquire_cnt;
	s.lock_head_t_cnt += t.lock_head_t_cnt;
	s.lock_await_alt_cnt += t.lock_await_alt_cnt;
	s.lock_extraneous_req_cnt += t.lock_extraneous_req_cnt;
	s.lock_conversion_cnt += t.lock_conversion_cnt;
	s.lock_cache_hit_cnt += t.lock_cache_hit_cnt;
	s.lock_request_t_cnt += t.lock_request_t_cnt;
	s.lock_esc_to_page += t.lock_esc_to_page;
	s.lock_esc_to_store += t.lock_esc_to_store;
	s.lock_esc_to_volume += t.lock_esc_to_volume;
	s.lk_vol_acq += t.lk_vol_acq;
	s.lk_store_acq += t.lk_store_acq;
	s.lk_page_acq += t.lk_page_acq;
	s.lk_kvl_acq += t.lk_kvl_acq;
	s.lk_rec_acq += t.lk_rec_acq;
	s.lk_ext_acq += t.lk_ext_acq;
	s.lk_user1_acq += t.lk_user1_acq;
	s.lk_user2_acq += t.lk_user2_acq;
	s.lk_user3_acq += t.lk_user3_acq;
	s.lk_user4_acq += t.lk_user4_acq;
	s.lock_wait_cnt += t.lock_wait_cnt;
	s.lock_block_cnt += t.lock_block_cnt;
	s.lk_vol_wait += t.lk_vol_wait;
	s.lk_store_wait += t.lk_store_wait;
	s.lk_page_wait += t.lk_page_wait;
	s.lk_kvl_wait += t.lk_kvl_wait;
	s.lk_rec_wait += t.lk_rec_wait;
	s.lk_ext_wait += t.lk_ext_wait;
	s.lk_user1_wait += t.lk_user1_wait;
	s.lk_user2_wait += t.lk_user2_wait;
	s.lk_user3_wait += t.lk_user3_wait;
	s.lk_user4_wait += t.lk_user4_wait;
	s.sli_requested += t.sli_requested;
	s.sli_acquired += t.sli_acquired;
	s.sli_upgrades += t.sli_upgrades;
	s.sli_eligible += t.sli_eligible;
	s.sli_inherited += t.sli_inherited;
	s.sli_used += t.sli_used;
	s.sli_too_weak += t.sli_too_weak;
	s.sli_found_late += t.sli_found_late;
	s.sli_purged += t.sli_purged;
	s.sli_invalidated += t.sli_invalidated;
	s.sli_kept += t.sli_kept;
	s.sli_evicted += t.sli_evicted;
	s.sli_no_parent += t.sli_no_parent;
	s.sli_waited_on += t.sli_waited_on;
	s.dir_cache_hit += t.dir_cache_hit;
	s.dir_cache_miss += t.dir_cache_miss;
	s.dir_cache_inherit += t.dir_cache_inherit;
	s.dir_cache_stale += t.dir_cache_stale;
	return s;
}

#endif /* SM_STATS_T_INC_GEN_CPP */
