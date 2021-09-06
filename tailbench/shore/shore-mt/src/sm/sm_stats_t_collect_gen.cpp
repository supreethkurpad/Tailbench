#ifndef SM_STATS_T_COLLECT_GEN_CPP
#define SM_STATS_T_COLLECT_GEN_CPP

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
/*  -- do not edit anything above this line --   </std-header>*/


	t.set_base(VT_bf_one_page_write, TMP_GET_STAT(bf_one_page_write));
	t.set_base(VT_bf_two_page_write, TMP_GET_STAT(bf_two_page_write));
	t.set_base(VT_bf_three_page_write, TMP_GET_STAT(bf_three_page_write));
	t.set_base(VT_bf_four_page_write, TMP_GET_STAT(bf_four_page_write));
	t.set_base(VT_bf_five_page_write, TMP_GET_STAT(bf_five_page_write));
	t.set_base(VT_bf_six_page_write, TMP_GET_STAT(bf_six_page_write));
	t.set_base(VT_bf_seven_page_write, TMP_GET_STAT(bf_seven_page_write));
	t.set_base(VT_bf_eight_page_write, TMP_GET_STAT(bf_eight_page_write));
	t.set_base(VT_bf_more_page_write, TMP_GET_STAT(bf_more_page_write));
	t.set_base(VT_bf_cleaner_sweeps, TMP_GET_STAT(bf_cleaner_sweeps));
	t.set_base(VT_bf_cleaner_signalled, TMP_GET_STAT(bf_cleaner_signalled));
	t.set_base(VT_bf_already_evicted, TMP_GET_STAT(bf_already_evicted));
	t.set_base(VT_bf_dirty_page_cleaned, TMP_GET_STAT(bf_dirty_page_cleaned));
	t.set_base(VT_bf_flushed_OHD_page, TMP_GET_STAT(bf_flushed_OHD_page));
	t.set_base(VT_bf_kick_full, TMP_GET_STAT(bf_kick_full));
	t.set_base(VT_bf_kick_replacement, TMP_GET_STAT(bf_kick_replacement));
	t.set_base(VT_bf_kick_threshold, TMP_GET_STAT(bf_kick_threshold));
	t.set_base(VT_bf_sweep_page_hot_skipped, TMP_GET_STAT(bf_sweep_page_hot_skipped));
	t.set_base(VT_bf_discarded_hot, TMP_GET_STAT(bf_discarded_hot));
	t.set_base(VT_bf_log_flush_all, TMP_GET_STAT(bf_log_flush_all));
	t.set_base(VT_bf_log_flush_lsn, TMP_GET_STAT(bf_log_flush_lsn));
	t.set_base(VT_bf_write_out, TMP_GET_STAT(bf_write_out));
	t.set_base(VT_bf_sleep_await_clean, TMP_GET_STAT(bf_sleep_await_clean));
	t.set_base(VT_bf_fg_scan_cnt, TMP_GET_STAT(bf_fg_scan_cnt));
	t.set_base(VT_bf_core_unpin_cleaned, TMP_GET_STAT(bf_core_unpin_cleaned));
	t.set_base(VT_bf_unfix_cleaned, TMP_GET_STAT(bf_unfix_cleaned));
	t.set_base(VT_bf_look_cnt, TMP_GET_STAT(bf_look_cnt));
	t.set_base(VT_bf_hit_cnt, TMP_GET_STAT(bf_hit_cnt));
	t.set_base(VT_bf_grab_latch_failed, TMP_GET_STAT(bf_grab_latch_failed));
	t.set_base(VT_bf_replace_out, TMP_GET_STAT(bf_replace_out));
	t.set_base(VT_bf_replaced_dirty, TMP_GET_STAT(bf_replaced_dirty));
	t.set_base(VT_bf_replaced_clean, TMP_GET_STAT(bf_replaced_clean));
	t.set_base(VT_bf_no_transit_bucket, TMP_GET_STAT(bf_no_transit_bucket));
	t.set_base(VT_bf_prefetch_requests, TMP_GET_STAT(bf_prefetch_requests));
	t.set_base(VT_bf_prefetches, TMP_GET_STAT(bf_prefetches));
	t.set_base(VT_bf_upgrade_latch_race, TMP_GET_STAT(bf_upgrade_latch_race));
	t.set_base(VT_bf_upgrade_latch_changed, TMP_GET_STAT(bf_upgrade_latch_changed));
	t.set_base(VT_restart_repair_rec_lsn, TMP_GET_STAT(restart_repair_rec_lsn));
	t.set_base(VT_vol_reads, TMP_GET_STAT(vol_reads));
	t.set_base(VT_vol_writes, TMP_GET_STAT(vol_writes));
	t.set_base(VT_vol_blks_written, TMP_GET_STAT(vol_blks_written));
	t.set_base(VT_vol_alloc_exts, TMP_GET_STAT(vol_alloc_exts));
	t.set_base(VT_vol_free_exts, TMP_GET_STAT(vol_free_exts));
	t.set_base(VT_io_m_linear_searches, TMP_GET_STAT(io_m_linear_searches));
	t.set_base(VT_io_m_linear_search_extents, TMP_GET_STAT(io_m_linear_search_extents));
	t.set_base(VT_vol_cache_primes, TMP_GET_STAT(vol_cache_primes));
	t.set_base(VT_vol_cache_clears, TMP_GET_STAT(vol_cache_clears));
	t.set_base(VT_vol_last_extent_search, TMP_GET_STAT(vol_last_extent_search));
	t.set_base(VT_vol_last_extent_search_cost, TMP_GET_STAT(vol_last_extent_search_cost));
	t.set_base(VT_vol_last_page_cache_update, TMP_GET_STAT(vol_last_page_cache_update));
	t.set_base(VT_vol_last_page_cache_find, TMP_GET_STAT(vol_last_page_cache_find));
	t.set_base(VT_vol_last_page_cache_find_hit, TMP_GET_STAT(vol_last_page_cache_find_hit));
	t.set_base(VT_vol_histo_ext_cache_update, TMP_GET_STAT(vol_histo_ext_cache_update));
	t.set_base(VT_vol_histo_ext_cache_find, TMP_GET_STAT(vol_histo_ext_cache_find));
	t.set_base(VT_vol_histo_ext_cache_find_hit, TMP_GET_STAT(vol_histo_ext_cache_find_hit));
	t.set_base(VT_vol_resv_cache_insert, TMP_GET_STAT(vol_resv_cache_insert));
	t.set_base(VT_vol_resv_cache_erase, TMP_GET_STAT(vol_resv_cache_erase));
	t.set_base(VT_vol_resv_cache_hit, TMP_GET_STAT(vol_resv_cache_hit));
	t.set_base(VT_vol_resv_cache_fail, TMP_GET_STAT(vol_resv_cache_fail));
	t.set_base(VT_vol_lock_noalloc, TMP_GET_STAT(vol_lock_noalloc));
	t.set_base(VT_log_dup_sync_cnt, TMP_GET_STAT(log_dup_sync_cnt));
	t.set_base(VT_log_sync_cnt, TMP_GET_STAT(log_sync_cnt));
	t.set_base(VT_log_fsync_cnt, TMP_GET_STAT(log_fsync_cnt));
	t.set_base(VT_log_chkpt_cnt, TMP_GET_STAT(log_chkpt_cnt));
	t.set_base(VT_log_chkpt_wake, TMP_GET_STAT(log_chkpt_wake));
	t.set_base(VT_log_fetches, TMP_GET_STAT(log_fetches));
	t.set_base(VT_log_inserts, TMP_GET_STAT(log_inserts));
	t.set_base(VT_log_full, TMP_GET_STAT(log_full));
	t.set_base(VT_log_full_old_xct, TMP_GET_STAT(log_full_old_xct));
	t.set_base(VT_log_full_old_page, TMP_GET_STAT(log_full_old_page));
	t.set_base(VT_log_full_wait, TMP_GET_STAT(log_full_wait));
	t.set_base(VT_log_full_force, TMP_GET_STAT(log_full_force));
	t.set_base(VT_log_full_giveup, TMP_GET_STAT(log_full_giveup));
	t.set_base(VT_log_file_wrap, TMP_GET_STAT(log_file_wrap));
	t.set_base(VT_log_bytes_generated, TMP_GET_STAT(log_bytes_generated));
	t.set_base(VT_lock_deadlock_cnt, TMP_GET_STAT(lock_deadlock_cnt));
	t.set_base(VT_lock_false_deadlock_cnt, TMP_GET_STAT(lock_false_deadlock_cnt));
	t.set_base(VT_lock_dld_call_cnt, TMP_GET_STAT(lock_dld_call_cnt));
	t.set_base(VT_lock_dld_first_call_cnt, TMP_GET_STAT(lock_dld_first_call_cnt));
	t.set_base(VT_lock_dld_false_victim_cnt, TMP_GET_STAT(lock_dld_false_victim_cnt));
	t.set_base(VT_lock_dld_victim_self_cnt, TMP_GET_STAT(lock_dld_victim_self_cnt));
	t.set_base(VT_lock_dld_victim_other_cnt, TMP_GET_STAT(lock_dld_victim_other_cnt));
	t.set_base(VT_nonunique_fingerprints, TMP_GET_STAT(nonunique_fingerprints));
	t.set_base(VT_unique_fingerprints, TMP_GET_STAT(unique_fingerprints));
	t.set_base(VT_rec_pin_cnt, TMP_GET_STAT(rec_pin_cnt));
	t.set_base(VT_rec_unpin_cnt, TMP_GET_STAT(rec_unpin_cnt));
	t.set_base(VT_rec_repin_cvt, TMP_GET_STAT(rec_repin_cvt));
	t.set_base(VT_fm_pagecache_hit, TMP_GET_STAT(fm_pagecache_hit));
	t.set_base(VT_fm_page_nolatch, TMP_GET_STAT(fm_page_nolatch));
	t.set_base(VT_fm_page_moved, TMP_GET_STAT(fm_page_moved));
	t.set_base(VT_fm_page_invalid, TMP_GET_STAT(fm_page_invalid));
	t.set_base(VT_fm_page_nolock, TMP_GET_STAT(fm_page_nolock));
	t.set_base(VT_fm_alloc_page_reject, TMP_GET_STAT(fm_alloc_page_reject));
	t.set_base(VT_fm_page_full, TMP_GET_STAT(fm_page_full));
	t.set_base(VT_fm_error_not_handled, TMP_GET_STAT(fm_error_not_handled));
	t.set_base(VT_fm_ok, TMP_GET_STAT(fm_ok));
	t.set_base(VT_fm_histogram_hit, TMP_GET_STAT(fm_histogram_hit));
	t.set_base(VT_fm_search_pages, TMP_GET_STAT(fm_search_pages));
	t.set_base(VT_fm_search_failed, TMP_GET_STAT(fm_search_failed));
	t.set_base(VT_fm_search_hit, TMP_GET_STAT(fm_search_hit));
	t.set_base(VT_fm_lastpid_cached, TMP_GET_STAT(fm_lastpid_cached));
	t.set_base(VT_fm_lastpid_hit, TMP_GET_STAT(fm_lastpid_hit));
	t.set_base(VT_fm_alloc_pg, TMP_GET_STAT(fm_alloc_pg));
	t.set_base(VT_fm_ext_touch, TMP_GET_STAT(fm_ext_touch));
	t.set_base(VT_fm_ext_touch_nop, TMP_GET_STAT(fm_ext_touch_nop));
	t.set_base(VT_fm_nospace, TMP_GET_STAT(fm_nospace));
	t.set_base(VT_fm_cache, TMP_GET_STAT(fm_cache));
	t.set_base(VT_fm_compact, TMP_GET_STAT(fm_compact));
	t.set_base(VT_fm_append, TMP_GET_STAT(fm_append));
	t.set_base(VT_fm_appendonly, TMP_GET_STAT(fm_appendonly));
	t.set_base(VT_bt_find_cnt, TMP_GET_STAT(bt_find_cnt));
	t.set_base(VT_bt_insert_cnt, TMP_GET_STAT(bt_insert_cnt));
	t.set_base(VT_bt_remove_cnt, TMP_GET_STAT(bt_remove_cnt));
	t.set_base(VT_bt_traverse_cnt, TMP_GET_STAT(bt_traverse_cnt));
	t.set_base(VT_bt_partial_traverse_cnt, TMP_GET_STAT(bt_partial_traverse_cnt));
	t.set_base(VT_bt_restart_traverse_cnt, TMP_GET_STAT(bt_restart_traverse_cnt));
	t.set_base(VT_bt_posc, TMP_GET_STAT(bt_posc));
	t.set_base(VT_bt_scan_cnt, TMP_GET_STAT(bt_scan_cnt));
	t.set_base(VT_bt_splits, TMP_GET_STAT(bt_splits));
	t.set_base(VT_bt_leaf_splits, TMP_GET_STAT(bt_leaf_splits));
	t.set_base(VT_bt_cuts, TMP_GET_STAT(bt_cuts));
	t.set_base(VT_bt_grows, TMP_GET_STAT(bt_grows));
	t.set_base(VT_bt_shrinks, TMP_GET_STAT(bt_shrinks));
	t.set_base(VT_bt_links, TMP_GET_STAT(bt_links));
	t.set_base(VT_bt_upgrade_fail_retry, TMP_GET_STAT(bt_upgrade_fail_retry));
	t.set_base(VT_bt_clr_smo_traverse, TMP_GET_STAT(bt_clr_smo_traverse));
	t.set_base(VT_bt_pcompress, TMP_GET_STAT(bt_pcompress));
	t.set_base(VT_bt_plmax, TMP_GET_STAT(bt_plmax));
	t.set_base(VT_bt_update_cnt, TMP_GET_STAT(bt_update_cnt));
	t.set_base(VT_sort_keycmp_cnt, TMP_GET_STAT(sort_keycmp_cnt));
	t.set_base(VT_sort_lexindx_cnt, TMP_GET_STAT(sort_lexindx_cnt));
	t.set_base(VT_sort_getinfo_cnt, TMP_GET_STAT(sort_getinfo_cnt));
	t.set_base(VT_sort_mof_cnt, TMP_GET_STAT(sort_mof_cnt));
	t.set_base(VT_sort_umof_cnt, TMP_GET_STAT(sort_umof_cnt));
	t.set_base(VT_sort_memcpy_cnt, TMP_GET_STAT(sort_memcpy_cnt));
	t.set_base(VT_sort_memcpy_bytes, TMP_GET_STAT(sort_memcpy_bytes));
	t.set_base(VT_sort_keycpy_cnt, TMP_GET_STAT(sort_keycpy_cnt));
	t.set_base(VT_sort_mallocs, TMP_GET_STAT(sort_mallocs));
	t.set_base(VT_sort_malloc_bytes, TMP_GET_STAT(sort_malloc_bytes));
	t.set_base(VT_sort_malloc_hiwat, TMP_GET_STAT(sort_malloc_hiwat));
	t.set_base(VT_sort_malloc_max, TMP_GET_STAT(sort_malloc_max));
	t.set_base(VT_sort_malloc_curr, TMP_GET_STAT(sort_malloc_curr));
	t.set_base(VT_sort_tmpfile_cnt, TMP_GET_STAT(sort_tmpfile_cnt));
	t.set_base(VT_sort_tmpfile_bytes, TMP_GET_STAT(sort_tmpfile_bytes));
	t.set_base(VT_sort_duplicates, TMP_GET_STAT(sort_duplicates));
	t.set_base(VT_sort_page_fixes, TMP_GET_STAT(sort_page_fixes));
	t.set_base(VT_sort_page_fixes_2, TMP_GET_STAT(sort_page_fixes_2));
	t.set_base(VT_sort_lg_page_fixes, TMP_GET_STAT(sort_lg_page_fixes));
	t.set_base(VT_sort_rec_pins, TMP_GET_STAT(sort_rec_pins));
	t.set_base(VT_sort_files_created, TMP_GET_STAT(sort_files_created));
	t.set_base(VT_sort_recs_created, TMP_GET_STAT(sort_recs_created));
	t.set_base(VT_sort_rec_bytes, TMP_GET_STAT(sort_rec_bytes));
	t.set_base(VT_sort_runs, TMP_GET_STAT(sort_runs));
	t.set_base(VT_sort_run_size, TMP_GET_STAT(sort_run_size));
	t.set_base(VT_sort_phases, TMP_GET_STAT(sort_phases));
	t.set_base(VT_sort_ntapes, TMP_GET_STAT(sort_ntapes));
	t.set_base(VT_page_fix_cnt, TMP_GET_STAT(page_fix_cnt));
	t.set_base(VT_page_refix_cnt, TMP_GET_STAT(page_refix_cnt));
	t.set_base(VT_page_unfix_cnt, TMP_GET_STAT(page_unfix_cnt));
	t.set_base(VT_page_alloc_cnt, TMP_GET_STAT(page_alloc_cnt));
	t.set_base(VT_page_dealloc_cnt, TMP_GET_STAT(page_dealloc_cnt));
	t.set_base(VT_page_btree_alloc, TMP_GET_STAT(page_btree_alloc));
	t.set_base(VT_page_file_alloc, TMP_GET_STAT(page_file_alloc));
	t.set_base(VT_page_file_mrbt_alloc, TMP_GET_STAT(page_file_mrbt_alloc));
	t.set_base(VT_page_btree_dealloc, TMP_GET_STAT(page_btree_dealloc));
	t.set_base(VT_page_file_dealloc, TMP_GET_STAT(page_file_dealloc));
	t.set_base(VT_page_file_mrbt_dealloc, TMP_GET_STAT(page_file_mrbt_dealloc));
	t.set_base(VT_ext_lookup_hits, TMP_GET_STAT(ext_lookup_hits));
	t.set_base(VT_ext_lookup_misses, TMP_GET_STAT(ext_lookup_misses));
	t.set_base(VT_alloc_page_in_ext, TMP_GET_STAT(alloc_page_in_ext));
	t.set_base(VT_extent_lsearch, TMP_GET_STAT(extent_lsearch));
	t.set_base(VT_begin_xct_cnt, TMP_GET_STAT(begin_xct_cnt));
	t.set_base(VT_commit_xct_cnt, TMP_GET_STAT(commit_xct_cnt));
	t.set_base(VT_abort_xct_cnt, TMP_GET_STAT(abort_xct_cnt));
	t.set_base(VT_log_warn_abort_cnt, TMP_GET_STAT(log_warn_abort_cnt));
	t.set_base(VT_prepare_xct_cnt, TMP_GET_STAT(prepare_xct_cnt));
	t.set_base(VT_rollback_savept_cnt, TMP_GET_STAT(rollback_savept_cnt));
	t.set_base(VT_mpl_attach_cnt, TMP_GET_STAT(mpl_attach_cnt));
	t.set_base(VT_anchors, TMP_GET_STAT(anchors));
	t.set_base(VT_compensate_in_log, TMP_GET_STAT(compensate_in_log));
	t.set_base(VT_compensate_in_xct, TMP_GET_STAT(compensate_in_xct));
	t.set_base(VT_compensate_records, TMP_GET_STAT(compensate_records));
	t.set_base(VT_compensate_skipped, TMP_GET_STAT(compensate_skipped));
	t.set_base(VT_log_switches, TMP_GET_STAT(log_switches));
	t.set_base(VT_await_1thread_log, TMP_GET_STAT(await_1thread_log));
	t.set_base(VT_acquire_1thread_log, TMP_GET_STAT(acquire_1thread_log));
	t.set_base(VT_get_logbuf, TMP_GET_STAT(get_logbuf));
	t.set_base(VT_await_1thread_xct, TMP_GET_STAT(await_1thread_xct));
	t.set_base(VT_await_vol_monitor, TMP_GET_STAT(await_vol_monitor));
	t.set_base(VT_need_vol_lock_r, TMP_GET_STAT(need_vol_lock_r));
	t.set_base(VT_need_vol_lock_w, TMP_GET_STAT(need_vol_lock_w));
	t.set_base(VT_await_vol_lock_r, TMP_GET_STAT(await_vol_lock_r));
	t.set_base(VT_await_vol_lock_w, TMP_GET_STAT(await_vol_lock_w));
	t.set_base(VT_await_vol_lock_r_pct, TMP_GET_STAT(await_vol_lock_r_pct));
	t.set_base(VT_await_vol_lock_w_pct, TMP_GET_STAT(await_vol_lock_w_pct));
	t.set_base(VT_s_prepared, TMP_GET_STAT(s_prepared));
	t.set_base(VT_lock_query_cnt, TMP_GET_STAT(lock_query_cnt));
	t.set_base(VT_unlock_request_cnt, TMP_GET_STAT(unlock_request_cnt));
	t.set_base(VT_lock_request_cnt, TMP_GET_STAT(lock_request_cnt));
	t.set_base(VT_lock_acquire_cnt, TMP_GET_STAT(lock_acquire_cnt));
	t.set_base(VT_lock_head_t_cnt, TMP_GET_STAT(lock_head_t_cnt));
	t.set_base(VT_lock_await_alt_cnt, TMP_GET_STAT(lock_await_alt_cnt));
	t.set_base(VT_lock_extraneous_req_cnt, TMP_GET_STAT(lock_extraneous_req_cnt));
	t.set_base(VT_lock_conversion_cnt, TMP_GET_STAT(lock_conversion_cnt));
	t.set_base(VT_lock_cache_hit_cnt, TMP_GET_STAT(lock_cache_hit_cnt));
	t.set_base(VT_lock_request_t_cnt, TMP_GET_STAT(lock_request_t_cnt));
	t.set_base(VT_lock_esc_to_page, TMP_GET_STAT(lock_esc_to_page));
	t.set_base(VT_lock_esc_to_store, TMP_GET_STAT(lock_esc_to_store));
	t.set_base(VT_lock_esc_to_volume, TMP_GET_STAT(lock_esc_to_volume));
	t.set_base(VT_lk_vol_acq, TMP_GET_STAT(lk_vol_acq));
	t.set_base(VT_lk_store_acq, TMP_GET_STAT(lk_store_acq));
	t.set_base(VT_lk_page_acq, TMP_GET_STAT(lk_page_acq));
	t.set_base(VT_lk_kvl_acq, TMP_GET_STAT(lk_kvl_acq));
	t.set_base(VT_lk_rec_acq, TMP_GET_STAT(lk_rec_acq));
	t.set_base(VT_lk_ext_acq, TMP_GET_STAT(lk_ext_acq));
	t.set_base(VT_lk_user1_acq, TMP_GET_STAT(lk_user1_acq));
	t.set_base(VT_lk_user2_acq, TMP_GET_STAT(lk_user2_acq));
	t.set_base(VT_lk_user3_acq, TMP_GET_STAT(lk_user3_acq));
	t.set_base(VT_lk_user4_acq, TMP_GET_STAT(lk_user4_acq));
	t.set_base(VT_lock_wait_cnt, TMP_GET_STAT(lock_wait_cnt));
	t.set_base(VT_lock_block_cnt, TMP_GET_STAT(lock_block_cnt));
	t.set_base(VT_lk_vol_wait, TMP_GET_STAT(lk_vol_wait));
	t.set_base(VT_lk_store_wait, TMP_GET_STAT(lk_store_wait));
	t.set_base(VT_lk_page_wait, TMP_GET_STAT(lk_page_wait));
	t.set_base(VT_lk_kvl_wait, TMP_GET_STAT(lk_kvl_wait));
	t.set_base(VT_lk_rec_wait, TMP_GET_STAT(lk_rec_wait));
	t.set_base(VT_lk_ext_wait, TMP_GET_STAT(lk_ext_wait));
	t.set_base(VT_lk_user1_wait, TMP_GET_STAT(lk_user1_wait));
	t.set_base(VT_lk_user2_wait, TMP_GET_STAT(lk_user2_wait));
	t.set_base(VT_lk_user3_wait, TMP_GET_STAT(lk_user3_wait));
	t.set_base(VT_lk_user4_wait, TMP_GET_STAT(lk_user4_wait));
	t.set_base(VT_sli_requested, TMP_GET_STAT(sli_requested));
	t.set_base(VT_sli_acquired, TMP_GET_STAT(sli_acquired));
	t.set_base(VT_sli_upgrades, TMP_GET_STAT(sli_upgrades));
	t.set_base(VT_sli_eligible, TMP_GET_STAT(sli_eligible));
	t.set_base(VT_sli_inherited, TMP_GET_STAT(sli_inherited));
	t.set_base(VT_sli_used, TMP_GET_STAT(sli_used));
	t.set_base(VT_sli_too_weak, TMP_GET_STAT(sli_too_weak));
	t.set_base(VT_sli_found_late, TMP_GET_STAT(sli_found_late));
	t.set_base(VT_sli_purged, TMP_GET_STAT(sli_purged));
	t.set_base(VT_sli_invalidated, TMP_GET_STAT(sli_invalidated));
	t.set_base(VT_sli_kept, TMP_GET_STAT(sli_kept));
	t.set_base(VT_sli_evicted, TMP_GET_STAT(sli_evicted));
	t.set_base(VT_sli_no_parent, TMP_GET_STAT(sli_no_parent));
	t.set_base(VT_sli_waited_on, TMP_GET_STAT(sli_waited_on));
	t.set_base(VT_dir_cache_hit, TMP_GET_STAT(dir_cache_hit));
	t.set_base(VT_dir_cache_miss, TMP_GET_STAT(dir_cache_miss));
	t.set_base(VT_dir_cache_inherit, TMP_GET_STAT(dir_cache_inherit));
	t.set_base(VT_dir_cache_stale, TMP_GET_STAT(dir_cache_stale));

#endif /* SM_STATS_T_COLLECT_GEN_CPP */
