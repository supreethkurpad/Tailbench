## Process this file with automake to produce Makefile.in
SUBDIRS = tests smsh
include $(top_srcdir)/Makefile.generic
EXTRA_DIST = e_error.dat\
	 sm_stats.dat logdef.dat \
	 bf_htab_stats.dat

AM_CXXFLAGS          += -I$(top_srcdir)/src/fc \
		       -I$(top_srcdir)/src/sthread \
		       -I$(top_srcdir)/src/common 

lib_LIBRARIES     = libsm.a

E_ERROR_GENFILES_H = \
	e_errmsg_gen.h \
	e_einfo_gen.h \
	e_error_def_gen.h \
	e_error_enum_gen.h \
	e_einfo_bakw_gen.h

SM_STATS_GENFILES_H = \
	sm_stats_t_collect_enum_gen.h \
	sm_stats_t_msg_gen.h \
	sm_stats_t_struct_gen.h 

SM_STATS_GENFILES_CPP = \
	sm_stats_t_collect_gen.cpp \
	sm_stats_t_dec_gen.cpp \
	sm_stats_t_inc_gen.cpp \
	sm_stats_t_out_gen.cpp 

BF_HTAB_STATS_GENFILES_H = \
	bf_htab_stats_t_collect_enum_gen.h \
	bf_htab_stats_t_msg_gen.h \
	bf_htab_stats_t_struct_gen.h 

BF_HTAB_STATS_GENFILES_CPP = \
	bf_htab_stats_t_collect_gen.cpp \
	bf_htab_stats_t_dec_gen.cpp \
	bf_htab_stats_t_inc_gen.cpp \
	bf_htab_stats_t_out_gen.cpp

LOGDEF_GENFILES_H = \
	logfunc_gen.h \
	logtype_gen.h

LOGDEF_GENFILES_CPP = \
	undo_gen.cpp \
	redo_gen.cpp \
	logdef_gen.cpp \
	logstr_gen.cpp \
	logstub_gen.cpp

GENFILES_CPP = $(SM_STATS_GENFILES_CPP) \
	       $(BF_HTAB_STATS_GENFILES_CPP) \
	       $(LOGDEF_GENFILES_CPP)

GENFILES_H = $(E_ERROR_GENFILES_H) \
	   $(SM_STATS_GENFILES_H) \
	   $(BF_HTAB_STATS_GENFILES_H) \
	   $(LOGDEF_GENFILES_H)

 
GENFILES = $(GENFILES_H) $(GENFILES_CPP)

include_HEADERS = \
	$(GENFILES_H) \
	app_support.h \
	bf.h bf_core.h  bf_htab.h bf_transit_bucket.h\
	bf_prefetch.h bf_s.h \
	btcursor.h btree.h btree_impl.h btree_p.h \
	btree_latch_manager.h \
	chkpt.h chkpt_serial.h \
	crash.h \
        data_access_histogram.h \
	device.h dir.h \
	extent.h \
	file.h file_s.h \
	histo.h keyed.h lexify.h \
	lgrec.h lid.h \
	lock.h lock_cache.h lock_core.h lock_s.h lock_s_inline.h lock_x.h \
	log.h log_core.h partition.h logrec.h \
	key_ranges_map.h \
	page.h page_alias.h page_h.h page_s.h \
	pin.h \
	pmap.h \
	prologue.h \
	ranges_p.h \
	restart.h restart_s.h \
	rtree.h rtree_p.h \
	scan.h sdesc.h \
	sm.h sm_base.h sm_du_stats.h sm_escalation.h \
	sm_int_0.h sm_int_1.h sm_int_2.h sm_int_3.h sm_int_4.h \
	sm_io.h sm_s.h \
	sm_vtable_enum.h \
	smstats.h \
	smthread.h \
	sort.h sort_s.h \
	sysdefs.h \
	vol.h \
	xct.h xct_dependent.h \
	zkeyed.h 

libsm_a_SOURCES      =  \
	bf.cpp bf_core.cpp \
	bf_htab.cpp bf_htab_test.cpp \
	bf_prefetch.cpp \
	btcursor.cpp btree.cpp btree_bl.cpp btree_impl.cpp btree_p.cpp \
	btree_latch_manager.cpp \
	chkpt.cpp chkpt_serial.cpp \
	common_templates.cpp \
	crash.cpp \
	data_access_histogram.cpp device.cpp \
	dir.cpp \
	file.cpp \
	histo.cpp \
	key_ranges_map.cpp keyed.cpp zkeyed.cpp \
	ranges_p.cpp \
	lexify.cpp \
	lgrec.cpp \
	lid.cpp \
	lock.cpp lock_core.cpp \
	log.cpp logrec.cpp logstub.cpp \
	partition.cpp log_core.cpp \
	sort.cpp newsort.cpp \
	page.cpp \
	pin.cpp \
	pmap.cpp \
	restart.cpp \
	rtree.cpp \
	scan.cpp \
	sm.cpp sm-no-inline.cpp \
	sm_du_stats.cpp \
	sm_io.cpp \
	sm_s.cpp \
	smfile.cpp smindex.cpp \
	smstats.cpp \
	smthread.cpp \
	vol.cpp \
	xct.cpp \
	vtable_sm.cpp \
	vtable_smthread.cpp \
	vtable_xct.cpp

MOSTLYCLEANFILES = $(GENFILES) e_error.timestamp logdef.timestamp \
	sm_stats.timestamp

bf.$(OBJEXT): $(E_ERROR_GENFILES_H)
$(E_ERROR_GENFILES_H): e_error.dat
	$(PERL) $(top_srcdir)/tools/errors.pl --d --e $?

bf.$(OBJEXT): $(BF_HTAB_STATS_GENFILES_H)
sm.$(OBJEXT): $(BF_HTAB_STATS_GENFILES_CPP)
$(BF_HTAB_STATS_GENFILES_CPP) $(BF_HTAB_STATS_GENFILES_H): bf_htab_stats.dat
	$(PERL) $(top_srcdir)/tools/stats.pl $?

bf.$(OBJEXT): $(SM_STATS_GENFILES_H)
sm.$(OBJEXT): $(SM_STATS_GENFILES_CPP)
$(SM_STATS_GENFILES_CPP) $(SM_STATS_GENFILES_H): sm_stats.dat
	$(PERL) $(top_srcdir)/tools/stats.pl $?

chkpt.$(OBJEXT): $(LOGDEF_GENFILES_CPP)
$(LOGDEF_GENFILES_H) $(LOGDEF_GENFILES_CPP): logdef.dat
	$(PERL) -w $(top_srcdir)/tools/logdef.pl $?

