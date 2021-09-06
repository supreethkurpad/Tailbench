## Process this file with automake to produce Makefile.in
SUBDIRS = tests
include $(top_srcdir)/Makefile.generic
EXTRA_DIST = st_error.dat sthread_stats.dat 

AM_CXXFLAGS          += -I. \
		       -I$(top_srcdir)/config \
		       -I$(top_srcdir)/src/fc


lib_LIBRARIES     = libsthread.a

ST_ERROR_GENFILES  = st_errmsg_gen.h\
		     st_einfo_gen.h\
		     st_error_def_gen.h\
		     st_error_enum_gen.h\
		     st_einfo_bakw_gen.h

STHREAD_GENFILES_H  = \
    sthread_stats_collect_enum_gen.h \
	sthread_stats_msg_gen.h \
	sthread_stats_struct_gen.h

STHREAD_GENFILES_CPP = \
	sthread_stats_collect_gen.cpp \
	sthread_stats_dec_gen.cpp \
	sthread_stats_inc_gen.cpp \
	sthread_stats_out_gen.cpp 


GENFILES_H = $(SHMC_GENFILES_H)  $(STHREAD_GENFILES_H) $(ST_ERROR_GENFILES)

GENFILES_CPP = $(SHMC_GENFILES_CPP)  $(STHREAD_GENFILES_CPP) 

GENFILES = $(GENFILES_H) $(GENFILES_CPP)

include_HEADERS = \
	$(GENFILES_H) \
	auto_release.h \
	mcs_lock.h \
	os_fcntl.h os_interface.h \
	sdisk.h \
	sdisk_unix.h \
	stcore_pthread.h \
	srwlock.h \
	sthread.h \
	sthread_stats.h sthread_vtable_enum.h 

libsthread_a_SOURCES      = \
	sthread.cpp \
	sthread_core_pthread.cpp \
	sthread_stats.cpp \
	srwlock.cpp \
	no-inline.cpp \
	io.cpp \
	sdisk_unix.cpp \
	sdisk.cpp \
	vtable_sthread.cpp


# libsthread_a_LIBADD       = @LIBOBJS@

MOSTLYCLEANFILES = $(GENFILES_H) $(GENFILES_CPP)

## sthread.h: st_error_enum_gen.h
## sthread_stats.h: sthread_stats_struct_gen.h

$(ST_ERROR_GENFILES): st_error.dat
	$(PERL) $(top_srcdir)/tools/errors.pl --d --e $?


event.$(OBJEXT): $(STHREAD_GENFILES_H)
sthread_stats.$(OBJEXT): $(STHREAD_GENFILES_CPP)
## vtable_sthread.$(OBJEXT): $(STHREAD_GENFILES_H)
$(STHREAD_GENFILES_CPP) $(STHREAD_GENFILES_H): sthread_stats.dat
	$(PERL) $(top_srcdir)/tools/stats.pl $?

