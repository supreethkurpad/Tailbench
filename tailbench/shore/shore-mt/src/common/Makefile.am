## Process this file with automake to produce Makefile.in
SUBDIRS = tests
include $(top_srcdir)/Makefile.generic
EXTRA_DIST = opt_error.dat regex_engine.cpp

AM_CXXFLAGS          += -I. \
		       -I$(top_srcdir)/config \
		       -I$(top_srcdir)/src/fc \
		       -I$(top_srcdir)/src/sthread

lib_LIBRARIES     = libcommon.a

OPT_ERROR_GENFILES_H  = opt_errmsg_gen.h opt_einfo_gen.h \
		       	opt_error_def_gen.h \
			    opt_einfo_bakw_gen.h

GENFILES = $(OPT_ERROR_GENFILES_H)

include_HEADERS    = \
	basics.h bitmap.h \
	devid_t.h \
	lsn.h \
	hostname.h kvl_t.h latch.h \
	lid_t.h option.h regcomp_i.h \
	regerror_i.h regex.h regex2.h \
	regex_cclass.h regex_cname.h regex_engine_i.h \
	regex_posix.h regex_utils.h  \
	store_latch_manager.h \
	sm_vas.h stid_t.h \
	tid_t.h umemcmp.h vec_t.h \
	vid_t.h w_opaque.h \
	$(GENFILES)

libcommon_a_SOURCES      = \
	basics.cpp bitmap.cpp devid_t.cpp latch.cpp lid_t.cpp kvl_t.cpp	\
	lsn.cpp option.cpp \
    regerror.cpp regcomp.cpp \
    regexec.cpp regfree.cpp regex_posix.cpp \
	store_latch_manager.cpp \
	stid_t.cpp tid_t.cpp vec_mkchunk.cpp		\
	vec_t.cpp vid_t.cpp 


MOSTLYCLEANFILES = $(GENFILES)

option.o: $(OPT_ERROR_GENFILES_H)
$(OPT_ERROR_GENFILES_H): opt_error.dat
	$(PERL) $(top_srcdir)/tools/errors.pl -d $?
