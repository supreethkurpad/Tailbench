## Process this file with automake to produce Makefile.in
SUBDIRS = scripts lib
include $(top_srcdir)/Makefile.generic 
smshdir = $(top_srcdir)/src/sm/smsh
EXTRA_DIST = test-short testall testall.maintainer \
	    .shoreconfig.example .smshrc.example runsmsh \
	    README README.2PC README.how-to-start runtest run.cd.perf \
	    $(include_HEADERS)

CLEANFILES = ./log/* ./volumes/*


AM_CXXFLAGS          += -I$(top_srcdir)/src/fc \
		       -I$(top_srcdir)/src/sthread \
		       -I$(top_srcdir)/src/common \
		       -I$(top_srcdir)/src/sm 

AM_CXXFLAGS    += -I$(TCL_INCLUDE)

check_PROGRAMS     = smsh$(EXEEXT) random_kick$(EXEEXT)

TESTS = test-short

random_kick_SOURCES      =  random_kick.cpp

include_HEADERS      =  \
	shell.h smsh.h smsh_error.h \
	sort_funcs.h tcl_thread.h tcl_workaround.h

smsh_SOURCES      =  \
	out_of_log_space.cpp \
	shell.cpp \
	shell2.cpp \
	sm_sizeof.cpp \
	sort_funcs.cpp \
	sort_funcs2.cpp \
	sort_funcs3.cpp \
	sort_funcs4.cpp \
	smsh.cpp \
	smsh_error.cpp \
	tcl_thread.cpp

LDADD      = \
	$(top_builddir)/src/sm/libsm.a \
	$(top_builddir)/src/common/libcommon.a  \
	$(top_builddir)/src/sthread/libsthread.a  \
	$(top_builddir)/src/fc/libfc.a  \
	$(TCL_LIB)  $(LOCALLDADD)
# note: TCL_LIB must be defined in top/Makefile.local

smsh.pure: $(smsh_SOURCES:.cpp=.o)
	$(PURIFY_BIN)/purify $(PURIFY_FLAGS)  -collector=$(PURIFY_COLLECTOR)  $(CXX) -o $@ $(smsh_SOURCES:.cpp=.o) $(LDADD) $(LIBS)

smsh.quant: $(smsh_SOURCES:.cpp=.o)
	$(QUANTIFY_BIN)/quantify $(QUANTIFY_FLAGS)  $(QUANTIFY_COLLECTOR)  $(CXX) -o $@ $(smsh_SOURCES:.cpp=.o) $(LDADD) $(LIBS)

smsh.purecov: $(smsh_SOURCES:.cpp=.o)
	$(PURECOV_BIN)/purecov $(PURECOV_FLAGS)  $(PURECOV_COLLECTOR)  $(CXX) -o $@ $(smsh_SOURCES:.cpp=.o) $(LDADD) $(LIBS)

