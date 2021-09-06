## Process this file with automake to produce Makefile.in
include $(top_srcdir)/Makefile.generic
RESULT_COMPARISON_FILES =  thread3-out

EXTRA_DIST = testall $(RESULT_COMPARISON_FILES)
AM_CXXFLAGS    += -I$(top_srcdir)/src/sthread \
		-I$(top_srcdir)/src/fc
## no-op rule for making the -out files
## this is to avoid automagic makefiles thinking that the -out
## files are executable files to be made from the cpp files of the same
## prefix
$(RESULT_COMPARISON_FILES) : noop

noop:

check_PROGRAMS     = thread1$(EXEEXT) thread2$(EXEEXT)\
		     thread3$(EXEEXT) thread4$(EXEEXT) \
		     ioperf$(EXEEXT) mmap$(EXEEXT) \
		     except$(EXEEXT) pthread_test$(EXEEXT)

TESTS = testall

LDADD = ../libsthread.a ../../fc/libfc.a $(LOCALLDADD)

pthread_test_SOURCES  = pthread_test.cpp

thread1_SOURCES      = thread1.cpp

thread2_SOURCES      = thread2.cpp

thread3_SOURCES      = thread3.cpp

thread4_SOURCES      = thread4.cpp

ioperf_SOURCES      = ioperf.cpp

except_SOURCES      = except.cpp

mmap_SOURCES      = mmap.cpp
