## Process this file with automake to produce Makefile.in
include $(top_srcdir)/Makefile.generic
AM_CXXFLAGS    += -I$(top_srcdir)/src/common \
		  -I$(top_srcdir)/src/sthread \
		  -I$(top_srcdir)/src/fc 

RESULT_COMPARISON_FILES = \
	lsns-out mapp-out bitmap-out errcodes-out latch1-out \
	latch2-out latch3-out opaque-out vectors-out \
	kvl-out.x86_64 kvl-out.athlon kvl-out.sun4v

## no-op rule for making the -out files
## this is to avoid automagic makefiles thinking that the -out
## files are executable files to be made from the cpp files of the same
## prefix
$(RESULT_COMPARISON_FILES) : noop

noop:

EXTRA_DIST = testall  $(RESULT_COMPARISON_FILES)

check_PROGRAMS     = bitmap$(EXEEXT)  kvl$(EXEEXT) opaque$(EXEEXT) \
		   vectors$(EXEEXT) \
		   errcodes$(EXEEXT) \
		   lsns$(EXEEXT) \
		   mapp$(EXEEXT) \
		   latch1$(EXEEXT)

TESTS = testall

LDADD = ../libcommon.a ../../sthread/libsthread.a ../../fc/libfc.a $(LOCALLDADD)

bitmap_SOURCES      = bitmap.cpp

errcodes_SOURCES      = errcodes.cpp

kvl_SOURCES      = kvl.cpp

lsns_SOURCES      = lsns.cpp

mapp_SOURCES      = mapp.cpp

opaque_SOURCES      = opaque.cpp

vectors_SOURCES      = vectors.cpp

latch1_SOURCES      = latch1.cpp
