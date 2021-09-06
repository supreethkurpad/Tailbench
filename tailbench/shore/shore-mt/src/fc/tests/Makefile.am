## Process this file with automake to produce Makefile.in
include $(top_srcdir)/Makefile.generic

RESULT_COMPARISON_FILES = stats2-out \
	 box.2-out box.3-out box.4-out \
	 list1-out  darray-out memblock-out

EXTRA_DIST = testall test_stat.dat test_stat.h $(RESULT_COMPARISON_FILES) test_me.h

## no-op rule for making the -out files
## this is to avoid automagic makefiles thinking that the -out
## files are executable files to be made from the cpp files of the same
## prefix
$(RESULT_COMPARISON_FILES) : noop

noop:

AM_CXXFLAGS    += -I$(top_srcdir)/src/fc

check_PROGRAMS     = list1$(EXEEXT) list2$(EXEEXT) list3$(EXEEXT) \
		hash1$(EXEEXT) hash2$(EXEEXT) \
		ntime$(EXEEXT) \
			darray$(EXEEXT) \
			memblock$(EXEEXT) \
	        heap$(EXEEXT) \
	       	rc$(EXEEXT) \
			errlogtest$(EXEEXT) \
	       	random$(EXEEXT) \
	       	stats$(EXEEXT) \
	       	stats2$(EXEEXT) \
	       	box$(EXEEXT) \
	       	io$(EXEEXT) \
	       	ss$(EXEEXT) \
	       	rand$(EXEEXT) \
	       	container$(EXEEXT) \
	       	cuckoo$(EXEEXT) \
	       	findprime$(EXEEXT) \
	       	findsizes$(EXEEXT) \
	       	bitvector$(EXEEXT) \
	       	basic_sizes$(EXEEXT)


TESTS = testall

LDADD = ../libfc.a  $(LOCALLDADD)

memblock_SOURCES = memblock.cpp

darray_SOURCES = darray.cpp

basic_sizes_SOURCES      = basic_sizes.cpp

ss_SOURCES      = ss.cpp

list1_SOURCES      = list1.cpp

list2_SOURCES      = list2.cpp

list3_SOURCES      = list3.cpp

hash1_SOURCES      = hash1.cpp

hash2_SOURCES      = hash2.cpp

ntime_SOURCES      = ntime.cpp

heap_SOURCES      = heap.cpp

rc_SOURCES      = rc.cpp

errlogtest_SOURCES      = errlogtest.cpp

random_SOURCES      = random.cpp

stats_SOURCES      = stats.cpp

stats2_SOURCES      = test_stat.cpp stats2.cpp

box_SOURCES      = box.cpp

io_SOURCES      = io.cpp

rand_SOURCES      = rand.cpp

container_SOURCES      = container.cpp

findprime_SOURCES      = findprime.cpp

findsizes_SOURCES      = findsizes.cpp

cuckoo_SOURCES      = cuckoo.cpp

bitvector_SOURCES      = bitvector.cpp


TEST = testall
testall:  $(check_PROGRAMS)

TEST_STAT_GENFILES = test_stat_struct_gen.h \
                     test_stat_msg_gen.h \
                     test_stat_collect_enum_gen.h \
                     test_stat_collect_gen.cpp \
                     test_stat_dec_gen.cpp \
                     test_stat_inc_gen.cpp \
                     test_stat_out_gen.cpp

GENFILES = $(TEST_STAT_GENFILES)

MOSTLYCLEANFILES = $(GENFILES)

test_stat.$(OBJEXT): $(TEST_STAT_GENFILES)
stats.$(OBJEXT):  $(TEST_STAT_GENFILES)

$(TEST_STAT_GENFILES): test_stat.dat
	$(PERL) $(top_srcdir)/tools/stats.pl $?
