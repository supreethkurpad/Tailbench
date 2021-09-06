/*<std-header orig-src='shore'>

 $Id: thread3.cpp,v 1.31.2.5 2010/03/19 22:20:03 nhall Exp $

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

/*
 * If possible compile this _without_ optimization; so the 
 * register variables are really put in registers.
 */

#include <cassert>
#include <w_getopt.h>

#include <w.h>
#include <sthread.h>

#include <iostream>
#include <sstream>
#include <iomanip>

__thread stringstream *_safe_io(NULL);
void safe_io_init() 
{ 
	if (_safe_io==NULL) _safe_io = new stringstream; 
}

#define SAFE_IO(XXXX) { safe_io_init(); \
	*_safe_io <<  XXXX; \
	fprintf(stdout, _safe_io->str().c_str()); }

class float_thread_t : public sthread_t {
public:
	float_thread_t(int fid);
protected:
	virtual void run();
private:
	int id;
};

class int_thread_t : public sthread_t {
public:
	int_thread_t(int fid);
protected:
	virtual void run();
private:
	int id;
};

float_thread_t::float_thread_t(int fid)
: id(fid)
{
	w_ostrstream_buf s(40);		// XXX magic number
	s << "float[" << id << "]" << ends;
	rename(s.c_str());
}
    

int_thread_t::int_thread_t(int fid)
: id(fid)
{
	w_ostrstream_buf s(40);		// XXX magic number
	s << "int[" << id << "]" << ends;
	rename(s.c_str());
}


int	NumFloatThreads = 4;
int	NumIntThreads = 4;
bool	mix_it_up = false;
bool	verbose = false;

int	parse_args(int, char **);

int	*ack;
sthread_t **worker;

void harvest(int threads)
{
	int	i;

	for(i=0; i < threads; ++i){
		W_COERCE( worker[i]->join() );
		if (verbose) {
			SAFE_IO( "Finished: " << endl << *worker[i] << endl);
		}

		w_assert1(ack[i]);
		delete worker[i];
	}
}

int main(int argc, char **argv)
{
	int i;
	int	threads;

	if (parse_args(argc, argv) == -1)
		return 1;

	if (mix_it_up)
		threads = NumFloatThreads + NumIntThreads;
	else
		threads = NumFloatThreads > NumIntThreads ?
			    NumFloatThreads : NumIntThreads;

	ack = new int[threads];
	if (!ack)
		W_FATAL(fcOUTOFMEMORY);
	worker = new sthread_t *[threads];
	if (!worker)
		W_FATAL(fcOUTOFMEMORY);

	for (i=0; i<NumIntThreads; ++i) {
		ack[i] = 0;
		worker[i] = new int_thread_t(i);
		w_assert1(worker[i]);
		W_COERCE( worker[i]->fork() );
	}

	if (!mix_it_up)
		harvest(NumIntThreads);

	int	base = mix_it_up ? NumIntThreads : 0;
	
	for(i=base ; i < base + NumFloatThreads; ++i){
		ack[i] = 0;
		worker[i] = new float_thread_t(i);
		w_assert1(worker[i]);
		W_COERCE( worker[i]->fork() );
	}
	harvest(mix_it_up ? threads : NumFloatThreads);

	delete [] worker;
	delete [] ack;

	return 0;
}


int	parse_args(int argc, char **argv)
{
	int	c;
	int	errors = 0;

	while ((c = getopt(argc, argv, "i:f:mv")) != EOF) {
		switch (c) {
		case 'i':
			NumIntThreads = atoi(optarg);
			break;
		case 'f':
			NumFloatThreads = atoi(optarg);
			break;
		case 'm':
			mix_it_up = true;
			break;
		case 'v':
			verbose = true;
			break;
		default:
			errors++;
			break;
		}
	}
	if (errors) {
		cerr << "usage: " << argv[0]
			<< " [-i int_threads]"
			<< " [-f float_threads]"
			<< " [-m]"
			<< " [-v]"
			<< endl;
	}
	return errors ? -1 : optind;
}


void float_thread_t::run()
{
    float result;
    register float r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13,
	r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25,
	r26, r27, r28, r29, r30, r31;

    r1 = (float) id;
    r2 = (float) id;
    r3 = (float) id;
    r4 = (float) id;
    r5 = (float) id;
    r6 = (float) id;
    r7 = (float) id;
    r8 = (float) id;
    r9 = (float) id;
    r10 = (float) id;
    r11 = (float) id;
    r12 = (float) id;
    r13 = (float) id;
    r14 = (float) id;
    r15= (float) id;
    r16= (float) id;
    r17= (float) id;
    r18= (float) id;
    r19= (float) id;
    r20 = (float) id;
    r21 = (float) id;
    r22 = (float) id;
    r23 = (float) id;
    r24 = (float) id;
    r25 = (float) id;
    r26 = (float) id;
    r27 = (float) id;
    r28 = (float) id;
    r29 = (float) id;
    r30 = (float) id;
    r31 = (float) id;

    result = r1+ r2+ r3+ r4+ r5+ r6+ r7+ r8+ r9+ r10+ r11+ r12+ r13+
	r14+ r15+ r16+ r17+ r18+ r19+ r20+ r21+ r22+ r23+ r24+ r25+
	r26+ r27+ r28+ r29+ r30+ r31;

    SAFE_IO ( "Hello, world from " << id 
	 << ", result = " 
	 << setprecision(6) << setiosflags(ios::fixed) 
	 << result << resetiosflags(ios::fixed)
	 << ", check = " << 31 * id << endl
    )
    sthread_t::yield();

    result = r1+ r2+ r3+ r4+ r5+ r6+ r7+ r8+ r9+ r10+ r11+ r12+ r13+
	r14+ r15+ r16+ r17+ r18+ r19+ r20+ r21+ r22+ r23+ r24+ r25+
	r26+ r27+ r28+ r29+ r30+ r31;

    SAFE_IO( "Hello, world from " << id 
	 << ", result = " << setprecision(6) << setiosflags(ios::fixed) 
	 << result << resetiosflags(ios::fixed)
	 << ", check = " << 31 * id << endl;)

    sthread_t::yield();
    ack[id] = 1;
}

void int_thread_t::run()
{
    int result;
    register int r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13,
	r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24, r25,
	r26, r27, r28, r29, r30, r31;

    r1 = id;
    r2 = id;
    r3 = id;
    r4 = id;
    r5 = id;
    r6 = id;
    r7 = id;
    r8 = id;
    r9 = id;
    r10 = id;
    r11 = id;
    r12 = id;
    r13 = id;
    r14 = id;
    r15= id;
    r16= id;
    r17= id;
    r18= id;
    r19= id;
    r20 = id;
    r21 = id;
    r22 = id;
    r23 = id;
    r24 = id;
    r25 = id;
    r26 = id;
    r27 = id;
    r28 = id;
    r29 = id;
    r30 = id;
    r31 = id;

    result = r1+ r2+ r3+ r4+ r5+ r6+ r7+ r8+ r9+ r10+ r11+ r12+ r13+
	r14+ r15+ r16+ r17+ r18+ r19+ r20+ r21+ r22+ r23+ r24+ r25+
	r26+ r27+ r28+ r29+ r30+ r31;

    SAFE_IO( "Hello, world from " << id 
	 << ", result = " << result
	 << ", check = " << 31 * id << endl;)
    sthread_t::yield();

    result = r1+ r2+ r3+ r4+ r5+ r6+ r7+ r8+ r9+ r10+ r11+ r12+ r13+
	r14+ r15+ r16+ r17+ r18+ r19+ r20+ r21+ r22+ r23+ r24+ r25+
	r26+ r27+ r28+ r29+ r30+ r31;
    SAFE_IO( "Hello, world from " << id 
	 << ", result = " << result
	 << ", check = " << 31 * id << endl;)
    sthread_t::yield();
    ack[id] = 1;
}

