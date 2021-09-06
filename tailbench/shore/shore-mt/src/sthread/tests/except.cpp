/*<std-header orig-src='shore'>

 $Id: except.cpp,v 1.9.2.4 2010/03/19 22:20:03 nhall Exp $

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

#include <iostream>
#include <w_strstream.h>

#include <cassert>
#include <w_getopt.h>

#include <w.h>
#include <sthread.h>

class MyException {
public:
	int	error;

	MyException(int err = 0) : error(err) { }
	MyException(w_rc_t e) : error(e.err_num()) { }

	ostream &print(ostream &o) const {
		return o << "MyException(error = " << error << ")";
	}
};

ostream &operator<<(ostream &o, const MyException &e)
{
	return e.print(o);
}

class except_thread_t : public sthread_t {
public:
	except_thread_t(int fid, bool do_catch);
protected:
	void	run();
private:
	int	id;
	bool	do_catch;		// let exception slide past run
	void	NestExcept(int count);
	void	_run();
};


except_thread_t::except_thread_t(int fid, bool _do_catch)
: id(fid),
  do_catch(_do_catch)
{
	w_ostrstream_buf s(40);		// XXX magic number
	s << "except[" << id << "]" << ends;
	rename(s.c_str());
}
    

int	NumThreads = 4;
int	NumExceptions = 4;
bool	verbose = false;
enum except_action { x_none, x_all, x_alternate };
except_action	xaction = x_none;

int	parse_args(int, char **);

int		*ack;
sthread_t	**worker;

void harvest(int threads)
{
	int	i;

	for (i = 0; i < threads; ++i) {
		W_COERCE(worker[i]->join());
		if (verbose)
			cout << "Finished: " << endl << *worker[i] << endl;

		if (ack[i])
			cout << "thread " << i << " acknowleded" << endl;
		else
			cout << "thread " << i << " no acknowledge" << endl;

		delete worker[i];
	}
}

int main(int argc, char **argv)
{
	int	i;

	if (parse_args(argc, argv) == -1)
		return 1;

	ack = new int[NumThreads];
	if (!ack)
		W_FATAL(fcOUTOFMEMORY);
	worker = new sthread_t *[NumThreads];
	if (!worker)
		W_FATAL(fcOUTOFMEMORY);

	for (i=0; i < NumThreads; ++i) {
		bool do_catch = (xaction != x_none);
		if (xaction == x_alternate && (i % 2) == 0)
			do_catch = false;
			
		ack[i] = 0;
		worker[i] = new except_thread_t(i, do_catch);
		if (!worker[i])
			W_FATAL(fcOUTOFMEMORY);
		W_COERCE(worker[i]->fork());
	}

	harvest(NumThreads);

	delete [] worker;
	delete [] ack;

	return 0;
}


int	parse_args(int argc, char **argv)
{
	int	c;
	int	errors = 0;

	while ((c = getopt(argc, argv, "t:vxX")) != EOF) {
		switch (c) {
		case 't':
			NumThreads = atoi(optarg);
			break;
		case 'v':
			verbose = true;
			break;
		case 'X':
			xaction = x_alternate;
			break;
		case 'x':
			xaction = x_all;
			break;
		default:
			errors++;
			break;
		}
	}
	if (errors) {
		cerr << "usage: " << argv[0]
			<< " [-t threads]"
			<< " [-v]"
			<< " [-x]"
			<< " [-X]"
			<< endl;
	}
	return errors ? -1 : optind;
}

/* The idea here is to have multiple threads all unrolling
   exceptions at once. */
  
void except_thread_t::NestExcept(int count)
{
	yield();
	if (count == 0) {
	    throw MyException(count);
	    /*NOTREACHED*/
	    w_assert1(0);
	}
	
	try {
		NestExcept(count - 1);
	}
	catch (MyException x) {
		cout << "Caught Exception " << x << " from " << id << endl;
		yield();
		throw MyException(count);
	}
}

void except_thread_t::run()
{
	if (do_catch) {
		try {
			_run();
		}
		catch (...) {
			cerr << name() << ": thread run() catches exception"
			     << endl;
		}
	}
	else
		_run();
}

void except_thread_t::_run()
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

    cout << "Hello, world from " << id 
	 << ", result = " << result
	 << ", check = " << 31 * id << endl << flush;
    
    try {
    	NestExcept(NumExceptions);
    }
    catch (...) {
	result = r1+ r2+ r3+ r4+ r5+ r6+ r7+ r8+ r9+ r10+ r11+ r12+ r13+
	r14+ r15+ r16+ r17+ r18+ r19+ r20+ r21+ r22+ r23+ r24+ r25+
	r26+ r27+ r28+ r29+ r30+ r31;
	cout << "CATCH, world from " << id 
	     << ", result = " << result
	     << ", check = " << 31 * id << endl;
	sthread_t::yield();
	throw;
    }

    result = r1+ r2+ r3+ r4+ r5+ r6+ r7+ r8+ r9+ r10+ r11+ r12+ r13+
	r14+ r15+ r16+ r17+ r18+ r19+ r20+ r21+ r22+ r23+ r24+ r25+
	r26+ r27+ r28+ r29+ r30+ r31;
    cout << "Hello, world from " << id 
	 << ", result = " << result
	 << ", check = " << 31 * id << endl;
    sthread_t::yield();
    ack[id] = 1;
}

