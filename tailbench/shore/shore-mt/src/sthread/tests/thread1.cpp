/*<std-header orig-src='shore'>

 $Id: thread1.cpp,v 1.51.2.11 2010/03/19 22:20:03 nhall Exp $

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

#include <w_debug.h>
#include <cstdlib>
#include <cassert>
#include <ctime>

#include <w.h>
#include <sthread.h>
#include <sthread_stats.h>
#include <w_getopt.h>
#include <iostream>
#include <w_strstream.h>


#define DefaultNumThreads 8

#define DefaultPongTimes  10000
#define LocalMsgPongTimes 1000000
#define RemoteMsgPongTimes 100000

#define OUT { w_ostrstream _out; _out
#define  FLUSHOUT \
	fprintf(stdout, "%s", _out.c_str()); \
	fflush(stdout); \
    w_reset_strstream(_out); }

bool	DumpInThreads = false;

class worker_thread_t : public sthread_t {
public:
    worker_thread_t(int id);
protected:
    virtual void run();
private:
    int work_id;
};


class pong_thread_t;

struct ping_pong_t {
	int		    whoseShot;
	pthread_mutex_t	theBall;
	pthread_cond_t paddle[2];

	pong_thread_t	*ping;
	pong_thread_t	*pong;
	
	ping_pong_t() : whoseShot(0), ping(0), pong(0) { 
		DO_PTHREAD(pthread_mutex_init(&theBall, NULL));
		DO_PTHREAD(pthread_cond_init(&paddle[0], NULL));
		DO_PTHREAD(pthread_cond_init(&paddle[1], NULL));
	}
};


class wait_for_t {
	pthread_mutex_t	_lock;
	pthread_cond_t	_done;

	int	expected;
	int	have;

public:
	wait_for_t(int expecting) : expected(expecting), have(0) { 
		DO_PTHREAD(pthread_mutex_init(&_lock, NULL));
		DO_PTHREAD(pthread_cond_init(&_done, NULL));
	}
	void	wait();
	void	done();
};


class pong_thread_t : public sthread_t {
public:
	pong_thread_t(ping_pong_t &game, int _id, wait_for_t &note);

protected:
	void run();

private:
	ping_pong_t	&game;
	int		id;
	wait_for_t	&note;
};


class timer_thread_t : public sthread_t {
public:
	timer_thread_t();

protected:
	void run();
};


class overflow_thread_t : public sthread_t {
public:
	overflow_thread_t() : sthread_t(t_regular, "overflow")
	{ }

protected:
	void run();
	void	recurse(unsigned overflow, char *sp0, char *last);

	enum	{ overflowFrameSize = 8192 };
};

class error_thread_t : public sthread_t {
public:
	error_thread_t() : sthread_t(t_regular, "error")
	{ }

protected:
	void run();
};


void msgPongFunc(void*);
void remotePongFunc(void*);


/* Configurable parameters */
int	NumThreads = DefaultNumThreads;
int	PongTimes = DefaultPongTimes;
int	SleepTime = 10000;		/* 10 seconds */
int	PongGames = 1;
int	StackOverflow = 0xa00110;	/* check stack overflow by allocatings
				   this much on the stack */
bool	DumpThreads = false;
bool	TestAssert = false;
bool	TestFatal = false;
bool	TestErrorInThread = true;
bool	verbose = true;

worker_thread_t		**worker;
int			*ack; 


int	parse_args(int argc, char **argv)
{
	int	errors = 0;
	int	c;

	while ((c = getopt(argc, argv, "afn:p:s:g:o:xXdDTv")) != EOF) {
		switch (c) {
		case 'n':
			NumThreads = atoi(optarg);
			break;
		case 'p':
			PongTimes = atoi(optarg);
			break;
		case 's':
			SleepTime = atoi(optarg);
			break;
		case 'g':
			PongGames = atoi(optarg);
			break;
		case 'o':
			StackOverflow = atoi(optarg);
			break;
		case 'd':
			DumpThreads = true;
			break;
		case 'D':
			DumpInThreads = true;
			break;
		case 'a':
			TestAssert = true;
			break;
		case 'f':
			TestFatal = true;
			break;
		case 'T':
			TestErrorInThread = true;
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
		cerr 
			<< "usage: "
			<< argv[0]
			<< " [-n threads]"
			<< " [-p pongs]"
			<< " [-g pong_games]"
			<< " [-s sleep_time]"
			<< " [-o overflow_alloc]"
			<< " [-a]"
			<< " [-f]"
			<< " [-T]"
			<< endl;
	}

	return errors ? -1 : optind;
}


void playPong()
{
	OUT << "playPong" << endl; FLUSHOUT;

	int	i;
	stime_t	startTime, endTime;
	ping_pong_t	*games;

	games = new ping_pong_t[PongGames];
	if (!games)
		W_FATAL(fcOUTOFMEMORY);
	
	wait_for_t	imdone(PongGames * 2);

	for (i = 0; i < PongGames; i++) {
		/* pongs wait for pings */
		games[i].pong = new pong_thread_t(games[i], 1, imdone);
		w_assert1(games[i].pong);

		games[i].ping = new pong_thread_t(games[i], 0, imdone);
		w_assert1(games[i].ping);

		OUT << "forking pong " << i << endl; FLUSHOUT;
		W_COERCE(games[i].pong->fork());
		OUT << "forking ping " << i << endl; FLUSHOUT;
		W_COERCE(games[i].ping->fork());
	}

	/* and this starts it all :-) */
	startTime = stime_t::now();
	OUT << "waiting " << endl; FLUSHOUT;
	imdone.wait(); // join
	endTime = stime_t::now();
	OUT << "done " << endl; FLUSHOUT;

	OUT << (sinterval_t)((endTime-startTime) / (PongGames*PongTimes))
		<< " per ping." << endl;
	FLUSHOUT;


	for (i = 0; i < PongGames; i++) {
		W_COERCE(games[i].pong->join());
		if (DumpThreads) {
			OUT << "Pong Thread Done:" << endl
				<< *games[i].pong << endl;
			FLUSHOUT;
		}
		delete games[i].pong;
		games[i].pong = 0;

		W_COERCE(games[i].ping->join());
		if (DumpThreads) {
			OUT << "Ping Thread Done:" << endl
				<< *games[i].ping << endl;
			FLUSHOUT;
		}
		delete games[i].ping;
		games[i].ping = 0;
	}
	delete [] games;
}

void doErrorTests()
{
	if (TestAssert) {
		OUT << endl << "** Generating an assertion failure." << endl;
		FLUSHOUT;
		w_assert1(false);
	}

	if (TestFatal) {
		OUT << endl << "** Generating a fatal error." << endl;
		FLUSHOUT;
		W_FATAL(fcINTERNAL);
	}
}

int	main(int argc, char* argv[])
{
    int i;

    int next_arg = parse_args(argc, argv);
    if (next_arg < 0)
	    return 1;

    if (NumThreads) {
	    ack = new int[NumThreads];
	    if (!ack)
		    W_FATAL(fcOUTOFMEMORY);

	    worker = new worker_thread_t *[NumThreads];
	    if (!worker)
		    W_FATAL(fcOUTOFMEMORY);
    
	    /* print some stuff */
	    for(i=0; i<NumThreads; ++i) {
			OUT << "creating i= " << i << endl; FLUSHOUT;
		    ack[i] = 0;
		    worker[i] = new worker_thread_t(i);
		    w_assert1(worker[i]);
		    W_COERCE(worker[i]->fork());
			OUT << "forked i= " << i << endl; FLUSHOUT;
	    }

	    if (DumpThreads) {
			OUT << "";
		    sthread_t::dumpall("dump", _out);
			FLUSHOUT;
		}

		::usleep(2);
    
	    for(i=0; i<NumThreads; ++i) {
			OUT << "joining i= " << i << endl; FLUSHOUT;

		    W_COERCE( worker[i]->join() );
		    w_assert0(ack[i]);
		    if (DumpThreads) {
			    OUT << "Thread Done:"
				    <<  endl << *worker[i] << endl;
				FLUSHOUT;
			}
			OUT << "deleting thread i= " << i << endl; FLUSHOUT;
		    delete worker[i];
		    worker[i] = 0;
	    }

	    delete [] worker;
	    delete [] ack;
    }

	::usleep(2);

    if (PongTimes || PongGames)
	    playPong();

	::usleep(2);

    if (SleepTime) {
		OUT << "SleepTime " << SleepTime << endl; FLUSHOUT;
	    timer_thread_t* timer_thread = new timer_thread_t;
	    w_assert0(timer_thread);
	    W_COERCE( timer_thread->fork() );
		OUT << "Timer thread forked " << endl; FLUSHOUT;
	    W_COERCE( timer_thread->join() );
		OUT << "Timer thread joined " << endl; FLUSHOUT;
	    if (DumpThreads) {
		    OUT << "Timer Thread Done:" << endl
			    << *timer_thread << endl;
			FLUSHOUT;
		}
	    delete timer_thread;
    }

    if (StackOverflow) {
	    overflow_thread_t *overflow = new overflow_thread_t;
	    w_assert0(overflow);
		OUT << "forking overflow_thread_t " << endl; FLUSHOUT;
	    W_COERCE(overflow->fork());
	    W_COERCE(overflow->join());
	    delete overflow;
    }

    if (TestAssert || TestFatal) {
		if (TestErrorInThread) {
			error_thread_t *error = new error_thread_t;
			if (!error) {
				W_FATAL(fcOUTOFMEMORY);
			}
			OUT << "forking error_thread_t " << endl; FLUSHOUT;
			W_COERCE(error->fork());
			W_COERCE(error->join());
			delete error;
		}
		else {
			OUT << "Errors testing in main thread" << endl;
			FLUSHOUT;
			doErrorTests();
		}
    }

    if (verbose) {
    	sthread_t::dump_stats(cout);
	}

    return 0;
}

    

worker_thread_t::worker_thread_t(int _id)
    : work_id(_id)
{
    w_ostrstream_buf s(40);		// XXX magic number
    s << "worker[" << _id << "]" << ends;
    rename(s.c_str());
}

void worker_thread_t::run()
{
    OUT << "Hello, world from " << work_id << endl;
    if(isStackOK(__FILE__, __LINE__)) {
		_out << " stack is ok " << work_id << endl;
    }
	FLUSHOUT;

    ack[work_id] = 1;

	OUT << *this << endl;
	_out << "Good-bye, world from " << work_id << endl;
	FLUSHOUT;
}


pong_thread_t::pong_thread_t(ping_pong_t &which_game,
			     int _id,
			     wait_for_t &notify_me)
: game(which_game), id(_id), note(notify_me)
{
	w_ostrstream_buf	s(128);		// XXX magic number
	W_FORM2(s,("pong[%d]", id));
	s << ends;
	rename(s.c_str());
}


void pong_thread_t::run()
{
    int i;
    int	self = id;
	
	{ 
		CRITICAL_SECTION(cs, game.theBall);
		for(i=0; i<PongTimes; ++i){
			while(game.whoseShot != self){
				DO_PTHREAD(pthread_cond_wait(&game.paddle[self], &game.theBall));
			}
			game.whoseShot = 1-self;
			DO_PTHREAD(pthread_cond_signal(&game.paddle[1-self]));

		}
	}

    // OUT.form("pong(%#lx) id=%d done\n", (long)this, id);
    note.done();

    if (DumpInThreads) {
		OUT << *this << endl;
		FLUSHOUT;
	}
}


timer_thread_t::timer_thread_t()
    : sthread_t(t_regular, "timer")
{
}


void timer_thread_t::run()
{
	OUT << "timeThread going to sleep ";
    W_FORM2(_out,("for %d ms\n", SleepTime));

    sthread_t::sleep(SleepTime);
    _out << "timeThread awakened and die" << endl;
    if (DumpInThreads) {
		_out << *this << endl;
	}
	FLUSHOUT;
}


/* To do a decent job of stack overflow, it may be appropriate
   to recursively call run with a fixed size object, instead of
   using the gcc-specific dynamic array size hack. */

void overflow_thread_t::recurse(unsigned overflow, char *sp0, char *last)
{
	/* XXX This is supposed to be a huge stack allocation, don't
	   get rid of it */
	char	on_stack[overflowFrameSize];
#ifdef PURIFY
        memset(on_stack, '\0', overflowFrameSize);
#endif

	last = last;	/* create variable use so compilers don't complain */

	int	i;

	i = on_stack[0];	// save it from the optimizer

	/* make sure the context switch checks can catch it */
	for (i = 0; i < 10; i++)
		yield();

	ptrdiff_t	depth = (char*) sp0 - on_stack;
	if (depth < 0)
		depth = -depth;

	OUT << "Recurse to " << depth << endl << flush;
	FLUSHOUT;

	if ((unsigned)depth < overflow) {
		bool	ok = isStackFrameOK(overflowFrameSize);
		if (!ok) {
			OUT << "will_overflow says yeah!" << endl;
			FLUSHOUT;
			return;
		}

		recurse(overflow, sp0, on_stack);
	}
}


void overflow_thread_t::run()
{
#if 0 && defined(__GNUG__)
	/* XXX This is supposed to be a huge stack allocation, don't
	   get rid of it */
	char	on_stack[StackOverflow];
	int	i;

	i = on_stack[0];	// save it from the optimizer
	
	/* make sure the context switch checks can catch it */
	for (i = 0; i < 100; i++)
		yield();
#else
	char	sp0;
	recurse(StackOverflow, &sp0, &sp0);
#endif
}


void error_thread_t::run()
{
	OUT << "Error Testing Thread Running" << endl;
	FLUSHOUT;
	doErrorTests();
}


void	wait_for_t::wait()
{
	CRITICAL_SECTION(cs, _lock);
	while (have < expected) {
		DO_PTHREAD(pthread_cond_wait(&_done, &_lock));
	}
}

void	wait_for_t::done()
{
	CRITICAL_SECTION(cs, _lock);
	have++;
	if (have >= expected) {
		DO_PTHREAD(pthread_cond_signal(&_done));
	}
}


/*
  void msgPongFunc(void* arg)
  {
  int i;
  int ret;
  int me = (int)arg;
  int inBuf, outBuf;

  threadUsesFloatingPoint(0);
  outBuf = me;
  for(i=0; i<LocalMsgPongTimes; ++i){
  Tsend(&outBuf, sizeof(int), MyNodeNum, 1-me);
  inBuf = -1;
  ret = Trecv(&inBuf, sizeof(int), me);
  assert(ret == sizeof(int));
  assert(inBuf == 1-me);
  }
  }

  void remotePongFunc(void* ignoredArg)
  {
  int i;
  int ret;
  int me = MyNodeNum;
  int inBuf, outBuf;

  threadUsesFloatingPoint(0);
  outBuf = me;
  for(i=0; i<RemoteMsgPongTimes; ++i){
  Tsend(&outBuf, sizeof(int), 1-me, 2);
  inBuf = -1;
  ret = Trecv(&inBuf, sizeof(int), 2);
  assert(ret == sizeof(int));
  assert(inBuf == 1-me);
  }
  }

  */

