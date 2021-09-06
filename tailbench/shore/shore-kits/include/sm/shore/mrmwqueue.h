/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/** @file:  mrmwqueue.h
 *
 *  @brief: A multiple-reader, multiple-writer queue.
 *
 *  Queue size is unbounded and the (shore_worker) reader initially spins while 
 *  waiting for new elements to arrive and then sleeps on a condex.
 *
 *  Based on the single-reader, multiple-writer queue included with the original
 *  distribution
 *  @author: Harshad Kasture (harshad@mit.edu)
 */

#ifndef __SHORE_MRMW_QUEUE_H
#define __SHORE_MRMW_QUEUE_H

#include <sthread.h>
#include <vector>

#include "util.h"
#include "sm/shore/common.h"
#include "sm/shore/shore_worker.h"


ENTER_NAMESPACE(shore);


template<class Action>
struct mrmwqueue 
{
    typedef typename PooledVec<Action*>::Type ActionVec;
    typedef std::vector<base_worker_t*> Waiters;
    
    guard<ActionVec> _queue;
    Waiters _waiters;
    mcs_lock      _lock;

    eWorkingState _my_ws;

    int _loops; // how many loops (spins) it will do before going to sleep (1=sleep immediately)
    int _thres; // threshold value before waking up

    mrmwqueue(Pool* actionPtrPool) 
        : _my_ws(WS_UNDEF), 
          _loops(0), _thres(0)
    { 
        assert (actionPtrPool);
        _queue = new ActionVec(actionPtrPool);
        _waiters.resize(0);
    }
    ~mrmwqueue() { }


    // sets the pointer of the queue to the controls of a specific worker thread
    void setqueue(eWorkingState aws, const int& loops, const int& thres) 
    {
        CRITICAL_SECTION(q_cs, _lock);
        _my_ws = aws;
        _loops = loops;
        _thres = thres;
    }

    Action* pop(base_worker_t* worker) {
        int loopcnt = 0;
        Action* action = NULL;
        while (true) {
            bool sleep = false;

            {
                CRITICAL_SECTION(cs, _lock);
                if (!_queue->empty()) {
                    action = _queue->front();
                    _queue->erase(_queue->begin());
                    break;
                } else if (++loopcnt > _loops) {
                    _waiters.push_back(worker);
                    loopcnt = 0;
                    sleep = true;
                }
            }

            // condex sleep/wakeup code already accounts for wakeups that
            // happen before we can sleep: it counts the total number of sleep
            // and wakup attempts, and only sleeps if #sleeps > #wakeups. So
            // there is no race here
            if (sleep) {
                int slept = worker->condex_sleep();
                {
                    CRITICAL_SECTION(cs, _lock);

                    // This is horribly inefficient
                    int sleepPos = -1;
                    for (int i = 0; i < _waiters.size(); i++) {
                        if (_waiters[i] == worker) {
                            sleepPos = i;
                            break;
                        }
                    }

                    if (sleepPos != -1) {
                        _waiters.erase(_waiters.begin()+sleepPos);
                    }
                }
            }

            uint_t wc = worker->get_control();
            if (wc != WC_ACTIVE) {
                worker->set_ws(WS_FINISHED);
                action = NULL;
                break;
            }

            if (!worker->can_continue(_my_ws)) {
                action = NULL;
                break;
            }
        }

        return action;
    }

    
    inline void push(Action* a, const bool bWake) {
        int queue_sz;

        // push action
        {
            CRITICAL_SECTION(cs, _lock);
            _queue->push_back(a);
            queue_sz = _queue->size();

            if ((queue_sz >= _thres) || bWake) {
                int woken = 0;
                for (Waiters::iterator it = _waiters.begin(); \
                        it != _waiters.end(); it++) {
                    (*it)->set_ws(_my_ws);
                    if (++woken == queue_sz) break;
                }
            }
        }
    }

}; // EOF: struct mrmwqueue


EXIT_NAMESPACE(shore);

#endif /** __SHORE_MRMW_QUEUE_H */

