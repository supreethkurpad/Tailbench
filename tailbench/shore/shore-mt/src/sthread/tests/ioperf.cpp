/*<std-header orig-src='shore'>

 $Id: ioperf.cpp,v 1.49.2.9 2010/03/19 22:20:03 nhall Exp $

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

#include <w.h>
#include <w_debug.h>
#include <rand48.h>
#include <os_types.h>
#include <os_fcntl.h>
#include <cassert>
#include <ctime>

#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <iostream>
#include <w_rusage.h>
#include <w.h>
#include <sthread.h>
#include <sthread_stats.h>
#include <w_getopt.h>

#define SECTOR_SIZE        512
#define BUFSIZE                1024

char* buf = 0;
bool        use_random = false;
bool        raw_io = false;
bool        sync_io = false;

__thread rand48 generator;

class io_thread_t : public sthread_t {
public:

    io_thread_t(
        char        rw_flag,
        bool        check_flag,
        const        char* file,
        size_t        block_size,
        int        block_cnt,
        char*        buf
        );

    ~io_thread_t();
    void         rewind(int i);
    bool         error() const { return _error;} 

protected:

    virtual void run();

private:

    char        _rw_flag;
    bool        _check_flag;
    const char*        _fname;
    size_t        _block_size;
    int                 _block_cnt;
    char*        _buf;
    bool         _is_special;
    int         _nblocks;
    int         _fd;
    fileoff_t        _offset;
    bool        _error;
    int         _total_bytes;
};

io_thread_t::io_thread_t(
    char rw_flag,
    bool check_flag,
    const char* file,
    size_t block_size,
    int block_cnt,
    char* buf
    )
:
  sthread_t(t_regular, "io_thread"),
  _rw_flag(rw_flag),
  _check_flag(check_flag),
  _fname(file),
  _block_size(block_size), _block_cnt(block_cnt),
  _buf(buf),
  _is_special(false),
  _offset(0),
  _error(false),
  _total_bytes(0)
{
}

io_thread_t::~io_thread_t()
{
}

void
io_thread_t::rewind( int )
{
        DBGTHRD(<<"rewind " );

        w_rc_t                rc;
        fileoff_t        off = 0;

        // for device files, skip over the first 2 sectors so we don't trash
        // the disk label        XXX not very portable
        if (_is_special)
                off = 2 * SECTOR_SIZE;

        /* not a system call */
        rc = sthread_t::lseek(_fd, off, SEEK_AT_SET);
        if (rc.is_error()) {
            cerr << "lseek:" << endl << rc << endl;
            W_COERCE(rc);
        }

        // relative to end of sectors 1 & 2
        _offset = 0;

}

void
io_thread_t::run()
{
    stime_t timeBegin;
    stime_t timeEnd;
    const char* op_name = 0;
    w_rc_t rc;

DBGTHRD(<<"io_thread_t::run");

    // If it exists, see if it's a raw or character device
    rc = open(_fname, OPEN_RDONLY, 0, _fd);
    if (!rc.is_error()) {
        filestat_t        stbuf;
        W_COERCE(fstat(_fd, stbuf));
        _is_special = stbuf.is_device;

        if (_is_special) {
                // don't count the first two sectors.
                _nblocks = (stbuf.st_size - (2 * SECTOR_SIZE)) / _block_size;
        }
        else {
                _nblocks = ((int)stbuf.st_size) / _block_size;
        }

        if (_nblocks < _block_cnt) {
                cerr << "Warning: file " << _fname << " has only " << _nblocks
                        << " blocks of size " << _block_size << "." <<endl;
                cerr << "Will have to re-read file to read " << _block_cnt
                        << " blocks." <<  endl;
        }
        W_COERCE(close(_fd));
        _fd = -1;
    }
    else if (_rw_flag != 'r') {
        /* Don't adjust your file; we control the
           horizontal AND the vertical. */
        _nblocks = _block_cnt;
    }

DBGTHRD(<<"io_thread_t::run");

    int oflag = 0;

    // figure out what the open flags should be
    if(_rw_flag == 'r'){
        op_name = "read";
        oflag |= OPEN_RDONLY;
    }
    else if(_rw_flag == 'w') {
        op_name = "write";
        oflag |= OPEN_RDWR;
    }
    else if(_rw_flag == 'b') {
        op_name = "read/write";
        oflag |= OPEN_RDWR;
    }
    else
        cerr << "internal error at " << __LINE__ << endl;

    if (raw_io)
        oflag |= OPEN_RAW;
    if (sync_io)
        oflag |= OPEN_SYNC;

    // some systems don't like O_CREAT with device files
    if (!_is_special) {
        oflag |= OPEN_CREATE;
    }

        rc = sthread_t::open(_fname, oflag, 0666, _fd);
        if(rc.is_error()){
            cerr << "open:" << endl << rc << endl;
            W_COERCE(rc);
        }

DBGTHRD(<<"io_thread_t::run");

    // for device files, skip over the first 2 sectors so we don't trash
    // the disk label
    this->rewind(-1);

    cout << "starting: " << _block_cnt << " "
         << op_name << " ops of " << _block_size
         << " bytes each." << endl;

    timeBegin = stime_t::now(); /********************START ***************/

    int i=0;
    DBGTHRD(<<"_block_cnt= " << _block_cnt
        << " i=" << i);

    for (i = 0; i < _block_cnt; i++)  {

        DBGTHRD(
         << " i=" << i
         << " bytes=" << _total_bytes
        << " block_size=" << _block_size
        << " offset= " << _offset 
        << " left= " << (_block_cnt - i)
        );
#if 0
        cerr 
        << " i=" << i
        << " bytes=" << _total_bytes
        << " block_size=" << _block_size
        << " offset= " << _offset 
        << " left= " << (_block_cnt - i)
        << endl;
#endif

        if ((_rw_flag == 'r') || (_rw_flag == 'b') ) {
            if(i % _nblocks == (_nblocks-1) ) {
                this->rewind(i);
            }
        }
        {
            rc = sthread_t::lseek(_fd, _offset, SEEK_AT_SET);
            if (rc.is_error())  {
                    cerr << "lseek:" << endl << rc << endl;
                    _error = true;
                    W_COERCE(rc);
            }

            if(_check_flag){
                /*
                 * do checking  : put offset into buf and check
                 * it after read
                 */
                if(_block_size > sizeof(off_t)) {
                    memcpy(_buf, &_offset, sizeof(_offset));
                }
            }
        }

        if (_rw_flag == 'r' || _rw_flag == 'b') {
            rc = sthread_t::read(_fd, _buf, _block_size);
            if (rc.is_error())  {
                    cerr << "read:" << endl << rc << endl;
                    _error = true;
                    W_COERCE(rc);
            }
            
            if(_check_flag){
                off_t check;
                memcpy(&check, _buf, sizeof(_offset));
                if(check != _offset) {
                    cerr << "read check: expected " << _offset
                        << " got " << check <<endl;
                }
            }
        }
        if (_rw_flag == 'w' || _rw_flag == 'b') {
                rc = sthread_t::write(_fd, _buf, _block_size);
                if (rc.is_error()) {
                    cerr << "write:" << endl << rc << endl;
                    _error = true;
                    W_COERCE(rc);
                }
        }
        if(use_random) {
                _offset = (off_t) generator.rand() % _nblocks;
                _offset *= _block_size;
        } else {
                _offset += _block_size;
        }
        _total_bytes += _block_size;
    }

    timeEnd = stime_t::now(); /**************************** FINISH **********/

        if (_rw_flag == 'w' || _rw_flag == 'b') {
                W_COERCE( sthread_t::fsync(_fd) );
        }

    // timeEnd = stime_t::now(); 
    
    cout << "finished I/O: "
        << _block_cnt << " "
         << op_name << " ops of " << _block_size
         << " bytes each; " 
         << _total_bytes  << " bytes total. " 
         << endl;

    if (_rw_flag == 'b') {
        _block_cnt <<= 1; // times 2
    }

    sinterval_t delta(timeEnd - timeBegin);

    cout << "Total time: " << delta << endl;

    double f = (_block_size*_block_cnt) / (double)((stime_t)delta);
    if (f > 1024*1024)
            cout << "MB/sec: " << f / (1024*1024);
    else if (f > 1024)
        cout << "KB/sec: " << f / 1024;
    else
        cout << "bytes/sec: " << f;
    cout << endl;

    W_COERCE( sthread_t::close(_fd) );
DBGTHRD(<<"io_thread_t::run ending" );
}

int
main(int argc, char** argv)
{
    FUNC(main);
    DBGTHRD(<<"this is main");

    size_t         block_size = 8192;
    int          block_cnt = 1024;
    bool        check_flag = false;
    int                errors = 0;
    const char* fname = 0;
    char        rw_flag = 'r';

    int        c;
    while ((c = getopt(argc, argv, "dlks:n:crwbRZS")) != EOF) {
            switch (c) {
        case 's':
                block_size = atoi(optarg);
                break;
        case 'n':
                block_cnt = atoi(optarg);
                break;
        case 'c':
                check_flag = true;
                break;
        case 'R':
                use_random = true;
                break;
        case 'r':
        case 'w':
        case 'b':
                rw_flag = c;
                break;
        case 'Z':
                raw_io = true;
                break;
        case 'S':
                sync_io = true;
                break;
        default:
                errors++;
                break;
        }
    }
    if (optind < argc)
        fname = argv[optind];
    else                /* no filename */
            errors++;

    if (errors) {
        cerr << "Usage: " << argv[0]
                << " [-s block_size]"
                << " [-n block_count]"
                << " [-R random]"
                << " [-c check_flag]"
                << " [-r read_only] [ -w write_only] [-b read_and_write]"
                << " file"
                << endl;
        return 1;
    }

    if (use_random)
                generator.seed(block_cnt);

    w_rc_t        e;
#ifdef HAVE_HUGETLBFS
    e = sthread_t::set_hugetlbfs_path(HUGETLBFS_PATH);
    if (e.is_error()) W_COERCE(e);
#endif
    char*         buf = 0;
    e = sthread_t::set_bufsize(block_size, buf);
    if (e.is_error()) W_COERCE(e);


    DBGTHRD(<<"set_bufsize done");


    io_thread_t thr(rw_flag, check_flag, fname, block_size, block_cnt, buf);

    DBGTHRD(<<"before fork");
    W_COERCE( thr.fork() );
#if W_DEBUG_LEVEL > 2
    cerr << "awaiting io thread ... " << endl;
#endif
    W_COERCE( thr.join() );
    DBGTHRD(<<"after thr.wait");

    if (!thr.error()) {
		sthread_t::dump_stats(cout);
    }

    W_COERCE(sthread_t::set_bufsize(0, buf));

    return 0;
}

