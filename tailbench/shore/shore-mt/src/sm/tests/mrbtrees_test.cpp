/*<std-header orig-src='shore'>

 $Id: file_scan_many.cpp,v 1.3 2010/06/08 22:28:15 nhall Exp $

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
 * This program is a test of mrbtrees and different plp designs
 * Combination of create_rec and file_scan_many is taken as a template
 */

#include <w_stream.h>
#include <sys/types.h>
#include <cassert>
// This include brings in all header files needed for writing a VAs 
#include "sm_vas.h"
#include "w_getopt.h"
#include "stopwatch.h"
#include <vector>

ss_m* ssm = 0;

// shorten error code type name
typedef w_rc_t rc_t;

// this is implemented in options.cpp
w_rc_t init_config_options(option_group_t& options,
                        const char* prog_type,
                        int& argc, char** argv);

// keeps the file information - stored persistently
struct file_info_t {
  stid_t      fid;
  rid_t       first_rid;
  rid_t       last_rid;
  int         num_rec;
  int         rec_size;
  bool        append_only;
  static const char* key;
};
const char* file_info_t::key = "SCANFILE";

ostream & operator << (ostream &o, const file_info_t &info)
{
  o << "key " << info.key
    << " fid " << info.fid
    << " first_rid " << info.first_rid
    << " last_rid " << info.last_rid
    << " num_rec " << info.num_rec
    << " rec_size " << info.rec_size 
    << " append-only " << info.append_only ;
  return o;
}

typedef        smlevel_0::smksize_t        smksize_t;

void usage(option_group_t& options)
{
  // "hit:n:s:r:dpo:"
  cerr << "Usage: server [-h] [-i] [-t] [-n] [-s] [-r] [-d] [-p] [-o] [options]" << endl;
  cerr << "       -i initialize device/volume and create file - default false" << endl;
  cerr << "       -h print this message" << endl;
  cerr << "       -t <which test - see below for options> - default 0" << endl;
  cerr << "       -n <#partitions> - for test 6 - default 10" << endl;
  cerr << "       -s <#scans> - default 0" << endl;
  cerr << "       -r <record size> - default 100" << endl;
  cerr << "       -d ignore locks - default false" << endl;
  cerr << "       -p ignore latches - default false" << endl;
  cerr << "       -o <which plp design> - default plp-regular/mrbtnorm" << endl;

  cerr << "        \tTESTS" << endl;
  cerr << "        \t0) MRBtree with single partition!" << endl;
  cerr << "        \t1) Adding inital partitions to MRBtree!" << endl;
  cerr << "        \t2) Create a partition after the some assocs are created in MRBtree. Then add new assocs" << endl;
  cerr << "        \t3) Start with a single partition. Insert records. Then split and merge trees of the same height." << endl;
  cerr << "        \t4) Merge partitions when root1.level > root2.level in MRBtree." << endl;
  cerr << "        \t5) Merge partitions when root1.level < root2.level in MRBtree." << endl;
  cerr << "        \t6) Make equal initial partitions. Then insert the records." << endl;
  
  cerr << "Valid options are: " << endl;
  options.print_usage(true, cerr);
}

// for mrbtree insert
struct el_filler_utest : public el_filler {
    int j;
    stid_t _fid;
    smsize_t _rec_size;
    rid_t rid;
    rc_t fill_el(vec_t& el, const lpid_t& leaf);
};

// to insert records for plp-part and plp-leaf
rc_t  el_filler_utest::fill_el(vec_t& el, const lpid_t& leaf)
{
  cout << "Creating record " << j << endl;

  char* dummy = new char[_rec_size];
  memset(dummy, '\0', _rec_size);
  vec_t data(dummy, _rec_size); 
  {
    w_ostrstream o(dummy, _rec_size);
    o << j << ends;
    w_assert1(o.c_str() == dummy);
  }
  
  // header contains record #
  int i = j;
  const vec_t hdr(&i, sizeof(i));
  
  rc_t rc = ss_m::find_page_and_create_mrbt_rec(_fid, leaf, hdr, _rec_size, data, rid, false, false);
  cout << " rid: " << rid << endl;

  el.put((char*)(&rid), sizeof(rid_t));
  
  return rc;
}

// create an smthread based class for all sm-related work
class smthread_main_t : public smthread_t {
public:
  int        _argc;
  char        **_argv;
  
  const char *_device_name;
  smsize_t    _quota;
  int         _num_rec;  // number of records to insert (it inserts one extra record for test 2)
  smsize_t    _rec_size; // size of each record
  lvid_t      _lvid;  
  rid_t       _first_rid; // rid of record with key 0
  rid_t       _last_rid;  // rid of record with key _num_rec (for test 2)
  stid_t      _fid; // id of the file the records are created
  stid_t      _index_id; // id of the btree index
  bool        _initialize_device; // indicates whether we should initialize the device and create a new file
  option_group_t* _options;
  vid_t       _vid;
  int         _test_no; // test to execute
  int         _num_parts; // how many partitions to create (for test 6 - #threads created to work on each partition)
  int         _scan_file; // the number of times to scan the file and get an average scan time
  bool        _bIgnoreLocks; // indicates whether to ignore locks or not
  bool        _bIgnoreLatches; // indicates whether to ignore latches or not 
  int         _design_no; // 1-regular/2-part/3-leaf (which mrbt design)
  
  int         retval;
    
  smthread_main_t(int ac, char **av, const char* name="smthread_main_t") 
    : smthread_t(t_regular, name),
      _argc(ac), _argv(av), 
      _device_name(NULL),
      _quota(0),
      _num_rec(1000),
      _rec_size(100),
      _initialize_device(false),
      _options(NULL),
      _vid(1),
      _test_no(0),
      _num_parts(10),
      _scan_file(0),
      _bIgnoreLocks(false),
      _bIgnoreLatches(false),
      _design_no(1),
      retval(0) { }

  ~smthread_main_t()  { if(_options) delete _options; }
    
  void run();
    
  // helpers for run()
  w_rc_t handle_options();
  w_rc_t find_file_info();
  w_rc_t create_the_file();
  w_rc_t create_the_index();

  w_rc_t mr_index_test0();
  w_rc_t mr_index_test1();
  w_rc_t mr_index_test2();
  w_rc_t mr_index_test3();
  w_rc_t mr_index_test4();
  w_rc_t mr_index_test5();
  w_rc_t mr_index_test6();
  w_rc_t mr_index_test7();

  w_rc_t print_the_index();
  w_rc_t static print_updated_rids(vector<rid_t>& old_rids, vector<rid_t>& new_rids);

  w_rc_t do_work();
  w_rc_t do_init();
  w_rc_t no_init();

};

typedef smthread_main_t *  threadptr;

// thread to fill the file with a key-range of records
class smthread_creator_t : public smthread_main_t 
{
  int _start_key;
  int _end_key;
public:
  smthread_creator_t(int num_rec, int rec_size, bool bIgnoreLocks, bool bIgnoreLatches,
		     int design_no, int start_key, int end_key, stid_t stid, int test_no)
    : smthread_main_t(0,0, "smthread_creator_t"),
      _start_key(start_key), _end_key(end_key)
  {   
    _num_rec = num_rec;
    _rec_size = rec_size;
    _bIgnoreLocks = bIgnoreLocks;
    _bIgnoreLatches = bIgnoreLatches;
    _design_no = design_no;
    _index_id = stid;
    _test_no = test_no;
  }

  ~smthread_creator_t() {}

  // helper functions for run

  // without bulk loading
  w_rc_t fill_the_file_regular();
  w_rc_t fill_the_file_non_regular();

  // with bulk loading
  w_rc_t fill_the_file_regular_bl();
  w_rc_t fill_the_file_non_regular_bl();
  
  void run();
};

// to scan index or heap files with multiple threads
// pin: currently just one thread is used because we want to see how long it takes for one thread
//      if needed later can be improved
class smthread_scanner_t : public smthread_main_t 
{
  int _start_key;
  int _end_key;  
public:
  smthread_scanner_t(stid_t stid, int scan_file,
		     bool bIgnoreLatches,
		     int start_key = -1, int end_key = -1)
    : smthread_main_t(0,0, "smthread_scanner_t"),
      _start_key(start_key), _end_key(end_key)
  {
    _index_id = stid;
    _scan_file = scan_file;
    _bIgnoreLatches = bIgnoreLatches;
  }

  ~smthread_scanner_t()  { }

  // helper functions for run
  w_rc_t scan_the_file();
  w_rc_t scan_the_index();

  void run();
};

void smthread_creator_t::run()
{ 
  rc_t rc = find_file_info();

  if(!rc.is_error()) { 
    if(_design_no == 0) { 
      if(_test_no == 7) { // bulk loading test
	rc = fill_the_file_regular_bl();
      } else {
	// TODO: implement non-bulkloading option here
	cout << "This test is not supported for this design option yet!" << endl;
	rc = RC(fcASSERT);
      }
    }
    else if(_design_no == 1) {
      rc = fill_the_file_regular(); // mrbt regular
    } else {
      rc = fill_the_file_non_regular(); // mrbt part&leaf
    }
  }
    
  if (rc.is_error()) {
    cerr << "Could not create the records due to: " << endl;
    cerr << rc << endl;
    rc = RCOK;   // force deletion of w_error_t info hanging off rc
    // otherwise a leak for w_error_t will be reported
    if(rc.is_error()) 
      W_COERCE(rc); // avoid error not checked.
  }

  return;
}

void smthread_scanner_t::run()
{
  // TODO: you can seperate index and file scan and make index scan for a range
  rc_t rc = find_file_info();

  if(!rc.is_error()) {
    // scan the heap file
    double heap_scan_current = 0;
    double heap_scan_total = 0;
    for(int i=0; !rc.is_error() && i<_scan_file; i++) {
      stopwatch_t timer;
      rc = scan_the_file();
      heap_scan_current = timer.time();
      heap_scan_total += heap_scan_current;
    }
    cout << "Avg file scan: " << (heap_scan_total/_scan_file) << endl;
    
    if (rc.is_error()) {
      cerr << "Error in file scan due to: " << endl;
      cerr << rc << endl;
      rc = RCOK;   // force deletion of w_error_t info hanging off rc
      // otherwise a leak for w_error_t will be reported
      if(rc.is_error()) 
	W_COERCE(rc); // avoid error not checked.
    }
    
    // scan the index file
    double index_scan_current = 0;
    double index_scan_total = 0;
    for(int i=0; !rc.is_error() && i<_scan_file; i++) {
      stopwatch_t timer;
      rc = scan_the_index();
      index_scan_current = timer.time();
      index_scan_total += index_scan_current;
    }
    cout << "Avg index scan: " << (index_scan_total/_scan_file) << endl;
    
    if (rc.is_error()) {
      cerr << "Error in index scan due to: " << endl;
      cerr << rc << endl;
      rc = RCOK;   // force deletion of w_error_t info hanging off rc
      // otherwise a leak for w_error_t will be reported
      if(rc.is_error()) 
	W_COERCE(rc); // avoid error not checked.
    }
  } else {
    cerr << "Could not perform scan due to: " << endl;
    cerr << rc << endl;
    rc = RCOK;   // force deletion of w_error_t info hanging off rc
    // otherwise a leak for w_error_t will be reported
    if(rc.is_error()) 
      W_COERCE(rc); // avoid error not checked.
  }

  return;
}

// looks up file info in the root index
rc_t smthread_main_t::find_file_info()
{
    file_info_t  info;
    W_DO(ssm->begin_xct());

    bool        found;
    stid_t      _root_iid;
    W_DO(ss_m::vol_root_index(_vid, _root_iid));

    smsize_t    info_len = sizeof(info);
    const vec_t key_vec_tmp(file_info_t::key, strlen(file_info_t::key));
    W_DO(ss_m::find_assoc(_root_iid,
                          key_vec_tmp,
                          &info, info_len, found));
    if (!found) {
        cerr << "No file information found" <<endl;
        return RC(fcASSERT);
    } else {
       cerr << " found assoc "
                << file_info_t::key << " --> " << info << endl;
    }

    W_DO(ssm->commit_xct());

    _first_rid = info.first_rid;
    _last_rid = info.last_rid;
    _fid = info.fid;
    _rec_size = info.rec_size;
    _num_rec = info.num_rec;
    return RCOK;
}

// creates an empty heap file
rc_t smthread_main_t::create_the_file() 
{
    file_info_t info;  // will be made persistent in the
    // volume root index.

    // create the file
    cout << "Creating a file with " << _num_rec 
        << " records of size " << _rec_size << endl;
    W_DO(ssm->begin_xct());

    // Create the file. Stuff its fid in the persistent file_info
    if(_design_no < 2) {
      W_DO(ssm->create_file(_vid, info.fid, smlevel_3::t_regular));
    } else {
      W_DO(ssm->create_mrbt_file(_vid, info.fid, smlevel_3::t_regular));
    }

    _rec_size -= align(sizeof(int));

    info.num_rec = _num_rec;
    info.rec_size = _rec_size;

    // record file info in the root index : this stores some
    // attributes of the file in general
    stid_t      _root_iid;
    W_DO(ss_m::vol_root_index(_vid, _root_iid));

    const vec_t key_vec_tmp(file_info_t::key, strlen(file_info_t::key));
    const vec_t info_vec_tmp(&info, sizeof(info));
    W_DO(ss_m::create_assoc(_root_iid,
                            key_vec_tmp,
                            info_vec_tmp));
    cerr << "Creating assoc "
            << file_info_t::key << " --> " << info << endl;
    W_DO(ssm->commit_xct());
    return RCOK;
}

// creates the index based on the given design option
rc_t smthread_main_t::create_the_index() {
  if(_design_no == 0) { 
    W_DO(ssm->create_index(_vid, smlevel_0::t_btree, smlevel_3::t_regular, 
    			   "i4", smlevel_0::t_cc_kvl, _index_id));
  }
  else if(_design_no == 1) {
    W_DO(ssm->create_mr_index(_vid, smlevel_0::t_mrbtree, smlevel_3::t_regular, 
			      "i4", smlevel_0::t_cc_kvl, _index_id, false));
  } else if(_design_no == 2) {
    W_DO(ssm->create_mr_index(_vid, smlevel_0::t_mrbtree_p, smlevel_3::t_regular, 
			      "i4", smlevel_0::t_cc_kvl, _index_id, false));
  } else if(_design_no == 3) {
    W_DO(ssm->create_mr_index(_vid, smlevel_0::t_mrbtree_l, smlevel_3::t_regular, 
			      "i4", smlevel_0::t_cc_kvl, _index_id, false));
  } else {
    cout << "Wrong design option!" << endl;
    return RC(fcASSERT);
  }
  return RCOK;
}

// creates records and their associations for a regular mrbt design
rc_t smthread_creator_t::fill_the_file_regular() 
{
  int num_inserted = 1;
  
  W_DO(ssm->begin_xct());
  
  char* dummy = new char[_rec_size];
  memset(dummy, '\0', _rec_size);
  vec_t data(dummy, _rec_size);
  rid_t rid;
  for(int j=_start_key; j <= _end_key; j++, num_inserted++) {
    if( j == _end_key && (_end_key != _num_rec || _test_no != 2)) {
      continue; // for test 2 we want to insert one more record and
                // it is the reason for all the weird continues here TODO: try to make this better
    }
    {
      w_ostrstream o(dummy, _rec_size);
      o << j << ends;
      w_assert1(o.c_str() == dummy);
    }
    // header contains record #
    int i = j;
    const vec_t hdr(&i, sizeof(i));
    W_COERCE(ssm->create_rec(_fid, hdr, _rec_size, data, rid, _bIgnoreLocks));
    cout << "Created rec " << j << " rid:" << rid << endl;
    if (j == 0) {
      _first_rid = rid;
      if(_test_no == 2) {
	continue;
      }
    } else if(j == _num_rec) {
      _last_rid = rid;
      continue;
    }
    el_filler_utest eg;
    vec_t       key(&i, sizeof(i));
    vec_t el((char*)(&rid), sizeof(rid_t));
    eg._el.put(el);
    W_DO(ssm->create_mr_assoc(_index_id, key, eg, _bIgnoreLocks, _bIgnoreLatches));
    cout << "Created assoc " << j << endl;
  
    // if we want to insert a lot of records then we run out of log space
    // to avoid it, we should flush the log after inserting some number of records
    if(num_inserted >= 20000) {
      W_DO(ssm->commit_xct());
      num_inserted = 0;
      W_DO(ssm->begin_xct());
    }
  }
  cout << "Created all. First rid " << _first_rid << " Last rid " << _last_rid << endl;
  delete [] dummy;
  
  W_DO(ssm->commit_xct());
  return RCOK;
}

// creates records and their associations for mrbt part&leaf design
rc_t smthread_creator_t::fill_the_file_non_regular()
{
  int num_inserted = 0;
  
  cout << "creating assocs in index " << _index_id << "  from file " << _fid << endl;
  cout << "Key boundaries: [" << _start_key << ", " << _end_key << "]" << endl;

  W_DO(ssm->begin_xct());

  int i = _start_key;
    
  do {
    if(i == 0 && _test_no == 2) {
      i++; // for test 2 we want to insert 0 later
      continue;
    }
    el_filler_utest eg;
    eg._fid = _fid;
    eg._rec_size = _rec_size;
    eg._el_size = sizeof(rid_t);
    int j = i;
    vec_t       key(&j, sizeof(j));
    eg.j = i;
    W_DO(ssm->create_mr_assoc(_index_id, key, eg, _bIgnoreLocks, _bIgnoreLatches, &print_updated_rids));
    i++;

    // if we want to insert a lot of records then we run out of log space
    // to avoid it, we should flush the log after inserting some number of records
    num_inserted++;
    if(num_inserted >= 20000) {
      W_DO(ssm->commit_xct());
      num_inserted = 0;
      W_DO(ssm->begin_xct());
    }
    
  } while (i < _end_key);

  W_DO(ssm->commit_xct());

  return RCOK;
}

// TODO: implement something more general like specified in the comments
// creates records in a regular file to be later used in bulk loading ( can be used in the baseline system
// with or without mrbtrees and plp-regular )
// it's better not to do this record creation in parallel since we need a sorted order for bulk loading
// if records are not in sorted order then we should sort them but i'm leaving this as a TODO now
// WARNING: right now just testing simple bulk loading
// create the recs with one thread then perform bulk loading
rc_t smthread_creator_t::fill_the_file_regular_bl() 
{
  int num_inserted = 1;
  
  W_DO(ssm->begin_xct());
  
  char* dummy = new char[_rec_size];
  memset(dummy, '\0', _rec_size);
  vec_t data(dummy, _rec_size);
  rid_t rid;
  for(int j=_start_key; j <= _end_key; j++, num_inserted++) {
    if( j == _end_key && (_end_key != _num_rec || _test_no != 2)) {
      continue; // for test 2 we want to insert one more record and
                // it is the reason for all the weird continues here TODO: try to make this better
    }
    {
      w_ostrstream o(dummy, _rec_size);
      o << j << ends;
      w_assert1(o.c_str() == dummy);
    }
    // header contains record #
    int i = j;
    const vec_t hdr(&i, sizeof(i));
    W_COERCE(ssm->create_rec(_fid, hdr, _rec_size, data, rid, _bIgnoreLocks));
    cout << "Created rec " << j << " rid:" << rid << endl;
    if (j == 0) {
      _first_rid = rid;
      if(_test_no == 2) {
	continue;
      }
    } else if(j == _num_rec) {
      _last_rid = rid;
      continue;
    }
  
    // if we want to insert a lot of records then we run out of log space
    // to avoid it, we should flush the log after inserting some number of records
    if(num_inserted >= 20000) {
      W_DO(ssm->commit_xct());
      num_inserted = 0;
      W_DO(ssm->begin_xct());
    }
  }
  cout << "Created all. First rid " << _first_rid << " Last rid " << _last_rid << endl;
  delete [] dummy;
  
  W_DO(ssm->commit_xct());

  // filled the file, now perform bulk-loading
  W_DO(ssm->begin_xct());
  
  sm_du_stats_t        bl_stats;
  W_DO(ssm->bulkld_index(_index_id, _fid, bl_stats));
  
  W_DO(ssm->commit_xct());
  
  return RCOK;
}

// for using bulk loading with plp-part&leaf
rc_t smthread_creator_t::fill_the_file_non_regular_bl()
{
  // TODO: implement
  return RCOK;
}

// prints the old&new rids of the moved records, used instead of RELOCATE_RECS callback
rc_t smthread_main_t::print_updated_rids(vector<rid_t>& old_rids, vector<rid_t>& new_rids)
{
  cout << endl;
  cout << "Old rids\tNew rids" << endl;
  for(uint i=0; i<old_rids.size(); i++) {
    cout << old_rids[i] << "\t" << new_rids[i] << endl;
  }
  return RCOK;
}

// --------------------- TESTS ----------------------------------------
// the explanation for the tests are given either in the comments or as cout statements

rc_t smthread_main_t::mr_index_test0()
{
    cout << endl;
    cout << " ------- TEST0 -------" << endl;
    cout << "To test MRBtree with single partition!" << endl;
    cout << endl;
    
    cout << "Creating multi rooted btree index." << endl;
    W_DO(ssm->begin_xct());
    W_DO(create_the_index());
    W_DO(ssm->commit_xct());

    // create the records and their assocs
    threadptr creator_thread = new smthread_creator_t(_num_rec, _rec_size,
						      _bIgnoreLocks, _bIgnoreLatches,
						      _design_no, 0, _num_rec, _index_id, 0);
    creator_thread->fork();
    creator_thread->join();
    delete creator_thread;

    W_DO(print_the_index());
    
    return RCOK;
}

rc_t smthread_main_t::mr_index_test1()
{
    cout << endl;
    cout << " ------- TEST1 -------" << endl;
    cout << "To test adding inital partitions to MRBtree!" << endl;
    cout << endl;
    
    cout << "Creating multi rooted btree index." << endl;
    W_DO(ssm->begin_xct());
    W_DO(create_the_index());
    W_DO(ssm->commit_xct());

    W_DO(ssm->begin_xct());
    int key1 = (int) (0.7 * _num_rec);
    vec_t key_vec1(&key1, sizeof(key1));
    cout << "add_partition_init: stid = " << _index_id << ", key = " << key1 << endl;
    W_DO(ssm->add_partition_init(_index_id, key_vec1, false));    
    W_DO(ssm->commit_xct());

    W_DO(ssm->begin_xct());
    int key2 = (int) (0.5 * _num_rec);
    vec_t key_vec2(&key2, sizeof(key2));
    cout << "add_partition_init: stid = " << _index_id << ", key = " << key2 << endl;
    W_DO(ssm->add_partition_init(_index_id, key_vec2, false));
    W_DO(ssm->commit_xct());    


    // create the records and their assocs
    threadptr creator_thread1 = new smthread_creator_t(_num_rec, _rec_size,
						       _bIgnoreLocks, _bIgnoreLatches,
						       _design_no, 0, key2, _index_id, 1);
    threadptr creator_thread2 = new smthread_creator_t(_num_rec, _rec_size,
						       _bIgnoreLocks, _bIgnoreLatches,
						       _design_no, key2, key1, _index_id, 1);
    threadptr creator_thread3 = new smthread_creator_t(_num_rec, _rec_size,
						       _bIgnoreLocks, _bIgnoreLatches,
						       _design_no, key1, _num_rec, _index_id, 1);
    creator_thread1->fork();
    creator_thread2->fork();
    creator_thread3->fork();
    creator_thread1->join();
    creator_thread2->join();
    creator_thread3->join();
    delete creator_thread1;
    delete creator_thread2;
    delete creator_thread3;

    W_DO(print_the_index());
    
    return RCOK;
}

rc_t smthread_main_t::mr_index_test2()
{
    cout << endl;
    cout << " ------- TEST2 -------" << endl;
    cout << "Create a partition after the some assocs are created in MRBtree" << endl;
    cout << "Then add new assocs" << endl;
    cout << endl;

    cout << "Creating multi rooted btree index." << endl;
    W_DO(ssm->begin_xct());
    W_DO(create_the_index());
    W_DO(ssm->commit_xct());

    // create the records and their assocs
    threadptr creator_thread = new smthread_creator_t(_num_rec, _rec_size,
						      _bIgnoreLocks, _bIgnoreLatches,
						      _design_no, 0, _num_rec, _index_id, 2);
    creator_thread->fork();
    creator_thread->join();
    delete creator_thread;

    W_DO(print_the_index());

    int key = (int) (0.7 * _num_rec);
    vec_t key_vec(&key, sizeof(key));
    cout << "add_partition: stid = " << _index_id << ", key = " << key << endl;
    W_DO(ssm->add_partition(_index_id, key_vec, _bIgnoreLatches, &print_updated_rids));
    
    W_DO(print_the_index());

    // create two more recs&assocs 
    W_DO(ssm->begin_xct());
    cout << "ssm->create_mr_assoc" << endl;
    el_filler_utest eg;
    eg._fid = _fid;
    eg._rec_size = _rec_size;
    int new_key = _num_rec;
    eg.j = new_key;
    vec_t new_key_vec((char*)(&new_key), sizeof(new_key));
    cout << "Record key "  << new_key << endl;
    cout << "key size " << new_key_vec.size() << endl;    
    eg._el_size = sizeof(rid_t);
    if(_design_no == 1) {
      eg._el.set((char*)(&_last_rid), sizeof(rid_t));
    }
    W_DO(ssm->create_mr_assoc(_index_id, new_key_vec, eg, _bIgnoreLocks, _bIgnoreLatches, &print_updated_rids));
    cout << "ssm->create_mr_assoc" << endl;
    int new_key2 = 0;
    eg.j = new_key;
    vec_t new_key_vec2((char*)(&new_key2), sizeof(new_key2));
    cout << "Record key "  << new_key2 << endl;
    cout << "key size " << new_key_vec2.size() << endl;
    if(_design_no == 1) {
      eg._el.set((char*)(&_first_rid), sizeof(rid_t));
    }
    W_DO(ssm->create_mr_assoc(_index_id, new_key_vec2, eg, _bIgnoreLocks, _bIgnoreLatches, &print_updated_rids));
    W_DO(ssm->commit_xct());

    W_DO(print_the_index());

    return RCOK;
}

rc_t smthread_main_t::mr_index_test3()
{
    cout << endl;
    cout << " ------- TEST3 -------" << endl;
    cout << "Tests split/merge operations" << endl;
    cout << endl;
    
    cout << "Creating multi rooted btree index." << endl;
    W_DO(ssm->begin_xct());
    W_DO(create_the_index());
    W_DO(ssm->commit_xct());

    // create the records and their assocs
    threadptr creator_thread = new smthread_creator_t(_num_rec, _rec_size,
						      _bIgnoreLocks, _bIgnoreLatches,
						      _design_no, 0, _num_rec, _index_id, 3);
    creator_thread->fork();
    creator_thread->join();
    delete creator_thread;

    W_DO(print_the_index());
    
    int key1 = (int) (0.2 * _num_rec);
    vec_t key1_vec(&key1, sizeof(key1));
    cout << "add_partition: stid = " << _index_id << ", key = " << key1 << endl;
    W_DO(ssm->add_partition(_index_id, key1_vec, _bIgnoreLatches, &print_updated_rids));

    W_DO(print_the_index());

    int key2 = (int) (0.4 * _num_rec);
    vec_t key2_vec(&key2, sizeof(key2));
    cout << "add_partition: stid = " << _index_id << ", key = " << key2 << endl;
    W_DO(ssm->add_partition(_index_id, key2_vec, _bIgnoreLatches, &print_updated_rids));

    W_DO(print_the_index());

    int key3 = (int) (0.6 * _num_rec);
    vec_t key3_vec(&key3, sizeof(key3));
    cout << "add_partition: stid = " << _index_id << ", key = " << key3 << endl;
    W_DO(ssm->add_partition(_index_id, key3_vec, _bIgnoreLatches, &print_updated_rids));

    W_DO(print_the_index());

    int key4 = (int) (0.8 * _num_rec);
    vec_t key4_vec(&key4, sizeof(key4));
    cout << "add_partition: stid = " << _index_id << ", key = " << key4 << endl;
    W_DO(ssm->add_partition(_index_id, key4_vec, _bIgnoreLatches, &print_updated_rids));

    W_DO(print_the_index());

    cout << "delete_partition: stid = " << _index_id << ", key = " << key1 << endl;
    W_DO(ssm->delete_partition(_index_id, key1_vec, _bIgnoreLatches));

    W_DO(print_the_index());

    int key5 = (int) (0.7 * _num_rec);
    vec_t key5_vec(&key5, sizeof(key5));
    cout << "delete_partition: stid = " << _index_id << ", key = " << key5 << endl;
    W_DO(ssm->delete_partition(_index_id, key5_vec, _bIgnoreLatches));

    W_DO(print_the_index());

    int key6 = (int) (0.5 * _num_rec);
    vec_t key6_vec(&key6, sizeof(key6));
    cout << "delete_partition: stid = " << _index_id << ", key = " << key6 << endl;
    W_DO(ssm->delete_partition(_index_id, key6_vec, _bIgnoreLatches));

    W_DO(print_the_index());

    return RCOK;
}

rc_t smthread_main_t::mr_index_test4()
{
    cout << endl;
    cout << " ------- TEST4 -------" << endl;
    cout << " 1. Add initial partitions " << endl;
    cout << "To test merge partitions when root1,level > root2.level in MRBtree!" << endl;
    cout << endl;

    // create the records and their assocs as in test 1 to create partitions with different tree levels
    W_DO(mr_index_test1());

    int key = (int) (0.6 * _num_rec);
    vec_t key_vec(&key, sizeof(key));
    cout << "delete_partition: stid = " << _index_id << ", key = " << key << endl;
    W_DO(ssm->delete_partition(_index_id, key_vec, _bIgnoreLatches));

    W_DO(print_the_index());

    return RCOK;
}

rc_t smthread_main_t::mr_index_test5()
{
    cout << endl;
    cout << " ------- TEST5 -------" << endl;
    cout << " 1. Add initial partitions " << endl;
    cout << "To test merge partitions when root1,level < root2.level in MRBtree!" << endl;
    cout << endl;
    
    // create the records and their assocs as in test 1 to create partitions with different tree levels
    W_DO(mr_index_test1());

    int key = (int) (0.8 * _num_rec);
    vec_t key_vec(&key, sizeof(key));
    cout << "delete_partition: stid = " << _index_id << ", key = " << key << endl;
    W_DO(ssm->delete_partition(_index_id, key_vec, _bIgnoreLatches));

    W_DO(print_the_index());
    
    return RCOK;
}

rc_t smthread_main_t::mr_index_test6()
{
    cout << endl;
    cout << " ------- TEST6 -------" << endl;
    cout << "Test make equal partitions!" << endl;
    cout << endl;
    
    key_ranges_map ranges;
    int min_key = 0;
    vec_t min_key_vec((char*)(&min_key), sizeof(min_key));
    int max_key = _num_rec;
    vec_t max_key_vec((char*)(&max_key), sizeof(max_key));

    cout << "Creating multi rooted btree index." << endl;
    W_DO(ssm->begin_xct());
    W_DO(create_the_index());
    W_DO(ssm->commit_xct());

    cout << "Make equal initial partitions." << endl;
    cout << "min_key: " << min_key << " max_key: " << max_key << " partitions: " << _num_parts << endl;
    W_DO(ssm->begin_xct());
    W_DO(ssm->make_equal_partitions(_index_id, min_key_vec, max_key_vec, _num_parts));
    W_DO(ssm->commit_xct());

    // fork off #partitions times threads and create the records and their assocs
    int diff = (max_key - min_key) / _num_parts;
    int current_start = min_key;
    threadptr *  subthreads = new  threadptr [_num_parts];
    for(int i=0; i<_num_parts; i ++) {
      cout << "Thread: " << i << endl;
      subthreads[i] = new smthread_creator_t(_num_rec, _rec_size,
					     _bIgnoreLocks, _bIgnoreLatches,
					     _design_no, current_start, current_start+diff, _index_id, 6);
      current_start += diff; 
    }
    for(int i=0; i<_num_parts; i ++) {
      subthreads[i]->fork();
    }
    for(int i=0; i<_num_parts; i ++) {
      subthreads[i]->join();
    }
    for(int i=0; i<_num_parts; i ++) {
      delete subthreads[i];
    }
    delete[] subthreads;

    W_DO(print_the_index());

    return RCOK;
}

rc_t smthread_main_t::mr_index_test7()
{
    cout << endl;
    cout << " ------- TEST7 -------" << endl;
    cout << "To test bulk loading! Single threaded version." << endl;
    cout << endl;
    
    cout << "Creating conventional btree index." << endl;
    W_DO(ssm->begin_xct());
    W_DO(create_the_index());
    W_DO(ssm->commit_xct());

    // create the records and their assocs
    threadptr creator_thread = new smthread_creator_t(_num_rec, _rec_size,
						      _bIgnoreLocks, _bIgnoreLatches,
						      _design_no, 0, _num_rec, _index_id, 7);
    creator_thread->fork();
    creator_thread->join();
    delete creator_thread;

    W_DO(print_the_index());
    
    return RCOK;
}

// prints the btree
rc_t smthread_main_t::print_the_index() 
{
    cout << "printing the mr index from store " << _index_id << endl;
    W_DO(ssm->begin_xct());
    if(_design_no == 0) {
        W_DO(ssm->print_index(_index_id));
    } else {
      W_DO(ssm->print_mr_index(_index_id));
    }
    W_DO(ssm->commit_xct());
    return RCOK;
}   

// scans the heap file the records are created
rc_t smthread_scanner_t::scan_the_file() 
{
  cout << "Scanning file " << _fid << endl;
  W_DO(ssm->begin_xct());
  
  scan_file_i scan(_fid);
  pin_i*      cursor(NULL);
  bool        eof(false);
  int         i(0);
  
  do {
    w_rc_t rc = scan.next(cursor, 0, eof);
    if(rc.is_error()) {
      cerr << "Error getting next: " << rc << endl;
      retval = rc.err_num();
      return rc;
    }
    if(eof) break;
    
    cout << "Record " << i << "/" << _num_rec
	 << " Rid "  << cursor->rid() << endl;
    vec_t       header (cursor->hdr(), cursor->hdr_size());
    int         hdrcontents;
    header.copy_to(&hdrcontents, sizeof(hdrcontents));
    cout << "Record hdr "  << hdrcontents << endl;    
    const char *body = cursor->body();
    cout << "Record body "  << body << endl;
    cout << "Record body size " << cursor->body_size() << endl;
    i++;
  } while (!eof);
    
  W_DO(ssm->commit_xct());
  return RCOK;
}

// scans the index that keeps the assocs of the created records
rc_t smthread_scanner_t::scan_the_index() 
{
  W_DO(ssm->begin_xct());
  cout << "Scanning index " << _index_id << endl;
  scan_index_i scan(_index_id, 
		    scan_index_i::ge, vec_t::neg_inf,
		    scan_index_i::le, vec_t::pos_inf, false,
		    ss_m::t_cc_kvl);
  bool        eof(false);
  int         i(0);
  smsize_t    klen(0);
  smsize_t    elen(0);
#define MAXKEYSIZE 100
  char *      keybuf[MAXKEYSIZE];
  file_info_t info;
  rid_t rid;
  
  do {
    w_rc_t rc = scan.next(eof);
    if(rc.is_error()) {
      cerr << "Error getting next: " << rc << endl;
      retval = rc.err_num();
      return rc;
    }
    if(eof) break;

    // get the key len and element len
    W_DO(scan.curr(NULL, klen, NULL, elen));
    // Create vectors for the given lengths.
    vec_t key(keybuf, klen);
    vec_t elem(&rid, elen);
    // Get the key and element value
    W_DO(scan.curr(&key, klen, &elem, elen));

    cout << "Key " << *(int*) keybuf << endl;
    cout << "Value " << rid << endl;
    i++;
  } while (!eof);
  W_DO(ssm->commit_xct());
  return RCOK;
}

// initialize the volume and create an empty heap file
rc_t smthread_main_t::do_init()
{
    cout << "-i: Initialize " << endl;

    {
        devid_t        devid;
        cout << "Formatting device: " << _device_name 
             << " with a " << _quota << "KB quota ..." << endl;
        W_DO(ssm->format_dev(_device_name, _quota, true));

        cout << "Mounting device: " << _device_name  << endl;
        // mount the new device
        u_int        vol_cnt;
        W_DO(ssm->mount_dev(_device_name, vol_cnt, devid));

        cout << "Mounted device: " << _device_name  
             << " volume count " << vol_cnt
             << " device " << devid
             << endl;

        // generate a volume ID for the new volume we are about to
        // create on the device
        cout << "Generating new lvid: " << endl;
        W_DO(ssm->generate_new_lvid(_lvid));
        cout << "Generated lvid " << _lvid <<  endl;

        // create the new volume 
        cout << "Creating a new volume on the device" << endl;
        cout << "    with a " << _quota << "KB quota ..." << endl;

        W_DO(ssm->create_vol(_device_name, _lvid, _quota, false, _vid));
        cout << "    with local handle(phys volid) " << _vid << endl;

    } 

    W_DO(create_the_file());
    return RCOK;
}

// perform the test given in the options
rc_t smthread_main_t::no_init()
{
    cout << "Using already-existing device: " << _device_name << endl;
    // mount already existing device
    devid_t      devid;
    u_int        vol_cnt;
    w_rc_t rc = ssm->mount_dev(_device_name, vol_cnt, devid);
    if (rc.is_error()) {
        cerr << "Error: could not mount device: " 
            << _device_name << endl;
        cerr << "   Did you forget to run the server with -i?" 
            << endl;
        return rc;
    }
    
    // find ID of the volume on the device
    lvid_t* lvid_list;
    u_int   lvid_cnt;
    W_DO(ssm->list_volumes(_device_name, lvid_list, lvid_cnt));
    if (lvid_cnt == 0) {
        cerr << "Error, device has no volumes" << endl;
        exit(1);
    }
    _lvid = lvid_list[0];
    delete [] lvid_list;

    W_COERCE(find_file_info());
    switch(_test_no) {
    case 0:
      W_DO(mr_index_test0()); // 
      break;
    case 1:
      W_DO(mr_index_test1()); // 
      break;
    case 2:
      W_DO(mr_index_test2()); //
      break;
    case 3:
      W_DO(mr_index_test3()); //
      break;
    case 4:
      W_DO(mr_index_test4()); //
      break;
    case 5:
      W_DO(mr_index_test5()); //
      break;
    case 6:
      W_DO(mr_index_test6()); //
      break;
    case 7:
      W_DO(mr_index_test7()); //
      break;
    }

    // scan the file if given in the input
    if(_scan_file > 0) {
      threadptr scanner_thread = new smthread_scanner_t(_index_id, _scan_file,
							_bIgnoreLatches,
							0, _num_rec);
      scanner_thread->fork();
      scanner_thread->join();
      delete scanner_thread;
    }
    
    return RCOK;
}

rc_t smthread_main_t::do_work()
{
    if (_initialize_device) {
      W_DO(do_init());
    }
    W_DO(no_init());
    return RCOK;
}

/**\defgroup EGOPTIONS Example of setting up options.
 * This method creates configuration options, starts up
 * the storage manager,
 */
w_rc_t smthread_main_t::handle_options()
{
    option_t* opt_device_name = 0;
    option_t* opt_device_quota = 0;
    option_t* opt_num_rec = 0;

    cout << "Processing configuration options ..." << endl;

    // Create an option group for my options.
    // I use a 3-level naming scheme:
    // executable-name.server.option-name
    // Thus, the file will contain lines like this:
    // create_rec.server.device_name : /tmp/example/device
    // *.server.device_name : /tmp/example/device
    // create_rec.*.device_name : /tmp/example/device
    //
    const int option_level_cnt = 3; 

    _options = new option_group_t (option_level_cnt);
    if(!_options) {
        cerr << "Out of memory: could not allocate from heap." <<
            endl;
        retval = 1;
        return RC(fcINTERNAL);
    }
    option_group_t &options(*_options);

    W_COERCE(options.add_option("device_name", "device/file name",
                         NULL, "device containg volume holding file to scan",
                         true, option_t::set_value_charstr,
                         opt_device_name));

    W_COERCE(options.add_option("device_quota", "# > 1000",
                         "2000", "quota for device",
                         false, option_t::set_value_long,
                         opt_device_quota));

    // Default number of records to create is 1.
    W_COERCE(options.add_option("num_rec", "# > 0",
                         "1", "number of records in file",
                         true, option_t::set_value_long,
                         opt_num_rec));

    // Have the SSM add its options to my group.
    W_COERCE(ss_m::setup_options(&options));

    cout << "Finding configuration option settings." << endl;

    w_rc_t rc = init_config_options(options, "server", _argc, _argv);
    if (rc.is_error()) {
        usage(options);
        retval = 1;
        return rc;
    }
    cout << "Processing command line." << endl;

    // Process the command line
    int option;
    while ((option = getopt(_argc, _argv, "hit:n:s:r:dpo:")) != -1) {
        switch (option) {
        case 'i' :
            _initialize_device = true;
            break;

	case 's' : // how many iterations for scan
	  _scan_file = atoi(optarg);
	  break;

        case 'h' :
            usage(options);
            break;

	case 't': // which test to run
	  _test_no = atoi(optarg);
	  break;

	case 'n': 
	  _num_parts = atoi(optarg);
	  break;

	case 'r':
	  _rec_size = atoi(optarg);
	  break;

	case 'd': // dora
	  _bIgnoreLocks = true;
	  break;
	  
	case 'p': // plp
	  _bIgnoreLatches = true;
	  break;

	case 'o': // which mrbt design
	  _design_no = atoi(optarg);;
	  break;
	  
        default:
            usage(options);
            retval = 1;
            return RC(fcNOTIMPLEMENTED);
            break;
        }
    }
    
    {
        cout << "Checking for required options...";
        /* check that all required options have been set */
        w_ostrstream      err_stream;
        w_rc_t rc = options.check_required(&err_stream);
        if (rc.is_error()) {
            cerr << "These required options are not set:" << endl;
            cerr << err_stream.c_str() << endl;
            return rc;
        }
        cout << "Options OK; values are: { " << endl;
        options.print_values(false, cout);
        cout << "} end list of options values. " << endl;
    }

    // Grab the options values for later use by run()
    _device_name = opt_device_name->value();
    _quota = strtol(opt_device_quota->value(), 0, 0);
    _num_rec = strtol(opt_num_rec->value(), 0, 0);
    
    // print the options
    cout << "DESIGN: " << _design_no << endl
	 << "TEST: " << _test_no << endl
	 << "_num_rec: " << _num_rec << endl
	 << "_rec_size: " << _rec_size << endl
	 << " _initialize_device: " << _initialize_device << endl
	 << "_num_parts: " << _num_parts << endl
	 << "_scan_file: " << _scan_file << endl
      	 << "_bIgnoreLocks: " << _bIgnoreLocks << endl
	 << "_bIgnoreLatches: " << _bIgnoreLatches << endl;
    
    return RCOK;
}

void smthread_main_t::run()
{
    w_rc_t rc = handle_options();
    if(rc.is_error()) {
        retval = 1;
        return;
    }

    // Now start a storage manager.
    cout << "Starting SSM and performing recovery ..." << endl;
    ssm = new ss_m();
    if (!ssm) {
        cerr << "Error: Out of memory for ss_m" << endl;
        retval = 1;
        return;
    }

    cout << "Getting SSM config info for record size ..." << endl;

    sm_config_info_t config_info;
    W_COERCE(ss_m::config_info(config_info));

    // Subroutine to set up the device and volume and
    // create the num_rec records of rec_size if initialization
    // then perform the requested test
    rc = do_work();

    sm_stats_info_t       stats;
    W_COERCE(ss_m::gather_stats(stats));
    cout << " SM Statistics : " << endl
         << stats  << endl;

    if (rc.is_error()) {
        cerr << "Could not set up device/volume due to: " << endl;
        cerr << rc << endl;
        delete ssm;
        rc = RCOK;   // force deletion of w_error_t info hanging off rc
                     // otherwise a leak for w_error_t will be reported
        retval = 1;
        if(rc.is_error()) 
            W_COERCE(rc); // avoid error not checked.
        return;
    }

    // Clean up and shut down
    cout << "\nShutting down SSM ..." << endl;
    delete ssm;

    cout << "Finished!" << endl;

    return;
}

int main(int argc, char* argv[])
{
        smthread_main_t *smtu = new smthread_main_t(argc, argv);
        if (!smtu)
                W_FATAL(fcOUTOFMEMORY);

        w_rc_t e = smtu->fork();
        if(e.is_error()) {
            cerr << "error forking thread: " << e <<endl;
            return 1;
        }
        e = smtu->join();
        if(e.is_error()) {
            cerr << "error forking thread: " << e <<endl;
            return 1;
        }

        int        rv = smtu->retval;
        delete smtu;

        return rv;
}

