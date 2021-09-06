/*<std-header orig-src='shore'>

 $Id: sort_stream.cpp,v 1.1 2010/05/26 01:21:21 nhall Exp $

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

/**\anchor sort_stream_i_example */
/*
 * This program is a test of file scan and lid performance
 */

#include <w_stream.h>
#include <sys/types.h>
#include <cassert>
#include "sm_vas.h"
#include "w_getopt.h"
ss_m* ssm = 0;

// shorten error code type name
typedef w_rc_t rc_t;

// this is implemented in options.cpp
w_rc_t init_config_options(option_group_t& options,
                        const char* prog_type,
                        int& argc, char** argv);


struct file_info_t {
    static const char* key;
    stid_t         fid;
    rid_t       first_rid;
    int         num_rec;
    int         rec_size;
};
const char* file_info_t::key = "SCANFILE";

ostream &
operator << (ostream &o, const file_info_t &info)
{
    o << "key " << info.key
    << " fid " << info.fid
    << " first_rid " << info.first_rid
    << " num_rec " << info.num_rec
    << " rec_size " << info.rec_size ;
    return o;
}


typedef        smlevel_0::smksize_t        smksize_t;



void
usage(option_group_t& options)
{
    cerr << "Usage: sort_stream [-h] [-i] [options]" << endl;
    cerr << "       -i initialize device/volume and create file of records" << endl;
    cerr << "Valid options are: " << endl;
    options.print_usage(true, cerr);
}

/* create an smthread based class for all sm-related work */
class smthread_user_t : public smthread_t {
        int        _argc;
        char        **_argv;

        const char *_device_name;
        smsize_t    _quota;
        int         _num_rec;
        smsize_t    _rec_size;
        lvid_t      _lvid;  
        rid_t       _start_rid;
        stid_t      _fid;
        bool        _initialize_device;
        option_group_t* _options;
        vid_t       _vid;
public:
        int         retval;

        smthread_user_t(int ac, char **av) 
                : smthread_t(t_regular, "smthread_user_t"),
                _argc(ac), _argv(av), 
                _device_name(NULL),
                _quota(0),
                _num_rec(0),
                _rec_size(0),
                _initialize_device(false),
                _options(NULL),
                _vid(1),
                retval(0) { }

        ~smthread_user_t()  { if(_options) delete _options; }

        void run();

        // helpers for run()
        w_rc_t handle_options();
        w_rc_t find_file_info();
        w_rc_t create_the_file();
        w_rc_t demo_sort_stream();
        w_rc_t scan_the_root_index();
        w_rc_t do_work();
        w_rc_t do_init();
        w_rc_t no_init();

};

/*
 * looks up file info in the root index
*/
w_rc_t
smthread_user_t::find_file_info()
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

    _start_rid = info.first_rid;
    _fid = info.fid;
	_rec_size = info.rec_size;
	_num_rec = info.num_rec;
    return RCOK;
}

/*
 * This function either formats a new device and creates a
 * volume on it, or mounts an already existing device and
 * returns the ID of the volume on it.
 *
 * It's borrowed from elsewhere; it can handle mounting
 * an already existing device, even though in this main program
 * we don't ever do that.
 */
rc_t
smthread_user_t::create_the_file() 
{
    file_info_t info;  // will be made persistent in the
    // volume root index.

    // create and fill file to scan
    cout << "Creating a file with " << _num_rec 
        << " records of size " << _rec_size << endl;
    W_DO(ssm->begin_xct());

    // Create the file. Stuff its fid in the persistent file_info
    W_DO(ssm->create_file(_vid, info.fid, smlevel_3::t_regular));
    rid_t rid;

    _rec_size -= align(sizeof(int));

/// each record will have its ordinal number in the header
/// and zeros for data 

    char* dummy = new char[_rec_size];
    memset(dummy, '\0', _rec_size);
    vec_t data(dummy, _rec_size);

    for(int j=0; j < _num_rec; j++)
    {
        {
            w_ostrstream o(dummy, _rec_size);
            o << "Record number " << j << ends;
            w_assert1(o.c_str() == dummy);
        }

        // header contains record #
        // body contains zeroes of size _rec_size
		w_base_t::int4_t i = j;
        const vec_t hdr(&i, sizeof(i));

        W_COERCE(ssm->create_rec(info.fid, hdr,
                                _rec_size, data, rid));
        cout << "Creating rec " << j << endl;
        if (j == 0) {
            info.first_rid = rid;
        }        
    }
    cout << "Created all. First rid " << info.first_rid << endl;
    delete [] dummy;
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

rc_t
smthread_user_t::scan_the_root_index() 
{
    W_DO(ssm->begin_xct());
	stid_t _root_iid;
    W_DO(ss_m::vol_root_index(_vid, _root_iid));
    cout << "Scanning index " << _root_iid << endl;
	scan_index_i scan(_root_iid, 
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
		vec_t elem(&info, elen);
		// Get the key and element value
		W_DO(scan.curr(&key, klen, &elem, elen));

        cout << "Key " << keybuf << endl;
        cout << "Value " 
		<< " { fid " << info.fid 
		<< " first_rid " << info.first_rid
		<< " #rec " << info.num_rec
		<< " rec size " << info.rec_size << " }"
		<< endl;
        i++;
    } while (!eof);
    W_DO(ssm->commit_xct());
	return RCOK;
}

w_rc_t 
smthread_user_t::demo_sort_stream()
{
	// Recall that the records contain a header that indicates the
	// order in which the records were created.  Let's sort in
	// reverse order.
	w_base_t::int4_t hdrcontents;

	// Key descriptor
	using ssm_sort::key_info_t ;

	key_info_t     kinfo;
	kinfo.type = sortorder::kt_i4;
	kinfo.derived = false;
	kinfo.where = key_info_t::t_hdr;
	kinfo.offset = 0;
	kinfo.len = sizeof(hdrcontents);
	kinfo.est_reclen = _rec_size;

	// Behavioral options
	using ssm_sort::sort_parm_t ;

	sort_parm_t   behav;
	behav.run_size = 10; // pages
	behav.vol = _vid;
	behav.unique = false; // there should be no duplicates
	behav.ascending = false; // reverse the order
	behav.destructive = false; // don't blow away the original file --
	// immaterial for sort_stream_i
	behav.property = ss_m::t_temporary; // don't log the scratch files used

	sort_stream_i  stream(kinfo, behav, _rec_size);
	w_assert1(stream.is_sorted()==false);

	// Scan the file, inserting key,rid pairs into a sort stream
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

        vec_t       header (cursor->hdr(), cursor->hdr_size());
        header.copy_to(&hdrcontents, sizeof(hdrcontents));
        cout << "Record hdr "  << hdrcontents << endl;

		rid_t       rid = cursor->rid();
        vec_t       elem (&rid, sizeof(rid));

		stream.put(header, elem);
        i++;
    } while (!eof);
    w_assert1(i == _num_rec);
    W_DO(ssm->commit_xct());

	// is not sorted until first get_next call
	w_assert1(stream.is_sorted()==false);

	// Iterate over the sort_stream_i 
    W_DO(ssm->begin_xct());
	i--;
	eof = false;
    do {
		vec_t       header; 
		vec_t       elem; 
		W_DO(stream.get_next(header, elem, eof));
		if(eof) break;

		w_assert1(stream.is_sorted()==true);
		w_assert1(stream.is_empty()==false);
		header.copy_to(&hdrcontents, sizeof(hdrcontents));

		rid_t       rid; 
		elem.copy_to(&rid, sizeof(rid));

		w_assert1(i == hdrcontents);

		cout << "i " << hdrcontents << " rid " << rid << endl;
		i--;
    } while (!eof);
    W_DO(ssm->commit_xct());
	w_assert1(stream.is_empty()==false);
	w_assert1(stream.is_sorted()==true);
    return RCOK;
}

rc_t
smthread_user_t::do_init()
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
    W_DO(find_file_info());
    W_DO(demo_sort_stream());
    return RCOK;
}

rc_t
smthread_user_t::no_init()
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
    W_COERCE(scan_the_root_index());
    W_DO(demo_sort_stream());
    return RCOK;
}

rc_t
smthread_user_t::do_work()
{
    if (_initialize_device) W_DO(do_init());
    else  W_DO(no_init());
    return RCOK;
}

/**\defgroup EGOPTIONS Example of setting up options.
 * This method creates configuration options, starts up
 * the storage manager,
 */
w_rc_t smthread_user_t::handle_options()
{
    option_t* opt_device_name = 0;
    option_t* opt_device_quota = 0;
    option_t* opt_num_rec = 0;

    cout << "Processing configuration options ..." << endl;

    // Create an option group for my options.
	// I use a 3-level naming scheme:
	// executable-name.server.option-name
	// Thus, the file will contain lines like this:
	// sort_stream.server.device_name : /tmp/example/device
	// *.server.device_name : /tmp/example/device
	// sort_stream.*.device_name : /tmp/example/device
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

    // Process the command line: looking for the "-h" flag
    int option;
    while ((option = getopt(_argc, _argv, "hi")) != -1) {
        switch (option) {
        case 'i' :
            _initialize_device = true;
            break;

        case 'h' :
            usage(options);
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
        cout << "OK " << endl;
    }

    // Grab the options values for later use by run()
    _device_name = opt_device_name->value();
    _quota = strtol(opt_device_quota->value(), 0, 0);
    _num_rec = strtol(opt_num_rec->value(), 0, 0);

    return RCOK;
}

void smthread_user_t::run()
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
    _rec_size = config_info.max_small_rec; // minus a header

    // Subroutine to set up the device and volume and
    // create the num_rec records of rec_size.
    rc = do_work();

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

// This was copied from file_scan so it has lots of extra junk
int
main(int argc, char* argv[])
{
        smthread_user_t *smtu = new smthread_user_t(argc, argv);
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

