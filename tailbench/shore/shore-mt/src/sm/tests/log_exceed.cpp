/*<std-header orig-src='shore'>

 $Id: log_exceed.cpp,v 1.2 2010/06/08 22:28:15 nhall Exp $

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
 * This program is a test of file scan and lid performance
 */

#include <w_stream.h>
#include <sys/types.h>
#include <cassert>
#include "w_getopt.h"
#define SM_LEVEL 1
#define SM_SOURCE
#define XCT_C
#include "sm_int_1.h"
#include "sm_vas.h"

ss_m* ssm = 0;

extern w_rc_t out_of_log_space (xct_i*, xct_t*&, ss_m::fileoff_t, 
		                         ss_m::fileoff_t, 
								 const char *logfile
								 );

extern w_rc_t get_archived_log_file (const char *logfile, ss_m::partition_number_t);

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
    cerr << "Usage: log_exceed [-h] [options]" << endl;
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
                _initialize_device(true), // always for this test
                _options(NULL),
                _vid(1),
                retval(0) { }

        ~smthread_user_t()  { if(_options) delete _options; }

        void run();

        // helpers for run()
        w_rc_t handle_options();
        w_rc_t find_file_info();
        w_rc_t create_the_file();
        w_rc_t scan_the_file();
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
    char* dummy2 = new char[_rec_size];
    char* dummy3 = new char[_rec_size];
    char* dummy4 = new char[_rec_size];
    memset(dummy, '\1', _rec_size); // compressible
    memset(dummy2, '\2', _rec_size); // not compressible
    memset(dummy3, '\3', _rec_size); // not compressible
    memset(dummy4, '\4', _rec_size); // not compressible
	for(smsize_t i=0; i < _rec_size; i++) {
		dummy2[i] = (char)(i%8);
		dummy3[i] = (dummy2[i] + 1)%8; 
		dummy4[i] = (dummy2[i] - 1)%8; 
	}

    vec_t data(dummy, _rec_size);
    vec_t data2(dummy2, _rec_size);
    vec_t data3(dummy3, _rec_size);
    vec_t data4(dummy4, _rec_size);

    for(int j=0; j < _num_rec; j++)
    {
        {
            w_ostrstream o(dummy, _rec_size);
            o << "Record number " << j << ends;
            w_assert1(o.c_str() == dummy);
        }
        // header contains record #
        int i = j;
        const vec_t hdr(&i, sizeof(i));
        W_COERCE(ssm->create_rec(info.fid, hdr,
                                _rec_size, data, rid));
        cout << "Creating rec " << j << endl;

		// Now modify the record a bunch of times
		// These should chew up log space pretty fast.
		for(int i=0; i < 200; i++) {
			W_DO(ssm->update_rec(rid, 0 /* start byte */, data2));
			W_DO(ssm->update_rec(rid, 0 /* start byte */, data3));
			W_DO(ssm->update_rec(rid, 0 /* start byte */, data4));
		}
        if (j == 0) {
            info.first_rid = rid;
        }        
    }
    cout << "Created all. First rid " << info.first_rid << endl;
    delete [] dummy;
    delete [] dummy2;
    delete [] dummy3;
    delete [] dummy4;
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

rc_t
smthread_user_t::scan_the_file() 
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
        w_assert1(cursor->body_size() == _rec_size);
        cout << "Record body "  << body << endl;
        i++;
    } while (!eof);
    w_assert1(i == _num_rec);

    W_DO(ssm->commit_xct());
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

    W_DO(find_file_info());
    W_DO(scan_the_root_index());
    W_DO(scan_the_file());
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
static option_t *logdir(NULL);
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
	// log_exceed.server.device_name : /tmp/example/device
	// *.server.device_name : /tmp/example/device
	// log_exceed.*.device_name : /tmp/example/device
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

    if(logdir==NULL) {
		W_DO(options.lookup("sm_logdir", false, logdir));
		fprintf(stdout, "Found logdir %s\n", logdir->value());
	}

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
    ssm = new ss_m(out_of_log_space, get_archived_log_file);
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
		{
			w_ostrstream o;
			static sm_stats_info_t curr;

			rc = ssm->gather_stats(curr);

			o << curr.sm.abort_xct_cnt<< ends;
			fprintf(stdout, "log_exceed stats: abort_xct_cnt %s\n" , o.c_str()); 
		}
        delete ssm;
        rc = RCOK;   // force deletion of w_error_t info hanging off rc
                     // otherwise a leak for w_error_t will be reported
        retval = 0;
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

#include <os_interface.h>

void dump()
{

	os_dirent_t *dd = NULL;
	os_dir_t ldir = os_opendir(logdir->value());
	if(!ldir) {
		fprintf(stdout, "Could not open directory %s\n", logdir->value());
		return;
	}
	fprintf(stdout, "---------------------- %s {\n", logdir->value());
	while ((dd = os_readdir(ldir)))
	{
		fprintf(stdout, "%s\n", dd->d_name);
	}
	os_closedir(ldir);
	fprintf(stdout, "---------------------- %s }\n", logdir->value());
}


w_rc_t get_archived_log_file (
		const char *filename, 
		ss_m::partition_number_t num)
{
	fprintf(stdout, 
			"\n**************************************** RECOVER %s : %d\n",
			filename, num
			);
	dump();

	w_rc_t rc;

	static char O[smlevel_0::max_devname<<1];
	strcat(O, filename);
	strcat(O, ".bak");

	static char N[smlevel_0::max_devname<<1];
	strcat(N, filename);

	int e = ::rename(O, N);
	if(e != 0) 
	{
		fprintf(stdout, "Could not move %s to %s: error : %d %s\n",
				O, N, e, strerror(errno));
		rc = RC2(smlevel_0::eOUTOFLOGSPACE, errno); 
	}
	dump();
	fprintf(stdout, "recovered ... OK!\n\n");
	fprintf(stdout, 
		"This recovery of the log file will enable us to finish the abort.\n");
	fprintf(stdout, 
		"It will not continue the device/volume set up.\n");
	fprintf(stdout, 
		"Expect an error message and stack trace about that:\n\n");
	return rc;
}

w_rc_t out_of_log_space (
	xct_i* iter, 
	xct_t *& xd,
    smlevel_0::fileoff_t curr,
    smlevel_0::fileoff_t thresh,
	const char *filename
)
{
	static int calls=0;

	calls++;

	w_rc_t rc;
	fprintf(stdout, "\n**************************************** %d\n", calls);
	dump();

	fprintf(stdout, 
			"Called out_of_log_space with curr %lld thresh %lld, file %s\n",
			(long long) curr, (long long) thresh, filename);
	{
		w_ostrstream o;
		o << xd->tid() << endl;
		fprintf(stdout, "called with xct %s\n" , o.c_str()); 
	}

	xd->log_warn_disable();
	iter->never_mind(); // release the mutex

	{
		w_ostrstream o;
		static sm_stats_info_t curr;

		W_DO( ssm->gather_stats(curr));

		o << curr.sm.log_bytes_generated << ends;
		fprintf(stdout, "stats: log_bytes_generated %s\n" , o.c_str()); 
	}
	xct_t *oldxct(NULL);
	lsn_t target;
	{
		w_ostrstream o;
		o << "Active xcts: " << xct_t::num_active_xcts() << " ";

		tid_t old = xct_t::oldest_tid();
		o << "Oldest transaction: " << old;

		xct_t *x = xct_t::look_up(old);
		if(x==NULL) {
			fprintf(stdout, "Could not find %s\n", o.c_str());
			W_FATAL(fcINTERNAL);
		}

		target = x->first_lsn();
		o << "   First lsn: " << x->first_lsn();
		o << "   Last lsn: " << x->last_lsn();

		fprintf(stdout, "%s\n" , o.c_str()); 

		oldxct = x;
	}

	if(calls > 3) {
		// See what happens...
		static tid_t aborted_tid;
		if(aborted_tid == xd->tid()) {
			w_ostrstream o;
			o << aborted_tid << endl;
			fprintf(stdout, 
					"Multiple calls with same victim! : %s total calls %d\n",
					o.c_str(), calls);
			W_FATAL(smlevel_0::eINTERNAL);
		}
		aborted_tid = xd->tid();
		fprintf(stdout, "\n\n******************************** ABORTING\n\n");
		return RC(smlevel_0::eUSERABORT); // sm will abort this guy
	}

	fprintf(stdout, "\n\n******************************** ARCHIVING \n\n");
	fprintf(stdout, "Move aside log file log.%d to log.%d.bak\n", 
			target.file(), target.file());
	static char O[smlevel_0::max_devname<<1];
	strcat(O, filename);
	static char N[smlevel_0::max_devname<<1];
	strcat(N, filename);
	strcat(N, ".bak");

	int e = ::rename(O, N);
	if(e != 0) {
		fprintf(stdout, "Could not move %s to %s: error : %d %s\n",
				O, N, e, strerror(errno));
		if(errno == ENOENT) {
			fprintf(stdout, "Ignored error.\n\n");
			return RCOK; // just to ignore these.
		}
		fprintf(stdout, "Returning eOUTOFLOGSPACE.\n\n");
		rc = RC2(smlevel_0::eOUTOFLOGSPACE, errno); 
	} else {
		dump();
		fprintf(stdout, "archived ... OK\n\n");
		W_COERCE(ss_m::log_file_was_archived(filename));
	}
    return rc;
}
