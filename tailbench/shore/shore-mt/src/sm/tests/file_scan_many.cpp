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
 * This program is a test of file scan and lid performance
 */

#include <w_stream.h>
#include <sys/types.h>
#include <cassert>
// This include brings in all header files needed for writing a VAs 
#include "sm_vas.h"
#include "w_getopt.h"

ss_m* ssm = 0;
static bool debug(false);
vid_t   vid(10);

// shorten error code type name
typedef w_rc_t rc_t;

// this is implemented in options.cpp
w_rc_t init_config_options(option_group_t& options,
                        const char* prog_type,
                        int& argc, char** argv);

struct file_info_t {
	file_info_t() : num_rec(0), rec_size(0), append_only(false)
	{ 
		memset(key, 0, 120);
	}
    stid_t      fid;
    rid_t       first_rid;
    int         num_rec;
    int         rec_size;
    bool        append_only;
	fill1       _fill1;
	fill2       _fill2;
    char        key[120];
};

ostream &
operator << (ostream &o, const file_info_t &info)
{
    o << "key " << info.key
    << " fid " << info.fid
    << " first_rid " << info.first_rid
    << " num_rec " << info.num_rec
    << " rec_size " << info.rec_size 
    << " append-only " << info.append_only ;
    return o;
}

typedef        smlevel_0::smksize_t        smksize_t;

void
usage(option_group_t& options)
{
    // getopt(argc, argv, "Adn:his:t:l:")
    cerr << "Usage: server [-h] [-i] [options]" << endl;
    cerr << "       -i initialize device/volume and create file with nrec records" << endl;
    cerr << "       -n <#records>" << endl;
    cerr << "       -A strict-append" << endl;
    cerr << "       -d print record ids found" << endl;
    cerr << "       -h print this message" << endl;
    cerr << "Valid options are: " << endl;
    options.print_usage(true, cerr);
}

// for bzero:
#include <strings.h>

/* create an smthread based class for all sm-related work */
class smthread_main_t : public smthread_t 
{
    int        argc;
    char        **argv;
public:
    int        retval;
protected:
    char     *key;
    bool      append_only;
	fill1     dummy1;
    vid_t     vid;
    w_rc_t    find_file_info(vid_t vid, stid_t root_iid, file_info_t &info);

    char *make(int _id, int /*n*/) {
		const int SZ=120;
        key = new char[SZ];
        bzero(key, SZ);
        // For the scanner, don't include the nrecs. Let it
        // find that number in the file info.
        sprintf(key, "SCANFILE %d.", _id);
        return key;
    }
public:
    smthread_main_t(int ac, char **av, const char *name="smthread_main_t" ) 
            : smthread_t(t_regular, name),
            argc(ac), argv(av), retval(0), key(0),
             append_only(false)
    { }
    ~smthread_main_t()  { delete[] key; }

    rc_t setup_device_and_volume(
            const char* device_name, bool init_device,
                    smksize_t quota
                    );
    void run();
};
typedef smthread_main_t *  threadptr;

/* This thread creates a file and fills it with records, the
 * puts an entry in the root index of the volume
 */
class smthread_creator_t : public smthread_main_t 
{
    int num_rec;
    int rec_size;
    int i;
public:
    smthread_creator_t(const vid_t &v, int _id, int n, int sz) : 
        smthread_main_t(0,0, "smthread_creator_t"), 
        num_rec(n) , rec_size(sz), i(_id)
    {
        key = make(_id, n);
        vid = v;
    }

    ~smthread_creator_t() 
    {
    }
    void run();
};

class smthread_scanner_t : public smthread_main_t 
{
    int num_rec;
    int rec_size;

    void scan_i_scan(const stid_t& fid, int num_rec, ss_m::concurrency_t cc);
public:
    smthread_scanner_t(
                         const vid_t &v, int _id, int n, int sz) : 
                              smthread_main_t(0,0, "smthread_scanner_t"),
                              num_rec(n),
                              rec_size(sz)
    {   vid = v;
        key = make(_id, n);
    }
    ~smthread_scanner_t()  { }
    void run();
};

/*
 * looks up file info in the root index
*/
w_rc_t
smthread_main_t::find_file_info(
        vid_t _vid, 
        stid_t root_iid, 
        file_info_t &info //out
        )
{

    W_DO(ssm->begin_xct());
    bool        found;
    W_DO(ss_m::vol_root_index(_vid, root_iid));


    smsize_t    info_len = sizeof(info);
    const vec_t        key_vec_tmp(key, strlen(key));
    W_DO(ss_m::find_assoc(root_iid, key_vec_tmp,
                          &info, info_len, found));
    if (!found) {
        cerr << "No file information found, looking for "
            << key
            << " lvid= " << _vid
            << " root_iid= " << root_iid
            <<endl;
        return RC(fcASSERT);
    } else {
       cout << " found assoc "
                << key << " --> " << info << endl;
    }

    W_DO(ssm->commit_xct());

    return RCOK;
 
}

/*
 * This function either formats a new device and creates a
 * volume on it, or mounts an already existing device and
 * returns the ID of the volume on it.
 */

rc_t
smthread_main_t:: setup_device_and_volume(const char* device_name, 
                        bool init_device,
                        smksize_t quota
                        )
{
    devid_t        devid;
    u_int        vol_cnt;
    rc_t         rc;

    vid         = 10;

    file_info_t info;

    if (init_device) 
    {
        cout << "Formatting device: " << device_name 
             << " with a " << quota << "KB quota ..." << endl;
        W_DO(ssm->format_dev(device_name, quota, true));

        cout << "Mounting device: " << device_name  << endl;
        // mount the new device
        W_DO(ssm->mount_dev(device_name, vol_cnt, devid));

        cout << "Mounted device: " << device_name  
             << " volume count " << vol_cnt
             << " device " << devid
             << endl;

        // generate a volume ID for the new volume we are about to
        // create on the device
		lvid_t lvid;
        cout << "Generating new lvid: " << endl;
        W_DO(ssm->generate_new_lvid(lvid));
        cout << "Generated lvid " << lvid <<  endl;

        // create the new volume 
        cout << "Creating a new volume on the device" << endl;
        cout << "    with a " << quota << "KB quota ..." << endl;
        cout << "    with local handle(phys volid) " << vid << endl;
        W_DO(ssm->create_vol(device_name, lvid, quota, false, vid));
        cout << "Created vid " <<  vid << endl;

#if USE_LID
        cout << "Adding LID index" << endl;
        // create the logical ID index on the volume, reserving no IDs
        W_DO(ssm->add_logical_id_index(lvid, 0, 0));
#endif
    }
    else
    {
        cout << "Using already existing device: " << device_name << endl;
        // mount already existing device
        w_rc_t rc = ssm->mount_dev(device_name, vol_cnt, devid, vid);
        if (rc.is_error()) {
            cerr << "Error: could not mount device: " << device_name << endl;
            cerr 
    << "   Did you forget to run the server with -i the first time?" << endl;
            return rc;
        }

        // find ID of the volume on the device
        lvid_t* lvid_list;
        u_int   lvid_cnt;
        W_DO(ssm->list_volumes(device_name, lvid_list, lvid_cnt));
        if (lvid_cnt == 0) {
            cerr << "Error, device has no volumes" << endl;
            ::exit(1);
        } else {
            cout << "Device has volumes:" ;
            for(unsigned i=0; i < lvid_cnt; i++) {
                cout << lvid_list[i] << " " ;
            }
            cout << endl;
        }
		W_DO(ss_m::lvid_to_vid(lvid_list[0], vid));
        delete [] lvid_list;
    }

    return RCOK;
}

void smthread_main_t::run()
{
    rc_t rc;

    // pointers to options we will create for the grid server program
    option_t* opt_device_name = 0;
    option_t* opt_device_quota = 0;
    option_t* opt_num_rec = 0;
    option_t* opt_rec_size = 0;

    cout << "processing configuration options ..." << endl;
    const int option_level_cnt = 3; 
    option_group_t options(option_level_cnt);

    W_COERCE(options.add_option("device_name", "device/file name",
                         NULL, "device containg volume holding file to scan",
                         true, option_t::set_value_charstr,
                         opt_device_name));

    W_COERCE(options.add_option("device_quota", "# > 1000",
                         "2000", "quota for device",
                         false, option_t::set_value_long,
                         opt_device_quota));

    W_COERCE(options.add_option("num_rec", "# > 0",
                         NULL, "number of records in file",
                         true, option_t::set_value_long,
                         opt_num_rec));

    W_COERCE(options.add_option("rec_size", "# > 0",
                         "7800", "size for records",
                         false, option_t::set_value_long,
                         opt_rec_size));

    // have the SSM add its options to the group
    W_COERCE(ss_m::setup_options(&options));

    rc = init_config_options(options, "server", argc, argv);
    if (rc.is_error()) {
        usage(options);
        retval = 1;
        return;
    }

    int cmdline_nrecords(-1); // trumps config options if set
    int nrecords(0); // set by config options

    // process command line: looking for the "-i" flag
    bool init_device = false;
    int option;
    int nthreads(1);
    while ((option = getopt(argc, argv, "Adn:hit:")) != -1) {
        switch (option) {
        case 'A' :
            append_only = true;
            break;
        case 'd' :
            debug = true;
            break;
        case 'n' :
            cmdline_nrecords = strtol(optarg, 0, 0);
            break;
        case 'h' :
            usage(options);
            retval = 1;
            return;
            break;
        case 'i' :
            {
                if (init_device) {
                    cerr << "Error only one -i parameter allowed" << endl;
                    usage(options);
                    retval = 1;
                    return;
                }

                init_device = true;
            }
            break;
        case 't' :
            nthreads = strtol(optarg, 0, 0);
            break;
        default:
            usage(options);
            retval = 1;
            return;
            break;
        }
    }

    cout << "Starting SSM and performing recovery ..." << endl;
    ssm = new ss_m();
    if (!ssm) {
        cerr << "Error: Out of memory for ss_m" << endl;
        retval = 1;
        return;
    }

    smksize_t quota = strtol(opt_device_quota->value(), 0, 0);
    nrecords = strtol(opt_num_rec->value(), 0, 0);
    smsize_t rec_size = strtol(opt_rec_size->value(), 0, 0);
    rid_t start_rid;
    stid_t fid;

    if(cmdline_nrecords > 0) {
        cout << " num rec of " << cmdline_nrecords 
                << " overrides config option num rec of "  << nrecords
                << endl;
        nrecords = cmdline_nrecords;
    }

    rc = setup_device_and_volume(opt_device_name->value(), 
            init_device, quota);

    if (rc.is_error()) {
        cerr << "could not set up device/volume due to: " << endl;
        cerr << rc << endl;
        delete ssm;
        rc = RCOK;   // force deletion of w_error_t info hanging off rc
                     // otherwise a leak for w_error_t will be reported
        retval = 1;
        if(rc.is_error()) 
            W_COERCE(rc); // avoid error not checked.
        return;
    }

    cout << "************ vid " << vid << endl;
    cout << "************ NTHREADS " << nthreads << endl;

    /* fork off the correct number of threads */
    threadptr *  subthreads = new  threadptr [nthreads];
    if(init_device) {
        for(int i=0; i<nthreads; i ++)
        {
              int nrecs = nrecords/(i?i:1) +1;
              subthreads[i] = 
                  new smthread_creator_t(vid, i, nrecs, rec_size);
#if 0 && defined(USING_VALGRIND) 
			if(RUNNING_ON_VALGRIND)
			{
				check_definedness(subthreads[i], sizeof(smthread_creator_t));
				check_valgrind_errors(__LINE__, __FILE__);
			}
#endif
        }
    } else {
        for(int i=0; i<nthreads; i ++) 
        {
              int nrecs = nrecords/(i?i:1) +1;
              subthreads[i] = 
                 new smthread_scanner_t(vid, i, nrecs, rec_size);
        }
    }
    for(int i=0; i<nthreads; i ++)
    {
        subthreads[i]->fork();
    }
    for(int i=0; i<nthreads; i ++)
    {
        subthreads[i]->join();
    }
    for(int i=0; i<nthreads; i ++)
	{
		delete subthreads[i];
	}
	delete[] subthreads;

    cout << "\nShutting down SSM ..." << endl;
    delete ssm;

    cout << "*******************************************" << endl;
    cout << "Finished! return value=" << retval << endl;
    cout << "*******************************************" << endl;
}

#define START 1

void smthread_creator_t::run()
{
    cout << "Created creator with key " << key << endl; 

    file_info_t info;
    strcpy(info.key, key);

    // create and fill file to scan
    cout << "Creating a file with " << num_rec 
        << " records of size " << rec_size 
        << " vid " << vid 
        << endl;
    W_COERCE(ssm->begin_xct());

    W_COERCE(ssm->create_file(vid, info.fid, smlevel_3::t_regular));
    rid_t rid;

    char* dummy = new char[rec_size];
    memset(dummy, '\0', rec_size);
    vec_t data(dummy, rec_size);
    if(append_only)
    {
        info.append_only=true;
        append_file_i iter(info.fid);
        for (int j = START; j <= num_rec; j++) {
                const vec_t hdr(&j, sizeof(j));;
                iter.create_rec(hdr, rec_size, data, rid);
                if (j == START) {
                info.first_rid = rid;
                }        
        }
    } 
    else 
    {
        for (int j = START; j <= num_rec; j++) {
            const vec_t hdr(&j, sizeof(j));;
            W_COERCE(ssm->create_rec(info.fid, hdr,
                                    rec_size, data, rid));
            if (j == START) {
                info.first_rid = rid;
            }        
        }
    }
    delete [] dummy;
    info.num_rec = num_rec;
    info.rec_size = rec_size;

    stid_t root_iid;  // root index ID
    // record file info in the root index : this stores some
    // attributes of the file in general
    cout << "calling vol_root_id lvid= " << vid << endl;
    W_COERCE(ss_m::vol_root_index(vid, root_iid));

    const vec_t key_vec_tmp(key, strlen(key));
    const vec_t info_vec_tmp(&info, sizeof(info));
#if 1 && defined(USING_VALGRIND) 
	// I did once have this at the beginning but then we
	// croak because we haven't called set_lsn_ck yet
    if(RUNNING_ON_VALGRIND)
    {
        check_valgrind_errors(__LINE__, __FILE__);
        check_definedness(key, strlen(key));
        check_valgrind_errors(__LINE__, __FILE__);
        check_definedness(&info, sizeof(info));
        check_valgrind_errors(__LINE__, __FILE__);
    }
#endif
    W_COERCE(ss_m::create_assoc(root_iid,
                            key_vec_tmp,
                            info_vec_tmp));
    cout << " Creating assoc "
                << key << " --> " << info << endl;
    W_COERCE(ssm->commit_xct());


    file_info_t info2;
    W_COERCE(find_file_info(vid, root_iid, info2 ));

    {
        if(info.first_rid != info2.first_rid) {
            cerr << "first_rid : " << info.first_rid
            << " stored info has " << info2.first_rid << endl; 
            W_COERCE( RC(fcASSERT) );
        }
        if(info.fid != info2.fid) {
            cerr << "fid : " << info.fid
            << " stored info has " << info2.fid << endl; 
            W_COERCE( RC(fcASSERT) );
        }
        if(info.append_only != info2.append_only) {
            cerr << "append_only : " << info.append_only
            << " stored info has " << info2.append_only << endl; 
            W_COERCE( RC(fcASSERT) );
        }
    }
    cout << "********** Done creating file " << info.fid << endl;
    threadptr subthread =
              new smthread_scanner_t(vid, i, num_rec, rec_size);
    subthread->fork();
    subthread->join();
	delete subthread;
}

void smthread_scanner_t::run()
{
    cout << "********** Created scanner for key "
        << key
        << endl;

    stid_t root_iid;  // root index ID
    // record file info in the root index : this stores some
    // attributes of the file in general
    W_COERCE(ss_m::vol_root_index(vid, root_iid));

    file_info_t info2;
    W_COERCE(find_file_info(vid, root_iid, info2 ));

    append_only = info2.append_only;
    rid_t first_rid = info2.first_rid;
    stid_t fid = info2.fid;
    num_rec = info2.num_rec;

    cout << "********** Starting scanning file " << fid  << endl;

    {
        W_COERCE(ssm->begin_xct());
        ss_m::concurrency_t cc = ss_m::t_cc_file;
        this->scan_i_scan(fid, num_rec, cc);
        W_COERCE(ssm->commit_xct());
    }
}

void smthread_scanner_t::scan_i_scan(const stid_t& fid, int num_recs,
                ss_m::concurrency_t cc)
{
    cout << "starting scan_i of " 
        << fid << ", " 
        << num_recs << " records" << endl;
    scan_file_i scan(fid, cc);

    w_rc_t rc = scan.error_code();
    if(rc.is_error()) {
        cerr << "Could not create scan_i with fid " << fid 
            << " error is " << rc
            << endl;
        ::exit(1);
    }
    pin_i         *handle;
    bool        eof = false;
    int         i = START;
    do {
        w_rc_t rc;
        rc = scan.error_code();
        if(rc.is_error()) {
            cerr << "Could not create scan_i with fid " << fid 
                << " error is " << rc
                << endl;
            ::exit(1);
        }
        w_assert1(scan.error_code().is_error() == false);

        rc = scan.next(handle, 0, eof);
        if(rc.is_error()) {
            W_COERCE(rc);
        }

        if(debug) {
            cout << " scanned i " << i << ": "  
				<< scan.curr_rid 
				<< " eof=" << eof
				<< endl;
        }
        if(eof) break;

        w_assert1(handle->pinned());
        const char *hdr =handle->hdr();
        smsize_t hdrsize=handle->hdr_size();
        /// alignment should be ok:
        vec_t ref(hdr, hdrsize);
        int refi;
        ref.copy_to(&refi, sizeof (refi));

        if(debug) {
            cout << "    header contains " 
                << refi << endl;
        }
        
        if(append_only) {
            // if we used append-only on the file creation,
            // we had better find the order preserved.
            w_assert1(refi == i);
        }

/*
        const char *body =handle->body();
        smsize_t bodysize=handle->body_size();
*/
        i++;
    } while (1) ;
    cout << "scan_i scan complete, i="  << i
		<< " num_rec expected =" << num_recs
		<< endl;
    assert(i-START == num_recs);
}

#if 0
void lid_scan(const vid_t& lvid, const rid_t& start_rid, int num_rec)
{
    cout << "starting lid scan of " << num_rec << " records" << endl;
    rid_t         rid = start_rid;
    pin_i         pin;
    int         i;
    for (i = 0, rid = start_rid; i < num_rec; i++, rid.increment(1)) {
        W_COERCE(pin.pin(lvid, rid, 0));
    }
    assert(i == num_rec);
    cout << "lid scan complete" << endl;
}
#endif


int
main(int argc, char* argv[])
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

