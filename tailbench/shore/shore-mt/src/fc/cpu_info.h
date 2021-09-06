#ifndef __CPU_INFO_H
#define __CPU_INFO_H

struct cpu_info {
    static long cpuid();
    static long socket_of(long cpuid);
    
    static long socket_self();
    static long socket_count() { return get_helper()->socket_count; }
    static long cpu_count() { return get_helper()->cpu_count; }

    struct helper {
	static void compute_counts(long *ccount, long* scount);
	helper() {
	    compute_counts(&cpu_count, &socket_count);
	}
	long socket_count;
	long cpu_count;
    };	
    static helper* get_helper() {
	static helper h;
	return &h;
    }
    struct impl_helper;
    static void init_impl();
};

static struct cpu_info_init {
    cpu_info_init() {
	cpu_info::get_helper();
	cpu_info::init_impl();
    }
} __cpu_info_init;

#endif
