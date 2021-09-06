#include <pthread.h>

extern "C" void cpu_info_dtrace_global_hook(long volatile* /*ready*/) { }
extern "C" void cpu_info_dtrace_thread_hook(long volatile* /*cpuid*/) { }
