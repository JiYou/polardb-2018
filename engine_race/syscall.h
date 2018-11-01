#include <sys/syscall.h>
#include <unistd.h>

#define _SYMSTR(str)	#str
#define SYMSTR(str)	_SYMSTR(str)

#define SYMVER(compat_sym, orig_sym, ver_sym)	\
	__asm__(".symver " SYMSTR(compat_sym) "," SYMSTR(orig_sym) "@LIBAIO_" SYMSTR(ver_sym));

#define DEFSYMVER(compat_sym, orig_sym, ver_sym)	\
	__asm__(".symver " SYMSTR(compat_sym) "," SYMSTR(orig_sym) "@@LIBAIO_" SYMSTR(ver_sym));

#if defined(__i386__)
#include "syscall-i386.h"
#elif defined(__x86_64__)
#include "syscall-x86_64.h"
#elif defined(__ia64__)
#include "syscall-ia64.h"
#elif defined(__PPC__)
#include "syscall-ppc.h"
#elif defined(__s390__)
#include "syscall-s390.h"
#elif defined(__alpha__)
#include "syscall-alpha.h"
#elif defined(__arm__)
#include "syscall-arm.h"
#elif defined(__m68k__)
#include "syscall-m68k.h"
#elif defined(__sparc__) && defined(__arch64__)
#include "syscall-sparc64.h"
#elif defined(__sparc__)
#include "syscall-sparc.h"
#elif defined(__hppa__)
#include "syscall-parisc.h"
#elif defined(__mips__)
#include "syscall-mips.h"
#elif defined(__sh__)
#include "syscall-sh.h"
#elif defined(__aarch64__)
#include "syscall-arm64.h"
#else
#error "add syscall-arch.h"
#endif
