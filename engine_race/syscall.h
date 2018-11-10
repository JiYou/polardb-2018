#include <sys/syscall.h>
#include <unistd.h>

#define _SYMSTR(str)	#str
#define SYMSTR(str)	_SYMSTR(str)

#define SYMVER(compat_sym, orig_sym, ver_sym)	\
	__asm__(".symver " SYMSTR(compat_sym) "," SYMSTR(orig_sym) "@LIBAIO_" SYMSTR(ver_sym));

#define DEFSYMVER(compat_sym, orig_sym, ver_sym)	\
	__asm__(".symver " SYMSTR(compat_sym) "," SYMSTR(orig_sym) "@@LIBAIO_" SYMSTR(ver_sym));

#include "syscall-x86_64.h"
