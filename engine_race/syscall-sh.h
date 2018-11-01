/* Copy from ./arch/sh/include/asm/unistd_32.h */
#define __NR_io_setup       245
#define __NR_io_destroy     246
#define __NR_io_getevents   247
#define __NR_io_submit      248
#define __NR_io_cancel      249

#define io_syscall1(type,fname,sname,type1,arg1) \
type fname(type1 arg1) \
{ \
register long __sc0 __asm__ ("r3") = __NR_##sname; \
register long __sc4 __asm__ ("r4") = (long) arg1; \
__asm__ __volatile__ ("trapa    #0x11" \
	: "=z" (__sc0) \
	: "0" (__sc0), "r" (__sc4) \
	: "memory"); \
	return (type) __sc0;\
}

#define io_syscall2(type,fname,sname,type1,arg1,type2,arg2) \
type fname(type1 arg1,type2 arg2) \
{ \
register long __sc0 __asm__ ("r3") = __NR_##sname; \
register long __sc4 __asm__ ("r4") = (long) arg1; \
register long __sc5 __asm__ ("r5") = (long) arg2; \
	__asm__ __volatile__ ("trapa    #0x12" \
	: "=z" (__sc0) \
	: "0" (__sc0), "r" (__sc4), "r" (__sc5) \
	: "memory"); \
	return (type) __sc0;\
}

#define io_syscall3(type,fname,sname,type1,arg1,type2,arg2,type3,arg3) \
type fname(type1 arg1,type2 arg2,type3 arg3) \
{ \
register long __sc0 __asm__ ("r3") = __NR_##sname; \
register long __sc4 __asm__ ("r4") = (long) arg1; \
register long __sc5 __asm__ ("r5") = (long) arg2; \
register long __sc6 __asm__ ("r6") = (long) arg3; \
	__asm__ __volatile__ ("trapa    #0x13" \
	: "=z" (__sc0) \
	: "0" (__sc0), "r" (__sc4), "r" (__sc5), "r" (__sc6) \
	: "memory"); \
	return (type) __sc0;\
}

#define io_syscall4(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4) \
type fname(type1 arg1, type2 arg2, type3 arg3, type4 arg4) \
{ \
register long __sc0 __asm__ ("r3") = __NR_##sname; \
register long __sc4 __asm__ ("r4") = (long) arg1; \
register long __sc5 __asm__ ("r5") = (long) arg2; \
register long __sc6 __asm__ ("r6") = (long) arg3; \
register long __sc7 __asm__ ("r7") = (long) arg4; \
__asm__ __volatile__ ("trapa    #0x14" \
	: "=z" (__sc0) \
	: "0" (__sc0), "r" (__sc4), "r" (__sc5), "r" (__sc6),  \
	"r" (__sc7) \
	: "memory" ); \
	return (type) __sc0;\
}

#define io_syscall5(type,fname,sname,type1,arg1,type2,arg2,type3,arg3,type4,arg4,type5,arg5) \
type fname(type1 arg1, type2 arg2, type3 arg3, type4 arg4, type5 arg5) \
{ \
register long __sc3 __asm__ ("r3") = __NR_##sname; \
register long __sc4 __asm__ ("r4") = (long) arg1; \
register long __sc5 __asm__ ("r5") = (long) arg2; \
register long __sc6 __asm__ ("r6") = (long) arg3; \
register long __sc7 __asm__ ("r7") = (long) arg4; \
register long __sc0 __asm__ ("r0") = (long) arg5; \
__asm__ __volatile__ ("trapa    #0x15" \
	: "=z" (__sc0) \
	: "0" (__sc0), "r" (__sc4), "r" (__sc5), "r" (__sc6), "r" (__sc7),  \
	"r" (__sc3) \
	: "memory" ); \
	return (type) __sc0;\
}
