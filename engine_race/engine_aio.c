#include "engine_aio.h"
#include "syscall.h"
#include <errno.h>
#include <stdlib.h>
#include <time.h>

io_syscall3(int, io_cancel, io_cancel, io_context_t, ctx, struct iocb *, iocb, struct io_event *, event)
io_syscall1(int, io_destroy, io_destroy, io_context_t, ctx)
io_syscall5(int, io_getevents, io_getevents, io_context_t, ctx, long, min_nr, long, nr, struct io_event *, events, struct timespec *, timeout)
io_syscall2(int, io_setup, io_setup, int, maxevents, io_context_t *, ctxp)
io_syscall3(int, io_submit, io_submit, io_context_t, ctx, long, nr, struct iocb **, iocbs)
