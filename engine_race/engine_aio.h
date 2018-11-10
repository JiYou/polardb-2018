#pragma once

// NOTE: from #include <linux/aio_abi.h>

// below struct is mainly defined in linux header files.
// in-order to use without install linux-headers package.
// Her just define these struct to call the
// linux-system-call to avoid the platform compile
// failed with struct not defined.
// wrapper for the linux system-call API.

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>
#include <string.h>

struct timespec;
struct sockaddr;
struct iovec;

typedef struct io_context *io_context_t;

typedef enum io_iocb_cmd {
	IO_CMD_PREAD = 0,
	IO_CMD_PWRITE = 1,

	IO_CMD_FSYNC = 2,
	IO_CMD_FDSYNC = 3,

	IO_CMD_POLL = 5,
	IO_CMD_NOOP = 6,
	IO_CMD_PREADV = 7,
	IO_CMD_PWRITEV = 8,
} io_iocb_cmd_t;

struct io_iocb_poll {
	int events, __pad1;
};

struct io_iocb_sockaddr {
	struct sockaddr *addr;
	int		len;
};

struct io_iocb_common {
  void *buf;
  unsigned long nbytes;
	long long	offset;
	long long	__pad3;
	unsigned	flags;
	unsigned	resfd;
};

struct io_iocb_vector {
	const struct iovec	*vec;
	int			nr;
	long long		offset;
};

struct iocb {
  void *data;
	unsigned key, __pad2;

	short		aio_lio_opcode;
	short		aio_reqprio;
	int		aio_fildes;

	union {
		struct io_iocb_common		c;
		struct io_iocb_vector		v;
		struct io_iocb_poll		poll;
		struct io_iocb_sockaddr	saddr;
	} u;
};

struct io_event {
  void *data;
  struct iocb *obj;
  unsigned long res;
  unsigned long res2;
};


typedef void (*io_callback_t)(io_context_t ctx, struct iocb *iocb, long res, long res2);

/* Linux syscalls */
extern int io_setup(int maxevents, io_context_t *ctxp);
extern int io_destroy(io_context_t ctx);
extern int io_submit(io_context_t ctx, long nr, struct iocb *ios[]);
extern int io_cancel(io_context_t ctx, struct iocb *iocb, struct io_event *evt);
extern int io_getevents(io_context_t ctx_id, long min_nr, long nr, struct io_event *events, struct timespec *timeout);


#ifdef __cplusplus
}
#endif
