#ifndef SR_ERROR_H_
#define SR_ERROR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srerror srerror;

enum {
	SR_ERROR_NONE,
	SR_ERROR,
	SR_ERROR_FATAL
};

struct srerror {
	srspinlock lock;
	int status;
	char error[256];
};

static inline void
sr_errorinit(srerror *e) {
	e->status = SR_ERROR_NONE;
	e->error[0] = 0;
	sr_spinlockinit(&e->lock);
}

static inline void
sr_errorfree(srerror *e) {
	sr_spinlockfree(&e->lock);
}

static inline void
sr_erroreset(srerror *e) {
	sr_spinlock(&e->lock);
	assert(e->status != SR_ERROR_FATAL);
	e->status = SR_ERROR_NONE;
	e->error[0] = 0;
	sr_spinunlock(&e->lock);
}

static inline int
sr_errorstatus(srerror *e) {
	sr_spinlock(&e->lock);
	int status = e->status;
	sr_spinunlock(&e->lock);
	return status;
}

static inline int
sr_errorcopy(srerror *e, char *buf, int bufsize) {
	sr_spinlock(&e->lock);
	int len = snprintf(buf, bufsize, "%s", e->error);
	sr_spinunlock(&e->lock);
	return len;
}

static inline void
sr_verror(srerror *e, int status, char *fmt, va_list args)
{
	sr_spinlock(&e->lock);
	if (srunlikely(e->status == SR_ERROR_FATAL)) {
		sr_spinunlock(&e->lock);
		return;
	}
	e->status = status;
	vsnprintf(e->error, sizeof(e->error), fmt, args);
	sr_spinunlock(&e->lock);
}

static inline int
sr_error(srerror *e, char *fmt, ...) {
	va_list args;
	va_start(args, fmt);
	sr_verror(e, SR_ERROR, fmt, args);
	va_end(args);
	return -1;
}

static inline int
sr_errorfatal(srerror *e, char *fmt, ...) {
	va_list args;
	va_start(args, fmt);
	sr_verror(e, SR_ERROR_FATAL, fmt, args);
	va_end(args);
	return -1;
}

#endif
