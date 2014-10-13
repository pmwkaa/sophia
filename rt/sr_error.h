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
	SR_ERROR_NONE = 0,
	SR_ERROR = 1,
	SR_ERROR_RECOVERABLE = 2
};

struct srerror {
	srspinlock lock;
	int status;
	const char *file;
	const char *function;
	int line;
	char error[256];
};

static inline void
sr_errorinit(srerror *e) {
	e->status = SR_ERROR_NONE;
	e->error[0] = 0;
	e->line = 0;
	e->function = NULL;
	e->file = NULL;
	sr_spinlockinit(&e->lock);
}

static inline void
sr_errorfree(srerror *e) {
	sr_spinlockfree(&e->lock);
}

static inline void
sr_errorreset(srerror *e) {
	sr_spinlock(&e->lock);
	if (! (e->status & SR_ERROR_RECOVERABLE)) {
		sr_spinunlock(&e->lock);
		return;
	}
	e->status = SR_ERROR_NONE;
	e->error[0] = 0;
	sr_spinunlock(&e->lock);
}

static inline int
sr_erroris(srerror *e) {
	sr_spinlock(&e->lock);
	int status = e->status;
	sr_spinunlock(&e->lock);
	return status & SR_ERROR;
}

static inline int
sr_erroris_recoverable(srerror *e) {
	sr_spinlock(&e->lock);
	int status = e->status;
	sr_spinunlock(&e->lock);
	return status & SR_ERROR_RECOVERABLE;
}

static inline int
sr_errorcopy(srerror *e, char *buf, int bufsize) {
	sr_spinlock(&e->lock);
	int len = snprintf(buf, bufsize, "%s", e->error);
	sr_spinunlock(&e->lock);
	return len;
}

static inline void
sr_verrorset(srerror *e,
             const char *file,
             const char *function, int line, int status,
             char *fmt, va_list args)
{
	sr_spinlock(&e->lock);
	if (srunlikely(e->status & SR_ERROR)) {
		sr_spinunlock(&e->lock);
		return;
	}
	e->file     = file;
	e->function = function;
	e->line     = line;
	e->status   = status;
	int len;
	len = snprintf(e->error, sizeof(e->error), "%s:%d ", file, line);
	vsnprintf(e->error + len, sizeof(e->error) - len, fmt, args);
	sr_spinunlock(&e->lock);
}

static inline int
sr_errorset(srerror *e,
            const char *file,
            const char *function, int line,
            char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	sr_verrorset(e, file, function, line, SR_ERROR, fmt, args);
	va_end(args);
	return -1;
}

static inline int
sr_error_recoverable(srerror *e) {
	sr_spinlock(&e->lock);
	assert(e->status & SR_ERROR);
	e->status |= SR_ERROR_RECOVERABLE;
	sr_spinunlock(&e->lock);
	return -1;
}

#define sr_error(e, fmt, ...) \
	sr_errorset(e, __FILE__, __FUNCTION__, __LINE__, __VA_ARGS__)

#endif
