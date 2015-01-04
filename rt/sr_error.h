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
	SR_ERROR_NONE  = 0,
	SR_ERROR = 1,
	SR_ERROR_MALFUNCTION = 2
};

struct srerror {
	srspinlock lock;
	int type;
	const char *file;
	const char *function;
	int line;
	char error[256];
};

static inline void
sr_errorinit(srerror *e) {
	e->type = SR_ERROR_NONE;
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
	e->type = SR_ERROR_NONE;
	e->error[0] = 0;
	e->line = 0;
	e->function = NULL;
	e->file = NULL;
	sr_spinunlock(&e->lock);
}

static inline void
sr_errorrecover(srerror *e) {
	sr_spinlock(&e->lock);
	assert(e->type == SR_ERROR_MALFUNCTION);
	e->type = SR_ERROR;
	sr_spinunlock(&e->lock);
}

static inline int
sr_errorof(srerror *e) {
	sr_spinlock(&e->lock);
	int type = e->type;
	sr_spinunlock(&e->lock);
	return type;
}

static inline int
sr_errorcopy(srerror *e, char *buf, int bufsize) {
	sr_spinlock(&e->lock);
	int len = snprintf(buf, bufsize, "%s", e->error);
	sr_spinunlock(&e->lock);
	return len;
}

static inline void
sr_verrorset(srerror *e, int type,
             const char *file,
             const char *function, int line,
             char *fmt, va_list args)
{
	sr_spinlock(&e->lock);
	if (srunlikely(e->type == SR_ERROR_MALFUNCTION)) {
		sr_spinunlock(&e->lock);
		return;
	}
	e->file     = file;
	e->function = function;
	e->line     = line;
	e->type     = type;
	int len;
	len = snprintf(e->error, sizeof(e->error), "%s:%d ", file, line);
	vsnprintf(e->error + len, sizeof(e->error) - len, fmt, args);
	sr_spinunlock(&e->lock);
}

static inline int
sr_errorset(srerror *e, int type,
            const char *file,
            const char *function, int line,
            char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	sr_verrorset(e, type, file, function, line, fmt, args);
	va_end(args);
	return -1;
}

#define sr_malfunction(e, fmt, ...) \
	sr_errorset(e, SR_ERROR_MALFUNCTION, __FILE__, __FUNCTION__, \
	            __LINE__, fmt, __VA_ARGS__)

#define sr_error(e, fmt, ...) \
	sr_errorset(e, SR_ERROR, __FILE__, __FUNCTION__, __LINE__, fmt, __VA_ARGS__)

#endif
