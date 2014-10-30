#ifndef SR_TRACE_H_
#define SR_TRACE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srtrace srtrace;

struct srtrace {
	srspinlock lock;
	const char *file;
	const char *function;
	int line;
	char message[100];
};

static inline void
sr_traceinit(srtrace *t) {
	sr_spinlockinit(&t->lock);
	t->message[0] = 0;
	t->line = 0;
	t->function = NULL;
	t->file = NULL;
}

static inline void
sr_tracefree(srtrace *t) {
	sr_spinlockfree(&t->lock);
}

static inline void
sr_vtrace(srtrace *t,
          const char *file,
          const char *function, int line,
          char *fmt, va_list args)
{
	sr_spinlock(&t->lock);
	t->file     = file;
	t->function = function;
	t->line     = line;
	vsnprintf(t->message, sizeof(t->message), fmt, args);
	sr_spinunlock(&t->lock);
}

static inline int
sr_traceset(srtrace *t,
            const char *file,
            const char *function, int line,
            char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	sr_vtrace(t, file, function, line, fmt, args);
	va_end(args);
	return -1;
}

#define sr_trace(t, fmt, ...) \
	sr_traceset(t, __FILE__, __FUNCTION__, __LINE__, fmt, __VA_ARGS__)

#endif
