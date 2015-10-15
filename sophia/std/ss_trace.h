#ifndef SS_TRACE_H_
#define SS_TRACE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sstrace sstrace;

struct sstrace {
	ssspinlock lock;
	const char *file;
	const char *function;
	int line;
	char message[100];
};

static inline void
ss_traceinit(sstrace *t) {
	ss_spinlockinit(&t->lock);
	t->message[0] = 0;
	t->line = 0;
	t->function = NULL;
	t->file = NULL;
}

static inline void
ss_tracefree(sstrace *t) {
	ss_spinlockfree(&t->lock);
}

static inline int
ss_tracecopy(sstrace *t, char *buf, int bufsize) {
	ss_spinlock(&t->lock);
	int len = snprintf(buf, bufsize, "%s", t->message);
	ss_spinunlock(&t->lock);
	return len;
}

static inline void
ss_vtrace(sstrace *t,
          const char *file,
          const char *function, int line,
          char *fmt, va_list args)
{
	ss_spinlock(&t->lock);
	t->file     = file;
	t->function = function;
	t->line     = line;
	vsnprintf(t->message, sizeof(t->message), fmt, args);
	ss_spinunlock(&t->lock);
}

static inline int
ss_traceset(sstrace *t,
            const char *file,
            const char *function, int line,
            char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	ss_vtrace(t, file, function, line, fmt, args);
	va_end(args);
	return -1;
}

#define ss_trace(t, fmt, ...) \
	ss_traceset(t, __FILE__, __func__, __LINE__, fmt, __VA_ARGS__)

#endif
