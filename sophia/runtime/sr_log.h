#ifndef SR_LOG_H_
#define SR_LOG_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srlog srlog;

typedef void (*srlogcbf)(char*, void*);

struct srlog {
	srlogcbf log;
	void *arg;
};

static inline void
sr_loginit(srlog *log)
{
	log->log = NULL;
	log->arg = NULL;
}

static inline void
sr_log(srlog *log, char *fmt, ...)
{
	if (log->log == NULL)
		return;
	char message[1024];
	va_list args;
	va_start(args, fmt);
	vsnprintf(message, sizeof(message), fmt, args);
	va_end(args);
	log->log(message, log->arg);
}

#endif
