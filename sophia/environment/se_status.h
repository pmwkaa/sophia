#ifndef SE_STATUS_H_
#define SE_STATUS_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

enum {
	SE_OFFLINE,
	SE_ONLINE,
	SE_RECOVER,
	SE_SHUTDOWN,
	SE_MALFUNCTION
};

typedef struct sestatus sestatus;

struct sestatus {
	int status;
	ssspinlock lock;
};

static inline void
se_statusinit(sestatus *s)
{
	s->status = SE_OFFLINE;
	ss_spinlockinit(&s->lock);
}

static inline void
se_statusfree(sestatus *s)
{
	ss_spinlockfree(&s->lock);
}

static inline void
se_statuslock(sestatus *s) {
	ss_spinlock(&s->lock);
}

static inline void
se_statusunlock(sestatus *s) {
	ss_spinunlock(&s->lock);
}

static inline int
se_statusset(sestatus *s, int status)
{
	ss_spinlock(&s->lock);
	int old = s->status;
	s->status = status;
	ss_spinunlock(&s->lock);
	return old;
}

static inline int
se_status(sestatus *s)
{
	ss_spinlock(&s->lock);
	int status = s->status;
	ss_spinunlock(&s->lock);
	return status;
}

static inline char*
se_statusof(sestatus *s)
{
	int status = se_status(s);
	switch (status) {
	case SE_OFFLINE:     return "offline";
	case SE_ONLINE:      return "online";
	case SE_RECOVER:     return "recover";
	case SE_SHUTDOWN:    return "shutdown";
	case SE_MALFUNCTION: return "malfunction";
	}
	assert(0);
	return NULL;
}

static inline int
se_statusactive_is(int status) {
	switch (status) {
	case SE_ONLINE:
	case SE_RECOVER:
		return 1;
	case SE_SHUTDOWN:
	case SE_OFFLINE:
	case SE_MALFUNCTION:
		return 0;
	}
	assert(0);
	return 0;
}

static inline int
se_statusactive(sestatus *s) {
	return se_statusactive_is(se_status(s));
}

static inline int
se_online(sestatus *s) {
	return se_status(s) == SE_ONLINE;
}

#endif
