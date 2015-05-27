#ifndef SO_STATUS_H_
#define SO_STATUS_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

enum {
	SO_OFFLINE,
	SO_ONLINE,
	SO_RECOVER,
	SO_SHUTDOWN,
	SO_MALFUNCTION
};

typedef struct sostatus sostatus;

struct sostatus {
	int status;
	ssspinlock lock;
};

static inline void
so_statusinit(sostatus *s)
{
	s->status = SO_OFFLINE;
	ss_spinlockinit(&s->lock);
}

static inline void
so_statusfree(sostatus *s)
{
	ss_spinlockfree(&s->lock);
}

static inline void
so_statuslock(sostatus *s) {
	ss_spinlock(&s->lock);
}

static inline void
so_statusunlock(sostatus *s) {
	ss_spinunlock(&s->lock);
}

static inline int
so_statusset(sostatus *s, int status)
{
	ss_spinlock(&s->lock);
	int old = s->status;
	if (old == SO_MALFUNCTION) {
		ss_spinunlock(&s->lock);
		return -1;
	}
	s->status = status;
	ss_spinunlock(&s->lock);
	return old;
}

static inline int
so_status(sostatus *s)
{
	ss_spinlock(&s->lock);
	int status = s->status;
	ss_spinunlock(&s->lock);
	return status;
}

static inline char*
so_statusof(sostatus *s)
{
	int status = so_status(s);
	switch (status) {
	case SO_OFFLINE:     return "offline";
	case SO_ONLINE:      return "online";
	case SO_RECOVER:     return "recover";
	case SO_SHUTDOWN:    return "shutdown";
	case SO_MALFUNCTION: return "malfunction";
	}
	assert(0);
	return NULL;
}

static inline int
so_statusactive_is(int status) {
	switch (status) {
	case SO_ONLINE:
	case SO_RECOVER:
		return 1;
	case SO_SHUTDOWN:
	case SO_OFFLINE:
	case SO_MALFUNCTION:
		return 0;
	}
	assert(0);
	return 0;
}

static inline int
so_statusactive(sostatus *s) {
	return so_statusactive_is(so_status(s));
}

static inline int
so_online(sostatus *s) {
	return so_status(s) == SO_ONLINE;
}

#endif
