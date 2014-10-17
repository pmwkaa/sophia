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
	srspinlock lock;
};

static inline void
so_statusinit(sostatus *s)
{
	s->status = SO_OFFLINE;
	sr_spinlockinit(&s->lock);
}

static inline void
so_statusfree(sostatus *s)
{
	sr_spinlockfree(&s->lock);
}

static inline void
so_statuslock(sostatus *s) {
	sr_spinlock(&s->lock);
}

static inline void
so_statusunlock(sostatus *s) {
	sr_spinunlock(&s->lock);
}

static inline int
so_statusset(sostatus *s, int status)
{
	sr_spinlock(&s->lock);
	int old = s->status;
	if (old == SO_MALFUNCTION) {
		sr_spinunlock(&s->lock);
		return -1;
	}
	s->status = status;
	sr_spinunlock(&s->lock);
	return old;
}

static inline int
so_status(sostatus *s)
{
	sr_spinlock(&s->lock);
	int status = s->status;
	sr_spinunlock(&s->lock);
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
	case SO_OFFLINE:
	case SO_SHUTDOWN:
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

#endif
