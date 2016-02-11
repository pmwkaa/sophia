#ifndef SR_STATUS_H_
#define SR_STATUS_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

enum {
	SR_OFFLINE,
	SR_ONLINE,
	SR_RECOVER,
	SR_SHUTDOWN,
	SR_DROP,
	SR_MALFUNCTION
};

typedef struct srstatus srstatus;

struct srstatus {
	int status;
	ssspinlock lock;
};

static inline void
sr_statusinit(srstatus *s)
{
	s->status = SR_OFFLINE;
	ss_spinlockinit(&s->lock);
}

static inline void
sr_statusfree(srstatus *s)
{
	ss_spinlockfree(&s->lock);
}

static inline void
sr_statuslock(srstatus *s) {
	ss_spinlock(&s->lock);
}

static inline void
sr_statusunlock(srstatus *s) {
	ss_spinunlock(&s->lock);
}

static inline int
sr_statusset(srstatus *s, int status)
{
	ss_spinlock(&s->lock);
	int old = s->status;
	s->status = status;
	ss_spinunlock(&s->lock);
	return old;
}

static inline int
sr_status(srstatus *s)
{
	ss_spinlock(&s->lock);
	int status = s->status;
	ss_spinunlock(&s->lock);
	return status;
}

static inline char*
sr_statusof(srstatus *s)
{
	int status = sr_status(s);
	switch (status) {
	case SR_OFFLINE:     return "offline";
	case SR_ONLINE:      return "online";
	case SR_RECOVER:     return "recover";
	case SR_SHUTDOWN:    return "shutdown";
	case SR_DROP:        return "drop";
	case SR_MALFUNCTION: return "malfunction";
	}
	assert(0);
	return NULL;
}

static inline int
sr_statusactive_is(int status)
{
	switch (status) {
	case SR_ONLINE:
	case SR_RECOVER:
		return 1;
	case SR_SHUTDOWN:
	case SR_DROP:
	case SR_OFFLINE:
	case SR_MALFUNCTION:
		return 0;
	}
	assert(0);
	return 0;
}

static inline int
sr_statusactive(srstatus *s) {
	return sr_statusactive_is(sr_status(s));
}

static inline int
sr_online(srstatus *s) {
	return sr_status(s) == SR_ONLINE;
}

#endif
