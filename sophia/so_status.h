#ifndef SO_STATUS_H_
#define SO_STATUS_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef enum {
	SO_OFFLINE,
	SO_ONLINE,
	SO_RECOVER,
	SO_SHUTDOWN,
	SO_MALFUNCTION
} sostatus;

static inline int
so_statusactive(sostatus s) {
	return s != SO_OFFLINE && s != SO_SHUTDOWN;
}

static inline char*
so_statusof(sostatus s)
{
	switch (s) {
	case SO_OFFLINE:     return "offline";
	case SO_ONLINE:      return "offline";
	case SO_RECOVER:     return "recover";
	case SO_SHUTDOWN:    return "shutdown";
	case SO_MALFUNCTION: return "malfunction";
	}
	assert(0);
	return NULL;
}

#endif
