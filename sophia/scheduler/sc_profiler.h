#ifndef SC_PROFILER_H_
#define SC_PROFILER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct scprofiler scprofiler;

struct scprofiler {
	scdb state;
};

static inline void
sc_profiler(sc *s, scprofiler *p, si *index)
{
	if (s->count == 0) {
		memset(&p->state, 0, sizeof(p->state));
		return;
	}
	ss_mutexlock(&s->lock);
	scdb *db = sc_of(s, index);
	p->state = *db;
	ss_mutexunlock(&s->lock);
}

#endif
