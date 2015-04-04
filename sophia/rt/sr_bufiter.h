#ifndef SR_BUFITER_H_
#define SR_BUFITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

extern sriterif sr_bufiter;
extern sriterif sr_bufiterref;

typedef struct srbufiter srbufiter;

struct srbufiter {
	srbuf *buf;
	int vsize;
	void *v;
} srpacked;

static inline int
sr_bufiter_open(sriter *i, srbuf *buf, int vsize)
{
	srbufiter *bi = (srbufiter*)i->priv;
	bi->buf = buf;
	bi->vsize = vsize;
	bi->v = bi->buf->s;
	if (srunlikely(bi->v == NULL))
		return 0;
	if (srunlikely(! sr_bufin(bi->buf, bi->v))) {
		bi->v = NULL;
		return 0;
	}
	return 1;
}

static inline void
sr_bufiter_close(sriter *i srunused)
{ }

static inline int
sr_bufiter_has(sriter *i)
{
	srbufiter *bi = (srbufiter*)i->priv;
	return bi->v != NULL;
}

static inline void*
sr_bufiter_of(sriter *i)
{
	srbufiter *bi = (srbufiter*)i->priv;
	return bi->v;
}

static inline void
sr_bufiter_next(sriter *i)
{
	srbufiter *bi = (srbufiter*)i->priv;
	if (srunlikely(bi->v == NULL))
		return;
	bi->v = (char*)bi->v + bi->vsize;
	if (srunlikely(! sr_bufin(bi->buf, bi->v)))
		bi->v = NULL;
}

static inline int
sr_bufiterref_open(sriter *i, srbuf *buf, int vsize) {
	return sr_bufiter_open(i, buf, vsize);
}

static inline void
sr_bufiterref_close(sriter *i srunused)
{ }

static inline int
sr_bufiterref_has(sriter *i) {
	return sr_bufiter_has(i);
}

static inline void*
sr_bufiterref_of(sriter *i)
{
	srbufiter *bi = (srbufiter*)i->priv;
	if (srunlikely(bi->v == NULL))
		return NULL;
	return *(void**)bi->v;
}

static inline void
sr_bufiterref_next(sriter *i) {
	sr_bufiter_next(i);
}

#endif
