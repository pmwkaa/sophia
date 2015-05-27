#ifndef SS_BUFITER_H_
#define SS_BUFITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

extern ssiterif ss_bufiter;
extern ssiterif ss_bufiterref;

typedef struct ssbufiter ssbufiter;

struct ssbufiter {
	ssbuf *buf;
	int vsize;
	void *v;
} sspacked;

static inline int
ss_bufiter_open(ssiter *i, ssbuf *buf, int vsize)
{
	ssbufiter *bi = (ssbufiter*)i->priv;
	bi->buf = buf;
	bi->vsize = vsize;
	bi->v = bi->buf->s;
	if (ssunlikely(bi->v == NULL))
		return 0;
	if (ssunlikely(! ss_bufin(bi->buf, bi->v))) {
		bi->v = NULL;
		return 0;
	}
	return 1;
}

static inline void
ss_bufiter_close(ssiter *i ssunused)
{ }

static inline int
ss_bufiter_has(ssiter *i)
{
	ssbufiter *bi = (ssbufiter*)i->priv;
	return bi->v != NULL;
}

static inline void*
ss_bufiter_of(ssiter *i)
{
	ssbufiter *bi = (ssbufiter*)i->priv;
	return bi->v;
}

static inline void
ss_bufiter_next(ssiter *i)
{
	ssbufiter *bi = (ssbufiter*)i->priv;
	if (ssunlikely(bi->v == NULL))
		return;
	bi->v = (char*)bi->v + bi->vsize;
	if (ssunlikely(! ss_bufin(bi->buf, bi->v)))
		bi->v = NULL;
}

static inline int
ss_bufiterref_open(ssiter *i, ssbuf *buf, int vsize) {
	return ss_bufiter_open(i, buf, vsize);
}

static inline void
ss_bufiterref_close(ssiter *i ssunused)
{ }

static inline int
ss_bufiterref_has(ssiter *i) {
	return ss_bufiter_has(i);
}

static inline void*
ss_bufiterref_of(ssiter *i)
{
	ssbufiter *bi = (ssbufiter*)i->priv;
	if (ssunlikely(bi->v == NULL))
		return NULL;
	return *(void**)bi->v;
}

static inline void
ss_bufiterref_next(ssiter *i) {
	ss_bufiter_next(i);
}

#endif
