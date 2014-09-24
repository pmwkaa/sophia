
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

typedef struct srbufiter srbufiter;

struct srbufiter {
	srbuf *buf;
	int vsize;
	void *v;
} srpacked;

static void
sr_bufiter_init(sriter *i)
{
	assert(sizeof(srbufiter) <= sizeof(i->priv));
	srbufiter *bi = (srbufiter*)i->priv;
	memset(bi, 0, sizeof(*bi));
}

static int
sr_bufiter_open(sriter *i, va_list args)
{
	srbufiter *bi = (srbufiter*)i->priv;
	bi->buf = va_arg(args, srbuf*);
	bi->vsize = va_arg(args, int);
	bi->v = bi->buf->s;
	if (srunlikely(bi->v == NULL))
		return 0;
	if (srunlikely(! sr_bufin(bi->buf, bi->v))) {
		bi->v = NULL;
		return 0;
	}
	return 1;
}

static void
sr_bufiter_close(sriter *i srunused)
{ }

static int
sr_bufiter_has(sriter *i)
{
	srbufiter *bi = (srbufiter*)i->priv;
	return bi->v != NULL;
}

static void*
sr_bufiter_of(sriter *i)
{
	srbufiter *bi = (srbufiter*)i->priv;
	return bi->v;
}

static void*
sr_bufiter_of_ref(sriter *i)
{
	srbufiter *bi = (srbufiter*)i->priv;
	if (srunlikely(bi->v == NULL))
		return NULL;
	return *(void**)bi->v;
}

static void
sr_bufiter_next(sriter *i)
{
	srbufiter *bi = (srbufiter*)i->priv;
	if (srunlikely(bi->v == NULL))
		return;
	bi->v = (char*)bi->v + bi->vsize;
	if (srunlikely(! sr_bufin(bi->buf, bi->v)))
		bi->v = NULL;
}

sriterif sr_bufiter =
{
	.init    = sr_bufiter_init,
	.open    = sr_bufiter_open,
	.close   = sr_bufiter_close,
	.has     = sr_bufiter_has,
	.of      = sr_bufiter_of,
	.next    = sr_bufiter_next
};

sriterif sr_bufiterref =
{
	.init    = sr_bufiter_init,
	.open    = sr_bufiter_open,
	.close   = sr_bufiter_close,
	.has     = sr_bufiter_has,
	.of      = sr_bufiter_of_ref,
	.next    = sr_bufiter_next
};
