
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>

typedef struct sliter sliter;

struct sliter {
	int validate;
	int error;
	srfile *log;
	srmap map;
	slv *begin;
	slv *v;
	uint32_t pos;
	sv current;
} srpacked;

static void
sl_iterinit(sriter *i)
{
	assert(sizeof(sliter) <= sizeof(i->priv));
	sliter *li = (sliter*)i->priv;
	memset(li, 0, sizeof(*li));
}

static void
sl_iterseterror(sliter *i)
{
	i->error = 1;
	i->begin = NULL;
	i->v     = NULL;
}

static inline int
sl_iterset(sriter *it, slv *v)
{
	sliter *i = (sliter*)it->priv;
	if (srunlikely((char*)v >= ((char*)i->map.p + i->map.size))) {
		sr_error(it->r->e, "corrupted log file '%s': bad record size",
		         i->log->file);
		sl_iterseterror(i);
		return -1;
	}
	if (i->validate) {
		char *end = (char*)v + v->keysize + v->valuesize;
		if (srunlikely(end > ((char*)i->map.p + i->map.size))) {
			sr_error(it->r->e, "corrupted log file '%s': bad record size",
			         i->log->file);
			sl_iterseterror(i);
			return -1;
		}
		uint32_t crc;
		crc = sr_crcp((char*)v + sizeof(slv), v->keysize, 0);
		crc = sr_crcp((char*)v + sizeof(slv) + v->keysize, v->valuesize, crc);
		crc = sr_crcs(v, sizeof(slv), crc);
		if (srunlikely(crc != v->crc)) {
			sl_iterseterror(i);
			return -1;
		}
	}
	svinit(&i->current, &sl_vif, v, NULL);
	i->v = v;
	return 0;
}

static inline int
sl_iterprepare(sriter *it)
{
	sliter *i = (sliter*)it->priv;
	srversion *ver = (srversion*)i->map.p;
	if (! sr_versioncheck(ver))
		return sr_error(it->r->e, "bad log file '%s' version",
		                i->log->file);
	if (srunlikely(i->log->size < (sizeof(srversion) + sizeof(slv)))) {
		sr_error(it->r->e, "corrupted log file '%s': bad size",
		         i->log->file);
		return -1;
	}
	i->begin = (slv*)((char*)i->map.p + sizeof(srversion));
	if (srunlikely( !(i->begin->flags & SVBEGIN))) {
		sr_error(it->r->e, "corrupted log file '%s': bad record flags",
		         i->log->file);
		return -1;
	}
	if (i->begin->valuesize == 0) {
		sr_error(it->r->e, "corrupted log file '%s': bad record size",
		         i->log->file);
		return -1;
	}
	if (i->validate) {
		uint32_t crc = sr_crcs(i->begin, sizeof(slv), 0);
		if (srunlikely(crc != i->begin->crc)) {
			sr_error(it->r->e, "corrupted log file '%s': bad record crc",
			         i->log->file);
			sl_iterseterror(i);
			return -1;
		}
	}
	slv *v = (slv*)((char*)i->begin + sizeof(slv));
	return sl_iterset(it, v);
}

static int
sl_iteropen(sriter *i, va_list args)
{
	sliter *li = (sliter*)i->priv;
	li->log      = va_arg(args, srfile*);
	li->validate = va_arg(args, int);
	li->v        = NULL;
	li->begin    = NULL;
	li->pos      = 0;
	if (srunlikely(li->log->size < sizeof(srversion))) {
		sr_error(i->r->e, "corrupted log file '%s': bad size",
		         li->log->file);
		return -1;
	}
	if (srunlikely(li->log->size == sizeof(srversion)))
		return 0;
	int rc = sr_map(&li->map, li->log->fd, li->log->size, 1);
	if (srunlikely(rc == -1)) {
		sr_error(i->r->e, "failed to mmap log file '%s': %s",
		         li->log->file, strerror(errno));
		return -1;
	}
	rc = sl_iterprepare(i);
	if (srunlikely(rc == -1))
		sr_mapunmap(&li->map);
	return 0;
}

static void
sl_iterclose(sriter *i srunused)
{
	sliter *li = (sliter*)i->priv;
	sr_mapunmap(&li->map);
}

static int
sl_iterhas(sriter *i)
{
	sliter *li = (sliter*)i->priv;
	return li->v != NULL;
}

static void*
sl_iterof(sriter *i)
{
	sliter *li = (sliter*)i->priv;
	if (srunlikely(li->v == NULL))
		return NULL;
	return &li->current;
}

static void
sl_iternext(sriter *i)
{
	sliter *li = (sliter*)i->priv;
	if (srunlikely(li->begin == NULL))
		return;
	if (srunlikely(li->v == NULL))
		return;
	li->pos++;

	slv *next =
		(slv*)((char*)li->v + sizeof(slv) +
	           li->v->keysize +
	           li->v->valuesize);
	if (srunlikely((char*)next >= ((char*)li->map.p + li->map.size))) {
		/* eof */
		if (li->pos != li->begin->valuesize) {
			sr_error(i->r->e, "corrupted log file '%s': bad record size",
			         li->log->file);
			sl_iterseterror(li);
			return;
		}
		li->v = NULL;
		li->begin = NULL;
		return;
	}

	if (srunlikely(next->flags & SVBEGIN)) {
		if (li->pos != li->begin->valuesize) {
			sr_error(i->r->e, "corrupted log file '%s': bad record size",
			         li->log->file);
			sl_iterseterror(li);
			return;
		}
		li->begin = next;
		li->v     = NULL;
		li->pos   = 0;
		if (li->validate) {
			uint32_t crc = sr_crcs(li->begin, sizeof(slv), 0);
			if (srunlikely(crc != li->begin->crc)) {
				sr_error(i->r->e, "corrupted log file '%s': bad record crc",
				         li->log->file);
				sl_iterseterror(li);
			}
		}
		return;
	}
	if (srunlikely(sl_iterset(i, next) == -1))
		return;
}

sriterif sl_iter =
{
	.init    = sl_iterinit,
	.open    = sl_iteropen,
	.close   = sl_iterclose,
	.has     = sl_iterhas,
	.of      = sl_iterof,
	.next    = sl_iternext
};

int sl_itererror(sriter *i)
{
	sliter *li = (sliter*)i->priv;
	return li->error;
}

int sl_itercontinue(sriter *i)
{
	sliter *li = (sliter*)i->priv;
	if (srunlikely(li->begin == NULL))
		return 0;
	if (srunlikely(li->v))
		return 1;
	if (srunlikely(li->error))
		return -1;
	li->pos = 0;
	slv *v = (slv*)((char*)li->begin + sizeof(slv));
	int rc = sl_iterset(i, v);
	if (srunlikely(rc == -1))
		return -1;
	return 1;
}
