
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
	slv *v;
	slv *next;
	uint32_t count;
	uint32_t pos;
	sv current;
} srpacked;

static void
sl_iterseterror(sliter *i)
{
	i->error = 1;
	i->v     = NULL;
	i->next  = NULL;
}

static int
sl_iternext_of(sriter *i, slv *next, int validate)
{
	sliter *li = (sliter*)i->priv;
	if (next == NULL)
		return 0;
	char *eof   = (char*)li->map.p + li->map.size;
	char *start = (char*)next;

	/* eof */
	if (srunlikely(start == eof)) {
		if (li->count != li->pos) {
			sr_malfunction(i->r->e, "corrupted log file '%s': transaction is incomplete",
			               li->log->file);
			sl_iterseterror(li);
			return -1;
		}
		li->v = NULL;
		li->next = NULL;
		return 0;
	}

	char *end = start + next->keysize + next->valuesize;
	if (srunlikely((start > eof || (end > eof)))) {
		sr_malfunction(i->r->e, "corrupted log file '%s': bad record size",
		               li->log->file);
		sl_iterseterror(li);
		return -1;
	}
	if (validate && li->validate)
	{
		uint32_t crc = 0;
		if (! (next->flags & SVBEGIN)) {
			crc = sr_crcp(i->r->crc, start + sizeof(slv), next->keysize, 0);
			crc = sr_crcp(i->r->crc, start + sizeof(slv) + next->keysize,
			              next->valuesize, crc);
		}
		crc = sr_crcs(i->r->crc, start, sizeof(slv), crc);
		if (srunlikely(crc != next->crc)) {
			sr_malfunction(i->r->e, "corrupted log file '%s': bad record crc",
			               li->log->file);
			sl_iterseterror(li);
			return -1;
		}
	}
	li->pos++;
	if (li->pos > li->count) {
		/* next transaction */
		li->v     = NULL;
		li->pos   = 0;
		li->count = 0;
		li->next  = next;
		return 0;
	}
	li->v = next;
	sv_init(&li->current, &sl_vif, li->v, NULL);
	return 1;
}

int sl_itercontinue_of(sriter *i)
{
	sliter *li = (sliter*)i->priv;
	if (srunlikely(li->error))
		return -1;
	if (srunlikely(li->v))
		return 1;
	if (srunlikely(li->next == NULL))
		return 0;
	int validate = 0;
	li->pos   = 0;
	li->count = 0;
	slv *v = li->next;
	if (v->flags & SVBEGIN) {
		validate = 1;
		li->count = v->valuesize;
		v = (slv*)((char*)li->next + sizeof(slv));
	} else {
		li->count = 1;
		v = li->next;
	}
	return sl_iternext_of(i, v, validate);
}

static inline int
sl_iterprepare(sriter *i)
{
	sliter *li = (sliter*)i->priv;
	srversion *ver = (srversion*)li->map.p;
	if (! sr_versioncheck(ver))
		return sr_malfunction(i->r->e, "bad log file '%s' version",
		                      li->log->file);
	if (srunlikely(li->log->size < (sizeof(srversion))))
		return sr_malfunction(i->r->e, "corrupted log file '%s': bad size",
		                      li->log->file);
	slv *next = (slv*)((char*)li->map.p + sizeof(srversion));
	int rc = sl_iternext_of(i, next, 1);
	if (srunlikely(rc == -1))
		return -1;
	if (srlikely(li->next))
		return sl_itercontinue_of(i);
	return 0;
}

int sl_iter_open(sriter *i, srfile *file, int validate)
{
	sliter *li = (sliter*)i->priv;
	memset(li, 0, sizeof(*li));
	li->log      = file;
	li->validate = validate;
	if (srunlikely(li->log->size < sizeof(srversion))) {
		sr_malfunction(i->r->e, "corrupted log file '%s': bad size",
		               li->log->file);
		return -1;
	}
	if (srunlikely(li->log->size == sizeof(srversion)))
		return 0;
	int rc = sr_map(&li->map, li->log->fd, li->log->size, 1);
	if (srunlikely(rc == -1)) {
		sr_malfunction(i->r->e, "failed to mmap log file '%s': %s",
		               li->log->file, strerror(errno));
		return -1;
	}
	rc = sl_iterprepare(i);
	if (srunlikely(rc == -1))
		sr_mapunmap(&li->map);
	return 0;
}

static void
sl_iter_close(sriter *i srunused)
{
	sliter *li = (sliter*)i->priv;
	sr_mapunmap(&li->map);
}

static int
sl_iter_has(sriter *i)
{
	sliter *li = (sliter*)i->priv;
	return li->v != NULL;
}

static void*
sl_iter_of(sriter *i)
{
	sliter *li = (sliter*)i->priv;
	if (srunlikely(li->v == NULL))
		return NULL;
	return &li->current;
}

static void
sl_iter_next(sriter *i)
{
	sliter *li = (sliter*)i->priv;
	if (srunlikely(li->v == NULL))
		return;
	slv *next =
		(slv*)((char*)li->v + sizeof(slv) +
	           li->v->keysize +
	           li->v->valuesize);
	sl_iternext_of(i, next, 1);
}

sriterif sl_iter =
{
	.close   = sl_iter_close,
	.has     = sl_iter_has,
	.of      = sl_iter_of,
	.next    = sl_iter_next
};

int sl_iter_error(sriter *i)
{
	sliter *li = (sliter*)i->priv;
	return li->error;
}

int sl_iter_continue(sriter *i)
{
	return sl_itercontinue_of(i);
}
