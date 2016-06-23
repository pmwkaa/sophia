
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libsl.h>

typedef struct sliter sliter;

struct sliter {
	int validate;
	int error;
	ssfile *log;
	ssmmap map;
	slv *v;
	slv *next;
	uint32_t count;
	uint32_t pos;
	sr *r;
} sspacked;

static void
sl_iterseterror(sliter *i)
{
	i->error = 1;
	i->v     = NULL;
	i->next  = NULL;
}

static int
sl_iternext_of(sliter *i, slv *next, int validate)
{
	if (next == NULL)
		return 0;
	char *eof   = (char*)i->map.p + i->map.size;
	char *start = (char*)next;

	/* eof */
	if (ssunlikely(start == eof)) {
		if (i->count != i->pos) {
			sr_malfunction(i->r->e, "corrupted log file '%s': transaction is incomplete",
			               ss_pathof(&i->log->path));
			sl_iterseterror(i);
			return -1;
		}
		i->v = NULL;
		i->next = NULL;
		return 0;
	}

	char *end = start + next->size;
	if (ssunlikely((start > eof || (end > eof)))) {
		sr_malfunction(i->r->e, "corrupted log file '%s': bad record size",
		               ss_pathof(&i->log->path));
		sl_iterseterror(i);
		return -1;
	}
	if (validate && i->validate)
	{
		uint32_t crc = 0;
		if (! (next->flags & SVBEGIN)) {
			crc = ss_crcp(i->r->crc, start + sizeof(slv), next->size, 0);
		}
		crc = ss_crcs(i->r->crc, start, sizeof(slv), crc);
		if (ssunlikely(crc != next->crc)) {
			sr_malfunction(i->r->e, "corrupted log file '%s': bad record crc",
			               ss_pathof(&i->log->path));
			sl_iterseterror(i);
			return -1;
		}
	}
	i->pos++;
	if (i->pos > i->count) {
		/* next transaction */
		i->v     = NULL;
		i->pos   = 0;
		i->count = 0;
		i->next  = next;
		return 0;
	}
	i->v = next;
	return 1;
}

int sl_itercontinue_of(sliter *i)
{
	if (ssunlikely(i->error))
		return -1;
	if (ssunlikely(i->v))
		return 1;
	if (ssunlikely(i->next == NULL))
		return 0;
	int validate = 0;
	i->pos   = 0;
	i->count = 0;
	slv *v = i->next;
	if (v->flags & SVBEGIN) {
		validate = 1;
		i->count = v->size;
		v = (slv*)((char*)i->next + sizeof(slv));
	} else {
		i->count = 1;
		v = i->next;
	}
	return sl_iternext_of(i, v, validate);
}

static inline int
sl_iterprepare(sliter *i)
{
	srversion *ver = (srversion*)i->map.p;
	if (! sr_versionstorage_check(ver))
		return sr_malfunction(i->r->e, "bad log file '%s' version",
		                      ss_pathof(&i->log->path));
	if (ssunlikely(i->log->size < (sizeof(srversion))))
		return sr_malfunction(i->r->e, "corrupted log file '%s': bad size",
		                      ss_pathof(&i->log->path));
	slv *next = (slv*)((char*)i->map.p + sizeof(srversion));
	int rc = sl_iternext_of(i, next, 1);
	if (ssunlikely(rc == -1))
		return -1;
	if (sslikely(i->next))
		return sl_itercontinue_of(i);
	return 0;
}

int sl_iter_open(ssiter *i, sr *r, ssfile *file, int validate)
{
	sliter *li = (sliter*)i->priv;
	memset(li, 0, sizeof(*li));
	li->r        = r;
	li->log      = file;
	li->validate = validate;
	if (ssunlikely(li->log->size < sizeof(srversion))) {
		sr_malfunction(li->r->e, "corrupted log file '%s': bad size",
		               ss_pathof(&li->log->path));
		return -1;
	}
	if (ssunlikely(li->log->size == sizeof(srversion)))
		return 0;
	int rc = ss_vfsmmap(r->vfs, &li->map, li->log->fd, li->log->size, 1);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(li->r->e, "failed to mmap log file '%s': %s",
		               ss_pathof(&li->log->path),
		               strerror(errno));
		return -1;
	}
	rc = sl_iterprepare(li);
	if (ssunlikely(rc == -1))
		ss_vfsmunmap(r->vfs, &li->map);
	return 0;
}

static void
sl_iter_close(ssiter *i)
{
	sliter *li = (sliter*)i->priv;
	ss_vfsmunmap(li->r->vfs, &li->map);
}

static int
sl_iter_has(ssiter *i)
{
	sliter *li = (sliter*)i->priv;
	return li->v != NULL;
}

static void*
sl_iter_of(ssiter *i)
{
	sliter *li = (sliter*)i->priv;
	return li->v;
}

static void
sl_iter_next(ssiter *i)
{
	sliter *li = (sliter*)i->priv;
	if (ssunlikely(li->v == NULL))
		return;
	slv *next =
		(slv*)((char*)li->v + sizeof(slv) + li->v->size);
	sl_iternext_of(li, next, 1);
}

ssiterif sl_iter =
{
	.close = sl_iter_close,
	.has   = sl_iter_has,
	.of    = sl_iter_of,
	.next  = sl_iter_next
};

int sl_iter_error(ssiter *i)
{
	sliter *li = (sliter*)i->priv;
	return li->error;
}

int sl_iter_continue(ssiter *i)
{
	sliter *li = (sliter*)i->priv;
	return sl_itercontinue_of(li);
}
