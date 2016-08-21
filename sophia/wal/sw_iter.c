
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
#include <libsw.h>

typedef struct switer switer;

struct switer {
	int validate;
	int error;
	ssfile *log;
	ssmmap map;
	swv *v;
	swv *next;
	uint32_t count;
	uint32_t pos;
	sr *r;
} sspacked;

static void
sw_iterseterror(switer *i)
{
	i->error = 1;
	i->v     = NULL;
	i->next  = NULL;
}

static int
sw_iternext_of(switer *i, swv *next, int validate)
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
			sw_iterseterror(i);
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
		sw_iterseterror(i);
		return -1;
	}
	if (validate && i->validate)
	{
		uint32_t crc = 0;
		if (! (next->flags & SVBEGIN)) {
			crc = ss_crcp(i->r->crc, start + sizeof(swv), next->size, 0);
		}
		crc = ss_crcs(i->r->crc, start, sizeof(swv), crc);
		if (ssunlikely(crc != next->crc)) {
			sr_malfunction(i->r->e, "corrupted log file '%s': bad record crc",
			               ss_pathof(&i->log->path));
			sw_iterseterror(i);
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

int sw_itercontinue_of(switer *i)
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
	swv *v = i->next;
	if (v->flags & SVBEGIN) {
		validate = 1;
		i->count = v->size;
		v = (swv*)((char*)i->next + sizeof(swv));
	} else {
		i->count = 1;
		v = i->next;
	}
	return sw_iternext_of(i, v, validate);
}

static inline int
sw_iterprepare(switer *i)
{
	srversion *ver = (srversion*)i->map.p;
	if (! sr_versionstorage_check(ver))
		return sr_malfunction(i->r->e, "bad log file '%s' version",
		                      ss_pathof(&i->log->path));
	if (ssunlikely(i->log->size < (sizeof(srversion))))
		return sr_malfunction(i->r->e, "corrupted log file '%s': bad size",
		                      ss_pathof(&i->log->path));
	swv *next = (swv*)((char*)i->map.p + sizeof(srversion));
	int rc = sw_iternext_of(i, next, 1);
	if (ssunlikely(rc == -1))
		return -1;
	if (sslikely(i->next))
		return sw_itercontinue_of(i);
	return 0;
}

int sw_iter_open(ssiter *i, sr *r, ssfile *file, int validate)
{
	switer *li = (switer*)i->priv;
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
	rc = sw_iterprepare(li);
	if (ssunlikely(rc == -1))
		ss_vfsmunmap(r->vfs, &li->map);
	return 0;
}

static void
sw_iter_close(ssiter *i)
{
	switer *li = (switer*)i->priv;
	ss_vfsmunmap(li->r->vfs, &li->map);
}

static int
sw_iter_has(ssiter *i)
{
	switer *li = (switer*)i->priv;
	return li->v != NULL;
}

static void*
sw_iter_of(ssiter *i)
{
	switer *li = (switer*)i->priv;
	return li->v;
}

static void
sw_iter_next(ssiter *i)
{
	switer *li = (switer*)i->priv;
	if (ssunlikely(li->v == NULL))
		return;
	swv *next =
		(swv*)((char*)li->v + sizeof(swv) + li->v->size);
	sw_iternext_of(li, next, 1);
}

ssiterif sw_iter =
{
	.close = sw_iter_close,
	.has   = sw_iter_has,
	.of    = sw_iter_of,
	.next  = sw_iter_next
};

int sw_iter_error(ssiter *i)
{
	switer *li = (switer*)i->priv;
	return li->error;
}

int sw_iter_continue(ssiter *i)
{
	switer *li = (switer*)i->priv;
	return sw_itercontinue_of(li);
}
