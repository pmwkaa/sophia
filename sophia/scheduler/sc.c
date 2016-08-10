
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
#include <libso.h>
#include <libsv.h>
#include <libsd.h>
#include <libsl.h>
#include <libsi.h>
#include <libsy.h>
#include <libsc.h>

int sc_init(sc *s, sr *r, slpool *lp)
{
	ss_mutexinit(&s->lock);
	/* task priorities */
	s->prio[SC_QBRANCH]         = 1;
	s->prio[SC_QGC]             = 1;
	s->prio[SC_QEXPIRE]         = 1;
	s->prio[SC_QBACKUP]         = 1;
	/* backup */
	s->backup_bsn               = 0;
	s->backup_bsn_last          = 0;
	s->backup_bsn_last_complete = 0;
	s->backup                   = 0;
	s->backup_in_progress       = 0;
	s->backup_path              = NULL;
	/* generic */
	s->rotate                   = 0;
	s->i                        = NULL;
	s->count                    = 0;
	s->rr                       = 0;
	s->r                        = r;
	s->lp                       = lp;
	ss_threadpool_init(&s->tp);
	sc_workerpool_init(&s->wp);
	return 0;
}

static inline int
sc_prepare(scdb *db)
{
	uint64_t now = ss_utime();
	db->checkpoint_lsn      = 0;
	db->checkpoint_lsn_last = 0;
	db->checkpoint          = 0;
	db->age                 = 0;
	db->age_time            = now;
	db->expire              = 0;
	db->expire_time         = now;
	db->gc                  = 0;
	db->gc_time             = now;
	db->backup              = 0;
	return 0;
}

int sc_set(sc *s, uint32_t count)
{
	int size = sizeof(scdb) * count;
	scdb *list = ss_malloc(s->r->a, size);
	if (ssunlikely(list == NULL))
		return -1;
	memset(list, 0, size);
	uint32_t i = 0;
	while (i < count) {
		sc_prepare(&list[i]);
		i++;
	}
	s->i = list;
	s->count = count;
	return 0;
}

int sc_setbackup(sc *s, char *backup_path)
{
	s->backup_path = backup_path;
	return 0;
}

int sc_run(sc *s, ssthreadf function, void *arg, int n)
{
	return ss_threadpool_new(&s->tp, s->r->a, n, function, arg);
}

int sc_shutdown(sc *s)
{
	sr *r = s->r;
	int rcret = 0;
	int rc = ss_threadpool_shutdown(&s->tp, r->a);
	if (ssunlikely(rc == -1))
		rcret = -1;
	rc = sc_workerpool_free(&s->wp, r);
	if (ssunlikely(rc == -1))
		rcret = -1;
	if (s->i) {
		ss_free(r->a, s->i);
		s->i = NULL;
	}
	ss_mutexfree(&s->lock);
	return rcret;
}
