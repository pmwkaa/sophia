
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

int sc_init(sc *s, sr *r, sstrigger *on_event, slpool *lp)
{
	uint64_t now = ss_utime();
	ss_mutexinit(&s->lock);
	s->checkpoint_lsn           = 0;
	s->checkpoint_lsn_last      = 0;
	s->checkpoint               = 0;
	s->age                      = 0;
	s->age_time                 = now;
	s->expire                   = 0;
	s->expire_time              = now;
	s->backup_bsn               = 0;
	s->backup_bsn_last          = 0;
	s->backup_bsn_last_complete = 0;
	s->backup_events            = 0;
	s->backup                   = 0;
	s->anticache_asn            = 0;
	s->anticache_asn_last       = 0;
	s->anticache_storage        = 0;
	s->anticache_time           = now;
	s->anticache                = 0;
	s->anticache_limit          = 0;
	s->snapshot_ssn             = 0;
	s->snapshot_ssn_last        = 0;
	s->snapshot_time            = now;
	s->snapshot                 = 0;
	s->gc                       = 0;
	s->gc_time                  = now;
	s->lru                      = 0;
	s->lru_time                 = now;
	s->rotate                   = 0;
	s->i                        = NULL;
	s->count                    = 0;
	s->rr                       = 0;
	s->r                        = r;
	s->on_event                 = on_event;
	s->backup_path              = NULL;
	s->lp                       = lp;
	ss_threadpool_init(&s->tp);
	sc_workerpool_init(&s->wp);
	return 0;
}

int sc_set(sc *s, uint32_t count, uint64_t anticache, char *backup_path)
{
	int size = sizeof(scdb) * count;
	scdb *db = ss_malloc(s->r->a, size);
	if (ssunlikely(db == NULL))
		return -1;
	memset(db, 0, size);
	s->i = db;
	s->count = count;
	s->anticache_limit = anticache;
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
