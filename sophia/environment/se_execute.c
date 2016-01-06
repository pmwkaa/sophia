
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
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libsy.h>
#include <libse.h>

static inline int
se_execute_read_db(sereq *r, sedb *db, void *key, uint32_t keysize,
                   char *prefix,
                   uint32_t prefixsize)
{
	sereqarg *arg = &r->arg;
	siread q;
	sitx x;
	si_begin(&x, &db->index, 1);
	si_readopen(&q, &x, arg->cache,
	            arg->order,
	            arg->vlsn,
	            prefix,
	            prefixsize, key, keysize);
	if (arg->upsert)
		si_readupsert(&q, &arg->vup, arg->upsert_eq);
	if (arg->cache_only)
		si_readcache_only(&q);
	if (arg->has)
		si_readhas(&q);
	r->rc = si_read(&q);
	r->read_disk  += q.read_disk;
	r->read_cache += q.read_cache;
	r->v = q.result.v;
	si_readclose(&q);
	si_commit(&x);
	return r->rc;
}

int se_execute_read(sereq *r)
{
	sereqarg *arg = &r->arg;
	sedb *db = (sedb*)r->db;
	/* set key */
	uint32_t keysize;
	void *key;
	if (sslikely(arg->v.v)) {
		keysize = sv_size(&arg->v);
		key = sv_pointer(&arg->v);
	} else {
		keysize = 0;
		key = NULL;
	}
	/* set prefix */
	char *prefix;
	uint32_t prefixsize;
	if (arg->vprefix.v) {
		void *vptr = sv_vpointer(arg->vprefix.v);
		prefix = sf_key(vptr, 0);
		prefixsize = sf_keysize(vptr, 0);
	} else {
		prefix = NULL;
		prefixsize = 0;
	}
	if (sslikely(arg->vlsn_generate))
		arg->vlsn = sr_seq(db->r.seq, SR_LSN);
	/* read cache */
	if (db->cache && (arg->order == SS_EQ))
	{
		int rc;
		rc = se_execute_read_db(r, db->cache, key, keysize,
		                        prefix,
		                        prefixsize);
		switch (rc) {
		case  0:
			/* not found.
			 * repeat search using primary storage.
			 **/
			break;
		case -1:
		case  1: /* found */
			return rc;
		case  2: /* delete found */
			return 0;
		}
	}
	/* read storage */
	return se_execute_read_db(r, db, key, keysize,
	                          prefix,
	                          prefixsize);
}

int se_execute_write(sereq *r)
{
	sereqarg *arg = &r->arg;
	svlog *log = r->arg.log;
	se *e = se_of(r->object);
	/* set lsn */
	sl_prepare(&e->lp, log, arg->lsn);
	/* log write */
	if (! arg->recover) {
		sltx tl;
		sl_begin(&e->lp, &tl);
		int rc = sl_write(&tl, log);
		if (ssunlikely(rc == -1)) {
			sl_rollback(&tl);
			r->rc = -1;
			return -1;
		}
		sl_commit(&tl);
	}

	/* commit */
	uint64_t now = ss_utime();
	svlogindex *i   = (svlogindex*)log->index.s;
	svlogindex *end = (svlogindex*)log->index.p;
	while (i < end) {
		sedb *db = i->ptr;
		sitx x, xc;
		si_begin(&x, &db->index, 0);
		if (db->cache) {
			si_begin(&xc, &db->cache->index, 0);
			si_write(&xc, arg->recover, 1, now, log, i);
		}
		si_write(&x, arg->recover, 0, now, log, i);
		if (db->cache)
			si_commit(&xc);
		si_commit(&x);
		i++;
	}
	return 0;
}
