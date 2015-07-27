
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

int se_reqread(sereq *r)
{
	sereqarg *arg = &r->arg;
	sedb *db = (sedb*)r->db;
	uint32_t keysize;
	void *key;
	if (sslikely(arg->v.v)) {
		keysize = sv_size(&arg->v);
		key = sv_pointer(&arg->v);
	} else {
		keysize = 0;
		key = NULL;
	}
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
	siquery q;
	si_queryopen(&q, arg->cache, &db->index,
	             arg->order,
	             arg->vlsn,
	             prefix,
	             prefixsize, key, keysize);
	if (arg->update)
		si_queryupdate(&q, &arg->vup);
	r->rc = si_query(&q);
	r->v = q.result.v;
	si_queryclose(&q);
	return r->rc;
}

int se_reqwrite(sereq *r)
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
	if (sslikely(arg->vlsn_generate))
		arg->vlsn = sx_vlsn(&e->xm);
	uint64_t now = ss_utime();
	svlogindex *i   = (svlogindex*)log->index.s;
	svlogindex *end = (svlogindex*)log->index.p;
	while (i < end) {
		sedb *db = i->ptr;
		sitx ti;
		si_begin(&ti, &db->index, arg->vlsn, now, log, i);
		si_write(&ti, arg->recover);
		si_commit(&ti);
		i++;
	}
	return 0;
}

int se_execute(sereq *r)
{
	switch (r->op) {
	case SE_REQREAD:  return se_reqread(r);
	case SE_REQWRITE: return se_reqwrite(r);
	default: assert(0);
	}
	return 0;
}
