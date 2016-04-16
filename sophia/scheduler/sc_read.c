
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

void sc_readclose(scread *r)
{
	sr *rt = r->r;
	/* free key, prefix, upsert and a pending result */
	if (r->arg.v.v)
		sv_vunref(rt, r->arg.v.v);
	if (r->arg.vup.v)
		sv_vunref(rt, r->arg.vup.v);
	if (ssunlikely(r->result))
		sv_vunref(rt, r->result);
	/* free read cache */
	if (sslikely(r->arg.cachegc && r->arg.cache))
		si_cachepool_push(r->arg.cache);
}

void sc_readopen(scread *r, sr *rt, so *db, si *index)
{
	r->db = db;
	r->index = index;
	r->start = 0;
	r->read_disk = 0;
	r->read_cache = 0;
	r->result = NULL;
	r->rc = 0;
	r->r = rt;
}

int sc_read(scread *r, sc *s)
{
	screadarg *arg = &r->arg;
	si *index = r->index;

	if (sslikely(arg->vlsn_generate))
		arg->vlsn = sr_seq(s->r->seq, SR_LSN);

	siread q;
	si_readopen(&q, index, arg->cache,
	            arg->order,
	            arg->vlsn,
	            arg->prefix,
	            arg->prefixsize,
	            sv_pointer(&arg->v),
	            sv_size(&arg->v));
	if (arg->upsert)
		si_readupsert(&q, &arg->vup, arg->upsert_eq);
	if (arg->cache_only)
		si_readcache_only(&q);
	if (arg->oldest_only)
		si_readoldest_only(&q);
	if (arg->has)
		si_readhas(&q);
	r->rc = si_read(&q);
	r->read_disk  += q.read_disk;
	r->read_cache += q.read_cache;
	r->result = q.result.v;
	si_readclose(&q);
	return r->rc;
}
