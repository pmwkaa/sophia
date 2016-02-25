
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
	if (r->arg.vprefix.v)
		sv_vunref(rt, r->arg.vprefix.v);
	if (r->arg.vup.v)
		sv_vunref(rt, r->arg.vup.v);
	if (ssunlikely(r->result))
		sv_vunref(rt, r->result);
	/* free read cache */
	if (sslikely(r->arg.cachegc && r->arg.cache))
		si_cachepool_push(r->arg.cache);
	si_unref(r->index, SI_REFBE);
}

static int
sc_readdestroy(so *o)
{
	scread *r = (scread*)o;
	sc_readclose(r);
	ss_free(r->r->a, r);
	return 0;
}

static soif screadif =
{
	.open         = NULL,
	.close        = NULL,
	.destroy      = sc_readdestroy,
	.error        = NULL,
	.document     = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = NULL,
	.upsert       = NULL,
	.del          = NULL,
	.get          = NULL,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

void sc_readopen(scread *r, sr *rt, so *db, si *index)
{
	so_init(&r->o, NULL, &screadif, NULL, NULL);
	r->id = sr_seq(rt->seq, SR_RSNNEXT);
	si_ref(index, SI_REFBE);
	r->db = db;
	r->index = index;
	memset(&r->arg, 0, sizeof(r->arg));
	r->start = 0;
	r->read_disk = 0;
	r->read_cache = 0;
	r->result = NULL;
	r->rc = 0;
	r->r = rt;
}

static inline int
sc_readindex(scread *r, si *index, void *key, uint32_t keysize,
             char *prefix,
             uint32_t prefixsize)
{
	screadarg *arg = &r->arg;
	siread q;
	sitx x;
	si_begin(&x, index, 1);
	si_readopen(&q, &x, arg->cache,
	            arg->order,
	            arg->vlsn,
	            prefix,
	            prefixsize, key, keysize);
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
	si_commit(&x);
	return r->rc;
}

int sc_read(scread *r, sc *s)
{
	screadarg *arg = &r->arg;
	si *index = r->index;
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
		arg->vlsn = sr_seq(s->r->seq, SR_LSN);

	/* read cache */
	if (index->cache && (arg->order == SS_EQ))
	{
		int rc;
		rc = sc_readindex(r, index->cache, key, keysize,
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
	return sc_readindex(r, index, key, keysize,
	                    prefix,
	                    prefixsize);
}
