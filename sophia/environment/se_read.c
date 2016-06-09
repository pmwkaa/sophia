
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
#include <libsc.h>
#include <libse.h>

static inline so*
se_readresult(se *e, siread *r)
{
	sedocument *v = (sedocument*)se_document_new(e, r->index->object, &r->result);
	if (ssunlikely(v == NULL))
		return NULL;
	v->read_disk    = r->read_disk;
	v->read_cache   = r->read_cache;
	v->read_latency = 0;
	if (r->result.v) {
		v->read_latency = ss_utime() - r->read_start;
		sr_statget(&e->stat,
		           v->read_latency,
		           v->read_disk,
		           v->read_cache);
	}

	/* propagate current document settings to
	 * the result one */
	v->orderset = 1;
	v->order = r->order;
	if (v->order == SS_GTE)
		v->order = SS_GT;
	else
	if (v->order == SS_LTE)
		v->order = SS_LT;

	/* set prefix */
	if (r->prefix) {
		v->prefix = r->prefix;
		v->prefix_copy = r->prefix;
		v->prefix_size = r->prefix_size;
	}

	v->cold_only = r->cold_only;
	v->created   = 1;
	v->flagset   = 1;
	return &v->o;
}

so *se_read(sedb *db, sedocument *o, sx *x, uint64_t vlsn,
            sicache *cache)
{
	se *e = se_of(&db->o);
	uint64_t start  = ss_utime();

	/* prepare the key */
	int auto_close = o->created <= 1;
	int rc = se_document_createkey(o);
	if (ssunlikely(rc == -1))
		goto error;
	rc = se_document_validate_ro(o, &db->o);
	if (ssunlikely(rc == -1))
		goto error;
	if (ssunlikely(! se_active(e)))
		goto error;

	sv vup;
	sv_init(&vup, &sv_vif, NULL, NULL);

	sedocument *ret = NULL;

	/* concurrent */
	if (x && o->order == SS_EQ) {
		/* note: prefix is ignored during concurrent
		 * index search */
		int rc = sx_get(x, &db->coindex, &o->v, &vup);
		if (ssunlikely(rc == -1 || rc == 2 /* delete */))
			goto error;
		if (rc == 1 && !sv_is(&vup, SVUPSERT)) {
			ret = (sedocument*)se_document_new(e, &db->o, &vup);
			if (sslikely(ret)) {
				ret->cold_only = o->cold_only;
				ret->created   = 1;
				ret->orderset  = 1;
				ret->flagset   = 1;
			} else {
				sv_vunref(db->r, vup.v);
			}
			if (auto_close)
				so_destroy(&o->o);
			return &ret->o;
		}
	} else {
		sx_get_autocommit(&e->xm, &db->coindex);
	}

	/* prepare read cache */
	int cachegc = 0;
	if (cache == NULL) {
		cachegc = 1;
		cache = si_cachepool_pop(&e->cachepool);
		if (ssunlikely(cache == NULL)) {
			if (vup.v)
				sv_vunref(db->r, vup.v);
			sr_oom(&e->error);
			goto error;
		}
	}

	sv_vref(o->v.v);

	/* do read */
	siread rq;
	si_readopen(&rq, db->index, cache, o->order,
	            vlsn,
	            sv_pointer(&o->v),
	            vup.v,
	            o->prefix_copy,
	            o->prefix_size,
	            o->cold_only,
	            0,
	            start);
	rc = si_read(&rq);
	si_readclose(&rq);

	/* prepare result */
	if (rc == 1) {
		ret = (sedocument*)se_readresult(e, &rq);
		if (ret)
			o->prefix_copy = NULL;
	}

	/* cleanup */
	if (o->v.v)
		sv_vunref(db->r, o->v.v);
	if (vup.v)
		sv_vunref(db->r, vup.v);
	if (ret == NULL && rq.result.v)
		sv_vunref(db->r, rq.result.v);
	if (cachegc && cache)
		si_cachepool_push(cache);

	if (auto_close)
		so_destroy(&o->o);
	return &ret->o;
error:
	if (auto_close)
		so_destroy(&o->o);
	return NULL;
}

