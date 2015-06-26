
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

static int
se_metacursor_destroy(so *o)
{
	semetacursor *c = se_cast(o, semetacursor*, SEMETACURSOR);
	se *e = se_of(o);
	ss_buffree(&c->dump, &e->a);
	so_listdel(&e->metacursor, &c->o);
	ss_free(&e->a_metacursor, c);
	return 0;
}

static inline so*
se_metacursor_object(semetacursor *c)
{
	se *e = se_of(&c->o);
	sfv key;
	key.key      = sr_metaname(c->pos);
	key.r.size   = c->pos->namesize;
	key.r.offset = 0;
	void *value = NULL;
	if (c->pos->valuesize > 0)
		value = sr_metavalue(c->pos);
	svv *v = sv_vbuild(&e->r, &key, 1, value, c->pos->valuesize);
	if (ssunlikely(v == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	sv vp;
	sv_init(&vp, &sv_vif, v, NULL);
	so *result = se_vnew(e, &e->o, &vp);
	if (ssunlikely(result == NULL)) {
		sr_oom(&e->error);
		sv_vfree(&e->a, v);
		return NULL;
	}
	return result;
}

static void*
se_metacursor_get(so *o, so *v ssunused)
{
	semetacursor *c = se_cast(o, semetacursor*, SEMETACURSOR);
	if (c->first) {
		assert( ss_bufsize(&c->dump) >= (int)sizeof(srmetadump) );
		c->first = 0;
		c->pos = (srmetadump*)c->dump.s;
	} else {
		int size = sizeof(srmetadump) + c->pos->namesize + c->pos->valuesize;
		c->pos = (srmetadump*)((char*)c->pos + size);
		if ((char*)c->pos >= c->dump.p)
			c->pos = NULL;
	}
	if (ssunlikely(c->pos == NULL))
		return NULL;
	return se_metacursor_object(c);
}

static soif semetacursorif =
{
	.open         = NULL,
	.destroy      = se_metacursor_destroy,
	.error        = NULL,
	.object       = NULL,
	.asynchronous = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = NULL,
	.update       = NULL,
	.del          = NULL,
	.get          = se_metacursor_get,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

so *se_metacursor_new(void *o)
{
	se *e = o;
	semetacursor *c = ss_malloc(&e->a_metacursor, sizeof(semetacursor));
	if (ssunlikely(c == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&c->o, &se_o[SEMETACURSOR], &semetacursorif, &e->o, &e->o);
	c->pos = NULL;
	c->first = 1;
	ss_bufinit(&c->dump);
	int rc = se_metaserialize(&e->meta, &c->dump);
	if (ssunlikely(rc == -1)) {
		c->o.i->destroy(&c->o);
		return NULL;
	}
	so_listadd(&e->metacursor, &c->o);
	return &c->o;
}
