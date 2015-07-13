
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
se_metav_destroy(so *o)
{
	semetav *v = se_cast(o, semetav*, SEMETAV);
	se *e = se_of(o);
	ss_free(&e->a, v->key);
	if (v->value)
		ss_free(&e->a, v->value);
	ss_free(&e->a_metav, v);
	return 0;
}

void *se_metav_string(so *o, char *path, int *size)
{
	semetav *v = se_cast(o, semetav*, SEMETAV);
	if (strcmp(path, "key") == 0) {
		if (size)
			*size = v->keysize;
		return v->key;
	} else
	if (strcmp(path, "value") == 0) {
		if (size)
			*size = v->valuesize;
		return v->value;
	}
	return NULL;
}

static soif semetavif =
{
	.open         = NULL,
	.destroy      = se_metav_destroy,
	.error        = NULL,
	.object       = NULL,
	.asynchronous = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = se_metav_string,
	.getint       = NULL,
	.set          = NULL,
	.update       = NULL,
	.del          = NULL,
	.get          = NULL,
	.batch        = NULL,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

static inline so *se_metav_new(se *e, srmetadump *vp)
{
	semetav *v = ss_malloc(&e->a_metav, sizeof(semetav));
	if (ssunlikely(v == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&v->o, &se_o[SEMETAV], &semetavif, &e->o, &e->o);
	v->keysize = vp->keysize;
	v->key = ss_malloc(&e->a, v->keysize);
	if (ssunlikely(v->key == NULL)) {
		ss_free(&e->a_metav, v);
		return NULL;
	}
	memcpy(v->key, sr_metakey(vp), v->keysize);
	v->valuesize = vp->valuesize;
	v->value = NULL;
	if (v->valuesize > 0) {
		v->value = ss_malloc(&e->a, v->valuesize);
		if (ssunlikely(v->key == NULL)) {
			ss_free(&e->a, v->key);
			ss_free(&e->a_metav, v);
			return NULL;
		}
	}
	memcpy(v->value, sr_metavalue(vp), v->valuesize);
	return &v->o;
}

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
	return se_metav_new(e, c->pos);
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
		int size = sizeof(srmetadump) + c->pos->keysize + c->pos->valuesize;
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
	.batch        = NULL,
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
