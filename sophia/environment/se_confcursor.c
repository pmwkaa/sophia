
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

static void
se_confkv_free(so *o)
{
	seconfkv *v = (seconfkv*)o;
	se *e = se_of(o);
	ss_buffree(&v->key, &e->a);
	ss_buffree(&v->value, &e->a);
	ss_free(&e->a, v);
}

static int
se_confkv_destroy(so *o)
{
	seconfkv *v = se_cast(o, seconfkv*, SECONFKV);
	se *e = se_of(o);
	ss_bufreset(&v->key);
	ss_bufreset(&v->value);
	so_mark_destroyed(&v->o);
	so_poolgc(&e->confcursor_kv, &v->o);
	return 0;
}

void *se_confkv_getstring(so *o, const char *path, int *size)
{
	seconfkv *v = se_cast(o, seconfkv*, SECONFKV);
	int len;
	if (strcmp(path, "key") == 0) {
		len = ss_bufused(&v->key);
		if (size)
			*size = len;
		return v->key.s;
	} else
	if (strcmp(path, "value") == 0) {
		len = ss_bufused(&v->value);
		if (size)
			*size = len;
		if (len == 0)
			return NULL;
		return v->value.s;
	}
	return NULL;
}

static soif seconfkvif =
{
	.open         = NULL,
	.destroy      = se_confkv_destroy,
	.free         = se_confkv_free,
	.document     = NULL,
	.poll         = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.setobject    = NULL,
	.getobject    = NULL,
	.getstring    = se_confkv_getstring,
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

static inline so *se_confkv_new(se *e, srconfdump *vp)
{
	int cache;
	seconfkv *v = (seconfkv*)so_poolpop(&e->confcursor_kv);
	if (! v) {
		v = ss_malloc(&e->a, sizeof(seconfkv));
		cache = 0;
	} else {
		cache = 1;
	}
	if (ssunlikely(v == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&v->o, &se_o[SECONFKV], &seconfkvif, &e->o, &e->o);
	if (! cache) {
		ss_bufinit(&v->key);
		ss_bufinit(&v->value);
	}
	int rc;
	rc = ss_bufensure(&v->key, &e->a, vp->keysize);
	if (ssunlikely(rc == -1)) {
		so_mark_destroyed(&v->o);
		so_poolpush(&e->confcursor_kv, &v->o);
		sr_oom(&e->error);
		return NULL;
	}
	rc = ss_bufensure(&v->value, &e->a, vp->valuesize);
	if (ssunlikely(rc == -1)) {
		so_mark_destroyed(&v->o);
		so_poolpush(&e->confcursor_kv, &v->o);
		sr_oom(&e->error);
		return NULL;
	}
	memcpy(v->key.s, sr_confkey(vp), vp->keysize);
	memcpy(v->value.s, sr_confvalue(vp), vp->valuesize);
	ss_bufadvance(&v->key, vp->keysize);
	ss_bufadvance(&v->value, vp->valuesize);
	so_pooladd(&e->confcursor_kv, &v->o);
	return &v->o;
}

static void
se_confcursor_free(so *o)
{
	assert(o->destroyed);
	se *e = se_of(o);
	seconfcursor *c = (seconfcursor*)o;
	ss_buffree(&c->dump, &e->a);
	ss_free(&e->a, o);
}

static int
se_confcursor_destroy(so *o)
{
	seconfcursor *c = se_cast(o, seconfcursor*, SECONFCURSOR);
	se *e = se_of(o);
	ss_bufreset(&c->dump);
	so_mark_destroyed(&c->o);
	so_poolgc(&e->confcursor, &c->o);
	return 0;
}

static void*
se_confcursor_get(so *o, so *v)
{
	seconfcursor *c = se_cast(o, seconfcursor*, SECONFCURSOR);
	if (v) {
		so_destroy(v);
	}
	if (c->first) {
		assert( ss_bufsize(&c->dump) >= (int)sizeof(srconfdump) );
		c->first = 0;
		c->pos = (srconfdump*)c->dump.s;
	} else {
		int size = sizeof(srconfdump) + c->pos->keysize + c->pos->valuesize;
		c->pos = (srconfdump*)((char*)c->pos + size);
		if ((char*)c->pos >= c->dump.p)
			c->pos = NULL;
	}
	if (ssunlikely(c->pos == NULL))
		return NULL;
	se *e = se_of(&c->o);
	return se_confkv_new(e, c->pos);
}

static soif seconfcursorif =
{
	.open         = NULL,
	.destroy      = se_confcursor_destroy,
	.free         = se_confcursor_free,
	.document     = NULL,
	.poll         = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.setobject    = NULL,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = NULL,
	.upsert       = NULL,
	.del          = NULL,
	.get          = se_confcursor_get,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

so *se_confcursor_new(so *o)
{
	se *e = (se*)o;
	int cache;
	seconfcursor *c = (seconfcursor*)so_poolpop(&e->confcursor);
	if (! c) {
		c = ss_malloc(&e->a, sizeof(seconfcursor));
		cache = 0;
	} else {
		cache = 1;
	}
	if (ssunlikely(c == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&c->o, &se_o[SECONFCURSOR], &seconfcursorif, &e->o, &e->o);
	c->pos = NULL;
	c->first = 1;
	if (! cache)
		ss_bufinit(&c->dump);
	int rc = se_confserialize(&e->conf, &c->dump);
	if (ssunlikely(rc == -1)) {
		so_mark_destroyed(&c->o);
		so_poolpush(&e->confcursor, &c->o);
		sr_oom(&e->error);
		return NULL;
	}
	so_pooladd(&e->confcursor, &c->o);
	return &c->o;
}
