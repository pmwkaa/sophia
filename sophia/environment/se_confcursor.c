
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
se_confv_destroy(so *o)
{
	seconfv *v = se_cast(o, seconfv*, SECONFV);
	se *e = se_of(o);
	ss_free(&e->a, v->key);
	if (v->value)
		ss_free(&e->a, v->value);
	se_mark_destroyed(&v->o);
	ss_free(&e->a_confv, v);
	return 0;
}

void *se_confv_string(so *o, const char *path, int *size)
{
	seconfv *v = se_cast(o, seconfv*, SECONFV);
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

static soif seconfvif =
{
	.open         = NULL,
	.destroy      = se_confv_destroy,
	.error        = NULL,
	.object       = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = se_confv_string,
	.getint       = NULL,
	.set          = NULL,
	.update       = NULL,
	.del          = NULL,
	.get          = NULL,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

static inline so *se_confv_new(se *e, srconfdump *vp)
{
	seconfv *v = ss_malloc(&e->a_confv, sizeof(seconfv));
	if (ssunlikely(v == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&v->o, &se_o[SECONFV], &seconfvif, &e->o, &e->o);
	v->keysize = vp->keysize;
	v->key = ss_malloc(&e->a, v->keysize);
	if (ssunlikely(v->key == NULL)) {
		se_mark_destroyed(&v->o);
		ss_free(&e->a_confv, v);
		return NULL;
	}
	memcpy(v->key, sr_confkey(vp), v->keysize);
	v->valuesize = vp->valuesize;
	v->value = NULL;
	if (v->valuesize > 0) {
		v->value = ss_malloc(&e->a, v->valuesize);
		if (ssunlikely(v->key == NULL)) {
			ss_free(&e->a, v->key);
			se_mark_destroyed(&v->o);
			ss_free(&e->a_confv, v);
			return NULL;
		}
	}
	memcpy(v->value, sr_confvalue(vp), v->valuesize);
	return &v->o;
}

static int
se_confcursor_destroy(so *o)
{
	seconfcursor *c = se_cast(o, seconfcursor*, SECONFCURSOR);
	se *e = se_of(o);
	ss_buffree(&c->dump, &e->a);
	so_listdel(&e->confcursor, &c->o);
	se_mark_destroyed(&c->o);
	ss_free(&e->a_confcursor, c);
	return 0;
}

static inline so*
se_confcursor_object(seconfcursor *c)
{
	se *e = se_of(&c->o);
	return se_confv_new(e, c->pos);
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
	return se_confcursor_object(c);
}

static soif seconfcursorif =
{
	.open         = NULL,
	.destroy      = se_confcursor_destroy,
	.error        = NULL,
	.object       = NULL,
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
	.get          = se_confcursor_get,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

so *se_confcursor_new(void *o)
{
	se *e = o;
	seconfcursor *c = ss_malloc(&e->a_confcursor, sizeof(seconfcursor));
	if (ssunlikely(c == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	so_init(&c->o, &se_o[SECONFCURSOR], &seconfcursorif, &e->o, &e->o);
	c->pos = NULL;
	c->first = 1;
	ss_bufinit(&c->dump);
	int rc = se_confserialize(&e->conf, &c->dump);
	if (ssunlikely(rc == -1)) {
		so_destroy(&c->o);
		return NULL;
	}
	so_listadd(&e->confcursor, &c->o);
	return &c->o;
}
