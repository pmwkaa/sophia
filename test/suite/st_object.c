
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sophia.h>
#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libso.h>
#include <libst.h>

void *st_object_generate(stgenerator *g, sf fmt, stlist *l, void *db,
                         uint32_t seed,
                         uint32_t seed_value)
{
	svv *v = st_svv_seed(g, seed, seed_value);
	if (ssunlikely(v == NULL))
		return NULL;
	assert(l->svv == 1);
	int rc = ss_bufadd(&l->list, g->r->a, &v, sizeof(svv**));
	if (ssunlikely(rc == -1)) {
		sv_vfree(g->r->a, v);
		return NULL;
	}
	void *o = sp_object(db);
	t( o != NULL );
	switch (fmt) {
	case SF_KV: {
		int i = 0;
		while (i < g->r->scheme->count) {
			rc = sp_setstring(o, g->r->scheme->parts[i].name,
			                  sf_key(sv_vpointer(v), i),
			                  sf_keysize(sv_vpointer(v), i));
			t( rc == 0 );
			i++;
		}
		if ((g->value_start + g->value_end) > 0) {
			rc = sp_setstring(o, "value",
			                  sf_value(g->r->fmt, sv_vpointer(v), g->r->scheme->count),
			                  sf_valuesize(g->r->fmt, sv_vpointer(v), v->size,
			                               g->r->scheme->count));
			t( rc == 0 );
		}
		break;
	}
	case SF_DOCUMENT: {
		char *vpointer = sv_vpointer(v);
		int i = 0;
		while (i < g->r->scheme->count) {
			rc = sp_setstring(o, g->r->scheme->parts[i].name,
			                  sf_key(vpointer, i),
			                  sf_keysize(vpointer, i));
			t( rc == 0 );
			i++;
		}
		rc = sp_setstring(o, "value", vpointer, v->size);
		t( rc == 0 );
		break;
	}
	}
	return o;
}

void st_object_eq(stgenerator *g, sf fmt, void *o,
                  uint32_t seed,
                  uint32_t seed_value)
{
	svv *v = st_svv_seed(g, seed, seed_value);
	if (ssunlikely(v == NULL)) {
		t(0);
		return;
	}
	switch (fmt) {
	case SF_KV: {
		int i = 0;
		int size = 0;
		while (i < g->r->scheme->count) {
			void *ptr = sp_getstring(o, g->r->scheme->parts[i].name, &size);
			t( ptr != NULL );
			t( size == sf_keysize(sv_vpointer(v), i) );
			t( memcmp(ptr, sf_key(sv_vpointer(v), i), size) == 0 );
			i++;
		}
		void *ptr = sp_getstring(o, "value", &size);
		if ((g->value_start + g->value_end) > 0) {
			t( ptr != NULL );
			t( size == sf_valuesize(g->r->fmt, sv_vpointer(v), v->size,
			                        g->r->scheme->count) );
			t( memcmp(ptr, sf_value(g->r->fmt, sv_vpointer(v), g->r->scheme->count),
			          size) == 0 );
		} else {
			t( ptr == NULL );
		}
		break;
	}
	case SF_DOCUMENT: {
		int i = 0;
		int size = 0;
		while (i < g->r->scheme->count) {
			void *ptr = sp_getstring(o, g->r->scheme->parts[i].name, &size);
			t( ptr != NULL );
			t( size == sf_keysize(sv_vpointer(v), i) );
			t( memcmp(ptr, sf_key(sv_vpointer(v), i), size) == 0 );
			i++;
		}
		void *ptr = sp_getstring(o, "value", &size);
		t( ptr != NULL );
		if ((g->value_start + g->value_end) > 0) {
			int vsize = sf_valuesize(g->r->fmt, ptr, size, g->r->scheme->count);
			t( vsize == sf_valuesize(g->r->fmt, sv_vpointer(v), v->size,
			                         g->r->scheme->count) );
			ptr = sf_value(g->r->fmt, ptr, g->r->scheme->count);
			t( memcmp(ptr, sf_value(g->r->fmt, sv_vpointer(v),
			          g->r->scheme->count), vsize) == 0 );
		}
		break;
	}
	}
	sv_vfree(g->r->a, v);
}
