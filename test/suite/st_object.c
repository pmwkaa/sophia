
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

void *st_object_generate(stgenerator *g, stlist *l, void *db,
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
	switch (g->r->fmt) {
	case SF_KV: {
		int i = 0;
		while (i < g->r->scheme->count) {
			rc = sp_setstring(o, g->r->scheme->parts[i].name,
			                  sf_key(sv_vpointer(v), i), 
			                  sf_keysize(sv_vpointer(v), i));
			t( rc == 0 );
			i++;
		}
		rc = sp_setstring(o, "value",
		                  sf_value(g->r->fmt, sv_vpointer(v), g->r->scheme->count), 
		                  sf_valuesize(g->r->fmt, sv_vpointer(v), v->size,
		                               g->r->scheme->count));
		t( rc == 0 );
		break;
	}
	case SF_DOCUMENT:
		break;
	}
	return o;
}
