
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

void *st_document_generate(stgenerator *g, stlist *l, void *db,
                           uint32_t seed,
                           uint32_t seed_value)
{
	svv *v = st_svv_seed(g, seed, seed_value);
	if (ssunlikely(v == NULL))
		return NULL;
	assert(l->type == ST_SVV);
	int rc = ss_bufadd(&l->list, g->r->a, &v, sizeof(svv**));
	if (ssunlikely(rc == -1)) {
		sv_vunref(g->r, v);
		return NULL;
	}
	void *o = sp_document(db);
	t( o != NULL );

	int i = 0;
	while (i < g->r->scheme->fields_count) {
		sffield *field = g->r->scheme->fields[i];
		if (field->lsn || field->size) {
			i++;
			continue;
		}
		uint32_t size;
		char *ptr = sf_fieldof(g->r->scheme, i, sv_vpointer(v), &size);
		rc = sp_setstring(o, g->r->scheme->fields[i]->name, ptr, size);
		t( rc == 0 );
		i++;
	}

	return o;
}

void st_document_eq(stgenerator *g, void *o,
                  uint32_t seed,
                  uint32_t seed_value)
{
	svv *v = st_svv_seed(g, seed, seed_value);
	if (ssunlikely(v == NULL)) {
		t(0);
		return;
	}
	int i = 0;
	int size = 0;
	while (i < g->r->scheme->fields_count) {
		sffield *field = g->r->scheme->fields[i];
		if (field->lsn || field->size) {
			i++;
			continue;
		}
		void *ptr = sp_getstring(o, g->r->scheme->fields[i]->name, &size);
		uint32_t size_b;
		char *ptr_b = sf_fieldof(g->r->scheme, i, sv_vpointer(v), &size_b);
		t( size == size_b );
		t( memcmp(ptr, ptr_b, size) == 0 );
		i++;
	}

	sv_vunref(g->r, v);
}
