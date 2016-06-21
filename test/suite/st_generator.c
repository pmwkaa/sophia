
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

void st_generator_init(stgenerator *g, sr *r,
                       int key_start, int key_end,
                       int value_start, int value_end)
{
	srand(0);
	g->r = r;
	g->key_start = key_start;
	g->key_end = key_end;
	g->value_start = value_start;
	g->value_end = value_end;
}

static svv*
st_generator_kv(stgenerator *g, va_list args)
{
	sfscheme *scheme = g->r->scheme;
	uint32_t u32fields[16];
	uint64_t u64fields[16];
	sfv fields[16];
	memset(fields, 0, sizeof(fields));
	assert(scheme->fields_count <= 16);
	int i = 0;
	while (i < scheme->fields_count)
	{
		sffield *field = g->r->scheme->fields[i];
		if (field->lsn || field->size || field->flags) {
			i++;
			continue;
		}
		sfv *fv = &fields[i];
		if (scheme->fields[i]->type == SS_U32) {
			u32fields[i] = va_arg(args, uint32_t);
			fv->pointer = (void*)&u32fields[i];
			fv->size = sizeof(uint32_t);
		} else
		if (scheme->fields[i]->type == SS_U64) {
			u64fields[i] = va_arg(args, uint64_t);
			fv->pointer = (void*)&u64fields[i];
			fv->size = sizeof(uint64_t);
		} else
		if (scheme->fields[i]->type == SS_STRING) {
			fv->pointer = va_arg(args, void*);
			fv->size = va_arg(args, int);
		}
		i++;
	}
	return sv_vbuild(g->r, fields);
}

static inline svv*
st_svv_va(stgenerator *g, stlist *l, uint64_t lsn, uint8_t flags, va_list args)
{
	svv *v = st_generator_kv(g, args);
	sf_flagsset(g->r->scheme, sv_vpointer(v), flags);
	sf_lsnset(g->r->scheme, sv_vpointer(v), lsn);
	if (v == NULL || l == NULL)
		return v;
	assert(l->type == ST_SVV);
	int rc = ss_bufadd(&l->list, g->r->a, &v, sizeof(svv**));
	if (ssunlikely(rc == -1)) {
		sv_vunref(g->r, v);
		return NULL;
	}
	return v;
}

svv *st_svv(stgenerator *g, stlist *l, uint64_t lsn, uint8_t flags, ...)
{
	va_list args;
	va_start(args, flags);
	svv *v = st_svv_va(g, l, lsn, flags, args);
	va_end(args);
	return v;
}

sv *st_sv(stgenerator *g, stlist *l, uint64_t lsn, uint8_t flags, ...)
{
	va_list args;
	va_start(args, flags);
	svv *v = st_svv_va(g, NULL, lsn, flags, args);
	va_end(args);
	if (v == NULL)
		return NULL;
	sv *vp = ss_malloc(g->r->a, sizeof(sv));
	if (vp == NULL) {
		sv_vunref(g->r, v);
		return NULL;
	}
	sv_init(vp, &sv_vif, v, NULL);
	if (l == NULL)
		return vp;
	assert(l->type == ST_SV);
	int rc = ss_bufadd(&l->list, g->r->a, &vp, sizeof(sv**));
	if (ssunlikely(rc == -1)) {
		ss_free(g->r->a, vp);
		sv_vunref(g->r, v);
		return NULL;
	}
	return vp;
}

static inline void
st_generator_key(char *key, int size, int seed)
{
	assert(size > 1);
	const char x[] =
		"abcdefghijklmnopqrstuvwxyz"
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		"1234567890";
	int i = 0;
	while (i < size - 1) {
		key[i] = x[seed % sizeof(x)];
		seed ^= key[i];
		i++;
	}
	key[size - 1] = 0;
}

static uint32_t u32fields[16];
static uint64_t u64fields[16];

svv *st_svv_seed(stgenerator *g, uint32_t seed, uint32_t seed_value)
{
	sfscheme *scheme = g->r->scheme;
	sfv fields[16];
	memset(fields, 0, sizeof(fields));
	assert(scheme->fields_count <= 16);
	int keysize = 0;
	void *key = NULL;
	int i = 0;
	while (i < scheme->fields_count)
	{
		sffield *field = g->r->scheme->fields[i];
		if (field->lsn || field->size || field->flags) {
			i++;
			continue;
		}
		sfv *fv = &fields[i];
		if (scheme->fields[i]->type == SS_U32) {
			u32fields[i] = seed;
			fv->pointer = (void*)&u32fields[i];
			fv->size = sizeof(uint32_t);
		} else
		if (scheme->fields[i]->type == SS_U64) {
			u64fields[i] = seed;
			fv->pointer = (void*)&u64fields[i];
			fv->size = sizeof(uint64_t);
		} else
		if (scheme->fields[i]->type == SS_STRING) {
			keysize = seed % g->key_end;
			if (keysize < g->key_start)
				keysize = g->key_start;
			key = ss_malloc(g->r->a, keysize);
			if (ssunlikely(key == NULL)) {
				while (--i >= 0) {
					sfv *fv = &fields[i];
					ss_free(g->r->a, fv->pointer);
				}
				return NULL;
			}
			st_generator_key(key, keysize, seed);
			fv->pointer = key;
			fv->size = keysize;
		}
		i++;
	}
	svv *v = sv_vbuild(g->r, fields);
	i = 0;
	while (i < scheme->fields_count) {
		sfv *fv = &fields[i];
		if (scheme->fields[i]->type == SS_STRING)
			ss_free(g->r->a, fv->pointer);
		i++;
	}
	return v;
}
