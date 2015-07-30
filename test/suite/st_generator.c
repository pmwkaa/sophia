
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
	srscheme *scheme = g->r->scheme;
	uint32_t u32parts[16];
	uint64_t u64parts[16];
	sfv parts[16];
	assert(scheme->count <= 16);
	int i = 0;
	while (i < scheme->count)
	{
		sfv *fv = &parts[i];
		fv->r.offset = 0;
		if (scheme->parts[i].type == SS_U32) {
			u32parts[i] = va_arg(args, uint32_t);
			fv->key = (void*)&u32parts[i];
			fv->r.size = sizeof(uint32_t);
		} else
		if (scheme->parts[i].type == SS_U64) {
			u64parts[i] = va_arg(args, uint64_t);
			fv->key = (void*)&u64parts[i];
			fv->r.size = sizeof(uint64_t);
		} else
		if (scheme->parts[i].type == SS_STRING) {
			fv->key = va_arg(args, void*);
			fv->r.size = va_arg(args, int);
		}
		i++;
	}
	svv *v = NULL;
	int valuesize = 0;
	void *value = NULL;
	if ((g->value_start + g->value_end) > 0) {
		valuesize = rand() % g->value_end;
		if (valuesize < g->value_start)
			valuesize = g->value_start;
		value = ss_malloc(g->r->a, valuesize);
		if (ssunlikely(value == NULL))
			return NULL;
		memset(value, 'x', valuesize);
	}
	v = sv_vbuild(g->r, parts, scheme->count, value, valuesize);
	if (value)
		ss_free(g->r->a, value);
	return v;
}

static inline svv*
st_svv_va(stgenerator *g, stlist *l, uint64_t lsn, uint8_t flags, va_list args)
{
	svv *v = NULL;
	switch (g->r->fmt) {
	case SF_KV: v = st_generator_kv(g, args);
		break;
	case SF_DOCUMENT:
		assert(0);
		break;
	}
	v->lsn = lsn;
	v->flags = flags;
	if (v == NULL || l == NULL)
		return v;
	assert(l->svv == 1);
	int rc = ss_bufadd(&l->list, g->r->a, &v, sizeof(svv**));
	if (ssunlikely(rc == -1)) {
		sv_vfree(g->r->a, v);
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
		sv_vfree(g->r->a, v);
		return NULL;
	}
	sv_init(vp, &sv_vif, v, NULL);
	if (l == NULL)
		return vp;
	assert(l->svv == 0);
	int rc = ss_bufadd(&l->list, g->r->a, &vp, sizeof(sv**));
	if (ssunlikely(rc == -1)) {
		ss_free(g->r->a, vp);
		sv_vfree(g->r->a, v);
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

static uint32_t u32parts[16];
static uint64_t u64parts[16];

svv *st_svv_seed(stgenerator *g, uint32_t seed, uint32_t seed_value)
{
	assert(g->r->fmt == SF_KV);
	srscheme *scheme = g->r->scheme;
	sfv parts[16];
	assert(scheme->count <= 16);
	int keysize = 0;
	void *key = NULL;
	int i = 0;
	while (i < scheme->count)
	{
		sfv *fv = &parts[i];
		fv->r.offset = 0;
		if (scheme->parts[i].type == SS_U32) {
			u32parts[i] = seed;
			fv->key = (void*)&u32parts[i];
			fv->r.size = sizeof(uint32_t);
		} else
		if (scheme->parts[i].type == SS_U64) {
			u64parts[i] = seed;
			fv->key = (void*)&u64parts[i];
			fv->r.size = sizeof(uint64_t);
		} else
		if (scheme->parts[i].type == SS_STRING) {
			keysize = seed % g->key_end;
			if (keysize < g->key_start)
				keysize = g->key_start;
			key = ss_malloc(g->r->a, keysize);
			if (ssunlikely(key == NULL)) {
				while (--i >= 0) {
					sfv *fv = &parts[i];
					ss_free(g->r->a, fv->key);
				}
				return NULL;
			}
			st_generator_key(key, keysize, seed);
			fv->key = key;
			fv->r.size = keysize;
		}
		i++;
	}
	svv *v = NULL;
	int valuesize = 0;
	void *value = NULL;
	if ((g->value_start + g->value_end) > 0) {
		valuesize = seed_value % g->value_end;
		if (valuesize < g->value_start)
			valuesize = g->value_start;
		value = ss_malloc(g->r->a, valuesize);
		if (ssunlikely(value == NULL))
			return NULL;
		assert(valuesize >= sizeof(seed_value));
		memcpy(value, &seed_value, sizeof(seed_value));
		memset(value + sizeof(seed_value), 0,
		       valuesize - sizeof(seed_value));
	}
	v = sv_vbuild(g->r, parts, scheme->count, value, valuesize);
	i = 0;
	while (i < scheme->count) {
		sfv *fv = &parts[i];
		if (scheme->parts[i].type == SS_STRING)
			ss_free(g->r->a, fv->key);
		i++;
	}
	if (value)
		ss_free(g->r->a, value);
	return v;
}
