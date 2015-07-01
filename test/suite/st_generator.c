
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

void st_generator_init(stgenerator *g, sr *r, int random,
                       int key_start, int key_end,
                       int value_start, int value_end)
{
	srand(0);

	g->r = r;
	g->is_random = random;
	g->key_start = key_start;
	g->key_end = key_end;
	g->value_start = value_start;
	g->value_end = value_end;
}

static inline void*
st_generator_part(stgenerator *g, sstype type,
                  int is_random,
                  int start, int end, int *size)
{
	int keysize = 0;
	switch (type) {
	case SS_STRING:
		keysize = (start + random()) % end;
		break;
	case SS_U32:
		keysize = sizeof(uint32_t);
		break;
	case SS_U64:
		keysize = sizeof(uint64_t);
		break;
	default: assert(0);
	}
	void *key = ss_malloc(g->r->a, keysize);
	if (key == NULL)
		return NULL;
	switch (type) {
	case SS_STRING: {
		int i = 0;
		while (i < keysize)
			((unsigned char*)key)[i] = random();
		break;
	}
	case SS_U32:
		if (is_random)
			*(uint32_t*)key = random();
		else
			*(uint32_t*)key = g->seq++; 
		break;
	case SS_U64:
		if (is_random)
			*(uint64_t*)key = random();
		else
			*(uint64_t*)key = g->seq++; 
		break;
	default: assert(0);
	}
	*size = keysize;
	return key;
}

static inline svv*
st_generator_kv(stgenerator *g)
{
	srscheme *scheme = g->r->scheme;
	sfv parts[16];
	int i = 0;
	while (i < scheme->count) {
		int keysize = 0;
		sfv *fv = &parts[i];
		fv->r.offset = 0;
		fv->key = st_generator_part(g, scheme->parts[i].type, g->is_random,
		                            g->key_start, g->key_end,
		                            &keysize);
		if (ssunlikely(fv->key == NULL)) {
			while (--i >= 0) {
				sfv *fv = &parts[i];
				ss_free(g->r->a, fv->key);
			}
			return NULL;
		}
		fv->r.size = keysize;
	}
	svv *v = NULL;
	int valuesize = 0;
	void *value = NULL;
	if ((g->value_start + g->value_end) > 0) {
		valuesize = (g->value_start + random()) % g->value_end;
		value = ss_malloc(g->r->a, valuesize);
		if (ssunlikely(value == NULL))
			goto free;
		memset(value, 'x', valuesize);
	}
	v = sv_vbuild(g->r, parts, scheme->count, value, valuesize);
free:
	i = 0;
	while (i < scheme->count) {
		sfv *fv = &parts[i];
		if (fv->key)
			ss_free(g->r->a, fv->key);
	}
	if (value)
		ss_free(g->r->a, value);
	return v;
}

static svv*
st_generator_vkv(stgenerator *g, va_list args)
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
		valuesize = (g->value_start + random()) % g->value_end;
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

svv *st_gsvv(stgenerator *g, stlist *l, uint64_t lsn, uint8_t flags)
{
	svv *v = NULL;
	switch (g->r->fmt) {
	case SF_KV: v = st_generator_kv(g);
		break;
	case SF_DOCUMENT:
		break;
	}
	v->lsn = lsn;
	v->flags = flags;
	if (l == NULL)
		return v;
	assert(l->svv == 1);
	int rc = ss_bufadd(&l->list, g->r->a, &v, sizeof(svv**));
	if (ssunlikely(rc == -1)) {
		sv_vfree(g->r->a, v);
		return NULL;
	}
	return v;
}

static inline svv*
st_svv_va(stgenerator *g, stlist *l, uint64_t lsn, uint8_t flags, va_list args)
{
	svv *v = NULL;
	switch (g->r->fmt) {
	case SF_KV: v = st_generator_vkv(g, args);
		break;
	case SF_DOCUMENT:
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

sv *st_gsv(stgenerator *g, stlist *l, uint64_t lsn, uint8_t flags)
{
	svv *v = st_gsvv(g, NULL, lsn, flags);
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

#if 0
void *st_generator_object(stgenerator *g, stlist *l, void *parent)
{
	svv *v = st_v(g, l);
	if (ssunlikely(v == NULL))
		return NULL;
	void *o = sp_object(parent);
	t( o != NULL );
	int rc;
	switch (g->r->fmt) {
	case SF_KV: {
		int i = 0;
		while (i < g->r->scheme->count) {
			rc = sp_set(o, g->r->scheme->parts[i].name,
			            sf_key(sv_vpointer(v), i), 
			            sf_keysize(sv_vpointer(v), i));
			t( rc == 0 );
			rc = sp_set(o, "value",
			            sf_value(g->r->fmt, sv_vpointer(v), g->r->scheme->count), 
			            sf_valuesize(g->r->fmt, sv_vpointer(v), sv_vsize(v),
			                         g->r->scheme->count));
			t( rc == 0 );
		}
		break;
	}
	case SF_DOCUMENT:
		break;
	}
	return o;
}
#endif
