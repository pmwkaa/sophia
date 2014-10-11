
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libso.h>
#include <sophia.h>

static int
so_vdestroy(soobj *obj)
{
	sov *v = (sov*)obj;
	if (v->allocated)
		sv_vfree(&v->e->a, (svv*)v->v.v);
	sr_free(&v->e->a, v);
	return 0;
}

static int
so_vset(soobj *obj, va_list args)
{
	sov *v = (sov*)obj;
	char *name = va_arg(args, char*);
	if (strcmp(name, "key") == 0) {
		v->lv.key = va_arg(args, char*);
		v->lv.keysize = va_arg(args, int);
		return 0;
	} else
	if (strcmp(name, "value") == 0) {
		v->lv.value = va_arg(args, char*);
		v->lv.valuesize = va_arg(args, int);
		return 0;
	} else
	if (strcmp(name, "lsn") == 0) {
		v->lv.lsn = va_arg(args, uint64_t);
		return 0;
	}
	return -1;
}

static void*
so_vget(soobj *obj, va_list args)
{
	sov *v = (sov*)obj;
	char *name = va_arg(args, char*);
	if (strcmp(name, "key") == 0) {
		void *key = svkey(&v->v);
		if (srunlikely(key == NULL))
			return NULL;
		int *keysize = va_arg(args, int*);
		if (keysize)
			*keysize = svkeysize(&v->v);
		return key;
	} else
	if (strcmp(name, "value") == 0) {
		void *value = svvalue(&v->v);
		if (srunlikely(value == NULL))
			return NULL;
		int *valuesize = va_arg(args, int*);
		if (valuesize)
			*valuesize = svvaluesize(&v->v);
		return value;
	} else
	if (strcmp(name, "lsn") == 0) {
		uint64_t *lsnp = NULL;
		if (v->v.i == &sv_localif)
			lsnp = &v->lv.lsn;
		else
		if (v->v.i == &sv_vif)
			lsnp = &((svv*)(v->v.v))->id.lsn;
		int *valuesize = va_arg(args, int*);
		if (valuesize)
			*valuesize = sizeof(uint64_t);
		return lsnp;
	}
	return NULL;
}

static void*
so_vcopy(soobj *o srunused, va_list args srunused)
{
	sov *v = (sov*)o;
	sov *copy = (sov*)so_vnew(v->e);
	if (srunlikely(copy == NULL))
		return NULL;
	copy->lv = v->lv;
	copy->v  = v->v;
	if (v->allocated) {
		svv *dup = sv_valloc(&v->e->a, &v->v);
		if (srunlikely(dup == NULL)) {
			sr_error(&v->e->error, "memory allocation failed");
			sr_error_recoverable(&v->e->error);
			sr_free(&v->e->a, copy);
			return NULL;
		}
		copy->allocated = 1;
		sv result;
		svinit(&result, &sv_vif, dup, NULL);
		so_vput(copy, &result);
	}
	return copy;
}

static void*
so_vtype(soobj *o srunused, va_list args srunused) {
	return "object";
}

static soobjif sovif =
{
	.ctl      = NULL,
	.open     = NULL,
	.destroy  = so_vdestroy,
	.error    = NULL,
	.set      = so_vset,
	.get      = so_vget,
	.del      = NULL,
	.begin    = NULL,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = NULL,
	.object   = NULL,
	.type     = so_vtype,
	.copy     = so_vcopy
};

soobj *so_vinit(sov *v, so *e)
{
	memset(v, 0, sizeof(*v));
	so_objinit(&v->o, SOV, &sovif);
	svinit(&v->v, &sv_localif, &v->lv, NULL);
	v->e = e;
	return &v->o;
}

soobj *so_vnew(so *e)
{
	sov *v = sr_malloc(&e->a, sizeof(sov));
	if (srunlikely(v == NULL)) {
		sr_error(&e->error, "memory allocation failed");
		sr_error_recoverable(&e->error);
		return NULL;
	}
	return so_vinit(v, e);
}

soobj *so_vrelease(sov *v)
{
	if (v->allocated)
		sv_vfree(&v->e->a, (svv*)v->v.v);
	v->allocated = 0;
	v->v.v = NULL;
	return &v->o;
}

soobj *so_vput(sov *o, sv *v)
{
	o->allocated = 1;
	o->v = *v;
	return &o->o;
}

soobj *so_vdup(so *e, sv *v)
{
	sov *ret = (sov*)so_vnew(e);
	if (srunlikely(ret == NULL))
		return NULL;
	ret->allocated = 1;
	ret->v = *v;
	return &ret->o;
}
