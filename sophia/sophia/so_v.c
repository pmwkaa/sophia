
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsx.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libse.h>
#include <libso.h>

static int
so_vdestroy(soobj *obj, va_list args srunused)
{
	sov *v = (sov*)obj;
	if (v->flags & SO_VIMMUTABLE)
		return 0;
	so_vrelease(v);
	sr_free(&so_of(obj)->a_v, v);
	return 0;
}

static int
so_vset(soobj *obj, va_list args)
{
	sov *v = (sov*)obj;
	so *e = so_of(obj);
	if (srunlikely(v->flags & SO_VRO)) {
		sr_error(&e->error, "%s", "object is read-only");
		return -1;
	}
	char *name = va_arg(args, char*);
	if (strcmp(name, "key") == 0) {
		if (v->v.i != &sv_localif) {
			sr_error(&e->error, "%s", "bad object operation");
			return -1;
		}
		v->lv.key = va_arg(args, char*);
		v->lv.keysize = va_arg(args, int);
		return 0;
	} else
	if (strcmp(name, "value") == 0) {
		if (v->v.i != &sv_localif) {
			sr_error(&e->error, "%s", "bad object operation");
			return -1;
		}
		v->lv.value = va_arg(args, char*);
		v->lv.valuesize = va_arg(args, int);
		return 0;
	} else
	if (strcmp(name, "prefix") == 0) {
		if (v->v.i != &sv_localif) {
			sr_error(&e->error, "%s", "bad object operation");
			return -1;
		}
		v->prefix = va_arg(args, char*);
		v->prefixsize = va_arg(args, int);
		return 0;
	} else
	if (strcmp(name, "log") == 0) {
		v->log = va_arg(args, void*);
		return 0;
	} else
	if (strcmp(name, "order") == 0) {
		char *order = va_arg(args, void*);
		srorder cmp = sr_orderof(order);
		if (srunlikely(cmp == SR_STOP)) {
			sr_error(&e->error, "%s", "bad order name");
			return -1;
		}
		v->order = cmp;
		return 0;
	}
	return -1;
}

static void*
so_vget(soobj *obj, va_list args)
{
	sov *v = (sov*)obj;
	so *e = so_of(obj);
	char *name = va_arg(args, char*);
	if (strcmp(name, "key") == 0) {
		void *key = sv_key(&v->v);
		if (srunlikely(key == NULL))
			return NULL;
		int *keysize = va_arg(args, int*);
		if (keysize)
			*keysize = sv_keysize(&v->v);
		return key;
	} else
	if (strcmp(name, "value") == 0) {
		void *value = sv_value(&v->v);
		if (srunlikely(value == NULL))
			return NULL;
		int *valuesize = va_arg(args, int*);
		if (valuesize)
			*valuesize = sv_valuesize(&v->v);
		return value;
	} else
	if (strcmp(name, "prefix") == 0) {
		if (srunlikely(v->prefix == NULL))
			return NULL;
		int *prefixsize = va_arg(args, int*);
		if (prefixsize)
			*prefixsize = v->prefixsize;
		return v->prefix;
	} else
	if (strcmp(name, "lsn") == 0) {
		uint64_t *lsnp = NULL;
		if (v->v.i == &sv_localif)
			lsnp = &v->lv.lsn;
		else
		if (v->v.i == &sv_vif)
			lsnp = &((svv*)(v->v.v))->lsn;
		else
		if (v->v.i == &sx_vif)
			lsnp = &((sxv*)(v->v.v))->v->lsn;
		else {
			assert(0);
		}
		int *valuesize = va_arg(args, int*);
		if (valuesize)
			*valuesize = sizeof(uint64_t);
		return lsnp;
	} else
	if (strcmp(name, "order") == 0) {
		src order = {
			.name     = "order",
			.value    = sr_ordername(v->order),
			.flags    = SR_CSZ,
			.ptr      = NULL,
			.function = NULL,
			.next     = NULL
		};
		void *o = so_ctlreturn(&order, e);
		if (srunlikely(o == NULL))
			return NULL;
		return o;
	}
	return NULL;
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
	.drop     = NULL,
	.begin    = NULL,
	.prepare  = NULL,
	.commit   = NULL,
	.cursor   = NULL,
	.object   = NULL,
	.type     = so_vtype
};

soobj *so_vinit(sov *v, so *e, soobj *parent)
{
	memset(v, 0, sizeof(*v));
	so_objinit(&v->o, SOV, &sovif, &e->o);
	sv_init(&v->v, &sv_localif, &v->lv, NULL);
	v->order = SR_GTE;
	v->parent = parent;
	return &v->o;
}

soobj *so_vnew(so *e, soobj *parent)
{
	sov *v = sr_malloc(&e->a_v, sizeof(sov));
	if (srunlikely(v == NULL)) {
		sr_error(&e->error, "%s", "memory allocation failed");
		return NULL;
	}
	return so_vinit(v, e, parent);
}

soobj *so_vrelease(sov *v)
{
	so *e = so_of(&v->o);
	if (v->flags & SO_VALLOCATED)
		sv_vfree(&e->a, (svv*)v->v.v);
	v->flags = 0;
	v->v.v = NULL;
	return &v->o;
}

soobj *so_vput(sov *o, sv *v)
{
	o->flags = SO_VALLOCATED|SO_VRO;
	o->v = *v;
	return &o->o;
}

soobj *so_vdup(so *e, soobj *parent, sv *v)
{
	sov *ret = (sov*)so_vnew(e, parent);
	if (srunlikely(ret == NULL))
		return NULL;
	ret->flags = SO_VALLOCATED|SO_VRO;
	ret->v = *v;
	return &ret->o;
}

int so_vimmutable(sov *v)
{
	v->flags |= SO_VIMMUTABLE;
	return 0;
}
