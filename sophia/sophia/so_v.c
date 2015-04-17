
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
	if (strcmp(name, "value") == 0) {
		v->value = va_arg(args, char*);
		v->valuesize = va_arg(args, int);
		return 0;
	}
	if (strcmp(name, "prefix") == 0) {
		v->prefix = va_arg(args, char*);
		v->prefixsize = va_arg(args, int);
		return 0;
	}
	if (strcmp(name, "log") == 0) {
		v->log = va_arg(args, void*);
		return 0;
	}
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
	if (strcmp(name, "raw") == 0) {
		v->raw = va_arg(args, char*);
		v->rawsize = va_arg(args, int);
		return 0;
	}
	assert(v->parent != NULL);
	/* set keypart */
	sodb *db = (sodb*)v->parent;
	srkeypart *part = sr_keyfind(&db->ctl.cmp, name);
	if (srunlikely(part == NULL))
		return -1;
	assert(part->pos < (int)(sizeof(v->keyv) / sizeof(srformatv)));
	srformatv *fv = &v->keyv[part->pos];
	fv->r.offset = 0;
	fv->key = va_arg(args, char*);
	fv->r.size = va_arg(args, int);
	if (fv->part == NULL)
		v->keyc++;
	fv->part = part;
	/* update key sum */
	v->keysize = sr_formatsize(db->r.format, v->keyv, v->keyc, 0);
	return 0;
}

static void*
so_vget(soobj *obj, va_list args)
{
	sov *v = (sov*)obj;
	so *e = so_of(obj);

	char *name = va_arg(args, char*);
	if (strcmp(name, "value") == 0) {
		int *valuesize = va_arg(args, int*);
		/* ctl value */
		if (v->parent == NULL) {
			int size = sv_valuesize(&v->v, &e->r);
			if (valuesize)
				*valuesize = size;
			if (size == 0)
				return NULL;
			return sv_value(&v->v, &e->r);
		}
		/* database key object */
		if (v->value) {
			if (valuesize)
				*valuesize = v->valuesize;
			return v->value;
		}
		/* database result object */
		sodb *db = (sodb*)v->parent;
		if (valuesize) {
			*valuesize = sv_valuesize(&v->v, &db->r);
		}
		return sv_value(&v->v, &db->r);
	}
	if (strcmp(name, "prefix") == 0) {
		if (srunlikely(v->prefix == NULL))
			return NULL;
		int *prefixsize = va_arg(args, int*);
		if (prefixsize)
			*prefixsize = v->prefixsize;
		return v->prefix;
	}
	if (strcmp(name, "lsn") == 0) {
		uint64_t *lsnp = NULL;
		if (v->v.v)
			lsnp = &((svv*)(v->v.v))->lsn;
		return lsnp;
	}
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
	if (strcmp(name, "raw") == 0) {
		int *rawsize = va_arg(args, int*);
		if (v->raw) {
			if (rawsize)
				*rawsize = v->rawsize;
			return v->raw;
		}
		return sv_pointer(&v->v);
	}

	int *partsize = va_arg(args, int*);
	/* ctl key */
	if (srunlikely(v->parent == NULL)) {
		if (strcmp(name, "key") != 0)
			return NULL;
		if (partsize)
			*partsize = sv_keysize(&v->v, &e->r, 0);
		return sv_key(&v->v, &e->r, 0);
	}
	/* match key-part */
	sodb *db = (sodb*)v->parent;
	srkeypart *part = sr_keyfind(&db->ctl.cmp, name);
	if (srunlikely(part == NULL))
		return NULL;
	/* database result object */
	if (v->v.v) {
		if (partsize)
			*partsize = sv_keysize(&v->v, &db->r, part->pos);
		return sv_key(&v->v, &db->r, part->pos);
	}
	/* database key object */
	assert(part->pos < (int)(sizeof(v->keyv) / sizeof(srformatv)));
	srformatv *fv = &v->keyv[part->pos];
	if (fv->key == NULL)
		return NULL;
	if (partsize)
		*partsize = fv->r.size;
	return fv->key;
}

static void*
so_vtype(soobj *o srunused, va_list args srunused) {
	return "object";
}

static soobjif sovif =
{
	.ctl     = NULL,
	.async   = NULL,
	.open    = NULL,
	.destroy = so_vdestroy,
	.error   = NULL,
	.set     = so_vset,
	.get     = so_vget,
	.del     = NULL,
	.drop    = NULL,
	.begin   = NULL,
	.prepare = NULL,
	.commit  = NULL,
	.cursor  = NULL,
	.object  = NULL,
	.type    = so_vtype
};

soobj *so_vinit(sov *v, so *e, soobj *parent)
{
	memset(v, 0, sizeof(*v));
	so_objinit(&v->o, SOV, &sovif, &e->o);
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
