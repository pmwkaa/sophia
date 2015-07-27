
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
se_vdestroy(so *o)
{
	sev *v = se_cast(o, sev*, SEV);
	se *e = se_of(o);
	if (v->v.v)
		sv_vfree(&e->a, (svv*)v->v.v);
	v->v.v = NULL;
	se_mark_destroyed(&v->o);
	ss_free(&e->a_v, v);
	return 0;
}

static sfv*
se_vsetpart(sev *v, char *path, void *pointer, int size)
{
	se *e = se_of(&v->o);
	sedb *db = (sedb*)v->o.parent;
	srkey *part = sr_schemefind(&db->scheme.scheme, path);
	if (ssunlikely(part == NULL))
		return NULL;
	assert(part->pos < (int)(sizeof(v->keyv) / sizeof(sfv)));
	const int keysize_max = 1 << 15;
	if (size == 0)
		size = strlen(pointer) + 1;
	if (ssunlikely(size > keysize_max)) {
		sr_error(&e->error, "key '%s' is too big (%d limit)",
		         pointer, keysize_max);
		return NULL;
	}
	sfv *fv = &v->keyv[part->pos];
	fv->r.offset = 0;
	fv->key = pointer;
	fv->r.size = size;
	if (fv->part == NULL)
		v->keyc++;
	fv->part = part;
	return fv;
}

static int
se_vsetstring(so *o, char *path, void *pointer, int size)
{
	sev *v = se_cast(o, sev*, SEV);
	se *e = se_of(o);
	if (ssunlikely(v->v.v))
		return sr_error(&e->error, "%s", "object is read-only");

	if (strcmp(path, "value") == 0) {
		const int valuesize_max = 1 << 21;
		if (ssunlikely(size > valuesize_max)) {
			sr_error(&e->error, "value is too big (%d limit)",
			         valuesize_max);
			return -1;
		}
		v->value = pointer;
		v->valuesize = size;
		return 0;
	}
	if (strcmp(path, "prefix") == 0) {
		v->prefix = pointer;
		v->prefixsize = size;
		return 0;
	}
	if (strcmp(path, "log") == 0) {
		v->log = pointer;
		return 0;
	}
	if (strcmp(path, "order") == 0) {
		if (size == 0)
			size = strlen(pointer);
		ssorder cmp = ss_orderof(pointer, size);
		if (ssunlikely(cmp == SS_STOP)) {
			sr_error(&e->error, "%s", "bad order name");
			return -1;
		}
		v->order = cmp;
		v->orderset = 1;
		return 0;
	}
	if (strcmp(path, "raw") == 0) {
		v->raw = pointer;
		v->rawsize = size;
		return 0;
	}
	/* object keypart */
	sfv *fv = se_vsetpart(v, path, pointer, size);
	if (ssunlikely(fv == NULL))
		return -1;
	return 0;
}

static void*
se_vgetstring(so *o, char *path, int *size)
{
	sev *v = se_cast(o, sev*, SEV);
	if (strcmp(path, "value") == 0) {
		/* key object */
		if (v->value) {
			if (size)
				*size = v->valuesize;
			if (v->valuesize == 0)
				return NULL;
			return v->value;
		}
		/* result object */
		sedb *db = (sedb*)o->parent;
		int vsize = sv_valuesize(&v->v, &db->r);
		if (size) {
			*size = vsize;
		}
		if (vsize == 0)
			return NULL;
		return sv_value(&v->v, &db->r);
	}
	if (strcmp(path, "prefix") == 0) {
		if (v->prefix == NULL)
			return NULL;
		if (size)
			*size = v->prefixsize;
		return v->prefix;
	}
	if (strcmp(path, "order") == 0) {
		char *order = ss_ordername(v->order);
		if (*size)
			*size = strlen(order) + 1;
		return order;
	}
	if (strcmp(path, "type") == 0) {
		char *type = se_reqof(v->async_operation);
		if (size)
			*size = strlen(type);
		return type;
	}
	if (strcmp(path, "raw") == 0) {
		if (v->raw) {
			if (size)
				*size = v->rawsize;
			return v->raw;
		}
		if (v->v.v == NULL)
			return NULL;
		if (size)
			*size = sv_size(&v->v);
		return sv_pointer(&v->v);
	}

	/* match key-part */
	sedb *db = (sedb*)o->parent;
	srkey *part = sr_schemefind(&db->scheme.scheme, path);
	if (ssunlikely(part == NULL))
		return NULL;
	/* database result object */
	if (v->v.v) {
		if (size)
			*size = sv_keysize(&v->v, &db->r, part->pos);
		return sv_key(&v->v, &db->r, part->pos);
	}
	/* database key object */
	assert(part->pos < (int)(sizeof(v->keyv) / sizeof(sfv)));
	sfv *fv = &v->keyv[part->pos];
	if (fv->key == NULL)
		return NULL;
	if (size)
		*size = fv->r.size;
	return fv->key;
}

static int64_t
se_vgetint(so *o, char *path)
{
	sev *v = se_cast(o, sev*, SEV);
	se *e = se_of(o);
	if (strcmp(path, "lsn") == 0) {
		uint64_t lsn = -1;
		if (v->v.v)
			lsn = ((svv*)(v->v.v))->lsn;
		return lsn;
	} else
	if (strcmp(path, "status") == 0) {
		return v->async_status;
	} else
	if (strcmp(path, "seq") == 0) {
		return v->async_seq;
	} else {
		sr_error(&e->error, "unknown object field '%s'",
		         path);
	}
	return -1;
}

static soif sevif =
{
	.open         = NULL,
	.destroy      = se_vdestroy,
	.error        = NULL,
	.object       = NULL,
	.asynchronous = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = se_vsetstring,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = se_vgetstring,
	.getint       = se_vgetint,
	.set          = NULL,
	.update       = NULL,
	.del          = NULL,
	.get          = NULL,
	.batch        = NULL,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

so *se_vnew(se *e, so *parent, sv *vp, int async)
{
	sev *v = ss_malloc(&e->a_v, sizeof(sev));
	if (ssunlikely(v == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	memset(v, 0, sizeof(*v));
	so_init(&v->o, &se_o[SEV], &sevif, parent, &e->o);
	v->order = SS_EQ;
	v->async = async;
	if (vp) {
		v->v = *vp;
	}
	return &v->o;
}
