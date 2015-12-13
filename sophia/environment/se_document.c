
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
se_document_destroy(so *o)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
	if (ssunlikely(v->immutable))
		return 0;
	se *e = se_of(o);
	if (v->v.v)
		sv_vfree(&e->r, (svv*)v->v.v);
	v->v.v = NULL;
	se_mark_destroyed(&v->o);
	ss_free(&e->a_document, v);
	return 0;
}

static sfv*
se_document_setpart(sedocument *v, const char *path, void *pointer, int size)
{
	se *e = se_of(&v->o);
	sedb *db = (sedb*)v->o.parent;
	srkey *part = sr_schemefind(&db->scheme.scheme, (char*)path);
	if (ssunlikely(part == NULL))
		return NULL;
	assert(part->pos < (int)(sizeof(v->keyv) / sizeof(sfv)));
	const int keysize_max = 1 << 15;
	if (size == 0)
		size = strlen(pointer);
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
	sr_statkey(&e->stat, size);
	return fv;
}

static int
se_document_setstring(so *o, const char *path, void *pointer, int size)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
	se *e = se_of(o);
	if (ssunlikely(v->v.v))
		return sr_error(&e->error, "%s", "document is read-only");

	if (strcmp(path, "value") == 0) {
		const int valuesize_max = 1 << 21;
		if (ssunlikely(size > valuesize_max)) {
			sr_error(&e->error, "value is too big (%d limit)",
			         valuesize_max);
			return -1;
		}
		v->value = pointer;
		v->valuesize = size;
		sr_statvalue(&e->stat, size);
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
	if (strcmp(path, "arg") == 0) {
		v->async_arg = pointer;
		return 0;
	}
	/* document keypart */
	sfv *fv = se_document_setpart(v, path, pointer, size);
	if (ssunlikely(fv == NULL))
		return -1;
	return 0;
}

static void*
se_document_getstring(so *o, const char *path, int *size)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
	if (strcmp(path, "value") == 0) {
		/* key document */
		if (v->value) {
			if (size)
				*size = v->valuesize;
			if (v->valuesize == 0)
				return NULL;
			return v->value;
		}
		if (v->v.v == NULL) {
			if (size)
				*size = 0;
			return NULL;
		}
		/* result document */
		sedb *db = (sedb*)o->parent;
		int vsize = sv_valuesize(&v->v, &db->r);
		if (size)
			*size = vsize;
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
	if (strcmp(path, "arg") == 0) {
		if (size)
			*size = 0;
		return v->async_arg;
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
	srkey *part = sr_schemefind(&db->scheme.scheme, (char*)path);
	if (ssunlikely(part == NULL))
		return NULL;
	/* database result document */
	if (v->v.v) {
		if (size)
			*size = sv_keysize(&v->v, &db->r, part->pos);
		return sv_key(&v->v, &db->r, part->pos);
	}
	/* database key document */
	assert(part->pos < (int)(sizeof(v->keyv) / sizeof(sfv)));
	sfv *fv = &v->keyv[part->pos];
	if (fv->key == NULL)
		return NULL;
	if (size)
		*size = fv->r.size;
	return fv->key;
}

static int
se_document_setint(so *o, const char *path, int64_t num)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
	if (strcmp(path, "cache_only") == 0) {
		v->cache_only = num;
		return 0;
	} else
	if (strcmp(path, "immutable") == 0) {
		v->immutable = num;
		return 0;
	} else
	if (strcmp(path, "async") == 0) {
		v->async = num;
		return 0;
	}
	return -1;
}

static int64_t
se_document_getint(so *o, const char *path)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
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
	} else
	if (strcmp(path, "cache_only") == 0) {
		return v->cache_only;
	} else
	if (strcmp(path, "immutable") == 0) {
		return v->immutable;
	} else {
		sr_error(&e->error, "unknown document field '%s'",
		         path);
	}
	return -1;
}

static soif sedocumentif =
{
	.open         = NULL,
	.destroy      = se_document_destroy,
	.error        = NULL,
	.document     = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setstring    = se_document_setstring,
	.setint       = se_document_setint,
	.getobject    = NULL,
	.getstring    = se_document_getstring,
	.getint       = se_document_getint,
	.set          = NULL,
	.upsert       = NULL,
	.del          = NULL,
	.get          = NULL,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

so *se_document_new(se *e, so *parent, sv *vp, int async)
{
	sedocument *v = ss_malloc(&e->a_document, sizeof(sedocument));
	if (ssunlikely(v == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	memset(v, 0, sizeof(*v));
	so_init(&v->o, &se_o[SEDOCUMENT], &sedocumentif, parent, &e->o);
	v->order = SS_EQ;
	v->async = async;
	if (vp) {
		v->v = *vp;
	}
	return &v->o;
}
