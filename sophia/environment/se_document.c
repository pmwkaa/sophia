
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
#include <libsc.h>
#include <libse.h>

enum {
	SE_DOCUMENT_KEY,
	SE_DOCUMENT_VALUE,
	SE_DOCUMENT_ORDER,
	SE_DOCUMENT_PREFIX,
	SE_DOCUMENT_LSN,
	SE_DOCUMENT_TIMESTAMP,
	SE_DOCUMENT_LOG,
	SE_DOCUMENT_RAW,
	SE_DOCUMENT_FLAGS,
	SE_DOCUMENT_CACHE_ONLY,
	SE_DOCUMENT_OLDEST_ONLY,
	SE_DOCUMENT_IMMUTABLE,
	SE_DOCUMENT_EVENT,
	SE_DOCUMENT_UNKNOWN
};

static inline int
se_document_opt(const char *path)
{
	switch (path[0]) {
	case 'v':
		if (sslikely(strcmp(path, "value") == 0))
			return SE_DOCUMENT_VALUE;
		break;
	case 'k':
		if (sslikely(strncmp(path, "key", 3) == 0))
			return SE_DOCUMENT_KEY;
		break;
	case 'o':
		if (sslikely(strcmp(path, "order") == 0))
			return SE_DOCUMENT_ORDER;
		if (sslikely(strcmp(path, "oldest_only") == 0))
			return SE_DOCUMENT_OLDEST_ONLY;
		break;
	case 'l':
		if (sslikely(strcmp(path, "lsn") == 0))
			return SE_DOCUMENT_LSN;
		if (sslikely(strcmp(path, "log") == 0))
			return SE_DOCUMENT_LOG;
		break;
	case 't':
		if (sslikely(strcmp(path, "timestamp") == 0))
			return SE_DOCUMENT_TIMESTAMP;
		break;
	case 'p':
		if (sslikely(strcmp(path, "prefix") == 0))
			return SE_DOCUMENT_PREFIX;
		break;
	case 'r':
		if (sslikely(strcmp(path, "raw") == 0))
			return SE_DOCUMENT_RAW;
		break;
	case 'f':
		if (sslikely(strcmp(path, "flags") == 0))
			return SE_DOCUMENT_FLAGS;
		break;
	case 'c':
		if (sslikely(strcmp(path, "cache_only") == 0))
			return SE_DOCUMENT_CACHE_ONLY;
		break;
	case 'i':
		if (sslikely(strcmp(path, "immutable") == 0))
			return SE_DOCUMENT_IMMUTABLE;
		break;
	case 'e':
		if (sslikely(strcmp(path, "event") == 0))
			return SE_DOCUMENT_EVENT;
		break;
	}
	return SE_DOCUMENT_UNKNOWN;
}

static void
se_document_free(so *o)
{
	assert(o->destroyed);
	se *e = se_of(o);
	ss_free(&e->a, o);
}

static int
se_document_destroy(so *o)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
	if (ssunlikely(v->immutable))
		return 0;
	se *e = se_of(o);
	if (v->v.v)
		si_gcv(&e->r, v->v.v);
	v->v.v = NULL;
	so_mark_destroyed(&v->o);
	so_poolgc(&e->document, &v->o);
	return 0;
}

static sfv*
se_document_setpart(sedocument *v, const char *path, void *pointer, int size)
{
	se *e = se_of(&v->o);
	sedb *db = (sedb*)v->o.parent;
	srkey *part = sr_schemefind(&db->scheme->scheme, (char*)path);
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
	switch (se_document_opt(path)) {
	case SE_DOCUMENT_KEY: {
		sfv *fv = se_document_setpart(v, path, pointer, size);
		if (ssunlikely(fv == NULL))
			return -1;
		break;
	}
	case SE_DOCUMENT_VALUE: {
		const int valuesize_max = 1 << 21;
		if (ssunlikely(size > valuesize_max)) {
			sr_error(&e->error, "value is too big (%d limit)",
			         valuesize_max);
			return -1;
		}
		v->value = pointer;
		if (ssunlikely(size == 0 && pointer))
			size = strlen(pointer);
		v->valuesize = size;
		sr_statvalue(&e->stat, size);
		break;
	}
	case SE_DOCUMENT_ORDER:
		if (size == 0)
			size = strlen(pointer);
		ssorder cmp = ss_orderof(pointer, size);
		if (ssunlikely(cmp == SS_STOP)) {
			sr_error(&e->error, "%s", "bad order name");
			return -1;
		}
		v->order = cmp;
		v->orderset = 1;
		break;
	case SE_DOCUMENT_PREFIX:
		v->prefix = pointer;
		v->prefixsize = size;
		break;
	case SE_DOCUMENT_LOG:
		v->log = pointer;
		break;
	case SE_DOCUMENT_RAW:
		v->raw = pointer;
		v->rawsize = size;
		break;
	default:
		return -1;
	}
	return 0;
}

static void*
se_document_getstring(so *o, const char *path, int *size)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
	switch (se_document_opt(path)) {
	case SE_DOCUMENT_KEY: {
		/* match key-part */
		sedb *db = (sedb*)o->parent;
		srkey *part = sr_schemefind(&db->scheme->scheme, (char*)path);
		if (ssunlikely(part == NULL))
			return NULL;
		/* database result document */
		if (v->v.v) {
			if (size)
				*size = sv_keysize(&v->v, db->r, part->pos);
			return sv_key(&v->v, db->r, part->pos);
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
	case SE_DOCUMENT_VALUE: {
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
		int vsize = sv_valuesize(&v->v, db->r);
		if (size)
			*size = vsize;
		if (vsize == 0)
			return NULL;
		return sv_value(&v->v, db->r);
	}
	case SE_DOCUMENT_PREFIX: {
		if (v->prefix == NULL)
			return NULL;
		if (size)
			*size = v->prefixsize;
		return v->prefix;
	}
	case SE_DOCUMENT_ORDER: {
		char *order = ss_ordername(v->order);
		if (size)
			*size = strlen(order) + 1;
		return order;
	}
	case SE_DOCUMENT_EVENT: {
		char *type = "none";
		if (v->event == 1)
			type = "on_backup";
		if (size)
			*size = strlen(type);
		return type;
	}
	case SE_DOCUMENT_RAW:
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
	return NULL;
}


static int
se_document_setint(so *o, const char *path, int64_t num)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
	switch (se_document_opt(path)) {
	case SE_DOCUMENT_TIMESTAMP:
		v->timestamp = num;
		break;
	case SE_DOCUMENT_CACHE_ONLY:
		v->cache_only = num;
		break;
	case SE_DOCUMENT_OLDEST_ONLY:
		v->oldest_only = num;
		break;
	case SE_DOCUMENT_IMMUTABLE:
		v->immutable = num;
		break;
	default:
		return -1;
	}
	return 0;
}

static int64_t
se_document_getint(so *o, const char *path)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
	switch (se_document_opt(path)) {
	case SE_DOCUMENT_LSN: {
		uint64_t lsn = -1;
		if (v->v.v)
			lsn = ((svv*)(v->v.v))->lsn;
		return lsn;
	}
	case SE_DOCUMENT_EVENT:
		return v->event;
	case SE_DOCUMENT_CACHE_ONLY:
		return v->cache_only;
	case SE_DOCUMENT_IMMUTABLE:
		return v->immutable;
	case SE_DOCUMENT_FLAGS: {
		uint64_t flags = -1;
		if (v->v.v)
			flags = ((svv*)(v->v.v))->flags;
		return flags;
	}
	}
	return -1;
}

static soif sedocumentif =
{
	.open         = NULL,
	.close        = NULL,
	.destroy      = se_document_destroy,
	.free         = se_document_free,
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

so *se_document_new(se *e, so *parent, sv *vp)
{
	sedocument *v = (sedocument*)so_poolpop(&e->document);
	if (v == NULL)
		v = ss_malloc(&e->a, sizeof(sedocument));
	if (ssunlikely(v == NULL)) {
		sr_oom(&e->error);
		return NULL;
	}
	memset(v, 0, sizeof(*v));
	so_init(&v->o, &se_o[SEDOCUMENT], &sedocumentif, parent, &e->o);
	v->order = SS_EQ;
	if (vp) {
		v->v = *vp;
	}
	so_pooladd(&e->document, &v->o);
	return &v->o;
}
