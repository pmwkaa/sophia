
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
	SE_DOCUMENT_FIELD,
	SE_DOCUMENT_ORDER,
	SE_DOCUMENT_PREFIX,
	SE_DOCUMENT_LSN,
	SE_DOCUMENT_TIMESTAMP,
	SE_DOCUMENT_LOG,
	SE_DOCUMENT_RAW,
	SE_DOCUMENT_FLAGS,
	SE_DOCUMENT_COLD_ONLY,
	SE_DOCUMENT_EVENT,
	SE_DOCUMENT_REUSE,
	SE_DOCUMENT_UNKNOWN
};

static inline int
se_document_opt(const char *path)
{
	switch (path[0]) {
	case 'o':
		if (sslikely(strcmp(path, "order") == 0))
			return SE_DOCUMENT_ORDER;
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
		if (sslikely(strcmp(path, "reuse") == 0))
			return SE_DOCUMENT_REUSE;
		break;
	case 'f':
		if (sslikely(strcmp(path, "flags") == 0))
			return SE_DOCUMENT_FLAGS;
		break;
	case 'c':
		if (sslikely(strcmp(path, "cold_only") == 0))
			return SE_DOCUMENT_COLD_ONLY;
		break;
	case 'e':
		if (sslikely(strcmp(path, "event") == 0))
			return SE_DOCUMENT_EVENT;
		break;
	}
	return SE_DOCUMENT_FIELD;
}

static inline int
se_document_create(sedocument *o)
{
	sedb *db = (sedb*)o->o.parent;
	se *e = se_of(&db->o);

	assert(o->v.v == NULL);

	/* reuse document */
	uint32_t timestamp = UINT32_MAX;
	if (db->scheme->expire > 0) {
		if (ssunlikely(o->timestamp > 0))
			timestamp = o->timestamp;
		else
			timestamp = ss_timestamp();
	}

	/* create document from raw data */
	svv *v;
	if (o->raw) {
		v = sv_vbuildraw(db->r, o->raw, o->rawsize, timestamp);
		if (ssunlikely(v == NULL))
			return sr_oom(&e->error);
		sv_init(&o->v, &sv_vif, v, NULL);
		return 0;
	}

	if (o->prefix) {
		if (db->scheme->scheme.keys[0]->type != SS_STRING)
			return sr_error(&e->error, "%s", "prefix search is only "
			                "supported for a string key");

		void *copy = ss_malloc(&e->a, o->prefixsize);
		if (ssunlikely(copy == NULL))
			return sr_oom(&e->error);
		memcpy(copy, o->prefix, o->prefixsize);
		o->prefixcopy = copy;

		if (o->fields_count_keys == 0 && o->prefix)
		{
			memset(o->fields, 0, sizeof(o->fields));
			o->fields[0].pointer = o->prefix;
			o->fields[0].size = o->prefixsize;
			sf_limitset(&e->limit, &db->scheme->scheme, o->fields, SS_GTE);
			goto allocate;
		}
	}

	/* create document using current format, supplied
	 * key-chain and value */
	if (ssunlikely(o->fields_count_keys != db->scheme->scheme.keys_count))
	{
		/* set unspecified min/max keys, depending on
		 * iteration order */
		sf_limitset(&e->limit, &db->scheme->scheme,
		            o->fields, o->order);
		o->fields_count = db->scheme->scheme.fields_count;
		o->fields_count_keys = db->scheme->scheme.keys_count;
	}

allocate:
	v = sv_vbuild(db->r, o->fields, timestamp);
	if (ssunlikely(v == NULL))
		return sr_oom(&e->error);
	sv_init(&o->v, &sv_vif, v, NULL);
	return 0;
}

static int
se_document_open(so *o)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
	if (sslikely(v->created)) {
		assert(v->v.v != NULL);
		return 0;
	}
	int rc = se_document_create(v);
	if (ssunlikely(rc == -1))
		return -1;
	v->created = 1;
	return 0;
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
	se *e = se_of(o);
	if (v->v.v)
		si_gcv(&e->r, v->v.v);
	v->v.v = NULL;
	if (v->prefixcopy)
		ss_free(&e->a, v->prefixcopy);
	v->prefixcopy = NULL;
	v->prefix = NULL;
	so_mark_destroyed(&v->o);
	so_poolgc(&e->document, &v->o);
	return 0;
}

static sfv*
se_document_setfield(sedocument *v, const char *path, void *pointer, int size)
{
	se *e = se_of(&v->o);
	sedb *db = (sedb*)v->o.parent;
	sffield *field = sf_schemefind(&db->scheme->scheme, (char*)path);
	if (ssunlikely(field == NULL))
		return NULL;
	assert(field->position < (int)(sizeof(v->fields) / sizeof(sfv)));
	sfv *fv = &v->fields[field->position];
	if (size == 0)
		size = strlen(pointer);
	int fieldsize_max;
	if (field->key) {
		fieldsize_max = 1024;
	} else {
		fieldsize_max = 2 * 1024 * 1024;
	}
	if (ssunlikely(size > fieldsize_max)) {
		sr_error(&e->error, "field '%s' is too big (%d limit)",
		         pointer, fieldsize_max);
		return NULL;
	}
	if (fv->pointer == NULL) {
		v->fields_count++;
		if (field->key)
			v->fields_count_keys++;
	}
	fv->pointer = pointer;
	fv->size = size;
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
	case SE_DOCUMENT_FIELD: {
		sfv *fv = se_document_setfield(v, path, pointer, size);
		if (ssunlikely(fv == NULL))
			return -1;
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
	case SE_DOCUMENT_FIELD: {
		/* match field */
		sedb *db = (sedb*)o->parent;
		sffield *field = sf_schemefind(&db->scheme->scheme, (char*)path);
		if (ssunlikely(field == NULL))
			return NULL;
		/* database result document */
		if (v->v.v)
			return sv_field(&v->v, db->r, field->position, (uint32_t*)size);
		/* database field document */
		assert(field->position < (int)(sizeof(v->fields) / sizeof(sfv)));
		sfv *fv = &v->fields[field->position];
		if (fv->pointer == NULL)
			return NULL;
		if (size)
			*size = fv->size;
		return fv->pointer;
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
	case SE_DOCUMENT_COLD_ONLY:
		v->cold_only = num;
		break;
	case SE_DOCUMENT_TIMESTAMP:
		v->timestamp = num;
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
	case SE_DOCUMENT_FLAGS: {
		uint64_t flags = -1;
		if (v->v.v)
			flags = ((svv*)(v->v.v))->flags;
		return flags;
	}
	}
	return -1;
}

static int
se_document_setobject(so *o, const char *path, void *object)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
	switch (se_document_opt(path)) {
	case SE_DOCUMENT_REUSE: {
		se *e = se_of(o);
		sedocument *reuse = se_cast(object, sedocument*, SEDOCUMENT);
		if (ssunlikely(v->created))
			return sr_error(&e->error, "%s", "document is read-only");
		assert(v->v.v == NULL);
		if (ssunlikely(object == o->parent))
			return sr_error(&e->error, "%s", "bad document operation");
		if (ssunlikely(! reuse->created))
			return sr_error(&e->error, "%s", "bad document operation");
		sv_init(&v->v, &sv_vif, reuse->v.v, NULL);
		sv_vref(v->v.v);
		v->created = 1;
		break;
	}
	default:
		return -1;
	}
	return 0;
}

static soif sedocumentif =
{
	.open         = se_document_open,
	.close        = NULL,
	.destroy      = se_document_destroy,
	.free         = se_document_free,
	.error        = NULL,
	.document     = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setstring    = se_document_setstring,
	.setint       = se_document_setint,
	.setobject    = se_document_setobject,
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
