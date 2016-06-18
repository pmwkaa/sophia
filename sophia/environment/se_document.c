
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
	SE_DOCUMENT_FIELD_0,
	SE_DOCUMENT_FIELD_1,
	SE_DOCUMENT_FIELD_2,
	SE_DOCUMENT_FIELD_3,
	SE_DOCUMENT_FIELD_4,
	SE_DOCUMENT_FIELD_5,
	SE_DOCUMENT_FIELD_6,
	SE_DOCUMENT_FIELD_7,
	SE_DOCUMENT_FIELD_8,
	SE_DOCUMENT_FIELD_9,
	SE_DOCUMENT_FIELD,
	SE_DOCUMENT_ORDER,
	SE_DOCUMENT_PREFIX,
	SE_DOCUMENT_LSN,
	SE_DOCUMENT_LOG,
	SE_DOCUMENT_RAW,
	SE_DOCUMENT_FLAGS,
	SE_DOCUMENT_COLD_ONLY,
	SE_DOCUMENT_EVENT,
	SE_DOCUMENT_UNKNOWN
};

static inline int
se_document_opt(const char *path)
{
	if (sslikely((intptr_t)path <= (intptr_t)SE_DOCUMENT_FIELD_9))
		return (int)(intptr_t)path;
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

int se_document_create(sedocument *o)
{
	sedb *db = (sedb*)o->o.parent;
	se *e = se_of(&db->o);

	if (ssunlikely(o->created)) {
		assert(o->v.v != NULL);
		return 0;
	}
	assert(o->v.v == NULL);

	/* create document from raw data */
	svv *v;
	if (o->raw) {
		v = sv_vbuildraw(db->r, o->raw, o->raw_size);
		if (ssunlikely(v == NULL))
			return sr_oom(&e->error);
		sv_init(&o->v, &sv_vif, v, NULL);
		o->created = 1;
		return 0;
	}

	/* ensure all keys are set */
	if (ssunlikely(o->fields_count_keys != db->scheme->scheme.keys_count))
		return sr_error(&e->error, "%s", "incomplete key");

	/* set auto fields */
	uint32_t timestamp = UINT32_MAX;
	if (db->scheme->scheme.has_timestamp) {
		timestamp = ss_timestamp();
		sf_autoset(&db->scheme->scheme, o->fields, &timestamp);
	}

	v = sv_vbuild(db->r, o->fields);
	if (ssunlikely(v == NULL))
		return sr_oom(&e->error);
	sv_init(&o->v, &sv_vif, v, NULL);
	o->created = 1;
	return 0;
}

int se_document_createkey(sedocument *o)
{
	sedb *db = (sedb*)o->o.parent;
	se *e = se_of(&db->o);

	if (ssunlikely(o->created)) {
		assert(o->v.v != NULL);
		return 0;
	}
	assert(o->v.v == NULL);

	/* set prefix */
	if (o->prefix) {
		if (db->scheme->scheme.keys[0]->type != SS_STRING)
			return sr_error(&e->error, "%s", "prefix search is only "
			                "supported for a string key");
		void *copy = ss_malloc(&e->a, o->prefix_size);
		if (ssunlikely(copy == NULL))
			return sr_oom(&e->error);
		memcpy(copy, o->prefix, o->prefix_size);
		o->prefix_copy = copy;
	}

	/* set unspecified min/max keys, depending on
	 * iteration order */
	if (ssunlikely(o->fields_count_keys != db->scheme->scheme.keys_count))
	{
		if (o->prefix && o->fields_count_keys == 0) {
			memset(o->fields, 0, sizeof(o->fields));
			o->fields[0].pointer = o->prefix;
			o->fields[0].size = o->prefix_size;
		}
		sf_limitset(&e->limit, &db->scheme->scheme,
		             o->fields, o->order);
		o->fields_count = db->scheme->scheme.fields_count;
		o->fields_count_keys = db->scheme->scheme.keys_count;
	}

	svv *v = sv_vbuild(db->r, o->fields);
	if (ssunlikely(v == NULL))
		return sr_oom(&e->error);
	sv_init(&o->v, &sv_vif, v, NULL);
	o->created = 1;
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
	sedb *db = (sedb*)v->o.parent;
	if (v->v.v)
		si_gcv(db->r, v->v.v);
	v->v.v = NULL;
	if (v->prefix_copy)
		ss_free(&e->a, v->prefix_copy);
	v->prefix_copy = NULL;
	v->prefix = NULL;
	so_mark_destroyed(&v->o);
	so_poolgc(&e->document, &v->o);
	return 0;
}

static inline int
se_document_setfield(sedocument *v, int pos, void *pointer, int size)
{
	se *e = se_of(&v->o);
	sedb *db = (sedb*)v->o.parent;
	if (ssunlikely(pos >= db->scheme->scheme.fields_count)) {
		sr_error(&e->error, "%s", "incorrect field position");
		return -1;
	}
	assert(pos < (int)(sizeof(v->fields) / sizeof(sfv)));
	sffield *field = sf_schemeof(&db->scheme->scheme, pos);
	sfv *fv = &v->fields[pos];
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
		return -1;
	}
	if (fv->pointer == NULL) {
		v->fields_count++;
		if (field->key)
			v->fields_count_keys++;
	}
	fv->pointer = pointer;
	fv->size = size;
	sr_statfield(&db->stat, size);
	return 0;
}

static inline void*
se_document_getfield(sedocument *v, int pos, int *size)
{
	se *e = se_of(&v->o);
	sedb *db = (sedb*)v->o.parent;
	if (ssunlikely(pos >= db->scheme->scheme.fields_count)) {
		sr_error(&e->error, "%s", "incorrect field position");
		return NULL;
	}
	assert(pos < (int)(sizeof(v->fields) / sizeof(sfv)));
	sffield *field = sf_schemeof(&db->scheme->scheme, pos);
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

static int
se_document_setstring(so *o, const char *path, void *pointer, int size)
{
	sedocument *v = se_cast(o, sedocument*, SEDOCUMENT);
	se *e = se_of(o);
	if (ssunlikely(v->v.v))
		return sr_error(&e->error, "%s", "document is read-only");
	int opt = se_document_opt(path);
	switch (opt) {
	case SE_DOCUMENT_FIELD_0:
	case SE_DOCUMENT_FIELD_1:
	case SE_DOCUMENT_FIELD_2:
	case SE_DOCUMENT_FIELD_3:
	case SE_DOCUMENT_FIELD_4:
	case SE_DOCUMENT_FIELD_5:
	case SE_DOCUMENT_FIELD_6:
	case SE_DOCUMENT_FIELD_7:
	case SE_DOCUMENT_FIELD_8:
	case SE_DOCUMENT_FIELD_9:
		return se_document_setfield(v, opt, pointer, size);
	case SE_DOCUMENT_FIELD: {
		sedb *db = (sedb*)v->o.parent;
		sffield *field = sf_schemefind(&db->scheme->scheme, (char*)path);
		if (ssunlikely(field == NULL))
			return -1;
		return se_document_setfield(v, field->position, pointer, size);
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
		v->prefix_size = size;
		break;
	case SE_DOCUMENT_LOG:
		v->log = pointer;
		break;
	case SE_DOCUMENT_RAW:
		v->raw = pointer;
		v->raw_size = size;
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
	int opt = se_document_opt(path);
	switch (opt) {
	case SE_DOCUMENT_FIELD_0:
	case SE_DOCUMENT_FIELD_1:
	case SE_DOCUMENT_FIELD_2:
	case SE_DOCUMENT_FIELD_3:
	case SE_DOCUMENT_FIELD_4:
	case SE_DOCUMENT_FIELD_5:
	case SE_DOCUMENT_FIELD_6:
	case SE_DOCUMENT_FIELD_7:
	case SE_DOCUMENT_FIELD_8:
	case SE_DOCUMENT_FIELD_9:
		return se_document_getfield(v, opt, size);
	case SE_DOCUMENT_FIELD: {
		/* match field */
		sedb *db = (sedb*)o->parent;
		sffield *field = sf_schemefind(&db->scheme->scheme, (char*)path);
		if (ssunlikely(field == NULL))
			return NULL;
		return se_document_getfield(v, field->position, size);
	}
	case SE_DOCUMENT_PREFIX: {
		if (v->prefix == NULL)
			return NULL;
		if (size)
			*size = v->prefix_size;
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
				*size = v->raw_size;
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

static soif sedocumentif =
{
	.open         = NULL,
	.destroy      = se_document_destroy,
	.free         = se_document_free,
	.document     = NULL,
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
