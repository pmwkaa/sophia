
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

static inline int
sr_metaexec_serialize(srmeta *c, srmetastmt *stmt, char *root)
{
	char path[256];
	while (c) {
		if (root)
			snprintf(path, sizeof(path), "%s.%s", root, c->name);
		else
			snprintf(path, sizeof(path), "%s", c->name);
		int rc;
		if (c->flags & SR_NS) {
			rc = sr_metaexec_serialize(c->value, stmt, path);
			if (ssunlikely(rc == -1))
				return -1;
		} else {
			stmt->path = path;
			rc = c->function(c, stmt);
			if (ssunlikely(rc == -1))
				return -1;
			stmt->path = NULL;
		}
		c = c->next;
	}
	return 0;
}

int sr_metaexec(srmeta *start, srmetastmt *s)
{
	if (s->op == SR_SERIALIZE)
		return sr_metaexec_serialize(start, s, NULL);
	char path[256];
	snprintf(path, sizeof(path), "%s", s->path);
	char *ptr = NULL;
	char *token;
	token = strtok_r(path, ".", &ptr);
	if (ssunlikely(token == NULL))
		return -1;
	srmeta *c = start;
	while (c) {
		if (strcmp(token, c->name) != 0) {
			c = c->next;
			continue;
		}
		if (c->flags & SR_NS) {
			token = strtok_r(NULL, ".", &ptr);
			if (ssunlikely(token == NULL))
			{
				if (s->op == SR_WRITE && c->type != SS_UNDEF) {
					if (c->type != s->valuetype)
						goto bad_type;
				}
				if (c->function)
					return c->function(c, s);
				/* not supported */
				goto bad_path;
			}
			c = (srmeta*)c->value;
			continue;
		}
		token = strtok_r(NULL, ".", &ptr);
		if (ssunlikely(token != NULL))
			goto bad_path;
		return c->function(c, s);
	}

bad_path:
	return sr_error(s->r->e, "bad metadata path: %s", s->path);

bad_type:
	return sr_error(s->r->e, "incompatible type (%s) for (%s) %s",
	                ss_typeof(s->valuetype),
	                ss_typeof(c->type), s->path);
}

int sr_meta_read(srmeta *m, srmetastmt *s)
{
	switch (m->type) {
	case SS_U32:
		s->result = m->value;
		s->resultsize = sizeof(uint32_t);
		if (s->valuetype == SS_I64) {
		} else
		if (s->valuetype == SS_U32) {
		} else
		if (s->valuetype == SS_U64) {
		} else {
			goto bad_type;
		}
		break;
	case SS_U64:
		s->result = m->value;
		s->resultsize = sizeof(uint64_t);
		if (s->valuetype == SS_I64) {
		} else
		if (s->valuetype == SS_U32) {
		} else
		if (s->valuetype == SS_U64) {
		} else {
			goto bad_type;
		}
		break;
	case SS_STRING: {
		if (s->valuetype != SS_STRING)
			goto bad_type;
		s->result = NULL;
		s->resultsize = 0;
		char **string = m->value;
		if (*string == NULL)
			break;
		int size = strlen(*string) + 1; 
		s->resultsize = size;
		s->result = malloc(size);
		if (ssunlikely(s->result == NULL))
			return sr_oom(s->r->e);
		memcpy(s->result, *string, size);
		break;
	}
	case SS_OBJECT:
		if (s->valuetype != SS_STRING)
			goto bad_type;
		s->result = m->value;
		s->resultsize = sizeof(void*);
		break;
	default:
		assert(0);
		return -1;
	}
	return 0;

bad_type:
	return sr_error(s->r->e, "bad meta read type (%s) -> (%s) %s",
	                ss_typeof(s->valuetype),
	                ss_typeof(m->type), s->path);
}

int sr_meta_write(srmeta *m, srmetastmt *s)
{
	if (m->flags & SR_RO) {
		sr_error(s->r->e, "%s is read-only", s->path);
		return -1;
	}
	switch (m->type) {
	case SS_U32:
		if (s->valuetype == SS_I64) {
			*((uint32_t*)m->value) = *(int64_t*)s->value;
		} else
		if (s->valuetype == SS_U32) {
			*((uint32_t*)m->value) = *(uint32_t*)s->value;
		} else
		if (s->valuetype == SS_U64) {
			*((uint32_t*)m->value) = *(uint64_t*)s->value;
		} else {
			goto bad_type;
		}
		break;
	case SS_U64:
		if (s->valuetype == SS_I64) {
			*((uint64_t*)m->value) = *(int64_t*)s->value;
		} else
		if (s->valuetype == SS_U32) {
			*((uint64_t*)m->value) = *(uint32_t*)s->value;
		} else
		if (s->valuetype == SS_U64) {
			*((uint64_t*)m->value) = *(uint64_t*)s->value;
		} else {
			goto bad_type;
		}
		break;
	case SS_STRING: {
		char **string = m->value;
		if (s->valuetype == SS_STRING) {
			char *sz = s->value;
			if (s->valuesize > 0) {
				sz = ss_malloc(s->r->a, s->valuesize);
				if (ssunlikely(sz == NULL))
					return sr_oom(s->r->e);
				memcpy(sz, s->value, s->valuesize);
			}
			if (*string)
				ss_free(s->r->a, *string);
			*string = sz;
		} else {
			goto bad_type;
		}
		break;
	}
	default:
		assert(0);
		return -1;
	}
	return 0;

bad_type:
	return sr_error(s->r->e, "bad meta write type (%s) for (%s) %s",
	                ss_typeof(s->valuetype),
	                ss_typeof(m->type), s->path);
}

#if 0
int sr_meta_serialize(srmeta *m, srmetastmt *s)
{
	void *value = NULL;
	srmetadump v;
	v.type = m->type;
	switch (m->type) {
	case SS_U32:
		v.valuesize = sizeof(uint32_t);
		value = m->value;
		break;
	case SS_U64:
		v.valuesize = sizeof(uint64_t);
		value = m->value;
		break;
	case SS_STRING: {
		char **string = (char**)m->value;
		if (*string)
			v.valuesize = strlen(*string) + 1;
		value = *string;
		break;
	}
	case SS_OBJECT:
	case SS_FUNCTION:
		v.valuesize = 0;
		break;
	default: assert(0);
	}
	char name[128];
	v.namesize  = snprintf(name, sizeof(name), "%s", s->path);
	v.namesize += 1;
	ssbuf *buf = s->serialize;
	int size = sizeof(v) + v.namesize + v.valuesize;
	int rc = ss_bufensure(buf, s->r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(s->r->e);
	memcpy(buf->p, &v, sizeof(v));
	memcpy(buf->p + sizeof(v), name, v.namesize);
	memcpy(buf->p + sizeof(v) + v.namesize, value, v.valuesize);
	ss_bufadvance(buf, size);
	return 0;
}
#endif

int sr_meta_serialize(srmeta *m, srmetastmt *s)
{
	char buf[128];
	char name_object[] = "object";
	char name_function[] = "object";
	void *value = NULL;
	srmetadump v = {
		.type = m->type
	};
	switch (m->type) {
	case SS_U32:
		v.valuesize  = snprintf(buf, sizeof(buf), "%" PRIu32, *(uint32_t*)m->value);
		v.valuesize += 1;
		value = buf;
		break;
	case SS_U64:
		v.valuesize  = snprintf(buf, sizeof(buf), "%" PRIu64, *(uint64_t*)m->value);
		v.valuesize += 1;
		value = buf;
		break;
	case SS_I64:
		v.valuesize  = snprintf(buf, sizeof(buf), "%" PRIi64, *(int64_t*)m->value);
		v.valuesize += 1;
		value = buf;
		break;
	case SS_STRING: {
		char **string = (char**)m->value;
		if (*string) {
			v.valuesize = strlen(*string) + 1;
			value = *string;
		}
		break;
	}
	case SS_OBJECT:
		v.valuesize = sizeof(name_object);
		value = name_object;
		break;
	case SS_FUNCTION:
		v.valuesize = sizeof(name_function);
		value = name_function;
		break;
	default: assert(0);
	}
	char name[128];
	v.namesize  = snprintf(name, sizeof(name), "%s", s->path);
	v.namesize += 1;
	ssbuf *p = s->serialize;
	int size = sizeof(v) + v.namesize + v.valuesize;
	int rc = ss_bufensure(p, s->r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(s->r->e);
	memcpy(p->p, &v, sizeof(v));
	memcpy(p->p + sizeof(v), name, v.namesize);
	memcpy(p->p + sizeof(v) + v.namesize, value, v.valuesize);
	ss_bufadvance(p, size);
	return 0;
}
