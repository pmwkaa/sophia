
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

int sr_conf_read(srconf *m, srconfstmt *s)
{
	switch (m->type) {
	case SS_U32:
		s->valuesize = sizeof(uint32_t);
		if (s->valuetype == SS_I64) {
			sscasti64(s->value) = sscastu32(m->value);
		} else
		if (s->valuetype == SS_U32) {
			sscastu32(s->value) = sscastu32(m->value);
		} else
		if (s->valuetype == SS_U64) {
			sscastu64(s->value) = sscastu32(m->value);
		} else {
			goto bad_type;
		}
		break;
	case SS_U64:
		s->valuesize = sizeof(uint64_t);
		if (s->valuetype == SS_I64) {
			sscasti64(s->value) = sscastu64(m->value);
		} else
		if (s->valuetype == SS_U32) {
			sscastu32(s->value) = sscastu64(m->value);
		} else
		if (s->valuetype == SS_U64) {
			sscastu64(s->value) = sscastu64(m->value);
		} else {
			goto bad_type;
		}
		break;
	case SS_STRING: {
		if (s->valuetype != SS_STRING)
			goto bad_type;
		char **result = s->value;
		*result = NULL;
		s->valuesize = 0;
		char *string = m->value;
		if (string == NULL)
			break;
		int size = strlen(string) + 1; 
		s->valuesize = size;
		*result = malloc(size);
		if (ssunlikely(*result == NULL))
			return sr_oom(s->r->e);
		memcpy(*result, string, size);
		break;
	}
	case SS_STRINGPTR: {
		if (s->valuetype != SS_STRING)
			goto bad_type;
		char **result = s->value;
		*result = NULL;
		s->valuesize = 0;
		char **string = m->value;
		if (*string == NULL)
			break;
		int size = strlen(*string) + 1;
		s->valuesize = size;
		*result = malloc(size);
		if (ssunlikely(*result == NULL))
			return sr_oom(s->r->e);
		memcpy(*result, *string, size);
		break;
	}
	case SS_OBJECT:
		if (s->valuetype != SS_STRING)
			goto bad_type;
		*(void**)s->value = m->value;
		s->valuesize = sizeof(void*);
		break;
	default:
		goto bad_type;
	}

	return 0;

bad_type:
	return sr_error(s->r->e, "configuration read bad type (%s) -> (%s) %s",
	                ss_typeof(s->valuetype),
	                ss_typeof(m->type), s->path);
}

int sr_conf_write(srconf *m, srconfstmt *s)
{
	if (m->flags & SR_RO) {
		sr_error(s->r->e, "%s is read-only", s->path);
		return -1;
	}
	switch (m->type) {
	case SS_U32:
		if (s->valuetype == SS_I64) {
			sscastu32(m->value) = sscasti64(s->value);
		} else
		if (s->valuetype == SS_U32) {
			sscastu32(m->value) = sscastu32(s->value);
		} else
		if (s->valuetype == SS_U64) {
			sscastu32(m->value) = sscastu64(s->value);
		} else {
			goto bad_type;
		}
		break;
	case SS_U64:
		if (s->valuetype == SS_I64) {
			sscastu64(m->value) = sscasti64(s->value);
		} else
		if (s->valuetype == SS_U32) {
			sscastu64(m->value) = sscastu32(s->value);
		} else
		if (s->valuetype == SS_U64) {
			sscastu64(m->value) = sscastu64(s->value);
		} else {
			goto bad_type;
		}
		break;
	case SS_STRINGPTR: {
		char **string = m->value;
		if (s->valuetype == SS_STRING) {
			int len = s->valuesize + 1;
			char *sz;
			sz = ss_malloc(s->r->a, len);
			if (ssunlikely(sz == NULL))
				return sr_oom(s->r->e);
			memcpy(sz, s->value, s->valuesize);
			sz[s->valuesize] = 0;
			if (*string)
				ss_free(s->r->a, *string);
			*string = sz;
		} else {
			goto bad_type;
		}
		break;
	}
	default:
		goto bad_type;
	}
	return 0;

bad_type:
	return sr_error(s->r->e, "configuration write bad type (%s) for (%s) %s",
	                ss_typeof(s->valuetype),
	                ss_typeof(m->type), s->path);
}

static inline int
sr_conf_write_cast(sstype a, sstype b)
{
	switch (a) {
	case SS_U32:
		if (b == SS_I64) {
		} else
		if (b == SS_U32) {
		} else
		if (b == SS_U64) {
		} else {
			return -1;
		}
		break;
	case SS_U64:
		if (b == SS_I64) {
		} else
		if (b == SS_U32) {
		} else
		if (b == SS_U64) {
		} else {
			return -1;
		}
		break;
	case SS_STRING:
	case SS_STRINGPTR:
		if (b == SS_STRING) {
		} else {
			return -1;
		}
		break;
	default:
		return -1;
	}
	return 0;
}

int sr_conf_serialize(srconf *m, srconfstmt *s)
{
	char buf[128];
	char name_function[] = "function";
	char name_object[] = "object";
	void *value = NULL;
	srconfdump v = {
		.type = m->type
	};
	switch (m->type) {
	case SS_U32:
		v.valuesize  = snprintf(buf, sizeof(buf), "%" PRIu32, sscastu32(m->value));
		v.valuesize += 1;
		value = buf;
		break;
	case SS_U64:
		v.valuesize  = snprintf(buf, sizeof(buf), "%" PRIu64, sscastu64(m->value));
		v.valuesize += 1;
		value = buf;
		break;
	case SS_I64:
		v.valuesize  = snprintf(buf, sizeof(buf), "%" PRIi64, sscasti64(m->value));
		v.valuesize += 1;
		value = buf;
		break;
	case SS_STRING: {
		char *string = m->value;
		if (string) {
			v.valuesize = strlen(string) + 1;
			value = string;
		} else {
			v.valuesize = 0;
		}
		break;
	}
	case SS_STRINGPTR: {
		char **string = (char**)m->value;
		if (*string) {
			v.valuesize = strlen(*string) + 1;
			value = *string;
		} else {
			v.valuesize = 0;
		}
		v.type = SS_STRING;
		break;
	}
	case SS_OBJECT:
		v.type = SS_STRING;
		v.valuesize = sizeof(name_object);
		value = name_object;
		break;
	case SS_FUNCTION:
		v.type = SS_STRING;
		v.valuesize = sizeof(name_function);
		value = name_function;
		break;
	default:
		return -1;
	}
	char name[128];
	v.keysize  = snprintf(name, sizeof(name), "%s", s->path);
	v.keysize += 1;
	ssbuf *p = s->serialize;
	int size = sizeof(v) + v.keysize + v.valuesize;
	int rc = ss_bufensure(p, s->r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(s->r->e);
	memcpy(p->p, &v, sizeof(v));
	memcpy(p->p + sizeof(v), name, v.keysize);
	memcpy(p->p + sizeof(v) + v.keysize, value, v.valuesize);
	ss_bufadvance(p, size);
	return 0;
}

static inline int
sr_confexec_serialize(srconf *c, srconfstmt *stmt, char *root)
{
	char path[256];
	while (c) {
		if (root)
			snprintf(path, sizeof(path), "%s.%s", root, c->key);
		else
			snprintf(path, sizeof(path), "%s", c->key);
		int rc;
		if (c->flags & SR_NS) {
			rc = sr_confexec_serialize(c->value, stmt, path);
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

int sr_confexec(srconf *start, srconfstmt *s)
{
	if (s->op == SR_SERIALIZE)
		return sr_confexec_serialize(start, s, NULL);
	char path[256];
	snprintf(path, sizeof(path), "%s", s->path);
	char *ptr = NULL;
	char *token;
	token = strtok_r(path, ".", &ptr);
	if (ssunlikely(token == NULL))
		return -1;
	srconf *c = start;
	while (c) {
		if (strcmp(token, c->key) != 0) {
			c = c->next;
			continue;
		}
		if (c->flags & SR_NS) {
			token = strtok_r(NULL, ".", &ptr);
			if (ssunlikely(token == NULL))
			{
				if (s->op == SR_WRITE && c->type != SS_UNDEF) {
					int rc = sr_conf_write_cast(c->type, s->valuetype);
					if (ssunlikely(rc == -1))
						goto bad_type;
				}
				s->match = c;
				if (c->function)
					return c->function(c, s);
				/* not supported */
				goto bad_path;
			}
			c = (srconf*)c->value;
			continue;
		}
		s->match = c;
		token = strtok_r(NULL, ".", &ptr);
		if (ssunlikely(token != NULL))
			goto bad_path;
		return c->function(c, s);
	}

bad_path:
	return sr_error(s->r->e, "bad configuration path: %s", s->path);

bad_type:
	return sr_error(s->r->e, "incompatible type (%s) for (%s) %s",
	                ss_typeof(s->valuetype),
	                ss_typeof(c->type), s->path);
}
