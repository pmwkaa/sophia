
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

static int
sr_cserializer(src *c, srcstmt *stmt, char *root, va_list args)
{
	char path[256];
	while (c) {
		if (root)
			snprintf(path, sizeof(path), "%s.%s", root, c->name);
		else
			snprintf(path, sizeof(path), "%s", c->name);
		int rc;
		int type = c->flags & ~SR_CRO;
		if (type == SR_CC) {
			rc = sr_cserializer(c->value, stmt, path, args);
			if (ssunlikely(rc == -1))
				return -1;
		} else {
			stmt->path = path;
			rc = c->function(c, stmt, args);
			if (ssunlikely(rc == -1))
				return -1;
			stmt->path = NULL;
		}
		c = c->next;
	}
	return 0;
}

int sr_cexecv(src *start, srcstmt *stmt, va_list args)
{
	if (stmt->op == SR_CSERIALIZE)
		return sr_cserializer(start, stmt, NULL, args);

	char path[256];
	snprintf(path, sizeof(path), "%s", stmt->path);
	char *ptr = NULL;
	char *token;
	token = strtok_r(path, ".", &ptr);
	if (ssunlikely(token == NULL))
		return -1;
	src *c = start;
	while (c) {
		if (strcmp(token, c->name) != 0) {
			c = c->next;
			continue;
		}
		int type = c->flags & ~SR_CRO;
		switch (type) {
		case SR_CU32:
		case SR_CU64:
		case SR_CSZREF:
		case SR_CSZ:
		case SR_CVOID:
			token = strtok_r(NULL, ".", &ptr);
			if (ssunlikely(token != NULL))
				goto error;
			return c->function(c, stmt, args);
		case SR_CC:
			token = strtok_r(NULL, ".", &ptr);
			if (ssunlikely(token == NULL))
			{
				if (c->function)
					return c->function(c, stmt, args);
				/* not supported */
				goto error;
			}
			c = (src*)c->value;
			continue;
		}
		assert(0);
	}

error:
	sr_error(stmt->r->e, "bad ctl path: %s", stmt->path);
	return -1;
}

int sr_cserialize(src *c, srcstmt *stmt)
{
	void *value = NULL;
	int type = c->flags & ~SR_CRO;
	srcv v = {
		.type     = type,
		.namelen  = 0,
		.valuelen = 0
	};
	switch (type) {
	case SR_CU32:
		v.valuelen = sizeof(uint32_t);
		value = c->value;
		break;
	case SR_CU64:
		v.valuelen = sizeof(uint64_t);
		value = c->value;
		break;
	case SR_CSZREF: {
		char **sz = (char**)c->value;
		if (*sz)
			v.valuelen = strlen(*sz) + 1;
		value = *sz;
		v.type = SR_CSZ;
		break;
	}
	case SR_CSZ:
		value = c->value;
		if (value)
			v.valuelen = strlen(value) + 1;
		break;
	case SR_CVOID:
		v.valuelen = 0;
		break;
	default: assert(0);
	}
	char name[128];
	v.namelen  = snprintf(name, sizeof(name), "%s", stmt->path);
	v.namelen += 1;
	ssbuf *buf = stmt->serialize;
	int size = sizeof(v) + v.namelen + v.valuelen;
	int rc = ss_bufensure(buf, stmt->r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(stmt->r->e);
	memcpy(buf->p, &v, sizeof(v));
	memcpy(buf->p + sizeof(v), name, v.namelen);
	memcpy(buf->p + sizeof(v) + v.namelen, value, v.valuelen);
	ss_bufadvance(buf, size);
	return 0;
}

static inline ssize_t sr_atoi(char *s)
{
	size_t v = 0;
	while (*s && *s != '.') {
		if (ssunlikely(! isdigit(*s)))
			return -1;
		v = (v * 10) + *s - '0';
		s++;
	}
	return v;
}

int sr_cset(src *c, srcstmt *stmt, char *value)
{
	int type = c->flags & ~SR_CRO;
	if (c->flags & SR_CRO) {
		sr_error(stmt->r->e, "%s is read-only", stmt->path);
		return -1;
	}
	switch (type) {
	case SR_CU32:
		*((uint32_t*)c->value) = sr_atoi(value);
		break;
	case SR_CU64:
		*((uint64_t*)c->value) = sr_atoi(value);
		break;
	case SR_CSZREF: {
		char *nsz = NULL;
		if (value) {
			nsz = ss_strdup(stmt->r->a, value);
			if (ssunlikely(nsz == NULL))
				return sr_oom(stmt->r->e);
		}
		char **sz = (char**)c->value;
		if (*sz)
			ss_free(stmt->r->a, *sz);
		*sz = nsz;
		break;
	}
	default: assert(0);
	}
	return 0;
}
