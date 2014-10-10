
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

static inline size_t sr_atoi(char *s)
{
	size_t v = 0;
	while (*s && *s != '.') {
		if (srunlikely(!isdigit(*s)))
			return -1;
		v = (v * 10) + *s - '0';
		s++;
	}
	return v;
}

int sr_ctlset(srctl *c, sra *a, void *arg, va_list args)
{
	int type = c->type & ~SR_CTLRO;
	if (srunlikely(type == SR_CTLTRIGGER))
		return c->func(c, arg, args);
	if (c->type & SR_CTLRO)
		return -1;
	char *value = va_arg(args, char*);
	switch (type) {
	case SR_CTLINT: *((int*)c->v) = sr_atoi(value);
		break;
	case SR_CTLU32: *((uint32_t*)c->v) = sr_atoi(value);
		break;
	case SR_CTLU64: *((uint64_t*)c->v) = sr_atoi(value);
		break;
	case SR_CTLSTRINGREF: {
		char *nsz = sr_strdup(a, value);
		if (srunlikely(nsz == NULL))
			return -1;
		char **sz = (char**)c->v;
		if (*sz)
			sr_free(a, *sz);
		*sz = nsz;
		break;
	}
	case SR_CTLSTRING: assert(0);
	case SR_CTLSUB:
		return -1;
	}
	return 0;
}

int sr_ctlget(srctl *list, char **path, srctl **result)
{
	char *token;
	token = strtok_r(NULL, ".", path);
	if (srunlikely(token == NULL))
		return 1;
	srctl *c = list;
	while (c->name) {
		if (strcmp(token, c->name) != 0) {
			c++;
			continue;
		}
		*result = c;
		return 0;
	}
	return -1;
}

static int
sr_ctldump(srctl *c, sra *a, char *prefix, srbuf *buf)
{
	srctldump dump = {
		.type     = c->type,
		.namelen  = 0,
		.valuelen = 0
	};
	int type = c->type & ~SR_CTLRO;
	void *value;
	switch (type) {
	case SR_CTLINT:
		dump.valuelen = sizeof(int);
		value = c->v;
		break;
	case SR_CTLU32:
		dump.valuelen = sizeof(uint32_t);
		value = c->v;
		break;
	case SR_CTLU64:
		dump.valuelen = sizeof(uint64_t);
		value = c->v;
		break;
	case SR_CTLSTRINGREF: {
		char **sz = (char**)c->v;
		if (*sz)
			dump.valuelen = strlen(*sz) + 1;
		value = *sz;
		dump.type = SR_CTLSTRING;
		break;
	}
	case SR_CTLSTRING:
		value = c->v;
		if (value)
			dump.valuelen = strlen(value) + 1;
		break;
	case SR_CTLTRIGGER:
		dump.valuelen = 0;
		value = NULL;
		break;
	case SR_CTLSUB:
		return 0;
	}
	char name[128];
	dump.namelen  = snprintf(name, sizeof(name), "%s%s", prefix, c->name);
	dump.namelen += 1;
	int size = sizeof(dump) + dump.namelen + dump.valuelen;
	int rc = sr_bufensure(buf, a, size);
	if (srunlikely(rc == -1))
		return -1;
	memcpy(buf->p, &dump, sizeof(dump));
	memcpy(buf->p + sizeof(dump), name, dump.namelen);
	memcpy(buf->p + sizeof(dump) + dump.namelen, value, dump.valuelen);
	sr_bufadvance(buf, size);
	return 0;
}

int sr_ctlserialize(srctl *list, sra *a, char *prefix, srbuf *buf)
{
	srctl *c = list;
	while (c->name) {
		int rc = sr_ctldump(c, a, prefix, buf);
		if (srunlikely(rc == -1))
			return -1;
		c++;
	}
	return 0;
}
