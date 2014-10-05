
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
	case SR_CTLSTRING: {
		char *nsz = sr_strdup(a, value);
		if (srunlikely(nsz == NULL))
			return -1;
		char **sz = (char**)c->v;
		if (*sz)
			sr_free(a, *sz);
		*sz = nsz;
		break;
	}
	case SR_CTLSUB: return -1;
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
