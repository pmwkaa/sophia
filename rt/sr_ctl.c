
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

int sr_ctl(srctl **ctl, char *prefix, char *str, va_list args)
{
	int plen = strlen(prefix);
	int slen = strlen(str);
	if (srunlikely(plen > slen))
		return -1;
	if (memcmp(prefix, str, plen) != 0)
		return -1;
	char *path = str + plen;
	char q[200];
	snprintf(q, sizeof(q), "%s", path);
	char *ptr = NULL;
	char *token;
	token = strtok_r(q, ".", &ptr);
	if (srunlikely(token == NULL))
		return -1;
	int i = 0;
	srctl *c = ctl[i];
	while (c->name) {
		if (strcmp(token, c->name) != 0) {
			c = ctl[++i];
			continue;
		}
		switch (c->type) {
		case SR_CTLINT:
			*((int*)c->ptr) = va_arg(args, int);
			break;
		case SR_CTLU32:
			*((uint32_t*)c->ptr) = va_arg(args, int);
			break;
		case SR_CTLU64:
			*((uint64_t*)c->ptr) = va_arg(args, int);
			break;
		case SR_CTLSTRING:
			*((char**)c->ptr) = va_arg(args, char*);
			break;
		}
		c->set = 1;
		return 1;
	}
	return 0;
}
