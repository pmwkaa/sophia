
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

srhot int
sr_cmpu32(char *a, size_t asz, char *b, size_t bsz,
          void *arg srunused)
{
	(void)asz;
	(void)bsz;
	register uint32_t av = *(uint32_t*)a;
	register uint32_t bv = *(uint32_t*)b;
	if (av == bv)
		return 0;
	return (av > bv) ? 1 : -1;
}

srhot int
sr_cmpstring(char *a, size_t asz, char *b, size_t bsz,
             void *arg srunused)
{
	register int size = (asz < bsz) ? asz : bsz;
	register int rc = memcmp(a, b, size);
	if (srunlikely(rc == 0)) {
		if (srlikely(asz == bsz))
			return 0;
		return (asz < bsz) ? -1 : 1;
	}
	return rc > 0 ? 1 : -1;
}

int sr_cmpset(srcomparator *c, char *name)
{
	if (strcmp(name, "u32") == 0) {
		c->cmp = sr_cmpu32;
		return 0;
	}
	if (strcmp(name, "string") == 0) {
		c->cmp = sr_cmpstring;
		return 0;
	}
	errno = 0;
	char *end;
	uintptr_t pointer = strtoull(name, &end, 16);
	if (pointer == 0 && end == name) {
		return -1;
	} else
	if (pointer == ULLONG_MAX && errno) {
		return -1;
	}
	c->cmp = (srcmpf)pointer;
	c->cmparg = 0;
	return 0;
}

int sr_cmpsetarg(srcomparator *c, char *name)
{
	errno = 0;
	char *end;
	uintptr_t pointer = strtoull(name, &end, 16);
	if (pointer == 0 && end == name) {
		return -1;
	} else
	if (pointer == ULLONG_MAX && errno) {
		return -1;
	}
	c->cmparg = (char*)pointer;
	return 0;
}
