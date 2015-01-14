
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
sr_cmpu64(char *a, size_t asz, char *b, size_t bsz,
          void *arg srunused)
{
	(void)asz;
	(void)bsz;
	register uint64_t av = *(uint64_t*)a;
	register uint64_t bv = *(uint64_t*)b;
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

static inline void*
sr_cmppointer_of(char *name)
{
	if (strncmp(name, "pointer:", 8) != 0)
		return NULL;
	name += 8;
	errno = 0;
	char *end;
	unsigned long long int pointer = strtoull(name, &end, 16);
	if (pointer == 0 && end == name) {
		return NULL;
	} else
	if (pointer == ULLONG_MAX && errno) {
		return NULL;
	}
	return (void*)(uintptr_t)pointer;
}

int sr_cmpset(srcomparator *c, char *name)
{
	if (strcmp(name, "u32") == 0) {
		c->cmp = sr_cmpu32;
		return 0;
	}
	if (strcmp(name, "u64") == 0) {
		c->cmp = sr_cmpu64;
		return 0;
	}
	if (strcmp(name, "string") == 0) {
		c->cmp = sr_cmpstring;
		return 0;
	}
	void *ptr = sr_cmppointer_of(name);
	if (srunlikely(ptr == NULL))
		return -1;
	c->cmp = (srcmpf)(uintptr_t)ptr;
	return 0;
}

int sr_cmpsetarg(srcomparator *c, char *name)
{
	void *ptr = sr_cmppointer_of(name);
	if (srunlikely(ptr == NULL))
		return -1;
	c->cmparg = ptr;
	return 0;
}
