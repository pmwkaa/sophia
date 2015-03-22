
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

srhot int
sr_cmpstring_prefix(char *prefix, size_t prefixsz, char *key, size_t keysz,
                    void *arg srunused)
{
	if (keysz < prefixsz)
		return 0;
	return (memcmp(prefix, key, prefixsz) == 0) ? 1 : 0;
}

int sr_cmpset(srcomparator *c, char *name)
{
	if (strcmp(name, "u32") == 0) {
		c->cmp = sr_cmpu32;
		c->prefix = NULL;
		c->prefixarg = NULL;
		return 0;
	}
	if (strcmp(name, "u64") == 0) {
		c->cmp = sr_cmpu64;
		c->prefix = NULL;
		c->prefixarg = NULL;
		return 0;
	}
	if (strcmp(name, "string") == 0) {
		c->cmp = sr_cmpstring;
		c->prefix = sr_cmpstring_prefix;
		c->prefixarg = NULL;
		return 0;
	}
	void *ptr = sr_triggerpointer_of(name);
	if (srunlikely(ptr == NULL))
		return -1;
	c->cmp = (srcmpf)(uintptr_t)ptr;
	return 0;
}

int sr_cmpsetarg(srcomparator *c, char *name)
{
	void *ptr = sr_triggerpointer_of(name);
	if (srunlikely(ptr == NULL))
		return -1;
	c->cmparg = ptr;
	return 0;
}

int sr_cmpset_prefix(srcomparator *c, char *name)
{
	if (strcmp(name, "string_prefix") == 0) {
		c->cmp = sr_cmpstring;
		c->prefix = sr_cmpstring_prefix;
		return 0;
	}
	void *ptr = sr_triggerpointer_of(name);
	if (srunlikely(ptr == NULL))
		return -1;
	c->prefix = (srcmpf)(uintptr_t)ptr;
	return 0;
}

int sr_cmpset_prefixarg(srcomparator *c, char *name)
{
	void *ptr = sr_triggerpointer_of(name);
	if (srunlikely(ptr == NULL))
		return -1;
	c->prefixarg = ptr;
	return 0;
}
