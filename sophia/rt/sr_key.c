
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

static inline srhot int
sr_cmpany(char *prefix, int prefixsz,
          char *key, int keysz,
          void *arg)
{
	(void)prefix;
	(void)prefixsz;
	(void)key;
	(void)keysz;
	(void)arg;
	return 0;
}

#define sr_compare_u32(a, b) \
do { \
	uint32_t av = *(uint32_t*)a; \
	uint32_t bv = *(uint32_t*)b; \
	if (av == bv) \
		return 0; \
	return (av > bv) ? 1 : -1; \
} while (0)

static inline srhot int
sr_cmpu32_raw(char *a, int asz srunused, char *b, int bsz srunused, void *arg srunused)
{
	sr_compare_u32(a, b);
}

static inline srhot int
sr_cmpu32(char *a, int asz srunused, char *b, int bsz srunused, void *arg srunused)
{
	int part = ((srkeypart*)arg)->pos;
	a = sr_fmtkey(a, part);
	b = sr_fmtkey(b, part);
	sr_compare_u32(a, b);
}

#define sr_compare_u64(a, b) \
do { \
	uint64_t av = *(uint64_t*)a; \
	uint64_t bv = *(uint64_t*)b; \
	if (av == bv) \
		return 0; \
	return (av > bv) ? 1 : -1; \
} while (0)

static inline srhot int
sr_cmpu64_raw(char *a, int asz srunused, char *b, int bsz srunused,
              void *arg srunused)
{
	sr_compare_u64(a, b);
}

static inline srhot int
sr_cmpu64(char *a, int asz srunused, char *b, int bsz srunused, void *arg)
{
	int part = ((srkeypart*)arg)->pos;
	a = sr_fmtkey(a, part);
	b = sr_fmtkey(b, part);
	sr_compare_u64(a, b);
}

static inline srhot int
sr_cmpstring_prefix(char *prefix, int prefixsz, char *key, int keysz,
                    void *arg srunused)
{
	keysz = sr_fmtkey_size(key, 0);
	key   = sr_fmtkey(key, 0);
	if (keysz < prefixsz)
		return 0;
	return (memcmp(prefix, key, prefixsz) == 0) ? 1 : 0;
}

#define sr_compare_string(a, asz, b, bsz) \
do { \
	int size = (asz < bsz) ? asz : bsz; \
	int rc = memcmp(a, b, size); \
	if (srunlikely(rc == 0)) { \
		if (srlikely(asz == bsz)) \
			return 0; \
		return (asz < bsz) ? -1 : 1; \
	} \
	return rc > 0 ? 1 : -1; \
} while (0)

static inline srhot int
sr_cmpstring_raw(char *a, int asz, char *b, int bsz, void *arg srunused)
{
	sr_compare_string(a, asz, b, bsz);
}

static inline srhot int
sr_cmpstring(char *a, int asz, char *b, int bsz, void *arg)
{
	int part = ((srkeypart*)arg)->pos;
	asz = sr_fmtkey_size(a, part);
	a   = sr_fmtkey(a, part);
	bsz = sr_fmtkey_size(b, part);
	b   = sr_fmtkey(b, part);
	sr_compare_string(a, asz, b, bsz);
}

inline srhot int
sr_keycompare(char *a, int asize, char *b, int bsize, void *arg)
{
	srkey *key = arg;
	srkeypart *part = key->parts;
	srkeypart *last = part + key->count;
	int rc;
	while (part < last) {
		rc = part->cmp(a, asize, b, bsize, part);
		if (rc != 0)
			return rc;
		part++;
	}
	return 0;
}

inline srhot int
sr_keycompare_prefix(char *prefix, int prefixsize, char *key, int keysize,
                     void *arg)
{
	srkeypart *part = &((srkey*)arg)->parts[0];
	return part->cmpprefix(prefix, prefixsize, key, keysize, part);
}

int sr_keypart_setname(srkeypart *part, sra *a, char *name)
{
	char *p = sr_strdup(a, name);
	if (srunlikely(p == NULL))
		return -1;
	if (part->name)
		sr_free(a, part->name);
	part->name = p;
	return 0;
}

int sr_keypart_set(srkeypart *part, sra *a, char *path)
{
	srcmpf cmpprefix = sr_cmpany;
	srcmpf cmp;
	srcmpf cmpraw;
	srkeytype type;
	if (strcmp(path, "string") == 0) {
		type = SR_STRING;
		cmp = sr_cmpstring;
		cmpraw = sr_cmpstring_raw;
		cmpprefix = sr_cmpstring_prefix;
	} else
	if (strcmp(path, "u32") == 0) {
		type = SR_U32;
		cmp = sr_cmpu32;
		cmpraw = sr_cmpu32_raw;
	} else
	if (strcmp(path, "u64") == 0) {
		type = SR_U64;
		cmp = sr_cmpu64;
		cmpraw = sr_cmpu64_raw;
	} else {
		return -1;
	}
	char *p = sr_strdup(a, path);
	if (srunlikely(p == NULL))
		return -1;
	if (part->path)
		sr_free(a, part->path);
	part->type = type;
	part->path = p;
	part->cmpprefix = cmpprefix;
	part->cmp = cmp;
	part->cmpraw = cmpraw;
	return 0;
}
