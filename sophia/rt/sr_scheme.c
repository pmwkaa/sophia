
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
	int part = ((srkey*)arg)->pos;
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
	int part = ((srkey*)arg)->pos;
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
	int part = ((srkey*)arg)->pos;
	asz = sr_fmtkey_size(a, part);
	a   = sr_fmtkey(a, part);
	bsz = sr_fmtkey_size(b, part);
	b   = sr_fmtkey(b, part);
	sr_compare_string(a, asz, b, bsz);
}

inline srhot int
sr_schemecompare(char *a, int asize, char *b, int bsize, void *arg)
{
	srscheme *s = arg;
	srkey *part = s->parts;
	srkey *last = part + s->count;
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
sr_schemecompare_prefix(char *prefix, int prefixsize, char *key, int keysize,
                     void *arg)
{
	srkey *part = &((srscheme*)arg)->parts[0];
	return part->cmpprefix(prefix, prefixsize, key, keysize, part);
}

int sr_keysetname(srkey *part, sra *a, char *name)
{
	char *p = sr_strdup(a, name);
	if (srunlikely(p == NULL))
		return -1;
	if (part->name)
		sr_free(a, part->name);
	part->name = p;
	return 0;
}

int sr_keyset(srkey *part, sra *a, char *path)
{
	srtype type;
	srcmpf cmpprefix = sr_cmpany;
	srcmpf cmp;
	srcmpf cmpraw;
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

int sr_schemesave(srscheme *s, sra *a, srbuf *buf)
{
	/* count */
	uint32_t v = s->count;
	int rc = sr_bufadd(buf, a, &v, sizeof(uint32_t));
	if (srunlikely(rc == -1))
		return -1;
	int i = 0;
	while (i < s->count) {
		srkey *key = &s->parts[i];
		/* name */
		v = strlen(key->name) + 1;
		rc = sr_bufensure(buf, a, sizeof(uint32_t) + v);
		if (srunlikely(rc == -1))
			goto error;
		memcpy(buf->p, &v, sizeof(v));
		sr_bufadvance(buf, sizeof(uint32_t));
		memcpy(buf->p, key->name, v);
		sr_bufadvance(buf, v);
		/* path */
		v = strlen(key->path) + 1;
		rc = sr_bufensure(buf, a, sizeof(uint32_t) + v);
		if (srunlikely(rc == -1))
			goto error;
		memcpy(buf->p, &v, sizeof(v));
		sr_bufadvance(buf, sizeof(uint32_t));
		memcpy(buf->p, key->path, v);
		sr_bufadvance(buf, v);
		i++;
	}
	return 0;
error:
	sr_buffree(buf, a);
	return -1;
}

int sr_schemeload(srscheme *s, sra *a, char *buf, int size srunused)
{
	/* count */
	char *p = buf;
	uint32_t v = *(uint32_t*)p;
	p += sizeof(uint32_t);
	int count = v;
	int i = 0;
	int rc;
	while (i < count) {
		srkey *key = sr_schemeadd(s, a);
		if (srunlikely(key == NULL))
			goto error;
		/* name */
		v = *(uint32_t*)p;
		p += sizeof(uint32_t);
		rc = sr_keysetname(key, a, p);
		if (srunlikely(rc == -1))
			goto error;
		p += v;
		/* path */
		v = *(uint32_t*)p;
		p += sizeof(uint32_t);
		rc = sr_keyset(key, a, p);
		if (srunlikely(rc == -1))
			goto error;
		p += v;
		i++;
	}
	return 0;
error:
	sr_schemefree(s, a);
	return -1;
}
