
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

static inline sshot int
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

static inline sshot int
sr_compare_u32(const char *a, const char *b)
{
	uint32_t av = sscastu32(a);
	uint32_t bv = sscastu32(b);
	if (av == bv)
		return 0;
	return (av > bv) ? 1 : -1;
}

static inline sshot int
sr_cmpu32_raw(char *a, int asz ssunused, char *b, int bsz ssunused, void *arg ssunused)
{
	return sr_compare_u32(a, b);
}

static inline sshot int
sr_cmpu32(char *a, int asz ssunused, char *b, int bsz ssunused, void *arg ssunused)
{
	int part = ((srkey*)arg)->pos;
	a = sf_key(a, part);
	b = sf_key(b, part);
	return sr_compare_u32(a, b);
}

static inline sshot int
sr_cmpu32_raw_reverse(char *a, int asz ssunused, char *b, int bsz ssunused, void *arg ssunused)
{
	return -sr_compare_u32(a, b);
}

static inline sshot int
sr_cmpu32_reverse(char *a, int asz ssunused, char *b, int bsz ssunused, void *arg ssunused)
{
	int part = ((srkey*)arg)->pos;
	a = sf_key(a, part);
	b = sf_key(b, part);
	return -sr_compare_u32(a, b);
}

static inline sshot int sr_compare_u64(const char *a, const char *b)
{
	uint64_t av = sscastu64(a);
	uint64_t bv = sscastu64(b);
	if (av == bv)
		return 0;
	return (av > bv) ? 1 : -1;
}

static inline sshot int
sr_cmpu64_raw(char *a, int asz ssunused, char *b, int bsz ssunused,
              void *arg ssunused)
{
	return sr_compare_u64(a, b);
}

static inline sshot int
sr_cmpu64(char *a, int asz ssunused, char *b, int bsz ssunused, void *arg)
{
	int part = ((srkey*)arg)->pos;
	a = sf_key(a, part);
	b = sf_key(b, part);
	return sr_compare_u64(a, b);
}

static inline sshot int
sr_cmpu64_raw_reverse(char *a, int asz ssunused, char *b, int bsz ssunused,
              void *arg ssunused)
{
	return -sr_compare_u64(a, b);
}

static inline sshot int
sr_cmpu64_reverse(char *a, int asz ssunused, char *b, int bsz ssunused, void *arg)
{
	int part = ((srkey*)arg)->pos;
	a = sf_key(a, part);
	b = sf_key(b, part);
	return -sr_compare_u64(a, b);
}

static inline sshot int
sr_cmpstring_prefix(char *prefix, int prefixsz, char *key, int keysz,
                    void *arg ssunused)
{
	keysz = sf_keysize(key, 0);
	key   = sf_key(key, 0);
	if (keysz < prefixsz)
		return 0;
	return (memcmp(prefix, key, prefixsz) == 0) ? 1 : 0;
}

#define sr_compare_string(a, asz, b, bsz) \
do { \
	int size = (asz < bsz) ? asz : bsz; \
	int rc = memcmp(a, b, size); \
	if (ssunlikely(rc == 0)) { \
		if (sslikely(asz == bsz)) \
			return 0; \
		return (asz < bsz) ? -1 : 1; \
	} \
	return rc > 0 ? 1 : -1; \
} while (0)

static inline sshot int
sr_cmpstring_raw(char *a, int asz, char *b, int bsz, void *arg ssunused)
{
	sr_compare_string(a, asz, b, bsz);
}

static inline sshot int
sr_cmpstring(char *a, int asz, char *b, int bsz, void *arg)
{
	int part = ((srkey*)arg)->pos;
	asz = sf_keysize(a, part);
	a   = sf_key(a, part);
	bsz = sf_keysize(b, part);
	b   = sf_key(b, part);
	sr_compare_string(a, asz, b, bsz);
}

inline sshot int
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

inline sshot int
sr_schemecompare_prefix(char *prefix, int prefixsize, char *key, int keysize,
                     void *arg)
{
	srkey *part = &((srscheme*)arg)->parts[0];
	return part->cmpprefix(prefix, prefixsize, key, keysize, part);
}

int sr_keysetname(srkey *part, ssa *a, char *name)
{
	char *p = ss_strdup(a, name);
	if (ssunlikely(p == NULL))
		return -1;
	if (part->name)
		ss_free(a, part->name);
	part->name = p;
	return 0;
}

int sr_keyset(srkey *part, ssa *a, char *path)
{
	sstype type;
	srcmpf cmpprefix = sr_cmpany;
	srcmpf cmp;
	srcmpf cmpraw;
	if (strcmp(path, "string") == 0) {
		type = SS_STRING;
		cmp = sr_cmpstring;
		cmpraw = sr_cmpstring_raw;
		cmpprefix = sr_cmpstring_prefix;
	} else
	if (strcmp(path, "u32") == 0) {
		type = SS_U32;
		cmp = sr_cmpu32;
		cmpraw = sr_cmpu32_raw;
	} else
	if (strcmp(path, "u32_rev") == 0) {
		type = SS_U32;
		cmp = sr_cmpu32_reverse;
		cmpraw = sr_cmpu32_raw_reverse;
	} else
	if (strcmp(path, "u64") == 0) {
		type = SS_U64;
		cmp = sr_cmpu64;
		cmpraw = sr_cmpu64_raw;
	} else
	if (strcmp(path, "u64_rev") == 0) {
		type = SS_U64;
		cmp = sr_cmpu64_reverse;
		cmpraw = sr_cmpu64_raw_reverse;
	} else {
		return -1;
	}
	char *p = ss_strdup(a, path);
	if (ssunlikely(p == NULL))
		return -1;
	if (part->path)
		ss_free(a, part->path);
	part->type = type;
	part->path = p;
	part->cmpprefix = cmpprefix;
	part->cmp = cmp;
	part->cmpraw = cmpraw;
	return 0;
}

int sr_schemesave(srscheme *s, ssa *a, ssbuf *buf)
{
	/* count */
	uint32_t v = s->count;
	int rc = ss_bufadd(buf, a, &v, sizeof(uint32_t));
	if (ssunlikely(rc == -1))
		return -1;
	int i = 0;
	while (i < s->count) {
		srkey *key = &s->parts[i];
		/* name */
		v = strlen(key->name) + 1;
		rc = ss_bufensure(buf, a, sizeof(uint32_t) + v);
		if (ssunlikely(rc == -1))
			goto error;
		memcpy(buf->p, &v, sizeof(v));
		ss_bufadvance(buf, sizeof(uint32_t));
		memcpy(buf->p, key->name, v);
		ss_bufadvance(buf, v);
		/* path */
		v = strlen(key->path) + 1;
		rc = ss_bufensure(buf, a, sizeof(uint32_t) + v);
		if (ssunlikely(rc == -1))
			goto error;
		memcpy(buf->p, &v, sizeof(v));
		ss_bufadvance(buf, sizeof(uint32_t));
		memcpy(buf->p, key->path, v);
		ss_bufadvance(buf, v);
		i++;
	}
	return 0;
error:
	ss_buffree(buf, a);
	return -1;
}

int sr_schemeload(srscheme *s, ssa *a, char *buf, int size ssunused)
{
	/* count */
	char *p = buf;
	uint32_t v = sscastu32(p);
	p += sizeof(uint32_t);
	int count = v;
	int i = 0;
	int rc;
	while (i < count) {
		srkey *key = sr_schemeadd(s);
		if (ssunlikely(key == NULL))
			goto error;
		/* name */
		v = sscastu32(p);
		p += sizeof(uint32_t);
		rc = sr_keysetname(key, a, p);
		if (ssunlikely(rc == -1))
			goto error;
		p += v;
		/* path */
		v = sscastu32(p);
		p += sizeof(uint32_t);
		rc = sr_keyset(key, a, p);
		if (ssunlikely(rc == -1))
			goto error;
		p += v;
		i++;
	}
	return 0;
error:
	sr_schemefree(s, a);
	return -1;
}
