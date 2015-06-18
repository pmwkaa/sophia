#ifndef SR_SCHEME_H_
#define SR_SCHEME_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srkey srkey;
typedef struct srscheme srscheme;

typedef int (*srcmpf)(char*, int, char*, int, void*);

struct srkey {
	char *name;
	char *path;
	sstype type;
	int pos;
	srcmpf cmpprefix;
	srcmpf cmpraw;
	srcmpf cmp;
};

struct srscheme {
	srkey *parts;
	int count;
	srcmpf cmp;
	srcmpf cmpprefix;
	void *cmparg;
	void *cmpprefix_arg;
};

int sr_schemecompare_prefix(char*, int, char*, int, void*);
int sr_schemecompare(char*, int, char*, int, void*);

static inline void
sr_schemeinit(srscheme *s)
{
	s->parts = NULL;
	s->count = 0;
	s->cmp = sr_schemecompare;
	s->cmparg = s;
	s->cmpprefix = sr_schemecompare_prefix;
	s->cmpprefix_arg = s;
}

static inline void
sr_schemefree(srscheme *s, ssa *a)
{
	if (s->parts == NULL)
		return;
	int i = 0;
	while (i < s->count) {
		if (s->parts[i].name)
			ss_free(a, s->parts[i].name);
		if (s->parts[i].path)
			ss_free(a, s->parts[i].path);
		i++;
	}
	ss_free(a, s->parts);
	s->parts = NULL;
}

static inline void
sr_schemesetcmp(srscheme *s, srcmpf cmp, void *arg)
{
	s->cmp = cmp;
	s->cmparg = arg;
}

static inline void
sr_schemesetcmp_prefix(srscheme *s, srcmpf cmp, void *arg)
{
	s->cmpprefix = cmp;
	s->cmpprefix_arg = arg;
}

static inline srkey*
sr_schemeadd(srscheme *s, ssa *a)
{
	srkey *parts = ss_malloc(a, sizeof(srkey) * (s->count + 1));
	if (ssunlikely(parts == NULL))
		return NULL;
	memcpy(parts, s->parts, sizeof(srkey) * s->count);
	if (s->parts)
		ss_free(a, s->parts);
	s->parts = parts;
	int pos = s->count++;
	srkey *part = &s->parts[pos];
	memset(part, 0, sizeof(*part));
	part->pos = pos;
	return part;
}

static inline int
sr_schemedelete(srscheme *s, ssa *a, int pos)
{
	srkey *parts = ss_malloc(a, sizeof(srkey) * (s->count - 1));
	if (ssunlikely(parts == NULL))
		return -1;
	int i = 0;
	int j = 0;
	while (i < s->count)
	{
		if (i == pos) {
			if (s->parts[i].name)
				ss_free(a, s->parts[i].name);
			if (s->parts[i].path)
				ss_free(a, s->parts[i].path);
			i++;
			continue;
		}
		parts[j++] = s->parts[i];
		i++;
	}
	if (s->parts)
		ss_free(a, s->parts);
	s->parts = parts;
	s->count -= 1;
	return 0;
}

static inline srkey*
sr_schemefind(srscheme *s, char *name)
{
	int i = 0;
	while (i < s->count) {
		if (strcmp(s->parts[i].name, name) == 0)
			return &s->parts[i];
		i++;
	}
	return NULL;
}

static inline srkey*
sr_schemeof(srscheme *s, int pos)
{
	assert(pos < s->count);
	return &s->parts[pos];
}

int sr_keysetname(srkey*, ssa*, char*);
int sr_keyset(srkey*, ssa*, char*);

int sr_schemesave(srscheme*, ssa*, ssbuf*);
int sr_schemeload(srscheme*, ssa*, char*, int);

static inline int
sr_compare(srscheme *s, char *a, int asize, char *b, int bsize) {
	return s->cmp(a, asize, b, bsize, s->cmparg);
}

static inline int
sr_compareprefix(srscheme *s, char *prefix, int prefixsize, char *b, int bsize)
{
	return s->cmpprefix(prefix, prefixsize, b, bsize, s->cmpprefix_arg);
}

#endif
