#ifndef SF_SCHEME_H_
#define SF_SCHEME_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sffield sffield;
typedef struct sfscheme sfscheme;

typedef int (*sfcmpf)(char*, int, char*, int, void*);

struct sffield {
	sstype    type;
	int       position;
	int       position_ref;
	int       position_key;
	uint32_t  fixed_size;
	uint32_t  fixed_offset;
	char     *name;
	char     *options;
	int       key;
	sfcmpf    cmp;
};

struct sfscheme {
	sffield **fields;
	sffield **keys;
	int       fields_count;
	int       keys_count;
	sfcmpf    cmp;
	void     *cmparg;
	int       var_offset;
	int       var_count;
};

static inline sffield*
sf_fieldnew(ssa *a, char *name)
{
	sffield *f = ss_malloc(a, sizeof(sffield));
	if (ssunlikely(f == NULL))
		return NULL;
	f->key = 0;
	f->fixed_size = 0;
	f->fixed_offset = 0;
	f->position = 0;
	f->position_ref = 0;
	f->name = ss_strdup(a, name);
	if (ssunlikely(f->name == NULL)) {
		ss_free(a, f);
		return NULL;
	}
	f->type = SS_UNDEF;
	f->options = NULL;
	f->cmp = NULL;
	return f;
}

static inline void
sf_fieldfree(sffield *f, ssa *a)
{
	if (f->name) {
		ss_free(a, f->name);
		f->name = NULL;
	}
	if (f->options) {
		ss_free(a, f->options);
		f->options = NULL;
	}
	ss_free(a, f);
}

static inline int
sf_fieldoptions(sffield *f, ssa *a, char *options)
{
	char *sz = ss_strdup(a, options);
	if (ssunlikely(sz == NULL))
		return -1;
	if (f->options)
		ss_free(a, f->options);
	f->options = sz;
	return 0;
}

void sf_schemeinit(sfscheme*);
void sf_schemefree(sfscheme*, ssa*);
int  sf_schemeadd(sfscheme*, ssa*, sffield*);
int  sf_schemevalidate(sfscheme*, ssa*);
int  sf_schemesave(sfscheme*, ssa*, ssbuf*);
int  sf_schemeload(sfscheme*, ssa*, char*, int);
sffield*
sf_schemefind(sfscheme*, char *);

int  sf_schemecompare_prefix(sfscheme*, char*, uint32_t, char*);
int  sf_schemecompare(char*, int, char*, int, void*);

static inline int
sf_compare(sfscheme *s, char *a, int asize, char *b, int bsize)
{
	return s->cmp(a, asize, b, bsize, s->cmparg);
}

static inline int
sf_compareprefix(sfscheme *s, char *a, int asize, char *b, int bsize ssunused)
{
	return sf_schemecompare_prefix(s, a, asize, b);
}

static inline int
sf_schemeeq(sfscheme *a, sfscheme *b)
{
	if (a->fields_count != b->fields_count)
		return 0;
	int i = 0;
	while (i < a->fields_count) {
		sffield *key_a = a->fields[i];
		sffield *key_b = b->fields[i];
		if (key_a->type != key_b->type)
			return 0;
		i++;
	}
	return 1;
}

#endif
