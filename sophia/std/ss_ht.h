#ifndef SS_HT_H_
#define SS_HT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sshtnode sshtnode;
typedef struct ssht ssht;

struct sshtnode {
	uint32_t hash;
};

struct ssht {
	sshtnode **i;
	int count;
	int size;
};

static inline int
ss_htinit(ssht *t, ssa *a, int size)
{
	int sz = size * sizeof(sshtnode*);
	t->i = (sshtnode**)ss_malloc(a, sz);
	if (ssunlikely(t->i == NULL))
		return -1;
	t->count = 0;
	t->size = size;
	memset(t->i, 0, sz);
	return 0;
}

static inline void
ss_htfree(ssht *t, ssa *a)
{
	if (ssunlikely(t->i == NULL))
		return;
	ss_free(a, t->i);
	t->i = NULL;
	t->size = 0;
}

static inline void
ss_htreset(ssht *t)
{
	int sz = t->size * sizeof(sshtnode*);
	memset(t->i, 0, sz);
	t->count = 0;
}

static inline int
ss_htisfull(ssht *t)
{
	return t->count > (t->size / 2);
}

static inline int
ss_htplace(ssht *t, sshtnode *node)
{
	uint32_t pos = node->hash % t->size;
	for (;;) {
		if (t->i[pos] != NULL) {
			pos = (pos + 1) % t->size;
			continue;
		}
		return pos;
	}
	return -1;
}

static inline int
ss_htresize(ssht *t, ssa *a)
{
	ssht nt;
	int rc = ss_htinit(&nt, a, t->size * 2);
	if (ssunlikely(rc == -1))
		return -1;
	int i = 0;
	while (i < t->size) {
		if (t->i[i]) {
			int pos = ss_htplace(&nt, t->i[i]);
			nt.i[pos] = t->i[i];
		}
		i++;
	}
	nt.count = t->count;
	ss_htfree(t, a);
	*t = nt;
	return 0;
}

#define ss_htsearch(name, compare) \
static inline int \
name(ssht *t, uint32_t hash, \
     char *key ssunused, \
     uint32_t size ssunused, void *ptr ssunused) \
{ \
	uint32_t pos = hash % t->size; \
	for (;;) { \
		if (t->i[pos] != NULL) { \
			if ( (compare) ) \
				return pos; \
			pos = (pos + 1) % t->size; \
			continue; \
		} \
		return pos; \
	} \
	return -1; \
}

static inline void
ss_htset(ssht *t, int pos, sshtnode *node)
{
	if (t->i[pos] == NULL)
		t->count++;
	t->i[pos] = node;
}

#endif
