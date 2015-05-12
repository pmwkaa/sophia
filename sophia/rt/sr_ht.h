#ifndef SR_HT_H_
#define SR_HT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srhtnode srhtnode;
typedef struct srht srht;

struct srhtnode {
	uint32_t hash;
};

struct srht {
	srhtnode **i;
	int count;
	int size;
};

static inline int
sr_htinit(srht *t, sra *a, int size)
{
	t->count = 0;
	t->size = size;
	int sz = size * sizeof(srhtnode*);
	t->i = (srhtnode**)sr_malloc(a, sz);
	if (srunlikely(t->i == NULL))
		return -1;
	memset(t->i, 0, sz);
	return 0;
}

static inline void
sr_htfree(srht *t, sra *a)
{
	if (srunlikely(t->i == NULL))
		return;
	sr_free(a, t->i);
	t->i = NULL;
}

static inline void
sr_htreset(srht *t)
{
	int sz = t->size * sizeof(srhtnode*);
	memset(t->i, 0, sz);
	t->count = 0;
}

static inline int
sr_htisfull(srht *t)
{
	return t->count > (t->size / 2);
}

static inline int
sr_htplace(srht *t, srhtnode *node)
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
sr_htresize(srht *t, sra *a)
{
	srht nt;
	int rc = sr_htinit(&nt, a, t->size * 2);
	if (srunlikely(rc == -1))
		return -1;
	int i = 0;
	while (i < t->size) {
		if (t->i[i]) {
			int pos = sr_htplace(&nt, t->i[i]);
			nt.i[pos] = t->i[i];
		}
		i++;
	}
	nt.count = t->count;
	sr_htfree(t, a);
	*t = nt;
	return 0;
}

#define sr_htsearch(name, compare) \
static inline int \
name(srht *t, uint32_t hash, \
     char *key srunused, \
     uint32_t size srunused, void *ptr srunused) \
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
sr_htset(srht *t, int pos, srhtnode *node)
{
	if (t->i[pos] == NULL)
		t->count++;
	t->i[pos] = node;
}

#endif
