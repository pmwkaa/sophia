
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#ifndef sri_instance
#error "sri_instance is not defined"
#endif

#ifndef sri_cmp
#error "sri_cmp is not defined"
#endif

#ifndef sri_type
#error "sri_type is not defined"
#endif

#ifndef sri_free
#define sri_free(a, x)
#endif

#define sri(name) sr_template(sri_instance, name)

typedef struct sri(page) sri(page);
typedef struct sri(i) sri(i);
typedef struct sri() sri();

struct sri(page) {
	uint16_t n;
	sri_type i[];
} srpacked;

struct sri() {
	sra *a;
	int pagesize;
	int half;
	sri(page) **i;
	uint32_t itop;
	uint32_t in;
	uint32_t n;
	srcomparator *cmp;
	void *arg;
};

struct sri(i) {
	sri() *i;
	int64_t p, n;
};

static inline srunused sri_type
sri(min)(sri() *i) {
	if (srunlikely(i->n == 0))
		return NULL;
	return i->i[0]->i[0];
}

static inline srunused sri_type
sri(max)(sri() *i) {
	if (srunlikely(i->n == 0))
		return NULL;
	return i->i[i->in-1]->i[i->i[i->in-1]->n-1];
}

static inline srunused void
sri(first)(sri(i) *it) {
	it->p = 0;
	it->n = 0;
}

static inline srunused void
sri(last)(sri(i) *it) {
	it->p = it->i->in - 1;
	it->n = it->i->i[it->i->in - 1]->n - 1;
}

static inline srunused void
sri(open)(sri(i) *it, sri() *i) {
	it->i = i;
	sri(first)(it);
}

static inline srunused int
sri(has)(sri(i) *it) {
	assert(it->i != NULL);
	return (it->p >= 0 && it->n >= 0) &&
	       (it->p < it->i->in) &&
	       (it->n < it->i->i[it->p]->n);
}

static inline srunused void
sri(valset)(sri(i) *it, sri_type v) {
	it->i->i[it->p]->i[it->n] = v;
}

static inline srunused sri_type
sri(val)(sri(i) *it) {
	if (srunlikely(! sri(has)(it)))
		return NULL;
	return it->i->i[it->p]->i[it->n];
}

static inline srunused srhot int
sri(next)(sri(i) *it) {
	if (srunlikely(! sri(has)(it)))
		return 0;
	it->n++;
	while (it->p < it->i->in) {
		sri(page) *p = it->i->i[it->p];
		if (srunlikely(it->n >= p->n)) {
			it->p++;
			it->n = 0;
			continue;
		}
		return 1;
	}
	return 0;
}

static inline srunused srhot int
sri(prev)(sri(i) *it) {
	if (srunlikely(! sri(has)(it)))
		return 0;
	it->n--;
	while (it->p >= 0) {
		if (srunlikely(it->n < 0)) {
			if (it->p == 0)
				return 0;
			it->p--;
			it->n = it->i->i[it->p]->n - 1;
			continue;
		}
		return 1;
	}
	return 0;
}

static inline srunused void
sri(inv)(sri() *i, sri(i) *ii) {
	ii->i = i;
	ii->p = -1;
	ii->n = -1;
}

static inline int
sri(ensure)(sri() *i)
{
	if (srlikely((i->in + 1) < i->itop))
		return 0;
	i->itop *= 2;
	i->i = sr_realloc(i->a, i->i, i->itop * sizeof(void*));
	if (srunlikely(i->i == NULL))
		return -1;
	return 0;
}

static inline int
sri(pagesize)(sri() *i) {
	return sizeof(sri(page)) + sizeof(sri_type) * i->pagesize;
}

static inline sri(page)*
sri(pagealloc)(sri() *i) {
	sri(page) *p = sr_malloc(i->a, sri(pagesize)(i));
	if (srunlikely(p == NULL))
		return NULL;
	p->n = 0;
	return p;
}

static inline srunused int
sri(init)(sri() *i, sra *a, int pagesize, srcomparator *cmp)
{
	if (srunlikely((pagesize % 2) != 0))
		return -1;
	i->a = a;
	i->arg = NULL;
	i->cmp = cmp;
	i->n = 0;
	i->pagesize = pagesize;
	i->half = pagesize / 2;
	/* start from 4 pages vector */
	i->itop = 2;
	i->in = 1;
	i->i = NULL;
	int rc = sri(ensure)(i);
	if (srunlikely(rc == -1))
		return -1;
	/* allocate first page */
	i->i[0] = sri(pagealloc)(i);
	if (srunlikely(i->i[0] == NULL)) {
		sr_free(i->a, i->i);
		i->i = NULL;
		return -1;
	}
	return 0;
}

static inline int
sri(clone)(sri() *i, sri() *p)
{
	i->a = p->a;
	i->arg = p->arg;
	i->cmp = p->cmp;
	i->n = p->n;
	i->pagesize = p->pagesize;
	i->half = p->half;
	/* start from 4 pages vector */
	i->itop = p->in;
	i->in = p->in;
	i->i = sr_malloc(i->a, i->in * sizeof(void*));
	if (srunlikely(i->i == NULL))
		return -1;

	uint32_t j = 0;
	while (j < i->in) {
		i->i[j] = sri(pagealloc)(i);
		if (srunlikely(i->i[j] == NULL))
			return -1;
		memcpy(i->i[j], p->i[j], sizeof(sri(page)) + p->i[j]->n * sizeof(sri_type));
		j++;
	}

	return 0;
}

static inline srunused void
sri(free)(sri() *i)
{
	uint32_t p = 0;
	while (p < i->in) {
		uint32_t k = 0;
		while (k < i->i[p]->n) {
			sri_free(i->a, i->i[p]->i[k]);
			k++;
		}
		sr_free(i->a, i->i[p]);
		p++;
	}
	if (i->i)
		sr_free(i->a, i->i);
	i->in = 0;
	i->i = NULL;
}

static inline srunused void
sri(freei)(sri() *i)
{
	uint32_t p = 0;
	while (p < i->in) {
		sr_free(i->a, i->i[p]);
		p++;
	}
	if (i->i)
		sr_free(i->a, i->i);
	i->in = 0;
	i->i = NULL;
}

static inline srunused int
sri(truncate)(sri() *i)
{
	sri(free)(i);
	return sri(init)(i, i->a, i->pagesize, i->cmp);
}

static inline srunused int
sri(reset)(sri() *i)
{
	sri(freei)(i);
	return sri(init)(i, i->a, i->pagesize, i->cmp);
}

static inline sri_type
sri(minof)(sri() *i srunused, sri(page) *p, void *key, int size srunused, int *idx)
{
	register int min = 0;
	register int max = p->n - 1;
	while (max >= min) {
		register int mid =
			min + ((max - min) >> 1);
		switch (sri_cmp(i, p->i[mid], key, size)) {
		case -1: min = mid + 1;
			continue;
		case  1: max = mid - 1;
			continue;
		default: *idx = mid;
			return p->i[mid];
		}
	}
	*idx = min;
	return NULL;
}

static inline sri_type
sri(maxof)(sri() *i srunused, sri(page) *p, void *key, int size srunused, int *idx)
{
	register int min = 0;
	register int max = p->n - 1;
	while (max >= min) {
		register int mid =
			min + ((max - min) >> 1);
		switch(sri_cmp(i, p->i[mid], key, size)) {
		case -1: min = mid + 1;
			continue;
		case  1: max = mid - 1;
			continue;
		default: *idx = mid;
			return p->i[mid];
		}
	}
	*idx = max;
	return NULL;
}

static inline int
sri(pagecmp)(sri() *i srunused, sri(page) *p, void *key, int size srunused)
{
	if (srunlikely(p->n == 0))
		return 0;
	register int l = sri_cmp(i, p->i[0], key, size);
	register int r =
		sri_cmp(i, p->i[p->n-1], key, size);
	/* inside page range */
	if (l <= 0 && r >= 0)
		return 0;
	/* page min < key */
	if (l == -1)
		return -1;
	/* page max > key */
	assert(r == 1);
	return 1;
}

static inline sri(page)*
sri(pageof)(sri() *i, void *key, int size, uint32_t *idx)
{
	register int min = 0;
	register int max = i->in - 1;
	while (max >= min) {
		register int mid =
			min + ((max - min) >> 1);
		switch (sri(pagecmp)(i, i->i[mid], key, size)) {
		case -1: min = mid + 1;
			continue;
		case  1: max = mid - 1;
			continue;
		default:
			*idx = mid;
			return i->i[mid];
		}
	}
	*idx = min;
	return NULL;
}

static inline srunused srhot int
sri(prepare)(sri(i) *it, void *key, int size)
{
	/* 1. do binary search on the vector and find usable
	 * page for a key */
	assert(it->i != NULL);
	sri() *i = it->i;
	sri(page) *p = i->i[0];
	uint32_t a = 0;
	if (srlikely(i->in > 1)) {
		p = sri(pageof)(i, key, size, &a);
		if (srunlikely(p == NULL)) {
			if (a >= i->in)
				a = i->in-1;
			p = i->i[a];
			assert(a < i->in);
		}
		assert(p != NULL);
	}

	/* 2. if page is full, split it and insert new one:
	 * copy second half of the keys from first page to second */
	/* 2.1. insert page to vector, by moving pointers forward */
	int rc;
	sri(page) *n = NULL;
	if (srunlikely(p->n == i->pagesize)) {
		rc = sri(ensure)(i);
		if (srunlikely(rc == -1))
			return -1;
		/* split page */
		n = sri(pagealloc)(i);
		if (srunlikely(n == NULL))
			return -1;
		memcpy(&n->i[0], &p->i[i->half], sizeof(sri_type) * i->half);
		n->n = i->half;
		p->n = i->half;
		/* split page i and insert new page */
		memmove(&i->i[a + 1], &i->i[a],
		        sizeof(void*) * (i->in - a));
		i->i[a] = p;
		i->i[a+1] = n;
		i->in++;
		/* choose which part to use */
		if (sri(pagecmp)(i, n, key, size) <= 0) {
			p = n;
			a++;
		}
	}

	/* 3. if page is not full, do nothing */
	/* 4. do binary search on a page and match room, move
	 * pointers forward */
	assert(p->n < i->pagesize);

	int j;
	sri_type o = sri(minof)(i, p, key, size, &j);
	if (srunlikely(o)) {
		it->p = a;
		it->n = j;
		assert(sri(val)(it) == o);
		return 1;
	}

	/* 5. insert key, increment counters */
	if (srlikely(j < p->n))
		memmove(&p->i[j + 1], &p->i[j], sizeof(sri_type) * (p->n - j));
	else
		j = p->n;
	i->n++;
	p->n++;
	it->p = a;
	it->n = j;
	return 0;
}

static inline srunused int
sri(set)(sri() *i, void *key, int size, sri_type v, sri_type *old)
{
	sri(i) pos;
	sri(open)(&pos, i);
	int rc = sri(prepare)(&pos, key, size);
	switch (rc) {
	case  1: *old = sri(val)(&pos);
	case  0: sri(valset)(&pos, v);
	case -1: break;
	}
	return rc;
}

static inline srunused srhot void
sri(del)(sri(i) *it)
{
	assert(it->i != NULL);
	sri(page) *p = it->i->i[it->p]; if (srlikely(it->n != (uint32_t)(p->n - 1)))
		memmove(&p->i[it->n],
		        &p->i[it->n + 1],
		        sizeof(sri_type) * (p->n - it->n - 1));
	p->n--;
	it->i->n--;
	/* do not touch last page */
	if (p->n > 0 || it->i->in == 1)
		return;
	/* remove empty page */
	sr_free(it->i->a, it->i->i[it->p]);
	if (srlikely(it->p != (it->i->in - 1)))
		memmove(&it->i->i[it->p],
		        &it->i->i[it->p + 1],
		        sizeof(void*) * (it->i->in - it->p - 1));
	it->i->in--;
}

static inline srunused sri_type
sri(get)(sri() *i, void *key, int size)
{
	sri(page) *p = i->i[0];
	uint32_t a = 0;
	if (srlikely(i->in > 1)) {
		p = sri(pageof)(i, key, size, &a);
		if (srunlikely(p == NULL))
			return NULL;
	}
	int j;
	return sri(minof)(i, p, key, size, &j);
}

static inline srunused sri_type
sri(geti)(sri(i) *it, void *key, int size)
{
	assert(it->i != NULL);
	sri(page) *p = it->i->i[0];
	uint32_t a = 0;
	if (srlikely(it->i->in > 1)) {
		p = sri(pageof)(it->i, key, size, &a);
		if (srunlikely(p == NULL)) {
			if (srunlikely(a >= it->i->in))
				a = it->i->in - 1;
			p = it->i->i[a];
		}
	}
	int j;
	sri_type v = sri(minof)(it->i, p, key, size, &j);
	if (srunlikely(j >= p->n))
		j = p->n - 1;
	it->p = a;
	it->n = j;
	return v;
}

static inline srunused int
sri(worldcmp)(sri() *i, void *key, int size srunused)
{
	register sri(page) *last srunused = i->i[i->in-1];
	register int l = sri_cmp(i, i->i[0]->i[0], key, size);
	register int r =
		sri_cmp(i, last->i[last->n - 1], key, size);
	/* inside index range */
	if (l <= 0 && r >= 0)
		return 0;
	/* index min < key */
	if (l == -1)
		return -1;
	/* index max > key */
	assert(r == 1);
	return 1;
}

static inline srunused int
sri(lte)(sri(i) *it, void *key, int size)
{
	assert(it->i != NULL);
	register sri() *i = it->i;
	if (srunlikely(i->n == 0)) {
		sri(inv)(i, it);
		return 0;
	}
	sri(page) *p = i->i[0];
	uint32_t a = 0;
	if (srlikely(i->in > 1))
		p = sri(pageof)(i, key, size, &a);
	if (p == NULL) {
		switch (sri(worldcmp)(i, key, size)) {
		case -1:
			sri(inv)(i, it);
			break;
		case  1:
			it->p = 0;
			it->n = 0;
			break;
		case  0:
			if (srunlikely(a >= i->in))
				a = i->in - 1;
			it->p = a;
			it->n = 0;
			break;
		}
		return 0;
	}
	int j;
	int eq = sri(minof)(i, p, key, size, &j) != NULL;
	if (srunlikely(j >= p->n))
		j = p->n - 1;
	else
	if (srunlikely(j < 0))
		j = 0;
	it->p = a;
	it->n = j;
	int rc = sri_cmp(i, p->i[j], key, size);
	if (rc == 1) {
		sri(prev)(it);
		if (! sri(has)(it))
			sri(open)(it, i);
	}
	return eq;
}

static inline srunused int
sri(gte)(sri(i) *it, char *key, int size)
{
	assert(it->i != NULL);
	register sri() *i = it->i;
	if (srunlikely(i->n == 0)) {
		sri(inv)(i, it);
		return 0;
	}
	sri(page) *p = i->i[0];
	uint32_t a = 0;
	if (srlikely(i->in > 1))
		p = sri(pageof)(i, key, size, &a);
	if (p == NULL) {
		switch (sri(worldcmp)(i, key, size)) {
		case -1:
			it->p = i->in - 1;
			it->n = i->i[i->in - 1]->n - 1;
			break;
		case  1:
			/*
			sri(inv)(i, it);
			*/

			it->p = a;
			it->n = 0;
			break;
		case  0:
			if (srunlikely(a >= i->in))
				a = i->in - 1;
			it->p = a;
			it->n = i->i[a]->n - 1;
			break;
		}
		return 0;
	}
	int j;
	int eq = sri(maxof)(i, p, key, size, &j) != NULL;
	if (srunlikely(j >= p->n))
		j = p->n - 1;
	else
	if (srunlikely(j < 0))
		j = 0;
	it->p = a;
	it->n = j;
	int rc = sri_cmp(i, p->i[j], key, size);
	if (rc == -1)
		sri(next)(it);
	return eq;
}

#undef sri_instance
#undef sri_cmp
#undef sri_free
#undef sri_type
#undef sri
