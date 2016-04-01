#ifndef SR_BOUND_H_
#define SR_BOUND_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srlimit srlimit;

struct srlimit {
	uint32_t u32_min;
	uint32_t u32_max;
	uint64_t u64_min;
	uint64_t u64_max;
	int64_t  i64_min;
	int64_t  i64_max;
	char    *string_min;
	int      string_min_size;
	char    *string_max;
	int      string_max_size;
};

static inline int
sr_limitinit(srlimit *b, ssa *a)
{
	b->u32_min = 0;
	b->u32_max = UINT32_MAX;
	b->u64_min = 0;
	b->u64_max = UINT64_MAX;
	b->i64_min = INT64_MIN;
	b->i64_max = UINT64_MAX;
	b->string_min_size = 0;
	b->string_min = "";
	b->string_max_size = 1024;
	b->string_max = ss_malloc(a, b->string_max_size);
	if (ssunlikely(b->string_max == NULL))
		return -1;
	memset(b->string_max, 0xff, b->string_max_size);
	return 0;
}

static inline void
sr_limitfree(srlimit *b, ssa *a)
{
	if (b->string_max)
		ss_free(a, b->string_max);
}

static inline void
sr_limitset(srlimit *b, srscheme *s, sfv *keyv, int keyc, ssorder order)
{
	int i = 0;
	while (i < keyc) {
		sfv *v = &keyv[i];
		if (v->key) {
			i++;
			continue;
		}
		srkey *part = &s->parts[i];
		switch (part->type) {
		case SS_U32:
			if (order == SS_LT || order == SS_LTE) {
				v->key = (char*)&b->u32_max;
				v->r.size = sizeof(uint32_t);
			} else {
				v->key = (char*)&b->u32_min;
				v->r.size = sizeof(uint32_t);
			}
			break;
		case SS_U64:
			if (order == SS_LT || order == SS_LTE) {
				v->key = (char*)&b->u64_max;
				v->r.size = sizeof(uint64_t);
			} else {
				v->key = (char*)&b->u64_min;
				v->r.size = sizeof(uint64_t);
			}
			break;
		case SS_I64:
			if (order == SS_LT || order == SS_LTE) {
				v->key = (char*)&b->i64_max;
				v->r.size = sizeof(int64_t);
			} else {
				v->key = (char*)&b->i64_min;
				v->r.size = sizeof(int64_t);
			}
			break;
		case SS_STRING:
			if (order == SS_LT || order == SS_LTE) {
				v->key = b->string_max;
				v->r.size = b->string_max_size;
			} else {
				v->key = b->string_min;
				v->r.size = b->string_min_size;
			}
			break;
		default: assert(0);
			break;
		}
		i++;
	}
}

#endif
