#ifndef SC_INDEX_H_
#define SC_INDEX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svindex svindex;

struct svindex {
	ssrb i;
	uint32_t count;
	uint32_t used;
	uint64_t lsnmin;
} sspacked;

ss_rbget(sv_indexmatch,
         sr_compare(scheme, sv_vpointer(sscast(n, svv, node)),
                    (sscast(n, svv, node))->size,
                    key, keysize))

int sv_indexinit(svindex*);
int sv_indexfree(svindex*, sr*);
int sv_indexset(svindex*, sr*, uint64_t, svv*, svv**);

static inline uint32_t
sv_indexused(svindex *i) {
	return i->count * sizeof(svv) + i->used;
}

#endif
