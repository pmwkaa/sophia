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
	srrb i;
	uint32_t count;
	uint16_t keymax;
} srpacked;

sr_rbget(sv_indexmatch,
         sr_compare(cmp, sv_vkey(srcast(n, svv, node)),
                    (srcast(n, svv, node))->keysize,
                    key, keysize))

int sv_indexinit(svindex*);
int sv_indexfree(svindex*, sr*);
int sv_indexset(svindex*, sr*, uint64_t, svv*, svv**);

#endif
