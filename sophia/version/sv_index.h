#ifndef SC_INDEX_H_
#define SC_INDEX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svindexpos svindexpos;
typedef struct svindex svindex;

struct svindexpos {
	ssrbnode *node;
	int rc;
};

struct svindex {
	ssrb i;
	uint32_t count;
	uint32_t used;
	uint64_t lsnmin;
} sspacked;

ss_rbget(sv_indexmatch,
         sf_compare(scheme, sv_vpointer(sscast(n, svv, node)), key))

int  sv_indexinit(svindex*);
int  sv_indexfree(svindex*, sr*);
int  sv_indexupdate(svindex*, sr*, svindexpos*, svv*);
svv *sv_indexget(svindex*, sr*, svindexpos*, svv*);

static inline int
sv_indexset(svindex *i, sr *r, svv *v)
{
	svindexpos pos;
	sv_indexget(i, r, &pos, v);
	sv_indexupdate(i, r, &pos, v);
	return 0;
}

#endif
