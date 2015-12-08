#ifndef SI_NODE_H_
#define SI_NODE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sinode sinode;

#define SI_NONE       0
#define SI_LOCK       1
#define SI_ROTATE     2
#define SI_PROMOTE    4
#define SI_REVOKE     8

#define SI_RDB        16
#define SI_RDB_DBI    32
#define SI_RDB_DBSEAL 64
#define SI_RDB_UNDEF  128
#define SI_RDB_REMOVE 256

struct sinode {
	uint32_t  recover;
	uint8_t   flags;
	uint64_t  update_time;
	uint32_t  used;
	uint32_t  backup;
	uint64_t  lru;
	uint64_t  ac;
	uint32_t  in_memory;
	sibranch  self;
	sibranch *branch;
	uint32_t  branch_count;
	uint32_t  temperature;
	uint64_t  temperature_reads;
	svindex   i0, i1;
	ssfile    file;
	ssmmap    map, map_swap;
	ssrbnode  node;
	ssrqnode  nodecompact;
	ssrqnode  nodebranch;
	ssrqnode  nodetemp;
	sslist    commit;
} sspacked;

sinode *si_nodenew(sr*);
int si_nodeopen(sinode*, sr*, sischeme*, sspath*, sdsnapshotnode*);
int si_nodecreate(sinode*, sr*, sischeme*, sdid*);
int si_nodefree(sinode*, sr*, int);
int si_nodegc_index(sr*, svindex*);
int si_nodemap(sinode*, sr*);
int si_noderead(sinode*, sr*, ssbuf*);
int si_nodeseal(sinode*, sr*, sischeme*);
int si_nodecomplete(sinode*, sr*, sischeme*);

static inline void
si_nodelock(sinode *node) {
	assert(! (node->flags & SI_LOCK));
	node->flags |= SI_LOCK;
}

static inline void
si_nodeunlock(sinode *node) {
	assert((node->flags & SI_LOCK) > 0);
	node->flags &= ~SI_LOCK;
}

static inline svindex*
si_noderotate(sinode *node) {
	node->flags |= SI_ROTATE;
	return &node->i0;
}

static inline void
si_nodeunrotate(sinode *node) {
	assert((node->flags & SI_ROTATE) > 0);
	node->flags &= ~SI_ROTATE;
	node->i0 = node->i1;
	sv_indexinit(&node->i1);
}

static inline svindex*
si_nodeindex(sinode *node) {
	if (node->flags & SI_ROTATE)
		return &node->i1;
	return &node->i0;
}

static inline svindex*
si_nodeindex_priority(sinode *node, svindex **second)
{
	if (ssunlikely(node->flags & SI_ROTATE)) {
		*second = &node->i0;
		return &node->i1;
	}
	*second = NULL;
	return &node->i0;
}

static inline sinode*
si_nodeof(ssrbnode *node) {
	return sscast(node, sinode, node);
}

static inline int
si_nodecmp(sinode *n, void *key, int size, srscheme *s)
{
	sdindexpage *min = sd_indexmin(&n->self.index);
	sdindexpage *max = sd_indexmax(&n->self.index);
	int l = sr_compare(s, sd_indexpage_min(&n->self.index, min),
	                   min->sizemin, key, size);
	int r = sr_compare(s, sd_indexpage_max(&n->self.index, max),
	                   max->sizemax, key, size);
	/* inside range */
	if (l <= 0 && r >= 0)
		return 0;
	/* key > range */
	if (l == -1)
		return -1;
	/* key < range */
	assert(r == 1);
	return 1;
}

static inline uint64_t
si_nodesize(sinode *n)
{
	uint64_t size = 0;
	sibranch *b = n->branch;
	while (b) {
		size += sd_indexsize(b->index.h) +
		        sd_indextotal(&b->index);
		b = b->next;
	}
	return size;
}

#endif
