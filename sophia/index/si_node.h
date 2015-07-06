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
	sibranch  self;
	sibranch *branch;
	uint32_t  branch_count;
	svindex   i0, i1;
	ssfile    file;
	ssmmap    map, map_swap;
	ssrbnode  node;
	ssrqnode  nodecompact;
	ssrqnode  nodebranch;
	sslist    commit;
} sspacked;

sinode *si_nodenew(sr*);
int si_nodeopen(sinode*, sr*, sischeme*, sspath*);
int si_nodecreate(sinode*, sr*, sischeme*, sdid*, sdindex*, sdbuild*);
int si_nodefree(sinode*, sr*, int);
int si_nodegc_index(sr*, svindex*);
int si_nodemap(sinode*, sr*);
int si_nodesync(sinode*, sr*);
int si_nodecmp(sinode*, void*, int, srscheme*);
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

#endif
