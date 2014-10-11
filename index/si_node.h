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

#define SI_MERGE      1
#define SI_BRANCH     4
#define SI_I1         16

#define SI_RDB        32
#define SI_RDBI       64
#define SI_RDB_DBI    256
#define SI_RDB_DBSEAL 512
#define SI_RDB_UNDEF  1024
#define SI_RDB_REMOVE 2048

struct sinode {
	sdid      id;
	uint8_t   flags;
	uint32_t  recover;
	srfile    file;
	srmap     map;
	sdindex   index;
	svindex   i0, i1;
	uint32_t  iused;
	uint32_t  iusedkv;
	uint32_t  icount;
	uint32_t  lv;
	sinode   *next;
	srrbnode  node;
	srrbnode  nodemerge;
	srrbnode  nodebranch;
} srpacked;

sinode *si_nodenew(sr*);
int si_nodecreate(sinode*, sr*, siconf*, sdid*, sdindex*, sdbuild*);
int si_nodecreate_attach(sinode*, sr*, siconf*, sdid*, sdindex*, sdbuild*);
int si_nodeopen(sinode*, sr*, srpath*);
int si_nodefree(sinode*, sr*);
int si_nodefree_all(sinode*, sr*);
int si_nodecmp(sinode*, void*, int, srcomparator*);
int si_nodegc(sinode*, sr*);
int si_nodeseal(sinode*, sr*, siconf*);
int si_nodecomplete(sinode*, sr*, siconf*);

static inline svindex*
si_noderotate(sinode *node) {
	node->flags |= SI_I1;
	return &node->i0;
}

static inline void
si_nodeunrotate(sinode *node) {
	node->flags &= ~SI_I1;
	node->i0 = node->i1;
	sv_indexinit(&node->i1);
}

static inline svindex*
si_nodeindex(sinode *node) {
	if (node->flags & SI_I1)
		return &node->i1;
	return &node->i0;
}

static inline sinode*
si_nodeof(srrbnode *node) {
	return srcast(node, sinode, node);
}

#endif
