#ifndef SO_SNAPSHOTDB_H_
#define SO_SNAPSHOTDB_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sosnapshotdb sosnapshotdb;

struct sosnapshotdb {
	soobj o;
	soobj *db;
	soobjindex list;
	so *e;
} srpacked;

soobj *so_snapshotdb_new(so*);

#endif
