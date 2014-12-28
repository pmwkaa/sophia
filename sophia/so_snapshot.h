#ifndef SO_SNAPSHOT_H_
#define SO_SNAPSHOT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sosnapshot sosnapshot;

struct sosnapshot {
	soobj o;
	sx t;
	uint64_t vlsn;
	char *name;
	sosnapshotdb *db;
	so *e;
} srpacked;

soobj *so_snapshotnew(sosnapshotdb*, uint64_t, char*);

#endif
