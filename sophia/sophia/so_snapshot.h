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
	srobj o;
	sx t;
	uint64_t vlsn;
	char *name;
} sspacked;

srobj *so_snapshotnew(so*, uint64_t, char*);
int    so_snapshotupdate(sosnapshot*);

#endif
