#ifndef SE_SNAPSHOT_H_
#define SE_SNAPSHOT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sesnapshot sesnapshot;

struct sesnapshot {
	so o;
	sx t;
	uint64_t vlsn;
	char *name;
} sspacked;

so  *se_snapshotnew(se*, uint64_t, char*);
int  se_snapshotupdate(sesnapshot*);

#endif
