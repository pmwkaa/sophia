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
	uint64_t vlsn;
	char *name;
	sx t;
	int db_view_only;
	solist cursor;
} sspacked;

so  *se_snapshotnew(se*, uint64_t, char*);
int  se_snapshotupdate(sesnapshot*);

#endif
