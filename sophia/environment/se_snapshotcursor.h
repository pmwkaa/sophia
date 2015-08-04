#ifndef SE_SNAPSHOTCURSOR_H_
#define SE_SNAPSHOTCURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sesnapshotcursor sesnapshotcursor;

struct sesnapshotcursor {
	so o;
	int ready;
	ssbuf list;
	char *pos;
	sedb *v;
	sesnapshot *s;
} sspacked;

so *se_snapshotcursor_new(sesnapshot*);

#endif
