#ifndef SE_VIEWDB_H_
#define SE_VIEWDB_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct seviewdb seviewdb;

struct seviewdb {
	so        o;
	uint64_t  txn_id;
	int       ready;
	ssbuf     list;
	char     *pos;
	sedb     *v;
} sspacked;

so *se_viewdb_new(se*, uint64_t);

#endif
