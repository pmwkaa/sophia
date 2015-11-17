#ifndef SE_DBCURSOR_H_
#define SE_DBCURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sedbcursor sedbcursor;

struct sedbcursor {
	so o;
	uint64_t txn_id;
	int ready;
	ssbuf list;
	char *pos;
	sedb *v;
} sspacked;

so *se_dbcursor_new(se*, uint64_t);

#endif
