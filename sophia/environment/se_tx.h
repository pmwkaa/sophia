#ifndef SE_TX_H_
#define SE_TX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct setx setx;

struct setx {
	so o;
	int64_t lsn;
	uint64_t start;
	svlog log;
	sx t;
};

so *se_txnew(se*);

#endif
