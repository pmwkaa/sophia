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
	int half_commit;
	uint64_t start;
	sx t;
};

so *se_txnew(se*);

#endif
