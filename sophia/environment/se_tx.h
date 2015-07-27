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
	uint64_t lsn;
	sx t;
};

so   *se_txnew(se*);
void  se_txend(setx*);

#endif
