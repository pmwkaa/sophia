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
	int async;
	uint64_t lsn;
	sx t;
};

int   se_txdbwrite(sedb*, sev*, int, uint8_t);
void *se_txdbget(sedb*, sev*, int, uint64_t, int);
void  se_txend(setx*);
so   *se_txnew(se*, int);

#endif
