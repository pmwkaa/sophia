#ifndef SO_TX_H_
#define SO_TX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sotx sotx;

struct sotx {
	soobj o;
	smtx t;
	sodb *db;
};

soobj *so_txnew(sodb*);

int   so_txdbset(sodb*, uint8_t, va_list);
void *so_txdbget(sodb*, va_list);

#endif
