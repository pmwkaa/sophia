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
	srobj o;
	int async;
	sx t;
} sspacked;

int    so_txdbset(sodb*, int, uint8_t, va_list);
void  *so_txdbget(sodb*, int, uint64_t, int, va_list);
void   so_txend(sotx*);
srobj *so_txnew(so*, int);

#endif
