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
	sx t;
} srpacked;

int    so_txdbset(sodb*, int, uint8_t, va_list);
void  *so_txdbget(sodb*, int, uint64_t, int, va_list);
soobj *so_txnew(so*);
int    so_query(sorequest*);

#endif
