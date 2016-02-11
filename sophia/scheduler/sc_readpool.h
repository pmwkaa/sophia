#ifndef SC_READPOOL_H_
#define SC_READPOOL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct screadpool screadpool;

struct screadpool {
	ssmutex  lock;
	sscond   cond;
	solist   list;
	solist   list_active;
	solist   list_ready;
	sr      *r;
};

void  sc_readpool_init(screadpool*, sr*);
void  sc_readpool_free(screadpool*);
so   *sc_readpool_new(screadpool*, so*, int);
so   *sc_readpool_pop(screadpool*, int);
so   *sc_readpool_popready(screadpool*);
void  sc_readpool_ready(screadpool*, so*);
void  sc_readpool_wakeup(screadpool*);
int   sc_readpool_queue(screadpool*);

#endif
