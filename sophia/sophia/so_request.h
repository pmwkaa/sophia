#ifndef SO_REQUEST_H_
#define SO_REQUEST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sorequest sorequest;

typedef enum {
	SO_REQWRITE,
	SO_REQGET
} sorequestop;

struct sorequest {
	soobj o;
	uint64_t id;
	uint32_t op;
	soobj *object;
	sv arg;
	int arg_free;
	uint64_t vlsn;
	int vlsn_generate;
	void *result;
	int rc; 
};

void so_requestinit(so*, sorequest*, sorequestop, soobj*, sv*);
void so_requestvlsn(sorequest*, uint64_t, int);
void so_requestadd(so*, sorequest*);
void so_requestready(sorequest*);
int  so_requestcount(so*);
sorequest *so_requestnew(so*, sorequestop, soobj*, sv*);
sorequest *so_requestdispatch(so*);

#endif
