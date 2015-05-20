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
	SO_REQGET,
	SO_REQON_BACKUP
} sorequestop;

struct sorequest {
	soobj o;
	uint64_t id;
	uint32_t op;
	soobj *object;
	svlog log, *logp;
	sv search;
	int search_free;
	uint64_t vlsn;
	int vlsn_generate;
	void *result;
	int rc; 
};

void so_requestinit(so*, sorequest*, sorequestop, soobj*);
void so_requestarg(sorequest*, svlog*, sv*, int);
void so_requestvlsn(sorequest*, uint64_t, int);
void so_requestadd(so*, sorequest*);
void so_request_on_backup(so*);
void so_requestready(sorequest*);
int  so_requestcount(so*);
sorequest *so_requestnew(so*, sorequestop, soobj*);
sorequest *so_requestdispatch(so*);
sorequest *so_requestdispatch_ready(so*);

#endif
