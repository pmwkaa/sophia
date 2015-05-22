#ifndef SO_REQUEST_H_
#define SO_REQUEST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sorequestarg sorequestarg;
typedef struct sorequest sorequest;

typedef enum {
	SO_REQDBSET,
	SO_REQDBGET,
	SO_REQTXSET,
	SO_REQTXGET,
	SO_REQBEGIN,
	SO_REQPREPARE,
	SO_REQCOMMIT,
	SO_REQROLLBACK,
	SO_REQON_BACKUP
} sorequestop;

struct sorequestarg {
	srorder order;
	sv v;
	int recover;
	uint64_t lsn;
	int vlsn_generate;
	uint64_t vlsn;
};

struct sorequest {
	soobj o;
	uint64_t id;
	uint32_t op;
	soobj *object;
	soobj *db;
	sorequestarg arg;
	void *result;
	int rc; 
} srpacked;

void so_requestinit(so*, sorequest*, sorequestop, soobj*, soobj*);
void so_requestadd(so*, sorequest*);
void so_request_on_backup(so*);
void so_requestready(sorequest*);
int  so_requestcount(so*);
sorequest *so_requestnew(so*, sorequestop, soobj*, soobj*);
sorequest *so_requestdispatch(so*);
sorequest *so_requestdispatch_ready(so*);

#endif
