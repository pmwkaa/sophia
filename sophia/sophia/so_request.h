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
	SO_REQCURSOROPEN,
	SO_REQCURSORGET,
	SO_REQCURSORDESTROY,
	SO_REQBEGIN,
	SO_REQPREPARE,
	SO_REQCOMMIT,
	SO_REQROLLBACK,
	SO_REQON_BACKUP
} sorequestop;

struct sorequestarg {
	ssorder order;
	sv v;
	sicache *cache;
	char *prefix;
	int prefixsize;
	int recover;
	uint64_t lsn;
	int vlsn_generate;
	uint64_t vlsn;
};

struct sorequest {
	srobj o;
	uint64_t id;
	uint32_t op;
	srobj *object;
	srobj *db;
	sorequestarg arg;
	void *v;
	void *result;
	int rc; 
} sspacked;

void so_requestinit(so*, sorequest*, sorequestop, srobj*, srobj*);
void so_requestadd(so*, sorequest*);
void so_request_on_backup(so*);
void so_requestready(sorequest*);
int  so_requestcount(so*);
int  so_requestqueue(so*);
sorequest *so_requestnew(so*, sorequestop, srobj*, srobj*);
sorequest *so_requestdispatch(so*, int);
sorequest *so_requestdispatch_ready(so*);
srobj     *so_requestresult(sorequest*);
void       so_requestend(sorequest*);

#endif
