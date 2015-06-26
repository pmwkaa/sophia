#ifndef SE_REQUEST_H_
#define SE_REQUEST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct serequestarg serequestarg;
typedef struct serequest serequest;

typedef enum {
	SE_REQDBSET,
	SE_REQDBGET,
	SE_REQTXSET,
	SE_REQTXGET,
	SE_REQCURSOROPEN,
	SE_REQCURSORGET,
	SE_REQCURSORDESTROY,
	SE_REQBEGIN,
	SE_REQPREPARE,
	SE_REQCOMMIT,
	SE_REQROLLBACK,
	SE_REQON_BACKUP
} serequestop;

struct serequestarg {
	ssorder   order;
	sv        v;
	sicache  *cache;
	char     *prefix;
	int       prefixsize;
	int       update;
	sv       *update_v;
	int       recover;
	uint64_t  lsn;
	int       vlsn_generate;
	uint64_t  vlsn;
};

struct serequest {
	so            o;
	uint64_t      id;
	uint32_t      op;
	so           *object;
	so           *db;
	serequestarg  arg;
	void         *v;
	void         *result;
	int           rc; 
};

void       se_requestinit(se*, serequest*, serequestop, so*, so*);
void       se_requestadd(se*, serequest*);
void       se_request_on_backup(se*);
void       se_requestready(serequest*);
int        se_requestcount(se*);
int        se_requestqueue(se*);
serequest *se_requestnew(se*, serequestop, so*, so*);
serequest *se_requestdispatch(se*, int);
serequest *se_requestdispatch_ready(se*);
void       se_requestwakeup(se*);
so        *se_requestresult(serequest*);
void       se_requestend(serequest*);

#endif
