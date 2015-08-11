#ifndef SE_REQ_H_
#define SE_REQ_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sereqarg sereqarg;
typedef struct sereq sereq;

typedef enum {
	SE_REQREAD,
	SE_REQWRITE,
	SE_REQON_BACKUP
} sereqop;

struct sereqarg {
	sv        v;
	sv        vprefix;
	sv        vup;
	sicache  *cache;
	int       cachegc;
	ssorder   order;
	int       update;
	int       update_eq;
	int       vlsn_generate;
	uint64_t  vlsn;
	svlog    *log;
	void     *arg;
	int       recover;
	uint64_t  lsn;
};

struct sereq {
	so        o;
	uint64_t  id;
	sereqop   op;
	sereqarg  arg;
	so       *object;
	so       *db;
	void     *v;
	int       rc; 
};

void   se_reqinit(se*, sereq*, sereqop, so*, so*);
char  *se_reqof(sereqop);
void   se_reqend(sereq*);
void   se_reqonbackup(se*);
void   se_reqready(sereq*);
int    se_reqcount(se*);
int    se_reqqueue(se*);
sereq *se_reqnew(se*, sereq*, int);
sereq *se_reqdispatch(se*, int);
sereq *se_reqdispatch_ready(se*);
void   se_reqwakeup(se*);
so    *se_reqresult(sereq*, int);

#endif
