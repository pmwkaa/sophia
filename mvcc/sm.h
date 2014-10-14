#ifndef SM_H_
#define SM_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sm sm;
typedef struct smtx smtx;

typedef enum {
	SMUNDEF,
	SMREADY,
	SMCOMMIT,
	SMROLLBACK,
	SMWAIT,
	SMPREPARE
} smstate;

typedef smstate (*smpreparef)(smtx*, sv*, void*);

struct sm {
	srspinlock lock;
	int tn;
	srrb t;
	srrb i;
	sr *r;
};

struct smtx {
	uint32_t id;
	smstate s;
	uint64_t lsvn;
	svlog log;
	sm *c;
	srlist deadlock;
	srrbnode node;
};

int sm_init(sm*, sr*);
int sm_free(sm*);

smtx   *sm_find(sm*, uint32_t);
smstate sm_begin(sm*, smtx*);
smstate sm_end(smtx*);
smstate sm_prepare(smtx*, smpreparef, void*);
smstate sm_commit(smtx*);
smstate sm_rollback(smtx*);

uint64_t sm_lsvn(sm*);
int sm_set(smtx*, svv*);
int sm_get(smtx*, sv*, sv*);

smstate sm_set_stmt(sm*, svv*);
smstate sm_get_stmt(sm*);

#endif
