#ifndef SL_H_
#define SL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sl sl;
typedef struct slpool slpool;
typedef struct sltx sltx;

struct sl {
	uint32_t id;
	srgc gc;
	uint32_t used;
	srmutex filelock;
	srfile file;
	slpool *p;
	srlist link;
};

struct slpool {
	srspinlock lock;
	slconf *conf;
	srlist list;
	int enabled;
	int n;
	sriov iov;
	sr *r;
};

struct sltx {
	slpool *p;
	sl *l;
	uint64_t svp;
};

int sl_poolinit(slpool*, sr*, slconf*);
int sl_poolopen(slpool*);
int sl_poolrotate(slpool*);
int sl_poolrotate_ready(slpool*, int);
int sl_poolshutdown(slpool*);
int sl_poolgc(slpool*);

int sl_begin(slpool*, sltx*);
int sl_prepare(slpool*, svlog*);
int sl_commit(sltx*);
int sl_rollback(sltx*);
int sl_write(sltx*, svlog*);
int sl_writelsn(svlog*, sl*, uint64_t);

#endif
