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
	uint64_t id;
	ssgc gc;
	ssmutex filelock;
	ssfile file;
	slpool *p;
	sslist link;
	sslist linkcopy;
};

struct slpool {
	ssspinlock lock;
	slconf *conf;
	sslist list;
	int gc;
	int n;
	ssiov iov;
	sr *r;
};

struct sltx {
	slpool *p;
	sl *l;
	int recover;
	uint64_t lsn;
	uint64_t svp;
};

int sl_poolinit(slpool*, sr*);
int sl_poolopen(slpool*, slconf*);
int sl_poolrotate(slpool*);
int sl_poolrotate_ready(slpool*);
int sl_poolshutdown(slpool*);
int sl_poolgc_enable(slpool*, int);
int sl_poolgc(slpool*);
int sl_poolfiles(slpool*);
int sl_poolcopy(slpool*, char*, ssbuf*);

int sl_begin(slpool*, sltx*, uint64_t, int);
int sl_commit(sltx*);
int sl_rollback(sltx*);
int sl_write(sltx*, svlog*);

#endif
