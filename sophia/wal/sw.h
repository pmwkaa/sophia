#ifndef SW_H_
#define SW_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sw sw;
typedef struct swmanager swmanager;
typedef struct swtx swtx;

struct sw {
	uint64_t   id;
	ssgc       gc;
	ssmutex    filelock;
	ssfile     file;
	swmanager *p;
	sslist     link;
	sslist     linkcopy;
};

struct swmanager {
	ssspinlock lock;
	swconf     conf;
	sslist     list;
	int        gc;
	int        n;
	ssiov      iov;
	sr        *r;
};

struct swtx {
	swmanager *p;
	sw        *l;
	int        recover;
	uint64_t   lsn;
	uint64_t   svp;
};

static inline swconf*
sw_conf(swmanager *p) {
	return &p->conf;
}

int sw_managerinit(swmanager*, sr*);
int sw_manageropen(swmanager*);
int sw_managerrotate(swmanager*);
int sw_managerrotate_ready(swmanager*);
int sw_managershutdown(swmanager*);
int sw_managergc_enable(swmanager*, int);
int sw_managergc(swmanager*);
int sw_managerfiles(swmanager*);
int sw_managercopy(swmanager*, char*, ssbuf*);

int sw_begin(swmanager*, swtx*, uint64_t, int);
int sw_commit(swtx*);
int sw_rollback(swtx*);
int sw_write(swtx*, svlog*);

#endif
