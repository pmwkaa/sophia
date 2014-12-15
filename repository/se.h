#ifndef SE_H_
#define SE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct se se;

struct se {
	seconf *conf;
	srspinlock lock;
	sdss snapshot;
};

static inline void
se_lock(se *e) {
	sr_spinlock(&e->lock);
}

static inline void
se_unlock(se *e) {
	sr_spinunlock(&e->lock);
}

int se_init(se*);
int se_open(se*, sr*, seconf*);
int se_close(se*, sr*);
int se_snapshot(se*, sr*, uint64_t, char*);
int se_snapshot_remove(se*, sr*, char*);

#endif
