#ifndef SO_SCHEDULER_H_
#define SO_SCHEDULER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct soscheduler soscheduler;
typedef struct sotask sotask;

struct sotask {
	siplan plan;
	int rotate;
	void *db;
};

struct soscheduler {
	soworkers workers;
	srspinlock lock;
	uint64_t checkpoint_lsn;
	int checkpoint_active;
	int branch;
	int branch_limit;
	int rotate;
	int rr;
	void **i;
	int count;
	void *env;
};

int so_scheduler_init(soscheduler*, void*);
int so_scheduler_run(soscheduler*);
int so_scheduler_shutdown(soscheduler*);
int so_scheduler_add(soscheduler*, void*);
int so_scheduler_del(soscheduler*, void*);
int so_scheduler(soscheduler*, soworker*);

int so_scheduler_branch(void*);
int so_scheduler_compact(void*);
int so_scheduler_checkpoint(void*);

#endif
