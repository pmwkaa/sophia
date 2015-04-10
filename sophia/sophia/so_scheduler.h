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
	int req;
	int gc;
	int checkpoint_complete;
	int backup_complete;
	void *db;
};

struct soscheduler {
	soworkers workers;
	srmutex lock;
	uint64_t checkpoint_lsn_last;
	uint64_t checkpoint_lsn;
	uint32_t checkpoint;
	uint32_t age;
	uint32_t age_last;
	uint32_t backup_bsn;
	uint32_t backup_last;
	uint32_t backup_last_complete;
	uint32_t backup;
	uint32_t gc;
	uint32_t gc_last;
	uint32_t workers_backup;
	uint32_t workers_branch;
	uint32_t workers_gc;
	uint32_t workers_gc_db;
	int rotate;
	int req;
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
int so_scheduler_gc(void*);
int so_scheduler_backup(void*);
int so_scheduler_call(void*);

#endif
