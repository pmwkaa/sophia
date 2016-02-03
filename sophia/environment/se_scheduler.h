#ifndef SE_SCHEDULER_H_
#define SE_SCHEDULER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct seschedulerdb seschedulerdb;
typedef struct sescheduler sescheduler;
typedef struct setask setask;

enum {
	SE_SCHEDQ_BRANCH  = 0,
	SE_SCHEDQ_GC      = 1,
	SE_SCHEDQ_LRU     = 2,
	SE_SCHEDQ_BACKUP  = 3,
	SE_SCHEDQ_NONE
};

struct seschedulerdb {
	uint32_t workers[SE_SCHEDQ_NONE];
	void *db;
};

struct setask {
	siplan         plan;
	int            rotate;
	int            req;
	int            gc;
	int            checkpoint_complete;
	int            anticache_complete;
	int            snapshot_complete;
	int            backup_complete;
	seschedulerdb *db;
	void          *db_gc;
};

struct sescheduler {
	ssmutex        lock;
	uint64_t       checkpoint_lsn_last;
	uint64_t       checkpoint_lsn;
	uint32_t       checkpoint;
	uint32_t       age;
	uint64_t       age_time;
	uint64_t       anticache_asn;
	uint64_t       anticache_asn_last;
	uint64_t       anticache_storage;
	uint64_t       anticache_time;
	uint64_t       anticache;
	uint64_t       snapshot_ssn;
	uint64_t       snapshot_ssn_last;
	uint64_t       snapshot_time;
	uint64_t       snapshot;
	uint64_t       gc_time;
	uint32_t       gc;
	uint64_t       lru_time;
	uint32_t       lru;
	uint32_t       backup_bsn;
	uint32_t       backup_bsn_last;
	uint32_t       backup_bsn_last_complete;
	uint32_t       backup_events;
	uint32_t       backup;
	uint32_t       workers_gc_db;
	int            rotate;
	int            req;
	int            rr;
	int            count;
	seschedulerdb *i;
	ssthreadpool   tp;
	seworkerpool   workers;
	so            *env;
};

int se_scheduler_init(sescheduler*, so*);
int se_scheduler_run(sescheduler*);
int se_scheduler_shutdown(sescheduler*);
int se_scheduler_add(sescheduler*, void*);
int se_scheduler_del(sescheduler*, void*);
int se_scheduler(sescheduler*, seworker*);
int se_scheduler_branch(void*);
int se_scheduler_compact(void*);
int se_scheduler_compact_index(void*);
int se_scheduler_anticache(void*);
int se_scheduler_snapshot(void*);
int se_scheduler_checkpoint(void*);
int se_scheduler_gc(void*);
int se_scheduler_lru(void*);
int se_scheduler_backup(void*);
int se_scheduler_call(void*);

#endif
