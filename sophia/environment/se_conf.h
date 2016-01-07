#ifndef SE_CONF_H_
#define SE_CONF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct seconfrt seconfrt;
typedef struct seconf seconf;

typedef void (*serecovercbf)(char*, void*);

typedef struct {
	serecovercbf function;
	void *arg;
} serecovercb;

struct seconfrt {
	/* sophia */
	char      version[16];
	char      version_storage[16];
	char      build[32];
	/* memory */
	uint64_t  memory_used;
	uint32_t  pager_pools;
	uint32_t  pager_pool_size;
	uint32_t  pager_ref_pools;
	uint32_t  pager_ref_pool_size;
	/* scheduler */
	char      zone[4];
	uint32_t  checkpoint_active;
	uint64_t  checkpoint_lsn;
	uint64_t  checkpoint_lsn_last;
	uint32_t  snapshot_active;
	uint64_t  snapshot_ssn;
	uint64_t  snapshot_ssn_last;
	uint32_t  anticache_active;
	uint64_t  anticache_asn;
	uint64_t  anticache_asn_last;
	uint32_t  backup_active;
	uint32_t  backup_last;
	uint32_t  backup_last_complete;
	uint32_t  gc_active;
	uint32_t  lru_active;
	/* log */
	uint32_t  log_files;
	/* metric */
	srseq     seq;
	/* performance */
	uint32_t  reqs;
	uint32_t  req_queue;
	uint32_t  req_ready;
	uint32_t  req_active;
	uint32_t  tx_rw;
	uint32_t  tx_ro;
	uint32_t  tx_gc_queue;
	srstat    stat;
};

struct seconf {
	/* sophia */
	char         *path;
	uint32_t      path_create;
	/* backup */
	char         *backup_path;
	/* compaction */
	srzonemap     zones;
	/* scheduler */
	uint32_t      threads;
	serecovercb   on_recover;
	sstrigger     on_event;
	uint32_t      event_on_backup;
	/* memory */
	uint64_t      memory_limit;
	uint64_t      anticache;
	/* log */
	uint32_t      log_enable;
	char         *log_path;
	uint32_t      log_sync;
	uint32_t      log_rotate_wm;
	uint32_t      log_rotate_sync;
	uint32_t      two_phase_recover;
	srscheme      scheme;
	srconf       *conf;
	so           *env;
};

int      se_confinit(seconf*, so*);
void     se_conffree(seconf*);
int      se_confvalidate(seconf*);
int      se_confserialize(seconf*, ssbuf*);
int      se_confset_string(so*, const char*, void*, int);
int      se_confset_int(so*, const char*, int64_t);
void    *se_confget_object(so*, const char*);
void    *se_confget_string(so*, const char*, int*);
int64_t  se_confget_int(so*, const char*);

#endif
