#ifndef SE_META_H_
#define SE_META_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct semetart semetart;
typedef struct semeta semeta;

typedef void (*serecovercbf)(char*, void*);

typedef struct {
	serecovercbf function;
	void *arg;
} serecovercb;

struct semetart {
	/* sophia */
	char      version[16];
	char      build[32];
	/* memory */
	uint64_t  memory_used;
	/* scheduler */
	char      zone[4];
	uint32_t  checkpoint_active;
	uint64_t  checkpoint_lsn;
	uint64_t  checkpoint_lsn_last;
	uint32_t  backup_active;
	uint32_t  backup_last;
	uint32_t  backup_last_complete;
	uint32_t  gc_active;
	uint32_t  reqs;
	/* log */
	uint32_t  log_files;
	/* metric */
	srseq     seq;
};

struct semeta {
	/* sophia */
	char         *path;
	uint32_t      path_create;
	/* backup */
	char         *backup_path;
	/* compaction */
	uint32_t      node_size;
	uint32_t      page_size;
	uint32_t      page_checksum;
	srzonemap     zones;
	/* scheduler */
	uint32_t      threads;
	serecovercb   on_recover;
	sstrigger     on_event;
	uint32_t      event_on_backup;
	/* memory */
	uint64_t      memory_limit;
	/* log */
	uint32_t      log_enable;
	char         *log_path;
	uint32_t      log_sync;
	uint32_t      log_rotate_wm;
	uint32_t      log_rotate_sync;
	uint32_t      two_phase_recover;
	uint32_t      commit_lsn;
	srscheme      scheme;
	so           *env;
};

void     se_metainit(semeta*, so*);
void     se_metafree(semeta*);
int      se_metavalidate(semeta*);
int      se_metaserialize(semeta*, ssbuf*);
int      se_metaset_object(so*, char*, void*);
int      se_metaset_string(so*, char*, void*, int);
int      se_metaset_int(so*, char*, int64_t);
void    *se_metaget_object(so*, char*);
void    *se_metaget_string(so*, char*, int*);
int64_t  se_metaget_int(so*, char*);
void    *se_metacursor(so*, so*);

#endif
