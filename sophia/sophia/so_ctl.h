#ifndef SO_CTL_H_
#define SO_CTL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct soctlrt soctlrt;
typedef struct soctl soctl;

struct soctlrt {
	/* sophia */
	char      version[16];
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
	/* log */
	uint32_t  log_files;
	/* metric */
	srseq     seq;
};

struct soctl {
	soobj o;
	/* sophia */
	char      *path;
	uint32_t   path_create;
	/* backup */
	char      *backup_path;
	srtrigger  backup_on_complete;
	/* compaction */
	uint32_t   node_size;
	uint32_t   page_size;
	sizonemap  zones;
	/* scheduler */
	uint32_t   threads;
	srtrigger  checkpoint_on_complete;
	/* memory */
	uint64_t   memory_limit;
	/* log */
	uint32_t   log_enable;
	char      *log_path;
	uint32_t   log_sync;
	uint32_t   log_rotate_wm;
	uint32_t   log_rotate_sync;
	uint32_t   two_phase_recover;
	uint32_t   commit_lsn;
};

void  so_ctlinit(soctl*, void*);
void  so_ctlfree(soctl*);
int   so_ctlvalidate(soctl*);
int   so_ctlserialize(soctl*, srbuf*);
void *so_ctlreturn(src*, void*);

#endif
