#ifndef SO_CTL_H_
#define SO_CTL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct soctl soctl;
typedef struct soctlzone soctlzone;

struct soctlzone {
	uint32_t mode;
	uint32_t compact_wm;
	uint32_t branch_prio;
	uint32_t branch_wm;
	uint32_t branch_ttl;
	uint32_t branch_ttl_wm;
};

struct soctl {
	soobj o;
	/* sophia */
	char      *path;
	/* compaction */
	uint32_t   node_size;
	uint32_t   page_size;
	soctlzone  z0;
	soctlzone  za;
	soctlzone  zb;
	soctlzone  zc;
	soctlzone  zd;
	soctlzone  ze;
	/* scheduler */
	uint32_t   threads;
	/* memory */
	uint64_t   memory_limit;
	/* log */
	int        log_enable;
	char      *log_path;
	int        log_sync;
	int        log_rotate_wm;
	int        log_rotate_sync;
	int        two_phase_recover;
	int        commit_lsn;
	void *e;
};

void  so_ctlinit(soctl*, void*);
void  so_ctlfree(soctl*);
int   so_ctlvalidate(soctl*);
int   so_ctldump(soctl*, srbuf*);
void *so_ctlreturn(srctl*, void*);

#endif
