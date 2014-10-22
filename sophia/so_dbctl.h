#ifndef SO_DBCTL_H_
#define SO_DBCTL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sodbctl sodbctl;

struct sodbctl {
	void         *parent;
	char         *name;
	srcomparator  cmp;
	/* logger */
	char         *log_dir;
	int           log_dirwrite;
	int           log_dircreate;
	int           log_sync;
	int           log_rotate_wm;
	int           log_rotate_sync;
	/* index and mvcc */
	char         *dir;
	int           dir_write;
	int           dir_create;
	int           dir_created;
	int           dir_sync;
	int           two_phase_recover;
	uint64_t      memory_limit;
	uint32_t      node_size;
	uint32_t      node_page_size;
	uint32_t      node_branch_wm;
	uint32_t      node_merge_wm;
	/* env */
	int           threads_branch;
	int           threads_merge;
	int           threads;
} srpacked;

int   so_dbctl_init(sodbctl*, char*, void*);
int   so_dbctl_free(sodbctl*);
int   so_dbctl_validate(sodbctl*);
int   so_dbctl_set(sodbctl*, char*, va_list);
void *so_dbctl_get(sodbctl*, char*, va_list);
int   so_dbctl_dump(sodbctl*, srbuf*);

#endif
