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
	/* sl */
	char         *logdir;
	int           logdir_write;
	int           logdir_create;
	int           logdir_sync;
	int           logdir_rotate_wm;
	int           logdir_rotate_sync;
	/* si */
	char         *dir;
	int           dir_write;
	int           dir_create;
	int           dir_sync;
	uint64_t      memory_limit;
	uint32_t      node_size;
	uint32_t      node_page_size;
	uint32_t      node_branch_wm;
	uint32_t      node_merge_wm;
	/* so_db */
	int           threads_branch;
	int           threads_merge;
	int           threads;
};

int   so_dbctl_init(sodbctl*, char*, void*);
int   so_dbctl_free(sodbctl*);
int   so_dbctl_validate(sodbctl*);
int   so_dbctl_set(sodbctl*, char*, va_list);
void *so_dbctl_get(sodbctl*, char*, va_list);
int   so_dbctl_dump(sodbctl*, srbuf*);

#endif
