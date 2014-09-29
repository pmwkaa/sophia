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
	void *parent;
	char *name;
	srcomparator cmp;
	char *logdir;
	int   logdir_read;
	int   logdir_write;
	int   logdir_create;
	int   logdir_rotate_wm;
	char *dir;
	int   dir_read;
	int   dir_write;
	int   dir_create;
	uint64_t memory_limit;
	uint32_t node_size;
	uint32_t node_page_size;
	uint32_t node_branch_wm;
	uint32_t node_merge_wm;
	int threads_branch;
	int threads_merge;
	int threads;
};

int   so_dbctl_init(sodbctl*, char*, void*);
int   so_dbctl_free(sodbctl*);
int   so_dbctl_validate(sodbctl*);
int   so_dbctl_set(sodbctl*, char*, va_list);
void *so_dbctl_get(sodbctl*, char*, va_list);

#endif
