#ifndef SO_DBCONF_H_
#define SO_DBCONF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sodbconf sodbconf;

struct sodbconf {
	soobj o;
	void *parent;
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

void so_dbconf_init(sodbconf*, void*);
int  so_dbconf_validate(sodbconf*);

#endif
