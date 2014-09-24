#ifndef SI_CONF_H_
#define SI_CONF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct siconf siconf;

struct siconf {
	char *dir;
	int dir_read;
	int dir_write;
	int dir_create;
	uint64_t memory_limit;
	uint32_t node_size;
	uint32_t node_page_size;
	uint32_t node_branch_wm;
	uint32_t node_merge_wm;
};

#endif
