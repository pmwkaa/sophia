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
	char     *name;
	char     *path;
	char     *path_backup;
	int       sync;
	uint64_t  node_size;
	uint32_t  node_page_size;
};

#endif
