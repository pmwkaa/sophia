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
	char     *path;
	int       sync;
	uint32_t  node_size;
	uint32_t  node_page_size;
};

#endif
