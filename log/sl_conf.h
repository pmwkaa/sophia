#ifndef SL_CONF_H_
#define SL_CONF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct slconf slconf;

struct slconf {
	char *path;
	int   read_only;
	int   create;
	int   sync_on_rotate;
	int   sync_on_write;
	int   rotatewm;
};

#endif
