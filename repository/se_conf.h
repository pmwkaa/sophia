#ifndef SE_CONF_H_
#define SE_CONF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct seconf seconf;

struct seconf {
	char *path;
	int sync;
};

#endif
