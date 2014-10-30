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
	uint32_t      id;
	srcomparator  cmp;
	char         *dir;
	int           dir_write;
	int           dir_create;
	int           dir_created;
	int           dir_sync;
} srpacked;

int   so_dbctl_init(sodbctl*, char*, void*);
int   so_dbctl_free(sodbctl*);
int   so_dbctl_validate(sodbctl*);
int   so_dbctl_set(sodbctl*, char*, va_list);
void *so_dbctl_get(sodbctl*, char*, va_list);
int   so_dbctl_dump(sodbctl*, srbuf*);

#endif
