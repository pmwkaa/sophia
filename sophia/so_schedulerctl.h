#ifndef SO_SCHEDULERCTL_H_
#define SO_SCHEDULERCTL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int   so_schedulerctl_set(soobj*, char*, va_list);
void *so_schedulerctl_get(soobj*, char*, va_list);
int   so_schedulerctl_dump(soobj*, srbuf*);

#endif
