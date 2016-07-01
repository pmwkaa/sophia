#ifndef SD_ITER_H_
#define SD_ITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sd_iter_open(ssiter*, sr*, ssfile*);
int sd_iter_iserror(ssiter*);
int sd_iter_isroot(ssiter*);

extern ssiterif sd_iter;

#endif
