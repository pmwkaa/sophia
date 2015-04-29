#ifndef SD_COMMIT_H_
#define SD_COMMIT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sd_commit(sdbuild*, sr*, sdindex*, srfile*);
int sd_commitpage(sdbuild*, sr*, srbuf*);

#endif
