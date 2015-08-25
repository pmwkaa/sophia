#ifndef SD_COMMIT_H_
#define SD_COMMIT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sd_commit(sdbuild*, sr*, sdindex*, ssfile*);
int sd_committo(sdbuild*, sr*, sdindex*, char*, int);

int sd_commitpage(sdbuild*, sr*, ssbuf*);

#endif
