#ifndef SD_WRITE_H_
#define SD_WRITE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sd_commitpage(sdbuild*, sr*, ssbuf*);

int sd_writepage(sr*, ssfile*, ssblob*, sdbuild*);
int sd_writeindex(sr*, ssfile*, ssblob*, sdindex*);
int sd_writeseal(sr*, ssfile*, ssblob*, sdindex*);

#endif
