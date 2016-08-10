#ifndef SD_BUILDINDEX_H_
#define SD_BUILDINDEX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdbuildindex sdbuildindex;

struct sdbuildindex {
	ssbuf         v, m;
	sdindexheader build;
};

void sd_buildindex_init(sdbuildindex*);
void sd_buildindex_free(sdbuildindex*, sr*);
void sd_buildindex_reset(sdbuildindex*);
void sd_buildindex_gc(sdbuildindex*, sr*, int);
int  sd_buildindex_begin(sdbuildindex*);
int  sd_buildindex_end(sdbuildindex*, sr*, sdid*, uint32_t, uint64_t);
int  sd_buildindex_add(sdbuildindex*, sr*, sdbuild*, uint64_t);

#endif
