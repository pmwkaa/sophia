#ifndef SD_SS_H_
#define SD_SS_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdssheader sdssheader;
typedef struct sdssrecord sdssrecord;
typedef struct sdss sdss;

struct sdssheader {
	uint32_t crc;
	uint32_t count;
} srpacked;

struct sdssrecord {
	uint64_t lsn;
	uint16_t namelen;
	char name[];
} srpacked;

struct sdss {
	srbuf buf;
};

int sd_ssinit(sdss*);
int sd_ssopen(sdss*, sr*, srbuf*);
int sd_ssfree(sdss*, sr*);
int sd_sscreate(sdss*, sr*);
int sd_ssadd(sdss*, sdss*, sr*, uint64_t, char*);
int sd_ssdelete(sdss*, sdss*, sr*, char*);

#endif
