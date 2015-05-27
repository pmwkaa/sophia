#ifndef SS_DIR_H_
#define SS_DIR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssdirtype ssdirtype;
typedef struct ssdirid ssdirid;

struct ssdirtype {
	char *ext;
	uint32_t mask;
	int count;
};

struct ssdirid {
	uint32_t mask;
	uint64_t id;
};

int ss_dirread(ssbuf*, ssa*, ssdirtype*, char*);

#endif
