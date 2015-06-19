#ifndef SL_DIR_H_
#define SL_DIR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sldirtype sldirtype;
typedef struct sldirid sldirid;

struct sldirtype {
	char *ext;
	uint32_t mask;
	int count;
};

struct sldirid {
	uint32_t mask;
	uint64_t id;
};

int sl_dirread(ssbuf*, ssa*, sldirtype*, char*);

#endif
