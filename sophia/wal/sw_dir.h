#ifndef SW_DIR_H_
#define SW_DIR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct swdirtype swdirtype;
typedef struct swdirid swdirid;

struct swdirtype {
	char *ext;
	uint32_t mask;
	int count;
};

struct swdirid {
	uint32_t mask;
	uint64_t id;
};

int sw_dirread(ssbuf*, ssa*, swdirtype*, char*);

#endif
