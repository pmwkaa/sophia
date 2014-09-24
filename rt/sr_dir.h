#ifndef SR_DIR_H_
#define SR_DIR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srdirtype srdirtype;
typedef struct srdirid srdirid;

struct srdirtype {
	char *ext;
	uint32_t mask;
	int count;
};

struct srdirid {
	uint32_t mask;
	uint64_t id;
};

int sr_dirread(srbuf*, sra*, srdirtype*, char*);

#endif
