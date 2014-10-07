#ifndef SI_SPLIT_H_
#define SI_SPLIT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct {
	sinode   *parent;
	int       flags;
	sriter   *i;
	uint32_t  size_node;
	uint32_t  size_key;
	uint32_t  size_stream;
	uint64_t  lsvn;
	siconf   *conf;
} sisplit;

int si_split(sisplit*, sr*, sdc*, srbuf*);
int si_splitfree(srbuf*, sr*);

#endif
