#ifndef SI_READ_H_
#define SI_READ_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct siread siread;

struct siread {
	ssorder   order;
	void     *prefix;
	void     *key;
	void     *upsert;
	uint32_t  key_size;
	uint32_t  prefix_size;
	uint32_t  upsert_size;
	int       has;
	uint64_t  vlsn;
	svmerge   merge;
	int       cold_only;
	int       read_start;
	int       read_disk;
	int       read_cache;
	int       upsert_eq;
	sv        result;
	sicache  *cache;
	sr       *r;
	si       *index;
};

int  si_readopen(siread*, si*, sicache*, ssorder,
                 uint64_t,
                 void*, uint32_t,
                 void*, uint32_t,
                 void*, uint32_t, int, int, int);
int  si_readclose(siread*);
int  si_read(siread*);
int  si_readcommited(si*, sr*, sv*, int);

#endif
