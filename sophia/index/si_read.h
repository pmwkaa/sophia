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
	uint32_t  keysize;
	uint32_t  prefixsize;
	int       has;
	uint64_t  vlsn;
	svmerge   merge;
	int       cache_only;
	int       read_disk;
	int       read_cache;
	sv       *upsert_v;
	int       upsert_eq;
	sv        result;
	sicache  *cache;
	sr       *r;
	si       *index;
	sitx     *x;
};

int  si_readopen(siread*, sitx*, sicache*, ssorder,
                 uint64_t,
                 void*, uint32_t,
                 void*, uint32_t);
int  si_readclose(siread*);
void si_readcache_only(siread*);
void si_readhas(siread*);
void si_readupsert(siread*, sv*, int);
int  si_read(siread*);
int  si_readcommited(si*, sr*, sv*, int);

#endif
