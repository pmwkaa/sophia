#ifndef SC_READ_H_
#define SC_READ_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct screadarg screadarg;
typedef struct scread scread;

struct screadarg {
	sv        v;
	sv        vprefix;
	sv        vup;
	sicache  *cache;
	int       cachegc;
	ssorder   order;
	int       has;
	int       upsert;
	int       upsert_eq;
	int       cache_only;
	int       oldest_only;
	uint64_t  vlsn;
	int       vlsn_generate;
};

struct scread {
	so         *db;
	si         *index;
	screadarg   arg;
	int         start;
	int         read_disk;
	int         read_cache;
	svv        *result;
	int         rc;
	sr         *r;
};

void sc_readopen(scread*, sr*, so*, si*);
void sc_readclose(scread*);
int  sc_read(scread*, sc*);

#endif
