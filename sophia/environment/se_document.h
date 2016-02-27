#ifndef SE_DOCUMENT_H_
#define SE_DOCUMENT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sedocument sedocument;

struct sedocument {
	so        o;
	sv        v;
	sv        vprefix;
	ssorder   order;
	int       orderset;
	sfv       keyv[8];
	int       keyc;
	void     *prefix;
	uint32_t  prefixsize;
	void     *value;
	uint32_t  valuesize;
	/* recover */
	void     *raw;
	uint32_t  rawsize;
	void     *log;
	/* misc */
	int       immutable;
	/* read tweaks */
	int       cache_only;
	int       oldest_only;
	/* stats */
	int       read_disk;
	int       read_cache;
	int       read_latency;
	/* events */
	int       event;
};

so *se_document_new(se*, so*, sv*);

#endif
