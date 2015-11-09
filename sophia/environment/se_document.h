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
	/* async */
	int       immutable;
	int       cache_only;
	int       async;
	int       async_status;
	uint32_t  async_operation;
	uint64_t  async_seq;
	void     *async_arg;
	/* stats */
	int       read_disk;
	int       read_cache;
	int       read_latency;
};

so *se_document_new(se*, so*, sv*, int);

#endif
