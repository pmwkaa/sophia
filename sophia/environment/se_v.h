#ifndef SE_V_H_
#define SE_V_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sev sev;

struct sev {
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
	int       cache_only;
	int       async;
	int       async_status;
	uint32_t  async_operation;
	uint64_t  async_seq;
	void     *async_arg;
};

so *se_vnew(se*, so*, sv*, int);

#endif
