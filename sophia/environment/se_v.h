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
	ssorder   order;
	sfv       keyv[8];
	int       keyc;
	uint16_t  keysize;
	void     *value;
	uint32_t  valuesize;
	void     *raw;
	uint32_t  rawsize;
	void     *prefix;
	uint16_t  prefixsize;
	void     *log;
};

so *se_vnew(se*, so*, sv*);

#endif
