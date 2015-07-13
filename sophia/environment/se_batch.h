#ifndef SE_BATCH_H_
#define SE_BATCH_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sebatch sebatch;

struct sebatch {
	so o;
	uint64_t lsn;
	svlog log;
};

so *se_batchnew(sedb*);

#endif
