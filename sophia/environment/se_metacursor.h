#ifndef SE_METACURSOR_H_
#define SE_METACURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct semetacursor semetacursor;

struct semetacursor {
	so o;
	ssbuf dump;
	int first;
	srmetadump *pos;
};

so *se_metacursor_new(void*);

#endif
