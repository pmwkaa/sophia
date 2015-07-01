#ifndef SE_METACURSOR_H_
#define SE_METACURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct semetav semetav;
typedef struct semetacursor semetacursor;

struct semetav {
	so o;
	char *key;
	char *value;
	int keysize;
	int valuesize;
};

struct semetacursor {
	so o;
	ssbuf dump;
	int first;
	srmetadump *pos;
};

so *se_metacursor_new(void*);

#endif
