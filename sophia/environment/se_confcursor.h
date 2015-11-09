#ifndef SE_CONFCURSOR_H_
#define SE_CONFCURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct seconfkv seconfkv;
typedef struct seconfcursor seconfcursor;

struct seconfkv {
	so    o;
	char *key;
	char *value;
	int   keysize;
	int   valuesize;
};

struct seconfcursor {
	so o;
	ssbuf dump;
	int first;
	srconfdump *pos;
};

so *se_confcursor_new(void*);

#endif
