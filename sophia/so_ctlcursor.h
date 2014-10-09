#ifndef SO_CTLCURSOR_H_
#define SO_CTLCURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct soctlcursor soctlcursor;

struct soctlcursor {
	soobj o;
	int ready;
	srbuf dump;
	srctldump *pos;
	soobj *v;
	void *e;
};

soobj *so_ctlcursor_new(void*);

#endif
