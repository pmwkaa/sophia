#ifndef SF_UPDATE_H_
#define SF_UPDATE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef int (*sfupdatef)(int, char*, int, int, char*, int, void*, void**, int*);

typedef struct {
	sfupdatef function;
	void *arg;
} sfupdate;

static inline void
sf_updateinit(sfupdate *u)
{
	memset(u, 0, sizeof(*u));
}

static inline void
sf_updateset(sfupdate *u, sfupdatef function)
{
	u->function = function;
}

static inline void
sf_updateset_arg(sfupdate *u, void *arg)
{
	u->arg = arg;
}

static inline int
sf_updatehas(sfupdate *u) {
	return u->function != NULL;
}

#endif
