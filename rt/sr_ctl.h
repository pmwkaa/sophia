#ifndef SR_CTL_H_
#define SR_CTL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srctl srctl;

typedef int (*srctlf)(srctl*, void*, va_list);

typedef enum {
	SR_CTLRO      = 1,
	SR_CTLINT     = 2,
	SR_CTLU32     = 4,
	SR_CTLU64     = 8,
	SR_CTLSTRING  = 16,
	SR_CTLTRIGGER = 32,
	SR_CTLSUB     = 64
} srctltype;

struct srctl {
	char *name;
	int type;
	void *v;
	srctlf func;
};

static inline srctl*
sr_ctladd(srctl *c, char *name, int type, void *v, srctlf func)
{
	c->name = name;
	c->type = type;
	c->v    = v;
	c->func = func;
	return ++c;
}

int sr_ctlget(srctl*, char**, srctl**);
int sr_ctlset(srctl*, sra*, void*, va_list);

#endif
