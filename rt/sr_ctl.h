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
	SR_CTLTRIGGER = 32
} srctltype;

struct srctl {
	char *name;
	int type;
	void *v;
	srctlf func;
};

int sr_ctlget(srctl*, char*, srctl**);
int sr_ctlset(srctl*, sra*, void*, va_list);

#endif
