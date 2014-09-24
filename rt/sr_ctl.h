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

typedef enum {
	SR_CTLINT,
	SR_CTLU32,
	SR_CTLU64,
	SR_CTLSTRING
} srctltype;

struct srctl {
	char *name;
	srctltype type;
	void *ptr;
	int set;
};

int sr_ctl(srctl**, char*, char*, va_list);

#endif
