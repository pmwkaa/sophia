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
typedef struct srctldump srctldump;

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

struct srctldump {
	uint8_t type;
	int namelen;
	int valuelen;
} srpacked;

static inline srctl*
sr_ctladd(srctl *c, char *name, int type, void *v, srctlf func)
{
	c->name = name;
	c->type = type;
	c->v    = v;
	c->func = func;
	return ++c;
}

static inline char*
sr_ctldump_name(srctldump *c) {
	return (char*)c + sizeof(srctldump);
}

static inline char*
sr_ctldump_value(srctldump *c) {
	return (char*)c + sizeof(srctldump) + c->namelen;
}

int sr_ctlget(srctl*, char**, srctl**);
int sr_ctlset(srctl*, sra*, void*, va_list);
int sr_ctlserialize(srctl*, sra*, char*, srbuf*);

#endif
