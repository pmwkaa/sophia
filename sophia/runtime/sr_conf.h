#ifndef SR_CONF_H_
#define SR_CONF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srconf srconf;
typedef struct srconfdump srconfdump;
typedef struct srconfstmt srconfstmt;

typedef int (*srconff)(srconf*, srconfstmt*);

typedef enum {
	SR_WRITE,
	SR_READ,
	SR_SERIALIZE
} srconfop;

enum {
	SR_RO = 1,
	SR_NS = 2
};

struct srconf {
	char    *key;
	int      flags;
	sstype   type;
	srconff  function;
	void    *value;
	void    *ptr;
	srconf  *next;
};

struct srconfdump {
	uint8_t  type;
	uint16_t keysize;
	uint32_t valuesize;
} sspacked;

struct srconfstmt {
	srconfop    op;
	const char *path;
	void       *value;
	sstype      valuetype;
	int         valuesize;
	srconf     *match;
	ssbuf      *serialize;
	void       *ptr;
	sr         *r;
};

int sr_confexec(srconf*, srconfstmt*);
int sr_conf_read(srconf*, srconfstmt*);
int sr_conf_write(srconf*, srconfstmt*);
int sr_conf_serialize(srconf*, srconfstmt*);

static inline srconf*
sr_c(srconf **link, srconf **cp, srconff func,
     char *key, int type,
     void *value)
{
	srconf *c = *cp;
	c->key      = key;
	c->function = func;
	c->flags    = 0;
	c->type     = type;
	c->value    = value;
	c->ptr      = NULL;
	c->next     = NULL;
	*cp = c + 1;
	if (sslikely(link)) {
		if (sslikely(*link))
			(*link)->next = c;
		*link = c;
	}
	return c;
}

static inline srconf*
sr_C(srconf **link, srconf **cp, srconff func,
     char *key, int type,
     void *value, int flags, void *ptr)
{
	srconf *c = sr_c(link, cp, func, key, type, value);
	c->flags = flags;
	c->ptr = ptr;
	return c;
}

static inline char*
sr_confkey(srconfdump *v) {
	return (char*)v + sizeof(srconfdump);
}

static inline char*
sr_confvalue(srconfdump *v) {
	return sr_confkey(v) + v->keysize;
}

static inline void*
sr_confnext(srconfdump *v) {
	return sr_confvalue(v) + v->valuesize;
}

#endif
