#ifndef SR_C_H_
#define SR_C_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct src src;
typedef struct srcstmt srcstmt;
typedef struct srcv srcv;

typedef enum {
	SR_CSET,
	SR_CGET,
	SR_CSERIALIZE
} srcop;

typedef enum {
	SR_CRO    = 1,
	SR_CC     = 2,
	SR_CU32   = 4,
	SR_CU64   = 8,
	SR_CSZREF = 16,
	SR_CSZ    = 32, 
	SR_CVOID  = 64
} srctype;

typedef int (*srcf)(src*, srcstmt*, va_list);

struct src {
	char    *name;
	uint8_t  flags;
	srcf     function;
	void    *value;
	void    *ptr;
	src     *next;
} sspacked;

struct srcstmt {
	srcop   op;
	char   *path;
	ssbuf  *serialize;
	void  **result;
	void   *ptr;
	sr     *r;
} sspacked;

struct srcv {
	uint8_t  type;
	uint16_t namelen;
	uint32_t valuelen;
} sspacked;

static inline char*
sr_cvname(srcv *v) {
	return (char*)v + sizeof(srcv);
}

static inline char*
sr_cvvalue(srcv *v) {
	return sr_cvname(v) + v->namelen;
}

static inline void*
sr_cvnext(srcv *v) {
	return sr_cvvalue(v) + v->valuelen;
}

int sr_cserialize(src*, srcstmt*);
int sr_cset(src*, srcstmt*, char*);
int sr_cexecv(src*, srcstmt*, va_list);

static inline int
sr_cexec(src *c, srcstmt *stmt, ...)
{
	va_list args;
	va_start(args, stmt);
	int rc = sr_cexecv(c, stmt, args);
	va_end(args);
	return rc;
}

static inline src*
sr_c(src **cp, srcf func, char *name, uint8_t flags, void *v)
{
	src *c = *cp;
	c->function = func;
	c->name     = name;
	c->flags    = flags;
	c->value    = v;
	c->ptr      = NULL;
	c->next     = NULL;
	*cp = c + 1;
	return c;
}

static inline void sr_clink(src **prev, src *c) {
	if (sslikely(*prev))
		(*prev)->next = c;
	*prev = c;
}

static inline src*
sr_cptr(src *c, void *ptr) {
	c->ptr = ptr;
	return c;
}

#endif
