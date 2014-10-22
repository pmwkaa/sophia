#ifndef SO_OBJ_H_
#define SO_OBJ_H_

/*
 * sophia database
 * sehia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef enum {
	SOUNDEF     = 0L,
	SOENV       = 0x06154834L,
	SOCTL       = 0x1234FFBBL,
	SOCTLCURSOR = 0x6AB65429L,
	SOV         = 0x2FABCDE2L,
	SODB        = 0x34591111L,
	SOTX        = 0x13491FABL,
	SOCURSOR    = 0x45ABCDFAL
} soobjid;

static inline soobjid
so_objof(void *ptr) {
	return *(soobjid*)ptr;
}

typedef struct soobjif soobjif;
typedef struct soobj soobj;

struct soobjif {
	void *(*ctl)(soobj*, va_list);
	int   (*open)(soobj*, va_list);
	int   (*error)(soobj*, va_list);
	int   (*destroy)(soobj*);
	int   (*set)(soobj*, va_list);
	void *(*get)(soobj*, va_list);
	int   (*del)(soobj*, va_list);
	void *(*begin)(soobj*);
	int   (*prepare)(soobj*, va_list);
	int   (*commit)(soobj*, va_list);
	int   (*rollback)(soobj*);
	void *(*cursor)(soobj*, va_list);
	void *(*object)(soobj*, va_list);
	void *(*type)(soobj*, va_list);
};

struct soobj {
	soobjid  id;
	soobjif *i;
	soobj   *env;
	srlist   link;
};

static inline void
so_objinit(soobj *o, soobjid id, soobjif *i, soobj *env)
{
	o->id  = id;
	o->i   = i;
	o->env = env;
	sr_listinit(&o->link);
}

static inline int
so_objdestroy(soobj *o) {
	return o->i->destroy(o);
}

static inline void*
so_objobject(soobj *o, ...)
{
	va_list args;
	va_start(args, o);
	void *h = o->i->object(o, args);
	va_end(args);
	return h;
}

static inline int
so_objset(soobj *o, ...)
{
	va_list args;
	va_start(args, o);
	int rc = o->i->set(o, args);
	va_end(args);
	return rc;
}

static inline void*
so_objget(soobj *o, ...)
{
	va_list args;
	va_start(args, o);
	void *h = o->i->get(o, args);
	va_end(args);
	return h;
}

static inline int
so_objdelete(soobj *o, ...)
{
	va_list args;
	va_start(args, o);
	int rc = o->i->del(o, args);
	va_end(args);
	return rc;
}

static inline void*
so_objbegin(soobj *o) {
	return o->i->begin(o);
}

static inline int
so_objcommit(soobj *o, ...)
{
	va_list args;
	va_start(args, o);
	int rc = o->i->commit(o, args);
	va_end(args);
	return rc;
}

static inline int
so_objrollback(soobj *o) {
	return o->i->rollback(o);
}

#endif
