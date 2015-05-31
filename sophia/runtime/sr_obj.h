#ifndef SR_OBJ_H_
#define SR_OBJ_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srobjif srobjif;
typedef struct srobj srobj;

struct srobjif {
	void *(*ctl)(srobj*, va_list);
	void *(*async)(srobj*, va_list);
	int   (*open)(srobj*, va_list);
	int   (*error)(srobj*, va_list);
	int   (*destroy)(srobj*, va_list);
	int   (*set)(srobj*, va_list);
	int   (*del)(srobj*, va_list);
	void *(*get)(srobj*, va_list);
	void *(*poll)(srobj*, va_list);
	int   (*drop)(srobj*, va_list);
	void *(*begin)(srobj*, va_list);
	int   (*prepare)(srobj*, va_list);
	int   (*commit)(srobj*, va_list);
	void *(*cursor)(srobj*, va_list);
	void *(*object)(srobj*, va_list);
	void *(*type)(srobj*, va_list);
};

struct srobj {
	uint32_t id;
	srobjif *i;
	srobj *env;
	sslist link;
};

static inline void*
sr_cast(void *obj, uint32_t id ssunused)
{
	assert( *(uint32_t*)obj == id );
	return obj;
}

static inline void
sr_objinit(srobj *o, uint32_t id, srobjif *i, srobj *env)
{
	o->id  = id;
	o->i   = i;
	o->env = env;
	ss_listinit(&o->link);
}

static inline int
sr_objopen(srobj *o, ...)
{
	va_list args;
	va_start(args, o);
	int rc = o->i->open(o, args);
	va_end(args);
	return rc;
}

static inline int
sr_objdestroy(srobj *o, ...) {
	va_list args;
	va_start(args, o);
	int rc = o->i->destroy(o, args);
	va_end(args);
	return rc;
}

static inline int
sr_objerror(srobj *o, ...)
{
	va_list args;
	va_start(args, o);
	int rc = o->i->error(o, args);
	va_end(args);
	return rc;
}

static inline void*
sr_objobject(srobj *o, ...)
{
	va_list args;
	va_start(args, o);
	void *h = o->i->object(o, args);
	va_end(args);
	return h;
}

static inline int
sr_objset(srobj *o, ...)
{
	va_list args;
	va_start(args, o);
	int rc = o->i->set(o, args);
	va_end(args);
	return rc;
}

static inline void*
sr_objget(srobj *o, ...)
{
	va_list args;
	va_start(args, o);
	void *h = o->i->get(o, args);
	va_end(args);
	return h;
}

static inline int
sr_objdelete(srobj *o, ...)
{
	va_list args;
	va_start(args, o);
	int rc = o->i->del(o, args);
	va_end(args);
	return rc;
}

static inline void*
sr_objbegin(srobj *o, ...) {
	va_list args;
	va_start(args, o);
	void *h = o->i->begin(o, args);
	va_end(args);
	return h;
}

static inline int
sr_objprepare(srobj *o, ...)
{
	va_list args;
	va_start(args, o);
	int rc = o->i->prepare(o, args);
	va_end(args);
	return rc;
}

static inline int
sr_objcommit(srobj *o, ...)
{
	va_list args;
	va_start(args, o);
	int rc = o->i->commit(o, args);
	va_end(args);
	return rc;
}

static inline void*
sr_objcursor(srobj *o, ...) {
	va_list args;
	va_start(args, o);
	void *h = o->i->cursor(o, args);
	va_end(args);
	return h;
}

#endif
