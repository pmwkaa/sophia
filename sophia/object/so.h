#ifndef SO_H_
#define SO_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct soif soif;
typedef struct sotype sotype;
typedef struct so so;

struct soif {
	int      (*open)(so*);
	int      (*destroy)(so*);
	void     (*free)(so*);
	int      (*error)(so*);
	void    *(*document)(so*);
	void    *(*poll)(so*);
	int      (*setstring)(so*, const char*, void*, int);
	int      (*setint)(so*, const char*, int64_t);
	int      (*setobject)(so*, const char*, void*);
	void    *(*getobject)(so*, const char*);
	void    *(*getstring)(so*, const char*, int*);
	int64_t  (*getint)(so*, const char*);
	int      (*set)(so*, so*);
	int      (*upsert)(so*, so*);
	int      (*del)(so*, so*);
	void    *(*get)(so*, so*);
	void    *(*begin)(so*);
	int      (*prepare)(so*);
	int      (*commit)(so*);
	void    *(*cursor)(so*);
};

struct sotype {
	uint32_t magic;
	char *name;
};

struct so {
	soif *i;
	sotype *type;
	so *parent;
	so *env;
	uint8_t destroyed;
	sslist link;
};

static inline void
so_init(so *o, sotype *type, soif *i, so *parent, so *env)
{
	o->type      = type;
	o->i         = i;
	o->parent    = parent;
	o->env       = env;
	o->destroyed = 0;
	ss_listinit(&o->link);
}

static inline void
so_mark_destroyed(so *o)
{
	o->destroyed = 1;
}

static inline void*
so_cast_dynamic(void *ptr, sotype *type,
          const char *file,
          const char *function, int line)
{
	int eq = ptr != NULL && ((so*)ptr)->type == type;
	if (sslikely(eq))
		return ptr;
	fprintf(stderr, "%s:%d %s(%p) expected '%s' object\n",
	        file, line, function, ptr, type->name);
	abort();
	return NULL;
}

#define so_cast(o, cast, type) \
	((cast)so_cast_dynamic(o, type, __FILE__, __func__, __LINE__))

#define so_open(o)      (o)->i->open(o)
#define so_destroy(o)   (o)->i->destroy(o)
#define so_free(o)      (o)->i->free(o)
#define so_error(o)     (o)->i->error(o)
#define so_document(o)  (o)->i->document(o)
#define so_poll(o)      (o)->i->poll(o)
#define so_set(o, v)    (o)->i->set(o, v)
#define so_upsert(o, v) (o)->i->upsert(o, v)
#define so_delete(o, v) (o)->i->del(o, v)
#define so_get(o, v)    (o)->i->get(o, v)
#define so_begin(o)     (o)->i->begin(o)
#define so_prepare(o)   (o)->i->prepare(o)
#define so_commit(o)    (o)->i->commit(o)
#define so_cursor(o)    (o)->i->cursor(o)

#define so_setstring(o, path, pointer, size) \
	(o)->i->setstring(o, path, pointer, size)
#define so_setint(o, path, v) \
	(o)->i->setint(o, path, v)
#define so_getobject(o, path) \
	(o)->i->getobject(o, path)
#define so_getstring(o, path, sizep) \
	(o)->i->getstring(o, path, sizep)
#define so_getint(o, path) \
	(o)->i->getnumber(o, path)

#endif
