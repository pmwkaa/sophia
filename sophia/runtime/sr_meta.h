#ifndef SR_META_H_
#define SR_META_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srmeta srmeta;
typedef struct srmetadump srmetadump;
typedef struct srmetastmt srmetastmt;

typedef int (*srmetaf)(srmeta*, srmetastmt*);

typedef enum {
	SR_WRITE,
	SR_READ,
	SR_SERIALIZE
} srmetaop;

enum {
	SR_RO = 1,
	SR_NS = 2
};

struct srmeta {
	char    *key;
	int      flags;
	sstype   type;
	srmetaf  function;
	void    *value;
	void    *ptr;
	srmeta  *next;
};

struct srmetadump {
	uint8_t  type;
	uint16_t keysize;
	uint32_t valuesize;
} sspacked;

struct srmetastmt {
	srmetaop    op;
	const char *path;
	void       *value;
	sstype      valuetype;
	int         valuesize;
	srmeta     *match;
	ssbuf      *serialize;
	void       *ptr;
	sr         *r;
};

int sr_metaexec(srmeta*, srmetastmt*);
int sr_meta_read(srmeta*, srmetastmt*);
int sr_meta_write(srmeta*, srmetastmt*);
int sr_meta_serialize(srmeta*, srmetastmt*);

static inline srmeta*
sr_m(srmeta **link, srmeta **cp, srmetaf func,
     char *key, int type,
     void *value)
{
	srmeta *c = *cp;
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

static inline srmeta*
sr_M(srmeta **link, srmeta **cp, srmetaf func,
      char *key, int type,
      void *value, int flags, void *ptr)
{
	srmeta *c = sr_m(link, cp, func, key, type, value);
	c->flags = flags;
	c->ptr = ptr;
	return c;
}

static inline char*
sr_metakey(srmetadump *v) {
	return (char*)v + sizeof(srmetadump);
}

static inline char*
sr_metavalue(srmetadump *v) {
	return sr_metakey(v) + v->keysize;
}

static inline void*
sr_metanext(srmetadump *v) {
	return sr_metavalue(v) + v->valuesize;
}

#endif
