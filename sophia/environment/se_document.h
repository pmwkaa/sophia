#ifndef SE_DOCUMENT_H_
#define SE_DOCUMENT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sedocument sedocument;

struct sedocument {
	so        o;
	int       created;
	sv        v;
	ssorder   order;
	int       orderset;
	int       flagset;
	sfv       fields[8];
	int       fields_count;
	int       fields_count_keys;
	void     *prefix;
	void     *prefix_copy;
	uint32_t  prefix_size;
	void     *value;
	uint32_t  value_size;
	/* recover */
	void     *raw;
	uint32_t  raw_size;
	uint32_t  timestamp;
	void     *log;
	/* read options */
	int       cold_only;
	/* stats */
	int       read_disk;
	int       read_cache;
	int       read_latency;
};

so *se_document_new(se*, so*, sv*);
int se_document_create(sedocument*);
int se_document_createkey(sedocument*);

static inline int
se_document_validate_ro(sedocument *o, so *dest)
{
	se *e = se_of(&o->o);
	if (ssunlikely(o->o.parent != dest))
		return sr_error(&e->error, "%s", "incompatible document parent db");
	svv *v = o->v.v;
	if (! o->flagset) {
		o->flagset = 1;
		v->flags = SVGET;
	}
	return 0;
}

int se_document_validate(sedocument*, so*, uint8_t);

#endif
