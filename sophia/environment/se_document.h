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
	svv      *v;
	ssorder   order;
	int       orderset;
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
	void     *log;
	/* read options */
	int       cold_only;
	/* stats */
	int       read_disk;
	int       read_cache;
	int       read_latency;
};

so *se_document_new(se*, so*, svv*);
int se_document_create(sedocument*, uint8_t);
int se_document_createkey(sedocument*);

static inline int
se_document_validate(sedocument *o, so *dest)
{
	se *e = se_of(&o->o);
	if (o->created)
		return sr_error(&e->error, "%s", "attempt to reuse document");
	if (ssunlikely(o->o.parent != dest))
		return sr_error(&e->error, "%s", "incompatible document parent db");
	return 0;
}

static inline int
se_document_validate_ro(sedocument *o, so *dest)
{
	se *e = se_of(&o->o);
	if (ssunlikely(o->o.parent != dest))
		return sr_error(&e->error, "%s", "incompatible document parent db");
	return 0;
}

#endif
