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
	void     *prefixcopy;
	uint32_t  prefixsize;
	void     *value;
	uint32_t  valuesize;
	/* recover */
	void     *raw;
	uint32_t  rawsize;
	uint32_t  timestamp;
	void     *log;
	/* read options */
	int       cache_only;
	int       oldest_only;
	/* stats */
	int       read_disk;
	int       read_cache;
	int       read_latency;
	/* events */
	int       event;
};

so *se_document_new(se*, so*, sv*);

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

static inline int
se_document_validate(sedocument *o, so *dest, uint8_t flags)
{
	se *e = se_of(&o->o);
	if (ssunlikely(o->o.parent != dest))
		return sr_error(&e->error, "%s", "incompatible document parent db");
	svv *v = o->v.v;
	if (o->flagset) {
		if (ssunlikely(v->flags != flags))
			return sr_error(&e->error, "%s", "incompatible document flags");
	} else {
		o->flagset = 1;
		v->flags = flags;
	}
	if (v->lsn != 0) {
		uint64_t lsn = sr_seq(&e->seq, SR_LSN);
		if (v->lsn <= lsn)
			return sr_error(&e->error, "%s", "incompatible document lsn");
	}
	return 0;
}

#endif
