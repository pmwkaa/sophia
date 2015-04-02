#ifndef SV_LOCAL_H_
#define SV_LOCAL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct svlocal svlocal;

struct svlocal {
	uint64_t lsn;
	uint8_t  flags;
	uint16_t keysize;
	uint32_t valuesize;
	void *key;
	void *value;
};

extern svif sv_localif;

static inline svlocal*
sv_copy(sra *a, sv *v)
{
	int keysize = sv_keysize(v);
	int valuesize = sv_valuesize(v);
	int size = sizeof(svlocal) + keysize + valuesize;
	svlocal *l = sr_malloc(a, size);
	if (srunlikely(l == NULL))
		return NULL;
	char *key = (char*)l + sizeof(svlocal);
	l->lsn       = sv_lsn(v);
	l->flags     = sv_flags(v);
	l->key       = key;
	l->keysize   = keysize; 
	l->value     = key + keysize;
	l->valuesize = valuesize;
	memcpy(key, sv_key(v), l->keysize);
	memcpy(key + keysize, sv_value(v), valuesize);
	return l;
}

#endif
