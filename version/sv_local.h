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
	uint64_t valueoffset;
	void *key;
	void *value;
};

extern svif sv_localif;

static inline svlocal*
sv_copy(sra *a, sv *v)
{
	int keysize = svkeysize(v);
	int valuesize = svvaluesize(v);
	int size = sizeof(svlocal) + keysize + valuesize;
	svlocal *l = sr_malloc(a, size);
	if (srunlikely(l == NULL))
		return NULL;
	char *key = (char*)l + sizeof(svlocal);
	l->lsn       = svlsn(v);
	l->flags     = svflags(v);
	l->key       = key;
	l->keysize   = keysize; 
	l->value     = key + keysize;
	l->valuesize = valuesize;
	memcpy(key, svkey(v), l->keysize);
	int rc = svvaluecopy(v, key + keysize);
	if (srunlikely(rc == -1)) {
		sr_free(a, l);
		l = NULL;
	}
	return l;
}

#endif
