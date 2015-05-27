#ifndef SS_HASH_H_
#define SS_HASH_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline unsigned int
ss_fnv(char *key, int len)
{
	unsigned char *p = (unsigned char*)key;
	unsigned char *end = p + len;
	unsigned h = 2166136261;
	while (p < end) {
		h = (h * 16777619) ^ *p;
		p++;
	}
	return h;
}

#endif
