#ifndef SR_KEY_H_
#define SR_KEY_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srkeypart srkeypart;
typedef struct srkey srkey;

typedef int (*srcmpf)(char*, int, char*, int, void*);

typedef enum {
	SR_U32,
	SR_U64,
	SR_STRING
} srkeytype;

struct srkeypart {
	srcmpf cmpprefix;
	srcmpf cmpraw;
	srcmpf cmp;
	srkeytype type;
	char *name;
	char *path;
	int pos;
};

struct srkey {
	srkeypart *parts;
	int count;
	srcmpf cmp;
	void *cmparg;
	srcmpf cmpprefix;
	void *cmpprefix_arg;
};

int sr_keycompare_prefix(char*, int, char*, int, void*);
int sr_keycompare(char*, int, char*, int, void*);

static inline void
sr_keyinit(srkey *key)
{
	key->parts = NULL;
	key->count = 0;
	key->cmp = sr_keycompare;
	key->cmparg = key;
	key->cmpprefix = sr_keycompare_prefix;
	key->cmpprefix_arg = key;
}

static inline void
sr_keyfree(srkey *key, sra *a)
{
	if (key->parts == NULL)
		return;
	int i = 0;
	while (i < key->count) {
		if (key->parts[i].name)
			sr_free(a, key->parts[i].name);
		if (key->parts[i].path)
			sr_free(a, key->parts[i].path);
		i++;
	}
	sr_free(a, key->parts);
	key->parts = NULL;
}

static inline void
sr_keysetcmp(srkey *key, srcmpf cmp, void *arg)
{
	key->cmp = cmp;
	key->cmparg = arg;
}

static inline void
sr_keysetcmp_prefix(srkey *key, srcmpf cmp, void *arg)
{
	key->cmpprefix = cmp;
	key->cmpprefix_arg = arg;
}

static inline srkeypart*
sr_keyadd(srkey *key, sra *a)
{
	srkeypart *parts =
		sr_malloc(a, sizeof(srkeypart) * (key->count + 1));
	if (srunlikely(parts == NULL))
		return NULL;
	memcpy(parts, key->parts, sizeof(srkeypart) * key->count);
	if (key->parts)
		sr_free(a, key->parts);
	key->parts = parts;
	int pos = key->count++;
	srkeypart *part = &key->parts[pos];
	memset(part, 0, sizeof(*part));
	part->pos = pos;
	return part;
}

static inline int
sr_keydelete(srkey *key, sra *a, int pos)
{
	srkeypart *parts =
		sr_malloc(a, sizeof(srkeypart) * (key->count - 1));
	if (srunlikely(parts == NULL))
		return -1;
	int i = 0;
	int j = 0;
	while (i < key->count)
	{
		if (i == pos) {
			if (key->parts[i].name)
				sr_free(a, key->parts[i].name);
			if (key->parts[i].path)
				sr_free(a, key->parts[i].path);
			i++;
			continue;
		}
		parts[j++] = key->parts[i];
		i++;
	}
	if (key->parts)
		sr_free(a, key->parts);
	key->parts = parts;
	key->count -= 1;
	return 0;
}

static inline srkeypart*
sr_keyfind(srkey *key, char *name)
{
	int i = 0;
	while (i < key->count) {
		if (strcmp(key->parts[i].name, name) == 0)
			return &key->parts[i];
		i++;
	}
	return NULL;
}

static inline srkeypart*
sr_keyof(srkey *key, int pos)
{
	assert(pos < key->count);
	return &key->parts[pos];
}

int sr_keypart_setname(srkeypart*, sra*, char*);
int sr_keypart_set(srkeypart*, sra*, char*);

static inline int
sr_compare(srkey *key, char *a, int asize, char *b, int bsize) {
	return key->cmp(a, asize, b, bsize, key->cmparg);
}

static inline int
sr_compareprefix(srkey *key, char *prefix, int prefixsize, char *b, int bsize)
{
	return key->cmpprefix(prefix, prefixsize, b, bsize, key->cmpprefix_arg);
}

#endif
