#ifndef SR_VERSION_H_
#define SR_VERSION_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define SR_VERSION_MAGIC  8529643324614668147ULL
#define SR_VERSION_MAJOR '1'
#define SR_VERSION_MINOR '2'

typedef struct srversion srversion;

struct srversion {
	uint64_t magic;
	uint8_t  major, minor;
} srpacked;

static inline void
sr_version(srversion *v)
{
	v->magic = SR_VERSION_MAGIC;
	v->major = SR_VERSION_MAJOR;
	v->minor = SR_VERSION_MINOR;
}

static inline int
sr_versioncheck(srversion *v)
{
	if (v->magic != SR_VERSION_MAGIC)
		return 0;
	if (v->major != SR_VERSION_MAJOR)
		return 0;
	if (v->minor != SR_VERSION_MINOR)
		return 0;
	return 1;
}

#endif
