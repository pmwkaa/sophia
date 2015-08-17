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
#define SR_VERSION_A '2'
#define SR_VERSION_B '1'
#define SR_VERSION_C '1'

#if defined(SOPHIA_BUILD)
# define SR_VERSION_COMMIT SOPHIA_BUILD
#else
# define SR_VERSION_COMMIT "unknown"
#endif

typedef struct srversion srversion;

struct srversion {
	uint64_t magic;
	uint8_t  a, b, c;
} sspacked;

static inline void
sr_version(srversion *v)
{
	v->magic = SR_VERSION_MAGIC;
	v->a = SR_VERSION_A;
	v->b = SR_VERSION_B;
	v->c = SR_VERSION_C;
}

static inline int
sr_versioncheck(srversion *v)
{
	if (v->magic != SR_VERSION_MAGIC)
		return 0;
	if (v->a != SR_VERSION_A)
		return 0;
	if (v->b != SR_VERSION_B)
		return 0;
	if (v->c != SR_VERSION_C)
		return 0;
	return 1;
}

#endif
