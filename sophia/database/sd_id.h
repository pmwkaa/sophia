#ifndef SD_ID_H_
#define SD_ID_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdid sdid;

#define SD_IDBRANCH 1

struct sdid {
	uint64_t parent;
	uint64_t id;
	uint8_t  flags;
} sspacked;

static inline void
sd_idinit(sdid *i, uint64_t id, uint64_t parent, uint8_t flags)
{
	i->id     = id;
	i->parent = parent;
	i->flags  = flags;
}

#endif
