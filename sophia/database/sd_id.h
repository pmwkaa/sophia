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
	uint32_t parent;
	uint32_t id;
	uint8_t  flags;
} sspacked;

static inline void
sd_idinit(sdid *i, uint32_t id, uint32_t parent, uint32_t flags)
{
	i->id     = id;
	i->parent = parent;
	i->flags  = flags;
}

#endif
