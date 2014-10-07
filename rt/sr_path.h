#ifndef SR_PATH_H_
#define SR_PATH_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srpath srpath;

struct srpath {
	char path[PATH_MAX];
};

static inline void
sr_pathset(srpath *p, char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(p->path, sizeof(p->path), fmt, args);
	va_end(args);
}

static inline void
sr_pathA(srpath *p, char *dir, uint32_t id, char *ext)
{
	sr_pathset(p, "%s/%010"PRIu32"%s", dir, id, ext);
}

static inline void
sr_pathAB(srpath *p, char *dir, uint32_t a, uint32_t b, char *ext)
{
	sr_pathset(p, "%s/%010"PRIu32".%010"PRIu32"%s", dir, a, b, ext);
}

#endif
